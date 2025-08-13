# app/mqtt_handler.py
import json, time, logging
from queue import Queue
from datetime import datetime
from dateutil import parser as dtparser
import paho.mqtt.client as mqtt

from .db import get_session
from .models import Device, Telemetry, Event
from .settings import settings

log = logging.getLogger("mqtt")
logging.basicConfig(level=logging.INFO)

def _parse_ts(ts: str | None) -> datetime:
    if not ts:
        return datetime.utcnow()
    try:
        return dtparser.isoparse(ts)
    except Exception:
        return datetime.utcnow()
    
# top of file (near other imports)
def _rc_int(rc) -> int:
    # Handles paho v2.x ReasonCode objects or plain ints
    try:
        return int(getattr(rc, "value", rc))
    except Exception:
        return -1

def _rc_str(rc) -> str:
    # Nice printable form for logs
    name = getattr(rc, "name", None) or getattr(rc, "getName", None)
    if callable(name):
        try:
            return f"{getattr(rc, 'value', rc)}:{name()}"
        except Exception:
            pass
    val = getattr(rc, "value", rc)
    return str(val)

def start_mqtt(message_queue: Queue):
    client = mqtt.Client(
        client_id=f"api-subscriber-{int(time.time())}",
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,  # paho 2.x
    )
    client.enable_logger(log)  # paho internal logs → stdout

    if settings.mqtt_username and settings.mqtt_password:
        client.username_pw_set(settings.mqtt_username, settings.mqtt_password)

    client.reconnect_delay_set(min_delay=1, max_delay=30)

    # Debug counters
    stats = {"rx_total": 0, "rx_tel": 0, "rx_status": 0, "rx_event": 0}

    def on_connect(client, userdata, flags, reason_code, properties):
        rc = _rc_int(reason_code)
        print(f"[MQTT] rc={rc}", flush=True)
        if rc != mqtt.CONNACK_ACCEPTED:
            print(f"[MQTT] Connect failed rc={rc} (5=Not authorized). Retrying…", flush=True)
            return
        topic = f"{settings.mqtt_topic_base}/+/+/#"
        res, mid = client.subscribe(topic, qos=0)
        print(f"[MQTT] Connected rc=0. SUB {topic} res={res} mid={mid}", flush=True)

        # (Optional) catch-all for debugging ACL/wildcards. Enable once if needed:
        # res2, mid2 = client.subscribe(f"{settings.mqtt_topic_base}/#", qos=0)
        # print(f"[MQTT] Debug SUB {settings.mqtt_topic_base}/# res={res2} mid={mid2}", flush=True)

    def on_subscribe(client, userdata, mid, granted_qos, properties):
        print(f"[MQTT] SUBACK mid={mid} granted_qos={granted_qos}", flush=True)
        if any(q == 0x80 for q in granted_qos):
            print("[MQTT] WARNING: subscription rejected by broker ACL. Check mosquitto.conf/acl.", flush=True)

    def on_disconnect(client, userdata, reason_code, properties):
        print(f"[MQTT] Disconnected rc={int(reason_code)}. Reconnecting…", flush=True)

    # Uncomment for deep protocol traces from paho if needed:
    # def on_log(client, userdata, level, buf):
    #     print(f"[PAHO] {level} {buf}", flush=True)
    # client.on_log = on_log

    def on_message(client, userdata, msg):
        try:
            stats["rx_total"] += 1
            parts = msg.topic.split("/")
            if len(parts) < 4:
                return
            _, dev_type, dev_id, suffix = parts[:4]
            payload = json.loads(msg.payload.decode("utf-8")) if msg.payload else {}

            # Ensure device exists
            with get_session() as s:
                d = s.get(Device, dev_id)
                if not d:
                    d = Device(id=dev_id, name=payload.get("name") or f"{dev_type}-{dev_id}", type=dev_type)
                    s.add(d); s.commit()
                    
                dev_name = d.name

            ts = payload.get("ts") or payload.get("at") or datetime.utcnow().isoformat()
            out = {"device_id": dev_id, "type": dev_type, "ts": ts, "name": dev_name}

            if suffix == "telemetry":
                stats["rx_tel"] += 1
                metrics = payload.get("metrics") or payload.get("data") or {}
                with get_session() as s:
                    s.add(Telemetry(device_id=dev_id, ts=_parse_ts(ts), data=metrics))
                    s.commit()
                out["kind"] = "telemetry"; out["data"] = metrics

            elif suffix == "status":
                stats["rx_status"] += 1
                out["kind"] = "status"; out["status"] = payload.get("status", "unknown")

            elif suffix == "event":
                stats["rx_event"] += 1
                out["kind"] = "event"
                out["event"] = payload.get("event")
                out["level"] = payload.get("level", "info")
                #if "details" in payload: out["details"] = payload["details"]
                details = payload.get("details") or {}
                out["details"] = details
                # persist event for fault history
                with get_session() as s:
                    s.add(Event(
                        device_id=dev_id,
                        ts=_parse_ts(ts),
                        level=out["level"],
                        event=out["event"],
                        details=details,
                    ))
                    s.commit()
            else:
                return

            # Occasionally print counters so you know it's alive
            if stats["rx_total"] % 10 == 1:
                print(f"[MQTT] msg counts: total={stats['rx_total']} tel={stats['rx_tel']} status={stats['rx_status']} event={stats['rx_event']}", flush=True)

            message_queue.put(json.dumps(out))
        except Exception as e:
            print(f"[MQTT] on_message error: {e}", flush=True)

    client.on_connect = on_connect
    client.on_subscribe = on_subscribe
    client.on_disconnect = on_disconnect
    client.on_message = on_message

    print(
        f"[MQTT] Bootstrapping host={settings.mqtt_host} port={settings.mqtt_port} "
        f"user={'<set>' if settings.mqtt_username else '<none>'} base={settings.mqtt_topic_base}",
        flush=True,
    )

    client.connect(settings.mqtt_host, settings.mqtt_port, keepalive=30)
    client.loop_start()
    return client
