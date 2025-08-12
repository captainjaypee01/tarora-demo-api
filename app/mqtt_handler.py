import json
import time
from queue import Queue
from datetime import datetime
import paho.mqtt.client as mqtt
from sqlmodel import select

from .db import get_session
from .models import Device, Telemetry
from .settings import settings

# Topic format: {base}/{device_type}/{device_id}/telemetry
# Payload: {"ts": ISO8601, "metrics": { ... }, "name": "Optional Device Name"}

def start_mqtt(message_queue: Queue):
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"api-subscriber-{int(time.time())}")

    if settings.mqtt_username and settings.mqtt_password:
        client.username_pw_set(settings.mqtt_username, settings.mqtt_password)

    def on_connect(client, userdata, flags, reason_code, properties=None):
        topic = f"{settings.mqtt_topic_base}/+/+/telemetry"
        client.subscribe(topic)
        print(f"[MQTT] Connected. Subscribed to {topic}")

    def on_message(client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
        except Exception:
            return
        parts = msg.topic.split("/")
        if len(parts) < 4:
            return
        _, device_type, device_id, _ = parts[:4]

        ts_str = payload.get("ts")
        try:
            ts = datetime.fromisoformat(ts_str) if ts_str else datetime.utcnow()
        except Exception:
            ts = datetime.utcnow()
        name = payload.get("name", f"{device_type}-{device_id}")
        metrics = payload.get("metrics", {})

        with get_session() as session:
            device = session.get(Device, device_id)
            if not device:
                device = Device(id=device_id, name=name, type=device_type)
                session.add(device)
                session.commit()
            telem = Telemetry(device_id=device_id, ts=ts, data=metrics)
            session.add(telem)
            session.commit()

        out = {
            "topic": msg.topic,
            "device_id": device_id,
            "type": device_type,
            "ts": ts.isoformat(),
            "data": metrics,
        }
        message_queue.put(json.dumps(out))

    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(settings.mqtt_host, settings.mqtt_port, keepalive=30)
    client.loop_start()
    return client