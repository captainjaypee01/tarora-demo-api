import asyncio
import json
import uuid
from queue import Queue, Empty
from typing import List

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Query
from fastapi.responses import JSONResponse
from sqlmodel import select
import paho.mqtt.client as mqtt

from .db import init_db, get_session
from .models import Device, Telemetry, Command, Event
from .schemas import DeviceOut, TelemetryOut, InsightRequest, CommandRequest, CommandResponse, DeviceUpdate, EventOut
from .mqtt_handler import start_mqtt
from .ws_manager import ConnectionManager
from .ai import generate_insight
from .utils import add_cors
from .settings import settings

app = FastAPI(title="Tarora API", version="0.1.0")
add_cors(app)

manager = ConnectionManager()
message_queue: Queue[str] = Queue()
mqtt_client: mqtt.Client | None = None

@app.on_event("startup")
async def on_startup():
    global mqtt_client
    init_db()
    try:
        mqtt_client = start_mqtt(message_queue)
    except Exception as e:
        print(f"[MQTT] failed to start: {e}", flush=True)
        mqtt_client = None
    asyncio.create_task(queue_forwarder())

async def queue_forwarder():
    while True:
        try:
            msg = message_queue.get(timeout=1.0)
            await manager.broadcast_text(msg)
        except Empty:
            await asyncio.sleep(0.1)

@app.get("/api/devices", response_model=List[DeviceOut])
def list_devices():
    with get_session() as session:
        rows = session.exec(select(Device).order_by(Device.created_at.desc())).all()
        return [DeviceOut(id=r.id, name=r.name, type=r.type) for r in rows]

@app.put("/api/devices/{device_id}", response_model=DeviceOut)
def update_device(device_id: str, body: DeviceUpdate):
    with get_session() as session:
        d = session.get(Device, device_id)
        if not d:
            raise HTTPException(status_code=404, detail="Device not found")
        d.name = body.name
        session.add(d)
        session.commit()
        session.refresh(d)
        return DeviceOut(id=d.id, name=d.name, type=d.type)

@app.get("/api/telemetry", response_model=List[TelemetryOut])
def get_telemetry(device_id: str, limit: int = 200):
    with get_session() as session:
        stmt = select(Telemetry).where(Telemetry.device_id == device_id)
        stmt = stmt.order_by(Telemetry.ts.desc()).limit(limit)
        rows = session.exec(stmt).all()
        return [TelemetryOut(device_id=r.device_id, ts=r.ts, data=r.data) for r in rows]
    
@app.get("/api/events", response_model=List[EventOut])
def list_events(device_id: str | None = None, level: str | None = None, limit: int = 200):
    with get_session() as session:
        stmt = select(Event)
        if device_id:
            stmt = stmt.where(Event.device_id == device_id)
        if level:
            stmt = stmt.where(Event.level == level)
        stmt = stmt.order_by(Event.ts.desc()).limit(limit)
        rows = session.exec(stmt).all()
        # enrich with device name for convenience
        out: list[EventOut] = []
        for r in rows:
            d = session.get(Device, r.device_id)
            out.append(EventOut(
                id=r.id, device_id=r.device_id, ts=r.ts, level=r.level, event=r.event,
                details=r.details, name=(d.name if d else None)
            ))
        return out

@app.get("/api/faults/summary")
def faults_summary(since_hours: int = 24):
    """Simple per-device alarm counts in the last N hours."""
    from datetime import datetime, timedelta
    since = datetime.utcnow() - timedelta(hours=since_hours)
    with get_session() as session:
        rows = session.exec(
            select(Event).where(Event.ts >= since, Event.level.in_(["alarm", "error"]))
        ).all()
        agg: dict[str, dict] = {}
        for e in rows:
            entry = agg.setdefault(e.device_id, {"alarm": 0, "error": 0, "last": None})
            entry[e.level] = entry.get(e.level, 0) + 1
            entry["last"] = max(entry["last"] or e.ts, e.ts)
        # attach names
        for dev_id in list(agg.keys()):
            d = session.get(Device, dev_id)
            agg[dev_id]["name"] = d.name if d else dev_id
        return agg

@app.post("/api/insights")
def insights(req: InsightRequest, debug: bool = Query(False)):
    result = generate_insight(req.device_id, req.horizon_minutes, debug=debug)
    # When debug=True, `result` is already a dict with { summary, ...meta }
    return JSONResponse(result if debug else {"summary": result})

@app.post("/api/commands", response_model=CommandResponse)
def post_command(cmd: CommandRequest):
    # Ensure device exists and capture fields while session is open
    with get_session() as session:
        d = session.get(Device, cmd.device_id)
        if not d:
            raise HTTPException(status_code=404, detail="Device not found")

        d_id = d.id            # copy before session closes
        d_type = d.type        # copy before session closes
        command_id = f"cmd_{uuid.uuid4().hex[:10]}"

        # queue the command in DB
        rec = Command(
            command_id=command_id,
            device_id=cmd.device_id,
            command=cmd.command,
            params=cmd.params,
            status="queued",
        )
        session.add(rec)
        session.commit()

    # Publish to MQTT (outside DB session is fine)
    if mqtt_client is None:
        raise HTTPException(status_code=500, detail="MQTT not initialized")

    topic = f"{settings.mqtt_topic_base}/{d_type}/{d_id}/command"
    payload = json.dumps({
        "command_id": command_id,
        "command": cmd.command,
        "params": cmd.params
    })

    try:
        info = mqtt_client.publish(topic, payload, qos=0, retain=False)
        # paho v2: info.rc == mqtt.MQTT_ERR_SUCCESS (0) when queued
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError(f"publish rc={info.rc}")
        info.wait_for_publish()

        with get_session() as session:
            row = session.exec(select(Command).where(Command.command_id == command_id)).first()
            if row:
                row.status = "sent"
                session.add(row)
                session.commit()
    except Exception as e:
        with get_session() as session:
            row = session.exec(select(Command).where(Command.command_id == command_id)).first()
            if row:
                row.status = "failed"
                session.add(row)
                session.commit()
        raise HTTPException(status_code=502, detail=f"MQTT publish failed: {e}")

    return CommandResponse(status="queued", commandId=command_id)

@app.websocket("/ws/telemetry")
async def telemetry_ws(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await asyncio.sleep(30)
    except WebSocketDisconnect:
        await manager.disconnect(websocket)