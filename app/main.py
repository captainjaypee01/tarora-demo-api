import asyncio
import json
import uuid
from queue import Queue, Empty
from typing import List

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import JSONResponse
from sqlmodel import select
import paho.mqtt.client as mqtt

from .db import init_db, get_session
from .models import Device, Telemetry, Command
from .schemas import DeviceOut, TelemetryOut, InsightRequest, CommandRequest, CommandResponse, DeviceUpdate
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
    mqtt_client = start_mqtt(message_queue)
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
        stmt = select(Telemetry).where(Telemetry.device_id == device_id).order_by(Telemetry.ts.desc()).limit(limit)
        rows = session.exec(stmt).all()
        rows.reverse()
        return [TelemetryOut(device_id=r.device_id, ts=r.ts, data=r.data) for r in rows]

@app.post("/api/insights")
def insights(req: InsightRequest):
    summary = generate_insight(req.device_id, req.horizon_minutes)
    return JSONResponse({"summary": summary})

@app.post("/api/commands", response_model=CommandResponse)
def post_command(cmd: CommandRequest):
    # Ensure device exists and get type for topic
    with get_session() as session:
        d = session.get(Device, cmd.device_id)
        if not d:
            raise HTTPException(status_code=404, detail="Device not found")
        command_id = f"cmd_{uuid.uuid4().hex[:10]}"
        # Save queued command
        rec = Command(command_id=command_id, device_id=cmd.device_id, command=cmd.command, params=cmd.params, status="queued")
        session.add(rec)
        session.commit()

    # Publish to MQTT command topic
    topic = f"{settings.mqtt_topic_base}/{d.type}/{d.id}/command"  # e.g., simulators/thermostat/thermostat-01/command
    payload = json.dumps({"command_id": command_id, "command": cmd.command, "params": cmd.params})

    if mqtt_client is None:
        raise HTTPException(status_code=500, detail="MQTT not initialized")

    try:
        info = mqtt_client.publish(topic, payload, qos=0, retain=False)
        info.wait_for_publish()
        with get_session() as session:
            row = session.exec(select(Command).where(Command.command_id == command_id)).first()
            if row:
                row.status = "sent"
                session.add(row)
                session.commit()
    except Exception:
        with get_session() as session:
            row = session.exec(select(Command).where(Command.command_id == command_id)).first()
            if row:
                row.status = "failed"
                session.add(row)
                session.commit()
        raise HTTPException(status_code=500, detail="Failed to publish command")

    return CommandResponse(status="queued", commandId=command_id)

@app.websocket("/ws/telemetry")
async def telemetry_ws(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await asyncio.sleep(30)
    except WebSocketDisconnect:
        await manager.disconnect(websocket)