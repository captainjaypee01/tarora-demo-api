from datetime import datetime
from pydantic import BaseModel
from typing import Any

class DeviceOut(BaseModel):
    id: str
    name: str
    type: str

class DeviceUpdate(BaseModel):
    name: str

class TelemetryOut(BaseModel):
    device_id: str
    ts: datetime
    data: dict[str, Any]

class InsightRequest(BaseModel):
    device_id: str | None = None
    horizon_minutes: int = 60

class CommandRequest(BaseModel):
    device_id: str
    command: str
    params: dict[str, Any] = {}

class CommandResponse(BaseModel):
    status: str
    commandId: str