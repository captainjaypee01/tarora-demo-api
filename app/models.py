from typing import Optional
from datetime import datetime
from sqlmodel import SQLModel, Field, Column, JSON

class Device(SQLModel, table=True):
    id: str = Field(primary_key=True, index=True)
    name: str
    type: str
    created_at: datetime = Field(default_factory=datetime.utcnow)

class Telemetry(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    device_id: str = Field(index=True, foreign_key="device.id")
    ts: datetime = Field(index=True)
    data: dict = Field(sa_column=Column(JSON))

class Command(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    command_id: str = Field(index=True)
    device_id: str = Field(index=True)
    command: str
    params: dict = Field(sa_column=Column(JSON))
    queued_at: datetime = Field(default_factory=datetime.utcnow)
    status: str = Field(default="queued")  # queued|sent|failed