from pydantic import BaseModel
import os

class Settings(BaseModel):
    database_url: str = os.getenv("DATABASE_URL", "postgresql://tarora:tarora_pw@db:5432/tarora_demo")
    cors_origins: list[str] = os.getenv("CORS_ORIGINS", "http://localhost:4173,http://localhost:5173").split(",")

    mqtt_host: str = os.getenv("MQTT_HOST", "mqtt")
    mqtt_port: int = int(os.getenv("MQTT_PORT", "1883"))
    mqtt_username: str | None = os.getenv("MQTT_USERNAME") or None
    mqtt_password: str | None = os.getenv("MQTT_PASSWORD") or None
    mqtt_topic_base: str = os.getenv("MQTT_TOPIC_BASE", "simulators")

    openai_api_key: str | None = os.getenv("OPENAI_API_KEY") or None
    openai_model: str = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

settings = Settings()