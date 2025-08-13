from sqlmodel import SQLModel, create_engine, Session
from .settings import settings

engine = create_engine(settings.database_url, pool_pre_ping=True)

def init_db():
    SQLModel.metadata.create_all(engine)

def get_session():
    # 👇 prevent attribute expiration so simple reads after commit are safe
    return Session(engine, expire_on_commit=False)
