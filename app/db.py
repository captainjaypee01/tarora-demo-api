from sqlmodel import SQLModel, create_engine, Session
from .settings import settings

engine = create_engine(settings.database_url, pool_pre_ping=True)

def init_db():
    SQLModel.metadata.create_all(engine)

def get_session():
    return Session(engine)