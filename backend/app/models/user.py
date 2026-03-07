from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime,
)
from sqlalchemy.sql import func

from app.database import Base


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(50), unique=True, nullable=False, index=True)
    email = Column(String(200), unique=True, nullable=True)
    hashed_password = Column(String(255), nullable=False)
    display_name = Column(String(100), nullable=True)
    role = Column(String(20), nullable=False, default="viewer")  # admin | analyst | viewer
    is_active = Column(Boolean, default=True)
    last_login_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
