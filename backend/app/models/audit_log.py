"""Audit log model for tracking user operations."""

from sqlalchemy import Column, Integer, String, DateTime, Text, func

from app.database import Base


class AuditLog(Base):
    __tablename__ = "audit_logs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, nullable=False, index=True)
    username = Column(String(50), nullable=False)
    action = Column(String(50), nullable=False, index=True)  # e.g. user.create, user.disable, user.role_change
    target_type = Column(String(50), nullable=True)  # e.g. user, fund, product
    target_id = Column(Integer, nullable=True)
    detail = Column(Text, nullable=True)  # JSON or human-readable detail
    ip_address = Column(String(45), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, index=True)
