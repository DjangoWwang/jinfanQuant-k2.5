"""Risk monitoring rules and alert event models."""

from __future__ import annotations

from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime,
    Numeric, ForeignKey, Index, Text,
)
from sqlalchemy.sql import func

from app.database import Base


class RiskRule(Base):
    __tablename__ = "risk_rules"
    __table_args__ = (
        Index("ix_risk_rules_type_active", "rule_type", "is_active"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(200), nullable=False)
    rule_type = Column(
        String(30), nullable=False,
    )  # drawdown | volatility | concentration | liquidity | nav_anomaly
    target_type = Column(
        String(20), nullable=False,
    )  # fund | product | portfolio
    target_id = Column(Integer, nullable=True)  # NULL = apply to all targets of that type
    threshold = Column(Numeric(12, 6), nullable=False)
    comparison = Column(String(5), nullable=False)  # gt | lt | gte | lte
    severity = Column(String(10), nullable=False, default="warning")  # warning | critical
    is_active = Column(Boolean, default=True)
    created_by = Column(Integer, ForeignKey("users.id"), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class AlertEvent(Base):
    __tablename__ = "alert_events"
    __table_args__ = (
        Index("ix_alert_events_target", "target_type", "target_id", "created_at"),
        Index("ix_alert_events_read_severity", "is_read", "severity"),
        Index(
            "ix_alert_events_dedup", "rule_id", "target_id",
            unique=True, postgresql_where=Column("resolved_at").is_(None),
        ),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    rule_id = Column(Integer, ForeignKey("risk_rules.id", ondelete="CASCADE"), nullable=False)
    target_type = Column(String(20), nullable=False)
    target_id = Column(Integer, nullable=False)
    target_name = Column(String(200), nullable=True)
    metric_value = Column(Numeric(12, 6), nullable=True)
    threshold_value = Column(Numeric(12, 6), nullable=True)
    severity = Column(String(10), nullable=False, default="warning")
    message = Column(Text, nullable=True)
    is_read = Column(Boolean, default=False)
    resolved_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
