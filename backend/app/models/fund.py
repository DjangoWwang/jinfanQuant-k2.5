from sqlalchemy import (
    Column, Integer, String, Boolean, Date, DateTime,
    Numeric, ForeignKey, UniqueConstraint, Index,
)
from sqlalchemy.sql import func

from app.database import Base


class Fund(Base):
    __tablename__ = "funds"

    id = Column(Integer, primary_key=True, autoincrement=True)
    fund_name = Column(String(200), nullable=False)
    filing_number = Column(String(50), unique=True, nullable=True)
    manager_name = Column(String(200), nullable=False)
    inception_date = Column(Date, nullable=True)
    strategy_type = Column(String(50), nullable=True)
    strategy_sub = Column(String(50), nullable=True)
    latest_nav = Column(Numeric(12, 4), nullable=True)
    latest_nav_date = Column(Date, nullable=True)
    nav_frequency = Column(String(10), default="daily")
    data_source = Column(String(20), default="fof99")
    is_private = Column(Boolean, default=True)
    fof99_fund_id = Column(String(50), nullable=True)
    status = Column(String(20), default="active")
    nav_status = Column(String(20), default="pending")  # pending|has_data|no_data|rate_limited|fetch_failed
    data_quality_score = Column(Integer, nullable=True)  # 0-100, 数据质量评分
    data_quality_tags = Column(String(500), nullable=True)  # 逗号分隔标签: interleaved,sparse,jump,...
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())


class NavHistory(Base):
    __tablename__ = "nav_history"
    __table_args__ = (
        UniqueConstraint("fund_id", "nav_date", name="uq_nav_history_fund_date"),
        Index("ix_nav_history_fund_date", "fund_id", "nav_date"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    fund_id = Column(Integer, ForeignKey("funds.id"), nullable=False)
    nav_date = Column(Date, nullable=False)
    unit_nav = Column(Numeric(12, 6), nullable=True)
    cumulative_nav = Column(Numeric(12, 6), nullable=True)
    adjusted_nav = Column(Numeric(12, 6), nullable=True)
    daily_return = Column(Numeric(10, 6), nullable=True)
    data_source = Column(String(20), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
