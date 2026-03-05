from sqlalchemy import (
    Column, Integer, String, DateTime, Text,
    ForeignKey, UniqueConstraint,
)
from sqlalchemy.sql import func

from app.database import Base


class FundPool(Base):
    __tablename__ = "fund_pools"
    __table_args__ = (
        UniqueConstraint("pool_type", "fund_id", name="uq_fund_pool_type_fund"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    pool_type = Column(String(20), nullable=False)
    fund_id = Column(Integer, ForeignKey("funds.id"), nullable=False)
    added_by = Column(String(50), default="system")
    notes = Column(Text, nullable=True)
    added_at = Column(DateTime(timezone=True), server_default=func.now())
