from sqlalchemy import (
    Column, Integer, String, Boolean, Date, DateTime,
    Numeric, Text, ForeignKey, UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.sql import func

from app.database import Base


class Portfolio(Base):
    __tablename__ = "portfolios"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(200), nullable=False)
    description = Column(Text, nullable=True)
    portfolio_type = Column(String(20), default="simulation")
    allocation_model = Column(String(30), nullable=True)
    rebalance_freq = Column(String(20), default="monthly")
    start_date = Column(Date, nullable=True)
    end_date = Column(Date, nullable=True)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())


class PortfolioAllocation(Base):
    __tablename__ = "portfolio_allocations"
    __table_args__ = (
        UniqueConstraint(
            "portfolio_id", "fund_id", "effective_date",
            name="uq_portfolio_alloc_fund_date",
        ),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    portfolio_id = Column(
        Integer, ForeignKey("portfolios.id", ondelete="CASCADE"), nullable=False
    )
    fund_id = Column(Integer, ForeignKey("funds.id"), nullable=False)
    target_weight = Column(Numeric(8, 6), nullable=False)
    effective_date = Column(Date, nullable=False)


class BacktestResult(Base):
    __tablename__ = "backtest_results"

    id = Column(Integer, primary_key=True, autoincrement=True)
    portfolio_id = Column(
        Integer, ForeignKey("portfolios.id", ondelete="CASCADE"), nullable=True
    )
    run_date = Column(DateTime(timezone=True), server_default=func.now())
    config_json = Column(JSON, nullable=True)
    metrics_json = Column(JSON, nullable=True)
    nav_series_json = Column(JSON, nullable=True)
    status = Column(String(20), default="completed")
