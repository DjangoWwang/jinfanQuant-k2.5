from sqlalchemy import (
    Column, Integer, String, Boolean, Date, DateTime, Text,
    Numeric, SmallInteger, ForeignKey, UniqueConstraint,
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.database import Base


class Product(Base):
    __tablename__ = "products"

    id = Column(Integer, primary_key=True, autoincrement=True)
    product_name = Column(String(200), nullable=False)
    product_code = Column(String(50), nullable=True)
    custodian = Column(String(100), nullable=True)
    administrator = Column(String(100), nullable=True)
    product_type = Column(String(20), default="live")  # live / simulation
    inception_date = Column(Date, nullable=True)
    total_shares = Column(Numeric(16, 4), nullable=True)
    management_fee_rate = Column(Numeric(6, 4), default=0)
    performance_fee_rate = Column(Numeric(6, 4), default=0)
    high_watermark = Column(Numeric(12, 6), nullable=True)
    linked_portfolio_id = Column(Integer, ForeignKey("portfolios.id"), nullable=True)
    benchmark_id = Column(Integer, ForeignKey("composite_benchmarks.id"), nullable=True)
    notes = Column(Text, nullable=True)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    snapshots = relationship("ValuationSnapshot", back_populates="product", lazy="selectin")


class ValuationSnapshot(Base):
    __tablename__ = "valuation_snapshots"
    __table_args__ = (
        UniqueConstraint(
            "product_id", "valuation_date",
            name="uq_valuation_snapshot_product_date",
        ),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    product_id = Column(Integer, ForeignKey("products.id"), nullable=False)
    valuation_date = Column(Date, nullable=False)
    total_nav = Column(Numeric(16, 2), nullable=True)
    unit_nav = Column(Numeric(12, 6), nullable=True)
    cumulative_nav = Column(Numeric(12, 6), nullable=True)
    total_shares = Column(Numeric(16, 4), nullable=True)
    source_file = Column(String(500), nullable=True)
    imported_at = Column(DateTime(timezone=True), server_default=func.now())

    product = relationship("Product", back_populates="snapshots")
    items = relationship("ValuationItem", back_populates="snapshot", lazy="selectin", cascade="all, delete-orphan")


class ValuationItem(Base):
    __tablename__ = "valuation_items"

    id = Column(Integer, primary_key=True, autoincrement=True)
    snapshot_id = Column(
        Integer, ForeignKey("valuation_snapshots.id", ondelete="CASCADE"),
        nullable=False,
    )
    item_code = Column(String(30), nullable=True)
    item_name = Column(String(200), nullable=True)
    level = Column(SmallInteger, nullable=True)
    parent_code = Column(String(30), nullable=True)
    quantity = Column(Numeric(16, 4), nullable=True)
    unit_cost = Column(Numeric(12, 6), nullable=True)
    cost_amount = Column(Numeric(16, 2), nullable=True)
    cost_pct_nav = Column(Numeric(8, 4), nullable=True)
    market_price = Column(Numeric(12, 6), nullable=True)
    market_value = Column(Numeric(16, 2), nullable=True)
    value_pct_nav = Column(Numeric(8, 4), nullable=True)
    value_diff = Column(Numeric(16, 2), nullable=True)
    linked_fund_id = Column(Integer, ForeignKey("funds.id"), nullable=True)
    remark = Column(String(200), nullable=True)

    snapshot = relationship("ValuationSnapshot", back_populates="items")
