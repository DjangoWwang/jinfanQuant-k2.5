from sqlalchemy import (
    Column, Integer, String, Boolean, Date, DateTime,
    Numeric, Text, ForeignKey, UniqueConstraint,
)
from sqlalchemy.sql import func

from app.database import Base


class Benchmark(Base):
    __tablename__ = "benchmarks"

    id = Column(Integer, primary_key=True, autoincrement=True)
    index_code = Column(String(50), unique=True, nullable=False)
    index_name = Column(String(200), nullable=False)
    category = Column(String(50), nullable=True)
    is_public = Column(Boolean, default=True)
    data_source = Column(String(20), nullable=True)
    is_active = Column(Boolean, default=True)


class IndexNav(Base):
    __tablename__ = "index_nav"
    __table_args__ = (
        UniqueConstraint("index_code", "nav_date", name="uq_index_nav_code_date"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    index_code = Column(String(50), nullable=False)
    nav_date = Column(Date, nullable=False)
    nav_value = Column(Numeric(14, 6), nullable=True)
    daily_return = Column(Numeric(10, 6), nullable=True)


class CompositeBenchmark(Base):
    __tablename__ = "composite_benchmarks"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(200), nullable=False)
    description = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class CompositeBenchmarkItem(Base):
    __tablename__ = "composite_benchmark_items"
    __table_args__ = (
        UniqueConstraint(
            "composite_id", "index_code",
            name="uq_composite_item_code",
        ),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    composite_id = Column(
        Integer, ForeignKey("composite_benchmarks.id"), nullable=False
    )
    index_code = Column(String(50), nullable=False)
    weight = Column(Numeric(8, 6), nullable=False)
