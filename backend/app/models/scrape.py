from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime,
    Text, ForeignKey,
)
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.sql import func

from app.database import Base


class DataSource(Base):
    __tablename__ = "data_sources"

    id = Column(Integer, primary_key=True, autoincrement=True)
    source_name = Column(String(100), nullable=False)
    source_type = Column(String(50), nullable=True)
    config_json = Column(JSON, nullable=True)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class ScrapeJob(Base):
    __tablename__ = "scrape_jobs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_type = Column(String(50), nullable=False)
    data_source_id = Column(Integer, ForeignKey("data_sources.id"), nullable=True)
    status = Column(String(20), default="pending")
    started_at = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)
    records_added = Column(Integer, default=0)
    records_updated = Column(Integer, default=0)
    error_log = Column(Text, nullable=True)
    config_json = Column(JSON, nullable=True)
