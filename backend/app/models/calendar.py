from sqlalchemy import Column, String, Boolean, Date

from app.database import Base


class TradingCalendar(Base):
    __tablename__ = "trading_calendar"

    cal_date = Column(Date, primary_key=True)
    is_trading_day = Column(Boolean, nullable=False)
    market = Column(String(10), default="CN")
