"""填充A股交易日历 (2016-2027)。

用法: cd backend && python scripts/fill_calendar.py
"""

import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv

load_dotenv()

from app.database import async_session
from app.engine.calendar import fill_trading_calendar


async def main():
    print("正在填充A股交易日历 (2016-2026)...")
    async with async_session() as session:
        count = await fill_trading_calendar(session, start_year=2016, end_year=2026)
        await session.commit()
        print(f"完成: 插入 {count} 行 (含交易日和非交易日)")

        # 验证
        from sqlalchemy import select, func
        from app.models.calendar import TradingCalendar

        result = await session.execute(
            select(func.count()).where(TradingCalendar.is_trading_day.is_(True))
        )
        trading_days = result.scalar()
        print(f"其中交易日: {trading_days} 天")


if __name__ == "__main__":
    asyncio.run(main())
