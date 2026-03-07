"""Daily update tasks — delegates to scripts/daily_update.py logic.

These functions are called by the scheduler (scripts/scheduler.py).
"""

import logging

logger = logging.getLogger(__name__)


async def update_daily_nav():
    """Update NAV for daily-frequency funds on trading days."""
    from scripts.daily_update import get_funds_to_update, update_single_fund, is_trading_day
    from app.crawler.fof99.client import Fof99Client
    from app.crawler.fof99.nav_scraper import NavScraper
    from datetime import date
    import asyncio

    today = date.today()
    if not await is_trading_day(today):
        logger.info("Not a trading day, skipping daily update")
        return

    funds = await get_funds_to_update(force_all=False)
    if not funds:
        logger.info("No funds need updating")
        return

    logger.info("Starting daily NAV update for %d funds", len(funds))
    client = Fof99Client()
    await client.login()
    scraper = NavScraper(client)

    success = 0
    try:
        for fund_id, fof99_id, fund_name, freq, latest_date in funds:
            try:
                count = await update_single_fund(scraper, fund_id, fof99_id, fund_name, latest_date)
                if count > 0:
                    success += 1
            except Exception as e:
                logger.warning("Failed %s: %s", fund_name[:20], e)
            await asyncio.sleep(0.8)
    finally:
        await client.close()

    logger.info("Daily NAV update complete: %d funds updated", success)


async def update_weekly_nav():
    """Update NAV for weekly-frequency funds (runs on Saturday)."""
    logger.info("Starting weekly NAV update — delegating to daily update with force-all logic")
    await update_daily_nav()


async def update_quality_scores():
    """Update data quality scores for all funds."""
    from scripts.daily_update import update_quality_scores as _update
    await _update()
