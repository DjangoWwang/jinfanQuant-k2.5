import logging

logger = logging.getLogger(__name__)


async def update_daily_nav():
    """Update NAV for daily-frequency funds on trading days."""
    logger.info("Starting daily NAV update...")
    # TODO: Implement after crawler module is complete
    # 1. Check if today is a trading day
    # 2. Get all daily-frequency funds
    # 3. For each fund, fetch latest NAV from data source
    # 4. Insert new records into nav_history
    logger.info("Daily NAV update complete.")


async def update_weekly_nav():
    """Update NAV for weekly-frequency funds (runs on Saturday)."""
    logger.info("Starting weekly NAV update...")
    # TODO: Implement after crawler module is complete
    logger.info("Weekly NAV update complete.")


async def update_index_data():
    """Update index/benchmark data on trading days."""
    logger.info("Starting index data update...")
    # TODO: Implement after crawler module is complete
    logger.info("Index data update complete.")
