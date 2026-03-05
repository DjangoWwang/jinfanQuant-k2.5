from apscheduler.schedulers.asyncio import AsyncIOScheduler

scheduler = AsyncIOScheduler()


def init_scheduler():
    from app.tasks.daily_update import update_daily_nav, update_weekly_nav, update_index_data

    # Daily fund NAV update - every trading day at 20:00
    scheduler.add_job(update_daily_nav, "cron", hour=20, minute=0, id="daily_nav_update")

    # Weekly fund NAV update - every Saturday at 10:00
    scheduler.add_job(update_weekly_nav, "cron", day_of_week="sat", hour=10, minute=0, id="weekly_nav_update")

    # Index data update - every trading day at 20:30
    scheduler.add_job(update_index_data, "cron", hour=20, minute=30, id="index_data_update")

    scheduler.start()
