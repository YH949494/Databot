from __future__ import annotations

import asyncio
import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from app.clients.mongo_client import MongoService
from app.clients.telegram_client import TelegramService
from app.collectors.channel_collector import build_dispatcher
from app.config.settings import settings
from app.jobs.pipelines import run_daily_pipeline, run_weekly_pipeline

logger = logging.getLogger(__name__)


def _cron_trigger(expr: str) -> CronTrigger:
    minute, hour, day, month, day_of_week = expr.split()
    return CronTrigger(
        minute=minute, hour=hour, day=day, month=month, day_of_week=day_of_week, timezone="UTC"
    )


def start_scheduler(mongo: MongoService, telegram: TelegramService) -> AsyncIOScheduler:
    scheduler = AsyncIOScheduler(timezone="UTC")
    scheduler.add_job(
        run_daily_pipeline,
        _cron_trigger(settings.schedule_daily_cron),
        args=[mongo, telegram],
        max_instances=1,
    )
    scheduler.add_job(
        run_weekly_pipeline,
        _cron_trigger(settings.schedule_weekly_cron),
        args=[mongo, telegram],
        max_instances=1,
    )
    scheduler.start()
    logger.info("Scheduler started")
    return scheduler


async def run_forever() -> None:
    telegram: TelegramService | None = None
    try:
        logger.info("Initialising MongoDB connection and derived indexes…")
        mongo = MongoService()
        mongo.setup_derived_indexes()
        mongo.setup_source_indexes()
        logger.info("Initialising Telegram client…")
        telegram = TelegramService()
    except Exception:
        logger.critical(
            "Fatal error during service startup — scheduler will NOT start. "
            "Check MongoDB URI, credentials, and Telegram bot token.",
            exc_info=True,
        )
        raise

    try:
        if settings.scheduler_enabled:
            start_scheduler(mongo, telegram)
        else:
            logger.warning("Scheduler is disabled (SCHEDULER_ENABLED=false). Running in idle mode.")

        # Start the channel event collector via aiogram long-polling.
        # This runs concurrently with the scheduler — one asyncio event loop handles both.
        logger.info("Starting channel event collector (polling for join/leave/post events)…")
        dp = build_dispatcher(mongo)
        await dp.start_polling(
            telegram.bot,
            allowed_updates=["chat_member", "channel_post", "edited_channel_post"],
        )
    finally:
        if telegram is not None:
            await telegram.close()
            logger.info("Telegram session closed")
