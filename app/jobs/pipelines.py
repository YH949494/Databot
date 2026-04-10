from __future__ import annotations

import logging
from datetime import timedelta

from app.analytics.channel import compute_channel_daily
from app.analytics.content import compute_content_daily
from app.analytics.referral import compute_referral_daily, compute_referral_weekly
from app.clients.mongo_client import MongoService
from app.clients.telegram_client import TelegramService
from app.config.settings import settings
from app.reporting.formatter import build_daily_report, build_weekly_report
from app.utils.time import utc_now

logger = logging.getLogger(__name__)


async def run_daily_pipeline(mongo: MongoService, telegram: TelegramService) -> None:
    target_date = utc_now() - timedelta(days=1)
    referral = compute_referral_daily(mongo, target_date)
    channel = compute_channel_daily(mongo, target_date)
    content = compute_content_daily(mongo, target_date)

    report = build_daily_report(target_date, settings.tz, referral, channel, content)
    await telegram.send_report(report)
    logger.info("Daily pipeline completed")


async def run_weekly_pipeline(mongo: MongoService, telegram: TelegramService) -> None:
    target_date = utc_now() - timedelta(days=1)
    weekly_referral = compute_referral_weekly(mongo, target_date)
    report = build_weekly_report(target_date, settings.tz, weekly_referral)
    await telegram.send_report(report)
    logger.info("Weekly pipeline completed")
