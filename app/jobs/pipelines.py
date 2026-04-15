from __future__ import annotations
import logging
from datetime import timedelta
from app.analytics.channel import compute_channel_daily
from app.analytics.content import compute_content_daily
from app.analytics.referral import compute_referral_daily, compute_referral_weekly
from app.analytics.segmentation import compute_segmentation_kpis, compute_user_profiles
from app.clients.mongo_client import MongoService
from app.clients.telegram_client import TelegramService
from app.config.settings import settings
from app.reporting.formatter import build_daily_report, build_weekly_report
from app.utils.time import utc_now

logger = logging.getLogger(__name__)

def _load_channel_stats(mongo):
    try:
        return mongo.source_db[settings.source_collections.channel_stats_overview].find_one(
            {"_type": "channel_stats_snapshot"}, {"_id": 0}
        )
    except Exception:
        logger.warning("Could not load channel_stats_overview", stack_info=True)
        return None

async def run_daily_pipeline(mongo, telegram):
    d = utc_now() - timedelta(days=1)
    report = build_daily_report(
        d, settings.tz,
        compute_referral_daily(mongo, d),
        compute_channel_daily(mongo, d),
        compute_content_daily(mongo, d),
        compute_user_profiles(mongo, d),
        compute_segmentation_kpis(mongo, d),
        channel_stats=_load_channel_stats(mongo),
    )
    await telegram.send_report(report)
    logger.info("Daily pipeline completed")

async def run_weekly_pipeline(mongo, telegram):
    d = utc_now() - timedelta(days=1)
    report = build_weekly_report(
        d, settings.tz,
        compute_referral_weekly(mongo, d),
        channel_stats=_load_channel_stats(mongo),
    )
    await telegram.send_report(report)
    logger.info("Weekly pipeline completed")
