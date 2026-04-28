from __future__ import annotations
import logging
from datetime import timedelta, timezone
from app.analytics.channel import compute_channel_daily, compute_channel_weekly
from app.analytics.content import compute_content_daily
from app.analytics.referral import compute_referral_daily, compute_referral_weekly
from app.analytics.segmentation import compute_segmentation_kpis, compute_user_profiles
from app.clients.mongo_client import MongoService
from app.clients.telegram_client import TelegramService
from app.config.settings import settings
from app.dashboard.generator import DASHBOARD_PATH, generate_dashboard
from app.reporting.formatter import build_daily_report, build_weekly_report
from app.utils.time import utc_now, week_bounds_utc

logger = logging.getLogger(__name__)

def _load_channel_stats(mongo):
    try:
        return mongo.source_db[settings.source_collections.channel_stats_overview].find_one(
            {"_type": "channel_stats_snapshot"}, {"_id": 0}
        )
    except Exception:
        logger.warning("Could not load channel_stats_overview", stack_info=True)
        return None


def _load_weekly_post_stats(mongo, week_start, week_end):
    """Load the post-stats doc whose period overlaps the given UTC week window.

    fetch_message_stats stores week_start_utc/week_end_utc using an Asia/KL week
    boundary (8 h offset from UTC). An overlap query handles that mismatch and
    prevents a stale doc from a different week being used on manual runs.
    """
    try:
        return mongo.source_db[settings.source_collections.channel_stats_overview].find_one(
            {
                "_type": "weekly_post_stats",
                "week_start_utc": {"$lt": week_end},
                "week_end_utc": {"$gte": week_start},
            },
            {"_id": 0},
            sort=[("recorded_at", -1)],
        )
    except Exception:
        logger.warning("Could not load weekly_post_stats", stack_info=True)
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
    generate_dashboard(mongo)
    await telegram.send_dashboard(DASHBOARD_PATH)
    logger.info("Daily pipeline completed")

async def run_weekly_pipeline(mongo, telegram):
    d = utc_now() - timedelta(days=1)
    week_start, week_end = week_bounds_utc(d.astimezone(timezone.utc))
    channel_stats = _load_channel_stats(mongo)
    weekly_post_stats = _load_weekly_post_stats(mongo, week_start, week_end)
    if channel_stats and weekly_post_stats:
        pc = weekly_post_stats.get("post_count") or 0
        if pc > 0:
            channel_stats = {
                **channel_stats,
                "mean_view_count": round(weekly_post_stats.get("views", 0) / pc),
                "mean_share_count": round(weekly_post_stats.get("shares", 0) / pc),
                "mean_reaction_count": round(weekly_post_stats.get("reactions", 0) / pc),
            }
    report = build_weekly_report(
        d, settings.tz,
        compute_referral_weekly(mongo, d),
        compute_channel_weekly(mongo, d),
        channel_stats=channel_stats,
    )
    await telegram.send_report(report)
    generate_dashboard(mongo)
    await telegram.send_dashboard(DASHBOARD_PATH)
    logger.info("Weekly pipeline completed")
