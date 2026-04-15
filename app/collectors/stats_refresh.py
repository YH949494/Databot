from __future__ import annotations

import logging
from datetime import datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

from aiogram.exceptions import TelegramAPIError

from app.config.settings import settings

logger = logging.getLogger(__name__)


def _utcnow():
    return datetime.now(timezone.utc)


def _channel_id():
    try:
        return int(settings.tg_channel_id)
    except Exception:
        return settings.tg_channel_id


def _week_window_kl(reference_utc: datetime | None = None):
    now_utc = reference_utc or _utcnow()
    kl = ZoneInfo("Asia/Kuala_Lumpur")
    now_local = now_utc.astimezone(kl)
    week_start_local = datetime.combine(
        (now_local - timedelta(days=now_local.weekday())).date(),
        time(0, 0, 0),
        tzinfo=kl,
    )
    week_end_local = datetime.combine(
        week_start_local.date() + timedelta(days=6),
        time(23, 59, 59, 999999),
        tzinfo=kl,
    )
    return week_start_local, week_end_local, week_start_local.astimezone(timezone.utc), week_end_local.astimezone(timezone.utc)


async def fetch_channel_stats(bot, mongo):
    channel_id = _channel_id()
    try:
        chat = await bot.get_chat(chat_id=channel_id)
        member_count = await bot.get_chat_member_count(chat_id=channel_id)
    except TelegramAPIError as e:
        logger.warning("channel stats refresh failed chat_id=%s error=%s", channel_id, e)
        return None

    admin_count = None
    try:
        admins = await bot.get_chat_administrators(chat_id=channel_id)
        admin_count = len(admins or [])
    except TelegramAPIError as e:
        logger.debug("get_chat_administrators skipped chat_id=%s error=%s", channel_id, e)

    doc = {
        "_type": "channel_stats_snapshot",
        "recorded_at": _utcnow(),
        "chat_id": channel_id,
        "chat_type": getattr(chat, "type", None),
        "title": getattr(chat, "title", None),
        "username": getattr(chat, "username", None),
        "member_count": member_count,
        "administrator_count": admin_count,
    }
    mongo.source_db[settings.source_collections.channel_stats_overview].update_one(
        {"_type": "channel_stats_snapshot", "chat_id": channel_id},
        {"$set": doc},
        upsert=True,
    )
    logger.info(
        "fetch_channel_stats: chat_id=%s members=%s admins=%s",
        channel_id,
        member_count,
        admin_count,
    )
    return doc


async def fetch_message_stats(bot, mongo):
    channel_id = _channel_id()
    week_start_local, week_end_local, week_start_utc, week_end_utc = _week_window_kl()
    post_col = mongo.source_db[settings.source_collections.post_logs]
    posts = list(
        post_col.find(
            {
                "chat_id": channel_id,
                "post_time": {"$gte": week_start_utc, "$lte": week_end_utc},
            },
            {"post_id": 1, "views": 1, "shares": 1, "reactions": 1, "post_time": 1},
        )
    )
    if not posts:
        logger.info(
            "fetch_message_stats: no posts for week chat_id=%s week_start=%s week_end=%s",
            channel_id,
            week_start_local.isoformat(),
            week_end_local.isoformat(),
        )
        return None

    totals = {
        "post_count": len(posts),
        "views": sum(int(p.get("views", 0) or 0) for p in posts),
        "shares": sum(int(p.get("shares", 0) or 0) for p in posts),
        "reactions": sum(int(p.get("reactions", 0) or 0) for p in posts),
    }
    doc = {
        "_type": "weekly_post_stats",
        "chat_id": channel_id,
        "timezone": "Asia/Kuala_Lumpur",
        "week_start_local": week_start_local,
        "week_end_local": week_end_local,
        "week_start_utc": week_start_utc,
        "week_end_utc": week_end_utc,
        "recorded_at": _utcnow(),
        **totals,
    }
    mongo.source_db[settings.source_collections.channel_stats_overview].update_one(
        {
            "_type": "weekly_post_stats",
            "chat_id": channel_id,
            "week_start_local": week_start_local,
            "week_end_local": week_end_local,
        },
        {"$set": doc},
        upsert=True,
    )
    logger.info(
        "fetch_message_stats: aggregated chat_id=%s week=%s..%s posts=%s views=%s shares=%s reactions=%s",
        channel_id,
        week_start_local.isoformat(),
        week_end_local.isoformat(),
        totals["post_count"],
        totals["views"],
        totals["shares"],
        totals["reactions"],
    )
    return doc


async def fetch_subscriber_count(bot, mongo):
    try:
        count = await bot.get_chat_member_count(chat_id=_channel_id())
        mongo.source_db[settings.source_collections.channel_events].update_one(
            {"_type": "subscriber_snapshot"},
            {"$set": {"_type": "subscriber_snapshot", "count": count, "recorded_at": _utcnow()}},
            upsert=True,
        )
        return count
    except TelegramAPIError as e:
        logger.warning("subscriber_count failed: %s", e)


async def refresh_post_stats(mongo, bot):
    await fetch_subscriber_count(bot, mongo)
    await fetch_channel_stats(bot, mongo)
    await fetch_message_stats(bot, mongo)
