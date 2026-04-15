from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from aiogram import Bot
from aiogram.exceptions import TelegramAPIError

from app.clients.mongo_client import MongoService
from app.config.settings import settings

logger = logging.getLogger(__name__)
_REFRESH_WINDOW_DAYS = 7

def _utcnow(): return datetime.now(timezone.utc)
def _channel_id():
    try: return int(settings.tg_channel_id)
    except: return settings.tg_channel_id

async def fetch_channel_stats(bot, mongo):
    try:
        stats = await bot.get_chat_statistics(chat_id=_channel_id())
    except TelegramAPIError as e:
        logger.warning("getChatStatistics failed: %s", e); return None
    doc = {
        "recorded_at": _utcnow(),
        "period_start": getattr(stats, "period", {}).get("min_date") if hasattr(stats, "period") and stats.period else None,
        "period_end": getattr(stats, "period", {}).get("max_date") if hasattr(stats, "period") and stats.period else None,
        "member_count": _cval(getattr(stats, "member_count", None)),
        "member_count_delta": _cdelta(getattr(stats, "member_count", None)),
        "mean_view_count": _cval(getattr(stats, "mean_view_count", None)),
        "mean_view_count_delta": _cdelta(getattr(stats, "mean_view_count", None)),
        "mean_share_count": _cval(getattr(stats, "mean_share_count", None)),
        "mean_share_count_delta": _cdelta(getattr(stats, "mean_share_count", None)),
        "mean_reaction_count": _cval(getattr(stats, "mean_reaction_count", None)),
        "mean_reaction_count_delta": _cdelta(getattr(stats, "mean_reaction_count", None)),
        "enabled_notifications_percent": _cval(getattr(stats, "enabled_notifications_percentage", None)),
    }
    mongo.source_db[settings.source_collections.channel_stats_overview].update_one(
        {"_type": "channel_stats_snapshot"}, {"$set": {**doc, "_type": "channel_stats_snapshot"}}, upsert=True
    )
    logger.info("fetch_channel_stats: members=%s", doc["member_count"])
    return doc

async def fetch_message_stats(bot, mongo):
    channel_id = _channel_id()
    cutoff = _utcnow() - timedelta(days=_REFRESH_WINDOW_DAYS)
    post_col = mongo.source_db[settings.source_collections.post_logs]
    posts = list(post_col.find({"chat_id": channel_id, "post_time": {"$gte": cutoff}}, {"post_id": 1}))
    if not posts: return
    logger.info("fetch_message_stats: refreshing %d posts", len(posts))
    refreshed = 0
    for post in posts:
        pid = post["post_id"]
        try: ms = await bot.get_message_statistics(chat_id=channel_id, message_id=pid)
        except TelegramAPIError as e: logger.debug("skip %s: %s", pid, e); continue
        v = getattr(ms, "views", 0) or 0
        ps = getattr(ms, "public_shares", 0) or 0
        prs = getattr(ms, "private_shares", 0) or 0
        rt, rb = _extract_reactions(ms)
        post_col.update_one(
            {"post_id": pid, "chat_id": channel_id},
            {"$set": {"views": v, "public_shares": ps, "private_shares": prs,
                      "shares": ps + prs, "reactions": rt, "reaction_breakdown": rb,
                      "stats_refreshed_at": _utcnow()}}
        )
        refreshed += 1
    logger.info("fetch_message_stats: refreshed %d/%d", refreshed, len(posts))

async def fetch_subscriber_count(bot, mongo):
    try:
        count = await bot.get_chat_member_count(chat_id=_channel_id())
        mongo.source_db[settings.source_collections.channel_events].update_one(
            {"_type": "subscriber_snapshot"},
            {"$set": {"_type": "subscriber_snapshot", "count": count, "recorded_at": _utcnow()}},
            upsert=True
        )
        return count
    except TelegramAPIError as e: logger.warning("subscriber_count failed: %s", e)

async def refresh_post_stats(mongo, bot):
    await fetch_subscriber_count(bot, mongo)
    await fetch_channel_stats(bot, mongo)
    await fetch_message_stats(bot, mongo)

def _cval(c):
    if c is None: return None
    if isinstance(c, (int, float)): return float(c)
    v = getattr(c, "value", None); return float(v) if v is not None else None

def _cdelta(c):
    if c is None: return None
    p = getattr(c, "previous_value", None)
    if p is not None:
        cur = getattr(c, "value", None)
        if cur is not None: return float(cur) - float(p)
    return None

def _extract_reactions(ms):
    o = getattr(ms, "reactions", None)
    if o is None: return 0, {}
    if isinstance(o, (int, float)): return int(o), {}
    if isinstance(o, list):
        b, t = {}, 0
        for r in o:
            e = getattr(getattr(r, "type", None), "emoji", None) or str(r)
            b[e] = int(getattr(r, "total_count", 0) or 0); t += b[e]
        return t, b
    return 0, {}
