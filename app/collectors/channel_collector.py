from __future__ import annotations

"""
channel_collector.py

Listens for live Telegram events and writes to MongoDB source collections.

Collections written (source DB — referral_bot):
  channel_events  — one doc per join/leave event
  post_logs       — one doc per channel post, enriched with:
                    * content_type  (voucher/pool/leaderboard/announcement/event/media/text)
                    * media_type    (photo/video/document/poll/gif/text/…)
                    * reactions     (live count via message_reaction_count updates)
                    * reaction_breakdown  ({"👍": 3, "🔥": 7, …})
                    * shares        (forward_count, refreshed by periodic job)
                    * voucher_code  (extracted from text if present)
                    * drop_id       (extracted from text if present)

Nothing here writes to derived collections.
"""

import logging
import re
from datetime import datetime, timezone

from aiogram import Dispatcher
from aiogram.filters import ChatMemberUpdatedFilter, IS_MEMBER, IS_NOT_MEMBER
from aiogram.types import (
    ChatMemberUpdated,
    Message,
    MessageReactionCountUpdated,
)

from app.clients.mongo_client import MongoService
from app.config.settings import settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Content type detection
# ---------------------------------------------------------------------------

_CONTENT_TYPE_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(r'\b(voucher|redeem|claim|promo|code)\b',    re.I), "voucher"),
    (re.compile(r'\b(pool|drop|airdrop|reward\s*pool)\b',   re.I), "pool"),
    (re.compile(r'\b(leaderboard|top\s*\d|ranking|winner)\b', re.I), "leaderboard"),
    (re.compile(r'\b(welcome|join\s*us|new\s*member)\b',    re.I), "welcome"),
    (re.compile(r'\b(event|contest|giveaway|challenge)\b',  re.I), "event"),
    (re.compile(r'\b(announce|update|notice|important)\b',  re.I), "announcement"),
]

_VOUCHER_CODE_RE = re.compile(r'\b([A-Z0-9]{5,8})\b')
_DROP_ID_RE      = re.compile(r'\b([a-f0-9]{24})\b')
_STOP_WORDS      = {"UTC", "VIP", "NEW", "TOP", "ALL", "XP", "API", "URL"}


def _tag_content_type(text: str, has_media: bool) -> str:
    for pattern, tag in _CONTENT_TYPE_PATTERNS:
        if pattern.search(text):
            return tag
    return "media" if has_media else "text"


def _extract_voucher_code(text: str) -> str | None:
    for m in _VOUCHER_CODE_RE.findall(text):
        if m not in _STOP_WORDS:
            return m
    return None


def _extract_drop_id(text: str) -> str | None:
    m = _DROP_ID_RE.search(text)
    return m.group(1) if m else None


def _media_type(message: Message) -> str:
    if message.photo:       return "photo"
    if message.video:       return "video"
    if message.document:    return "document"
    if message.poll:        return "poll"
    if message.animation:   return "gif"
    if message.sticker:     return "sticker"
    if message.voice:       return "voice"
    if message.video_note:  return "video_note"
    return "text"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _channel_id() -> int | str:
    try:
        return int(settings.tg_channel_id)
    except (ValueError, TypeError):
        return settings.tg_channel_id


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

def build_dispatcher(mongo: MongoService) -> Dispatcher:
    dp         = Dispatcher()
    channel_id = _channel_id()

    def post_col():
        return mongo.source_db[settings.source_collections.post_logs]

    def event_col():
        return mongo.source_db[settings.source_collections.channel_events]

    # --- Join / Leave ---

    @dp.chat_member(ChatMemberUpdatedFilter(IS_NOT_MEMBER >> IS_MEMBER))
    async def on_member_join(event: ChatMemberUpdated) -> None:
        if event.chat.id != channel_id:
            return
        event_col().insert_one({
            "event_type":  "join",
            "event_time":  event.date or _utcnow(),
            "user_id":     event.new_chat_member.user.id,
            "username":    event.new_chat_member.user.username,
            "chat_id":     event.chat.id,
            "is_referred": False,
            "recorded_at": _utcnow(),
        })
        logger.debug("channel_event join user_id=%s", event.new_chat_member.user.id)

    @dp.chat_member(ChatMemberUpdatedFilter(IS_MEMBER >> IS_NOT_MEMBER))
    async def on_member_leave(event: ChatMemberUpdated) -> None:
        if event.chat.id != channel_id:
            return
        event_col().insert_one({
            "event_type":  "leave",
            "event_time":  event.date or _utcnow(),
            "user_id":     event.new_chat_member.user.id,
            "username":    event.new_chat_member.user.username,
            "chat_id":     event.chat.id,
            "recorded_at": _utcnow(),
        })
        logger.debug("channel_event leave user_id=%s", event.new_chat_member.user.id)

    # --- New post ---

    @dp.channel_post()
    async def on_channel_post(message: Message) -> None:
        if message.chat.id != channel_id:
            return
        text      = message.text or message.caption or ""
        has_media = bool(message.photo or message.video or message.document or message.animation)

        doc = {
            "post_id":       message.message_id,
            "post_time":     message.date or _utcnow(),
            "chat_id":       message.chat.id,
            "text":          text[:1000],
            # Content classification
            "content_type":  _tag_content_type(text, has_media),
            "media_type":    _media_type(message),
            "voucher_code":  _extract_voucher_code(text),
            "drop_id":       _extract_drop_id(text),
            # Engagement — starts at 0, updated by events + periodic refresh
            "views":              0,
            "reactions":          0,
            "reaction_breakdown": {},
            "shares":             0,
            "comments":           0,
            # Attribution (filled by analytics pipeline)
            "claims_1h":    0,
            "claims_6h":    0,
            "claims_24h":   0,
            "referred_joins_after_post": 0,
            "recorded_at":  _utcnow(),
        }
        post_col().update_one(
            {"post_id": doc["post_id"], "chat_id": doc["chat_id"]},
            {"$setOnInsert": doc},
            upsert=True,
        )
        logger.info(
            "post_log post_id=%s content_type=%s media_type=%s voucher_code=%s",
            doc["post_id"], doc["content_type"], doc["media_type"], doc["voucher_code"],
        )

    # --- Edited post ---

    @dp.edited_channel_post()
    async def on_channel_post_edited(message: Message) -> None:
        if message.chat.id != channel_id:
            return
        text      = message.text or message.caption or ""
        has_media = bool(message.photo or message.video or message.document or message.animation)
        post_col().update_one(
            {"post_id": message.message_id, "chat_id": message.chat.id},
            {"$set": {
                "text":         text[:1000],
                "content_type": _tag_content_type(text, has_media),
                "media_type":   _media_type(message),
                "voucher_code": _extract_voucher_code(text),
                "drop_id":      _extract_drop_id(text),
                "updated_at":   _utcnow(),
            }},
        )

    # --- Reaction count updates ---

    @dp.message_reaction_count()
    async def on_reaction_count(update: MessageReactionCountUpdated) -> None:
        if update.chat.id != channel_id:
            return
        total     = sum(r.total_count for r in (update.reactions or []))
        breakdown = {
            r.type.emoji: r.total_count
            for r in (update.reactions or [])
            if hasattr(r.type, "emoji")
        }
        post_col().update_one(
            {"post_id": update.message_id, "chat_id": update.chat.id},
            {"$set": {
                "reactions":            total,
                "reaction_breakdown":   breakdown,
                "reactions_updated_at": _utcnow(),
            }},
        )
        logger.debug("reactions updated post_id=%s total=%d", update.message_id, total)

    return dp
