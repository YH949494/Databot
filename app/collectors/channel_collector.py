from __future__ import annotations

"""
channel_collector.py

Listens for live Telegram events on the configured channel and writes them
to MongoDB source collections so the analytics pipeline has data to read.

Collections written (source DB — referral_bot):
  channel_events  — one doc per join/leave event
  post_logs       — one doc per message posted to the channel

These are the ONLY writes this module performs. It never touches derived collections.
"""

import logging
from datetime import datetime, timezone

from aiogram import Bot, Dispatcher
from aiogram.filters import ChatMemberUpdatedFilter, IS_MEMBER, IS_NOT_MEMBER
from aiogram.types import ChatMemberUpdated, Message

from app.clients.mongo_client import MongoService
from app.config.settings import settings

logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _channel_id() -> int | str:
    """Return channel ID as int if possible (aiogram compares int chat ids)."""
    try:
        return int(settings.tg_channel_id)
    except (ValueError, TypeError):
        return settings.tg_channel_id


def build_dispatcher(mongo: MongoService) -> Dispatcher:
    """Build and return a configured aiogram Dispatcher with all event handlers."""
    dp = Dispatcher()
    channel_id = _channel_id()

    # ------------------------------------------------------------------
    # Channel member join / leave
    # ------------------------------------------------------------------

    @dp.chat_member(ChatMemberUpdatedFilter(IS_NOT_MEMBER >> IS_MEMBER))
    async def on_member_join(event: ChatMemberUpdated) -> None:
        if event.chat.id != channel_id:
            return
        doc = {
            "event_type": "join",
            "event_time": event.date or _utcnow(),
            "user_id": event.new_chat_member.user.id,
            "username": event.new_chat_member.user.username,
            "chat_id": event.chat.id,
            "is_referred": False,   # referral linkage not available here; enrichable later
            "recorded_at": _utcnow(),
        }
        mongo.source_db[settings.source_collections.channel_events].insert_one(doc)
        logger.debug("channel_event join user_id=%s", doc["user_id"])

    @dp.chat_member(ChatMemberUpdatedFilter(IS_MEMBER >> IS_NOT_MEMBER))
    async def on_member_leave(event: ChatMemberUpdated) -> None:
        if event.chat.id != channel_id:
            return
        doc = {
            "event_type": "leave",
            "event_time": event.date or _utcnow(),
            "user_id": event.new_chat_member.user.id,
            "username": event.new_chat_member.user.username,
            "chat_id": event.chat.id,
            "recorded_at": _utcnow(),
        }
        mongo.source_db[settings.source_collections.channel_events].insert_one(doc)
        logger.debug("channel_event leave user_id=%s", doc["user_id"])

    # ------------------------------------------------------------------
    # Channel posts
    # ------------------------------------------------------------------

    @dp.channel_post()
    async def on_channel_post(message: Message) -> None:
        if message.chat.id != channel_id:
            return
        doc = {
            "post_id": message.message_id,
            "post_time": message.date or _utcnow(),
            "chat_id": message.chat.id,
            "text": (message.text or message.caption or "")[:500],  # truncate for storage
            "post_type": _detect_post_type(message),
            "views": 0,
            "reactions": 0,
            "comments": 0,
            "claims_1h": 0,
            "claims_6h": 0,
            "claims_24h": 0,
            "referred_joins_after_post": 0,
            "qualified_after_post": 0,
            "recorded_at": _utcnow(),
        }
        mongo.source_db[settings.source_collections.post_logs].update_one(
            {"post_id": doc["post_id"], "chat_id": doc["chat_id"]},
            {"$setOnInsert": doc},
            upsert=True,
        )
        logger.debug("post_log recorded post_id=%s type=%s", doc["post_id"], doc["post_type"])

    @dp.edited_channel_post()
    async def on_channel_post_edited(message: Message) -> None:
        """Update text on edits — don't reset counters."""
        if message.chat.id != channel_id:
            return
        mongo.source_db[settings.source_collections.post_logs].update_one(
            {"post_id": message.message_id, "chat_id": message.chat.id},
            {"$set": {
                "text": (message.text or message.caption or "")[:500],
                "updated_at": _utcnow(),
            }},
        )

    return dp


def _detect_post_type(message: Message) -> str:
    if message.photo:
        return "photo"
    if message.video:
        return "video"
    if message.document:
        return "document"
    if message.poll:
        return "poll"
    if message.text:
        return "text"
    return "other"
