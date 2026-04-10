from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from pymongo import ASCENDING, MongoClient, UpdateOne
from pymongo.collection import Collection
from pymongo.database import Database

from app.config.settings import settings

logger = logging.getLogger(__name__)

_CONNECT_TIMEOUT_MS = 5_000
_SOCKET_TIMEOUT_MS = 30_000
_SERVER_SELECTION_TIMEOUT_MS = 5_000


class MongoService:
    def __init__(self) -> None:
        self.client = MongoClient(
            settings.mongodb_uri,
            tz_aware=True,
            connectTimeoutMS=_CONNECT_TIMEOUT_MS,
            socketTimeoutMS=_SOCKET_TIMEOUT_MS,
            serverSelectionTimeoutMS=_SERVER_SELECTION_TIMEOUT_MS,
        )
        self.db: Database = self.client[settings.mongodb_db_name]

    def source(self, name: str) -> Collection:
        collection_name = getattr(settings.source_collections, name)
        return self.db[collection_name]

    def derived(self, name: str) -> Collection:
        collection_name = getattr(settings.derived_collections, name)
        return self.db[collection_name]

    def setup_derived_indexes(self) -> None:
        self.derived("referral_daily").create_index([("date", ASCENDING)], unique=True)
        self.derived("referral_weekly").create_index([("week_start", ASCENDING), ("week_end", ASCENDING)], unique=True)
        self.derived("channel_daily").create_index([("date", ASCENDING)], unique=True)
        self.derived("content_daily").create_index([("date", ASCENDING), ("post_id", ASCENDING)], unique=True)
        self.derived("inviter_daily").create_index([("date", ASCENDING), ("inviter_user_id", ASCENDING)], unique=True)
        logger.info("Derived collection indexes initialized")

    def upsert_one(self, collection: str, filter_query: dict[str, Any], document: dict[str, Any]) -> None:
        now = datetime.now(timezone.utc)
        # Do NOT put created_at in $set — use $setOnInsert only so it is never overwritten on re-runs.
        doc_to_set = {**document, "updated_at": now}
        doc_to_set.pop("created_at", None)
        self.derived(collection).update_one(
            filter_query,
            {"$set": doc_to_set, "$setOnInsert": {"created_at": now}},
            upsert=True,
        )

    def bulk_upsert(self, collection: str, operations: list[tuple[dict[str, Any], dict[str, Any]]]) -> None:
        if not operations:
            return
        now = datetime.now(timezone.utc)
        writes = []
        for filter_query, document in operations:
            doc_to_set = {**document, "updated_at": now}
            doc_to_set.pop("created_at", None)
            writes.append(
                UpdateOne(
                    filter_query,
                    {"$set": doc_to_set, "$setOnInsert": {"created_at": now}},
                    upsert=True,
                )
            )
        self.derived(collection).bulk_write(writes, ordered=False)
