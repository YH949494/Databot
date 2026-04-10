from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from pymongo import ASCENDING, MongoClient, UpdateOne
from pymongo.collection import Collection
from pymongo.database import Database

from app.config.settings import settings

logger = logging.getLogger(__name__)


class MongoService:
    def __init__(self) -> None:
        self.client = MongoClient(settings.mongodb_uri, tz_aware=True)
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
        now = datetime.utcnow()
        document["updated_at"] = now
        document.setdefault("created_at", now)
        self.derived(collection).update_one(filter_query, {"$set": document, "$setOnInsert": {"created_at": now}}, upsert=True)

    def bulk_upsert(self, collection: str, operations: list[tuple[dict[str, Any], dict[str, Any]]]) -> None:
        if not operations:
            return
        now = datetime.utcnow()
        writes = []
        for filter_query, document in operations:
            document["updated_at"] = now
            document.setdefault("created_at", now)
            writes.append(
                UpdateOne(filter_query, {"$set": document, "$setOnInsert": {"created_at": now}}, upsert=True)
            )
        self.derived(collection).bulk_write(writes, ordered=False)
