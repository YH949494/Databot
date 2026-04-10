from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from app.analytics.rules import safe_divide
from app.clients.mongo_client import MongoService
from app.utils.time import day_bounds_utc

logger = logging.getLogger(__name__)


def compute_content_daily(mongo: MongoService, for_date: datetime) -> dict[str, Any]:
    day_start, day_end = day_bounds_utc(for_date.astimezone(timezone.utc))

    posts = list(
        mongo.source("post_logs").find(
            {"post_time": {"$gte": day_start, "$lt": day_end}},
            {
                "post_id": 1,
                "post_time": 1,
                "post_type": 1,
                "angle": 1,
                "campaign_id": 1,
                "voucher_id": 1,
                "views": 1,
                "reactions": 1,
                "comments": 1,
                "claims_1h": 1,
                "claims_6h": 1,
                "claims_24h": 1,
                "referred_joins_after_post": 1,
                "qualified_after_post": 1,
            },
        )
    )

    rows: list[dict[str, Any]] = []
    for post in posts:
        views = int(post.get("views", 0) or 0)
        claims_24h = int(post.get("claims_24h", 0) or 0)
        row = {
            "date": day_start,
            "post_id": post.get("post_id"),
            "post_time": post.get("post_time"),
            "post_type": post.get("post_type"),
            "angle": post.get("angle"),
            "campaign_id": post.get("campaign_id"),
            "voucher_id": post.get("voucher_id"),
            "views": views,
            "reactions": int(post.get("reactions", 0) or 0),
            "comments": int(post.get("comments", 0) or 0),
            "claims_1h": int(post.get("claims_1h", 0) or 0),
            "claims_6h": int(post.get("claims_6h", 0) or 0),
            "claims_24h": claims_24h,
            "referred_joins_after_post": post.get("referred_joins_after_post"),
            "qualified_after_post": post.get("qualified_after_post"),
            "claim_rate_per_view": safe_divide(claims_24h, views),
        }
        rows.append(row)

    mongo.bulk_upsert("content_daily", [({"date": row["date"], "post_id": row["post_id"]}, row) for row in rows])

    top_post = max(rows, key=lambda x: x.get("claims_24h", 0), default=None)
    weakest_post = min(rows, key=lambda x: x.get("claim_rate_per_view") or 1, default=None)

    result = {
        "date": day_start,
        "post_count": len(rows),
        "top_post": top_post,
        "weakest_post": weakest_post,
    }
    logger.info("Computed content daily summary for %s", day_start.date())
    return result
