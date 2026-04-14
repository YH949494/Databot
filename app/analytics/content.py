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
    logger.info("Content source resolution: posts=%s", "post_logs")
    if not mongo.has_source_collection("post_logs"):
        logger.warning("Content source collection not available: %s — writing source_missing sentinel", "post_logs")
        sentinel = {
            "date": day_start,
            "post_count": None,
            "top_post": None,
            "weakest_post": None,
            "_source_missing": True,
        }
        mongo.bulk_upsert("content_daily", [({"date": day_start, "post_id": "__sentinel__"}, sentinel)])
        return sentinel

    posts = list(
        mongo.source("post_logs").find(
            {"post_time": {"$gte": day_start, "$lt": day_end}},
            {
                "post_id": 1, "post_time": 1,
                "content_type": 1, "media_type": 1,
                "voucher_code": 1, "drop_id": 1,
                "views": 1, "reactions": 1, "reaction_breakdown": 1,
                "shares": 1, "comments": 1,
                "claims_1h": 1, "claims_6h": 1, "claims_24h": 1,
                "referred_joins_after_post": 1,
            },
        )
    )

    rows: list[dict[str, Any]] = []
    for post in posts:
        views      = int(post.get("views", 0) or 0)
        reactions  = int(post.get("reactions", 0) or 0)
        shares     = int(post.get("shares", 0) or 0)
        claims_24h = int(post.get("claims_24h", 0) or 0)
        row = {
            "date":              day_start,
            "post_id":           post.get("post_id"),
            "post_time":         post.get("post_time"),
            "content_type":      post.get("content_type", "text"),
            "media_type":        post.get("media_type", "text"),
            "voucher_code":      post.get("voucher_code"),
            "drop_id":           post.get("drop_id"),
            "views":             views,
            "reactions":         reactions,
            "reaction_breakdown": post.get("reaction_breakdown", {}),
            "shares":            shares,
            "comments":          int(post.get("comments", 0) or 0),
            "claims_1h":         int(post.get("claims_1h", 0) or 0),
            "claims_6h":         int(post.get("claims_6h", 0) or 0),
            "claims_24h":        claims_24h,
            "referred_joins_after_post": post.get("referred_joins_after_post", 0),
            # Engagement score: views + reactions×2 + shares×3 + claims×5
            "engagement_score":  views + reactions * 2 + shares * 3 + claims_24h * 5,
            "claim_rate_per_view": safe_divide(claims_24h, views),
        }
        rows.append(row)

    mongo.bulk_upsert("content_daily", [({"date": row["date"], "post_id": row["post_id"]}, row) for row in rows])

    # Rank by engagement_score: views + reactions×2 + shares×3 + claims×5
    top_post     = max(rows, key=lambda x: x.get("engagement_score", 0), default=None)
    rated_rows   = [r for r in rows if r.get("claim_rate_per_view") is not None]
    weakest_post = min(rated_rows, key=lambda x: x["claim_rate_per_view"], default=None)

    # Per content_type breakdown
    by_type: dict[str, dict] = {}
    for r in rows:
        ct = r.get("content_type", "text")
        if ct not in by_type:
            by_type[ct] = {"count": 0, "total_views": 0, "total_reactions": 0,
                           "total_shares": 0, "total_claims_24h": 0}
        by_type[ct]["count"]             += 1
        by_type[ct]["total_views"]       += r.get("views", 0)
        by_type[ct]["total_reactions"]   += r.get("reactions", 0)
        by_type[ct]["total_shares"]      += r.get("shares", 0)
        by_type[ct]["total_claims_24h"]  += r.get("claims_24h", 0)

    result = {
        "date":         day_start,
        "post_count":   len(rows),
        "top_post":     top_post,
        "weakest_post": weakest_post,
        "by_content_type": by_type,
    }
    logger.info("Computed content daily summary for %s", day_start.date())
    return result
