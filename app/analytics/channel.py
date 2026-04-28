from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from app.analytics.rules import abnormal_spike
from app.clients.mongo_client import MongoService
from app.utils.time import day_bounds_utc, week_bounds_utc

logger = logging.getLogger(__name__)


def compute_channel_daily(mongo: MongoService, for_date: datetime) -> dict[str, Any]:
    day_start, day_end = day_bounds_utc(for_date.astimezone(timezone.utc))
    logger.info("Channel source resolution: events=%s", "channel_events")
    if not mongo.has_source_collection("channel_events"):
        logger.warning("Channel source collection not available: %s — writing source_missing sentinel", "channel_events")
        summary = {
            "date": day_start,
            "new_joins": None,
            "leaves": None,
            "net_growth": None,
            "active_subscribers": None,
            "referred_joins": None,
            "non_referred_joins": None,
            "churn_signals": [],
            "_source_missing": True,
        }
        mongo.upsert_one("channel_daily", {"date": day_start}, summary)
        return summary

    pipeline: list[dict] = [
        {"$match": {"event_time": {"$gte": day_start, "$lt": day_end}}},
        {
            "$group": {
                "_id": None,
                "joins": {"$sum": {"$cond": [{"$eq": ["$event_type", "join"]}, 1, 0]}},
                "leaves": {"$sum": {"$cond": [{"$eq": ["$event_type", "leave"]}, 1, 0]}},
                "referred_joins": {
                    "$sum": {
                        "$cond": [
                            {
                                "$and": [
                                    {"$eq": ["$event_type", "join"]},
                                    {"$eq": ["$is_referred", True]},
                                ]
                            },
                            1,
                            0,
                        ]
                    }
                },
            }
        },
    ]

    result = next(mongo.source("channel_events").aggregate(pipeline), None)
    joins = int(result["joins"]) if result else 0
    leaves = int(result["leaves"]) if result else 0
    referred_joins = int(result["referred_joins"]) if result else 0

    prior_days = list(
        mongo.derived("channel_daily").find(
            {"date": {"$gte": day_start - timedelta(days=7), "$lt": day_start}},
            {"leaves": 1},
        )
    )
    baseline_leave_avg = (
        sum(int(doc.get("leaves") or 0) for doc in prior_days) / len(prior_days)
        if prior_days
        else None
    )

    churn_signals: list[str] = []
    if abnormal_spike(leaves, baseline_leave_avg):
        churn_signals.append("leave_spike_vs_recent_baseline")

    summary = {
        "date": day_start,
        "new_joins": joins,
        "leaves": leaves,
        "net_growth": joins - leaves,
        "active_subscribers": None,
        "referred_joins": referred_joins,
        "non_referred_joins": joins - referred_joins,
        "churn_signals": churn_signals,
    }

    mongo.upsert_one("channel_daily", {"date": day_start}, summary)
    logger.info("Computed channel daily summary for %s", day_start.date())
    return summary


def compute_channel_weekly(mongo: MongoService, for_date: datetime) -> dict[str, Any]:
    week_start, week_end = week_bounds_utc(for_date.astimezone(timezone.utc))
    daily_docs = list(
        mongo.derived("channel_daily").find({"date": {"$gte": week_start, "$lt": week_end}})
    )
    # Exclude days where the source collection was unavailable — their metrics are
    # None (not real zeros), so including them would silently undercount the week.
    active_docs = [doc for doc in daily_docs if not doc.get("_source_missing")]
    joins = sum(int(doc.get("new_joins") or 0) for doc in active_docs)
    leaves = sum(int(doc.get("leaves") or 0) for doc in active_docs)
    referred_joins = sum(int(doc.get("referred_joins") or 0) for doc in active_docs)
    summary = {
        "week_start": week_start,
        "week_end": week_end,
        "joins": joins,
        "leaves": leaves,
        "net_growth": joins - leaves,
        "referred_joins": referred_joins,
        "days_with_data": len(active_docs),
        "days_source_missing": len(daily_docs) - len(active_docs),
    }
    logger.info(
        "Computed channel weekly summary for week starting %s: joins=%d leaves=%d active_days=%d missing_days=%d",
        week_start.date(), joins, leaves, len(active_docs), summary["days_source_missing"],
    )
    return summary
