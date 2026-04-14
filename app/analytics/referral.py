from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

from app.analytics.rules import abnormal_spike, quality_flag, safe_divide, suspicious_inviter
from app.clients.mongo_client import MongoService
from app.utils.time import day_bounds_utc, week_bounds_utc

logger = logging.getLogger(__name__)

# Minimum join count before flagging low conversion to avoid noise on low-volume days.
_MIN_JOINS_FOR_CONVERSION_ALERT = 10


def _field_exists(mongo: MongoService, collection: str, field_name: str) -> bool:
    return mongo.source(collection).find_one({field_name: {"$exists": True}}, {"_id": 1}) is not None


def compute_referral_daily(mongo: MongoService, for_date: datetime) -> dict[str, Any]:
    day_start, day_end = day_bounds_utc(for_date.astimezone(timezone.utc))
    logger.info("Referral source resolution: events=%s", "referral_events")
    has_status = _field_exists(mongo, "referral_events", "status")
    has_event_time = _field_exists(mongo, "referral_events", "event_time")
    has_created_at = _field_exists(mongo, "referral_events", "created_at")

    if not has_status or not has_event_time:
        if not has_created_at:
            logger.warning(
                "Referral source unavailable for daily status metrics: collection=%s required_fields=status+event_time or created_at",
                "referral_events",
            )
            summary = {
                "date": day_start,
                "joins": 0,
                "qualified": 0,
                "pending_hold": 0,
                "failed_no_checkin": 0,
                "failed_not_subscribed": 0,
                "failed_left_before_hold": 0,
                "avg_time_to_qualify_hours": None,
                "top_inviters": [],
                "low_quality_inviters": [],
                "suspicious_patterns": [],
            }
            mongo.upsert_one("referral_daily", {"date": day_start}, summary)
            return summary

        logger.warning(
            "Referral status fields not available; using created_at join-only aggregation from %s",
            "referral_events",
        )
        join_pipeline: list[dict] = [
            {"$match": {"created_at": {"$gte": day_start, "$lt": day_end}}},
            {"$group": {"_id": "$referrer_user_id", "joins": {"$sum": 1}}},
        ]
        inviter_stats = []
        total_joins = 0
        for row in mongo.source("referral_events").aggregate(join_pipeline):
            joins = int(row.get("joins", 0))
            total_joins += joins
            inviter_stats.append(
                {
                    "inviter_user_id": str(row.get("_id") or "unknown"),
                    "date": day_start,
                    "joins": joins,
                    "qualified": 0,
                    "pending": 0,
                    "conversion_rate": None,
                    "avg_time_to_qualify_hours": None,
                    "suspicious_flag": False,
                    "quality_flag": "insufficient_data",
                }
            )
        inviter_stats_sorted = sorted(inviter_stats, key=lambda x: x["joins"], reverse=True)
        summary = {
            "date": day_start,
            "joins": total_joins,
            "qualified": 0,
            "pending_hold": 0,
            "failed_no_checkin": 0,
            "failed_not_subscribed": 0,
            "failed_left_before_hold": 0,
            "avg_time_to_qualify_hours": None,
            "top_inviters": inviter_stats_sorted[:5],
            "low_quality_inviters": [],
            "suspicious_patterns": [],
        }
        mongo.upsert_one("referral_daily", {"date": day_start}, summary)
        mongo.bulk_upsert(
            "inviter_daily",
            [({"date": x["date"], "inviter_user_id": x["inviter_user_id"]}, x) for x in inviter_stats],
        )
        return summary

    # Server-side aggregation — avoids pulling all documents into Python memory.
    pipeline_status_counts: list[dict] = [
        {"$match": {"event_time": {"$gte": day_start, "$lt": day_end}}},
        {"$group": {"_id": "$status", "count": {"$sum": 1}}},
    ]
    status_counts: dict[str, int] = {}
    for row in mongo.source("referral_events").aggregate(pipeline_status_counts):
        key = str(row["_id"] or "unknown")
        status_counts[key] = int(row["count"])

    # "joined" and "join" are treated as the same event depending on source schema.
    joins = status_counts.get("joined", 0) + status_counts.get("join", 0)
    qualified = status_counts.get("qualified", 0)
    pending = status_counts.get("pending", 0)
    failed_no_checkin = status_counts.get("failed_no_checkin", 0)
    failed_not_subscribed = status_counts.get("failed_not_subscribed", 0)
    failed_left_before_hold = status_counts.get("failed_left_before_hold", 0)

    # Per-inviter aggregation — server-side.
    pipeline_per_inviter: list[dict] = [
        {"$match": {"event_time": {"$gte": day_start, "$lt": day_end}}},
        {
            "$group": {
                "_id": "$inviter_user_id",
                "joins": {
                    "$sum": {
                        "$cond": [{"$in": ["$status", ["joined", "join"]]}, 1, 0]
                    }
                },
                "qualified": {
                    "$sum": {"$cond": [{"$eq": ["$status", "qualified"]}, 1, 0]}
                },
                "pending": {
                    "$sum": {"$cond": [{"$eq": ["$status", "pending"]}, 1, 0]}
                },
                "failed_no_checkin": {
                    "$sum": {"$cond": [{"$eq": ["$status", "failed_no_checkin"]}, 1, 0]}
                },
                "failed_left_before_hold": {
                    "$sum": {"$cond": [{"$eq": ["$status", "failed_left_before_hold"]}, 1, 0]}
                },
                # Average seconds from join to qualification (only for qualified events with both timestamps).
                "avg_qualify_seconds": {
                    "$avg": {
                        "$cond": [
                            {
                                "$and": [
                                    {"$eq": ["$status", "qualified"]},
                                    {"$gt": ["$qualified_at", None]},
                                    {"$gt": ["$joined_at", None]},
                                    {"$gte": ["$qualified_at", "$joined_at"]},
                                ]
                            },
                            {
                                "$divide": [
                                    {"$subtract": ["$qualified_at", "$joined_at"]},
                                    1000,  # milliseconds → seconds
                                ]
                            },
                            None,
                        ]
                    }
                },
            }
        },
    ]

    inviter_stats: list[dict[str, Any]] = []
    for row in mongo.source("referral_events").aggregate(pipeline_per_inviter):
        inviter_id = str(row["_id"] or "unknown")
        inv_joins = int(row.get("joins", 0))
        inv_qualified = int(row.get("qualified", 0))
        inv_pending = int(row.get("pending", 0))
        inv_failed_left = int(row.get("failed_left_before_hold", 0))
        inv_failed_no_checkin = int(row.get("failed_no_checkin", 0))
        avg_qualify_seconds = row.get("avg_qualify_seconds")
        avg_qualify_hours = (
            round(avg_qualify_seconds / 3600.0, 2)
            if avg_qualify_seconds is not None
            else None
        )
        conversion = safe_divide(inv_qualified, inv_joins)
        inviter_stats.append(
            {
                "inviter_user_id": inviter_id,
                "date": day_start,
                "joins": inv_joins,
                "qualified": inv_qualified,
                "pending": inv_pending,
                "conversion_rate": conversion,
                "avg_time_to_qualify_hours": avg_qualify_hours,
                "suspicious_flag": suspicious_inviter(
                    inv_joins, conversion, inv_failed_left, inv_failed_no_checkin
                ),
                "quality_flag": quality_flag(inv_joins, conversion),
            }
        )

    inviter_stats_sorted = sorted(
        inviter_stats, key=lambda x: (x["qualified"], x["joins"]), reverse=True
    )
    top_inviters = inviter_stats_sorted[:5]
    low_quality_inviters = [x for x in inviter_stats if x["quality_flag"] == "low_quality"][:5]

    previous_days = list(
        mongo.derived("referral_daily").find(
            {"date": {"$gte": day_start - timedelta(days=7), "$lt": day_start}},
            {"joins": 1},
        )
    )
    baseline_avg = (
        sum(day.get("joins", 0) for day in previous_days) / len(previous_days)
        if previous_days
        else None
    )

    suspicious_patterns: list[str] = []
    if abnormal_spike(joins, baseline_avg):
        suspicious_patterns.append("join_spike_vs_recent_baseline")
    conversion_rate = safe_divide(qualified, joins)
    if (
        joins >= _MIN_JOINS_FOR_CONVERSION_ALERT
        and conversion_rate is not None
        and conversion_rate < 0.15
    ):
        suspicious_patterns.append("low_conversion_rate")

    # Daily avg_time_to_qualify — weighted mean across all inviters with data.
    all_qualify_hours = [
        s["avg_time_to_qualify_hours"]
        for s in inviter_stats
        if s["avg_time_to_qualify_hours"] is not None
    ]
    daily_avg_qualify = (
        round(sum(all_qualify_hours) / len(all_qualify_hours), 2) if all_qualify_hours else None
    )

    summary = {
        "date": day_start,
        "joins": joins,
        "qualified": qualified,
        "pending_hold": pending,
        "failed_no_checkin": failed_no_checkin,
        "failed_not_subscribed": failed_not_subscribed,
        "failed_left_before_hold": failed_left_before_hold,
        "avg_time_to_qualify_hours": daily_avg_qualify,
        "top_inviters": top_inviters,
        "low_quality_inviters": low_quality_inviters,
        "suspicious_patterns": suspicious_patterns,
    }

    mongo.upsert_one("referral_daily", {"date": day_start}, summary)
    mongo.bulk_upsert(
        "inviter_daily",
        [({"date": x["date"], "inviter_user_id": x["inviter_user_id"]}, x) for x in inviter_stats],
    )
    logger.info("Computed referral daily summary for %s", day_start.date())
    return summary


def compute_referral_weekly(mongo: MongoService, for_date: datetime) -> dict[str, Any]:
    week_start, week_end = week_bounds_utc(for_date.astimezone(timezone.utc))
    daily_docs = list(
        mongo.derived("referral_daily").find({"date": {"$gte": week_start, "$lt": week_end}})
    )

    joins = sum(doc.get("joins", 0) for doc in daily_docs)
    qualified = sum(doc.get("qualified", 0) for doc in daily_docs)

    failure_reason_breakdown = {
        "failed_no_checkin": sum(doc.get("failed_no_checkin", 0) for doc in daily_docs),
        "failed_not_subscribed": sum(doc.get("failed_not_subscribed", 0) for doc in daily_docs),
        "failed_left_before_hold": sum(doc.get("failed_left_before_hold", 0) for doc in daily_docs),
    }

    # Weekly avg_time: use the daily values as proxies. Documented limitation — true
    # event-level average would require re-aggregating source events.
    avg_candidates = [
        doc.get("avg_time_to_qualify_hours")
        for doc in daily_docs
        if doc.get("avg_time_to_qualify_hours") is not None
    ]
    avg_time = round(sum(avg_candidates) / len(avg_candidates), 2) if avg_candidates else None

    # Narrow projection — only fields needed for weekly inviter aggregation.
    inviter_docs = list(
        mongo.derived("inviter_daily").find(
            {"date": {"$gte": week_start, "$lt": week_end}},
            {"inviter_user_id": 1, "joins": 1, "qualified": 1},
        )
    )
    per_inviter: dict[str, dict[str, int]] = defaultdict(lambda: {"joins": 0, "qualified": 0})
    for row in inviter_docs:
        inviter_id = str(row.get("inviter_user_id"))
        per_inviter[inviter_id]["joins"] += int(row.get("joins", 0))
        per_inviter[inviter_id]["qualified"] += int(row.get("qualified", 0))

    inviter_rows = []
    for inviter_id, stat in per_inviter.items():
        conv = safe_divide(stat["qualified"], stat["joins"])
        inviter_rows.append(
            {
                "inviter_user_id": inviter_id,
                "joins": stat["joins"],
                "qualified": stat["qualified"],
                "conversion": conv,
            }
        )

    top_inviters = sorted(inviter_rows, key=lambda x: x["qualified"], reverse=True)[:5]
    inviters_with_best_conversion = sorted(
        [x for x in inviter_rows if x["joins"] >= 5 and x["conversion"] is not None],
        key=lambda x: x["conversion"],
        reverse=True,
    )[:5]
    inviters_with_low_quality_traffic = sorted(
        [
            x
            for x in inviter_rows
            if x["joins"] >= 5 and (x["conversion"] is None or x["conversion"] < 0.2)
        ],
        key=lambda x: x["joins"],
        reverse=True,
    )[:5]

    previous_week_start = week_start - timedelta(days=7)
    previous_week = mongo.derived("referral_weekly").find_one(
        {"week_start": previous_week_start}
    )

    current_conversion = safe_divide(qualified, joins)
    trend_vs_previous_week = None
    if previous_week:
        prev_conversion = previous_week.get("overall_conversion")
        if current_conversion is not None and prev_conversion is not None:
            trend_vs_previous_week = round(current_conversion - prev_conversion, 4)

    summary = {
        "week_start": week_start,
        "week_end": week_end,
        "joins": joins,
        "qualified": qualified,
        "overall_conversion": current_conversion,
        "avg_time_to_qualify_hours": avg_time,
        "top_inviters": top_inviters,
        "inviters_with_best_conversion": inviters_with_best_conversion,
        "inviters_with_low_quality_traffic": inviters_with_low_quality_traffic,
        "failure_reason_breakdown": failure_reason_breakdown,
        "trend_vs_previous_week": trend_vs_previous_week,
    }

    mongo.upsert_one("referral_weekly", {"week_start": week_start, "week_end": week_end}, summary)
    logger.info("Computed referral weekly summary for week starting %s", week_start.date())
    return summary
