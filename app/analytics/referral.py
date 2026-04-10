from __future__ import annotations

import logging
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

from app.analytics.rules import abnormal_spike, quality_flag, safe_divide, suspicious_inviter
from app.clients.mongo_client import MongoService
from app.utils.time import day_bounds_utc, week_bounds_utc

logger = logging.getLogger(__name__)


def _event_counts(events: list[dict[str, Any]]) -> Counter:
    return Counter(str(event.get("status") or event.get("event_type") or "unknown") for event in events)


def compute_referral_daily(mongo: MongoService, for_date: datetime) -> dict[str, Any]:
    day_start, day_end = day_bounds_utc(for_date.astimezone(timezone.utc))

    events = list(
        mongo.source("referral_events").find(
            {"event_time": {"$gte": day_start, "$lt": day_end}},
            {
                "inviter_user_id": 1,
                "status": 1,
                "event_type": 1,
                "qualified_at": 1,
                "joined_at": 1,
            },
        )
    )
    counts = _event_counts(events)

    per_inviter: dict[str, dict[str, Any]] = defaultdict(lambda: {"joins": 0, "qualified": 0, "pending": 0, "failed_no_checkin": 0, "failed_left_before_hold": 0})
    qualify_hours: list[float] = []

    for event in events:
        inviter = str(event.get("inviter_user_id") or "unknown")
        status = str(event.get("status") or event.get("event_type") or "unknown")
        per_inviter[inviter]["joins"] += 1 if status in {"joined", "join", "pending"} else 0
        per_inviter[inviter]["qualified"] += 1 if status == "qualified" else 0
        per_inviter[inviter]["pending"] += 1 if status == "pending" else 0
        per_inviter[inviter]["failed_no_checkin"] += 1 if status == "failed_no_checkin" else 0
        per_inviter[inviter]["failed_left_before_hold"] += 1 if status == "failed_left_before_hold" else 0

        joined_at = event.get("joined_at")
        qualified_at = event.get("qualified_at")
        if isinstance(joined_at, datetime) and isinstance(qualified_at, datetime) and qualified_at >= joined_at:
            qualify_hours.append((qualified_at - joined_at).total_seconds() / 3600.0)

    inviter_stats: list[dict[str, Any]] = []
    for inviter_id, stat in per_inviter.items():
        conversion = safe_divide(stat["qualified"], stat["joins"])
        suspicious = suspicious_inviter(stat["joins"], conversion, stat["failed_left_before_hold"], stat["failed_no_checkin"])
        inviter_stats.append(
            {
                "inviter_user_id": inviter_id,
                "date": day_start,
                "joins": stat["joins"],
                "qualified": stat["qualified"],
                "pending": stat["pending"],
                "conversion_rate": conversion,
                "avg_time_to_qualify": None,
                "suspicious_flag": suspicious,
                "quality_flag": quality_flag(stat["joins"], conversion),
            }
        )

    inviter_stats_sorted = sorted(inviter_stats, key=lambda x: (x["qualified"], x["joins"]), reverse=True)
    top_inviters = inviter_stats_sorted[:5]
    low_quality_inviters = [x for x in inviter_stats if x["quality_flag"] == "low_quality"][:5]

    previous_days = list(
        mongo.derived("referral_daily").find({"date": {"$gte": day_start - timedelta(days=7), "$lt": day_start}}, {"joins": 1})
    )
    baseline_avg = None
    if previous_days:
        baseline_avg = sum(day.get("joins", 0) for day in previous_days) / len(previous_days)

    suspicious_patterns: list[str] = []
    joins = counts.get("joined", 0) + counts.get("join", 0)
    qualified = counts.get("qualified", 0)
    if abnormal_spike(joins, baseline_avg):
        suspicious_patterns.append("join_spike_vs_recent_baseline")
    if joins > 0 and safe_divide(qualified, joins) is not None and safe_divide(qualified, joins) < 0.15:
        suspicious_patterns.append("low_conversion_rate")

    summary = {
        "date": day_start,
        "joins": joins,
        "qualified": qualified,
        "pending_hold": counts.get("pending", 0),
        "failed_no_checkin": counts.get("failed_no_checkin", 0),
        "failed_not_subscribed": counts.get("failed_not_subscribed", 0),
        "failed_left_before_hold": counts.get("failed_left_before_hold", 0),
        "avg_time_to_qualify_hours": round(sum(qualify_hours) / len(qualify_hours), 2) if qualify_hours else None,
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
    daily_docs = list(mongo.derived("referral_daily").find({"date": {"$gte": week_start, "$lt": week_end}}))

    joins = sum(doc.get("joins", 0) for doc in daily_docs)
    qualified = sum(doc.get("qualified", 0) for doc in daily_docs)

    failure_reason_breakdown = {
        "failed_no_checkin": sum(doc.get("failed_no_checkin", 0) for doc in daily_docs),
        "failed_not_subscribed": sum(doc.get("failed_not_subscribed", 0) for doc in daily_docs),
        "failed_left_before_hold": sum(doc.get("failed_left_before_hold", 0) for doc in daily_docs),
    }

    avg_candidates = [doc.get("avg_time_to_qualify_hours") for doc in daily_docs if doc.get("avg_time_to_qualify_hours") is not None]
    avg_time = round(sum(avg_candidates) / len(avg_candidates), 2) if avg_candidates else None

    inviter_docs = list(mongo.derived("inviter_daily").find({"date": {"$gte": week_start, "$lt": week_end}}))
    per_inviter: dict[str, dict[str, int]] = defaultdict(lambda: {"joins": 0, "qualified": 0})
    for row in inviter_docs:
        inviter_id = str(row.get("inviter_user_id"))
        per_inviter[inviter_id]["joins"] += int(row.get("joins", 0))
        per_inviter[inviter_id]["qualified"] += int(row.get("qualified", 0))

    inviter_rows = []
    for inviter_id, stat in per_inviter.items():
        conv = safe_divide(stat["qualified"], stat["joins"])
        inviter_rows.append({"inviter_user_id": inviter_id, "joins": stat["joins"], "qualified": stat["qualified"], "conversion": conv})

    top_inviters = sorted(inviter_rows, key=lambda x: x["qualified"], reverse=True)[:5]
    inviters_with_best_conversion = sorted(
        [x for x in inviter_rows if x["joins"] >= 5 and x["conversion"] is not None],
        key=lambda x: x["conversion"],
        reverse=True,
    )[:5]
    inviters_with_low_quality_traffic = sorted(
        [x for x in inviter_rows if x["joins"] >= 5 and (x["conversion"] is None or x["conversion"] < 0.2)],
        key=lambda x: x["joins"],
        reverse=True,
    )[:5]

    previous_week_start = week_start - timedelta(days=7)
    previous_week = mongo.derived("referral_weekly").find_one({"week_start": previous_week_start})

    trend_vs_previous_week = None
    current_conversion = safe_divide(qualified, joins)
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
