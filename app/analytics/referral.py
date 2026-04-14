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
    """Compute daily referral metrics from users.referral_count and vouchers.

    Source of truth:
      - users.referral_count  — cumulative referrals per user (set by referral_bot)
      - vouchers               — daily claim activity (claimedAt, usernameLower/claimedBy, status)

    'joins' = new voucher claims today (status=claimed, claimedAt within day).
    'top_inviters' = users with highest referral_count, ranked descending.
    Qualified/pending/failure breakdown not available from this schema — reported as None.
    """
    day_start, day_end = day_bounds_utc(for_date.astimezone(timezone.utc))
    logger.info("Referral daily: deriving from users.referral_count + vouchers")

    # --- Daily voucher claims (proxy for new joins / engagement) ---
    _CLAIMED_AT_EXPR = {"$ifNull": ["$claimed_at", "$claimedAt"]}
    claims_pipeline: list[dict] = [
        {
            "$match": {
                "$expr": {
                    "$and": [
                        {"$gte": [_CLAIMED_AT_EXPR, day_start]},
                        {"$lt":  [_CLAIMED_AT_EXPR, day_end]},
                    ]
                },
                "status": "claimed",
            }
        },
        {"$count": "total"},
    ]
    result = next(mongo.source("claim_events").aggregate(claims_pipeline), None)
    daily_claims = int(result["total"]) if result else 0

    # --- Top inviters from users.referral_count ---
    top_inviters_cursor = mongo.source("users").find(
        {"referral_count": {"$exists": True, "$gt": 0}},
        {"_id": 1, "user_id": 1, "username": 1, "referral_count": 1},
    ).sort("referral_count", -1).limit(10)

    top_inviters = []
    for row in top_inviters_cursor:
        uid = str(row.get("user_id") or row.get("_id") or "unknown")
        top_inviters.append({
            "inviter_user_id": uid,
            "username": row.get("username"),
            "referral_count": int(row.get("referral_count", 0)),
        })

    # --- Total referral_count across all users (snapshot) ---
    total_referral_agg = list(mongo.source("users").aggregate([
        {"$match": {"referral_count": {"$exists": True, "$gt": 0}}},
        {"$group": {"_id": None, "total": {"$sum": "$referral_count"}}},
    ]))
    total_referrals = int(total_referral_agg[0]["total"]) if total_referral_agg else 0

    # --- Spike detection vs prior 7 days ---
    previous_days = list(
        mongo.derived("referral_daily").find(
            {"date": {"$gte": day_start - timedelta(days=7), "$lt": day_start}},
            {"daily_claims": 1},
        )
    )
    baseline_avg = (
        sum(d.get("daily_claims", 0) for d in previous_days) / len(previous_days)
        if previous_days else None
    )
    suspicious_patterns: list[str] = []
    if abnormal_spike(daily_claims, baseline_avg):
        suspicious_patterns.append("claim_spike_vs_recent_baseline")

    summary = {
        "date": day_start,
        "joins": daily_claims,           # voucher claims today — best proxy for daily referral activity
        "total_referrals_snapshot": total_referrals,  # cumulative across all users
        "qualified": None,               # not available from this schema
        "pending_hold": None,
        "failed_no_checkin": None,
        "failed_not_subscribed": None,
        "failed_left_before_hold": None,
        "avg_time_to_qualify_hours": None,
        "top_inviters": top_inviters[:5],
        "low_quality_inviters": [],
        "suspicious_patterns": suspicious_patterns,
    }

    mongo.upsert_one("referral_daily", {"date": day_start}, summary)

    # Write per-inviter rows to inviter_daily for weekly rollup
    if top_inviters:
        mongo.bulk_upsert(
            "inviter_daily",
            [
                (
                    {"date": day_start, "inviter_user_id": inv["inviter_user_id"]},
                    {**inv, "date": day_start, "joins": inv["referral_count"]},
                )
                for inv in top_inviters
            ],
        )

    logger.info("Computed referral daily summary for %s: claims=%d total_referrals=%d",
                day_start.date(), daily_claims, total_referrals)
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
