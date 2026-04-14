from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from app.analytics.rules import safe_divide
from app.analytics.segments import action_for_segment, classify_segment
from app.clients.mongo_client import MongoService
from app.utils.time import day_bounds_utc

logger = logging.getLogger(__name__)

_BATCH_SIZE = 500  # docs flushed per bulk_write call


def _field_exists(mongo: MongoService, collection: str, field_name: str) -> bool:
    return mongo.source(collection).find_one({field_name: {"$exists": True}}, {"_id": 1}) is not None


def _user_ids_for_day(mongo: MongoService, day_start: datetime, day_end: datetime) -> set[str]:
    pipeline: list[dict[str, Any]] = [
        {
            "$match": {
                "claimed_at": {"$gte": day_start, "$lt": day_end},
                "bet_amount": {"$gt": 0},
            }
        },
        {"$group": {"_id": "$user_id"}},
    ]
    return {str(row["_id"]) for row in mongo.source("claim_events").aggregate(pipeline) if row.get("_id") is not None}


def compute_user_profiles(mongo: MongoService, for_date: datetime) -> dict[str, int]:
    day_start, _ = day_bounds_utc(for_date.astimezone(timezone.utc))
    now = datetime.now(timezone.utc)

    has_bet_amount = _field_exists(mongo, "claim_events", "bet_amount")
    has_result = _field_exists(mongo, "claim_events", "result")

    # ------------------------------------------------------------------ #
    # Build two server-side aggregation maps.  These are the only things  #
    # we keep in Python memory for the duration of the job.               #
    # Both are {user_id -> aggregated row} and stay well under 512 MB     #
    # because the values are simple counters, not full documents.         #
    # ------------------------------------------------------------------ #
    claim_pipeline: list[dict[str, Any]] = [
        {
            "$group": {
                "_id": "$user_id",
                "total_claims": {"$sum": 1},
                "last_claim_at": {"$max": "$claimed_at"},
                "first_claim_at": {"$min": "$claimed_at"},
                "total_bet": {
                    "$sum": {
                        "$cond": [
                            {"$in": [{"$type": "$bet_amount"}, ["double", "int", "long", "decimal"]]},
                            "$bet_amount",
                            0,
                        ]
                    }
                },
                "win_count": {"$sum": {"$cond": [{"$eq": ["$result", "win"]}, 1, 0]}},
                "loss_count": {"$sum": {"$cond": [{"$eq": ["$result", "loss"]}, 1, 0]}},
                "result_count": {
                    "$sum": {
                        "$cond": [
                            {"$in": ["$result", ["win", "loss"]]},
                            1,
                            0,
                        ]
                    }
                },
            }
        }
    ]
    claim_rows: dict[str, Any] = {
        str(row["_id"]): row
        for row in mongo.source("claim_events").aggregate(claim_pipeline)
        if row.get("_id") is not None
    }

    referral_pipeline: list[dict[str, Any]] = [
        {"$group": {"_id": "$inviter_user_id", "referral_count": {"$sum": 1}}}
    ]
    referral_rows: dict[str, int] = {
        str(row["_id"]): int(row.get("referral_count", 0))
        for row in mongo.source("referral_events").aggregate(referral_pipeline)
        if row.get("_id") is not None
    }

    # ------------------------------------------------------------------ #
    # Stream the users collection one document at a time.                 #
    # We never materialise all_user_ids as a Python set.                  #
    # For each user we look up their claim/referral rows from the maps     #
    # above, compute the profile in-place, and batch-write every          #
    # _BATCH_SIZE documents.  Peak additional RAM ≈ _BATCH_SIZE × ~512 B. #
    # ------------------------------------------------------------------ #
    segment_counts: dict[str, int] = {
        "new": 0, "active": 0, "at_risk": 0, "dead": 0, "high_value": 0, "unknown": 0
    }
    batch: list[tuple[dict[str, Any], dict[str, Any]]] = []

    # Track which user_ids we have seen so we can later add any that only
    # appear in claim_rows / referral_rows but not in the users collection.
    seen_ids: set[str] = set()

    def _flush(b: list) -> None:
        if b:
            mongo.bulk_upsert("user_profile_summary", b)
            b.clear()

    def _build_doc(user_id: str) -> dict[str, Any]:
        claim = claim_rows.get(user_id)
        total_claims = int(claim.get("total_claims", 0)) if claim else 0

        last_active_days = None
        play_frequency = None
        total_bet = None
        win_loss_pattern = None

        if claim and claim.get("last_claim_at") is not None:
            last_active_days = (
                day_start.date() - claim["last_claim_at"].astimezone(timezone.utc).date()
            ).days

            first_claim_at = claim.get("first_claim_at")
            if first_claim_at is not None:
                days_since_first = (
                    day_start.date() - first_claim_at.astimezone(timezone.utc).date()
                ).days
                if days_since_first >= 2:
                    play_frequency = round(total_claims / days_since_first, 4)

            if has_bet_amount:
                total_bet = float(claim.get("total_bet", 0.0))

            if has_result:
                result_count = int(claim.get("result_count", 0))
                if result_count > 0 and total_claims > 0:
                    win_rate = int(claim.get("win_count", 0)) / result_count
                    if win_rate >= 0.55:
                        win_loss_pattern = "winning"
                    elif win_rate <= 0.35:
                        win_loss_pattern = "losing"
                    else:
                        win_loss_pattern = "neutral"

        segment = classify_segment(total_claims=total_claims, last_active_days=last_active_days)
        action_tag = action_for_segment(segment)
        segment_counts[segment] = segment_counts.get(segment, 0) + 1

        return {
            "user_id": user_id,
            "total_claims": total_claims,
            "last_active_days": last_active_days,
            "total_bet": total_bet,
            "play_frequency": play_frequency,
            "win_loss_pattern": win_loss_pattern,
            "referral_count": int(referral_rows.get(user_id, 0)),
            "segment": segment,
            "action_tag": action_tag,
            "computed_at": now,
        }

    # Pass 1: stream users collection
    users_cursor = mongo.source("users").aggregate(
        [
            {
                "$project": {
                    "_id": 0,
                    "user_id": {"$ifNull": ["$user_id", {"$toString": "$_id"}]},
                }
            }
        ]
    )
    for row in users_cursor:
        uid = str(row.get("user_id") or "")
        if not uid:
            continue
        seen_ids.add(uid)
        batch.append(({"user_id": uid}, _build_doc(uid)))
        if len(batch) >= _BATCH_SIZE:
            _flush(batch)

    # Pass 2: users that only appear in claim_rows or referral_rows
    orphan_ids = (set(claim_rows) | set(referral_rows)) - seen_ids
    for uid in orphan_ids:
        batch.append(({"user_id": uid}, _build_doc(uid)))
        if len(batch) >= _BATCH_SIZE:
            _flush(batch)

    _flush(batch)  # final partial batch
    logger.info("Computed user profile summary for %s", day_start.date())
    return segment_counts


def compute_segmentation_kpis(mongo: MongoService, for_date: datetime) -> dict[str, Any]:
    day_start, day_end = day_bounds_utc(for_date.astimezone(timezone.utc))

    has_bet_amount = _field_exists(mongo, "claim_events", "bet_amount")
    has_voucher_value = _field_exists(mongo, "claim_events", "voucher_value")

    claims_pipeline = [
        {"$match": {"claimed_at": {"$gte": day_start, "$lt": day_end}}},
        {"$group": {"_id": "$user_id"}},
    ]
    claimed_users = {
        str(row["_id"])
        for row in mongo.source("claim_events").aggregate(claims_pipeline)
        if row.get("_id") is not None
    }

    claim_to_play_conversion = None
    d3_retention_rate = None
    d7_retention_rate = None
    cost_per_active_player = None

    if has_bet_amount:
        played_users = _user_ids_for_day(mongo, day_start, day_end)
        claim_to_play_conversion = safe_divide(len(claimed_users & played_users), len(claimed_users))

        d3_base_start, d3_base_end = day_bounds_utc(day_start - timedelta(days=3))
        d3_cohort = _user_ids_for_day(mongo, d3_base_start, d3_base_end)
        d3_target_start, d3_target_end = day_bounds_utc(day_start)
        d3_retained = _user_ids_for_day(mongo, d3_target_start, d3_target_end)
        d3_retention_rate = safe_divide(len(d3_cohort & d3_retained), len(d3_cohort))

        d7_base_start, d7_base_end = day_bounds_utc(day_start - timedelta(days=7))
        d7_cohort = _user_ids_for_day(mongo, d7_base_start, d7_base_end)
        d7_target_start, d7_target_end = day_bounds_utc(day_start)
        d7_retained = _user_ids_for_day(mongo, d7_target_start, d7_target_end)
        d7_retention_rate = safe_divide(len(d7_cohort & d7_retained), len(d7_cohort))

    if has_bet_amount and has_voucher_value:
        voucher_pipeline: list[dict[str, Any]] = [
            {"$match": {"claimed_at": {"$gte": day_start, "$lt": day_end}}},
            {
                "$group": {
                    "_id": None,
                    "voucher_value_sum": {
                        "$sum": {
                            "$cond": [
                                {"$in": [{"$type": "$voucher_value"}, ["double", "int", "long", "decimal"]]},
                                "$voucher_value",
                                0,
                            ]
                        }
                    },
                }
            },
        ]
        voucher_row = next(mongo.source("claim_events").aggregate(voucher_pipeline), None)
        voucher_total = float(voucher_row.get("voucher_value_sum", 0.0)) if voucher_row else 0.0
        active_users = claimed_users & played_users
        cost_per_active_player = safe_divide(voucher_total, len(active_users))

    summary = {
        "date": day_start,
        "claim_to_play_conversion": claim_to_play_conversion,
        "d3_retention_rate": d3_retention_rate,
        "d7_retention_rate": d7_retention_rate,
        "cost_per_active_player": cost_per_active_player,
    }

    mongo.upsert_one("segmentation_kpis", {"date": day_start}, summary)
    logger.info("Computed segmentation KPIs for %s", day_start.date())
    return summary
