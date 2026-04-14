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
_CLAIMED_AT_EXPR: dict[str, Any] = {"$ifNull": ["$claimed_at", "$claimedAt"]}
_USERNAME_LOWER_EXPR: dict[str, Any] = {
    "$ifNull": ["$usernameLower", {"$ifNull": ["$username_lower", "$claimedBy"]}]
}


def _resolve_user_id(raw: Any) -> str | None:
    if raw is None:
        return None
    if isinstance(raw, dict):
        if "user_id" in raw:
            return _resolve_user_id(raw.get("user_id"))
        if "_id" in raw:
            return _resolve_user_id(raw.get("_id"))
        if len(raw) == 1:
            return _resolve_user_id(next(iter(raw.values())))
        return None
    value = str(raw).strip()
    return value or None


def _field_exists(mongo: MongoService, collection: str, field_name: str) -> bool:
    return mongo.source(collection).find_one({field_name: {"$exists": True}}, {"_id": 1}) is not None


def _user_id_by_username_lower(mongo: MongoService) -> dict[str, str]:
    """Build username_lower -> user_id map. Users collection stores 'username';
    vouchers link via usernameLower / claimedBy (already lowercased)."""
    mapping: dict[str, str] = {}
    for row in mongo.source("users").find({}, {"_id": 1, "user_id": 1, "username": 1, "usernameLower": 1}):
        username_lower = row.get("usernameLower") or (
            str(row["username"]).strip().lower() if row.get("username") else None
        )
        if username_lower is None:
            continue
        user_id = _resolve_user_id(row.get("user_id") or row.get("_id"))
        if user_id is None:
            continue
        mapping[username_lower] = user_id
    return mapping


def _resolve_claim_user_id(row: dict[str, Any], username_map: dict[str, str]) -> str | None:
    claim_id = row.get("_id")
    if isinstance(claim_id, dict):
        direct = _resolve_user_id(claim_id.get("user_id"))
        username_lower = claim_id.get("username_lower")
    else:
        direct = _resolve_user_id(row.get("user_id") or claim_id)
        username_lower = row.get("username_lower")
    if direct:
        return direct
    if username_lower is None:
        return None
    return username_map.get(str(username_lower).strip().lower())


def _user_ids_for_day(mongo: MongoService, day_start: datetime, day_end: datetime) -> set[str]:
    has_claimed_time = _field_exists(mongo, "claim_events", "claimed_at") or _field_exists(mongo, "claim_events", "claimedAt")
    if not has_claimed_time:
        logger.warning("Claim timestamp field unavailable in %s; cannot compute play users", "claim_events")
        return set()
    username_map = _user_id_by_username_lower(mongo)
    pipeline: list[dict[str, Any]] = [
        {
            "$match": {
                "$expr": {"$and": [{"$gte": [_CLAIMED_AT_EXPR, day_start]}, {"$lt": [_CLAIMED_AT_EXPR, day_end]}]},
                "bet_amount": {"$gt": 0},
            }
        },
        {"$group": {"_id": {"user_id": "$user_id", "username_lower": _USERNAME_LOWER_EXPR}}},
    ]
    user_ids: set[str] = set()
    for row in mongo.source("claim_events").aggregate(pipeline):
        uid = _resolve_claim_user_id(row, username_map)
        if uid is not None:
            user_ids.add(uid)
    return user_ids


def compute_user_profiles(mongo: MongoService, for_date: datetime) -> dict[str, int]:
    day_start, _ = day_bounds_utc(for_date.astimezone(timezone.utc))
    now = datetime.now(timezone.utc)
    username_map = _user_id_by_username_lower(mongo)

    logger.info("Segmentation source resolution: claims=%s referrals=%s users=%s", "claim_events", "referral_events", "users")

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
                "_id": {"user_id": "$user_id", "username_lower": _USERNAME_LOWER_EXPR},
                "total_claims": {"$sum": 1},
                "last_claim_at": {"$max": _CLAIMED_AT_EXPR},
                "first_claim_at": {"$min": _CLAIMED_AT_EXPR},
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
    claim_rows: dict[str, Any] = {}
    for row in mongo.source("claim_events").aggregate(claim_pipeline):
        uid = _resolve_claim_user_id(row, username_map)
        if uid:
            claim_rows[uid] = row

    # referral_count is stored directly on each user doc — read it from users
    # rather than aggregating the referrals collection.
    referral_rows: dict[str, int] = {}
    for row in mongo.source("users").find(
        {"referral_count": {"$exists": True, "$gt": 0}},
        {"_id": 1, "user_id": 1, "referral_count": 1},
    ):
        uid = _resolve_user_id(row.get("user_id") or row.get("_id"))
        if uid:
            referral_rows[uid] = int(row.get("referral_count", 0))

    # ------------------------------------------------------------------ #
    # Stream the users collection one document at a time.                 #
    # We never materialise all_user_ids as a Python set.                  #
    # For each user we look up their claim/referral rows from the maps     #
    # above, compute the profile in-place, and batch-write every          #
    # _BATCH_SIZE documents.  Peak additional RAM ≈ _BATCH_SIZE × ~512 B. #
    # ------------------------------------------------------------------ #
    segment_counts: dict[str, int] = {
        "new": 0,
        "active": 0,
        "at_risk": 0,
        "dead": 0,
        "high_value": 0,
        "unknown": 0,
        "no_claim_history": 0,
    }
    batch: list[tuple[dict[str, Any], dict[str, Any]]] = []

    def _flush(b: list) -> None:
        if b:
            mongo.bulk_upsert("user_profile_summary", list(b))
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
                result_count = int(claim.get("result_count", 0) or 0)
                if result_count > 0:
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

        meta = user_meta.get(user_id, {})
        return {
            "user_id": user_id,
            "username": meta.get("username"),
            "xp": meta.get("xp"),
            "region": meta.get("region"),
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

    # Stream users collection only to count users with no claim history.
    users_seen: set[str] = set()
    # Also build a uid -> {xp, region} map for profile enrichment
    user_meta: dict[str, dict] = {}
    users_cursor = mongo.source("users").aggregate(
        [
            {
                "$project": {
                    "_id": 0,
                    "user_id": {"$ifNull": ["$user_id", {"$toString": "$_id"}]},
                    "xp": 1,
                    "region": 1,
                    "username": 1,
                }
            }
        ]
    )
    for row in users_cursor:
        uid = _resolve_user_id(row.get("user_id") or row.get("_id"))
        if not uid or uid in users_seen:
            continue
        users_seen.add(uid)
        user_meta[uid] = {"xp": row.get("xp"), "region": row.get("region"), "username": row.get("username")}
        if uid not in claim_rows:
            segment_counts["no_claim_history"] += 1

    # Build profiles only for users with claim history.
    for uid in claim_rows.keys():
        batch.append(({"user_id": uid}, _build_doc(uid)))
        if len(batch) >= _BATCH_SIZE:
            _flush(batch)

    _flush(batch)  # final partial batch
    logger.info("Computed user profile summary for %s", day_start.date())
    return segment_counts


def compute_segmentation_kpis(mongo: MongoService, for_date: datetime) -> dict[str, Any]:
    day_start, day_end = day_bounds_utc(for_date.astimezone(timezone.utc))
    username_map = _user_id_by_username_lower(mongo)
    has_claimed_time = _field_exists(mongo, "claim_events", "claimed_at") or _field_exists(mongo, "claim_events", "claimedAt")

    has_bet_amount = _field_exists(mongo, "claim_events", "bet_amount")
    has_voucher_value = _field_exists(mongo, "claim_events", "voucher_value")

    if not has_claimed_time:
        logger.warning("Claim timestamp field unavailable in %s; claim-derived KPIs may be null", "claim_events")

    claims_pipeline = [
        {"$match": {"$expr": {"$and": [{"$gte": [_CLAIMED_AT_EXPR, day_start]}, {"$lt": [_CLAIMED_AT_EXPR, day_end]}]}}},
        {"$group": {"_id": {"user_id": "$user_id", "username_lower": _USERNAME_LOWER_EXPR}}},
    ]
    claimed_users = set()
    for row in mongo.source("claim_events").aggregate(claims_pipeline):
        uid = _resolve_claim_user_id(row, username_map)
        if uid is not None:
            claimed_users.add(uid)

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
    else:
        logger.warning("Gameplay source fields unavailable in %s: bet_amount missing; play/result KPIs set to null", "claim_events")

    if has_bet_amount and has_voucher_value:
        voucher_pipeline: list[dict[str, Any]] = [
            {"$match": {"$expr": {"$and": [{"$gte": [_CLAIMED_AT_EXPR, day_start]}, {"$lt": [_CLAIMED_AT_EXPR, day_end]}]}}},
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
