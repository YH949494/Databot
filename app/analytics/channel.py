from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from app.analytics.rules import abnormal_spike
from app.clients.mongo_client import MongoService
from app.utils.time import day_bounds_utc

logger = logging.getLogger(__name__)


def compute_channel_daily(mongo: MongoService, for_date: datetime) -> dict[str, Any]:
    day_start, day_end = day_bounds_utc(for_date.astimezone(timezone.utc))

    events = list(
        mongo.source("channel_events").find(
            {"event_time": {"$gte": day_start, "$lt": day_end}},
            {"event_type": 1, "is_referred": 1},
        )
    )

    joins = sum(1 for event in events if event.get("event_type") == "join")
    leaves = sum(1 for event in events if event.get("event_type") == "leave")
    referred_joins = sum(1 for event in events if event.get("event_type") == "join" and event.get("is_referred") is True)

    prior_days = list(mongo.derived("channel_daily").find({"date": {"$gte": day_start - timedelta(days=7), "$lt": day_start}}))
    baseline_leave_avg = None
    if prior_days:
        baseline_leave_avg = sum(doc.get("leaves", 0) for doc in prior_days) / len(prior_days)

    churn_signals = []
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
