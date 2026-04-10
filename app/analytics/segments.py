from __future__ import annotations


def classify_segment(total_claims: int, last_active_days: int | None) -> str:
    if last_active_days is None:
        return "unknown"

    if total_claims >= 30 and last_active_days <= 7:
        return "high_value"
    if last_active_days <= 2 and total_claims <= 5:
        return "new"
    if last_active_days <= 3:
        return "active"
    if 4 <= last_active_days <= 7:
        return "at_risk"
    if last_active_days > 7:
        return "dead"
    return "unknown"


def action_for_segment(segment: str) -> str:
    if segment == "new":
        return "onboarding_push"
    if segment == "active":
        return "leaderboard_competition"
    if segment == "at_risk":
        return "comeback_voucher"
    if segment == "dead":
        return "aggressive_reactivation"
    if segment == "high_value":
        return "vip_treatment"
    return "no_action"
