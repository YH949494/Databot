from __future__ import annotations


def safe_divide(numerator: float, denominator: float) -> float | None:
    if denominator <= 0:
        return None
    return round(numerator / denominator, 4)


def suspicious_inviter(joins: int, conversion_rate: float | None, left_before_hold: int, no_checkin: int) -> bool:
    if joins >= 20 and (conversion_rate is None or conversion_rate < 0.1):
        return True
    if left_before_hold >= 10:
        return True
    if no_checkin >= 10:
        return True
    return False


def quality_flag(joins: int, conversion_rate: float | None) -> str:
    if joins < 5 or conversion_rate is None:
        return "insufficient_data"
    if conversion_rate >= 0.6:
        return "high_quality"
    if conversion_rate >= 0.3:
        return "normal"
    return "low_quality"


def abnormal_spike(current: int, baseline_avg: float | None) -> bool:
    if baseline_avg is None or baseline_avg <= 0:
        return False
    return current >= (baseline_avg * 2.0)
