from datetime import datetime, timezone

from app.reporting.formatter import build_daily_report


def test_build_daily_report_contains_required_sections() -> None:
    report = build_daily_report(
        report_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
        tz_name="Asia/Kuala_Lumpur",
        referral={"joins": 10, "qualified": 4, "pending_hold": 2, "suspicious_patterns": []},
        channel={"new_joins": 5, "leaves": 1, "net_growth": 4, "churn_signals": []},
        content={"top_post": {"post_id": 111}, "weakest_post": {"post_id": 222}},
    )

    assert "Daily Growth Intelligence Report" in report
    assert "Join→Qualified conversion" in report
    assert "Alerts" in report
    assert "Actions" in report
