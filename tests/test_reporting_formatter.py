from datetime import datetime, timezone

from app.reporting.formatter import build_daily_report, build_weekly_report


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


def test_build_daily_report_low_conversion_alert() -> None:
    report = build_daily_report(
        report_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
        tz_name="Asia/Kuala_Lumpur",
        referral={"joins": 50, "qualified": 5, "pending_hold": 0, "suspicious_patterns": []},
        channel={"new_joins": 5, "leaves": 1, "net_growth": 4, "churn_signals": []},
        content={"top_post": None, "weakest_post": None},
    )
    assert "Conversion is below 20%" in report


def test_build_daily_report_no_posts() -> None:
    """Formatter should not crash when top_post and weakest_post are None."""
    report = build_daily_report(
        report_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
        tz_name="Asia/Kuala_Lumpur",
        referral={"joins": 0, "qualified": 0, "pending_hold": 0, "suspicious_patterns": []},
        channel={"new_joins": 0, "leaves": 0, "net_growth": 0, "churn_signals": []},
        content={"top_post": None, "weakest_post": None},
    )
    assert "null" in report


def test_build_weekly_report_no_channel_data() -> None:
    """Weekly report must explicitly note missing channel data rather than silently omitting."""
    report = build_weekly_report(
        report_date=datetime(2026, 1, 6, tzinfo=timezone.utc),
        tz_name="Asia/Kuala_Lumpur",
        weekly_referral={
            "joins": 100,
            "qualified": 40,
            "overall_conversion": 0.4,
            "trend_vs_previous_week": 0.05,
            "avg_time_to_qualify_hours": 6.5,
            "failure_reason_breakdown": {"failed_no_checkin": 5, "failed_not_subscribed": 2, "failed_left_before_hold": 3},
        },
    )
    assert "Weekly Growth Intelligence Report" in report
    assert "not available" in report  # channel section must be explicit about missing data


def test_build_weekly_report_with_channel_data() -> None:
    report = build_weekly_report(
        report_date=datetime(2026, 1, 6, tzinfo=timezone.utc),
        tz_name="Asia/Kuala_Lumpur",
        weekly_referral={
            "joins": 100,
            "qualified": 40,
            "overall_conversion": 0.4,
            "trend_vs_previous_week": None,
            "avg_time_to_qualify_hours": None,
            "failure_reason_breakdown": {},
        },
        weekly_channel={"net_growth": 250},
    )
    assert "250" in report

