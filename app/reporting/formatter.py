from __future__ import annotations

from datetime import datetime
from typing import Any

from app.analytics.rules import safe_divide
from app.utils.time import format_local


def _fmt_pct(value: float | None) -> str:
    if value is None:
        return "null"
    return f"{round(value * 100, 2)}%"


def build_daily_report(
    report_date: datetime,
    tz_name: str,
    referral: dict[str, Any],
    channel: dict[str, Any],
    content: dict[str, Any],
    segmentation: dict[str, Any] | None = None,
    segmentation_kpis: dict[str, Any] | None = None,
) -> str:
    conversion = safe_divide(referral.get("qualified", 0), referral.get("joins", 0))
    top_post = content.get("top_post")
    weakest_post = content.get("weakest_post")

    alerts: list[str] = []
    if referral.get("suspicious_patterns"):
        alerts.append("Referral anomaly: " + ", ".join(referral["suspicious_patterns"]))
    if channel.get("churn_signals"):
        alerts.append("Channel churn signal: " + ", ".join(channel["churn_signals"]))
    if referral.get("joins", 0) > 0 and conversion is not None and conversion < 0.2:
        alerts.append("Conversion is below 20%")

    actions = [
        "Review top 5 low-quality inviters and suspend suspicious acquisition sources.",
        "Prioritize content angles from top post during high-response posting window.",
        "Audit no-checkin and left-before-hold failures for onboarding friction.",
    ]

    lines = [
        "📊 Daily Growth Intelligence Report",
        f"Date: {format_local(report_date, tz_name)}",
        "",
        "Referral",
        f"- Joins: {referral.get('joins')}",
        f"- Qualified: {referral.get('qualified')}",
        f"- Pending hold: {referral.get('pending_hold')}",
        f"- Join→Qualified conversion: {_fmt_pct(conversion)}",
        "",
        "Channel",
        f"- Joins/Leaves/Net: {channel.get('new_joins')} / {channel.get('leaves')} / {channel.get('net_growth')}",
        "",
        "Content",
        f"- Top post: {top_post.get('post_id') if top_post else 'null'}",
        f"- Weakest post: {weakest_post.get('post_id') if weakest_post else 'null'}",
        "",
        "Alerts",
    ]

    lines.extend([f"- {alert}" for alert in alerts] or ["- none"])
    lines.extend(
        [
            "",
            "Segmentation",
            f"- New: {(segmentation or {}).get('new', 0)}  → onboarding_push",
            f"- Active: {(segmentation or {}).get('active', 0)}  → leaderboard_competition",
            f"- At risk: {(segmentation or {}).get('at_risk', 0)}  → comeback_voucher",
            f"- Dead: {(segmentation or {}).get('dead', 0)}  → aggressive_reactivation",
            f"- High value: {(segmentation or {}).get('high_value', 0)}  → vip_treatment",
            f"- Unknown: {(segmentation or {}).get('unknown', 0)}  → no_action",
            f"- No claim history: {(segmentation or {}).get('no_claim_history', 0)}  → excluded_from_main_segments",
            "",
            "KPIs",
            f"- Claim→Play conversion: {_fmt_pct((segmentation_kpis or {}).get('claim_to_play_conversion'))}",
            f"- D3 retention: {_fmt_pct((segmentation_kpis or {}).get('d3_retention_rate'))}",
            f"- D7 retention: {_fmt_pct((segmentation_kpis or {}).get('d7_retention_rate'))}",
            f"- Cost per active player: {(segmentation_kpis or {}).get('cost_per_active_player', 'null')}",
        ]
    )
    lines.extend(["", "Actions"])
    lines.extend([f"- {action}" for action in actions])
    return "\n".join(lines)


def build_weekly_report(report_date: datetime, tz_name: str, weekly_referral: dict[str, Any], weekly_channel: dict[str, Any] | None = None) -> str:
    actions = [
        "Shift operator focus to inviters with highest stable conversion.",
        "Reduce traffic sources producing repeated no-checkin/left-before-hold failures.",
        "Replicate best-performing content type and angle in next weekly plan.",
    ]

    lines = [
        "🧠 Weekly Growth Intelligence Report",
        f"Generated: {format_local(report_date, tz_name)}",
        "",
        "Referral",
        f"- Total referred joins: {weekly_referral.get('joins')}",
        f"- Total qualified: {weekly_referral.get('qualified')}",
        f"- Conversion: {_fmt_pct(weekly_referral.get('overall_conversion'))}",
        f"- Trend vs previous week: {_fmt_pct(weekly_referral.get('trend_vs_previous_week')) if weekly_referral.get('trend_vs_previous_week') is not None else 'no prior week data'}",
        "- Avg time to qualify: {}h (approx — averaged from daily figures)".format(
            weekly_referral.get('avg_time_to_qualify_hours') if weekly_referral.get('avg_time_to_qualify_hours') is not None else 'null'
        ),
    ]

    if weekly_channel:
        lines.extend(
            [
                "",
                "Channel",
                f"- Weekly net growth: {weekly_channel.get('net_growth')}",
            ]
        )
    else:
        lines.extend(["", "Channel", "- Weekly channel summary: not available"])

    lines.extend(
        [
            "",
            "Bottlenecks",
            f"- Failure reasons: {weekly_referral.get('failure_reason_breakdown')}",
            "",
            "Actions",
        ]
    )
    lines.extend([f"- {action}" for action in actions])
    return "\n".join(lines)
