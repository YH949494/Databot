from __future__ import annotations

from datetime import datetime
from typing import Any

from app.analytics.rules import safe_divide
from app.utils.time import format_local


def _fmt_pct(value: float | None) -> str:
    if value is None:
        return "⚠️ unavailable"
    return f"{round(value * 100, 2)}%"


def _fmt_val(value: Any, unit: str = "") -> str:
    """Render a metric value: None → ⚠️ unavailable, otherwise the value."""
    if value is None:
        return "⚠️ unavailable"
    return f"{value}{unit}"


def build_daily_report(
    report_date: datetime,
    tz_name: str,
    referral: dict[str, Any],
    channel: dict[str, Any],
    content: dict[str, Any],
    segmentation: dict[str, Any] | None = None,
    segmentation_kpis: dict[str, Any] | None = None,
) -> str:
    ref_joins = referral.get("joins")
    ref_qualified = referral.get("qualified")
    conversion = safe_divide(ref_qualified or 0, ref_joins or 0) if (ref_joins and ref_qualified is not None) else None

    top_post = content.get("top_post")
    weakest_post = content.get("weakest_post")
    channel_missing = channel.get("_source_missing", False)
    content_missing = content.get("_source_missing", False)

    alerts: list[str] = []
    if referral.get("suspicious_patterns"):
        alerts.append("Referral anomaly: " + ", ".join(referral["suspicious_patterns"]))
    if channel.get("churn_signals"):
        alerts.append("Channel churn signal: " + ", ".join(channel["churn_signals"]))
    # Conversion alert removed — qualified breakdown not available from vouchers schema

    actions = [
        "Review top 5 low-quality inviters and suspend suspicious acquisition sources.",
        "Prioritize content angles from top post during high-response posting window.",
        "Audit no-checkin and left-before-hold failures for onboarding friction.",
    ]

    top_inv_str = ", ".join(
        f"{i.get('username') or i.get('inviter_user_id','?')} ({i.get('referral_count','?')})"
        for i in referral.get("top_inviters", [])[:3]
    ) or "none"

    lines = [
        "📊 Daily Growth Intelligence Report",
        f"Date: {format_local(report_date, tz_name)}",
        "",
        "Referral",
        f"- Voucher claims today: {_fmt_val(ref_joins)}",
        f"- Total referrals (all-time): {_fmt_val(referral.get('total_referrals_snapshot'))}",
        f"- Top inviters: {top_inv_str}",
        "",
        "Channel",
    ]

    if channel_missing:
        lines.append("- ⚠️ Channel source collection unavailable — data not collected")
    else:
        lines.append(f"- Joins/Leaves/Net: {_fmt_val(channel.get('new_joins'))} / {_fmt_val(channel.get('leaves'))} / {_fmt_val(channel.get('net_growth'))}")

    lines.append("")
    lines.append("Content")

    if content_missing:
        lines.append("- ⚠️ Content source collection unavailable — data not collected")
    else:
        lines.append(f"- Top post: {top_post.get('post_id') if top_post else 'none today'}")
        lines.append(f"- Weakest post: {weakest_post.get('post_id') if weakest_post else 'none today'}")

    lines.extend(["", "Alerts"])
    lines.extend([f"- {alert}" for alert in alerts] or ["- none"])

    seg = segmentation or {}
    kpis = segmentation_kpis or {}
    lines.extend(
        [
            "",
            "Segmentation",
            f"- New: {seg.get('new', 0)}  → onboarding_push",
            f"- Active: {seg.get('active', 0)}  → leaderboard_competition",
            f"- At risk: {seg.get('at_risk', 0)}  → comeback_voucher",
            f"- Dead: {seg.get('dead', 0)}  → aggressive_reactivation",
            f"- High value: {seg.get('high_value', 0)}  → vip_treatment",
            f"- Unknown: {seg.get('unknown', 0)}  → no_action",
            f"- No claim history: {seg.get('no_claim_history', 0)}  → excluded_from_main_segments",
            "",
            "KPIs",
            f"- Claim→Play conversion: {_fmt_pct(kpis.get('claim_to_play_conversion'))}",
            f"- D3 retention: {_fmt_pct(kpis.get('d3_retention_rate'))}",
            f"- D7 retention: {_fmt_pct(kpis.get('d7_retention_rate'))}",
            f"- Cost per active player: {_fmt_val(kpis.get('cost_per_active_player'))}",
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
