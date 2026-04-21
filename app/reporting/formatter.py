from __future__ import annotations
from datetime import datetime
from typing import Any
from app.analytics.rules import safe_divide
from app.utils.time import format_local

def _fmt_pct(t): return "⚠️ unavailable" if t is None else f"{round(t*100,2)}%"
def _fmt_val(v, u=""): return "⚠️ unavailable" if v is None else f"{v}{u}"
def _fmt_delta(d, s=""):
    if d is None: return ""
    return f"  ({'+' if d>=0 else ''}{d:.0f}{s})"

def build_channel_stats_section(stats):
    if not stats: return ["- ⚠️ Channel stats unavailable (getChatStatistics not yet run)"]
    lines = []
    m = stats.get("member_count"); md = stats.get("member_count_delta")
    lines.append(f"- Followers: {_fmt_val(m)}{_fmt_delta(md)}")
    np = stats.get("enabled_notifications_percent")
    lines.append(f"- Enabled Notifications: {_fmt_pct((np/100) if np is not None else None)}")
    v = stats.get("mean_view_count"); vd = stats.get("mean_view_count_delta")
    lines.append(f"- Views Per Post: {_fmt_val(v)}{_fmt_delta(vd)}")
    sh = stats.get("mean_share_count"); shd = stats.get("mean_share_count_delta")
    lines.append(f"- Shares Per Post: {_fmt_val(sh)}{_fmt_delta(shd)}")
    rt = stats.get("mean_reaction_count"); rtd = stats.get("mean_reaction_count_delta")
    lines.append(f"- Reactions Per Post: {_fmt_val(rt)}{_fmt_delta(rtd)}")
    ps = stats.get("period_start"); pe = stats.get("period_end")
    if ps and pe: lines.append(f"- Stats period: {ps} → {pe}")
    return lines

def build_message_stats_section(post):
    v = post.get("views", 0); re = post.get("reactions", 0)
    sh = post.get("shares", 0)
    lines = [f"  views={_fmt_val(v)}  reactions={_fmt_val(re)}  shares={_fmt_val(sh)}"]
    bd = post.get("reaction_breakdown", {})
    if bd: lines.append("  Reactions: " + "  ".join(f"{e}{n}" for e,n in bd.items()))
    return lines

def _build_summary(referral, channel, content, alerts):
    tp = content.get("top_post")
    top_react = tp.get("reactions", 0) if tp else 0
    top_type  = tp.get("content_type", "?") if tp else "—"
    net       = channel.get("net_growth")
    joins     = channel.get("new_joins", 0)
    leaves    = channel.get("leaves", 0)
    net_str   = f"{'+' if (net or 0) >= 0 else ''}{net}" if net is not None else "?"
    alert_str = f"⚠️ {len(alerts)} alert{'s' if len(alerts)!=1 else ''}" if alerts else "✅ No alerts"
    return [
        "┌─ Today at a Glance ─────────────────",
        f"│ 🎫 Claims: {_fmt_val(referral.get('joins'))}   📊 Total referrals: {_fmt_val(referral.get('total_referrals_snapshot'))}",
        f"│ 👥 Channel net: {net_str}  ({joins} in / {leaves} out)",
        f"│ 📝 Posts: {content.get('post_count', 0)}   🔥 Top post: {top_react} reactions [{top_type}]",
        f"│ {alert_str}",
        "└─────────────────────────────────────",
    ]

def build_daily_report(report_date, tz_name, referral, channel, content, segmentation=None, segmentation_kpis=None, channel_stats=None):
    tp = content.get("top_post"); wp = content.get("weakest_post")
    alerts = []
    if referral.get("suspicious_patterns"): alerts.append("Referral anomaly: " + ", ".join(referral["suspicious_patterns"]))
    if channel.get("churn_signals"): alerts.append("Channel churn signal: " + ", ".join(channel["churn_signals"]))
    ti = ", ".join(f"{i.get('username') or i.get('inviter_user_id','?')} ({i.get('referral_count','?')})" for i in referral.get("top_inviters_this_week",[])[:3]) or "none"
    lines = [
        "📊 Daily Growth Intelligence Report", f"Date: {format_local(report_date,tz_name)}", "",
    ]
    lines.extend(_build_summary(referral, channel, content, alerts))
    lines.extend([
        "", "Referral", f"- Voucher claims today: {_fmt_val(referral.get('joins'))}",
        f"- Total referrals (all-time): {_fmt_val(referral.get('total_referrals_snapshot'))}",
        f"- Top inviters this week: {ti}", "", "📡 Channel Stats (Telegram API)",
    ])
    lines.extend(build_channel_stats_section(channel_stats))
    lines.append(""); lines.append("Channel Events (MongoDB)")
    if channel.get("_source_missing"): lines.append("- ⚠️ Channel source collection unavailable")
    else: lines.append(f"- Joins/Leaves/Net: {_fmt_val(channel.get('new_joins'))} / {_fmt_val(channel.get('leaves'))} / {_fmt_val(channel.get('net_growth'))}")
    lines.append(""); lines.append("Content")
    if content.get("_source_missing"): lines.append("- ⚠️ Content source collection unavailable")
    elif tp:
        lines.append(f"- Posts today: {content.get('post_count',0)}")
        lines.append(f"- Top post: #{tp.get('post_id','?')} [{tp.get('content_type','?')}]")
        lines.extend(build_message_stats_section(tp))
        if wp:
            lines.append(f"- Weakest post: #{wp.get('post_id','?')} [{wp.get('content_type','?')}]")
            lines.extend(build_message_stats_section(wp))
        byt = content.get("by_content_type",{})
        if byt:
            lines.append("- By type:")
            for ct,s in sorted(byt.items()):
                lines.append(f"  {ct:14s} posts={s['count']}  views={s['total_views']}  react={s['total_reactions']}  shares={s['total_shares']}  claims={s['total_claims_24h']}")
    else: lines.append("- No posts today")
    lines.append("")
    if alerts:
        lines.append("⚠️ Alerts ─────────────────────────────")
        lines.extend([f"  ‼️ {a}" for a in alerts])
        lines.append("────────────────────────────────────────")
    else:
        lines.extend(["⚠️ Alerts", "- none"])
    seg = segmentation or {}
    lines.extend(["","Segmentation",
        f"- New: {seg.get('new',0)}", f"- Active: {seg.get('active',0)}",
        f"- At risk: {seg.get('at_risk',0)}", f"- Dead: {seg.get('dead',0)}",
        f"- High value: {seg.get('high_value',0)}", f"- Unknown: {seg.get('unknown',0)}",
        f"- No claim history: {seg.get('no_claim_history',0)}",
        "","✅ Daily Actions",
        "- Review top 5 low-quality inviters and suspend suspicious acquisition sources.",
        "- Prioritize content angles from top post during high-response posting window.",
        "- Audit no-checkin and left-before-hold failures for onboarding friction."
    ])
    return "\n".join(lines)

def build_weekly_report(report_date, tz_name, weekly_referral, weekly_channel=None, channel_stats=None):
    lines = [
        "🧠 Weekly Growth Intelligence Report", f"Generated: {format_local(report_date,tz_name)}", "",
        "Referral", f"- Total referred joins: {weekly_referral.get('joins')}",
        f"- Total qualified: {weekly_referral.get('qualified')}",
        f"- Conversion: {_fmt_pct(weekly_referral.get('overall_conversion'))}",
        f"- Trend vs previous week: {_fmt_pct(weekly_referral.get('trend_vs_previous_week')) if weekly_referral.get('trend_vs_previous_week') is not None else 'no prior week data'}",
        f"- Avg time to qualify: {weekly_referral.get('avg_time_to_qualify_hours') or 'null'}h",
        "", "📡 Channel Stats (Telegram API)",
    ]
    lines.extend(build_channel_stats_section(channel_stats))
    if weekly_channel: lines.extend(["", "Channel Events", f"- Weekly net growth: {weekly_channel.get('net_growth')}"])
    else: lines.extend(["", "Channel Events", "- Weekly channel summary: not available"])
    lines.extend(["","Bottlenecks", f"- Failure reasons: {weekly_referral.get('failure_reason_breakdown')}",
        "","Actions",
        "- Shift operator focus to inviters with highest stable conversion.",
        "- Reduce traffic sources producing repeated no-checkin/left-before-hold failures.",
        "- Replicate best-performing content type and angle in next weekly plan."
    ])
    return "\n".join(lines)
