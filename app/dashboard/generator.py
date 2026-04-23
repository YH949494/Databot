from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from app.clients.mongo_client import MongoService
from app.config.settings import settings
from app.utils.time import utc_now

logger = logging.getLogger(__name__)

DASHBOARD_PATH = Path(__file__).parent / "index.html"


def _safe(val: Any, default: Any = 0) -> Any:
    return val if val is not None else default


def _fmt_date(dt: Any) -> str:
    if isinstance(dt, datetime):
        return dt.strftime("%b %d")
    return str(dt)[-5:]


def _fetch_referral_trend(mongo: MongoService, days: int) -> list[dict]:
    cutoff = utc_now() - timedelta(days=days)
    return list(
        mongo.derived("referral_daily").find(
            {"date": {"$gte": cutoff}},
            {"_id": 0, "date": 1, "joins": 1, "total_referrals_snapshot": 1},
            sort=[("date", 1)],
        )
    )


def _fetch_channel_trend(mongo: MongoService, days: int) -> list[dict]:
    cutoff = utc_now() - timedelta(days=days)
    return list(
        mongo.derived("channel_daily").find(
            {"date": {"$gte": cutoff}},
            {"_id": 0, "date": 1, "new_joins": 1, "leaves": 1, "net_growth": 1},
            sort=[("date", 1)],
        )
    )


def _fetch_content_trend(mongo: MongoService, days: int) -> list[dict]:
    cutoff = utc_now() - timedelta(days=days)
    pipeline = [
        {"$match": {"date": {"$gte": cutoff}, "post_id": {"$ne": "__sentinel__"}}},
        {
            "$group": {
                "_id": "$date",
                "post_count": {"$sum": 1},
                "total_views": {"$sum": "$views"},
                "total_reactions": {"$sum": "$reactions"},
            }
        },
        {"$sort": {"_id": 1}},
    ]
    return list(mongo.derived("content_daily").aggregate(pipeline))


def _fetch_segmentation(mongo: MongoService) -> dict:
    return mongo.derived("segmentation_kpis").find_one(
        {}, {"_id": 0}, sort=[("date", -1)]
    ) or {}


def _fetch_channel_stats(mongo: MongoService) -> dict:
    return (
        mongo.source_db[settings.source_collections.channel_stats_overview].find_one(
            {"_type": "channel_stats_snapshot"}, {"_id": 0}
        )
        or {}
    )


def generate_dashboard(mongo: MongoService) -> None:
    try:
        referral_docs = _fetch_referral_trend(mongo, 7)
        channel_docs = _fetch_channel_trend(mongo, 7)
        content_docs = _fetch_content_trend(mongo, 7)
        seg = _fetch_segmentation(mongo)
        stats = _fetch_channel_stats(mongo)

        today_ref = referral_docs[-1] if referral_docs else {}
        today_ch = channel_docs[-1] if channel_docs else {}

        net = today_ch.get("net_growth")
        net_str = ("+" if isinstance(net, (int, float)) and net >= 0 else "") + str(net) if net is not None else "—"
        notif_pct = stats.get("enabled_notifications_percent")
        mean_views = stats.get("mean_view_count")
        mean_reactions = stats.get("mean_reaction_count")

        html = _build_html(
            generated_at=utc_now().strftime("%Y-%m-%d %H:%M UTC"),
            members=_safe(stats.get("member_count"), "—"),
            notif_str=f"{notif_pct:.1f}%" if notif_pct is not None else "—",
            mean_views_str=f"{mean_views:.0f}" if mean_views is not None else "—",
            mean_reactions_str=f"{mean_reactions:.1f}" if mean_reactions is not None else "—",
            today_claims=_safe(today_ref.get("joins"), "—"),
            total_referrals=_safe(today_ref.get("total_referrals_snapshot"), "—"),
            net_str=net_str,
            # Referral trend
            ref_dates=json.dumps([_fmt_date(d["date"]) for d in referral_docs]),
            ref_joins=json.dumps([_safe(d.get("joins")) for d in referral_docs]),
            # Channel trend
            ch_dates=json.dumps([_fmt_date(d["date"]) for d in channel_docs]),
            ch_joins=json.dumps([_safe(d.get("new_joins")) for d in channel_docs]),
            ch_leaves=json.dumps([_safe(d.get("leaves")) for d in channel_docs]),
            ch_net=json.dumps([_safe(d.get("net_growth")) for d in channel_docs]),
            # Content trend
            ct_dates=json.dumps([_fmt_date(d["_id"]) for d in content_docs]),
            ct_views=json.dumps([_safe(d.get("total_views")) for d in content_docs]),
            ct_reactions=json.dumps([_safe(d.get("total_reactions")) for d in content_docs]),
            # Segmentation
            seg_labels=json.dumps(["New", "Active", "At Risk", "Dead", "High Value", "Unknown"]),
            seg_values=json.dumps([
                _safe(seg.get("new")),
                _safe(seg.get("active")),
                _safe(seg.get("at_risk")),
                _safe(seg.get("dead")),
                _safe(seg.get("high_value")),
                _safe(seg.get("unknown")),
            ]),
        )

        DASHBOARD_PATH.write_text(html, encoding="utf-8")
        logger.info("Dashboard generated → %s", DASHBOARD_PATH)
    except Exception:
        logger.exception("Dashboard generation failed")


def _build_html(
    generated_at: str,
    members: Any,
    notif_str: str,
    mean_views_str: str,
    mean_reactions_str: str,
    today_claims: Any,
    total_referrals: Any,
    net_str: str,
    ref_dates: str,
    ref_joins: str,
    ch_dates: str,
    ch_joins: str,
    ch_leaves: str,
    ch_net: str,
    ct_dates: str,
    ct_views: str,
    ct_reactions: str,
    seg_labels: str,
    seg_values: str,
) -> str:
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <meta http-equiv="refresh" content="300">
  <title>Databot Dashboard</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
  <style>
    *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
    :root {{
      --bg:       #0f172a;
      --surface:  #1e293b;
      --border:   #334155;
      --text:     #e2e8f0;
      --muted:    #94a3b8;
      --blue:     #3b82f6;
      --teal:     #14b8a6;
      --amber:    #f59e0b;
      --rose:     #f43f5e;
      --purple:   #a855f7;
      --emerald:  #10b981;
    }}
    body {{
      background: var(--bg);
      color: var(--text);
      font-family: system-ui, -apple-system, sans-serif;
      font-size: 14px;
      min-height: 100vh;
    }}
    header {{
      background: var(--surface);
      border-bottom: 1px solid var(--border);
      padding: 18px 28px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      flex-wrap: wrap;
      gap: 8px;
    }}
    header h1 {{
      font-size: 20px;
      font-weight: 700;
      letter-spacing: -0.3px;
    }}
    header h1 span {{ color: var(--blue); }}
    .updated {{
      font-size: 12px;
      color: var(--muted);
    }}
    .refresh-note {{
      font-size: 11px;
      color: var(--muted);
      margin-top: 2px;
    }}
    main {{
      padding: 24px 28px;
      max-width: 1400px;
      margin: 0 auto;
    }}
    /* ── Stat Cards ── */
    .cards {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
      gap: 16px;
      margin-bottom: 28px;
    }}
    .card {{
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 18px 20px;
    }}
    .card-label {{
      font-size: 11px;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.6px;
      color: var(--muted);
      margin-bottom: 8px;
    }}
    .card-value {{
      font-size: 28px;
      font-weight: 700;
      line-height: 1;
    }}
    .card-sub {{
      font-size: 11px;
      color: var(--muted);
      margin-top: 4px;
    }}
    .accent-blue   {{ border-top: 3px solid var(--blue); }}
    .accent-teal   {{ border-top: 3px solid var(--teal); }}
    .accent-amber  {{ border-top: 3px solid var(--amber); }}
    .accent-emerald{{ border-top: 3px solid var(--emerald); }}
    .accent-purple {{ border-top: 3px solid var(--purple); }}
    .accent-rose   {{ border-top: 3px solid var(--rose); }}
    /* ── Chart Grid ── */
    .charts {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(420px, 1fr));
      gap: 20px;
    }}
    .chart-box {{
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 20px 22px;
    }}
    .chart-box.wide {{
      grid-column: 1 / -1;
    }}
    .chart-title {{
      font-size: 13px;
      font-weight: 600;
      color: var(--text);
      margin-bottom: 16px;
    }}
    .chart-title small {{
      font-weight: 400;
      color: var(--muted);
      font-size: 11px;
      margin-left: 6px;
    }}
    canvas {{ max-height: 260px; }}
    .seg-canvas {{ max-height: 220px; }}
    footer {{
      text-align: center;
      padding: 20px;
      font-size: 11px;
      color: var(--muted);
    }}
  </style>
</head>
<body>
<header>
  <div>
    <h1>Databot <span>Growth</span> Dashboard</h1>
  </div>
  <div style="text-align:right">
    <div class="updated">Last updated: {generated_at}</div>
    <div class="refresh-note">Auto-refreshes every 5 minutes</div>
  </div>
</header>

<main>
  <!-- Stat Cards -->
  <div class="cards">
    <div class="card accent-blue">
      <div class="card-label">Channel Members</div>
      <div class="card-value">{members}</div>
      <div class="card-sub">Telegram followers</div>
    </div>
    <div class="card accent-teal">
      <div class="card-label">Today's Claims</div>
      <div class="card-value">{today_claims}</div>
      <div class="card-sub">Voucher claims (24 h)</div>
    </div>
    <div class="card accent-amber">
      <div class="card-label">Net Growth</div>
      <div class="card-value">{net_str}</div>
      <div class="card-sub">Joins minus leaves (yesterday)</div>
    </div>
    <div class="card accent-emerald">
      <div class="card-label">Total Referrals</div>
      <div class="card-value">{total_referrals}</div>
      <div class="card-sub">All-time snapshot</div>
    </div>
    <div class="card accent-purple">
      <div class="card-label">Notifications</div>
      <div class="card-value">{notif_str}</div>
      <div class="card-sub">Members with alerts on</div>
    </div>
    <div class="card accent-rose">
      <div class="card-label">Avg Views / Post</div>
      <div class="card-value">{mean_views_str}</div>
      <div class="card-sub">Avg reactions: {mean_reactions_str}</div>
    </div>
  </div>

  <!-- Charts -->
  <div class="charts">
    <!-- Referral claims trend -->
    <div class="chart-box">
      <div class="chart-title">Daily Claims <small>last 7 days</small></div>
      <canvas id="refChart"></canvas>
    </div>

    <!-- Channel activity -->
    <div class="chart-box">
      <div class="chart-title">Channel Activity <small>joins vs leaves</small></div>
      <canvas id="chChart"></canvas>
    </div>

    <!-- Content performance -->
    <div class="chart-box wide">
      <div class="chart-title">Content Performance <small>views &amp; reactions by day</small></div>
      <canvas id="ctChart"></canvas>
    </div>

    <!-- Segmentation -->
    <div class="chart-box">
      <div class="chart-title">User Segmentation <small>latest snapshot</small></div>
      <canvas id="segChart" class="seg-canvas"></canvas>
    </div>

    <!-- Net growth trend -->
    <div class="chart-box">
      <div class="chart-title">Net Channel Growth <small>last 7 days</small></div>
      <canvas id="netChart"></canvas>
    </div>
  </div>
</main>

<footer>Databot Growth Intelligence &mdash; data from MongoDB derived collections</footer>

<script>
  Chart.defaults.color = '#94a3b8';
  Chart.defaults.borderColor = '#334155';
  Chart.defaults.font.family = "system-ui, -apple-system, sans-serif";
  Chart.defaults.font.size = 12;

  const refDates   = {ref_dates};
  const refJoins   = {ref_joins};
  const chDates    = {ch_dates};
  const chJoins    = {ch_joins};
  const chLeaves   = {ch_leaves};
  const chNet      = {ch_net};
  const ctDates    = {ct_dates};
  const ctViews    = {ct_views};
  const ctReactions= {ct_reactions};
  const segLabels  = {seg_labels};
  const segValues  = {seg_values};

  const lineOpts = {{
    responsive: true,
    maintainAspectRatio: true,
    plugins: {{ legend: {{ display: false }} }},
    scales: {{
      x: {{ grid: {{ color: '#1e293b' }} }},
      y: {{ grid: {{ color: '#1e293b' }}, beginAtZero: true }},
    }},
  }};

  // Referral claims
  new Chart(document.getElementById('refChart'), {{
    type: 'line',
    data: {{
      labels: refDates,
      datasets: [{{
        label: 'Claims',
        data: refJoins,
        borderColor: '#3b82f6',
        backgroundColor: 'rgba(59,130,246,0.12)',
        fill: true,
        tension: 0.35,
        pointRadius: 4,
        pointBackgroundColor: '#3b82f6',
      }}],
    }},
    options: lineOpts,
  }});

  // Channel joins vs leaves
  new Chart(document.getElementById('chChart'), {{
    type: 'bar',
    data: {{
      labels: chDates,
      datasets: [
        {{
          label: 'Joins',
          data: chJoins,
          backgroundColor: 'rgba(20,184,166,0.7)',
          borderRadius: 4,
        }},
        {{
          label: 'Leaves',
          data: chLeaves,
          backgroundColor: 'rgba(244,63,94,0.7)',
          borderRadius: 4,
        }},
      ],
    }},
    options: {{
      responsive: true,
      maintainAspectRatio: true,
      plugins: {{ legend: {{ labels: {{ boxWidth: 12, padding: 12 }} }} }},
      scales: {{
        x: {{ grid: {{ color: '#1e293b' }}, stacked: false }},
        y: {{ grid: {{ color: '#1e293b' }}, beginAtZero: true }},
      }},
    }},
  }});

  // Content views + reactions
  new Chart(document.getElementById('ctChart'), {{
    type: 'bar',
    data: {{
      labels: ctDates,
      datasets: [
        {{
          label: 'Views',
          data: ctViews,
          backgroundColor: 'rgba(59,130,246,0.7)',
          borderRadius: 4,
          yAxisID: 'y',
        }},
        {{
          label: 'Reactions',
          data: ctReactions,
          backgroundColor: 'rgba(168,85,247,0.7)',
          borderRadius: 4,
          yAxisID: 'y1',
        }},
      ],
    }},
    options: {{
      responsive: true,
      maintainAspectRatio: true,
      plugins: {{ legend: {{ labels: {{ boxWidth: 12, padding: 12 }} }} }},
      scales: {{
        x: {{ grid: {{ color: '#1e293b' }} }},
        y: {{
          type: 'linear', position: 'left',
          grid: {{ color: '#1e293b' }}, beginAtZero: true,
          title: {{ display: true, text: 'Views', color: '#3b82f6' }},
        }},
        y1: {{
          type: 'linear', position: 'right',
          grid: {{ drawOnChartArea: false }}, beginAtZero: true,
          title: {{ display: true, text: 'Reactions', color: '#a855f7' }},
        }},
      }},
    }},
  }});

  // Segmentation donut
  new Chart(document.getElementById('segChart'), {{
    type: 'doughnut',
    data: {{
      labels: segLabels,
      datasets: [{{
        data: segValues,
        backgroundColor: ['#3b82f6','#14b8a6','#f59e0b','#f43f5e','#10b981','#64748b'],
        borderColor: '#1e293b',
        borderWidth: 2,
        hoverOffset: 6,
      }}],
    }},
    options: {{
      responsive: true,
      maintainAspectRatio: true,
      plugins: {{
        legend: {{
          position: 'right',
          labels: {{ boxWidth: 12, padding: 10 }},
        }},
      }},
    }},
  }});

  // Net growth line
  new Chart(document.getElementById('netChart'), {{
    type: 'line',
    data: {{
      labels: chDates,
      datasets: [{{
        label: 'Net Growth',
        data: chNet,
        borderColor: '#f59e0b',
        backgroundColor: 'rgba(245,158,11,0.12)',
        fill: true,
        tension: 0.35,
        pointRadius: 4,
        pointBackgroundColor: '#f59e0b',
      }}],
    }},
    options: {{
      ...lineOpts,
      scales: {{
        x: {{ grid: {{ color: '#1e293b' }} }},
        y: {{ grid: {{ color: '#1e293b' }} }},
      }},
    }},
  }});
</script>
</body>
</html>"""
