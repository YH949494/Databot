# Telegram Growth Intelligence Bot

Standalone analytics and reporting service for Telegram referral growth intelligence.

## Scope and safety

This service is **read-heavy analytics/reporting only** and is intentionally decoupled from the production APReferral bot.

- Uses APReferral MongoDB data as source-of-truth input.
- Writes only analytics-safe derived collections.
- Does not issue rewards.
- Does not mutate qualification logic.
- Can be independently deployed/stopped on Fly.io.

## Architecture

- **Input sources**: configurable MongoDB source collections (APReferral-owned), optional Telegram-accessible data already collected in Mongo.
- **Processing**: idempotent daily/weekly jobs for referral, channel, and content analytics.
- **Output**: Telegram reports to operator chat.

## Repo tree

```text
app/
  analytics/
    channel.py
    content.py
    referral.py
    rules.py
  clients/
    mongo_client.py
    telegram_client.py
  config/
    settings.py
  jobs/
    pipelines.py
    scheduler.py
  reporting/
    formatter.py
  utils/
    logging.py
    time.py
  main.py
tests/
  test_rules.py
  test_reporting_formatter.py
.env.example
Dockerfile
fly.toml
requirements.txt
README.md
```

## Required environment variables

Required:
- `MONGODB_URI`
- `MONGODB_DB_NAME`
- `TG_GROWTH_BOT_TOKEN`
- `TG_REPORT_CHAT_ID`
- `TG_ADMIN_USER_IDS`
- `TG_CHANNEL_ID`
- `APP_ENV`

Optional with defaults:
- `LOG_LEVEL=INFO`
- `TZ=Asia/Kuala_Lumpur`
- `SCHEDULER_ENABLED=true`
- `SCHEDULE_DAILY_CRON=10 0 * * *` (UTC)
- `SCHEDULE_WEEKLY_CRON=20 0 * * 1` (UTC Monday)

Source collection mapping (read-only):
- `REFERRAL_EVENTS_COLLECTION`
- `REFERRAL_KPI_COLLECTION`
- `USER_COLLECTION`
- `CLAIM_EVENTS_COLLECTION`
- `POST_LOG_COLLECTION`
- `CHANNEL_EVENTS_COLLECTION`

Derived collection mapping (writes allowed):
- `DERIVED_REFERRAL_DAILY_COLLECTION`
- `DERIVED_REFERRAL_WEEKLY_COLLECTION`
- `DERIVED_CHANNEL_DAILY_COLLECTION`
- `DERIVED_CONTENT_DAILY_COLLECTION`
- `DERIVED_INVITER_DAILY_COLLECTION`

## Data model behavior

If source data is unavailable or unsupported for a metric, fields are stored as `null` and surfaced in reports conservatively.

Attribution priority:
1. direct mapping (`campaign_id`, `voucher_id`, `post_id`) when present;
2. fallback to available time-window fields if precomputed in source logs;
3. `null` when unsupported.

## Local run

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
python -m app.main --mode daily-once
python -m app.main --mode weekly-once
python -m app.main --mode scheduler
```

## Tests

```bash
python -m pytest -q
```

## Fly.io deploy

```bash
fly launch --no-deploy
fly secrets set \
  MONGODB_URI="..." \
  MONGODB_DB_NAME="..." \
  TG_GROWTH_BOT_TOKEN="..." \
  TG_REPORT_CHAT_ID="..." \
  TG_ADMIN_USER_IDS="..." \
  TG_CHANNEL_ID="..." \
  APP_ENV="production"
fly deploy
```

## Source index recommendations (document-only)

This service does **not** alter source indexes by default. If needed, coordinate with APReferral owners:

- Referral events: `(event_time)`, `(inviter_user_id, event_time)`, `(status, event_time)`
- Channel events: `(event_time)`, `(event_type, event_time)`
- Post logs: `(post_time)`, `(post_id)`

## Operational notes

- Storage timestamps are UTC.
- Reports are rendered in `TZ` (default `Asia/Kuala_Lumpur`).
- Jobs are idempotent via upsert keys (`date`, `week_start/week_end`, `date+post_id`, `date+inviter_user_id`).
