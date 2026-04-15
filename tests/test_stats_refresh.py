import asyncio
import importlib
import inspect
import sys
import types
from datetime import datetime, timezone


def _install_fake_settings_module() -> None:
    settings_obj = types.SimpleNamespace(
        tg_channel_id="-100123",
        tz="Asia/Kuala_Lumpur",
        schedule_daily_cron="10 0 * * *",
        schedule_weekly_cron="20 0 * * 1",
        source_collections=types.SimpleNamespace(
            post_logs="post_logs",
            channel_events="channel_events",
            channel_stats_overview="channel_stats_overview",
        ),
    )
    fake_settings = types.ModuleType("app.config.settings")
    fake_settings.settings = settings_obj
    sys.modules["app.config.settings"] = fake_settings


def _reload(module_name: str):
    sys.modules.pop(module_name, None)
    return importlib.import_module(module_name)


class _FakeCollection:
    def __init__(self, docs=None):
        self.docs = docs or []
        self.last_update = None

    def find(self, query, _projection):
        chat_id = query["chat_id"]
        start = query["post_time"]["$gte"]
        end = query["post_time"]["$lte"]
        return [
            d for d in self.docs
            if d.get("chat_id") == chat_id and start <= d.get("post_time") <= end
        ]

    def update_one(self, flt, update, upsert=False):
        self.last_update = {"filter": flt, "update": update, "upsert": upsert}


class _FakeMongo:
    def __init__(self, post_docs=None):
        self.source_db = {
            "post_logs": _FakeCollection(post_docs),
            "channel_events": _FakeCollection(),
            "channel_stats_overview": _FakeCollection(),
        }


class _FakeBot:
    async def get_chat(self, chat_id):
        return types.SimpleNamespace(type="channel", title="Test Channel", username="testchan")

    async def get_chat_member_count(self, chat_id):
        return 1234

    async def get_chat_administrators(self, chat_id):
        return [1, 2]


def test_unsupported_get_chat_statistics_path_removed():
    _install_fake_settings_module()
    mod = _reload("app.collectors.stats_refresh")
    source = inspect.getsource(mod)
    assert "get_chat_statistics" not in source
    assert "get_message_statistics" not in source


def test_weekly_scheduler_registration():
    _install_fake_settings_module()
    mod = _reload("app.jobs.scheduler")

    class _FakeScheduler:
        def __init__(self, timezone):
            self.jobs = []

        def add_job(self, func, trigger, args=None, max_instances=None, id=None):
            self.jobs.append({"func": func, "trigger": trigger, "args": args, "id": id})

        def start(self):
            return None

    mod.AsyncIOScheduler = _FakeScheduler
    scheduler = mod.start_scheduler(mongo=object(), telegram=types.SimpleNamespace(bot=object()))
    weekly = [j for j in scheduler.jobs if j["id"] == "stats_refresh_weekly"][0]
    assert str(weekly["trigger"].timezone) == "Asia/Kuala_Lumpur"
    assert str(weekly["trigger"].fields[4]) == "sun"
    assert str(weekly["trigger"].fields[6]) == "55"
    assert str(weekly["trigger"].fields[5]) == "23"


def test_week_window_calculation_uses_asia_kuala_lumpur():
    _install_fake_settings_module()
    mod = _reload("app.collectors.stats_refresh")
    ref = datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc)
    week_start_local, week_end_local, week_start_utc, week_end_utc = mod._week_window_kl(ref)
    assert week_start_local.isoformat() == "2026-04-13T00:00:00+08:00"
    assert week_end_local.isoformat() == "2026-04-19T23:59:59.999999+08:00"
    assert week_start_utc.isoformat() == "2026-04-12T16:00:00+00:00"
    assert week_end_utc.isoformat() == "2026-04-19T15:59:59.999999+00:00"


def test_weekly_aggregation_only_counts_monday_to_sunday_posts():
    _install_fake_settings_module()
    mod = _reload("app.collectors.stats_refresh")

    # Freeze week to KL week of 2026-04-13 .. 2026-04-19.
    mod._utcnow = lambda: datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc)

    inside = datetime(2026, 4, 13, 1, 0, tzinfo=timezone.utc)
    before = datetime(2026, 4, 12, 15, 59, tzinfo=timezone.utc)
    after = datetime(2026, 4, 19, 16, 0, tzinfo=timezone.utc)

    mongo = _FakeMongo(
        post_docs=[
            {"post_id": 1, "chat_id": -100123, "post_time": inside, "views": 10, "shares": 2, "reactions": 1},
            {"post_id": 2, "chat_id": -100123, "post_time": before, "views": 20, "shares": 3, "reactions": 2},
            {"post_id": 3, "chat_id": -100123, "post_time": after, "views": 30, "shares": 4, "reactions": 3},
        ]
    )
    asyncio.run(mod.fetch_message_stats(_FakeBot(), mongo))

    written = mongo.source_db["channel_stats_overview"].last_update["update"]["$set"]
    assert written["post_count"] == 1
    assert written["views"] == 10
    assert written["shares"] == 2
    assert written["reactions"] == 1
