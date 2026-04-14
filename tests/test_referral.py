import os

os.environ.setdefault("APP_ENV", "test")
os.environ.setdefault("MONGODB_URI", "mongodb://localhost:27017")
os.environ.setdefault("MONGODB_DB_NAME", "test_db")
os.environ.setdefault("TG_GROWTH_BOT_TOKEN", "token")
os.environ.setdefault("TG_REPORT_CHAT_ID", "-1001")
os.environ.setdefault("TG_ADMIN_USER_IDS", "1")
os.environ.setdefault("TG_CHANNEL_ID", "-1002")

from app.analytics import referral


class _FakeCollection:
    def __init__(self, rows=None, exists_fields=None):
        self._rows = rows or []
        self._exists_fields = exists_fields or set()

    def aggregate(self, _pipeline):
        return iter(self._rows)

    def find_one(self, query, _projection):
        field = next(iter(query.keys()))
        if field in self._exists_fields:
            return {"_id": 1}
        return None


class _FakeDerivedCollection:
    def find(self, *_args, **_kwargs):
        return iter([])

    def find_one(self, *_args, **_kwargs):
        return None


class _FakeMongo:
    def __init__(self, source_collections):
        self._source_collections = source_collections
        self.upserts = []
        self.bulk_operations = []

    def source(self, name):
        return self._source_collections[name]

    def derived(self, _name):
        return _FakeDerivedCollection()

    def upsert_one(self, collection, _query, document):
        self.upserts.append((collection, document))

    def bulk_upsert(self, collection, operations):
        self.bulk_operations.append((collection, operations))


def test_referral_daily_uses_referrals_created_at_when_status_fields_missing() -> None:
    mongo = _FakeMongo(
        {
            "referral_events": _FakeCollection(
                rows=[{"_id": "r1", "joins": 2}, {"_id": "r2", "joins": 1}],
                exists_fields={"created_at"},
            )
        }
    )
    result = referral.compute_referral_daily(
        mongo, referral.datetime(2026, 1, 11, tzinfo=referral.timezone.utc)
    )
    assert result["joins"] == 3
    assert result["qualified"] == 0
