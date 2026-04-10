from __future__ import annotations

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class SourceCollections(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore")

    referral_events: str = Field("referral_events", alias="REFERRAL_EVENTS_COLLECTION")
    referral_kpi: str = Field("referral_kpis", alias="REFERRAL_KPI_COLLECTION")
    users: str = Field("users", alias="USER_COLLECTION")
    claim_events: str = Field("claim_events", alias="CLAIM_EVENTS_COLLECTION")
    post_logs: str = Field("post_logs", alias="POST_LOG_COLLECTION")
    channel_events: str = Field("channel_events", alias="CHANNEL_EVENTS_COLLECTION")


class DerivedCollections(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore")

    referral_daily: str = Field("referral_daily_summary", alias="DERIVED_REFERRAL_DAILY_COLLECTION")
    referral_weekly: str = Field("referral_weekly_summary", alias="DERIVED_REFERRAL_WEEKLY_COLLECTION")
    channel_daily: str = Field("channel_daily_summary", alias="DERIVED_CHANNEL_DAILY_COLLECTION")
    content_daily: str = Field("content_daily_summary", alias="DERIVED_CONTENT_DAILY_COLLECTION")
    inviter_daily: str = Field("referral_inviter_stats_daily", alias="DERIVED_INVITER_DAILY_COLLECTION")
    user_profile_summary: str = Field("user_profile_summary", alias="DERIVED_USER_PROFILE_COLLECTION")
    segmentation_kpis: str = Field("segmentation_daily_kpis", alias="DERIVED_SEGMENTATION_KPIS_COLLECTION")


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    app_env: str = Field(..., alias="APP_ENV")
    log_level: str = Field("INFO", alias="LOG_LEVEL")
    tz: str = Field("Asia/Kuala_Lumpur", alias="TZ")

    mongodb_uri: str = Field(..., alias="MONGODB_URI")
    mongodb_db_name: str = Field(..., alias="MONGODB_DB_NAME")

    tg_growth_bot_token: str = Field(..., alias="TG_GROWTH_BOT_TOKEN")
    tg_report_chat_id: int = Field(..., alias="TG_REPORT_CHAT_ID")
    tg_admin_user_ids: str = Field(..., alias="TG_ADMIN_USER_IDS")
    tg_channel_id: str = Field(..., alias="TG_CHANNEL_ID")

    scheduler_enabled: bool = Field(True, alias="SCHEDULER_ENABLED")
    schedule_daily_cron: str = Field("10 0 * * *", alias="SCHEDULE_DAILY_CRON")
    schedule_weekly_cron: str = Field("20 0 * * 1", alias="SCHEDULE_WEEKLY_CRON")

    # Instantiated once at startup — not re-read on every access.
    source_collections: SourceCollections = Field(default_factory=SourceCollections)
    derived_collections: DerivedCollections = Field(default_factory=DerivedCollections)

    @field_validator("log_level")
    @classmethod
    def normalize_log_level(cls, value: str) -> str:
        return value.upper()

    @property
    def admin_user_ids(self) -> list[int]:
        return [int(user_id.strip()) for user_id in self.tg_admin_user_ids.split(",") if user_id.strip()]


settings = Settings()
