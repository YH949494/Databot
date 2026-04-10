from __future__ import annotations

import argparse
import asyncio
import logging

from app.clients.mongo_client import MongoService
from app.clients.telegram_client import TelegramService
from app.jobs.pipelines import run_daily_pipeline, run_weekly_pipeline
from app.jobs.scheduler import run_forever
from app.utils.logging import setup_logging
from app.config.settings import settings

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Telegram Growth Intelligence Bot")
    parser.add_argument(
        "--mode",
        choices=["scheduler", "daily-once", "weekly-once"],
        default="scheduler",
        help="scheduler: run recurring jobs; daily-once/weekly-once: one-shot runs",
    )
    return parser.parse_args()


async def _run_once(mode: str) -> None:
    mongo = MongoService()
    mongo.setup_derived_indexes()
    telegram = TelegramService()
    try:
        if mode == "daily-once":
            await run_daily_pipeline(mongo, telegram)
        elif mode == "weekly-once":
            await run_weekly_pipeline(mongo, telegram)
    finally:
        await telegram.close()


def main() -> None:
    setup_logging(settings.log_level)
    args = parse_args()
    logger.info("Starting growth intelligence service in mode=%s", args.mode)
    if args.mode == "scheduler":
        asyncio.run(run_forever())
    else:
        asyncio.run(_run_once(args.mode))


if __name__ == "__main__":
    main()
