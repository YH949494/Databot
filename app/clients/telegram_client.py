from __future__ import annotations

import logging
from pathlib import Path

from aiogram import Bot
from aiogram.exceptions import TelegramAPIError
from aiogram.types import BufferedInputFile

from app.config.settings import settings

logger = logging.getLogger(__name__)

_MAX_MESSAGE_LEN = 4096


class TelegramService:
    def __init__(self) -> None:
        self.bot = Bot(token=settings.tg_growth_bot_token)

    async def send_dashboard(self, html_path: Path) -> None:
        if not html_path.exists():
            logger.warning("Dashboard HTML not found at %s — skipping send", html_path)
            return
        try:
            content = html_path.read_bytes()
            await self.bot.send_document(
                chat_id=settings.tg_report_chat_id,
                document=BufferedInputFile(content, filename="dashboard.html"),
                caption="📊 Open this file in your browser to view today's dashboard.",
            )
        except TelegramAPIError:
            logger.exception(
                "Failed to send dashboard HTML (chat_id=%s)", settings.tg_report_chat_id
            )

    async def send_report(self, text: str) -> None:
        # Truncate to Telegram's hard limit to avoid silent send failures.
        if len(text) > _MAX_MESSAGE_LEN:
            text = text[: _MAX_MESSAGE_LEN - 20] + "\n… (truncated)"
        try:
            await self.bot.send_message(
                chat_id=settings.tg_report_chat_id,
                text=text,
                disable_web_page_preview=True,
            )
        except TelegramAPIError:
            # Log and continue — a failed Telegram send must not crash the analytics pipeline.
            logger.exception(
                "Telegram report delivery failed (chat_id=%s). Report data is persisted in MongoDB.",
                settings.tg_report_chat_id,
            )

    async def close(self) -> None:
        await self.bot.session.close()
