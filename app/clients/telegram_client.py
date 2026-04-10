from __future__ import annotations

import logging

from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest

from app.config.settings import settings

logger = logging.getLogger(__name__)


class TelegramService:
    def __init__(self) -> None:
        self.bot = Bot(token=settings.tg_growth_bot_token)

    async def send_report(self, text: str) -> None:
        try:
            await self.bot.send_message(chat_id=settings.tg_report_chat_id, text=text, disable_web_page_preview=True)
        except TelegramBadRequest:
            logger.exception("Telegram API rejected report message")
            raise

    async def close(self) -> None:
        await self.bot.session.close()
