"""
Telegram Bot Adapter implementation.

Uses aiogram 3.x to send messages, show chat actions, and manage messages.
Fits the BotAdapter Protocol.
"""
import logging
from aiogram import Bot
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramAPIError

from relict_core.config.adapter_interface import BaseAdapter
from relict_core.config.relict_settings import AdapterSettings
from relict_core.config.exceptions import AdapterError

logger = logging.getLogger(__name__)


class TelegramAdapter(BaseAdapter):
    """
    Adapter for Telegram API via aiogram 3.x.

    Attributes:
        bot: Configured aiogram Bot instance.
    """

    def __init__(self, opts: AdapterSettings):
        if not opts.bot_token:
            raise ValueError("Telegram bot token is missing!")

        self.bot = Bot(
            token=opts.bot_token,
            default=DefaultBotProperties(parse_mode=ParseMode.HTML)
        )
        logger.info("TelegramAdapter initialized.")

    async def send_message(self, chat_id: int | str, text: str) -> None:
        """Send a text message to a specific chat."""
        try:
            await self.bot.send_message(chat_id=chat_id, text=text)
            logger.debug(f"Message sent to chat {chat_id}")
        except TelegramAPIError as e:
            logger.error(f"Failed to send message to {chat_id}. Telegram API Error: {e}")
            raise AdapterError(f"Telegram API Error: {e}") from e

    async def send_typing(self, chat_id: int | str) -> None:
        """Show 'typing...' indicator in the chat."""
        try:
            await self.bot.send_chat_action(chat_id=chat_id, action="typing")
        except TelegramAPIError as e:
            logger.warning(f"Failed to send typing action to {chat_id}: {e}")


    async def close(self) -> None:
        """Gracefully close the aiohttp session. Safe to call multiple times."""
        if self.bot.session:
            await self.bot.session.close()
            logger.info("TelegramAdapter session closed.")