"""Пример самостоятельного запуска paid_gifts.py.

Если у тебя уже есть main.py с ботом и Mini App, не заменяй его этим файлом.
Смотри инструкцию в README.md и подключи модуль к существующим bot и dp.
"""

import asyncio
import logging
import os

from aiogram import Bot, Dispatcher
from aiogram.filters import CommandStart
from aiogram.types import Message
from dotenv import load_dotenv

from paid_gifts import register_paid_gifts, start_gift_listener


load_dotenv()
logging.basicConfig(level=logging.INFO)


async def main() -> None:
    token = os.getenv("BOT_TOKEN", "").strip()
    if not token or token == "вставь_токен_от_BotFather":
        raise RuntimeError("Заполни BOT_TOKEN в файле .env")

    bot = Bot(token=token)
    dp = Dispatcher()

    @dp.message(CommandStart())
    async def start(message: Message) -> None:
        await message.answer(
            "Привет! Это лента новых апгрейдов подарков.\n\n"
            "/subscribe — оформить подписку за Stars\n"
            "/gifts — последние обнаруженные апгрейды\n"
            "/status — статус подписки\n"
            "/cancel_sub — отключить автопродление\n"
            "/resume_sub — включить автопродление"
        )

    register_paid_gifts(dp, bot)
    telethon_client = None

    try:
        # Первый запуск попросит номер Telegram-аккаунта, код и, если включён,
        # пароль двухэтапной проверки. Не передавай эти данные никому.
        telethon_client = await start_gift_listener(bot)
        await dp.start_polling(bot)
    finally:
        if telethon_client is not None:
            await telethon_client.disconnect()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
