"""Диагностика: что режим ALL видит в каталоге подарков Telegram.

Запуск из папки проекта (где лежат .env и gift_listener_session.session):

    python find_collection.py [часть названия]

Например:
    python find_collection.py троян
    python find_collection.py horse

Скрипт подключается той же сессией, что и слушатель (вход не потребуется),
выводит все подарки из каталога: название, слаг, цену, открыты ли апгрейды.
Ничего не рассылает и ничего не меняет — только читает каталог.
"""

import asyncio
import os
import sys

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.tl import functions

load_dotenv()

API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "").strip()
SESSION_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "gift_listener_session")

needle = sys.argv[1].lower() if len(sys.argv) > 1 else None


async def main() -> None:
    if not API_ID or not API_HASH:
        raise SystemExit("Заполни API_ID и API_HASH в .env")

    client = TelegramClient(SESSION_PATH, API_ID, API_HASH)
    await client.connect()
    if not await client.is_user_authorized():
        raise SystemExit("Сессия не авторизована. Сначала запусти бот и войди в аккаунт.")

    res = await client(functions.payments.GetStarGiftsRequest(hash=0))
    gifts = getattr(res, "gifts", None) or []
    print(f"Всего подарков в каталоге: {len(gifts)}\n")

    upgradable = 0
    for g in gifts:
        title = str(getattr(g, "title", "") or "(без названия)")
        slug = str(getattr(g, "slug", "") or "(без слага)")
        stars = getattr(g, "stars", None)
        upgrade_stars = getattr(g, "upgrade_stars", None)
        if upgrade_stars:
            upgradable += 1

        line = (
            f"{'✅' if upgrade_stars else '➖'} {title}\n"
            f"    slug: {slug} | цена: {stars} | апгрейд: "
            f"{upgrade_stars if upgrade_stars else 'НЕ открыт'}"
        )
        if needle is None or needle in title.lower() or needle in slug.lower():
            print(line)

    print(f"\nМожно апгрейдить: {upgradable} из {len(gifts)}")
    if needle:
        print(f"Фильтр по '{needle}' применён. Запусти без аргумента, чтобы увидеть весь каталог.")
    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
