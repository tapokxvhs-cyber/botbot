"""Paid Telegram gift-upgrade feed for an existing aiogram 3 bot.

The bot handles Stars subscriptions and exposes /gifts to active subscribers.
A separate Telethon user session listens for MessageActionStarGiftUnique updates
that are delivered to that Telegram account.

This is NOT a global Telegram gift feed: the listener sees only updates that
Telegram delivers to the connected account in chats it can access.
"""

from __future__ import annotations

import asyncio
import html
import logging
import os
import re
import sqlite3
import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    LabeledPrice,
    Message,
    PreCheckoutQuery,
)
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
API_ID_RAW = os.getenv("API_ID", "").strip()
API_HASH = os.getenv("API_HASH", "").strip()

try:
    API_ID = int(API_ID_RAW) if API_ID_RAW else 0
except ValueError:
    API_ID = 0

# ID владельца бота: для него подписка всегда активна (без оплаты Stars).
OWNER_ID_RAW = os.getenv("OWNER_ID", "").strip()
try:
    OWNER_ID = int(OWNER_ID_RAW) if OWNER_ID_RAW else 0
except ValueError:
    OWNER_ID = 0

try:
    SUBSCRIPTION_STARS = int(os.getenv("SUBSCRIPTION_STARS", "100"))
except ValueError:
    SUBSCRIPTION_STARS = 100

SUBSCRIPTION_PERIOD = 30 * 24 * 60 * 60  # Telegram requires 30 days.
INVOICE_PAYLOAD = "gift_upgrade_feed_monthly_v1"
DB_PATH = BASE_DIR / "paid_gifts.sqlite3"
SESSION_PATH = BASE_DIR / "gift_listener_session"

# GIFT_DEBUG=1 в .env -> логировать вообще все служебные сообщения (для диагностики).
GIFT_DEBUG = os.getenv("GIFT_DEBUG", "0").strip() == "1"
# Необязательный опрос по слагам коллекций, например: GIFT_PROBE_SLUGS=PlushPepe,DurovsCap
# или GIFT_PROBE_SLUGS=ALL — тогда бот сам найдёт все коллекции с upgrade_stars.
PROBE_SLUGS = [s.strip() for s in os.getenv("GIFT_PROBE_SLUGS", "").split(",") if s.strip()]
try:
    PROBE_INTERVAL = max(5, int(os.getenv("GIFT_PROBE_INTERVAL", "20")))
except ValueError:
    PROBE_INTERVAL = 20
try:  # пауза между запросами к Telegram (защита от FloodWait)
    PROBE_CALL_DELAY = max(0.0, float(os.getenv("GIFT_PROBE_CALL_DELAY", "0.5")))
except ValueError:
    PROBE_CALL_DELAY = 0.5
try:  # режим ALL: пауза между коллекциями
    PROBE_ALL_DELAY = max(0.0, float(os.getenv("GIFT_PROBE_ALL_DELAY", "2")))
except ValueError:
    PROBE_ALL_DELAY = 2.0

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("paid-gifts")

if not 1 <= SUBSCRIPTION_STARS <= 10_000:
    raise ValueError("SUBSCRIPTION_STARS должен быть от 1 до 10000")

# Настройки, которые владелец меняет командой /set без перезапуска.
# ключ: (описание, минимум, максимум, значение по умолчанию)
SETTINGS: dict[str, tuple[str, int, int, int]] = {
    "stars": ("цена подписки, Stars (для новых счетов)", 1, 10_000, SUBSCRIPTION_STARS),
    "trial_hours": ("длина пробного периода, часов", 1, 720, 24),
    "probe_interval": ("интервал опроса коллекций, секунд", 5, 3600, PROBE_INTERVAL),
    "notify_max_age": ("не рассылать апгрейды старше N секунд (защита от потока после перезапуска)", 0, 86_400, 600),
}
# Первый проход опроса после запуска идёт «тихо»: только запоминаем, без рассылки.
_probe_warm: set[str] = set()


def db_connect() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con


def init_db() -> None:
    with db_connect() as con:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS subscriptions (
                user_id INTEGER PRIMARY KEY,
                expires_at INTEGER NOT NULL,
                charge_id TEXT NOT NULL,
                auto_renew INTEGER NOT NULL DEFAULT 1
            )
            """
        )

        # Таблица для учёта использованного пробного периода
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS trial_users (
                user_id INTEGER PRIMARY KEY,
                used_at INTEGER NOT NULL
            )
            """
        )

        # Все, кто хоть раз написал /start
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS start_users (
                user_id INTEGER PRIMARY KEY,
                first_seen INTEGER NOT NULL
            )
            """
        )
        # Те, кто уже есть в подписках/пробных, точно запускали бота — добавим их
        con.execute(
            "INSERT OR IGNORE INTO start_users(user_id, first_seen) "
            "SELECT user_id, used_at FROM trial_users"
        )
        con.execute(
            "INSERT OR IGNORE INTO start_users(user_id, first_seen) "
            "SELECT user_id, CAST(strftime('%s','now') AS INTEGER) FROM subscriptions"
        )

        con.execute(
            "CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value INTEGER NOT NULL)"
        )

        con.execute(
            """
            CREATE TABLE IF NOT EXISTS gift_probe (
                prefix TEXT PRIMARY KEY,
                last_num INTEGER NOT NULL
            )
            """
        )

        con.execute(
            """
            CREATE TABLE IF NOT EXISTS gifts (
                gift_key TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                gift_number INTEGER,
                slug TEXT,
                url TEXT,
                attributes TEXT,
                created_at INTEGER NOT NULL
            )
            """
        )


def get_setting(key: str) -> int:
    default = SETTINGS[key][3]
    try:
        with db_connect() as con:
            row = con.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
        return int(row["value"]) if row else default
    except sqlite3.Error:
        return default


def set_setting(key: str, value: int) -> None:
    with db_connect() as con:
        con.execute(
            "INSERT INTO settings(key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )


def get_subscription(user_id: int) -> sqlite3.Row | None:
    with db_connect() as con:
        return con.execute(
            "SELECT * FROM subscriptions WHERE user_id = ?", (user_id,)
        ).fetchone()


def is_owner(user_id: int) -> bool:
    """Владелец бота: доступ всегда активен, без Stars и без записи подписки."""
    return bool(OWNER_ID) and int(user_id) == OWNER_ID


def has_access(user_id: int) -> bool:
    if is_owner(user_id):
        return True

    row = get_subscription(user_id)
    return bool(row and int(row["expires_at"]) > int(time.time()))


def save_subscription(user_id: int, expires_at: int, charge_id: str) -> None:
    with db_connect() as con:
        con.execute(
            """
            INSERT INTO subscriptions(user_id, expires_at, charge_id, auto_renew)
            VALUES (?, ?, ?, 1)
            ON CONFLICT(user_id) DO UPDATE SET
                expires_at = excluded.expires_at,
                charge_id = excluded.charge_id,
                auto_renew = 1
            """,
            (user_id, expires_at, charge_id),
        )


def start_trial(user_id: int) -> bool:
    now = int(time.time())
    expires_at = now + get_setting("trial_hours") * 60 * 60

    with db_connect() as con:
        cursor = con.execute(
            """
            INSERT OR IGNORE INTO trial_users (user_id, used_at)
            VALUES (?, ?)
            """,
            (user_id, now),
        )
        print("TRIAL INSERT ROWCOUNT:", cursor.rowcount)

        if cursor.rowcount == 0:
            return False

        con.execute(
            """
            INSERT INTO subscriptions
                (user_id, expires_at, charge_id, auto_renew)
            VALUES (?, ?, 'trial', 0)
            ON CONFLICT(user_id) DO UPDATE SET
                expires_at = excluded.expires_at,
                charge_id = 'trial',
                auto_renew = 0
            """,
            (user_id, expires_at),
        )

    return True


def set_auto_renew(user_id: int, enabled: bool) -> None:
    with db_connect() as con:
        con.execute(
            "UPDATE subscriptions SET auto_renew = ? WHERE user_id = ?",
            (1 if enabled else 0, user_id),
        )


def save_gift(gift: Any, message_date: Any = None) -> tuple[bool, dict[str, Any]]:
    """Save one unique-gift object. Return (was_new, display_data)."""
    slug = str(getattr(gift, "slug", "") or "").strip()
    gift_id = str(getattr(gift, "id", "") or slug).strip()
    if not gift_id:
        log.warning("[save] у подарка нет ни id, ни slug — пропуск (тип объекта: %s)", type(gift).__name__)
        return False, {}

    title = str(getattr(gift, "title", "Коллекционный подарок") or "Коллекционный подарок")
    number_raw = getattr(gift, "num", None)
    try:
        number = int(number_raw) if number_raw is not None else None
    except (TypeError, ValueError):
        number = None

    names: list[str] = []
    for attribute in (getattr(gift, "attributes", None) or []):
        name = getattr(attribute, "name", None) or getattr(attribute, "title", None)
        if name:
            names.append(str(name))
    attributes_text = ", ".join(names[:8])

    if message_date is not None and hasattr(message_date, "timestamp"):
        created_at = int(message_date.timestamp())
    else:
        created_at = int(time.time())

    url = f"https://t.me/nft/{slug}" if slug else ""
    with db_connect() as con:
        cursor = con.execute(
            """
            INSERT OR IGNORE INTO gifts
                (gift_key, title, gift_number, slug, url, attributes, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (gift_id, title, number, slug, url, attributes_text, created_at),
        )
        is_new = cursor.rowcount > 0

    log.info(
        "[save] данные извлечены: title=%r num=%s slug=%r attrs=%r -> %s",
        title, number, slug, attributes_text,
        "записано в БД" if is_new else "уже было в БД",
    )
    return is_new, {
        "title": title,
        "number": number,
        "slug": slug,
        "url": url,
        "attributes": attributes_text,
        "created_at": created_at,
    }


def get_recent_gifts(limit: int = 15) -> list[sqlite3.Row]:
    with db_connect() as con:
        return con.execute(
            "SELECT * FROM gifts ORDER BY created_at DESC LIMIT ?", (limit,)
        ).fetchall()


def get_active_subscribers() -> list[int]:
    now = int(time.time())
    with db_connect() as con:
        rows = con.execute(
            "SELECT user_id FROM subscriptions WHERE expires_at > ?", (now,)
        ).fetchall()

    ids = [int(row["user_id"]) for row in rows]

    # Владелец всегда получает уведомления, даже без записи в subscriptions.
    if OWNER_ID and OWNER_ID not in ids:
        ids.append(OWNER_ID)

    return ids


def register_start_user(user_id: int) -> None:
    with db_connect() as con:
        con.execute(
            "INSERT OR IGNORE INTO start_users(user_id, first_seen) VALUES (?, ?)",
            (user_id, int(time.time())),
        )


def get_subscription_stats() -> dict[str, int]:
    """Сводка по подпискам: активные платные, активные пробные, истёкшие, всего."""
    now = int(time.time())
    with db_connect() as con:
        total = con.execute("SELECT COUNT(*) FROM subscriptions").fetchone()[0]
        active = con.execute(
            "SELECT COUNT(*) FROM subscriptions WHERE expires_at > ?", (now,)
        ).fetchone()[0]
        active_paid = con.execute(
            "SELECT COUNT(*) FROM subscriptions "
            "WHERE expires_at > ? AND charge_id != 'trial'", (now,)
        ).fetchone()[0]
        active_trial = con.execute(
            "SELECT COUNT(*) FROM subscriptions "
            "WHERE expires_at > ? AND charge_id = 'trial'", (now,)
        ).fetchone()[0]
        expired = con.execute(
            "SELECT COUNT(*) FROM subscriptions WHERE expires_at <= ?", (now,)
        ).fetchone()[0]
        trial_used = con.execute("SELECT COUNT(*) FROM trial_users").fetchone()[0]
        gifts_count = con.execute("SELECT COUNT(*) FROM gifts").fetchone()[0]
        started = con.execute("SELECT COUNT(*) FROM start_users").fetchone()[0]

    return {
        "started": int(started),
        "total": int(total),
        "active": int(active),
        "active_paid": int(active_paid),
        "active_trial": int(active_trial),
        "expired": int(expired),
        "trial_used": int(trial_used),
        "gifts": int(gifts_count),
    }


def gift_message(data: dict[str, Any]) -> str:
    title = html.escape(str(data.get("title") or "Коллекционный подарок"))
    number = data.get("number")
    number_line = f"Номер: <b>#{number}</b>\n" if number is not None else ""
    attributes = html.escape(str(data.get("attributes") or ""))
    attributes_line = f"Характеристики: {attributes}\n" if attributes else ""
    url = data.get("url") or ""
    link_line = f'Открыть: <a href="{html.escape(url, quote=True)}">подарок в Telegram</a>' if url else ""
    return (
        "🆕 <b>Новый апгрейд подарка</b>\n\n"
        f"Подарок: <b>{title}</b>\n"
        f"{number_line}"
        f"{attributes_line}"
        f"{link_line}"
    )


def register_paid_gifts(dp: Dispatcher, bot: Bot) -> None:
    """Register handlers on the EXISTING Dispatcher. Do not create another bot."""
    init_db()
    log.info("[db] файл: %s, подарков в базе: %s", DB_PATH, len(get_recent_gifts(10_000)))
    if OWNER_ID:
        log.info("[owner] владелец бота: %s (бессрочный доступ, без Stars)", OWNER_ID)

    startup_stats = get_subscription_stats()
    log.info(
        "[stats] платных активных: %d, пробных активных: %d, истёкших: %d, "
        "пробных всего: %d, подарков: %d",
        startup_stats["active_paid"], startup_stats["active_trial"], startup_stats["expired"],
        startup_stats["trial_used"], startup_stats["gifts"],
    )

    # Считаем всех, кто написал /start (middleware срабатывает до любых хендлеров,
    # поэтому работает и с твоим существующим обработчиком /start).
    async def count_start_middleware(handler, event, data):
        try:
            text = (event.text or "").strip()
            if event.from_user and (text == "/start" or text.startswith(("/start ", "/start@"))):
                register_start_user(event.from_user.id)
        except Exception:
            log.exception("Не удалось записать /start")
        return await handler(event, data)

    dp.message.outer_middleware(count_start_middleware)

    @dp.message(Command("subscribe"))
    async def subscribe_command(message: Message) -> None:
        user_id = message.from_user.id

        if is_owner(user_id):
            await message.answer(
                "👑 У тебя доступ владельца: бессрочный.\n"
                "Подписка и оплата Stars не требуются."
            )
            return

        row = get_subscription(user_id)
        now = int(time.time())

        if row and int(row["expires_at"]) > now:
            expires = datetime.fromtimestamp(
                int(row["expires_at"])
            ).strftime("%d.%m.%Y %H:%M")

            if row["charge_id"] == "trial":
                await message.answer(
                    f"🎁 Бесплатный пробный доступ действует до {expires}."
                )
            elif int(row["auto_renew"]):
                await message.answer(
                    f"✅ Подписка активна до {expires}. "
                    "Автопродление включено."
                )
            else:
                await message.answer(
                    f"✅ Доступ активен до {expires}. "
                    "Автопродление выключено."
                )
            return

        if start_trial(user_id):
            expires = datetime.fromtimestamp(
                now + get_setting("trial_hours") * 60 * 60
            ).strftime("%d.%m.%Y %H:%M")

            await message.answer(
                f"🎁 Ты получил бесплатный пробный доступ на {get_setting('trial_hours')} ч.!\n\n"
                f"Доступ действует до {expires}.\n"
                "После окончания можно оформить подписку за Stars."
            )
            return

        try:
            invoice_link = await bot.create_invoice_link(
                title="Лента апгрейдов",
                description="Доступ к списку новых апгрейдов на 30 дней.",
                payload=INVOICE_PAYLOAD,
                provider_token="",
                currency="XTR",
                prices=[
                    LabeledPrice(
                        label="Доступ на 30 дней",
                        amount=get_setting("stars"),
                    )
                ],
                subscription_period=SUBSCRIPTION_PERIOD,
            )

            keyboard = InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(
                            text=f"⭐ Купить за {get_setting('stars')} Stars",
                            url=invoice_link,
                        )
                    ]
                ]
            )

            await message.answer(
                "🎁 Бесплатный пробный период уже использован.\n\n"
                f"Подписка на 30 дней: {get_setting('stars')} Stars.",
                reply_markup=keyboard,
            )

        except Exception:
            log.exception("Не удалось создать ссылку оплаты")
            await message.answer(
                "Не удалось создать счёт. Попробуй позже."
            )

    @dp.pre_checkout_query(F.invoice_payload == INVOICE_PAYLOAD)
    async def process_pre_checkout(query: PreCheckoutQuery) -> None:
        if query.currency != "XTR" or query.total_amount != get_setting("stars"):
            await query.answer(
                ok=False,
                error_message="Не удалось проверить стоимость подписки. Попробуй открыть счёт заново.",
            )
            return
        await query.answer(ok=True)

    @dp.message(F.successful_payment.invoice_payload == INVOICE_PAYLOAD)
    async def successful_payment(message: Message) -> None:
        payment = message.successful_payment
        if payment.currency != "XTR":
            await message.answer("Платёж получен в неожиданной валюте. Напиши администратору.")
            return

        expires_at = payment.subscription_expiration_date
        if not expires_at:
            # Fallback for older Bot API/library responses; the invoice itself is recurring.
            expires_at = int(time.time()) + SUBSCRIPTION_PERIOD

        save_subscription(
            user_id=message.from_user.id,
            expires_at=int(expires_at),
            charge_id=payment.telegram_payment_charge_id,
        )
        expires_text = datetime.fromtimestamp(int(expires_at)).strftime("%d.%m.%Y %H:%M")
        await message.answer(
            "🎉 Оплата подтверждена!\n"
            f"Доступ открыт до {expires_text}.\n\n"
            "Новые апгрейды смотри командой /gifts.\n"
            "Автопродление: команда /cancel_sub отключает следующие списания, "
            "но текущий доступ сохранится до указанной даты."
        )

    @dp.message(Command("gifts"))
    async def gifts_command(message: Message) -> None:
        if not has_access(message.from_user.id):
            await message.answer(
                "🔒 Список доступен по подписке.\n"
                f"Стоимость: {get_setting('stars')} Stars за 30 дней.\n"
                "Оформить: /subscribe"
            )
            return

        gifts = get_recent_gifts()
        if not gifts:
            await message.answer(
                "Пока нет обнаруженных апгрейдов.\n"
                "Список пополняется событиями, которые получает подключённый Telegram-аккаунт."
            )
            return

        await message.answer(f"🎁 <b>Последние апгрейды ({len(gifts)})</b>")
        for row in gifts:
            data = {
                "title": row["title"],
                "number": row["gift_number"],
                "url": row["url"],
                "attributes": row["attributes"],
            }
            await message.answer(gift_message(data), disable_web_page_preview=False)

    @dp.message(Command("status"))
    async def status_command(message: Message) -> None:
        if is_owner(message.from_user.id):
            stats = get_subscription_stats()
            await message.answer(
                "👑 Доступ владельца: бессрочный.\n\n"
                "📊 <b>Статистика подписок</b>\n"
                f"• Нажали /start: <b>{stats['started']}</b>\n"
                f"• Бесплатных (пробный активен): <b>{stats['active_trial']}</b>\n"
                f"• Платных (активных): <b>{stats['active_paid']}</b>\n"
                f"• Истёкших: {stats['expired']}\n"
                f"• Всего записей о подписках: {stats['total']}\n"
                f"• Пробных периодов активировано за всё время: {stats['trial_used']}\n\n"
                f"🎁 Подарков в базе: <b>{stats['gifts']}</b>"
            )
            return

        row = get_subscription(message.from_user.id)
        if not row or int(row["expires_at"]) <= int(time.time()):
            await message.answer("Подписка не активна. Оформить: /subscribe")
            return
        expires = datetime.fromtimestamp(int(row["expires_at"])).strftime("%d.%m.%Y %H:%M")
        renewal = "включено" if int(row["auto_renew"]) else "выключено"
        await message.answer(f"✅ Доступ до {expires}.\nАвтопродление: {renewal}.")

    @dp.message(Command("info"))
    async def info_command(message: Message) -> None:
        if not is_owner(message.from_user.id):
            await message.answer("Команда доступна только владельцу.")
            return

        stats = get_subscription_stats()
        text = (
            "📊 <b>Сводка по боту</b>\n\n"
            "<b>Подписки</b>\n"
            f"• Нажали /start: <b>{stats['started']}</b>\n"
            f"• Бесплатных (пробный активен): <b>{stats['active_trial']}</b>\n"
            f"• Платных (активных): <b>{stats['active_paid']}</b>\n"
            f"• Истёкших: {stats['expired']}\n"
            f"• Всего записей: {stats['total']}\n"
            f"• Пробных активировано всего: {stats['trial_used']}\n\n"
            "<b>Подарки</b>\n"
            f"• Сохранено апгрейдов в БД: {stats['gifts']}\n"
        )
        if OWNER_ID:
            text += f"\n👑 Владелец: {OWNER_ID} (бессрочный доступ)"
        await message.answer(text)

    @dp.message(Command("subscribers", "подписчики"))
    async def subscribers_command(message: Message) -> None:
        if not is_owner(message.from_user.id):
            return  # для остальных команда молчит

        stats = get_subscription_stats()
        now = int(time.time())
        with db_connect() as con:
            paid = con.execute(
                "SELECT user_id, expires_at, auto_renew FROM subscriptions "
                "WHERE expires_at > ? AND charge_id != 'trial' ORDER BY expires_at",
                (now,),
            ).fetchall()
            trial = con.execute(
                "SELECT user_id, expires_at FROM subscriptions "
                "WHERE expires_at > ? AND charge_id = 'trial' ORDER BY expires_at",
                (now,),
            ).fetchall()

        def fmt(ts: int) -> str:
            return datetime.fromtimestamp(int(ts)).strftime("%d.%m.%Y %H:%M")

        lines = [
            "👥 <b>Подписчики</b>\n",
            f"• Нажали /start: <b>{stats['started']}</b>",
            f"• Бесплатных (пробный активен): <b>{stats['active_trial']}</b>",
            f"• Платных (активных): <b>{stats['active_paid']}</b>",
            f"• Пробных взято за всё время: {stats['trial_used']}",
            f"• Истёкших: {stats['expired']}",
        ]
        if paid:
            lines.append("\n⭐ <b>Платные:</b>")
            for r in paid:
                renew = "авто" if int(r["auto_renew"]) else "без авто"
                lines.append(f"<code>{r['user_id']}</code> — до {fmt(r['expires_at'])} ({renew})")
        if trial:
            lines.append("\n🎁 <b>Бесплатные (пробный):</b>")
            for r in trial:
                lines.append(f"<code>{r['user_id']}</code> — до {fmt(r['expires_at'])}")

        text = "\n".join(lines)
        for i in range(0, len(text), 4000):  # лимит Telegram 4096 символов
            await message.answer(text[i:i + 4000])

    @dp.message(Command("settings"))
    async def settings_command(message: Message) -> None:
        if not is_owner(message.from_user.id):
            return
        lines = ["⚙️ <b>Настройки</b> (менять: /set имя значение)\n"]
        for key, (desc, lo, hi, default) in SETTINGS.items():
            lines.append(f"<code>{key}</code> = <b>{get_setting(key)}</b> — {desc} [{lo}–{hi}, по умолчанию {default}]")
        await message.answer("\n".join(lines))

    @dp.message(Command("set"))
    async def set_command(message: Message) -> None:
        if not is_owner(message.from_user.id):
            return
        parts = (message.text or "").split()
        if len(parts) != 3 or parts[1] not in SETTINGS:
            await message.answer("Формат: /set имя значение\nСписок: /settings")
            return
        key = parts[1]
        _, lo, hi, _ = SETTINGS[key]
        try:
            value = int(parts[2])
        except ValueError:
            await message.answer("Значение должно быть целым числом.")
            return
        if not lo <= value <= hi:
            await message.answer(f"Допустимо от {lo} до {hi}.")
            return
        set_setting(key, value)
        await message.answer(f"✅ {key} = <b>{value}</b>. Применено сразу, перезапуск не нужен.")

    @dp.message(Command("cancel_sub"))
    async def cancel_subscription(message: Message) -> None:
        if is_owner(message.from_user.id):
            await message.answer(
                "👑 Для владельца автопродление не требуется — доступ бессрочный."
            )
            return

        row = get_subscription(message.from_user.id)
        if not row or int(row["expires_at"]) <= int(time.time()):
            await message.answer("Активной подписки не найдено.")
            return
        if not int(row["auto_renew"]):
            await message.answer("Автопродление уже выключено. Доступ действует до конца оплаченного периода.")
            return
        try:
            await bot.edit_user_star_subscription(
                user_id=message.from_user.id,
                telegram_payment_charge_id=str(row["charge_id"]),
                is_canceled=True,
            )
            set_auto_renew(message.from_user.id, False)
            expires = datetime.fromtimestamp(int(row["expires_at"])).strftime("%d.%m.%Y %H:%M")
            await message.answer(
                "Автопродление отключено. Новых списаний не будет; "
                f"доступ сохранится до {expires}."
            )
        except Exception:
            log.exception("Failed to cancel Stars subscription")
            await message.answer(
                "Не получилось отключить автопродление через Telegram. "
                "Проверь подписку в настройках Telegram и попробуй ещё раз."
            )

    @dp.message(Command("resume_sub"))
    async def resume_subscription(message: Message) -> None:
        if is_owner(message.from_user.id):
            await message.answer(
                "👑 Для владельца автопродление не требуется — доступ бессрочный."
            )
            return

        row = get_subscription(message.from_user.id)
        if not row or int(row["expires_at"]) <= int(time.time()):
            await message.answer("Активного оплаченного периода нет. Оформить: /subscribe")
            return
        if int(row["auto_renew"]):
            await message.answer("Автопродление уже включено.")
            return
        try:
            await bot.edit_user_star_subscription(
                user_id=message.from_user.id,
                telegram_payment_charge_id=str(row["charge_id"]),
                is_canceled=False,
            )
            set_auto_renew(message.from_user.id, True)
            await message.answer("Автопродление снова включено.")
        except Exception:
            log.exception("Failed to resume Stars subscription")
            await message.answer("Не получилось включить автопродление. Попробуй позже.")


async def _login_by_qr(client: Any) -> bool:
    """Log in by scanning a QR code from the Telegram app. Returns True on success."""
    from telethon.errors import SessionPasswordNeededError

    qr = await client.qr_login()
    print("\nВход по QR-коду.")
    print("На телефоне: Настройки → Устройства → Подключить устройство → наведи камеру на QR.")
    try:
        import qrcode  # type: ignore

        code = qrcode.QRCode(border=1)
        code.add_data(qr.url)
        code.make(fit=True)
        code.print_ascii(invert=True)
    except ImportError:
        print("Для QR в консоли выполни: python -m pip install qrcode")
        print("Либо вставь эту ссылку в любой генератор QR-кодов:")
    print(qr.url)

    for _ in range(3):  # QR живёт ~30 секунд, при истечении обновляем
        try:
            await qr.wait(30)
            return True
        except SessionPasswordNeededError:
            await client.sign_in(password=input("Пароль двухэтапной проверки: "))
            return True
        except TimeoutError:
            await qr.recreate()
            print("QR обновлён:", qr.url)
    return False


async def _login_by_code(client: Any) -> bool:
    """Log in by login code. Returns True on success."""
    from telethon.errors import PhoneCodeInvalidError, SessionPasswordNeededError

    phone = input("Введи номер телефона (в формате +7...): ").strip()
    sent = await client.send_code_request(phone)
    print("Тип отправки:", type(sent.type).__name__)
    print(
        "Код ищи в приложении Telegram — служебный чат «Telegram» (777000).\n"
        "Если пришло SentCodeTypeApp, SMS не будет.\n"
        "Введи 'sms', чтобы запросить повторную отправку другим способом."
    )

    while True:
        code = input("Введи код (или 'sms'): ").strip()
        if code.lower() == "sms":
            try:
                sent = await client.send_code_request(phone, force_sms=True)
                print("Повторный запрос. Тип отправки:", type(sent.type).__name__)
            except Exception as exc:
                print("Не удалось запросить повторно:", exc)
            continue
        try:
            await client.sign_in(phone=phone, code=code, phone_code_hash=sent.phone_code_hash)
            return True
        except SessionPasswordNeededError:
            await client.sign_in(password=input("Пароль двухэтапной проверки: "))
            return True
        except PhoneCodeInvalidError:
            print("Неверный код, попробуй ещё раз.")


# ---------------------------------------------------------------------------
# Обнаружение подарков
# ---------------------------------------------------------------------------


def classify_gift_action(action: Any) -> tuple[str, str]:
    """Решить, что делать со служебным действием. Вернуть (решение, причина).

    Решение: 'save' — сохранить; 'skip' — это подарок, но не апгрейд;
    'ignore' — вообще не про подарки.
    """
    name = type(action).__name__
    if name == "MessageActionStarGift":
        return "skip", "обычный подарок (ещё не апгрейднут)"
    if name != "MessageActionStarGiftUnique":
        return "ignore", name
    if getattr(action, "gift", None) is None:
        return "skip", "в действии нет объекта gift"
    if getattr(action, "upgrade", False):
        return "save", "upgrade=True"
    flags = [f for f in ("transferred", "refunded", "saved") if getattr(action, f, False)]
    return "skip", "уникальный подарок без флага upgrade (флаги: " + (", ".join(flags) or "нет") + ")"


async def store_and_notify(bot: Bot, gift: Any, date: Any, source: str, notify: bool = True) -> bool:
    """Сохранить подарок и разослать активным подписчикам. True — подарок новый."""
    if gift is None:
        log.warning("[%s] пропуск: объект подарка пустой", source)
        return False

    is_new, data = save_gift(gift, date)
    if not data:
        log.warning("[%s] пропуск: не удалось извлечь данные подарка", source)
        return False
    if not is_new:
        log.info("[%s] пропуск: %s #%s уже есть в базе", source, data.get("title"), data.get("number"))
        return False

    max_age = get_setting("notify_max_age")
    age = int(time.time()) - int(data.get("created_at") or 0)
    if not notify or (max_age and age > max_age):
        log.info("[%s] сохранён без рассылки (старый/тихий режим, возраст %d c): %s #%s",
                 source, age, data.get("title"), data.get("number"))
        return True

    subscribers = get_active_subscribers()
    log.info(
        "[%s] НОВЫЙ АПГРЕЙД: %s #%s (%s). Активных подписчиков: %d",
        source, data.get("title"), data.get("number"), data.get("url"), len(subscribers),
    )
    text = gift_message(data)
    for user_id in subscribers:
        try:
            await bot.send_message(user_id, text, disable_web_page_preview=False)
        except Exception:
            log.exception("[%s] не удалось отправить уведомление %s", source, user_id)
    return True


async def process_service_message(bot: Bot, msg: Any, stats: Counter) -> None:
    """Обработать одно сообщение из обновления Telegram."""
    action = getattr(msg, "action", None)
    if action is None:
        return
    name = type(action).__name__
    stats["service_messages"] += 1
    stats[f"action:{name}"] += 1

    decision, reason = classify_gift_action(action)
    if decision == "ignore":
        if GIFT_DEBUG:
            log.info("[event] служебное сообщение %s — не подарок", name)
        return

    log.info(
        "[event] получено %s msg_id=%s чат=%s -> %s (%s)",
        name, getattr(msg, "id", None), getattr(msg, "peer_id", None), decision, reason,
    )
    if decision != "save":
        stats["gift_skipped"] += 1
        return

    saved = await store_and_notify(bot, getattr(action, "gift"), getattr(msg, "date", None), "event")
    stats["gift_saved" if saved else "gift_duplicate"] += 1


async def _heartbeat(client: Any, stats: Counter) -> None:
    """Раз в 2 минуты показывает, что слушатель жив и что он реально получает."""
    while client.is_connected():
        await asyncio.sleep(120)
        log.info("[heartbeat] счётчики: %s", dict(stats) if stats else "обновлений от Telegram пока не было")


# --- необязательный опрос по слагам (GIFT_PROBE_SLUGS) -----------------------


def _get_probe(prefix: str) -> int | None:
    with db_connect() as con:
        row = con.execute("SELECT last_num FROM gift_probe WHERE prefix = ?", (prefix,)).fetchone()
    return int(row["last_num"]) if row else None


def _set_probe(prefix: str, num: int) -> None:
    with db_connect() as con:
        con.execute(
            "INSERT INTO gift_probe(prefix, last_num) VALUES (?, ?) "
            "ON CONFLICT(prefix) DO UPDATE SET last_num = excluded.last_num",
            (prefix, num),
        )


async def _fetch_unique(client: Any, slug: str) -> Any | None:
    """payments.getUniqueStarGift: вернуть уникальный подарок или None, если slug не существует."""
    from telethon.tl import functions

    for _ in range(3):
        try:
            res = await client(functions.payments.GetUniqueStarGiftRequest(slug=slug))
            await asyncio.sleep(PROBE_CALL_DELAY)
            return getattr(res, "gift", None)
        except Exception as exc:
            seconds = getattr(exc, "seconds", None)  # FloodWaitError
            if seconds:
                log.warning("[probe] FloodWait %s c, жду", seconds)
                await asyncio.sleep(int(seconds) + 1)
                continue
            if "SLUG_INVALID" in (getattr(exc, "message", "") or str(exc)).upper():
                await asyncio.sleep(PROBE_CALL_DELAY)
                return None
            raise
    return None


async def _find_last_num(client: Any, prefix: str) -> int:
    """Найти текущий максимальный номер апгрейда коллекции (галоп + бинарный поиск)."""
    if await _fetch_unique(client, f"{prefix}-1") is None:
        return 0
    lo, hi = 1, 2
    while hi < 4_000_000 and await _fetch_unique(client, f"{prefix}-{hi}") is not None:
        lo, hi = hi, hi * 2
    while hi - lo > 1:
        mid = (lo + hi) // 2
        if await _fetch_unique(client, f"{prefix}-{mid}") is not None:
            lo = mid
        else:
            hi = mid
    return lo


async def probe_step(bot: Bot, client: Any, prefix: str, warned: set[str] | None = None) -> None:
    """Один проход по коллекции: определить стартовый номер или найти и разослать новые апгрейды."""
    last = _get_probe(prefix)
    if last is None:
        last = await _find_last_num(client, prefix)
        if last == 0:
            if warned is None or prefix not in warned:
                log.warning("[probe] %s-1 не найден (нет апгрейдов или другое написание слага)", prefix)
                if warned is not None:
                    warned.add(prefix)
            return
        _set_probe(prefix, last)
        log.info("[probe] %s: стартовый номер %s (старые не рассылаются)", prefix, last)
        _probe_warm.add(prefix)

    # Последовательно проверяем новые номера и КАЖДЫЙ найденный сразу сохраняем и рассылаем
    for _ in range(9999999999):
        next_num = last + 1
        slug = f"{prefix}-{next_num}"

        log.info("[probe] проверяю %s", slug)

        gift = await _fetch_unique(client, slug)
        if gift is None:
            log.info("[probe] %s пока не найден", slug)
            break

        # Сохраняем и рассылаем подписчикам
        await store_and_notify(bot, gift, None, f"probe:{prefix}", notify=prefix in _probe_warm)

        last = next_num
        _set_probe(prefix, last)
        log.info("[probe] найден и обработан %s", slug)

    _probe_warm.add(prefix)  # дальше — обычный режим с рассылкой


async def gift_probe_loop(bot: Bot, client: Any, prefix: str) -> None:
    """Следить за одной коллекцией (GIFT_PROBE_SLUGS=PlushPepe)."""
    log.info("[probe] слежу за коллекцией %r каждые %s c", prefix, get_setting("probe_interval"))
    warned: set[str] = set()
    while client.is_connected():
        try:
            await probe_step(bot, client, prefix, warned)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("[probe] ошибка при опросе %s", prefix)
        await asyncio.sleep(get_setting("probe_interval"))


async def _list_upgradable_prefixes(client: Any) -> list[tuple[str, str]]:
    """Каталог подарков (payments.getStarGifts) -> (название, префикс слага) для апгрейдимых.

    Префикс берём из поля slug самого подарка — это точный префикс уникальных
    экземпляров (Slug-1, Slug-2, ...). Старое поведение (вывод из названия)
    оставлено как запасной вариант: оно ломается на кириллице, цифрах,
    эмодзи и неочевидных сокращениях в названии.
    """
    from telethon.tl import functions

    res = await client(functions.payments.GetStarGiftsRequest(hash=0))
    found: list[tuple[str, str]] = []
    seen: set[str] = set()
    for g in (getattr(res, "gifts", None) or []):
        if not getattr(g, "upgrade_stars", None):
            continue
        title = str(getattr(g, "title", "") or "").strip() or "(без названия)"
        slug = str(getattr(g, "slug", "") or "").strip()
        prefix = slug or re.sub(r"[^0-9A-Za-z]", "", title)
        if prefix and prefix not in seen:
            seen.add(prefix)
            found.append((title, prefix))
    return found


async def probe_all_loop(bot: Bot, client: Any) -> None:
    """GIFT_PROBE_SLUGS=ALL: по кругу опрашивать все коллекции из каталога подарков."""
    warned: set[str] = set()
    prefixes: list[str] = []
    refreshed = 0.0
    while client.is_connected():
        if not prefixes or time.time() - refreshed > 3600:
            try:
                found = await _list_upgradable_prefixes(client)
                prefixes = [prefix for _, prefix in found]
                refreshed = time.time()
                log.info(
                    "[probe-all] коллекций в наблюдении: %d: %s",
                    len(prefixes),
                    ", ".join(f"{title}→{prefix}" for title, prefix in found),
                )
                if not prefixes:
                    log.warning("[probe-all] каталог пуст или без апгрейдимых подарков")
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("[probe-all] не удалось получить каталог подарков")
            if not prefixes:
                await asyncio.sleep(60)
                continue
        started = time.time()
        for prefix in prefixes:
            if not client.is_connected():
                return
            try:
                await probe_step(bot, client, prefix, warned)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("[probe-all] ошибка при опросе %s", prefix)
            await asyncio.sleep(PROBE_ALL_DELAY)
        log.info("[probe-all] круг по %d коллекциям занял %d c", len(prefixes), int(time.time() - started))


async def start_gift_listener(bot: Bot):
    """Start the user's Telethon listener. Keep the returned client alive in main.py."""
    if not API_ID or not API_HASH:
        log.warning("API_ID/API_HASH не заданы: подписки будут работать, сбор апгрейдов — нет.")
        return None

    try:
        from telethon import TelegramClient, events
    except ImportError:
        log.exception("Telethon не установлен. Выполни: python -m pip install telethon")
        return None

    # catch_up=True: после простоя Telethon запросит пропущенные обновления у Telegram.
    client = TelegramClient(str(SESSION_PATH), API_ID, API_HASH, catch_up=True)
    stats: Counter = Counter()

    def extract_messages(update: Any) -> Iterator[Any]:
        msg = getattr(update, "message", None)
        if msg is not None and hasattr(msg, "action"):
            yield msg
        for item in (getattr(update, "updates", None) or []):
            yield from extract_messages(item)
        nested = getattr(update, "update", None)
        if nested is not None and nested is not update:
            yield from extract_messages(nested)

    @client.on(events.Raw)
    async def on_raw_update(update: Any) -> None:
        stats["raw_updates"] += 1
        stats[f"update:{type(update).__name__}"] += 1
        try:
            for msg in extract_messages(update):
                await process_service_message(bot, msg, stats)
        except Exception:
            log.exception("[event] ошибка обработки обновления %s", type(update).__name__)

    try:
        # First launch asks how to log in. Enter data only in this local console.
        await client.connect()

        if not await client.is_user_authorized():
            print("\nСпособ входа в Telegram-аккаунт:")
            print("  1 — QR-код (рекомендуется, не нужен код)")
            print("  2 — код из приложения Telegram / SMS")
            choice = input("Выбери 1 или 2: ").strip()

            ok = await _login_by_qr(client) if choice != "2" else await _login_by_code(client)
            if not ok or not await client.is_user_authorized():
                log.error("Не удалось авторизоваться в Telegram.")
                await client.disconnect()
                return None

        me = await client.get_me()  # первый запрос также «включает» поток обновлений
        log.info(
            "Telethon gift listener connected как id=%s @%s. "
            "Слушатель видит ТОЛЬКО события этого аккаунта (его личные чаты и группы), а не все апгрейды Telegram.",
            getattr(me, "id", "?"), getattr(me, "username", None),
        )
        client._gift_tasks = [asyncio.create_task(_heartbeat(client, stats))]
        if any(x.upper() == "ALL" for x in PROBE_SLUGS):
            client._gift_tasks.append(asyncio.create_task(probe_all_loop(bot, client)))
        for prefix in PROBE_SLUGS:
            if prefix.upper() != "ALL":
                client._gift_tasks.append(asyncio.create_task(gift_probe_loop(bot, client, prefix)))
        return client
    except Exception:
        log.exception("Не удалось запустить Telethon-слушатель апгрейдов.")
        await client.disconnect()
        return None