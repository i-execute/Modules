__version__ = (2, 0, 0)
# meta developer: I_execute.t.me

import os
import asyncio
import tempfile
import logging
import shutil
import re
import time
import stat
import aiohttp

from telethon import TelegramClient, events, Button
from telethon.tl.types import (
    Message,
    UpdateBotPrecheckoutQuery,
    UpdateNewMessage,
    MessageActionPaymentSentMe,
    PeerUser,
)
from telethon.errors import FloodWaitError

from .. import loader, utils

logger = logging.getLogger(__name__)

_BOT_TOKEN_PATTERN = re.compile(r"\b\d{8,10}:[A-Za-z0-9_-]{35}\b")
_RESOLUTION_PATTERN = re.compile(r"(\d+)\s*[xX*]\s*(\d+)")
_URL_PATTERN = re.compile(r"https?://[^\s<>\"']+", re.IGNORECASE)


def escape_html(text: str) -> str:
    if not text:
        return ""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def get_full_name(entity) -> str:
    if not entity:
        return "Unknown"
    first = getattr(entity, "first_name", "") or ""
    last = getattr(entity, "last_name", "") or ""
    return f"{first} {last}".strip() or "Unknown"


def get_user_link(user_id: int, name: str) -> str:
    return f'<a href="tg://user?id={user_id}">{escape_html(name)}</a>'


def get_entity_username(entity) -> str:
    if not entity:
        return None
    if hasattr(entity, "username") and entity.username:
        return entity.username
    if hasattr(entity, "usernames") and entity.usernames:
        for uname_obj in entity.usernames:
            if getattr(uname_obj, "active", False):
                return uname_obj.username
    return None


@loader.tds
class Market(loader.Module):
    """Universal bot for selling products via Telegram Stars"""

    strings = {
        "name": "Market",
        "help": (
            "<b>Market Module - Universal sales bot</b>\n\n"
            "<b>Main:</b>\n"
            "<code>{prefix}mrkt</code> - This menu\n"
            "<code>{prefix}mrkt status</code> - Bot status\n"
            "<code>{prefix}mrkt reboot</code> - Reboot (clear session)\n"
            "<code>{prefix}mrkt debug</code> - Clear all logs\n\n"
            "<b>Bot setup:</b>\n"
            "<code>{prefix}mrkt bot [token]</code> - Set token and start\n"
            "<code>{prefix}mrkt set [1-5]</code> - Number of product buttons\n"
            "<code>{prefix}mrkt users</code> - Export users\n\n"
            "<b>Product settings [1-5]:</b>\n"
            "<code>{prefix}mrkt btn [1-5] [text]</code> - Button text\n"
            "<code>{prefix}mrkt ti [1-5] [text]</code> - Instruction text\n"
            "<code>{prefix}mrkt tv [1-5] 1/2 [text]</code> - Invoice title(1)/description(2)\n"
            "<code>{prefix}mrkt pi [1-5] [url]</code> - Instruction photo\n"
            "<code>{prefix}mrkt pv [1-5] [url]</code> - Invoice photo\n"
            "<code>{prefix}mrkt res [1-5] [W]x[H]</code> - Invoice photo resolution\n"
            "<code>{prefix}mrkt price [1-5] [price]</code> - Price in stars\n\n"
            "<b>General texts:</b>\n"
            "<code>{prefix}mrkt tm [text]</code> - Main message\n"
            "<code>{prefix}mrkt ts [text]</code> - After payment\n"
            "<code>{prefix}mrkt tl [text]</code> - Purchase log (vars: {name}, {user_id}, {username}, {product}, {amount})\n"
            "<code>{prefix}mrkt pm [url]</code> - Main message photo\n\n"
            "<b>Log limits:</b>\n"
            "<code>{prefix}mrkt lg [number]</code> - Action log limit\n"
            "<code>{prefix}mrkt ls [number]</code> - Stars log limit\n\n"
            "<b>Bot commands (owner only, in bot DM):</b>\n"
            "<code>/premium 3/6/12 [user_id]</code> - Gift Premium\n"
            "<code>/stat</code> - Action log\n"
            "<code>/stars</code> - Stars log\n"
            "<code>/balance</code> - Bot star balance\n"
            "<code>/refund [user_id] [charge_id]</code> - Refund stars"
        ),
        "status_text": (
            "<b>Market Bot Status</b>\n\n"
            "<b>State:</b> {state}\n"
            "<b>Token:</b> {token}\n"
            "<b>Product buttons:</b> {button_count}\n"
            "<b>Users:</b> {users}\n"
            "<b>Log topic:</b> {log_topic}\n"
            "<b>Photo main:</b> {photo_main}\n"
            "<b>Action log limit:</b> {max_action_log}\n"
            "<b>Stars log limit:</b> {max_stars_log}\n"
            "<b>Action log entries:</b> {log_count}\n"
            "<b>Stars log entries:</b> {stars_log_count}\n\n"
            "{products_info}"
        ),
        "product_info_line": "<b>Product {num}:</b> {button} | {price} ☆",
        "state_on": "Active",
        "state_off": "Stopped",
        "no_token": "<b>No token set!</b>\nUse: <code>{prefix}mrkt bot [token]</code>",
        "token_invalid": "<b>Invalid token format!</b>",
        "token_saved": "<b>Token saved, starting bot...</b>",
        "token_same": "<b>This token is already set.</b>",
        "bot_started": "<b>Bot started!</b>",
        "bot_stopped": "<b>Bot stopped!</b>",
        "bot_start_error": "<b>Start error:</b>\n<code>{}</code>",
        "reboot_start": "<b>REBOOT...</b>",
        "reboot_cleaning": "<b>Cleaning session...</b>",
        "reboot_done": "<b>Reboot complete!</b>",
        "debug_done": (
            "<b>DEBUG - Cleanup done</b>\n\n"
            "<b>Cleared:</b>\n"
            "- Action log: {action_count} entries\n"
            "- Stars log: {stars_count} entries"
        ),
        "set_saved": "<b>Button count:</b> {count}",
        "set_invalid": "<b>Enter a number from 1 to 5!</b>",
        "btn_saved": "<b>Button {num} text:</b> {text}",
        "btn_invalid": "<b>Enter button number (1-5) and text!</b>",
        "users_empty": "<b>User list is empty.</b>",
        "users_exported": "<b>Users ({count})</b>",
        "text_saved": "<b>Text saved:</b> <code>{key}</code>",
        "text_invalid_key": "<b>Invalid key!</b>",
        "text_no_content": "<b>Enter text (reply or argument)!</b>",
        "photo_main_saved": "<b>Main message photo set</b>",
        "photo_instruction_saved": "<b>Instruction photo {num} set</b>",
        "photo_invoice_saved": "<b>Invoice photo {num} set</b>",
        "photo_invalid": "<b>Enter photo URL!</b>",
        "photo_status_set": "Yes",
        "photo_status_none": "No",
        "res_saved": "<b>Resolution {num}:</b> {width}x{height}",
        "res_invalid": "<b>Invalid format!</b>\nExample: <code>{prefix}mrkt res 1 512x512</code>",
        "price_saved": "<b>Price {num}:</b> {price} ☆",
        "price_invalid": "<b>Enter number (1-5) and price!</b>",
        "log_limit_saved": "<b>Limit {log_type}:</b> {limit}",
        "log_limit_invalid": "<b>Enter a positive number!</b>",
        "export_wait": "<b>Exporting...</b>",
        "export_error": "<b>Export error:</b> {}",
        "bot_premium_usage": "<b>Usage:</b> <code>/premium 3/6/12 user_id</code>",
        "bot_premium_invalid_months": "<b>Available periods:</b> 3, 6 or 12 months",
        "bot_premium_success": "<b>Premium sent!</b>\nUser: <code>{user_id}</code>\nPeriod: {months} mo.",
        "bot_premium_error": "<b>Error:</b>\n<code>{error}</code>",
        "bot_premium_low_balance": "<b>Not enough stars on bot balance!</b>",
        "bot_refund_usage": "<b>Usage:</b> <code>/refund user_id charge_id</code>",
        "bot_refund_success": "<b>Refund done!</b>\nUser: <code>{user_id}</code>\nCharge: <code>{charge_id}</code>",
        "bot_refund_error": "<b>Refund error:</b>\n<code>{error}</code>",
        "bot_stat_empty": "<b>Action log is empty.</b>",
        "bot_stat_header": "<b>Action log ({count}):</b>",
        "bot_stars_empty": "<b>Stars log is empty.</b>",
        "bot_stars_header": "<b>Stars log ({count}):</b>",
        "bot_balance": "<b>Star balance:</b> {balance} ☆",
        "bot_balance_error": "<b>Failed to get balance</b>",
        "log_topic_failed": (
            "<b>Failed to setup Market log topic</b>\n\n"
            "The module will still work but without logging."
        ),
        "log_topic_ready": "<b>Market log topic ready</b>",
        "log_user_message": (
            "<b>User message</b>\n\n"
            "<b>User:</b> {user_link}\n"
            "<b>ID:</b> <code>{user_id}</code>\n"
            "{username_line}"
            "<b>Message:</b>\n<blockquote>{text}</blockquote>"
        ),
        "log_payment_success": (
            "<b>Payment received!</b>\n\n"
            "<b>Buyer:</b> {user_link}\n"
            "<b>ID:</b> <code>{user_id}</code>\n"
            "{username_line}"
            "<b>Product:</b> {product}\n"
            "<b>Amount:</b> {amount} ☆\n"
            "<b>Owner:</b> {owner_tag}"
        ),
        "log_bot_started": (
            "<b>Bot started by user</b>\n\n"
            "<b>User:</b> {user_link}\n"
            "<b>ID:</b> <code>{user_id}</code>\n"
            "{username_line}"
        ),
        "log_product_selected": (
            "<b>Product selected</b>\n\n"
            "<b>User:</b> {user_link}\n"
            "<b>ID:</b> <code>{user_id}</code>\n"
            "{username_line}"
            "<b>Product:</b> {product}"
        ),
        "default_none": "NONE",
        "btn_back": "Back",
        "btn_continue": "Continue",
    }

    strings_ru = {
        "help": (
            "<b>Market Module - Универсальный бот продаж</b>\n\n"
            "<b>Основные:</b>\n"
            "<code>{prefix}mrkt</code> - Это меню\n"
            "<code>{prefix}mrkt status</code> - Статус бота\n"
            "<code>{prefix}mrkt reboot</code> - Перезапуск (очистка сессии)\n"
            "<code>{prefix}mrkt debug</code> - Очистка всех логов\n\n"
            "<b>Настройка бота:</b>\n"
            "<code>{prefix}mrkt bot [токен]</code> - Установить токен и запустить\n"
            "<code>{prefix}mrkt set [1-5]</code> - Количество кнопок товаров\n"
            "<code>{prefix}mrkt users</code> - Экспорт пользователей\n\n"
            "<b>Настройка товаров [1-5]:</b>\n"
            "<code>{prefix}mrkt btn [1-5] [текст]</code> - Текст кнопки\n"
            "<code>{prefix}mrkt ti [1-5] [текст]</code> - Инструкция товара\n"
            "<code>{prefix}mrkt tv [1-5] 1/2 [текст]</code> - Заголовок(1)/описание(2) инвойса\n"
            "<code>{prefix}mrkt pi [1-5] [url]</code> - Фото инструкции\n"
            "<code>{prefix}mrkt pv [1-5] [url]</code> - Фото инвойса\n"
            "<code>{prefix}mrkt res [1-5] [W]x[H]</code> - Разрешение фото инвойса\n"
            "<code>{prefix}mrkt price [1-5] [цена]</code> - Цена в звездах\n\n"
            "<b>Общие тексты:</b>\n"
            "<code>{prefix}mrkt tm [текст]</code> - Главное сообщение\n"
            "<code>{prefix}mrkt ts [текст]</code> - После оплаты\n"
            "<code>{prefix}mrkt tl [текст]</code> - Лог покупки (переменные: {name}, {user_id}, {username}, {product}, {amount})\n"
            "<code>{prefix}mrkt pm [url]</code> - Фото главного сообщения\n\n"
            "<b>Логи:</b>\n"
            "<code>{prefix}mrkt lg [число]</code> - Лимит лога взаимодействий\n"
            "<code>{prefix}mrkt ls [число]</code> - Лимит лога звезд\n\n"
            "<b>Команды бота (только владелец, в ЛС бота):</b>\n"
            "<code>/premium 3/6/12 [user_id]</code> - Выдать Premium\n"
            "<code>/stat</code> - Лог взаимодействий\n"
            "<code>/stars</code> - Лог операций со звездами\n"
            "<code>/balance</code> - Баланс звезд бота\n"
            "<code>/refund [user_id] [charge_id]</code> - Возврат звезд"
        ),
        "status_text": (
            "<b>Market Bot Status</b>\n\n"
            "<b>Состояние:</b> {state}\n"
            "<b>Токен:</b> {token}\n"
            "<b>Кнопок товаров:</b> {button_count}\n"
            "<b>Пользователей:</b> {users}\n"
            "<b>Топик логов:</b> {log_topic}\n"
            "<b>Фото main:</b> {photo_main}\n"
            "<b>Лимит лога:</b> {max_action_log}\n"
            "<b>Лимит stars:</b> {max_stars_log}\n"
            "<b>Записей в логе:</b> {log_count}\n"
            "<b>Записей stars:</b> {stars_log_count}\n\n"
            "{products_info}"
        ),
        "product_info_line": "<b>Товар {num}:</b> {button} | {price} ☆",
        "state_on": "Работает",
        "state_off": "Остановлен",
        "no_token": "<b>Токен не указан!</b>\nИспользуй: <code>{prefix}mrkt bot [токен]</code>",
        "token_invalid": "<b>Неверный формат токена!</b>",
        "token_saved": "<b>Токен сохранен, запуск бота...</b>",
        "token_same": "<b>Этот токен уже установлен.</b>",
        "bot_started": "<b>Бот запущен!</b>",
        "bot_stopped": "<b>Бот остановлен!</b>",
        "bot_start_error": "<b>Ошибка запуска:</b>\n<code>{}</code>",
        "reboot_start": "<b>REBOOT...</b>",
        "reboot_cleaning": "<b>Очистка сессии...</b>",
        "reboot_done": "<b>Перезапуск завершен!</b>",
        "debug_done": (
            "<b>DEBUG - Очистка завершена</b>\n\n"
            "<b>Очищено:</b>\n"
            "- Action log: {action_count} записей\n"
            "- Stars log: {stars_count} записей"
        ),
        "set_saved": "<b>Количество кнопок:</b> {count}",
        "set_invalid": "<b>Укажи число от 1 до 5!</b>",
        "btn_saved": "<b>Текст кнопки {num}:</b> {text}",
        "btn_invalid": "<b>Укажи номер кнопки (1-5) и текст!</b>",
        "users_empty": "<b>Список пользователей пуст.</b>",
        "users_exported": "<b>Пользователи ({count})</b>",
        "text_saved": "<b>Текст сохранен:</b> <code>{key}</code>",
        "text_invalid_key": "<b>Неверный ключ!</b>",
        "text_no_content": "<b>Укажи текст (реплай или аргументом)!</b>",
        "photo_main_saved": "<b>Фото главного сообщения установлено</b>",
        "photo_instruction_saved": "<b>Фото инструкции {num} установлено</b>",
        "photo_invoice_saved": "<b>Фото инвойса {num} установлено</b>",
        "photo_invalid": "<b>Укажи URL фото!</b>",
        "photo_status_set": "Да",
        "photo_status_none": "Нет",
        "res_saved": "<b>Разрешение {num}:</b> {width}x{height}",
        "res_invalid": "<b>Неверный формат!</b>\nПример: <code>{prefix}mrkt res 1 512x512</code>",
        "price_saved": "<b>Цена {num}:</b> {price} ☆",
        "price_invalid": "<b>Укажи номер (1-5) и цену!</b>",
        "log_limit_saved": "<b>Лимит {log_type}:</b> {limit}",
        "log_limit_invalid": "<b>Укажи положительное число!</b>",
        "export_wait": "<b>Экспортирую...</b>",
        "export_error": "<b>Ошибка экспорта:</b> {}",
        "bot_premium_usage": "<b>Использование:</b> <code>/premium 3/6/12 user_id</code>",
        "bot_premium_invalid_months": "<b>Доступные периоды:</b> 3, 6 или 12 месяцев",
        "bot_premium_success": "<b>Premium отправлен!</b>\nПользователь: <code>{user_id}</code>\nПериод: {months} мес.",
        "bot_premium_error": "<b>Ошибка:</b>\n<code>{error}</code>",
        "bot_premium_low_balance": "<b>Недостаточно звезд на балансе бота!</b>",
        "bot_refund_usage": "<b>Использование:</b> <code>/refund user_id charge_id</code>",
        "bot_refund_success": "<b>Возврат выполнен!</b>\nUser: <code>{user_id}</code>\nCharge: <code>{charge_id}</code>",
        "bot_refund_error": "<b>Ошибка возврата:</b>\n<code>{error}</code>",
        "bot_stat_empty": "<b>Лог взаимодействий пуст.</b>",
        "bot_stat_header": "<b>Лог взаимодействий ({count}):</b>",
        "bot_stars_empty": "<b>Лог операций со звездами пуст.</b>",
        "bot_stars_header": "<b>Лог операций со звездами ({count}):</b>",
        "bot_balance": "<b>Баланс звезд:</b> {balance} ☆",
        "bot_balance_error": "<b>Ошибка получения баланса</b>",
        "log_topic_failed": (
            "<b>Не удалось настроить топик логов Market</b>\n\n"
            "Модуль продолжит работать, но без логирования."
        ),
        "log_topic_ready": "<b>Топик логов Market готов</b>",
        "log_user_message": (
            "<b>Сообщение пользователя</b>\n\n"
            "<b>Пользователь:</b> {user_link}\n"
            "<b>ID:</b> <code>{user_id}</code>\n"
            "{username_line}"
            "<b>Сообщение:</b>\n<blockquote>{text}</blockquote>"
        ),
        "log_payment_success": (
            "<b>Оплата получена!</b>\n\n"
            "<b>Покупатель:</b> {user_link}\n"
            "<b>ID:</b> <code>{user_id}</code>\n"
            "{username_line}"
            "<b>Товар:</b> {product}\n"
            "<b>Сумма:</b> {amount} ☆\n"
            "<b>Владелец:</b> {owner_tag}"
        ),
        "log_bot_started": (
            "<b>Пользователь запустил бота</b>\n\n"
            "<b>Пользователь:</b> {user_link}\n"
            "<b>ID:</b> <code>{user_id}</code>\n"
            "{username_line}"
        ),
        "log_product_selected": (
            "<b>Выбран товар</b>\n\n"
            "<b>Пользователь:</b> {user_link}\n"
            "<b>ID:</b> <code>{user_id}</code>\n"
            "{username_line}"
            "<b>Товар:</b> {product}"
        ),
        "default_none": "NONE",
        "btn_back": "Назад",
        "btn_continue": "Продолжить",
    }

    def __init__(self):
        self._bot = None
        self._bot_active = False
        self._bot_id = None
        self._bot_username = None
        self._aiogram_bot = None
        self._bot_token = None
        self._my_id = None
        self._my_name = None
        self._session_dir = None
        self._export_dir = None
        self._asset_channel = None
        self._log_topic = None
        self._setup_failed = False
        self._flood_lock = asyncio.Lock()

    def _get_owner_tag(self) -> str:
        return get_user_link(self._my_id, self._my_name)

    def _is_owner(self, user_id: int) -> bool:
        return user_id == self._my_id

    def _is_dm(self, event) -> bool:
        return isinstance(getattr(event, "peer_id", None), PeerUser)

    def _can_use_command(self, event) -> bool:
        return self._is_dm(event) and self._is_owner(event.sender_id)

    async def _send_with_flood_wait(self, coro_func, *args, **kwargs):
        max_retries = 5
        for attempt in range(max_retries):
            try:
                return await coro_func(*args, **kwargs)
            except FloodWaitError as e:
                await asyncio.sleep(e.seconds + 1)
            except Exception as e:
                error_str = str(e).lower()
                if "flood" in error_str:
                    await asyncio.sleep(5)
                    continue
                raise
        return None

    async def _ensure_log_topic(self):
        async with self._flood_lock:
            self._asset_channel = self._db.get("heroku.forums", "channel_id", None)
            if not self._asset_channel:
                logger.warning("[Market] heroku.forums channel_id not found in DB.")
                self._setup_failed = True
                return
            try:
                self._log_topic = await utils.asset_forum_topic(
                    self._client,
                    self._db,
                    self._asset_channel,
                    "Market Logs",
                    description="Market sales and activity logs.",
                    icon_emoji_id=5188466187448650036,
                )
                self._setup_failed = False
            except Exception as e:
                logger.error(f"[Market] Failed to create/get log topic: {e}")
                self._setup_failed = True
                try:
                    await self.inline.bot.send_message(
                        self._my_id,
                        self.strings["log_topic_failed"],
                        parse_mode="HTML",
                    )
                except Exception:
                    pass
                return

            try:
                await self._send_with_flood_wait(
                    self.inline.bot.send_message,
                    int(f"-100{self._asset_channel}"),
                    self.strings["log_topic_ready"],
                    parse_mode="HTML",
                    message_thread_id=self._log_topic.id,
                )
            except Exception as e:
                logger.warning(f"[Market] Failed to send log_topic_ready: {e}")

    async def _send_log(self, text: str):
        if not self._log_topic or not self._asset_channel:
            if not self._setup_failed:
                await self._ensure_log_topic()
            return
        try:
            await self._send_with_flood_wait(
                self.inline.bot.send_message,
                int(f"-100{self._asset_channel}"),
                text,
                parse_mode="HTML",
                disable_web_page_preview=True,
                message_thread_id=self._log_topic.id,
            )
        except Exception as e:
            logger.error(f"[Market] Failed to send log: {e}")
            if not self._setup_failed:
                await self._ensure_log_topic()

    async def client_ready(self, client, db):
        self._client = client
        self._db = db

        me = await client.get_me()
        self._my_id = me.id
        self._my_name = get_full_name(me)

        base_dir = os.path.join(tempfile.gettempdir(), f"market_{self._my_id}")
        self._session_dir = os.path.join(base_dir, "session")
        self._export_dir = os.path.join(base_dir, "export")

        os.makedirs(self._session_dir, exist_ok=True)
        os.makedirs(self._export_dir, exist_ok=True)
        os.chmod(base_dir, stat.S_IRWXU)

        self._users = set(self._db.get("Market", "users", []))
        self._texts = self._db.get("Market", "texts", {})
        self._action_log = self._db.get("Market", "action_log", [])
        self._stars_log = self._db.get("Market", "stars_log", [])
        self._button_count = self._db.get("Market", "button_count", 1)
        self._max_action_log = self._db.get("Market", "max_action_log", 40)
        self._max_stars_log = self._db.get("Market", "max_stars_log", 40)

        self._trim_logs()
        await self._ensure_log_topic()

        token = self._db.get("Market", "bot_token")
        if token and self._db.get("Market", "auto_start", False):
            try:
                await self._start_bot(token)
            except Exception as e:
                logger.error(f"[Market] Auto-start failed: {e}")

    def _trim_logs(self):
        changed = False
        if len(self._action_log) > self._max_action_log:
            self._action_log = self._action_log[-self._max_action_log:]
            changed = True
        if len(self._stars_log) > self._max_stars_log:
            self._stars_log = self._stars_log[-self._max_stars_log:]
            changed = True
        if changed:
            self._save_action_log()
            self._save_stars_log()

    def _get_text(self, key: str) -> str:
        if key in self._texts and self._texts[key]:
            return self._texts[key]
        return self.strings["default_none"]

    def _get_button_text(self, num: int) -> str:
        key = f"button_{num}"
        if key in self._texts and self._texts[key]:
            return self._texts[key]
        return self.strings["default_none"]

    def _get_price(self, num: int) -> int:
        key = f"price_{num}"
        if key in self._texts:
            try:
                return int(self._texts[key])
            except (ValueError, TypeError):
                pass
        return 0

    def _get_photo_main(self) -> str:
        return self._texts.get("photo_main", "")

    def _get_photo_instruction(self, num: int) -> str:
        return self._texts.get(f"photo_{num}", "")

    def _get_photo_invoice(self, num: int) -> str:
        return self._texts.get(f"photo_invoice_{num}", "")

    def _get_resolution(self, num: int) -> tuple:
        try:
            return (
                int(self._texts.get(f"res_{num}_w", 512)),
                int(self._texts.get(f"res_{num}_h", 512)),
            )
        except (ValueError, TypeError):
            return 512, 512

    def _save_users(self):
        self._db.set("Market", "users", list(self._users))

    def _save_texts(self):
        self._db.set("Market", "texts", self._texts)

    def _save_action_log(self):
        self._db.set("Market", "action_log", self._action_log)

    def _save_stars_log(self):
        self._db.set("Market", "stars_log", self._stars_log)

    def _save_button_count(self):
        self._db.set("Market", "button_count", self._button_count)

    def _save_log_limits(self):
        self._db.set("Market", "max_action_log", self._max_action_log)
        self._db.set("Market", "max_stars_log", self._max_stars_log)

    def _add_user(self, user_id: int):
        if user_id not in self._users:
            self._users.add(user_id)
            self._save_users()

    def _clear_all_logs(self) -> tuple:
        action_count = len(self._action_log)
        stars_count = len(self._stars_log)
        self._action_log = []
        self._stars_log = []
        self._save_action_log()
        self._save_stars_log()
        return action_count, stars_count

    def _log_action(self, action_type: str, user_id: int, details: str = ""):
        timestamp = time.strftime("%d.%m.%Y %H:%M:%S")
        entry = {"time": timestamp, "type": action_type, "user_id": user_id, "details": details}
        self._action_log.append(entry)
        if len(self._action_log) > self._max_action_log:
            self._action_log = self._action_log[-self._max_action_log:]
        self._save_action_log()

    def _format_action_entry(self, entry: dict) -> str:
        details = f" - {entry['details']}" if entry.get("details") else ""
        return (
            f"[{entry['time']}] User <code>{entry['user_id']}</code>: "
            f"<b>{entry['type']}</b>{details}"
        )

    def _log_stars(self, action_type: str, user_id: int, details: str = ""):
        timestamp = time.strftime("%d.%m.%Y %H:%M:%S")
        entry = {"time": timestamp, "type": action_type, "user_id": user_id, "details": details}
        self._stars_log.append(entry)
        if len(self._stars_log) > self._max_stars_log:
            self._stars_log = self._stars_log[-self._max_stars_log:]
        self._save_stars_log()

    def _format_stars_entry(self, entry: dict) -> str:
        details = f" - {entry['details']}" if entry.get("details") else ""
        return (
            f"[{entry['time']}] User <code>{entry['user_id']}</code>: "
            f"<b>{entry['type']}</b>{details}"
        )

    async def _get_bot_star_balance(self) -> int:
        if not self._bot_token:
            return None
        try:
            async with aiohttp.ClientSession() as session:
                url = f"https://api.telegram.org/bot{self._bot_token}/getMyStarBalance"
                async with session.get(url) as resp:
                    data = await resp.json()
                    if data.get("ok"):
                        return data["result"].get("amount", 0)
        except Exception as e:
            logger.error(f"Failed to get star balance: {e}")
        return None

    def _extract_token(self, text: str) -> str:
        if not text:
            return None
        match = _BOT_TOKEN_PATTERN.search(text)
        return match.group(0) if match else None

    def _extract_resolution(self, text: str) -> tuple:
        if not text:
            return None, None
        match = _RESOLUTION_PATTERN.search(text)
        if match:
            return int(match.group(1)), int(match.group(2))
        return None, None

    def _extract_url(self, text: str) -> str:
        if not text:
            return None
        match = _URL_PATTERN.search(text)
        return match.group(0) if match else None

    async def _get_url_from_message(self, message, skip_words: int = 0) -> str:
        if message.is_reply:
            reply = await message.get_reply_message()
            if reply and reply.raw_text:
                url = self._extract_url(reply.raw_text)
                if url:
                    return url
        if skip_words > 0:
            text = self._extract_text_after_command(message.raw_text, skip_words)
        else:
            text = message.raw_text
        return self._extract_url(text) if text else None

    def _extract_text_after_command(self, raw_text: str, num_words_to_skip: int) -> str:
        if not raw_text:
            return None
        parts = raw_text.split(maxsplit=num_words_to_skip)
        if len(parts) > num_words_to_skip:
            return parts[num_words_to_skip]
        return None

    async def _get_html_text(self, message, num_words_to_skip: int) -> str:
        if message.is_reply:
            reply = await message.get_reply_message()
            if reply and reply.raw_text:
                return reply.raw_text
        return self._extract_text_after_command(message.raw_text, num_words_to_skip)

    def _clean_dir(self, path: str):
        if os.path.exists(path):
            try:
                shutil.rmtree(path)
            except Exception as e:
                logger.error(f"Failed to clean {path}: {e}")

    async def _get_user_info(self, user_id: int):
        try:
            user = await self._client.get_entity(user_id)
            return get_full_name(user), get_entity_username(user)
        except Exception:
            return "Unknown", None

    async def _get_bot_user_info(self, user_id: int):
        try:
            user = await self._bot.get_entity(user_id)
            return get_full_name(user), get_entity_username(user)
        except Exception:
            return "Unknown", None

    async def _start_bot(self, token: str):
        await self._stop_bot()
        self._bot_token = token
        os.makedirs(self._session_dir, exist_ok=True)
        session_path = os.path.join(self._session_dir, "MarketBot")
        try:
            self._bot = TelegramClient(
                session_path,
                api_id=self._client.api_id,
                api_hash=self._client.api_hash,
            )
            await self._bot.start(bot_token=token)
            bot_me = await self._bot.get_me()
            self._bot_id = bot_me.id
            self._bot_username = bot_me.username
            logger.info(f"Market bot started as @{bot_me.username}")
            try:
                from aiogram import Bot
                from aiogram.client.bot import DefaultBotProperties
                from aiogram.enums import ParseMode
                self._aiogram_bot = Bot(
                    token,
                    default=DefaultBotProperties(parse_mode=ParseMode.HTML),
                )
            except ImportError:
                logger.warning("Aiogram not installed")
                self._aiogram_bot = None
        except Exception as e:
            self._clean_dir(self._session_dir)
            self._bot = None
            self._aiogram_bot = None
            self._bot_token = None
            raise Exception(f"Failed to start bot: {e}")
        self._bot_active = True
        self._bot.add_event_handler(self._handle_start, events.NewMessage(pattern="/start"))
        self._bot.add_event_handler(self._handle_premium, events.NewMessage(pattern="/premium"))
        self._bot.add_event_handler(self._handle_stat, events.NewMessage(pattern="/stat"))
        self._bot.add_event_handler(self._handle_stars, events.NewMessage(pattern="/stars"))
        self._bot.add_event_handler(self._handle_balance, events.NewMessage(pattern="/balance"))
        self._bot.add_event_handler(self._handle_refund, events.NewMessage(pattern="/refund"))
        self._bot.add_event_handler(self._handle_callback, events.CallbackQuery())
        self._bot.add_event_handler(self._handle_user_message, events.NewMessage())
        self._bot.add_event_handler(self._handle_raw, events.Raw())

    async def _stop_bot(self):
        if self._aiogram_bot:
            try:
                await self._aiogram_bot.session.close()
            except Exception:
                pass
            self._aiogram_bot = None
        if self._bot:
            try:
                await self._bot.disconnect()
            except Exception:
                pass
            self._bot = None
        self._bot_active = False
        self._bot_id = None
        self._bot_username = None
        self._bot_token = None

    async def _export_users(self, message):
        if not self._users:
            await utils.answer(message, self.strings["users_empty"])
            return
        status = await utils.answer(message, self.strings["export_wait"])
        lines = [f"Market Users ({len(self._users)})", "=" * 40, ""]
        for uid in sorted(self._users):
            name, username = await self._get_user_info(uid)
            uname_display = f"@{username}" if username else "no_username"
            lines.append(f"ID: {uid} | Name: {name} | Username: {uname_display}")
        content = "\n".join(lines)
        file_path = os.path.join(self._export_dir, "market_users.txt")
        try:
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(content)
            try:
                if hasattr(status, "delete"):
                    await status.delete()
                elif isinstance(status, list) and status:
                    await status[0].delete()
            except Exception:
                pass
            await self._client.send_file(
                message.chat_id,
                file_path,
                caption=self.strings["users_exported"].format(count=len(self._users)),
                parse_mode="html",
            )
        except Exception as e:
            await utils.answer(message, self.strings["export_error"].format(str(e)))
        finally:
            if os.path.exists(file_path):
                os.remove(file_path)

    @loader.command(
        ru_doc="Управление модулем Market",
        en_doc="Market module control",
    )
    async def mrkt(self, message: Message):
        """Market module control"""
        args = utils.get_args_raw(message).strip()
        prefix = self.get_prefix()

        if not args:
            await utils.answer(message, self.strings["help"].format(prefix=prefix))
            return

        parts = args.split(maxsplit=5)
        cmd = parts[0].lower()

        if cmd == "status":
            token = self._db.get("Market", "bot_token", "")
            token_display = f"...{token[-6:]}" if token and len(token) > 10 else "NO"
            state_str = self.strings["state_on"] if self._bot_active else self.strings["state_off"]
            photo_main = (
                self.strings["photo_status_set"] if self._get_photo_main()
                else self.strings["photo_status_none"]
            )
            log_topic = (
                f"<code>{self._log_topic.id}</code>" if self._log_topic else "No"
            )
            products_lines = []
            for i in range(1, 6):
                btn_text = self._get_button_text(i)
                price = self._get_price(i)
                price_display = f"{price}" if price else "NONE"
                products_lines.append(
                    self.strings["product_info_line"].format(
                        num=i, button=btn_text, price=price_display,
                    )
                )
            await utils.answer(message, self.strings["status_text"].format(
                state=state_str,
                token=token_display,
                button_count=self._button_count,
                users=len(self._users),
                log_topic=log_topic,
                photo_main=photo_main,
                max_action_log=self._max_action_log,
                max_stars_log=self._max_stars_log,
                log_count=len(self._action_log),
                stars_log_count=len(self._stars_log),
                products_info="\n".join(products_lines),
            ))

        elif cmd == "debug":
            action_count, stars_count = self._clear_all_logs()
            await utils.answer(message, self.strings["debug_done"].format(
                action_count=action_count, stars_count=stars_count,
            ))

        elif cmd == "bot":
            token = None
            if message.is_reply:
                reply = await message.get_reply_message()
                if reply and reply.raw_text:
                    token = self._extract_token(reply.raw_text)
            if not token and len(parts) > 1:
                token = self._extract_token(" ".join(parts[1:]))
            if not token:
                await utils.answer(message, self.strings["token_invalid"])
                return
            old_token = self._db.get("Market", "bot_token")
            if old_token == token:
                await utils.answer(message, self.strings["token_same"])
                return
            self._db.set("Market", "bot_token", token)
            await utils.answer(message, self.strings["token_saved"])
            try:
                await self._start_bot(token)
                self._db.set("Market", "auto_start", True)
                await utils.answer(message, self.strings["bot_started"])
            except Exception as e:
                await utils.answer(
                    message, self.strings["bot_start_error"].format(str(e)[:200]),
                )

        elif cmd == "reboot":
            msg = await utils.answer(message, self.strings["reboot_start"])
            if isinstance(msg, list):
                msg = msg[0]
            await self._stop_bot()
            self._db.set("Market", "auto_start", False)
            await msg.edit(self.strings["reboot_cleaning"])
            self._clean_dir(self._session_dir)
            os.makedirs(self._session_dir, exist_ok=True)
            await asyncio.sleep(1)
            token = self._db.get("Market", "bot_token")
            if token:
                try:
                    await self._start_bot(token)
                    self._db.set("Market", "auto_start", True)
                    await msg.edit(self.strings["reboot_done"])
                except Exception as e:
                    await msg.edit(self.strings["bot_start_error"].format(str(e)[:200]))
            else:
                await msg.edit(self.strings["reboot_done"])

        elif cmd == "set":
            if len(parts) < 2:
                await utils.answer(message, self.strings["set_invalid"])
                return
            try:
                count = int(parts[1])
                if count < 1 or count > 5:
                    raise ValueError
            except ValueError:
                await utils.answer(message, self.strings["set_invalid"])
                return
            self._button_count = count
            self._save_button_count()
            await utils.answer(message, self.strings["set_saved"].format(count=count))

        elif cmd == "users":
            await self._export_users(message)

        elif cmd == "btn":
            if len(parts) < 3:
                await utils.answer(message, self.strings["btn_invalid"])
                return
            try:
                num = int(parts[1])
                if num < 1 or num > 5:
                    raise ValueError
            except ValueError:
                await utils.answer(message, self.strings["btn_invalid"])
                return
            text = await self._get_html_text(message, 3)
            if not text:
                await utils.answer(message, self.strings["btn_invalid"])
                return
            self._texts[f"button_{num}"] = text
            self._save_texts()
            await utils.answer(message, self.strings["btn_saved"].format(num=num, text=text))

        elif cmd == "tm":
            text = await self._get_html_text(message, 2)
            if not text:
                await utils.answer(message, self.strings["text_no_content"])
                return
            self._texts["main"] = text
            self._save_texts()
            await utils.answer(message, self.strings["text_saved"].format(key="main"))

        elif cmd == "ts":
            text = await self._get_html_text(message, 2)
            if not text:
                await utils.answer(message, self.strings["text_no_content"])
                return
            self._texts["success"] = text
            self._save_texts()
            await utils.answer(message, self.strings["text_saved"].format(key="success"))

        elif cmd == "tl":
            text = await self._get_html_text(message, 2)
            if not text:
                await utils.answer(message, self.strings["text_no_content"])
                return
            self._texts["log"] = text
            self._save_texts()
            await utils.answer(message, self.strings["text_saved"].format(key="log"))

        elif cmd == "ti":
            if len(parts) < 3:
                await utils.answer(message, self.strings["text_invalid_key"])
                return
            try:
                num = int(parts[1])
                if num < 1 or num > 5:
                    raise ValueError
            except ValueError:
                await utils.answer(message, self.strings["text_invalid_key"])
                return
            text = await self._get_html_text(message, 3)
            if not text:
                await utils.answer(message, self.strings["text_no_content"])
                return
            key = f"instruction_{num}"
            self._texts[key] = text
            self._save_texts()
            await utils.answer(message, self.strings["text_saved"].format(key=key))

        elif cmd == "tv":
            if len(parts) < 4:
                await utils.answer(message, self.strings["text_invalid_key"])
                return
            try:
                num = int(parts[1])
                invoice_part = parts[2]
                if num < 1 or num > 5 or invoice_part not in ("1", "2"):
                    raise ValueError
            except ValueError:
                await utils.answer(message, self.strings["text_invalid_key"])
                return
            text = await self._get_html_text(message, 4)
            if not text:
                await utils.answer(message, self.strings["text_no_content"])
                return
            key = f"invoice_{num}_{invoice_part}"
            self._texts[key] = text
            self._save_texts()
            await utils.answer(message, self.strings["text_saved"].format(key=key))

        elif cmd == "pm":
            url = await self._get_url_from_message(message, 2)
            if not url:
                await utils.answer(message, self.strings["photo_invalid"])
                return
            self._texts["photo_main"] = url
            self._save_texts()
            await utils.answer(message, self.strings["photo_main_saved"])

        elif cmd == "pi":
            if len(parts) < 2:
                await utils.answer(message, self.strings["photo_invalid"])
                return
            try:
                num = int(parts[1])
                if num < 1 or num > 5:
                    raise ValueError
            except ValueError:
                await utils.answer(message, self.strings["photo_invalid"])
                return
            url = await self._get_url_from_message(message, 3)
            if not url:
                await utils.answer(message, self.strings["photo_invalid"])
                return
            self._texts[f"photo_{num}"] = url
            self._save_texts()
            await utils.answer(
                message, self.strings["photo_instruction_saved"].format(num=num),
            )

        elif cmd == "pv":
            if len(parts) < 2:
                await utils.answer(message, self.strings["photo_invalid"])
                return
            try:
                num = int(parts[1])
                if num < 1 or num > 5:
                    raise ValueError
            except ValueError:
                await utils.answer(message, self.strings["photo_invalid"])
                return
            url = await self._get_url_from_message(message, 3)
            if not url:
                await utils.answer(message, self.strings["photo_invalid"])
                return
            self._texts[f"photo_invoice_{num}"] = url
            self._save_texts()
            await utils.answer(
                message, self.strings["photo_invoice_saved"].format(num=num),
            )

        elif cmd == "res":
            if len(parts) < 3:
                await utils.answer(message, self.strings["res_invalid"].format(prefix=prefix))
                return
            try:
                num = int(parts[1])
                if num < 1 or num > 5:
                    raise ValueError
            except ValueError:
                await utils.answer(message, self.strings["res_invalid"].format(prefix=prefix))
                return
            res_text = " ".join(parts[2:])
            width, height = self._extract_resolution(res_text)
            if not width or not height:
                await utils.answer(message, self.strings["res_invalid"].format(prefix=prefix))
                return
            self._texts[f"res_{num}_w"] = width
            self._texts[f"res_{num}_h"] = height
            self._save_texts()
            await utils.answer(
                message, self.strings["res_saved"].format(num=num, width=width, height=height),
            )

        elif cmd == "price":
            if len(parts) < 3:
                await utils.answer(message, self.strings["price_invalid"])
                return
            try:
                num = int(parts[1])
                if num < 1 or num > 5:
                    raise ValueError
            except ValueError:
                await utils.answer(message, self.strings["price_invalid"])
                return
            try:
                price = int(parts[2])
                if price <= 0:
                    raise ValueError
            except ValueError:
                await utils.answer(message, self.strings["price_invalid"])
                return
            self._texts[f"price_{num}"] = price
            self._save_texts()
            await utils.answer(
                message, self.strings["price_saved"].format(num=num, price=price),
            )

        elif cmd == "lg":
            if len(parts) < 2:
                await utils.answer(message, self.strings["log_limit_invalid"])
                return
            try:
                limit = int(parts[1])
                if limit < 1:
                    raise ValueError
            except ValueError:
                await utils.answer(message, self.strings["log_limit_invalid"])
                return
            self._max_action_log = limit
            self._save_log_limits()
            self._trim_logs()
            await utils.answer(
                message,
                self.strings["log_limit_saved"].format(log_type="global", limit=limit),
            )

        elif cmd == "ls":
            if len(parts) < 2:
                await utils.answer(message, self.strings["log_limit_invalid"])
                return
            try:
                limit = int(parts[1])
                if limit < 1:
                    raise ValueError
            except ValueError:
                await utils.answer(message, self.strings["log_limit_invalid"])
                return
            self._max_stars_log = limit
            self._save_log_limits()
            self._trim_logs()
            await utils.answer(
                message,
                self.strings["log_limit_saved"].format(log_type="stars", limit=limit),
            )

        else:
            await utils.answer(message, self.strings["help"].format(prefix=prefix))

    async def _handle_start(self, event):
        if not self._is_dm(event):
            return
        user_id = event.sender_id
        self._add_user(user_id)
        self._log_action("start", user_id)
        main_text = self._get_text("main")
        main_photo = self._get_photo_main()
        buttons = [
            [Button.inline(self._get_button_text(i), data=f"device_{i}")]
            for i in range(1, self._button_count + 1)
        ]
        try:
            name, username = await self._get_bot_user_info(user_id)
            username_line = f"<b>Username:</b> @{username}\n" if username else ""
            user_link = get_user_link(user_id, name)
            await self._send_log(self.strings["log_bot_started"].format(
                user_link=user_link, user_id=user_id, username_line=username_line,
            ))
        except Exception:
            pass
        if main_photo:
            await self._bot.send_file(
                event.chat_id, main_photo,
                caption=main_text, buttons=buttons, parse_mode="html",
            )
        else:
            await event.reply(main_text, buttons=buttons, parse_mode="html")

    async def _handle_user_message(self, event):
        if not event.raw_text or event.raw_text.startswith("/"):
            return
        user_id = event.sender_id
        if user_id == self._bot_id:
            return
        if not self._is_dm(event):
            return
        self._add_user(user_id)
        try:
            name, username = await self._get_bot_user_info(user_id)
            username_line = f"<b>Username:</b> @{username}\n" if username else ""
            user_link = get_user_link(user_id, name)
            raw = getattr(event, "raw_text", None) or ""
            msg_text = escape_html(raw[:3000]) if raw else "<media>"
            await self._send_log(self.strings["log_user_message"].format(
                user_link=user_link, user_id=user_id,
                username_line=username_line, text=msg_text,
            ))
        except Exception as e:
            logger.error(f"[Market] Failed to log user message: {e}")

    async def _handle_premium(self, event):
        if not self._can_use_command(event):
            return
        user_id = event.sender_id
        self._log_action("command", user_id, "/premium")
        parts = event.raw_text.strip().split()
        if len(parts) != 3:
            await event.reply(self.strings["bot_premium_usage"], parse_mode="html")
            return
        try:
            months = int(parts[1])
            target_user_id = int(parts[2])
        except ValueError:
            await event.reply(self.strings["bot_premium_usage"], parse_mode="html")
            return
        if months not in (3, 6, 12):
            await event.reply(self.strings["bot_premium_invalid_months"], parse_mode="html")
            return
        star_costs = {3: 1000, 6: 1500, 12: 2500}
        try:
            if not self._aiogram_bot:
                raise Exception("Aiogram bot not initialized")
            await self._aiogram_bot.gift_premium_subscription(
                user_id=target_user_id,
                month_count=months,
                star_count=star_costs[months],
                text="Premium subscription",
                text_parse_mode="HTML",
            )
            self._log_stars(
                "premium_sent", user_id,
                f"to {target_user_id}, {months} months, {star_costs[months]} stars",
            )
            await event.reply(
                self.strings["bot_premium_success"].format(
                    user_id=target_user_id, months=months,
                ), parse_mode="html",
            )
        except Exception as e:
            error_str = str(e)
            if "BALANCE_TOO_LOW" in error_str:
                await event.reply(self.strings["bot_premium_low_balance"], parse_mode="html")
            else:
                await event.reply(
                    self.strings["bot_premium_error"].format(error=error_str[:200]),
                    parse_mode="html",
                )

    async def _handle_stat(self, event):
        if not self._can_use_command(event):
            return
        if not self._action_log:
            await event.reply(self.strings["bot_stat_empty"], parse_mode="html")
            return
        log_text = "\n".join(
            self._format_action_entry(e) for e in reversed(self._action_log)
        )
        header = self.strings["bot_stat_header"].format(count=len(self._action_log))
        await event.reply(
            f"{header}\n<blockquote expandable>{log_text}</blockquote>",
            parse_mode="html",
        )

    async def _handle_stars(self, event):
        if not self._can_use_command(event):
            return
        if not self._stars_log:
            await event.reply(self.strings["bot_stars_empty"], parse_mode="html")
            return
        log_text = "\n".join(
            self._format_stars_entry(e) for e in reversed(self._stars_log)
        )
        header = self.strings["bot_stars_header"].format(count=len(self._stars_log))
        await event.reply(
            f"{header}\n<blockquote expandable>{log_text}</blockquote>",
            parse_mode="html",
        )

    async def _handle_balance(self, event):
        if not self._can_use_command(event):
            return
        self._log_action("command", event.sender_id, "/balance")
        balance = await self._get_bot_star_balance()
        if balance is not None:
            await event.reply(
                self.strings["bot_balance"].format(balance=balance), parse_mode="html",
            )
        else:
            await event.reply(self.strings["bot_balance_error"], parse_mode="html")

    async def _handle_refund(self, event):
        if not self._can_use_command(event):
            return
        self._log_action("command", event.sender_id, "/refund")
        parts = event.raw_text.strip().split()
        if len(parts) != 3:
            await event.reply(self.strings["bot_refund_usage"], parse_mode="html")
            return
        try:
            target_user_id = int(parts[1])
            charge_id = parts[2]
        except ValueError:
            await event.reply(self.strings["bot_refund_usage"], parse_mode="html")
            return
        try:
            if not self._aiogram_bot:
                raise Exception("Aiogram bot not initialized")
            await self._aiogram_bot.refund_star_payment(
                user_id=target_user_id,
                telegram_payment_charge_id=charge_id,
            )
            self._log_stars("refund", event.sender_id, f"user {target_user_id}, charge {charge_id}")
            await event.reply(
                self.strings["bot_refund_success"].format(
                    user_id=target_user_id, charge_id=charge_id,
                ), parse_mode="html",
            )
        except Exception as e:
            await event.reply(
                self.strings["bot_refund_error"].format(error=str(e)[:200]),
                parse_mode="html",
            )

    async def _handle_callback(self, event):
        data = event.data.decode()
        user_id = event.sender_id
        self._add_user(user_id)

        if data.startswith("device_"):
            try:
                num = int(data.replace("device_", ""))
            except ValueError:
                return
            self._log_action(f"select_{num}", user_id)
            instruction = self._get_text(f"instruction_{num}")
            instruction_photo = self._get_photo_instruction(num)
            buttons = [
                [Button.inline(self.strings["btn_continue"], data=f"pay_{num}")],
                [Button.inline(self.strings["btn_back"], data="back_main")],
            ]
            try:
                name, username = await self._get_bot_user_info(user_id)
                username_line = f"<b>Username:</b> @{username}\n" if username else ""
                user_link = get_user_link(user_id, name)
                product_name = self._get_button_text(num)
                await self._send_log(self.strings["log_product_selected"].format(
                    user_link=user_link, user_id=user_id,
                    username_line=username_line, product=product_name,
                ))
            except Exception:
                pass
            if instruction_photo:
                await event.delete()
                await self._bot.send_file(
                    event.chat_id, instruction_photo,
                    caption=instruction, buttons=buttons, parse_mode="html",
                )
            else:
                await event.edit(instruction, buttons=buttons, parse_mode="html")

        elif data == "back_main":
            self._log_action("back_main", user_id)
            main_text = self._get_text("main")
            main_photo = self._get_photo_main()
            buttons = [
                [Button.inline(self._get_button_text(i), data=f"device_{i}")]
                for i in range(1, self._button_count + 1)
            ]
            if main_photo:
                await event.delete()
                await self._bot.send_file(
                    event.chat_id, main_photo,
                    caption=main_text, buttons=buttons, parse_mode="html",
                )
            else:
                await event.edit(main_text, buttons=buttons, parse_mode="html")

        elif data.startswith("pay_"):
            try:
                num = int(data.replace("pay_", ""))
            except ValueError:
                return
            title = self._get_text(f"invoice_{num}_1")
            description = self._get_text(f"invoice_{num}_2")
            price = self._get_price(num)
            photo_url = self._get_photo_invoice(num)
            photo_width, photo_height = self._get_resolution(num)
            try:
                if not self._aiogram_bot:
                    raise Exception("Aiogram bot not initialized")
                from aiogram.types import LabeledPrice
                invoice_params = {
                    "chat_id": user_id,
                    "title": title,
                    "description": description,
                    "payload": f"product_{num}_{user_id}",
                    "provider_token": "",
                    "currency": "XTR",
                    "prices": [LabeledPrice(label=f"{price} stars", amount=price)],
                    "start_parameter": f"pay_{num}",
                    "is_flexible": False,
                    "need_name": False,
                    "need_phone_number": False,
                    "need_email": False,
                    "send_email_to_provider": False,
                    "send_phone_number_to_provider": False,
                }
                if photo_url and photo_url.startswith("http"):
                    invoice_params["photo_url"] = photo_url
                    invoice_params["photo_width"] = photo_width
                    invoice_params["photo_height"] = photo_height
                await self._aiogram_bot.send_invoice(**invoice_params)
                self._log_stars("invoice_created", user_id, f"product_{num}, {price} stars")
                await event.answer()
            except Exception as e:
                logger.error(f"Invoice error: {e}")
                await event.answer(f"Error: {str(e)[:100]}", alert=True)

    async def _handle_raw(self, event):
        if isinstance(event, UpdateBotPrecheckoutQuery):
            try:
                from telethon.tl.functions.messages import SetBotPrecheckoutResultsRequest
                await self._bot(SetBotPrecheckoutResultsRequest(
                    query_id=event.query_id, success=True,
                ))
            except Exception as e:
                logger.error(f"Pre-checkout error: {e}")

        elif isinstance(event, UpdateNewMessage):
            msg = event.message
            if not (hasattr(msg, "action") and isinstance(msg.action, MessageActionPaymentSentMe)):
                return
            try:
                if hasattr(msg, "peer_id") and hasattr(msg.peer_id, "user_id"):
                    uid = msg.peer_id.user_id
                elif hasattr(msg, "from_id") and hasattr(msg.from_id, "user_id"):
                    uid = msg.from_id.user_id
                else:
                    return
                amount = getattr(msg.action, "total_amount", 0)
                payload = getattr(msg.action, "payload", b"").decode("utf-8", errors="ignore")
                product_num = 1
                if payload:
                    payload_parts = payload.split("_")
                    if len(payload_parts) >= 2:
                        try:
                            product_num = int(payload_parts[1])
                        except ValueError:
                            pass
                self._log_stars("payment_success", uid, f"product_{product_num}, {amount} stars")
                await self._bot.send_message(uid, self._get_text("success"), parse_mode="html")
                name, username = await self._get_bot_user_info(uid)
                username_line = f"<b>Username:</b> @{username}\n" if username else ""
                user_link = get_user_link(uid, name)
                product_name = self._get_button_text(product_num)
                owner_tag = self._get_owner_tag()
                await self._send_log(self.strings["log_payment_success"].format(
                    user_link=user_link, user_id=uid,
                    username_line=username_line, product=product_name,
                    amount=amount, owner_tag=owner_tag,
                ))
                log_template = self._get_text("log")
                if log_template and log_template != self.strings["default_none"]:
                    username_display = f"@{username}" if username else "no"
                    custom_log = log_template.format(
                        name=escape_html(name), user_id=uid,
                        username=username_display, product=product_name, amount=amount,
                    )
                    await self._send_log(custom_log)
            except Exception as e:
                logger.error(f"Payment success handler error: {e}")

    async def on_unload(self):
        await self._stop_bot()
        if self._session_dir:
            self._clean_dir(self._session_dir)
        if self._export_dir:
            self._clean_dir(self._export_dir)
