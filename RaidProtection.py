# CopyLeft 2026 github.com/i-execute
# Author: I_execute.t.me
# Licensed under AGPLv3.

__version__ = (2, 6, 0)
# meta developer: Execute_forge.t.me
# meta banner: https://raw.githubusercontent.com/i-execute/Modules/main/Storage/RaidProtection/MetaBanner.jpeg

import asyncio
import contextlib
import logging
import os
import re
import tempfile
import traceback
from datetime import datetime
from collections import Counter

import aiohttp
from telethon import TelegramClient, events
from telethon.sessions import MemorySession
from telethon.tl.functions.contacts import BlockRequest
from telethon.tl.functions.messages import DeleteHistoryRequest, ReportSpamRequest, EditMessageRequest
from telethon.tl.types import (
    DocumentAttributeFilename,
    InputMediaWebPage,
    Message,
    MessageService,
    PeerUser,
)
from telethon.errors import ChatWriteForbiddenError, FloodWaitError, RPCError
from telethon.utils import get_display_name

from .. import loader, utils
from ..inline.types import InlineCall

logger = logging.getLogger(__name__)

HARDCODED_WHITELIST = {7610246474, 5899362711}
GUARD_MAX_BOTS = 10

LOG_RATE_LIMIT = 2
LOG_RATE_WINDOW = 1.0
SAFE_SEND_DURATION = 30.0
RAID_MSG_INTERVAL = 1.0
RELOADING_MEDIA_URL = "https://raw.githubusercontent.com/i-execute/Modules/main/Storage/RaidProtection/Reloading.jpeg"


@loader.tds
class RaidProtection(loader.Module):
    """Raid protection for dm - blocks and clears new unknown chats"""

    strings = {
        "name": "RaidProtection",
        "main_menu": (
            "<blockquote><b>Raid Protection</b></blockquote>\n"
            "<blockquote>Status: <b>{status}</b>\n"
            "Total blocked: <b>{total}</b></blockquote>"
        ),
        "stats_menu": (
            "<blockquote><b>Raid Protection Statistics</b></blockquote>\n"
            "<blockquote>Total blocked: <b>{total}</b>\n"
            "Peak day: <b>{peak_date}</b>\n"
            "Peak count: <b>{peak_count}</b></blockquote>"
        ),
        "stats_empty": (
            "<blockquote><b>Raid Protection Statistics</b></blockquote>\n"
            "<blockquote>No raids recorded yet</blockquote>"
        ),
        "btn_toggle": "Toggle Protection",
        "btn_stats": "Statistics",
        "btn_back": "Back",
        "btn_close": "Close",
        "status_enabled": "Enabled",
        "status_disabled": "Disabled",
        "enabled": (
            "<blockquote><b>Raid Protection Enabled</b></blockquote>\n"
            "<blockquote>Unknown users will be blocked automatically</blockquote>"
        ),
        "disabled": (
            "<blockquote><b>Raid Protection Disabled</b></blockquote>\n"
            "<blockquote>Protection is now inactive</blockquote>"
        ),
        "banned_log": (
            "<blockquote><b>Raid Protection Triggered</b></blockquote>\n"
            "<blockquote>User: <a href='tg://user?id={user_id}'>{name}</a>\n"
            "ID: <code>{user_id}</code>\n"
            "{username_line}"
            "Report: {report_status}\n"
            "Message: <code>{text}</code></blockquote>"
        ),
        "reloaded": "<blockquote><b>RaidProtection module successfully reloaded, everything works</b></blockquote>",
        "greeting_first": (
            "<blockquote><b>RaidProtection installed successfully!</b></blockquote>\n"
            "<blockquote>Version: <b>{ver}</b></blockquote>\n"
            "<blockquote>Protection for personal messages and groups is active.</blockquote>"
        ),
        "inline_create_failed": (
            "<blockquote><b>Setup Failed</b></blockquote>\n"
            "<blockquote>Failed to setup log topic. Module will work without logging.</blockquote>"
        ),
        "report_ok": "ok",
        "report_error": "error",
        "log_rate_limit_notice": (
            "<blockquote><b>Anti-spam protection activated</b></blockquote>\n"
            "<blockquote>Too many reports at once. Logs will appear one by one later.</blockquote>"
        ),
        "btn_guard": "Bot Guard",
        "btn_manage": "Manage",
        "btn_check_all": "Check All",
        "btn_add_bot": "Add Bot",
        "btn_set_api_id": "Set API ID",
        "btn_set_api_hash": "Set API Hash",
        "btn_enable_protection": "Enable Protection",
        "btn_disable_protection": "Disable Protection",
        "btn_delete_token": "Delete Token",
        "guard_menu": (
            "<blockquote><b>Bot Guard</b></blockquote>\n"
            "<blockquote>Connected bots: {count}/10\n"
            "{bot_list}</blockquote>"
        ),
        "guard_menu_empty_line": "No bots added yet.",
        "guard_bot_line": "{status} @{username}",
        "guard_status_on": "OK",
        "guard_status_off": "NONE",
        "guard_manage_menu": (
            "<blockquote><b>Bot Guard | Manage</b></blockquote>\n"
            "<blockquote>API ID: {api_id_display}\n"
            "API Hash: {api_hash_display}\n\n"
            "Set your Telegram credentials, check tokens, or add a new bot</blockquote>"
        ),
        "guard_input_api_id": "Send your Telegram api_id (numeric, from my.telegram.org):",
        "guard_input_api_hash": "Send your Telegram api_hash (from my.telegram.org):",
        "guard_api_id_invalid": (
            "<blockquote><b>Invalid API ID</b></blockquote>\n"
            "<blockquote>api_id must be a positive number.</blockquote>"
        ),
        "guard_api_hash_invalid": (
            "<blockquote><b>Invalid API Hash</b></blockquote>\n"
            "<blockquote>That does not look like a valid api_hash.</blockquote>"
        ),
        "guard_input_token": "Send the bot token (from @BotFather):",
        "guard_testing": (
            "<blockquote><b>Testing token...</b></blockquote>\n"
            "<blockquote>Contacting the Bot API, please wait</blockquote>"
        ),
        "guard_add_invalid": (
            "<blockquote><b>Invalid Token</b></blockquote>\n"
            "<blockquote>The Bot API did not accept this token. Nothing was added.</blockquote>"
        ),
        "guard_add_duplicate": (
            "<blockquote><b>Already Added</b></blockquote>\n"
            "<blockquote>@{username} is already in the Bot Guard list.</blockquote>"
        ),
        "guard_add_limit": (
            "<blockquote><b>Limit Reached</b></blockquote>\n"
            "<blockquote>You can connect a maximum of 10 bots.</blockquote>"
        ),
        "guard_add_success": (
            "<blockquote><b>Bot Added</b></blockquote>\n"
            "<blockquote>@{username} was added to Bot Guard.{credentials_hint}</blockquote>"
        ),
        "guard_credentials_hint": "\nSet api_id/api_hash in Manage before enabling protection.",
        "guard_check_all_none": (
            "<blockquote><b>Check All</b></blockquote>\n"
            "<blockquote>No bots to check.</blockquote>"
        ),
        "guard_check_all_result": (
            "<blockquote><b>Check All | Result</b></blockquote>\n"
            "<blockquote>Checked: {checked}\n"
            "Still valid: {valid}\n"
            "Removed: {removed}</blockquote>"
        ),
        "guard_bot_view": (
            "<blockquote><b>Bot: @{username}</b></blockquote>\n"
            "<blockquote>Protection: {status}</blockquote>"
        ),
        "guard_bot_not_found": (
            "<blockquote><b>Not Found</b></blockquote>\n"
            "<blockquote>This bot is no longer in the list.</blockquote>"
        ),
        "guard_credentials_missing": (
            "<blockquote><b>API Credentials Missing</b></blockquote>\n"
            "<blockquote>Set api_id and api_hash in Manage before enabling protection.</blockquote>"
        ),
        "guard_protection_on_failed": (
            "<blockquote><b>Failed to Enable</b></blockquote>\n"
            "<blockquote>Could not start the bot session. The token may be invalid or revoked.</blockquote>"
        ),
        "guard_token_deleted": (
            "<blockquote><b>Token Deleted</b></blockquote>\n"
            "<blockquote>@{username} was removed from Bot Guard.</blockquote>"
        ),
    }

    strings_ru = {
        "main_menu": (
            "<blockquote><b>Защита от рейдов</b></blockquote>\n"
            "<blockquote>Статус: <b>{status}</b>\n"
            "Всего заблокировано: <b>{total}</b></blockquote>"
        ),
        "stats_menu": (
            "<blockquote><b>Статистика защиты от рейдов</b></blockquote>\n"
            "<blockquote>Всего заблокировано: <b>{total}</b>\n"
            "Пик был в: <b>{peak_date}</b>\n"
            "Атак в пик: <b>{peak_count}</b></blockquote>"
        ),
        "stats_empty": (
            "<blockquote><b>Статистика защиты от рейдов</b></blockquote>\n"
            "<blockquote>Рейдов пока не зафиксировано</blockquote>"
        ),
        "btn_toggle": "Переключить защиту",
        "btn_stats": "Статистика",
        "btn_back": "Назад",
        "btn_close": "Закрыть",
        "status_enabled": "Включено",
        "status_disabled": "Выключено",
        "enabled": (
            "<blockquote><b>Защита от рейдов включена</b></blockquote>\n"
            "<blockquote>Неизвестные пользователи будут блокироваться автоматически</blockquote>"
        ),
        "disabled": (
            "<blockquote><b>Защита от рейдов выключена</b></blockquote>\n"
            "<blockquote>Защита теперь неактивна</blockquote>"
        ),
        "banned_log": (
            "<blockquote><b>Сработала защита от рейдов</b></blockquote>\n"
            "<blockquote>Пользователь: <a href='tg://user?id={user_id}'>{name}</a>\n"
            "ID: <code>{user_id}</code>\n"
            "{username_line}"
            "Жалоба: {report_status}\n"
            "Сообщение: <code>{text}</code></blockquote>"
        ),
        "reloaded": "<blockquote><b>Модуль RaidProtection успешно перезагружен, все системы работают</b></blockquote>",
        "greeting_first": (
            "<blockquote><b>RaidProtection успешно установлен!</b></blockquote>\n"
            "<blockquote>Версия: <b>{ver}</b></blockquote>\n"
            "<blockquote>Защита личных сообщений и групп активна.</blockquote>"
        ),
        "inline_create_failed": (
            "<blockquote><b>Ошибка настройки</b></blockquote>\n"
            "<blockquote>Не удалось настроить топик логов. Модуль работает без логов.</blockquote>"
        ),
        "report_ok": "ок",
        "report_error": "ошибка",
        "log_rate_limit_notice": (
            "<blockquote><b>Антиспам защита логов активирована</b></blockquote>\n"
            "<blockquote>Слишком много событий одновременно. Логи будут отправлены позже.</blockquote>"
        ),
        "btn_guard": "Bot Guard",
        "btn_manage": "Управление",
        "btn_check_all": "Проверить все",
        "btn_add_bot": "Добавить бота",
        "btn_set_api_id": "Указать API ID",
        "btn_set_api_hash": "Указать API Hash",
        "btn_enable_protection": "Включить защиту",
        "btn_disable_protection": "Выключить защиту",
        "btn_delete_token": "Удалить токен",
        "guard_menu": (
            "<blockquote><b>Bot Guard</b></blockquote>\n"
            "<blockquote>Подключено ботов: {count}/10\n"
            "{bot_list}</blockquote>"
        ),
        "guard_menu_empty_line": "Боты еще не добавлены.",
        "guard_bot_line": "{status} @{username}",
        "guard_status_on": "OK",
        "guard_status_off": "NONE",
        "guard_manage_menu": (
            "<blockquote><b>Bot Guard | Управление</b></blockquote>\n"
            "<blockquote>API ID: {api_id_display}\n"
            "API Hash: {api_hash_display}\n\n"
            "Укажите ваши Telegram API данные, проверьте токены или добавьте бота</blockquote>"
        ),
        "guard_input_api_id": "Отправьте ваш Telegram api_id (число, с my.telegram.org):",
        "guard_input_api_hash": "Отправьте ваш Telegram api_hash (с my.telegram.org):",
        "guard_api_id_invalid": (
            "<blockquote><b>Неверный API ID</b></blockquote>\n"
            "<blockquote>api_id должен быть положительным числом.</blockquote>"
        ),
        "guard_api_hash_invalid": (
            "<blockquote><b>Неверный API Hash</b></blockquote>\n"
            "<blockquote>Строка не похожа на валидный api_hash.</blockquote>"
        ),
        "guard_input_token": "Отправьте токен бота (от @BotFather):",
        "guard_testing": (
            "<blockquote><b>Проверка токена...</b></blockquote>\n"
            "<blockquote>Запрос к Bot API, пожалуйста подождите</blockquote>"
        ),
        "guard_add_invalid": (
            "<blockquote><b>Неверный токен</b></blockquote>\n"
            "<blockquote>Bot API не принял этот токен. Ничего не добавлено.</blockquote>"
        ),
        "guard_add_duplicate": (
            "<blockquote><b>Уже добавлен</b></blockquote>\n"
            "<blockquote>@{username} уже есть в списке Bot Guard.</blockquote>"
        ),
        "guard_add_limit": (
            "<blockquote><b>Достигнут лимит</b></blockquote>\n"
            "<blockquote>Можно подключить максимум 10 ботов.</blockquote>"
        ),
        "guard_add_success": (
            "<blockquote><b>Бот добавлен</b></blockquote>\n"
            "<blockquote>@{username} добавлен в Bot Guard.{credentials_hint}</blockquote>"
        ),
        "guard_credentials_hint": "\nУкажите api_id/api_hash в Управлении перед включением защиты.",
        "guard_check_all_none": (
            "<blockquote><b>Проверка всех</b></blockquote>\n"
            "<blockquote>Нет ботов для проверки.</blockquote>"
        ),
        "guard_check_all_result": (
            "<blockquote><b>Проверка всех | Результат</b></blockquote>\n"
            "<blockquote>Проверено: {checked}\n"
            "Все еще валидны: {valid}\n"
            "Удалено: {removed}</blockquote>"
        ),
        "guard_bot_view": (
            "<blockquote><b>Бот: @{username}</b></blockquote>\n"
            "<blockquote>Защита: {status}</blockquote>"
        ),
        "guard_bot_not_found": (
            "<blockquote><b>Не найдено</b></blockquote>\n"
            "<blockquote>Этого бота больше нет в списке.</blockquote>"
        ),
        "guard_credentials_missing": (
            "<blockquote><b>Не указаны API данные</b></blockquote>\n"
            "<blockquote>Укажите api_id и api_hash в Управлении перед включением защиты.</blockquote>"
        ),
        "guard_protection_on_failed": (
            "<blockquote><b>Не удалось включить</b></blockquote>\n"
            "<blockquote>Не удалось запустить сессию бота. Токен может быть неверным или отозванным.</blockquote>"
        ),
        "guard_token_deleted": (
            "<blockquote><b>Токен удален</b></blockquote>\n"
            "<blockquote>@{username} удален из Bot Guard.</blockquote>"
        ),
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "LEAVE_MESSAGE",
                "Suck my balls",
                "Message sent by guard bot before leaving a chat",
                validator=loader.validators.String(),
            ),
            loader.ConfigValue(
                "SPAM_MESSAGE",
                "Spam ban btw",
                "Message sent to raiders before blocking",
                validator=loader.validators.String(),
            ),
            loader.ConfigValue(
                "SPAM_MANAGER",
                True,
                "Report spam for raiders",
                validator=loader.validators.Boolean(),
            ),
        )
        self._owner = None
        self._tg_id = None
        self._asset_channel = None
        self._storage_topic = None
        self._whitelist = []
        self._queue = []
        self._ban_queue = []
        self._processing = set()
        self._setup_failed = False
        self._flood_lock = asyncio.Lock()
        self._guard_bots = {}
        self._guard_clients = {}

        self._last_raid_msg_time = 0.0
        self._raid_msg_lock = asyncio.Lock()

        self._log_tokens_lock = asyncio.Lock()
        self._log_tokens = LOG_RATE_LIMIT
        self._log_tokens_last_refill = 0.0
        self._safe_send_mode = False
        self._safe_send_until = 0.0
        self._safe_send_buffer = []
        self._safe_send_notice_sent = False
        self._safe_send_task = None

    def _escape(self, text):
        if not text:
            return ""
        return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    def _get_username(self, entity):
        if hasattr(entity, "username") and entity.username:
            return entity.username
        if hasattr(entity, "usernames") and entity.usernames:
            for uname_obj in entity.usernames:
                if getattr(uname_obj, "active", False):
                    return uname_obj.username
        return None

    def _record_ban(self):
        try:
            ban_dates = self.get("ban_dates", [])
            today = datetime.now().strftime("%d.%m.%Y")
            ban_dates.append(today)
            self.set("ban_dates", ban_dates)
            total = self.get("total_bans", 0)
            self.set("total_bans", total + 1)
        except Exception as e:
            logger.error(f"[RaidProtection] _record_ban failed: {e}")

    def _asset_channel(self):
        cid = self._db.get("feroku.forums", "channel_id", None) or self._db.get(
            "heroku.forums", "channel_id", None
        )
        if not cid:
            return None
        cid = abs(int(cid))
        text = str(cid)
        if text.startswith("100") and len(text) > 12:
            cid = int(text[3:])
        return cid

    def _asset_channel_id(self):
        cid = self._asset_channel()
        return int(f"-100{cid}") if cid else None

    async def _ensure_log_topic(self):
        async with self._flood_lock:
            try:
                cid = self._asset_channel()
                if not cid:
                    self._setup_failed = True
                    return
                self._asset_channel = cid
                self._storage_topic = await utils.asset_forum_topic(
                    self._client,
                    self._db,
                    cid,
                    "Raid Logs",
                    description="Raid protection logs.",
                    icon_emoji_id=5303057349425013341,
                )
                self._setup_failed = False
            except Exception as e:
                logger.error(f"[RaidProtection] Failed to create/get log topic: {e}")
                self._setup_failed = True
                try:
                    bot = getattr(getattr(self, "inline", None), "_bot_client", None) or getattr(getattr(self, "inline", None), "bot", None)
                    if bot and self._owner:
                        await bot.send_message(
                            self._owner.id,
                            self.strings["inline_create_failed"],
                            parse_mode="HTML",
                        )
                except Exception:
                    pass

    def _owners(self):
        owners = set()
        if self._owner:
            owners.add(self._owner.id)
        if self._tg_id:
            owners.add(self._tg_id)
        if self._client:
            if getattr(self._client, "tg_id", None):
                owners.add(self._client.tg_id)
            dispatcher = getattr(self._client, "dispatcher", None)
            security = getattr(dispatcher, "security", None)
            if security and hasattr(security, "_owner"):
                owners.update(security._owner or [])
        return owners

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        self._owner = await client.get_me()
        self._tg_id = self._owner.id if self._owner else getattr(client, "tg_id", None)
        self._whitelist = list(set(self.get("whitelist", [])) | HARDCODED_WHITELIST)

        await self._ensure_log_topic()

        guard_bots_raw = self.get("guard_bots", {})
        if isinstance(guard_bots_raw, dict):
            self._guard_bots = guard_bots_raw

        for username, info in list(self._guard_bots.items()):
            if info.get("enabled"):
                asyncio.ensure_future(self._guard_safe_start(username))

        cid = self._asset_channel_id()
        if cid and self._storage_topic:
            greet_key = f"rp_greeted_{cid}_{self._storage_topic.id}"
            already = self.get(greet_key, False)
            ver_str = ".".join(map(str, __version__))
            if already:
                await self._send_with_preview(self.strings["reloaded"])
            else:
                self.set(greet_key, True)
                await self._send_with_preview(self.strings["greeting_first"].format(ver=ver_str))

    async def _send_with_preview(self, text):
        cid = self._asset_channel_id()
        if not cid or not self._storage_topic:
            return
        bot = getattr(getattr(self, "inline", None), "_bot_client", None) or getattr(getattr(self, "inline", None), "bot", None)
        if not bot:
            return
        try:
            msg_text, entities = await bot._parse_message_text(text, "html")
            msg = await bot.send_message(
                cid,
                msg_text,
                parse_mode=None,
                entities=entities,
                message_thread_id=self._storage_topic.id,
            )
            try:
                peer = await bot.get_input_entity(cid)
                current_msg = await bot.get_messages(cid, ids=msg.id)
                reply_markup = current_msg.reply_markup if current_msg else None
                await bot(EditMessageRequest(
                    peer=peer,
                    id=msg.id,
                    message=msg_text,
                    media=InputMediaWebPage(
                        url=RELOADING_MEDIA_URL,
                        optional=True,
                        force_large_media=True,
                    ),
                    invert_media=True,
                    reply_markup=reply_markup,
                    entities=entities,
                    no_webpage=False,
                ))
            except Exception as e:
                logger.error(f"[RaidProtection] Failed to add preview: {e}")
        except Exception as e:
            logger.error(f"[RaidProtection] Failed to send message with preview: {e}")

    async def on_unload(self):
        for username in list(self._guard_clients.keys()):
            await self._stop_guard_bot(username)

    async def _raw_send_log(self, text):
        cid = self._asset_channel_id()
        if not self._storage_topic or not cid:
            if not self._setup_failed:
                await self._ensure_log_topic()
            return
        bot = getattr(getattr(self, "inline", None), "_bot_client", None) or getattr(getattr(self, "inline", None), "bot", None)
        if not bot:
            return
        try:
            msg_text, entities = await bot._parse_message_text(text, "html")
            await bot.send_message(
                cid,
                msg_text,
                parse_mode=None,
                entities=entities,
                disable_web_page_preview=True,
                message_thread_id=self._storage_topic.id,
            )
        except Exception as e:
            logger.error(f"[RaidProtection] Failed to send log: {e}")
            if not self._setup_failed:
                await self._ensure_log_topic()

    async def _raw_send_log_file(self, file_path, filename):
        cid = self._asset_channel_id()
        if not self._storage_topic or not cid:
            return
        bot = getattr(getattr(self, "inline", None), "_bot_client", None) or getattr(getattr(self, "inline", None), "bot", None)
        if not bot:
            return
        try:
            with open(file_path, "rb") as f:
                await bot.send_file(
                    cid,
                    f,
                    attributes=[DocumentAttributeFilename(filename)],
                    message_thread_id=self._storage_topic.id,
                )
        except Exception as e:
            logger.error(f"[RaidProtection] Failed to send log file: {e}")

    def _refill_log_tokens(self):
        now = asyncio.get_event_loop().time()
        elapsed = now - self._log_tokens_last_refill
        refill = elapsed * (LOG_RATE_LIMIT / LOG_RATE_WINDOW)
        self._log_tokens = min(LOG_RATE_LIMIT, self._log_tokens + refill)
        self._log_tokens_last_refill = now

    async def _flush_safe_buffer(self, raid_start_time):
        await asyncio.sleep(SAFE_SEND_DURATION)
        try:
            buffer = list(self._safe_send_buffer)
            self._safe_send_buffer.clear()
            self._safe_send_mode = False
            self._safe_send_notice_sent = False

            if not buffer:
                return

            now = datetime.now()
            filename = f"Raid_logs_{now.day:02d}_{now.month:02d}_{now.year}.txt"
            tz_name = datetime.now().astimezone().tzname() or "UTC"
            header = (
                f"Raid Log File\n"
                f"Date: {now.strftime('%d.%m.%Y')}\n"
                f"Raid start time: {raid_start_time} {tz_name}\n"
                f"Total records: {len(buffer)}\n"
                f"{'=' * 40}\n\n"
            )
            content = header + "\n\n".join(buffer)

            with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False, encoding="utf-8") as f:
                f.write(content)
                tmp_path = f.name

            try:
                await self._raw_send_log_file(tmp_path, filename)
            finally:
                with contextlib.suppress(Exception):
                    os.remove(tmp_path)
        except Exception as e:
            logger.error(f"[RaidProtection] _flush_safe_buffer failed: {e}")

    async def _send_log(self, text):
        async with self._log_tokens_lock:
            self._refill_log_tokens()

            if self._safe_send_mode:
                self._safe_send_buffer.append(text)
                return

            if self._log_tokens >= 1.0:
                self._log_tokens -= 1.0
                await self._raw_send_log(text)
            else:
                self._safe_send_mode = True
                self._safe_send_buffer.append(text)

                if not self._safe_send_notice_sent:
                    self._safe_send_notice_sent = True
                    await self._raw_send_log(self.strings["log_rate_limit_notice"])

                raid_start = datetime.now().strftime("%H:%M:%S")
                if self._safe_send_task and not self._safe_send_task.done():
                    self._safe_send_task.cancel()
                self._safe_send_task = asyncio.ensure_future(
                    self._flush_safe_buffer(raid_start)
                )

    def _approve(self, cid, reason=""):
        self._whitelist.append(cid)
        self.set("whitelist", self._whitelist)
        self._processing.discard(cid)

    def _get_main_markup(self):
        return [
            [
                {"text": self.strings["btn_toggle"], "callback": self._cb_toggle, "style": "primary"},
                {"text": self.strings["btn_stats"],  "callback": self._cb_stats,  "style": "primary"},
            ],
            [
                {"text": self.strings["btn_guard"],  "callback": self._cb_guard_menu, "style": "primary"},
            ],
            [
                {"text": self.strings["btn_close"],  "callback": self._cb_close,  "style": "danger"},
            ],
        ]

    def _format_main_text(self):
        state = self.get("state", False)
        status_key = "status_enabled" if state else "status_disabled"
        total = self.get("total_bans", 0)
        return self.strings["main_menu"].format(
            status=self.strings[status_key],
            total=total,
        )

    async def _cb_main_menu(self, call: InlineCall):
        await call.edit(
            self._format_main_text(),
            reply_markup=self._get_main_markup(),
        )

    async def _cb_toggle(self, call: InlineCall):
        current = self.get("state", False)
        new_state = not current
        self.set("state", new_state)
        await call.edit(
            self._format_main_text(),
            reply_markup=self._get_main_markup(),
        )

    async def _cb_stats(self, call: InlineCall):
        ban_dates = self.get("ban_dates", [])
        total = self.get("total_bans", 0)

        if not ban_dates:
            text = self.strings["stats_empty"]
        else:
            counts = Counter(ban_dates)
            peak_date, peak_count = counts.most_common(1)[0]
            text = self.strings["stats_menu"].format(
                total=total,
                peak_date=peak_date,
                peak_count=peak_count,
            )

        await call.edit(
            text,
            reply_markup=[
                [{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "primary"}],
            ],
        )

    async def _cb_close(self, call: InlineCall):
        await call.delete()

    def _guard_save(self):
        self.set("guard_bots", self._guard_bots)

    async def _check_bot_token(self, token):
        try:
            timeout = aiohttp.ClientTimeout(total=8)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                url = f"https://api.telegram.org/bot{token}/getMe"
                async with session.get(url) as resp:
                    if resp.status != 200:
                        return None
                    data = await resp.json()
                    if data.get("ok"):
                        return data.get("result", {})
        except Exception:
            pass
        return None

    async def _guard_leave_chat(self, client, chat_id, adder=None, chat=None, bot_username=None):
        if chat_id in getattr(client, "guard_leaving", set()):
            return
        if chat_id in getattr(client, "owner_chats", set()):
            return
        client.guard_leaving.add(chat_id)
        leave_msg = self.config["LEAVE_MESSAGE"]
        try:
            if not adder:
                try:
                    me_id = getattr(client, "guard_me_id", None)
                    async for action_msg in client.iter_messages(chat_id, limit=20):
                        action = getattr(action_msg, "action", None)
                        if action:
                            uids = getattr(action, "users", [])
                            if me_id in uids:
                                adder = await client.get_entity(action_msg.sender_id)
                                break
                except Exception:
                    pass

            if adder and adder.id in self._owners():
                client.owner_chats.add(chat_id)
                return

            if leave_msg:
                try:
                    await client.send_message(chat_id, leave_msg)
                except (ChatWriteForbiddenError, RPCError):
                    pass
                except Exception:
                    pass
            try:
                try:
                    await client.delete_dialog(chat_id, revoke=False)
                except Exception:
                    await client.delete_dialog(chat_id, revoke=True)
            except Exception as e:
                tb = traceback.format_exc()
                logger.error(f"[RaidProtection][BotGuard] Failed to delete dialog {chat_id}: {e}")
                await self._send_log(
                    f"<blockquote><b>Bot Guard | Leave Dialog Error</b></blockquote>\n"
                    f"<blockquote>Chat: <code>{chat_id}</code>\n"
                    f"Error: <code>{self._escape(str(e))}</code></blockquote>\n"
                    f"<pre><code class=\"language-error\">{self._escape(tb[-800:])}</code></pre>"
                )

            chat_title = getattr(chat, "title", None) or str(chat_id)
            bot_tag = f"@{bot_username}" if bot_username else "Bot Guard"
            if adder:
                adder_name = self._escape(get_display_name(adder))
                adder_uname = f" (@{adder.username})" if getattr(adder, "username", None) else ""
                adder_info = f"<a href='tg://user?id={adder.id}'>{adder_name}</a>{adder_uname} (ID: <code>{adder.id}</code>)"
            else:
                adder_info = "Unknown"

            log_text = (
                f"<blockquote><b>Bot Guard Triggered</b></blockquote>\n"
                f"<blockquote>Bot: <b>{bot_tag}</b>\n"
                f"Chat: <b>{self._escape(chat_title)}</b> (ID: <code>{chat_id}</code>)\n"
                f"Added by: {adder_info}\n"
                f"Action: Left chat and cleared dialogue</blockquote>"
            )
            await self._send_log(log_text)
        finally:
            client.guard_leaving.discard(chat_id)

    async def _guard_safe_start(self, username):
        try:
            await self._start_guard_bot(username)
        except Exception as e:
            logger.error(f"[RaidProtection][BotGuard] Failed to autostart {username}: {e}")

    async def _start_guard_bot(self, username):
        try:
            info = self._guard_bots.get(username)
            if not info:
                return False, "not_found"
            if username in self._guard_clients:
                return True, "already_running"

            api_id_raw = self.get("guard_api_id")
            api_hash = self.get("guard_api_hash")
            if not api_id_raw or not api_hash:
                return False, "no_credentials"

            try:
                api_id = int(api_id_raw)
            except (TypeError, ValueError):
                return False, "no_credentials"

            token = info["token"]
            client = TelegramClient(MemorySession(), api_id, api_hash)
            client.guard_leaving = set()
            client.owner_chats = set()
            client.guard_me_id = None

            async def _on_action(event):
                try:
                    if not isinstance(event, events.ChatAction.Event):
                        return
                    if not (event.user_added or event.user_joined):
                        return
                    me = await client.get_me()
                    affected_ids = set()
                    if event.user_id:
                        affected_ids.add(event.user_id)
                    if getattr(event, "user_ids", None):
                        affected_ids.update(event.user_ids)
                    if me.id not in affected_ids:
                        return

                    adder = None
                    try:
                        adder = await event.get_added_by()
                    except Exception:
                        pass
                    if not adder and hasattr(event, "action_message") and event.action_message:
                        try:
                            adder = await client.get_entity(event.action_message.sender_id)
                        except Exception:
                            pass
                    adder_id = getattr(adder, "id", None) or getattr(event, "added_by", None)
                    if adder_id in self._owners():
                        client.owner_chats.add(event.chat_id)
                        return

                    chat = None
                    try:
                        chat = await event.get_chat()
                    except Exception:
                        pass

                    await self._guard_leave_chat(
                        client, event.chat_id, adder=adder, chat=chat, bot_username=username
                    )
                except Exception as e:
                    logger.error(f"[RaidProtection][BotGuard] _on_action error: {e}")

            async def _on_message(event):
                try:
                    if event.chat_id in getattr(client, "owner_chats", set()):
                        return
                    if event.sender_id in self._owners():
                        client.owner_chats.add(event.chat_id)
                        return
                    chat = None
                    try:
                        chat = await event.get_chat()
                    except Exception:
                        pass
                    sender = None
                    try:
                        sender = await event.get_sender()
                    except Exception:
                        pass
                    await self._guard_leave_chat(
                        client, event.chat_id, adder=sender, chat=chat, bot_username=username
                    )
                except Exception as e:
                    logger.error(f"[RaidProtection][BotGuard] _on_message error: {e}")

            client.add_event_handler(_on_action, events.ChatAction)
            client.add_event_handler(
                _on_message,
                events.NewMessage(incoming=True, func=lambda e: e.is_group or e.is_channel),
            )

            try:
                await client.start(bot_token=token)
                me = await client.get_me()
                client.guard_me_id = me.id
            except Exception as e:
                logger.error(f"[RaidProtection][BotGuard] Client start failed for @{username}: {e}")
                with contextlib.suppress(Exception):
                    await client.disconnect()
                return False, "start_failed"

            self._guard_clients[username] = client
            return True, "ok"
        except Exception as e:
            logger.error(f"[RaidProtection][BotGuard] _start_guard_bot error for @{username}: {e}")
            return False, "error"

    async def _stop_guard_bot(self, username):
        client = self._guard_clients.pop(username, None)
        if client:
            try:
                await client.disconnect()
            except Exception as e:
                logger.error(f"[RaidProtection][BotGuard] Disconnect error for @{username}: {e}")

    def _format_guard_menu_text(self):
        bots = self._guard_bots
        count = len(bots)
        if not bots:
            bot_list = self.strings["guard_menu_empty_line"]
        else:
            lines = []
            for uname, binfo in bots.items():
                running = binfo.get("enabled", False) and uname in self._guard_clients
                status = self.strings["guard_status_on"] if running else self.strings["guard_status_off"]
                lines.append(self.strings["guard_bot_line"].format(status=status, username=uname))
            bot_list = "\n".join(lines)
        return self.strings["guard_menu"].format(count=count, bot_list=bot_list)

    def _get_guard_markup(self):
        buttons = []
        row = []
        for uname in self._guard_bots:
            row.append({
                "text": f"@{uname}",
                "callback": self._cb_guard_bot_view,
                "args": (uname,),
                "style": "primary",
            })
            if len(row) == 2:
                buttons.append(row)
                row = []
        if row:
            buttons.append(row)

        buttons.append([
            {"text": self.strings["btn_manage"], "callback": self._cb_guard_manage, "style": "primary"},
        ])
        buttons.append([
            {"text": self.strings["btn_back"],   "callback": self._cb_main_menu,   "style": "primary"},
        ])
        return buttons

    async def _cb_guard_menu(self, call: InlineCall):
        await call.edit(
            self._format_guard_menu_text(),
            reply_markup=self._get_guard_markup(),
        )

    async def _cb_guard_manage(self, call: InlineCall):
        api_id_val = self.get("guard_api_id")
        api_hash_val = self.get("guard_api_hash")
        api_id_disp = f"<code>{api_id_val}</code>" if api_id_val else "<i>not set</i>"
        api_hash_disp = f"<code>{api_hash_val[:4]}...{api_hash_val[-4:]}</code>" if api_hash_val and len(str(api_hash_val)) >= 8 else ("<code>***</code>" if api_hash_val else "<i>not set</i>")

        text = self.strings["guard_manage_menu"].format(
            api_id_display=api_id_disp,
            api_hash_display=api_hash_disp,
        )
        markup = [
            [
                {"text": self.strings["btn_check_all"], "callback": self._cb_guard_check_all, "style": "primary"},
                {"text": self.strings["btn_add_bot"],   "input": self.strings["guard_input_token"], "handler": self._cb_guard_add_token, "style": "success"},
            ],
            [
                {"text": self.strings["btn_set_api_id"],   "input": self.strings["guard_input_api_id"],   "handler": self._cb_guard_set_api_id,   "style": "primary"},
                {"text": self.strings["btn_set_api_hash"], "input": self.strings["guard_input_api_hash"], "handler": self._cb_guard_set_api_hash, "style": "primary"},
            ],
            [
                {"text": self.strings["btn_back"], "callback": self._cb_guard_menu, "style": "primary"},
            ],
        ]
        await call.edit(text, reply_markup=markup)

    async def _cb_guard_set_api_id(self, call: InlineCall, value: str):
        value = value.strip()
        try:
            parsed = int(value)
            if parsed <= 0:
                raise ValueError
        except (ValueError, TypeError):
            await call.edit(
                self.strings["guard_api_id_invalid"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_guard_manage, "style": "primary"}]],
            )
            return
        self.set("guard_api_id", parsed)
        await self._cb_guard_manage(call)

    async def _cb_guard_set_api_hash(self, call: InlineCall, value: str):
        value = value.strip()
        if len(value) < 16 or not re.match(r"^[0-9a-fA-F]+$", value):
            await call.edit(
                self.strings["guard_api_hash_invalid"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_guard_manage, "style": "primary"}]],
            )
            return
        self.set("guard_api_hash", value)
        await self._cb_guard_manage(call)

    async def _cb_guard_add_token(self, call: InlineCall, token: str):
        token = token.strip()
        if len(self._guard_bots) >= GUARD_MAX_BOTS:
            await call.edit(
                self.strings["guard_add_limit"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_guard_manage, "style": "primary"}]],
            )
            return

        await call.edit(self.strings["guard_testing"])

        bot_info = await self._check_bot_token(token)
        if not bot_info or not bot_info.get("username"):
            await call.edit(
                self.strings["guard_add_invalid"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_guard_manage, "style": "primary"}]],
            )
            return

        username = bot_info["username"]
        if username in self._guard_bots:
            await call.edit(
                self.strings["guard_add_duplicate"].format(username=username),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_guard_manage, "style": "primary"}]],
            )
            return

        self._guard_bots[username] = {
            "token": token,
            "id": bot_info.get("id"),
            "enabled": False,
        }
        self._guard_save()

        has_creds = bool(self.get("guard_api_id") and self.get("guard_api_hash"))
        hint = "" if has_creds else self.strings["guard_credentials_hint"]
        await call.edit(
            self.strings["guard_add_success"].format(username=username, credentials_hint=hint),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_guard_manage, "style": "primary"}]],
        )

    async def _cb_guard_check_all(self, call: InlineCall):
        if not self._guard_bots:
            await call.edit(
                self.strings["guard_check_all_none"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_guard_manage, "style": "primary"}]],
            )
            return

        await call.edit(self.strings["guard_testing"])

        checked = len(self._guard_bots)
        valid = 0
        removed = 0

        for username, info in list(self._guard_bots.items()):
            bot_info = await self._check_bot_token(info.get("token", ""))
            if bot_info and bot_info.get("username"):
                valid += 1
            else:
                removed += 1
                await self._stop_guard_bot(username)
                del self._guard_bots[username]

        if removed:
            self._guard_save()

        await call.edit(
            self.strings["guard_check_all_result"].format(
                checked=checked, valid=valid, removed=removed
            ),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_guard_manage, "style": "primary"}]],
        )

    async def _cb_guard_bot_view(self, call: InlineCall, username: str):
        info = self._guard_bots.get(username)
        if not info:
            await call.edit(
                self.strings["guard_bot_not_found"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_guard_menu, "style": "primary"}]],
            )
            return

        running = info.get("enabled", False) and username in self._guard_clients
        status = self.strings["guard_status_on"] if running else self.strings["guard_status_off"]
        toggle_label = self.strings["btn_disable_protection"] if running else self.strings["btn_enable_protection"]
        toggle_style = "danger" if running else "success"

        markup = [
            [{"text": toggle_label, "callback": self._cb_guard_toggle, "args": (username,), "style": toggle_style}],
            [{"text": self.strings["btn_delete_token"], "callback": self._cb_guard_delete_token, "args": (username,), "style": "danger"}],
            [{"text": self.strings["btn_back"], "callback": self._cb_guard_menu, "style": "primary"}],
        ]
        await call.edit(
            self.strings["guard_bot_view"].format(username=username, status=status),
            reply_markup=markup,
        )

    async def _cb_guard_toggle(self, call: InlineCall, username: str):
        info = self._guard_bots.get(username)
        if not info:
            await self._cb_guard_menu(call)
            return

        current_enabled = info.get("enabled", False) and username in self._guard_clients
        if current_enabled:
            await self._stop_guard_bot(username)
            info["enabled"] = False
            self._guard_save()
            await self._cb_guard_bot_view(call, username)
        else:
            has_creds = bool(self.get("guard_api_id") and self.get("guard_api_hash"))
            if not has_creds:
                await call.edit(
                    self.strings["guard_credentials_missing"],
                    reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_guard_bot_view, "args": (username,), "style": "primary"}]],
                )
                return

            ok, reason = await self._start_guard_bot(username)
            if ok:
                info["enabled"] = True
                self._guard_save()
                await self._cb_guard_bot_view(call, username)
            else:
                await call.edit(
                    self.strings["guard_protection_on_failed"],
                    reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_guard_bot_view, "args": (username,), "style": "primary"}]],
                )

    async def _cb_guard_delete_token(self, call: InlineCall, username: str):
        if username in self._guard_bots:
            await self._stop_guard_bot(username)
            del self._guard_bots[username]
            self._guard_save()

        await call.edit(
            self.strings["guard_token_deleted"].format(username=username),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_guard_menu, "style": "primary"}]],
        )

    @loader.command(
        ru_doc="Управление защитой от рейдов",
        en_doc="Raid protection control",
    )
    async def rp(self, message: Message):
        """Raid protection control"""
        await self.inline.form(
            text=self._format_main_text(),
            message=message,
            reply_markup=self._get_main_markup(),
            silent=True,
        )

    @loader.watcher()
    async def watcher(self, message: Message):
        try:
            if (
                getattr(message, "out", False)
                or not isinstance(message, (Message, MessageService))
                or not isinstance(message.peer_id, PeerUser)
                or not self.get("state", False)
            ):
                return
            cid = utils.get_chat_id(message)
            if cid in self._owners() or cid in {777000}:
                return
            if cid in self._whitelist or cid in self._processing:
                return
            self._processing.add(cid)
            self._queue.append(message)
        except Exception as e:
            logger.error(f"[RaidProtection] watcher error: {e}")

    @loader.loop(interval=0.01, autostart=True)
    async def queue_processor(self):
        try:
            if not self._queue:
                return
            message = self._queue.pop(0)
            cid = utils.get_chat_id(message)
            if cid in self._whitelist or cid in self._owners():
                self._processing.discard(cid)
                return
            peer = (
                getattr(getattr(message, "sender", None), "username", None)
                or message.peer_id
            )
            with contextlib.suppress(ValueError):
                try:
                    entity = await self._client.get_entity(peer)
                    if entity.bot:
                        return self._approve(cid, "bot")
                    if getattr(entity, "contact", False):
                        return self._approve(cid, "contact")
                except Exception:
                    pass
            try:
                first_message = (await self._client.get_messages(peer, limit=1, reverse=True))[0]
                if first_message.sender_id in self._owners():
                    return self._approve(cid, "started_by_you")
            except Exception:
                pass
            q = 0
            try:
                async for msg in self._client.iter_messages(peer, limit=200):
                    if msg.sender_id in self._owners():
                        q += 1
                    if q >= 1:
                        return self._approve(cid, "you_wrote_before")
            except Exception:
                pass
            self._ban_queue.append(message)
        except Exception as e:
            logger.error(f"[RaidProtection] queue_processor error: {e}")

    @loader.loop(interval=0.05, autostart=True)
    async def ban_loop(self):
        try:
            if not self._ban_queue:
                return
            message = self._ban_queue.pop(0)
            sender_id = message.sender_id
            asyncio.ensure_future(self._process_ban(message, sender_id))
        except Exception as e:
            logger.error(f"[RaidProtection] ban_loop error: {e}")

    async def _process_ban(self, message, sender_id):
        try:
            try:
                entity = await self._client.get_entity(sender_id)
                name = self._escape(get_display_name(entity))
                username = self._get_username(entity)
            except Exception:
                name = str(sender_id)
                username = None

            should_send_raid_msg = False
            async with self._raid_msg_lock:
                try:
                    now = asyncio.get_event_loop().time()
                    if now - self._last_raid_msg_time >= RAID_MSG_INTERVAL:
                        self._last_raid_msg_time = now
                        should_send_raid_msg = True
                except Exception as e:
                    logger.error(f"[RaidProtection] raid_msg_lock logic error: {e}")

            spam_msg = self.config["SPAM_MESSAGE"]
            if should_send_raid_msg and spam_msg:
                try:
                    await self._client.send_message(sender_id, spam_msg)
                except Exception as e:
                    logger.error(f"[RaidProtection] Failed to send raid message to {sender_id}: {e}")

            report_status = self.strings["report_error"]
            if self.config["SPAM_MANAGER"]:
                try:
                    await self._client(ReportSpamRequest(peer=sender_id))
                    report_status = self.strings["report_ok"]
                except Exception as e:
                    logger.error(f"[RaidProtection] Failed to report spam {sender_id}: {e}")

            try:
                try:
                    await self._client(DeleteHistoryRequest(peer=sender_id, max_id=0, just_clear=False, revoke=False))
                except Exception:
                    await self._client(DeleteHistoryRequest(peer=sender_id, max_id=0, just_clear=False, revoke=True))
            except Exception as e:
                tb = traceback.format_exc()
                logger.error(f"[RaidProtection] Failed to delete history with {sender_id}: {e}")
                await self._send_log(
                    f"<blockquote><b>Dialogue deletion error</b></blockquote>\n"
                    f"<blockquote>User ID: <code>{sender_id}</code>\n"
                    f"Error: <code>{self._escape(str(e))}</code></blockquote>\n"
                    f"<pre><code class=\"language-error\">{self._escape(tb[-800:])}</code></pre>"
                )

            try:
                await self._client(BlockRequest(id=sender_id))
            except Exception as e:
                logger.error(f"[RaidProtection] Failed to block {sender_id}: {e}")

            try:
                raw = getattr(message, "raw_text", None) or ""
                action = getattr(message, "action", None)
                msg_text = self._escape(
                    "<sticker>" if getattr(message, "sticker", None)
                    else "<photo>" if getattr(message, "photo", None)
                    else "<video>" if getattr(message, "video", None)
                    else "<file>" if getattr(message, "document", None)
                    else f"<service: {type(action).__name__}>" if action is not None
                    else raw[:3000]
                )
                username_line = f"Username: @{username}\n" if username else ""
                log_text = self.strings["banned_log"].format(
                    user_id=sender_id,
                    name=name,
                    username_line=username_line,
                    report_status=report_status,
                    text=msg_text,
                )
                await self._send_log(log_text)
            except Exception as e:
                logger.error(f"[RaidProtection] Failed to format/send ban log: {e}")

            self._record_ban()
        except Exception as e:
            logger.error(f"[RaidProtection] _process_ban unexpected error for {sender_id}: {e}")
        finally:
            self._processing.discard(sender_id)