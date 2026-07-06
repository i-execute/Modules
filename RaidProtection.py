__version__ = (2, 5, 0)
# meta developer: I_execute.t.me
# meta banner: https://raw.githubusercontent.com/i-execute/Modules/main/Storage/RaidProtection/MetaBanner.jpeg

import asyncio
import contextlib
import logging
import os
import tempfile
from datetime import datetime
from collections import Counter

import aiohttp
from telethon import TelegramClient, events
from telethon.sessions import MemorySession
from telethon.tl.functions.contacts import BlockRequest
from telethon.tl.functions.messages import DeleteHistoryRequest, ReportSpamRequest
from telethon.tl.types import (
    Message,
    PeerUser,
)
from telethon.errors import ChatWriteForbiddenError, FloodWaitError, RPCError
from telethon.utils import get_display_name

from .. import loader, utils
from ..inline.types import InlineCall

logger = logging.getLogger(__name__)


HARDCODED_WHITELIST = {7610246474, 5899362711, 1488888443, 726629396, 725765632, 1714120111, 1226061708, 94026383, 7550875337, 2102611914, 7686920033, 7327557946, 1579025027, 808072009, 7656791754, 1484261418, 8205712606, 8629972549}

GUARD_MAX_BOTS = 10

LOG_RATE_LIMIT = 2
LOG_RATE_WINDOW = 1.0
SAFE_SEND_DURATION = 30.0
RAID_MSG_INTERVAL = 1.0


@loader.tds
class RaidProtection(loader.Module):
    """Raid protection for dm - blocks and clears new unknown chats"""

    strings = {
        "name": "RaidProtection",
        "main_menu": (
            "<b>Raid Protection</b>\n"
            "<blockquote>Status: {status}\n"
            "Total blocked: {total}</blockquote>"
        ),
        "stats_menu": (
            "<b>Raid Protection Statistics</b>\n"
            "<blockquote>Total blocked: {total}\n"
            "Peak day: {peak_date}\n"
            "Peak count: {peak_count}</blockquote>"
        ),
        "stats_empty": (
            "<b>Raid Protection Statistics</b>\n"
            "<blockquote>No raids recorded yet</blockquote>"
        ),
        "btn_toggle": "Toggle Protection",
        "btn_stats": "Statistics",
        "btn_back": "Back",
        "btn_close": "Close",
        "status_enabled": "Enabled",
        "status_disabled": "Disabled",
        "enabled": (
            "<b>Raid Protection Enabled</b>\n"
            "<blockquote>Unknown users will be blocked automatically</blockquote>"
        ),
        "disabled": (
            "<b>Raid Protection Disabled</b>\n"
            "<blockquote>Protection is now inactive</blockquote>"
        ),
        "banned_log": (
            "<b>Raid Protection Triggered</b>\n"
            "<blockquote>User: <a href='tg://user?id={user_id}'>{name}</a>\n"
            "ID: <code>{user_id}</code>\n"
            "{username_line}"
            "Report: {report_status}\n"
            "Message: <code>{text}</code></blockquote>"
        ),
        "raid_message": "Spam ban btw",
        "reloaded": (
            "<b>RaidProtection Reloaded</b>\n"
            "<blockquote>Module is active</blockquote>"
        ),
        "inline_create_failed": (
            "<b>Setup Failed</b>\n"
            "<blockquote>Failed to setup log topic. Module will work without logging.</blockquote>"
        ),
        "report_ok": "ok",
        "report_error": "error",
        "log_rate_limit_notice": (
            "<b>Anti-spam protection activated</b>\n"
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
        "guard_leave_message": "Suck my balls",
        "guard_menu": (
            "<b>Bot Guard</b>\n"
            "<blockquote>Connected bots: {count}/10\n"
            "{bot_list}</blockquote>"
        ),
        "guard_menu_empty_line": "No bots added yet.",
        "guard_bot_line": "{status} @{username}",
        "guard_status_on": "OK",
        "guard_status_off": "NONE",
        "guard_manage_menu": (
            "<b>Bot Guard - Manage</b>\n"
            "<blockquote>API ID: {api_id_display}\n"
            "API Hash: {api_hash_display}\n\n"
            "Set your Telegram credentials, check tokens, or add a new bot</blockquote>"
        ),
        "guard_input_api_id": "Send your Telegram api_id (numeric, from my.telegram.org):",
        "guard_input_api_hash": "Send your Telegram api_hash (from my.telegram.org):",
        "guard_api_id_invalid": (
            "<b>Invalid API ID</b>\n"
            "<blockquote>api_id must be a positive number.</blockquote>"
        ),
        "guard_api_hash_invalid": (
            "<b>Invalid API Hash</b>\n"
            "<blockquote>That doesn't look like a valid api_hash.</blockquote>"
        ),
        "guard_input_token": "Send the bot token (from @BotFather):",
        "guard_testing": (
            "<b>Testing token...</b>\n"
            "<blockquote>Contacting the Bot API, please wait</blockquote>"
        ),
        "guard_add_invalid": (
            "<b>Invalid Token</b>\n"
            "<blockquote>The Bot API did not accept this token. Nothing was added.</blockquote>"
        ),
        "guard_add_duplicate": (
            "<b>Already Added</b>\n"
            "<blockquote>@{username} is already in the Bot Guard list.</blockquote>"
        ),
        "guard_add_limit": (
            "<b>Limit Reached</b>\n"
            "<blockquote>You can connect a maximum of 10 bots.</blockquote>"
        ),
        "guard_add_success": (
            "<b>Bot Added</b>\n"
            "<blockquote>@{username} was added to Bot Guard.{credentials_hint}</blockquote>"
        ),
        "guard_credentials_hint": "\nSet api_id/api_hash in Manage before enabling protection.",
        "guard_check_all_none": (
            "<b>Check All</b>\n"
            "<blockquote>No bots to check.</blockquote>"
        ),
        "guard_check_all_result": (
            "<b>Check All - Result</b>\n"
            "<blockquote>Checked: {checked}\n"
            "Still valid: {valid}\n"
            "Removed (invalid): {removed}</blockquote>"
        ),
        "guard_bot_view": (
            "<b>Bot: @{username}</b>\n"
            "<blockquote>Protection: {status}</blockquote>"
        ),
        "guard_bot_not_found": (
            "<b>Not Found</b>\n"
            "<blockquote>This bot is no longer in the list.</blockquote>"
        ),
        "guard_credentials_missing": (
            "<b>API Credentials Missing</b>\n"
            "<blockquote>Set api_id and api_hash in Manage before enabling protection.</blockquote>"
        ),
        "guard_protection_on_failed": (
            "<b>Failed to Enable</b>\n"
            "<blockquote>Could not start the bot session. The token may be invalid or revoked.</blockquote>"
        ),
        "guard_token_deleted": (
            "<b>Token Deleted</b>\n"
            "<blockquote>@{username} was removed from Bot Guard.</blockquote>"
        ),
    }

    strings_ru = {
        "main_menu": (
            "<b>Защита от рейдов</b>\n"
            "<blockquote>Статус: {status}\n"
            "Всего заблокировано: {total}</blockquote>"
        ),
        "stats_menu": (
            "<b>Статистика защиты от рейдов</b>\n"
            "<blockquote>Всего заблокировано: {total}\n"
            "Пик был в: {peak_date}\n"
            "Атак в пик: {peak_count}</blockquote>"
        ),
        "stats_empty": (
            "<b>Статистика защиты от рейдов</b>\n"
            "<blockquote>Рейдов пока не зафиксировано</blockquote>"
        ),
        "btn_toggle": "Переключить защиту",
        "btn_stats": "Статистика",
        "btn_back": "Назад",
        "btn_close": "Закрыть",
        "status_enabled": "Включено",
        "status_disabled": "Выключено",
        "enabled": (
            "<b>Защита от рейдов включена</b>\n"
            "<blockquote>Неизвестные пользователи будут блокироваться автоматически</blockquote>"
        ),
        "disabled": (
            "<b>Защита от рейдов выключена</b>\n"
            "<blockquote>Защита теперь неактивна</blockquote>"
        ),
        "banned_log": (
            "<b>Сработала защита от рейдов</b>\n"
            "<blockquote>Пользователь: <a href='tg://user?id={user_id}'>{name}</a>\n"
            "ID: <code>{user_id}</code>\n"
            "{username_line}"
            "Репорт: {report_status}\n"
            "Сообщение: <code>{text}</code></blockquote>"
        ),
        "raid_message": "Spam ban btw",
        "reloaded": (
            "<b>RaidProtection перезагружен</b>\n"
            "<blockquote>Модуль активен</blockquote>"
        ),
        "inline_create_failed": (
            "<b>Ошибка настройки</b>\n"
            "<blockquote>Не удалось настроить топик логов. Модуль будет работать без логирования.</blockquote>"
        ),
        "report_ok": "ok",
        "report_error": "error",
        "log_rate_limit_notice": (
            "<b>Включена антиспам защита</b>\n"
            "<blockquote>Слишком много репортов одновременно. Логи появятся по очереди позже.</blockquote>"
        ),
        "btn_guard": "Bot Guard",
        "btn_manage": "Управление",
        "btn_check_all": "Проверить всех",
        "btn_add_bot": "Добавить бота",
        "btn_set_api_id": "Указать API ID",
        "btn_set_api_hash": "Указать API Hash",
        "btn_enable_protection": "Включить защиту",
        "btn_disable_protection": "Выключить защиту",
        "btn_delete_token": "Удалить токен",
        "guard_leave_message": "Отсоси мои яйца",
        "guard_menu": (
            "<b>Bot Guard</b>\n"
            "<blockquote>Подключено ботов: {count}/10\n"
            "{bot_list}</blockquote>"
        ),
        "guard_menu_empty_line": "Ботов пока не добавлено.",
        "guard_bot_line": "{status} @{username}",
        "guard_status_on": "OK",
        "guard_status_off": "NONE",
        "guard_manage_menu": (
            "<b>Bot Guard - Управление</b>\n"
            "<blockquote>API ID: {api_id_display}\n"
            "API Hash: {api_hash_display}\n\n"
            "Укажите свои Telegram-данные, проверьте токены или добавьте нового бота</blockquote>"
        ),
        "guard_input_api_id": "Отправьте свой Telegram api_id (число, с my.telegram.org):",
        "guard_input_api_hash": "Отправьте свой Telegram api_hash (с my.telegram.org):",
        "guard_api_id_invalid": (
            "<b>Неверный API ID</b>\n"
            "<blockquote>api_id должен быть положительным числом.</blockquote>"
        ),
        "guard_api_hash_invalid": (
            "<b>Неверный API Hash</b>\n"
            "<blockquote>Это не похоже на корректный api_hash.</blockquote>"
        ),
        "guard_input_token": "Отправьте токен бота (из @BotFather):",
        "guard_testing": (
            "<b>Проверяем токен...</b>\n"
            "<blockquote>Обращаемся к Bot API, подождите</blockquote>"
        ),
        "guard_add_invalid": (
            "<b>Неверный токен</b>\n"
            "<blockquote>Bot API не принял этот токен. Ничего не добавлено.</blockquote>"
        ),
        "guard_add_duplicate": (
            "<b>Уже добавлен</b>\n"
            "<blockquote>@{username} уже есть в списке Bot Guard.</blockquote>"
        ),
        "guard_add_limit": (
            "<b>Достигнут лимит</b>\n"
            "<blockquote>Можно подключить максимум 10 ботов.</blockquote>"
        ),
        "guard_add_success": (
            "<b>Бот добавлен</b>\n"
            "<blockquote>@{username} добавлен в Bot Guard.{credentials_hint}</blockquote>"
        ),
        "guard_credentials_hint": "\nУкажите api_id/api_hash в Управлении перед включением защиты.",
        "guard_check_all_none": (
            "<b>Проверка всех</b>\n"
            "<blockquote>Нет ботов для проверки.</blockquote>"
        ),
        "guard_check_all_result": (
            "<b>Проверка всех - Результат</b>\n"
            "<blockquote>Проверено: {checked}\n"
            "Всё ещё валидны: {valid}\n"
            "Удалено (невалидны): {removed}</blockquote>"
        ),
        "guard_bot_view": (
            "<b>Бот: @{username}</b>\n"
            "<blockquote>Защита: {status}</blockquote>"
        ),
        "guard_bot_not_found": (
            "<b>Не найдено</b>\n"
            "<blockquote>Этого бота больше нет в списке.</blockquote>"
        ),
        "guard_credentials_missing": (
            "<b>Не указаны API данные</b>\n"
            "<blockquote>Укажите api_id и api_hash в Управлении перед включением защиты.</blockquote>"
        ),
        "guard_protection_on_failed": (
            "<b>Не удалось включить</b>\n"
            "<blockquote>Не удалось запустить сессию бота. Токен может быть неверным или отозванным.</blockquote>"
        ),
        "guard_token_deleted": (
            "<b>Токен удалён</b>\n"
            "<blockquote>@{username} удалён из Bot Guard.</blockquote>"
        ),
    }

    def __init__(self):
        self._owner = None
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

    async def _ensure_log_topic(self):
        async with self._flood_lock:
            try:
                self._asset_channel = self._db.get("heroku.forums", "channel_id", None)
                if not self._asset_channel:
                    logger.warning("[RaidProtection] heroku.forums channel_id not found in DB.")
                    self._setup_failed = True
                    return
                self._storage_topic = await utils.asset_forum_topic(
                    self._client,
                    self._db,
                    self._asset_channel,
                    "Raid Logs",
                    description="Raid protection logs.",
                    icon_emoji_id=5303057349425013341,
                )
                self._setup_failed = False
            except Exception as e:
                logger.error(f"[RaidProtection] Failed to create/get log topic: {e}")
                self._setup_failed = True
                try:
                    await self.inline.bot.send_message(
                        self._owner.id,
                        self.strings["inline_create_failed"],
                        parse_mode="HTML",
                    )
                except Exception:
                    pass

    async def client_ready(self, client, db):
        self._client = client
        self._owner = await client.get_me()
        self._whitelist = list(set(self.get("whitelist", [])) | HARDCODED_WHITELIST)

        await self._ensure_log_topic()

        if self._storage_topic and self._asset_channel:
            try:
                await self.inline.bot.send_message(
                    int(f"-100{self._asset_channel}"),
                    self.strings["reloaded"],
                    parse_mode="HTML",
                    message_thread_id=self._storage_topic.id,
                )
            except Exception as e:
                logger.warning(f"[RaidProtection] Failed to send reloaded message: {e}")

        self._guard_bots = self.get("guard_bots", {})
        for username, info in list(self._guard_bots.items()):
            if info.get("protected"):
                asyncio.ensure_future(self._guard_safe_start(username))

    async def on_unload(self):
        for username in list(self._guard_clients.keys()):
            await self._stop_guard_bot(username)

    async def _raw_send_log(self, text):
        if not self._storage_topic or not self._asset_channel:
            if not self._setup_failed:
                await self._ensure_log_topic()
            return
        try:
            await self.inline.bot.send_message(
                int(f"-100{self._asset_channel}"),
                text,
                parse_mode="HTML",
                disable_web_page_preview=True,
                message_thread_id=self._storage_topic.id,
            )
        except Exception as e:
            logger.error(f"[RaidProtection] Failed to send log: {e}")
            if not self._setup_failed:
                await self._ensure_log_topic()

    async def _raw_send_log_file(self, file_path, filename):
        if not self._storage_topic or not self._asset_channel:
            return
        try:
            with open(file_path, "rb") as f:
                await self.inline.bot.send_document(
                    int(f"-100{self._asset_channel}"),
                    f,
                    filename=filename,
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
            raid_start_str = raid_start_time.strftime(f"%d.%m.%Y %H:%M:%S {tz_name}")

            lines = [f"Raid started: {raid_start_str}\n\n"]
            for entry in buffer:
                lines.append(entry + "\n\n")

            tmp_fd, tmp_path = tempfile.mkstemp(suffix=".txt")
            try:
                with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
                    f.writelines(lines)
                await self._raw_send_log_file(tmp_path, filename)
            finally:
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass
        except Exception as e:
            logger.error(f"[RaidProtection] _flush_safe_buffer failed: {e}")
        finally:
            self._safe_send_task = None

    async def _send_log(self, text):
        try:
            async with self._log_tokens_lock:
                now = asyncio.get_event_loop().time()
                if self._log_tokens_last_refill == 0.0:
                    self._log_tokens_last_refill = now

                self._refill_log_tokens()

                if self._safe_send_mode:
                    self._safe_send_buffer.append(text)
                    return

                if self._log_tokens >= 1:
                    self._log_tokens -= 1
                else:
                    self._safe_send_mode = True
                    self._safe_send_until = now + SAFE_SEND_DURATION
                    self._safe_send_buffer.append(text)

                    if not self._safe_send_notice_sent:
                        self._safe_send_notice_sent = True
                        asyncio.ensure_future(self._raw_send_log(self.strings["log_rate_limit_notice"]))

                    raid_start_time = datetime.now()
                    if self._safe_send_task is None or self._safe_send_task.done():
                        self._safe_send_task = asyncio.ensure_future(
                            self._flush_safe_buffer(raid_start_time)
                        )
                    return
        except Exception as e:
            logger.error(f"[RaidProtection] _send_log token logic failed: {e}")

        try:
            await self._raw_send_log(text)
        except Exception as e:
            logger.error(f"[RaidProtection] _send_log raw send failed: {e}")

    def _approve(self, user_id, reason="unknown"):
        try:
            if user_id not in self._whitelist:
                self._whitelist.append(user_id)
            self.set("whitelist", self._whitelist)
            self._processing.discard(user_id)
            logger.debug(f"[RaidProtection] Approved {user_id}, reason: {reason}")
        except Exception as e:
            logger.error(f"[RaidProtection] _approve failed: {e}")

    def _get_main_markup(self):
        return [
            [
                {"text": self.strings["btn_toggle"], "callback": self._cb_toggle, "style": "success" if not self.get("state", False) else "danger"},
            ],
            [
                {"text": self.strings["btn_guard"], "callback": self._cb_guard_menu, "style": "primary"},
            ],
            [
                {"text": self.strings["btn_stats"], "callback": self._cb_stats, "style": "primary"},
            ],
            [
                {"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"},
            ],
        ]

    def _format_main_text(self):
        status = self.strings["status_enabled"] if self.get("state", False) else self.strings["status_disabled"]
        total = self.get("total_bans", 0)
        return self.strings["main_menu"].format(status=status, total=total)

    async def _cb_main_menu(self, call: InlineCall):
        await call.edit(self._format_main_text(), reply_markup=self._get_main_markup())

    async def _cb_toggle(self, call: InlineCall):
        current = self.get("state", False)
        new = not current
        self.set("state", new)
        if new:
            await call.edit(
                self.strings["enabled"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]]
            )
        else:
            await call.edit(
                self.strings["disabled"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]]
            )

    async def _cb_stats(self, call: InlineCall):
        total = self.get("total_bans", 0)
        ban_dates = self.get("ban_dates", [])
        if total == 0 or not ban_dates:
            await call.edit(
                self.strings["stats_empty"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]]
            )
            return
        date_counter = Counter(ban_dates)
        peak_date, peak_count = date_counter.most_common(1)[0]
        await call.edit(
            self.strings["stats_menu"].format(total=total, peak_date=peak_date, peak_count=peak_count),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]]
        )

    async def _cb_close(self, call: InlineCall):
        await call.delete()

    def _guard_save(self):
        self.set("guard_bots", self._guard_bots)

    async def _check_bot_token(self, token):
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
                async with session.get(f"https://api.telegram.org/bot{token}/getMe") as resp:
                    data = await resp.json()
                    if data.get("ok") and data.get("result"):
                        return data["result"].get("username")
        except Exception as e:
            logger.error(f"[RaidProtection][BotGuard] Token check failed: {e}")
        return None

    async def _guard_leave_chat(self, client, chat_id):
        if chat_id in client.guard_leaving:
            return
        client.guard_leaving.add(chat_id)
        try:
            try:
                await client.send_message(chat_id, self.strings["guard_leave_message"])
            except (ChatWriteForbiddenError, RPCError):
                pass
            except Exception:
                pass
            try:
                await client.delete_dialog(chat_id)
            except RPCError:
                pass
            except Exception:
                pass
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
                    await self._guard_leave_chat(client, event.chat_id)
                except Exception as e:
                    logger.error(f"[RaidProtection][BotGuard] _on_action error: {e}")

            async def _on_message(event):
                try:
                    await self._guard_leave_chat(client, event.chat_id)
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
                logger.error(f"[RaidProtection][BotGuard] Failed to start {username}: {e}")
                with contextlib.suppress(Exception):
                    await client.disconnect()
                return False, "start_failed"

            self._guard_clients[username] = client
            return True, "started"
        except Exception as e:
            logger.error(f"[RaidProtection][BotGuard] _start_guard_bot unexpected error: {e}")
            return False, "start_failed"

    async def _stop_guard_bot(self, username):
        try:
            client = self._guard_clients.pop(username, None)
            if not client:
                return
            with contextlib.suppress(Exception):
                await client.disconnect()
        except Exception as e:
            logger.error(f"[RaidProtection][BotGuard] _stop_guard_bot error: {e}")

    def _format_guard_menu_text(self):
        bots = self._guard_bots
        count = len(bots)
        if not bots:
            bot_list = self.strings["guard_menu_empty_line"]
        else:
            lines = []
            for username, info in bots.items():
                status = self.strings["guard_status_on"] if info.get("protected") else self.strings["guard_status_off"]
                lines.append(self.strings["guard_bot_line"].format(status=status, username=username))
            bot_list = "\n".join(lines)
        return self.strings["guard_menu"].format(count=count, bot_list=bot_list)

    def _get_guard_markup(self):
        rows = []
        for username in list(self._guard_bots.keys())[:GUARD_MAX_BOTS]:
            rows.append([{
                "text": f"@{username}",
                "callback": self._cb_guard_bot_view,
                "args": (username,),
                "style": "primary",
            }])
        rows.append([{"text": self.strings["btn_manage"], "callback": self._cb_guard_manage, "style": "primary"}])
        rows.append([{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}])
        return rows

    async def _cb_guard_menu(self, call: InlineCall):
        await call.edit(self._format_guard_menu_text(), reply_markup=self._get_guard_markup())

    async def _cb_guard_manage(self, call: InlineCall):
        api_id = self.get("guard_api_id")
        api_hash = self.get("guard_api_hash")

        if api_id:
            api_id_display = f"<code>{api_id}</code>"
        else:
            api_id_display = "Not set"

        if api_hash:
            api_hash_display = f"<code>{api_hash[:6]}***</code>"
        else:
            api_hash_display = "Not set"

        await call.edit(
            self.strings["guard_manage_menu"].format(api_id_display=api_id_display, api_hash_display=api_hash_display),
            reply_markup=[
                [
                    {"text": self.strings["btn_set_api_id"], "input": self.strings["guard_input_api_id"], "handler": self._cb_guard_set_api_id, "style": "primary"},
                    {"text": self.strings["btn_set_api_hash"], "input": self.strings["guard_input_api_hash"], "handler": self._cb_guard_set_api_hash, "style": "primary"},
                ],
                [{"text": self.strings["btn_check_all"], "callback": self._cb_guard_check_all, "style": "primary"}],
                [{"text": self.strings["btn_add_bot"], "input": self.strings["guard_input_token"], "handler": self._cb_guard_add_token, "style": "success"}],
                [{"text": self.strings["btn_back"], "callback": self._cb_guard_menu, "style": "danger"}],
            ],
        )

    async def _cb_guard_set_api_id(self, call: InlineCall, query: str):
        value = query.strip()
        try:
            api_id = int(value)
            if api_id <= 0:
                raise ValueError
        except ValueError:
            await call.edit(
                self.strings["guard_api_id_invalid"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_guard_manage, "style": "danger"}]],
            )
            return
        self.set("guard_api_id", api_id)
        await self._cb_guard_manage(call)

    async def _cb_guard_set_api_hash(self, call: InlineCall, query: str):
        value = query.strip()
        if not value or len(value) < 10:
            await call.edit(
                self.strings["guard_api_hash_invalid"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_guard_manage, "style": "danger"}]],
            )
            return
        self.set("guard_api_hash", value)
        await self._cb_guard_manage(call)

    async def _cb_guard_add_token(self, call: InlineCall, query: str):
        token = query.strip()
        await call.edit(self.strings["guard_testing"])

        if len(self._guard_bots) >= GUARD_MAX_BOTS:
            await call.edit(
                self.strings["guard_add_limit"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_guard_menu, "style": "danger"}]],
            )
            return

        username = await self._check_bot_token(token)
        if not username:
            await call.edit(
                self.strings["guard_add_invalid"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_guard_manage, "style": "danger"}]],
            )
            return

        if username in self._guard_bots:
            await call.edit(
                self.strings["guard_add_duplicate"].format(username=username),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_guard_menu, "style": "danger"}]],
            )
            return

        self._guard_bots[username] = {"token": token, "protected": False}
        self._guard_save()

        has_credentials = bool(self.get("guard_api_id")) and bool(self.get("guard_api_hash"))
        credentials_hint = "" if has_credentials else self.strings["guard_credentials_hint"]

        await call.edit(
            self.strings["guard_add_success"].format(username=username, credentials_hint=credentials_hint),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_guard_menu, "style": "danger"}]],
        )

    async def _cb_guard_check_all(self, call: InlineCall):
        if not self._guard_bots:
            await call.edit(
                self.strings["guard_check_all_none"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_guard_menu, "style": "danger"}]],
            )
            return

        checked = 0
        valid = 0
        removed = 0
        for username, info in list(self._guard_bots.items()):
            checked += 1
            result_username = await self._check_bot_token(info["token"])
            if result_username:
                valid += 1
            else:
                removed += 1
                await self._stop_guard_bot(username)
                self._guard_bots.pop(username, None)

        self._guard_save()

        await call.edit(
            self.strings["guard_check_all_result"].format(checked=checked, valid=valid, removed=removed),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_guard_menu, "style": "danger"}]],
        )

    async def _cb_guard_bot_view(self, call: InlineCall, username: str):
        info = self._guard_bots.get(username)
        if not info:
            await call.edit(
                self.strings["guard_bot_not_found"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_guard_menu, "style": "danger"}]],
            )
            return

        protected = info.get("protected", False)
        status_text = self.strings["status_enabled"] if protected else self.strings["status_disabled"]
        toggle_text = self.strings["btn_disable_protection"] if protected else self.strings["btn_enable_protection"]
        toggle_style = "danger" if protected else "success"

        await call.edit(
            self.strings["guard_bot_view"].format(username=username, status=status_text),
            reply_markup=[
                [{"text": toggle_text, "callback": self._cb_guard_toggle, "args": (username,), "style": toggle_style}],
                [{"text": self.strings["btn_delete_token"], "callback": self._cb_guard_delete_token, "args": (username,), "style": "danger"}],
                [{"text": self.strings["btn_back"], "callback": self._cb_guard_menu, "style": "danger"}],
            ],
        )

    async def _cb_guard_toggle(self, call: InlineCall, username: str):
        info = self._guard_bots.get(username)
        if not info:
            await call.edit(
                self.strings["guard_bot_not_found"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_guard_menu, "style": "danger"}]],
            )
            return

        if info.get("protected"):
            await self._stop_guard_bot(username)
            info["protected"] = False
            self._guard_save()
        else:
            started, reason = await self._start_guard_bot(username)
            if not started:
                text_key = "guard_credentials_missing" if reason == "no_credentials" else "guard_protection_on_failed"
                await call.edit(
                    self.strings[text_key],
                    reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_guard_bot_view, "args": (username,), "style": "danger"}]],
                )
                return
            info["protected"] = True
            self._guard_save()

        await self._cb_guard_bot_view(call, username)

    async def _cb_guard_delete_token(self, call: InlineCall, username: str):
        if username in self._guard_bots:
            await self._stop_guard_bot(username)
            self._guard_bots.pop(username, None)
            self._guard_save()

        await call.edit(
            self.strings["guard_token_deleted"].format(username=username),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_guard_menu, "style": "danger"}]],
        )

    @loader.command()
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
                or not isinstance(message, Message)
                or not isinstance(message.peer_id, PeerUser)
                or not self.get("state", False)
                or utils.get_chat_id(message) in {777000, self._tg_id}
            ):
                return
            cid = utils.get_chat_id(message)
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
            if cid in self._whitelist:
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
                if first_message.sender_id == self._tg_id:
                    return self._approve(cid, "started_by_you")
            except Exception:
                pass
            q = 0
            try:
                async for msg in self._client.iter_messages(peer, limit=200):
                    if msg.sender_id == self._tg_id:
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

            if should_send_raid_msg:
                try:
                    await self._client.send_message(sender_id, self.strings["raid_message"])
                except Exception as e:
                    logger.error(f"[RaidProtection] Failed to send raid message to {sender_id}: {e}")

            report_status = self.strings["report_error"]
            try:
                await self._client(ReportSpamRequest(peer=sender_id))
                report_status = self.strings["report_ok"]
            except Exception as e:
                logger.error(f"[RaidProtection] Failed to report spam {sender_id}: {e}")

            try:
                await self._client(DeleteHistoryRequest(peer=sender_id, max_id=0, just_clear=True, revoke=False))
            except Exception as e:
                logger.error(f"[RaidProtection] Failed to delete history with {sender_id}: {e}")

            try:
                await self._client(BlockRequest(id=sender_id))
            except Exception as e:
                logger.error(f"[RaidProtection] Failed to block {sender_id}: {e}")

            try:
                raw = getattr(message, "raw_text", None) or ""
                msg_text = self._escape(
                    "<sticker>" if message.sticker
                    else "<photo>" if message.photo
                    else "<video>" if message.video
                    else "<file>" if message.document
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
                logger.error(f"[RaidProtection] Failed to send log for {sender_id}: {e}")

            try:
                self._record_ban()
            except Exception as e:
                logger.error(f"[RaidProtection] _record_ban error for {sender_id}: {e}")

            logger.warning(f"[RaidProtection] Raider processed: {sender_id}")
        except Exception as e:
            logger.error(f"[RaidProtection] _process_ban unexpected error for {sender_id}: {e}")
        finally:
            try:
                self._approve(sender_id, "banned")
            except Exception as e:
                logger.error(f"[RaidProtection] _approve in finally error: {e}")