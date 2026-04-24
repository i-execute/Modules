__version__ = (1, 1, 0)
# meta developer: FireJester.t.me

import logging
import asyncio
import re
import os
import sqlite3
import base64
import ipaddress
import struct
import tempfile
import io
import time

from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.types import Message, Channel
from telethon.errors import (
    FloodWaitError,
    AuthKeyUnregisteredError,
    UserDeactivatedBanError,
)

from .. import loader, utils

logger = logging.getLogger(__name__)

TELEGRAM_ID = 777000
STRING_SESSION_PATTERN = re.compile(r'1[A-Za-z0-9_-]{200,}={0,2}')

DC_IP_MAP = {
    1: "149.154.175.53",
    2: "149.154.167.51",
    3: "149.154.175.100",
    4: "149.154.167.91",
    5: "91.108.56.130",
}


def escape_html(text):
    if not text:
        return ""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def get_user_link(user_id, name):
    return f'<a href="tg://user?id={user_id}">{escape_html(name)}</a>'


def get_full_name(user):
    if not user:
        return "Unknown"
    first = getattr(user, "first_name", "") or ""
    last = getattr(user, "last_name", "") or ""
    return f"{first} {last}".strip() or "Unknown"


def parse_string_session(session_str):
    try:
        if not session_str or not session_str.startswith("1"):
            return None
        string = session_str[1:]
        string_padded = string + "=" * (-len(string) % 4)
        try:
            data = base64.urlsafe_b64decode(string_padded)
        except Exception:
            return None
        if len(data) == 263:
            dc_id, ip_bytes, port, auth_key = struct.unpack(">B4sH256s", data)
            ip = str(ipaddress.IPv4Address(ip_bytes))
        elif len(data) == 275:
            dc_id, ip_bytes, port, auth_key = struct.unpack(">B16sH256s", data)
            ip = str(ipaddress.IPv6Address(ip_bytes))
        else:
            return None
        return {"dc_id": dc_id, "ip": ip, "port": port, "auth_key": auth_key}
    except Exception:
        return None


def build_string_session(dc_id, auth_key):
    try:
        if dc_id not in DC_IP_MAP or auth_key is None:
            return None
        if isinstance(auth_key, str):
            auth_key = auth_key.encode("latin-1")
        elif not isinstance(auth_key, bytes):
            auth_key = bytes(auth_key)
        if len(auth_key) != 256:
            return None
        ip = ipaddress.IPv4Address(DC_IP_MAP[dc_id])
        data = struct.pack(">B4sH256s", dc_id, ip.packed, 443, auth_key)
        encoded = base64.urlsafe_b64encode(data).decode("ascii")
        return "1" + encoded
    except Exception:
        return None


def read_session_file(file_path):
    try:
        conn = sqlite3.connect(file_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='sessions'"
        )
        if not cursor.fetchone():
            conn.close()
            return None
        cursor.execute("SELECT dc_id, auth_key FROM sessions LIMIT 1")
        row = cursor.fetchone()
        conn.close()
        if row:
            dc_id = row[0]
            auth_key = row[1]
            if isinstance(auth_key, str):
                auth_key = auth_key.encode("latin-1")
            if not auth_key or len(auth_key) != 256:
                return None
            return {"dc_id": dc_id, "auth_key": auth_key}
        return None
    except Exception:
        return None


@loader.tds
class TGWatcher(loader.Module):
    """Watches linked Telegram accounts and forwards messages from Telegram (777000) to a log topic"""

    strings = {
        "name": "TGWatcher",
        "line": "--------------------",
        "help": (
            "<b>TGWatcher</b>\n\n"
            "<b>Commands:</b>\n"
            "<code>{prefix}tgw add [session]</code> - add account (string session, reply to string or .session file)\n"
            "<code>{prefix}tgw rm [N/all]</code> - remove account by number or all\n"
            "<code>{prefix}tgw status</code> - account info (txt file)\n"
        ),
        "no_accounts": "<b>No accounts connected</b>",
        "account_added": (
            "<b>Account added</b>\n"
            "{line}\n"
            "Number: #{num}\n"
            "Name: {name}\n"
            "ID: <code>{user_id}</code>\n"
            "{line}"
        ),
        "account_exists": "<b>Error:</b> This account is already added (#{num})",
        "account_not_found": "<b>Error:</b> Account #{num} not found",
        "account_removed": "<b>Account #{num} removed</b>",
        "all_removed": "<b>All accounts removed ({count})</b>",
        "session_invalid": "<b>Error:</b> Invalid or dead session\n{line}\nReason: {error}\n{line}",
        "provide_session": "<b>Error:</b> Provide string session (argument, reply to text or .session file)",
        "invalid_file": "<b>Error:</b> Invalid .session file",
        "rm_usage": "<b>Error:</b> Use <code>{prefix}tgw rm [number]</code> or <code>{prefix}tgw rm all</code>",
        "connecting": "<b>Connecting session...</b>",
        "health_ok": "OK",
        "health_dead": "DEAD",
        "health_running": "<b>Health check running...</b>",
        "health_complete": "<b>Health check complete</b>\nAlive: {alive} | Dead: {dead}",
        "log_greeting": (
            "<b>TGWatcher initialized</b>\n\n"
            "Owner: {owner_link}\n"
            "Accounts: {count}\n\n"
            "Messages from Telegram (777000) will appear here."
        ),
        "log_reloaded": "<b>TGWatcher reloaded, watchers active</b>",
        "log_message": (
            "<b>[TG Message]</b>\n\n"
            "<b>Account:</b> {account_link} [#{num}]\n"
            "<b>Owner:</b> {owner_link}\n"
            "{line}\n"
            "{text}\n"
            "{line}"
        ),
        "log_account_added": "[ADDED] Account #{num}: {name} (ID: {id})",
        "log_account_removed": "[REMOVED] Account #{num}: {name}",
        "log_health_dead": "[ALERT] {owner_link} - Session #{num} ({name}) is dead! Removing...",
        "log_health_result": "[HEALTH] Alive: {alive} | Dead: {dead} | Removed: {removed}",
        "setup_failed": (
            "<b>Failed to setup TGWatcher log topic</b>\n\n"
            "The module will still work but without logging."
        ),
    }

    strings_ru = {
        "line": "--------------------",
        "help": (
            "<b>TGWatcher</b>\n\n"
            "<b>Команды:</b>\n"
            "<code>{prefix}tgw add [сессия]</code> - добавить аккаунт (string session, реплай на текст или .session файл)\n"
            "<code>{prefix}tgw rm [N/all]</code> - удалить аккаунт по номеру или все\n"
            "<code>{prefix}tgw status</code> - информация об аккаунтах (txt файл)\n"
        ),
        "no_accounts": "<b>Нет подключенных аккаунтов</b>",
        "account_added": (
            "<b>Аккаунт добавлен</b>\n"
            "{line}\n"
            "Номер: #{num}\n"
            "Имя: {name}\n"
            "ID: <code>{user_id}</code>\n"
            "{line}"
        ),
        "account_exists": "<b>Ошибка:</b> Этот аккаунт уже добавлен (#{num})",
        "account_not_found": "<b>Ошибка:</b> Аккаунт #{num} не найден",
        "account_removed": "<b>Аккаунт #{num} удален</b>",
        "all_removed": "<b>Все аккаунты удалены ({count})</b>",
        "session_invalid": "<b>Ошибка:</b> Невалидная или мертвая сессия\n{line}\nПричина: {error}\n{line}",
        "provide_session": "<b>Ошибка:</b> Укажите string session (аргумент, реплай на текст или .session файл)",
        "invalid_file": "<b>Ошибка:</b> Невалидный .session файл",
        "rm_usage": "<b>Ошибка:</b> Используйте <code>{prefix}tgw rm [номер]</code> или <code>{prefix}tgw rm all</code>",
        "connecting": "<b>Подключение сессии...</b>",
        "health_ok": "OK",
        "health_dead": "DEAD",
        "health_running": "<b>Проверка здоровья...</b>",
        "health_complete": "<b>Проверка завершена</b>\nЖивых: {alive} | Мертвых: {dead}",
        "log_greeting": (
            "<b>TGWatcher запущен</b>\n\n"
            "Владелец: {owner_link}\n"
            "Аккаунтов: {count}\n\n"
            "Сообщения от Telegram (777000) будут появляться здесь."
        ),
        "log_reloaded": "<b>TGWatcher перезагружен, вотчеры активны</b>",
        "log_message": (
            "<b>[TG Сообщение]</b>\n\n"
            "<b>Аккаунт:</b> {account_link} [#{num}]\n"
            "<b>Владелец:</b> {owner_link}\n"
            "{line}\n"
            "{text}\n"
            "{line}"
        ),
        "log_account_added": "[ДОБАВЛЕН] Аккаунт #{num}: {name} (ID: {id})",
        "log_account_removed": "[УДАЛЕН] Аккаунт #{num}: {name}",
        "log_health_dead": "[ALERT] {owner_link} - Сессия #{num} ({name}) мертва! Удаление...",
        "log_health_result": "[HEALTH] Живых: {alive} | Мертвых: {dead} | Удалено: {removed}",
        "setup_failed": (
            "<b>Не удалось настроить топик TGWatcher</b>\n\n"
            "Модуль продолжит работать, но без логирования."
        ),
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "sessions",
                [],
                "List of string sessions",
                validator=loader.validators.Hidden(),
            ),
            loader.ConfigValue(
                "health_interval",
                1,
                "Health check interval in hours",
                validator=loader.validators.Integer(minimum=1),
            ),
        )
        self._accounts = {}
        self._clients = {}
        self._handlers = {}
        self._owner = None
        self._my_id = None
        self._asset_channel = None
        self._log_topic = None
        self._health_task = None
        self._flood_lock = asyncio.Lock()
        self._setup_failed = False

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        self._owner = await client.get_me()
        self._my_id = self._owner.id
        await self._ensure_log_topic()
        await self._restore_sessions()
        self._start_health_loop()

    async def on_unload(self):
        if self._health_task:
            self._health_task.cancel()
            try:
                await self._health_task
            except (asyncio.CancelledError, Exception):
                pass
        for num in list(self._clients.keys()):
            await self._disconnect_account(num, save=False)

    def _get_username(self, entity):
        if hasattr(entity, "username") and entity.username:
            return entity.username
        if hasattr(entity, "usernames") and entity.usernames:
            for u in entity.usernames:
                if getattr(u, "active", False):
                    return u.username
        return None

    async def _send_with_flood_wait(self, coro_func, *args, **kwargs):
        max_retries = 5
        for attempt in range(max_retries):
            try:
                return await coro_func(*args, **kwargs)
            except FloodWaitError as e:
                await asyncio.sleep(e.seconds + 1)
            except Exception as e:
                error_str = str(e).lower()
                if "flood" in error_str and "retry after" in error_str:
                    match = re.search(r"retry after (\d+)", error_str)
                    if match:
                        await asyncio.sleep(int(match.group(1)) + 1)
                        continue
                raise
        return None

    async def _ensure_log_topic(self):
        async with self._flood_lock:
            self._asset_channel = self._db.get("heroku.forums", "channel_id", None)
            if not self._asset_channel:
                logger.warning("[TGWatcher] heroku.forums channel_id not found in DB.")
                self._setup_failed = True
                return

            try:
                self._log_topic = await utils.asset_forum_topic(
                    self._client,
                    self._db,
                    self._asset_channel,
                    "TGWatcher Logs",
                    description="TGWatcher message logs.",
                    icon_emoji_id=5188466187448650036,
                )
                self._setup_failed = False
            except Exception as e:
                logger.error(f"[TGWatcher] Failed to create/get log topic: {e}")
                self._setup_failed = True
                try:
                    await self.inline.bot.send_message(
                        self._my_id,
                        self.strings["setup_failed"],
                        parse_mode="HTML",
                    )
                except Exception:
                    pass
                return

            owner_link = get_user_link(self._my_id, get_full_name(self._owner))
            try:
                await self._send_with_flood_wait(
                    self.inline.bot.send_message,
                    int(f"-100{self._asset_channel}"),
                    self.strings["log_greeting"].format(
                        owner_link=owner_link,
                        count=len(self._accounts),
                    ),
                    parse_mode="HTML",
                    message_thread_id=self._log_topic.id,
                )
            except Exception:
                try:
                    await self._send_with_flood_wait(
                        self.inline.bot.send_message,
                        int(f"-100{self._asset_channel}"),
                        self.strings["log_reloaded"],
                        parse_mode="HTML",
                        message_thread_id=self._log_topic.id,
                    )
                except Exception as e:
                    logger.error(f"[TGWatcher] Failed to send greeting: {e}")

    async def _send_log(self, text):
        if not self._log_topic or not self._asset_channel:
            if not self._setup_failed:
                await self._ensure_log_topic()
            return
        try:
            await self._send_with_flood_wait(
                self.inline.bot.send_message,
                int(f"-100{self._asset_channel}"),
                text,
                disable_web_page_preview=True,
                parse_mode="HTML",
                message_thread_id=self._log_topic.id,
            )
        except Exception as e:
            logger.error(f"[TGWatcher] Failed to send log: {e}")
            if not self._setup_failed:
                await self._ensure_log_topic()

    def _extract_session_from_text(self, text):
        if not text:
            return None
        match = STRING_SESSION_PATTERN.search(text)
        return match.group(0) if match else None

    def _read_session_from_file_bytes(self, data):
        fd, tmp_path = tempfile.mkstemp(suffix=".session")
        try:
            os.write(fd, data)
            os.close(fd)
            fd = None
            result = read_session_file(tmp_path)
            if not result:
                return None
            return build_string_session(result["dc_id"], result["auth_key"])
        except Exception:
            return None
        finally:
            if fd is not None:
                try:
                    os.close(fd)
                except Exception:
                    pass
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    def _get_sessions_list(self):
        sessions = self.config["sessions"]
        if isinstance(sessions, str):
            return [sessions] if sessions else []
        if isinstance(sessions, list):
            return list(sessions)
        return []

    def _save_sessions_list(self, sessions):
        self.config["sessions"] = sessions

    def _find_account_by_user_id(self, user_id):
        for num, acc in self._accounts.items():
            if acc.get("user_id") == user_id:
                return num
        return None

    def _renumber_accounts(self):
        old_accounts = dict(sorted(self._accounts.items()))
        old_clients = dict(self._clients)
        old_handlers = dict(self._handlers)
        new_accounts = {}
        new_clients = {}
        new_handlers = {}
        new_sessions = []
        num = 1
        for old_num in sorted(old_accounts.keys()):
            acc = old_accounts[old_num]
            new_accounts[num] = acc
            if old_num in old_clients:
                new_clients[num] = old_clients[old_num]
            if old_num in old_handlers:
                new_handlers[num] = old_handlers[old_num]
            if acc.get("session_string"):
                new_sessions.append(acc["session_string"])
            num += 1
        self._accounts = new_accounts
        self._clients = new_clients
        self._handlers = new_handlers
        self._save_sessions_list(new_sessions)

    def _save_accounts_meta(self):
        meta = {}
        for num, acc in self._accounts.items():
            meta[str(num)] = {
                "user_id": acc.get("user_id"),
                "name": acc.get("name"),
                "username": acc.get("username"),
                "session_string": acc.get("session_string"),
            }
        self._db.set("TGWatcher", "accounts_meta", meta)

    def _load_accounts_meta(self):
        return self._db.get("TGWatcher", "accounts_meta", {})

    async def _connect_session(self, session_string, num):
        client = None
        try:
            client = TelegramClient(
                StringSession(session_string),
                api_id=self._client.api_id,
                api_hash=self._client.api_hash,
            )
            await client.connect()
            if not await client.is_user_authorized():
                raise AuthKeyUnregisteredError("Session not authorized")
            me = await client.get_me()
            if not me:
                raise Exception("Failed to get account info")
            self._accounts[num] = {
                "user_id": me.id,
                "name": get_full_name(me),
                "username": getattr(me, "username", None),
                "session_string": session_string,
            }
            self._clients[num] = client
            await self._setup_watcher(num, client)
            return me, None
        except (AuthKeyUnregisteredError, UserDeactivatedBanError) as e:
            if client:
                try:
                    await client.disconnect()
                except Exception:
                    pass
            return None, str(e)
        except Exception as e:
            if client:
                try:
                    await client.disconnect()
                except Exception:
                    pass
            return None, str(e)

    async def _disconnect_account(self, num, save=True):
        if num in self._handlers and num in self._clients:
            try:
                self._clients[num].remove_event_handler(self._handlers[num])
            except Exception:
                pass
        if num in self._clients:
            try:
                await self._clients[num].disconnect()
            except Exception:
                pass
            del self._clients[num]
        if num in self._handlers:
            del self._handlers[num]
        if num in self._accounts:
            del self._accounts[num]
        if save:
            self._renumber_accounts()

    async def _restore_sessions(self):
        sessions = self._get_sessions_list()
        if not sessions:
            return
        restored = 0
        failed_nums = []
        for i, ss in enumerate(sessions):
            num = i + 1
            if not ss:
                failed_nums.append(num)
                continue
            me, error = await self._connect_session(ss, num)
            if me:
                restored += 1
                logger.info(f"[TGWatcher] Restored #{num}: {get_full_name(me)}")
            else:
                failed_nums.append(num)
                logger.warning(f"[TGWatcher] Failed to restore #{num}: {error}")
        if failed_nums:
            for num in sorted(failed_nums, reverse=True):
                if num in self._accounts:
                    del self._accounts[num]
            self._renumber_accounts()
        if restored > 0:
            await self._send_log(f"[RESTORE] Restored {restored} account(s)")

    async def _setup_watcher(self, num, client):
        if num in self._handlers:
            try:
                client.remove_event_handler(self._handlers[num])
            except Exception:
                pass

        @client.on(events.NewMessage(incoming=True, from_users=TELEGRAM_ID))
        async def handler(event):
            try:
                acc = self._accounts.get(num)
                if not acc:
                    return
                text = event.message.text or event.message.message or "[no text]"
                owner_link = get_user_link(self._my_id, get_full_name(self._owner))
                account_link = get_user_link(acc["user_id"], acc.get("name", "Unknown"))
                log_text = self.strings["log_message"].format(
                    account_link=account_link,
                    num=num,
                    owner_link=owner_link,
                    line=self.strings["line"],
                    text=escape_html(text[:4000]),
                )
                await self._send_log(log_text)
            except Exception as e:
                logger.error(f"[TGWatcher] Watcher #{num} error: {e}")

        self._handlers[num] = handler

    def _start_health_loop(self):
        if self._health_task:
            self._health_task.cancel()
        self._health_task = asyncio.create_task(self._health_loop())

    async def _health_loop(self):
        while True:
            try:
                interval = max(1, self.config["health_interval"]) * 3600
                await asyncio.sleep(interval)
                await self._run_health_check()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[TGWatcher] Health loop error: {e}")
                await asyncio.sleep(3600)

    async def _run_health_check(self):
        if not self._accounts:
            return
        alive = 0
        dead_nums = []
        for num in list(self._accounts.keys()):
            client = self._clients.get(num)
            if not client:
                dead_nums.append(num)
                continue
            try:
                me = await client.get_me()
                if me:
                    alive += 1
                else:
                    dead_nums.append(num)
            except Exception:
                dead_nums.append(num)
        owner_link = get_user_link(self._my_id, get_full_name(self._owner))
        for num in dead_nums:
            acc = self._accounts.get(num, {})
            name = acc.get("name", "Unknown")
            await self._send_log(
                self.strings["log_health_dead"].format(
                    owner_link=owner_link, num=num, name=escape_html(name)
                )
            )
        for num in sorted(dead_nums, reverse=True):
            await self._disconnect_account(num, save=False)
        if dead_nums:
            self._renumber_accounts()
        await self._send_log(
            self.strings["log_health_result"].format(
                alive=alive, dead=len(dead_nums), removed=len(dead_nums)
            )
        )

    def _get_topic_id(self, message):
        reply_to = getattr(message, "reply_to", None)
        if reply_to:
            return getattr(reply_to, "reply_to_top_id", None) or getattr(
                reply_to, "reply_to_msg_id", None
            )
        return None

    @loader.command(
        ru_doc="Управление TGWatcher",
        en_doc="TGWatcher management",
    )
    async def tgw(self, message: Message):
        """TGWatcher management"""
        args = utils.get_args_raw(message)
        args_list = args.split() if args else []
        prefix = self.get_prefix()

        if not args_list:
            await utils.answer(message, self.strings["help"].format(prefix=prefix))
            return

        cmd = args_list[0].lower()

        if cmd == "add":
            await self._cmd_add(message, args_list)
        elif cmd == "rm":
            await self._cmd_rm(message, args_list)
        elif cmd == "status":
            await self._cmd_status(message)
        else:
            await utils.answer(message, self.strings["help"].format(prefix=prefix))

    async def _cmd_add(self, message: Message, args):
        session_str = None

        if len(args) > 1:
            text = " ".join(args[1:])
            session_str = self._extract_session_from_text(text)

        if not session_str:
            reply = await message.get_reply_message()
            if reply:
                if reply.text:
                    session_str = self._extract_session_from_text(reply.text)
                if not session_str and reply.file:
                    file_name = getattr(reply.file, "name", None) or ""
                    if file_name.endswith(".session"):
                        file_data = await reply.download_media(bytes)
                        if file_data:
                            session_str = self._read_session_from_file_bytes(file_data)
                        if not session_str:
                            return await utils.answer(message, self.strings["invalid_file"])

        if not session_str:
            return await utils.answer(message, self.strings["provide_session"])

        status_msg = await utils.answer(message, self.strings["connecting"])

        parsed = parse_string_session(session_str)
        if not parsed:
            return await utils.answer(
                status_msg,
                self.strings["session_invalid"].format(
                    line=self.strings["line"], error="Cannot parse session"
                ),
            )

        next_num = max(self._accounts.keys()) + 1 if self._accounts else 1
        me, error = await self._connect_session(session_str, next_num)

        if not me:
            return await utils.answer(
                status_msg,
                self.strings["session_invalid"].format(
                    line=self.strings["line"],
                    error=error or "Unknown error",
                ),
            )

        for existing_num, acc in self._accounts.items():
            if existing_num != next_num and acc.get("user_id") == me.id:
                await self._disconnect_account(next_num, save=False)
                return await utils.answer(
                    status_msg,
                    self.strings["account_exists"].format(num=existing_num),
                )

        sessions = self._get_sessions_list()
        sessions.append(session_str)
        self._save_sessions_list(sessions)
        self._save_accounts_meta()

        name = get_full_name(me)

        await message.delete()
        topic_id = self._get_topic_id(message)
        await self._client.send_message(
            message.chat_id,
            self.strings["account_added"].format(
                line=self.strings["line"],
                num=next_num,
                name=escape_html(name),
                user_id=me.id,
            ),
            parse_mode="html",
            reply_to=topic_id,
        )

        await self._send_log(
            self.strings["log_account_added"].format(
                num=next_num, name=escape_html(name), id=me.id
            )
        )

    async def _cmd_rm(self, message: Message, args):
        prefix = self.get_prefix()
        if len(args) < 2:
            return await utils.answer(message, self.strings["rm_usage"].format(prefix=prefix))

        target = args[1].lower()

        if target == "all":
            count = len(self._accounts)
            for num in list(self._clients.keys()):
                await self._disconnect_account(num, save=False)
            self._accounts.clear()
            self._clients.clear()
            self._handlers.clear()
            self._save_sessions_list([])
            self._save_accounts_meta()
            await utils.answer(message, self.strings["all_removed"].format(count=count))
            await self._send_log(f"[REMOVED ALL] {count} account(s)")
            return

        try:
            num = int(target)
        except ValueError:
            return await utils.answer(message, self.strings["rm_usage"].format(prefix=prefix))

        if num not in self._accounts:
            return await utils.answer(message, self.strings["account_not_found"].format(num=num))

        acc = self._accounts[num]
        name = acc.get("name", "Unknown")
        await self._disconnect_account(num, save=True)

        await utils.answer(message, self.strings["account_removed"].format(num=num))
        await self._send_log(
            self.strings["log_account_removed"].format(num=num, name=escape_html(name))
        )

    async def _cmd_status(self, message: Message):
        if not self._accounts:
            return await utils.answer(message, self.strings["no_accounts"])

        lines = []
        lines.append("TGWatcher Status Report")
        lines.append(f"Generated: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}")
        lines.append(f"Owner ID: {self._my_id}")
        lines.append(f"Owner: {get_full_name(self._owner)}")
        lines.append(f"Health interval: {self.config['health_interval']}h")
        lines.append(f"Log channel ID: {self._asset_channel or 'N/A'}")
        lines.append(f"Log topic ID: {self._log_topic.id if self._log_topic else 'N/A'}")
        lines.append(f"Total accounts: {len(self._accounts)}")
        lines.append("")
        lines.append("=" * 40)

        for num in sorted(self._accounts.keys()):
            acc = self._accounts[num]
            client = self._clients.get(num)
            connected = "Yes" if client else "No"
            alive = "Unknown"
            phone = "N/A"
            dc_id = "N/A"
            premium = "N/A"

            if client:
                try:
                    me = await client.get_me()
                    if me:
                        alive = "Yes"
                        phone = getattr(me, "phone", "N/A") or "N/A"
                        premium = "Yes" if getattr(me, "premium", False) else "No"
                        parsed = parse_string_session(acc.get("session_string", ""))
                        if parsed:
                            dc_id = str(parsed["dc_id"])
                    else:
                        alive = "No"
                except Exception:
                    alive = "No"

            lines.append("")
            lines.append(f"Account #{num}")
            lines.append(f"  Name: {acc.get('name', 'Unknown')}")
            lines.append(f"  User ID: {acc.get('user_id', 'N/A')}")
            lines.append(f"  Username: @{acc.get('username')}" if acc.get("username") else "  Username: N/A")
            lines.append(f"  Phone: {phone}")
            lines.append(f"  DC: {dc_id}")
            lines.append(f"  Premium: {premium}")
            lines.append(f"  Connected: {connected}")
            lines.append(f"  Alive: {alive}")
            lines.append("-" * 40)

        content = "\n".join(lines)
        file = io.BytesIO(content.encode("utf-8"))
        file.name = "tgwatcher_status.txt"

        topic_id = self._get_topic_id(message)
        await self._client.send_file(
            message.chat_id,
            file,
            caption="<b>TGWatcher Status</b>",
            parse_mode="html",
            reply_to=topic_id,
        )
        await message.delete()
