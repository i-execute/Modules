# CopyLeft 2026 github.com/i-execute
# Author: I_execute.t.me
# Licensed under AGPLv3.

__version__ = (2, 5, 0)
# meta developer: Execute_forge.t.me
# meta banner: https://raw.githubusercontent.com/i-execute/Modules/main/Storage/Deleter/MetaBanner.jpeg

import asyncio
import logging
import os
import random
import re
import shutil
import tempfile
import time
from array import array
from datetime import timedelta, timezone, datetime

from telethon.errors import FloodWaitError
from telethon.tl.functions.channels import LeaveChannelRequest
from telethon.tl.functions.messages import EditForumTopicRequest, EditMessageRequest, GetHistoryRequest
from telethon.tl.types import Message, MessageEmpty, User, InputMediaWebPage

from .. import loader, utils
from ..inline.types import InlineCall

logger = logging.getLogger(__name__)

async def _retry_topic_send(send, refresh):
    try:
        return await send()
    except Exception:
        await refresh()
        return await send()


async def _apply_topic_icon(client, peer, topic, icon_id):
    if getattr(topic, "icon_emoji_id", None) == icon_id:
        return
    try:
        await client(EditForumTopicRequest(peer=peer, topic_id=topic.id, icon_emoji_id=icon_id))
    except Exception:
        pass


SAYONARA_URL = "https://raw.githubusercontent.com/i-execute/Modules/main/Storage/Deleter/Sayonara.mp4"
RELOADING_MEDIA_URL = "https://raw.githubusercontent.com/i-execute/Modules/main/Storage/Deleter/Reloading.jpeg"

WIPE_PREFIX = "deleter_wipe_"
WIPE_CHUNK = 10000
WIPE_BATCH = 100
WIPE_PARALLEL = 8
WIPE_DELAY = 0.2
WIPE_PROGRESS_EVERY = 3


def _rss_bytes():
    try:
        with open("/proc/self/statm") as fh:
            return int(fh.read().split()[1]) * os.sysconf("SC_PAGE_SIZE")
    except Exception:
        return 0


class _IdSpool:
    def __init__(self, ram_limit, base_dir=None):
        if not base_dir:
            base_dir = tempfile.gettempdir()
        os.makedirs(base_dir, exist_ok=True)
        self.dir = tempfile.mkdtemp(prefix=WIPE_PREFIX, dir=base_dir)
        self.limit = ram_limit
        self.queue = asyncio.Queue()
        self.mem = 0
        self.on_disk = 0
        self.spilled = 0
        self.seq = 0

    def push(self, ids):
        size = len(ids) * ids.itemsize
        if self.mem + size > self.limit or _rss_bytes() > self.limit:
            self.seq += 1
            path = os.path.join(self.dir, f"{self.seq:07d}.bin")
            with open(path, "wb") as fh:
                ids.tofile(fh)
            self.on_disk += 1
            self.spilled += 1
            self.queue.put_nowait(path)
        else:
            self.mem += size
            self.queue.put_nowait(ids)

    def load(self, item):
        if isinstance(item, str):
            ids = array("q")
            with open(item, "rb") as fh:
                ids.frombytes(fh.read())
            os.remove(item)
            self.on_disk -= 1
            return ids
        self.mem -= len(item) * item.itemsize
        return item

    def close(self):
        shutil.rmtree(self.dir, ignore_errors=True)


@loader.tds
class Deleter(loader.Module):
    """Swift deleting messages"""

    strings = {
        "name": "Deleter",
        "reloaded": "<blockquote><b>Deleter module successfully reloaded, everything works</b></blockquote>",
        "help": (
            "<b>Deleter - swift message deletion</b>\n"
            "<b>Own messages:</b>\n"
            "<blockquote>"
            "<code>{prefix}del me</code> - delete all your messages in current chat\n"
            "<code>{prefix}del me -f</code> - delete all your messages and leave the chat\n"
            "<code>{prefix}del [n]</code> - delete last n your messages\n"
            "<code>{prefix}del [n]</code> (reply) - delete n your messages before replied message\n"
            "<code>{prefix}del now</code> - delete your messages from last 5 minutes\n"
            "<code>{prefix}del today</code> - delete your messages since 00:00 today"
            "</blockquote>\n"
            "<b>Other users:</b>\n"
            "<blockquote>"
            "<code>{prefix}del @username</code> - delete all messages from specified user\n"
            "<code>{prefix}del before</code> (reply) - delete all messages from everyone before replied message\n"
            "<code>{prefix}del after</code> (reply) - delete all messages from everyone after replied message\n"
            "<code>{prefix}del</code> - <b>Wipe Entire Chat</b> button: delete all messages from all members, streaming ids to /tmp in chunks"
            "</blockquote>\n"
            "<b>Purger:</b>\n"
            "<blockquote>"
            "<code>{prefix}purge</code> - enable auto-delete of all new messages in current chat, survives restarts, disabled only via its inline form"
            "</blockquote>"
        ),
        "main_menu": (
            "<b>Deleter Control Panel</b>\n"
            "<blockquote>Select action:</blockquote>"
        ),
        "btn_usage": "Usage",
        "btn_del_today": "Delete All Today",
        "btn_wipe": "Wipe Entire Chat",
        "btn_back": "Back",
        "btn_confirm": "Confirm",
        "btn_cancel": "Cancel",
        "btn_stop": "Stop",
        "btn_close": "Close",
        "purger_activated": (
            "<b>Purger Mode Activated</b>\n"
            "<blockquote>All new messages in this chat will be deleted.</blockquote>"
        ),
        "purger_stopped": (
            "<b>Purger Stopped</b>\n"
            "<blockquote>Auto-delete disabled for this chat.</blockquote>"
        ),
        "del_today_confirm": (
            "<b>Are you sure?</b>\n"
            "<blockquote>This will delete ALL messages from everyone in this chat since 00:00 today.</blockquote>"
        ),
        "del_today_progress": (
            "<b>Deleting messages...</b>\n"
            "<blockquote>Deleted: {count}</blockquote>"
        ),
        "del_today_done": (
            "<b>Done</b>\n"
            "<blockquote>Deleted {count} messages from today.</blockquote>"
        ),
        "wipe_confirm": (
            "<b>Wipe Entire Chat</b>\n"
            "<blockquote>Target chat: <b>{chat_title}</b>\n"
            "Chat ID: <code>{chat_id}</code>\n"
            "Type: <b>{chat_type}</b>\n\n"
            "ALL messages from ALL members will be fetched and deleted permanently. "
            "This action cannot be undone.</blockquote>"
        ),
        "wipe_progress_collect": (
            "<b>Fetching messages...</b>\n"
            "<blockquote>Chat: <b>{chat_title}</b>\n"
            "Collected: <code>{collected}</code>\n"
            "Spool on disk: <code>{disk}</code>\n"
            "Elapsed: <code>{elapsed}</code></blockquote>"
        ),
        "wipe_progress_delete": (
            "<b>Deleting messages...</b>\n"
            "<blockquote>Chat: <b>{chat_title}</b>\n"
            "Total collected: <code>{collected}</code>\n"
            "Deleted: <code>{deleted}</code>\n"
            "Failed: <code>{failed}</code>\n"
            "Speed: <code>{speed}</code> msg/s\n"
            "Elapsed: <code>{elapsed}</code></blockquote>"
        ),
        "btn_wipe_run": "Wipe Now",
        "btn_input_chat_id": "Enter Chat ID",
        "prompt_chat_id": "Enter chat ID (e.g. -1001234567890) or username:",
        "wipe_chat_not_found": "<b>Error:</b>\n<blockquote>Could not resolve chat <code>{chat}</code></blockquote>",
        "wipe_progress": (
            "<b>Wiping chat...</b>\n"
            "<blockquote>Collected: <code>{collected}</code>\n"
            "Deleted: <code>{deleted}</code>\n"
            "Failed: <code>{failed}</code>\n"
            "Chunks on disk: <code>{disk}</code> (spilled total: <code>{spilled}</code>)\n"
            "Speed: <code>{speed}</code> msg/s\n"
            "Elapsed: <code>{elapsed}</code></blockquote>"
        ),
        "wipe_done": (
            "<b>Chat wiped</b>\n"
            "<blockquote>Chat: {chat}\n"
            "Collected: <code>{collected}</code>\n"
            "Deleted: <code>{deleted}</code>\n"
            "Failed: <code>{failed}</code>\n"
            "Chunks spilled to disk: <code>{spilled}</code>\n"
            "Time: <code>{elapsed}</code></blockquote>"
        ),
        "wipe_stopped": (
            "<b>Wipe stopped</b>\n"
            "<blockquote>Deleted: <code>{deleted}</code>\n"
            "Temporary chunks removed.</blockquote>"
        ),
        "wipe_running": "Wipe is already running in this chat",
        "wipe_no_rights": "<b>Error:</b>\n<blockquote>No right to delete messages of other members in this chat</blockquote>",
        "wipe_last_error": "\n<b>Last error:</b> <code>{error}</code>",
        "no_count": "<b>Error:</b> Provide a valid number of messages",
        "no_reply": "<b>Error:</b> Reply to a message",
        "no_user": "<b>Error:</b> User not found",
        "no_perms": "<b>Error:</b> Not enough permissions to delete some messages",
        "error": "<b>Error:</b>\n<blockquote>{error}</blockquote>",
        "done_me": (
            "<b>All your {count} messages in chat</b>\n"
            "<blockquote>{chat}</blockquote>\n"
            "<b>have been deleted.</b>"
        ),
        "done_leave": (
            "<b>Left chat:</b>\n"
            "<blockquote>{chat}</blockquote>\n"
            "<b>Deleted messages:</b> <code>{count}</code>\n"
            "<b>First message date:</b> <code>{first_date}</code>\n"
            "<b>Last message date:</b> <code>{last_date}</code>"
        ),
    }

    strings_ru = {
        "reloaded": "<blockquote><b>Модуль Deleter был успешно перезагружен, все воркает</b></blockquote>",
        "help": (
            "<b>Deleter - быстрое удаление сообщений</b>\n"
            "<b>Свои сообщения:</b>\n"
            "<blockquote>"
            "<code>{prefix}del me</code> - удалить все свои сообщения в текущем чате\n"
            "<code>{prefix}del me -f</code> - удалить все свои сообщения и покинуть чат\n"
            "<code>{prefix}del [n]</code> - удалить последние n своих сообщений\n"
            "<code>{prefix}del [n]</code> (реплай) - удалить n своих сообщений до указанного сообщения\n"
            "<code>{prefix}del now</code> - удалить свои сообщения за последние 5 минут\n"
            "<code>{prefix}del today</code> - удалить свои сообщения с 00:00 сегодня"
            "</blockquote>\n"
            "<b>Другие пользователи:</b>\n"
            "<blockquote>"
            "<code>{prefix}del @username</code> - удалить все сообщения указанного пользователя\n"
            "<code>{prefix}del before</code> (реплай) - удалить все сообщения от всех до указанного сообщения\n"
            "<code>{prefix}del after</code> (реплай) - удалить все сообщения от всех после указанного сообщения\n"
            "<code>{prefix}del</code> - кнопка <b>Полная очистка чата</b>: удалить все сообщения от всех участников, id сбрасываются чанками в /tmp"
            "</blockquote>\n"
            "<b>Purger:</b>\n"
            "<blockquote>"
            "<code>{prefix}purge</code> - включить авто-удаление всех новых сообщений в текущем чате, переживает перезагрузки, выключается только через инлайн форму"
            "</blockquote>"
        ),
        "main_menu": (
            "<b>Панель управления Deleter</b>\n"
            "<blockquote>Выберите действие:</blockquote>"
        ),
        "btn_usage": "Инструкция",
        "btn_del_today": "Удалить все за сегодня",
        "btn_wipe": "Полная очистка чата",
        "btn_back": "Назад",
        "btn_confirm": "Подтвердить",
        "btn_cancel": "Отмена",
        "btn_stop": "Остановить",
        "btn_close": "Закрыть",
        "purger_activated": (
            "<b>Режим Purger активирован</b>\n"
            "<blockquote>Все новые сообщения в этом чате будут удалены.</blockquote>"
        ),
        "purger_stopped": (
            "<b>Purger остановлен</b>\n"
            "<blockquote>Авто-удаление отключено для этого чата.</blockquote>"
        ),
        "del_today_confirm": (
            "<b>Вы уверены?</b>\n"
            "<blockquote>Будут удалены ВСЕ сообщения от всех в этом чате с 00:00 сегодня.</blockquote>"
        ),
        "del_today_progress": (
            "<b>Удаление сообщений...</b>\n"
            "<blockquote>Удалено: {count}</blockquote>"
        ),
        "del_today_done": (
            "<b>Готово</b>\n"
            "<blockquote>Удалено {count} сообщений за сегодня.</blockquote>"
        ),
        "wipe_confirm": (
            "<b>Полная очистка чата</b>\n"
            "<blockquote>Целевой чат: <b>{chat_title}</b>\n"
            "ID чата: <code>{chat_id}</code>\n"
            "Тип: <b>{chat_type}</b>\n\n"
            "ВСЕ сообщения от ВСЕХ участников будут выгружены и безвозвратно удалены. "
            "Это действие нельзя отменить.</blockquote>"
        ),
        "wipe_progress_collect": (
            "<b>Выгрузка сообщений...</b>\n"
            "<blockquote>Чат: <b>{chat_title}</b>\n"
            "Собрано: <code>{collected}</code>\n"
            "Чанков на диске: <code>{disk}</code>\n"
            "Прошло: <code>{elapsed}</code></blockquote>"
        ),
        "wipe_progress_delete": (
            "<b>Удаление сообщений...</b>\n"
            "<blockquote>Чат: <b>{chat_title}</b>\n"
            "Всего собрано: <code>{collected}</code>\n"
            "Удалено: <code>{deleted}</code>\n"
            "Ошибок: <code>{failed}</code>\n"
            "Скорость: <code>{speed}</code> сообщ/с\n"
            "Прошло: <code>{elapsed}</code></blockquote>"
        ),
        "btn_wipe_run": "Удалить сейчас",
        "btn_input_chat_id": "Ввести ID чата",
        "prompt_chat_id": "Введите ID чата (например -1001234567890) или юзернейм:",
        "wipe_chat_not_found": "<b>Ошибка:</b>\n<blockquote>Не удалось найти чат <code>{chat}</code></blockquote>",
        "wipe_progress": (
            "<b>Очистка чата...</b>\n"
            "<blockquote>Собрано: <code>{collected}</code>\n"
            "Удалено: <code>{deleted}</code>\n"
            "Ошибок: <code>{failed}</code>\n"
            "Чанков на диске: <code>{disk}</code> (всего выгружено: <code>{spilled}</code>)\n"
            "Скорость: <code>{speed}</code> сообщ/с\n"
            "Прошло: <code>{elapsed}</code></blockquote>"
        ),
        "wipe_done": (
            "<b>Чат очищен</b>\n"
            "<blockquote>Чат: {chat}\n"
            "Собрано: <code>{collected}</code>\n"
            "Удалено: <code>{deleted}</code>\n"
            "Ошибок: <code>{failed}</code>\n"
            "Чанков выгружено на диск: <code>{spilled}</code>\n"
            "Время: <code>{elapsed}</code></blockquote>"
        ),
        "wipe_stopped": (
            "<b>Очистка остановлена</b>\n"
            "<blockquote>Удалено: <code>{deleted}</code>\n"
            "Временные чанки удалены.</blockquote>"
        ),
        "wipe_running": "Очистка уже идет в этом чате",
        "wipe_no_rights": "<b>Ошибка:</b>\n<blockquote>Нет права удалять сообщения других участников в этом чате</blockquote>",
        "wipe_last_error": "\n<b>Последняя ошибка:</b> <code>{error}</code>",
        "no_count": "<b>Ошибка:</b> Укажите корректное количество сообщений",
        "no_reply": "<b>Ошибка:</b> Ответьте на сообщение",
        "no_user": "<b>Ошибка:</b> Пользователь не найден",
        "no_perms": "<b>Ошибка:</b> Недостаточно прав для удаления некоторых сообщений",
        "error": "<b>Ошибка:</b>\n<blockquote>{error}</blockquote>",
        "done_me": (
            "<b>Все ваши {count} сообщения в чате</b>\n"
            "<blockquote>{chat}</blockquote>\n"
            "<b>удалены.</b>"
        ),
        "done_leave": (
            "<b>Вышел из чата:</b>\n"
            "<blockquote>{chat}</blockquote>\n"
            "<b>Удалено сообщений:</b> <code>{count}</code>\n"
            "<b>Дата первого сообщения:</b> <code>{first_date}</code>\n"
            "<b>Дата последнего сообщения:</b> <code>{last_date}</code>"
        ),
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "TIMEZONE_OFFSET",
                3,
                doc="UTC timezone offset. Used for .del today command",
                validator=loader.validators.Integer(),
            ),
            loader.ConfigValue(
                "WIPE_RAM_LIMIT_MB",
                256,
                doc="RAM limit in MB for chat wipe. When the userbot process or the collected id buffer exceeds it, id chunks are written to /tmp instead of memory",
                validator=loader.validators.Integer(minimum=16, maximum=2048),
            ),
        )
        self._wipe_tasks = {}
        self._deleter_topic = None
        self._asset_channel = None
        self._purger_tasks = {}
        self._purger_chats = set()
        self._purger_locks = {}
        self._purger_protected = {}

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        self._owner = await client.get_me()
        self._tg_id = self._owner.id if self._owner else getattr(client, "tg_id", None)

        uid = getattr(self, "_tg_id", None) or "common"
        user_dir = os.path.join(tempfile.gettempdir(), "Deleter", str(uid))
        os.makedirs(user_dir, exist_ok=True)
        try:
            for name in os.listdir(user_dir):
                if name.startswith(WIPE_PREFIX):
                    shutil.rmtree(os.path.join(user_dir, name), ignore_errors=True)
        except Exception as e:
            logger.error("[Deleter] Failed to clean stale wipe chunks: %s", e)

        for key in list(self.get("purger_chats", {}) or {}):
            try:
                self._start_purger(int(key))
            except Exception as e:
                logger.error("[Deleter] Failed to restore purger for %s: %s", key, e)

        self._asset_channel = db.get("heroku.forums", "channel_id", None)

        if not self._asset_channel:
            logger.warning("[Deleter] heroku.forums channel_id not found in DB, attempting create new one...")
            return

        try:
            self._deleter_topic = await utils.asset_forum_topic(
                self._client,
                self._db,
                self._asset_channel,
                "Deleter",
                description="Logs of message deletion by Deleter module.",
                icon_emoji_id=5386654279341747455,
            )
            await _apply_topic_icon(self._client, self._asset_channel, self._deleter_topic, 5386654279341747455)
        except Exception as e:
            logger.error("[Deleter] Failed to create/get forum topic: %s", e)
            return

        chat_id = int(f"-100{self._asset_channel}")
        greeting_key = f"deleter_greeted_{self._asset_channel}_{self._deleter_topic.id}"
        already_greeted = self.get(greeting_key, False)
        if already_greeted:
            await self._send_with_preview(chat_id, self.strings["reloaded"])
        else:
            self.set(greeting_key, True)

    async def _refresh_deleter_topic(self):
        self._deleter_topic = await utils.asset_forum_topic(
            self._client,
            self._db,
            self._asset_channel,
            "Deleter",
            description="Logs of message deletion by Deleter module.",
            icon_emoji_id=5386654279341747455,
        )
        await _apply_topic_icon(self._client, self._asset_channel, self._deleter_topic, 5386654279341747455)

    async def _send_with_preview(self, chat_id, text):
        try:
            msg_text, entities = await self.inline.bot._parse_message_text(text, "html")
            msg = await _retry_topic_send(
                lambda: self.inline.bot.send_message(
                    chat_id,
                    msg_text,
                    parse_mode=None,
                    entities=entities,
                    message_thread_id=self._deleter_topic.id,
                ),
                self._refresh_deleter_topic,
            )
            if msg:
                try:
                    peer = await self.inline.bot.get_input_entity(chat_id)
                    current_msg = await self.inline.bot.get_messages(chat_id, ids=msg.id)
                    reply_markup = current_msg.reply_markup if current_msg else None
                    await self.inline.bot(EditMessageRequest(
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
                    logger.error("[Deleter] Failed to add preview: %s", e)
        except Exception as e:
            logger.error("[Deleter] Failed to send message with preview: %s", e)

    async def _send_log(self, text: str):
        if not self._deleter_topic or not self._asset_channel:
            try:
                me = await self._client.get_me()
                await self._client.send_message(me.id, text, parse_mode="html")
            except Exception as e:
                logger.error("[Deleter] Failed to send fallback log to Saved Messages: %s", e)
            return

        try:
            await _retry_topic_send(
                lambda: self.inline.bot.send_message(
                    int(f"-100{self._asset_channel}"),
                    text,
                    parse_mode="HTML",
                    message_thread_id=self._deleter_topic.id,
                ),
                self._refresh_deleter_topic,
            )
        except Exception as e:
            logger.error("[Deleter] Failed to send log to topic: %s", e)
            try:
                me = await self._client.get_me()
                await self._client.send_message(me.id, text, parse_mode="html")
            except Exception as e2:
                logger.error("[Deleter] Failed to send fallback log: %s", e2)

    async def _bulk_delete(self, client, chat_id, msg_ids: list) -> tuple:
        deleted = 0
        failed = 0

        if not msg_ids:
            return deleted, failed

        chunk_size = random.randint(90, 110)
        first_chunk = msg_ids[:chunk_size]
        rest = msg_ids[chunk_size:]

        try:
            await client.delete_messages(chat_id, first_chunk)
            deleted += len(first_chunk)
        except Exception:
            failed += len(first_chunk)

        chunk = []
        chunk_size = random.randint(90, 110)

        for mid in rest:
            chunk.append(mid)
            if len(chunk) >= chunk_size:
                await asyncio.sleep(random.uniform(0.5, 1.5))
                try:
                    await client.delete_messages(chat_id, chunk)
                    deleted += len(chunk)
                except Exception:
                    failed += len(chunk)
                chunk.clear()
                chunk_size = random.randint(90, 110)

        if chunk:
            await asyncio.sleep(random.uniform(0.5, 1.5))
            try:
                await client.delete_messages(chat_id, chunk)
                deleted += len(chunk)
            except Exception:
                failed += len(chunk)

        return deleted, failed

    def _main_markup(self):
        return [
            [{"text": self.strings["btn_usage"], "callback": self._cb_usage, "style": "primary"}],
            [{"text": self.strings["btn_del_today"], "callback": self._cb_del_today_confirm, "style": "danger"}],
            [{"text": self.strings["btn_wipe"], "callback": self._cb_wipe_confirm, "style": "danger"}],
        ]

    async def _cb_main_menu(self, call: InlineCall):
        await call.edit(self.strings["main_menu"], reply_markup=self._main_markup())

    async def _cb_usage(self, call: InlineCall):
        prefix = self.get_prefix()
        await call.edit(
            self.strings["help"].format(prefix=prefix),
            reply_markup=[
                [{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "primary"}],
            ],
        )

    async def on_unload(self):
        for task in self._purger_tasks.values():
            task.cancel()
        self._purger_tasks.clear()
        self._purger_chats.clear()
        for task in self._wipe_tasks.values():
            task.cancel()
        self._wipe_tasks.clear()

    def _purger_lock(self, chat_id: int) -> asyncio.Lock:
        lock = self._purger_locks.get(chat_id)
        if lock is None:
            lock = asyncio.Lock()
            self._purger_locks[chat_id] = lock
        return lock

    def _purger_save(self, chat_id: int, last_id: int, create: bool = False):
        chats = dict(self.get("purger_chats", {}) or {})
        key = str(chat_id)
        if key not in chats and not create:
            return
        chats[key] = last_id
        self.set("purger_chats", chats)

    def _purger_forget(self, chat_id: int):
        chats = dict(self.get("purger_chats", {}) or {})
        chats.pop(str(chat_id), None)
        self.set("purger_chats", chats)

    def _start_purger(self, chat_id: int):
        self._purger_chats.add(chat_id)
        task = self._purger_tasks.get(chat_id)
        if task and not task.done():
            return
        self._purger_tasks[chat_id] = asyncio.ensure_future(self._purger_worker(chat_id))

    def _stop_purger(self, chat_id: int):
        self._purger_chats.discard(chat_id)
        task = self._purger_tasks.pop(chat_id, None)
        if task and not task.done():
            task.cancel()
        self._purger_protected.pop(chat_id, None)
        self._purger_forget(chat_id)

    async def _cb_stop_purger(self, call: InlineCall, chat_id: int):
        self._stop_purger(chat_id)
        await call.edit(
            self.strings["purger_stopped"],
            reply_markup=[
                [{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}],
            ],
        )

    async def _cb_close(self, call: InlineCall):
        await call.delete()

    async def _purger_worker(self, chat_id: int):
        lock = self._purger_lock(chat_id)
        try:
            last_id = (self.get("purger_chats", {}) or {}).get(str(chat_id)) or 0

            if not last_id:
                async with lock:
                    async for msg in self._client.iter_messages(chat_id, limit=1):
                        last_id = msg.id
                        break
                self._purger_save(chat_id, last_id)

            while chat_id in self._purger_chats:
                await asyncio.sleep(2)

                try:
                    async with lock:
                        protected = self._purger_protected.get(chat_id, set())
                        top_id = last_id
                        new_msgs = []
                        async for msg in self._client.iter_messages(chat_id, min_id=last_id):
                            if msg.id > top_id:
                                top_id = msg.id
                            if msg.id not in protected:
                                new_msgs.append(msg.id)

                        if new_msgs:
                            try:
                                await self._client.delete_messages(chat_id, new_msgs)
                            except Exception as e:
                                logger.error("[Deleter] Purger delete error: %s", e)

                        if top_id != last_id:
                            last_id = top_id
                            self._purger_save(chat_id, last_id)

                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.error("[Deleter] Purger iter error: %s", e)
                    await asyncio.sleep(5)

        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error("[Deleter] Purger worker error: %s", e)

    async def _cb_del_today_confirm(self, call: InlineCall):
        await call.edit(
            self.strings["del_today_confirm"],
            reply_markup=[
                [
                    {"text": self.strings["btn_confirm"], "callback": self._cb_del_today_execute, "style": "danger"},
                    {"text": self.strings["btn_cancel"], "callback": self._cb_main_menu, "style": "primary"},
                ],
            ],
        )

    async def _cb_del_today_execute(self, call: InlineCall):
        chat_id = call.form["chat"]

        offset = self.config["TIMEZONE_OFFSET"]
        tz = timezone(timedelta(hours=offset))
        now = datetime.now(tz)
        midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)

        await call.edit(self.strings["del_today_progress"].format(count=0))

        try:
            ids = []
            count = 0

            async for msg in self._client.iter_messages(chat_id):
                msg_local = msg.date.astimezone(tz)
                if msg_local < midnight:
                    break

                ids.append(msg.id)
                count += 1

                if count % 100 == 0:
                    await call.edit(self.strings["del_today_progress"].format(count=count))

            if ids:
                await self._bulk_delete(self._client, chat_id, ids)

            await call.edit(
                self.strings["del_today_done"].format(count=count),
                reply_markup=[
                    [{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "primary"}],
                ],
            )

        except Exception as e:
            logger.error("[Deleter] del today execute error: %s", e)
            await call.edit(
                self.strings["error"].format(error=str(e)),
                reply_markup=[
                    [{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "primary"}],
                ],
            )

    async def _get_chat_info(self, chat_id):
        try:
            entity = await self._client.get_entity(chat_id)
            title = getattr(entity, "title", None) or getattr(entity, "first_name", None) or str(chat_id)
            if getattr(entity, "broadcast", False):
                ctype = "Channel"
            elif getattr(entity, "megagroup", False):
                ctype = "Supergroup"
            elif getattr(entity, "gigagroup", False):
                ctype = "Gigagroup"
            elif isinstance(entity, User):
                ctype = "Private Chat"
            else:
                ctype = "Group"
            cid = utils.get_chat_id(entity)
            return entity, title, cid, ctype
        except Exception:
            return None, str(chat_id), chat_id, "Unknown"

    async def _cb_wipe_confirm(self, call: InlineCall, target_chat=None):
        if target_chat is None:
            target_chat = call.form["chat"]
        _, title, cid, ctype = await self._get_chat_info(target_chat)
        text = self.strings["wipe_confirm"].format(
            chat_title=utils.escape_html(title),
            chat_id=cid,
            chat_type=ctype,
        )
        markup = [
            [
                {"text": self.strings["btn_wipe_run"], "callback": self._cb_wipe_execute, "args": (cid,), "style": "danger"},
            ],
            [
                {"text": self.strings["btn_input_chat_id"], "input": self.strings["prompt_chat_id"], "handler": self._ih_wipe_chat_id, "style": "primary"},
            ],
            [
                {"text": self.strings["btn_cancel"], "callback": self._cb_main_menu, "style": "primary"},
            ],
        ]
        await call.edit(text, reply_markup=markup)

    async def _ih_wipe_chat_id(self, call: InlineCall, raw_id: str):
        raw_id = raw_id.strip()
        target = raw_id
        if re.match(r"^-?\d+$", raw_id):
            target = int(raw_id)
        try:
            entity = await self._client.get_entity(target)
            await self._cb_wipe_confirm(call, target_chat=utils.get_chat_id(entity))
        except Exception as e:
            await call.edit(
                self.strings["wipe_chat_not_found"].format(chat=utils.escape_html(raw_id)),
                reply_markup=[
                    [{"text": self.strings["btn_back"], "callback": self._cb_wipe_confirm, "style": "primary"}],
                ],
            )

    async def _wipe_allowed(self, chat_id):
        try:
            entity = await self._client.get_entity(chat_id)
        except Exception:
            return True
        if isinstance(entity, User):
            return True
        try:
            perms = await self._client.get_permissions(entity, "me")
            return bool(perms.is_creator or perms.delete_messages)
        except Exception:
            return True

    async def _cb_wipe_execute(self, call: InlineCall, chat_id=None):
        if chat_id is None:
            chat_id = call.form["chat"]
        task = self._wipe_tasks.get(chat_id)
        if task and not task.done():
            await call.answer(self.strings["wipe_running"], show_alert=True)
            return
        if not await self._wipe_allowed(chat_id):
            await call.edit(
                self.strings["wipe_no_rights"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "primary"}]],
            )
            return
        protected = {call.form.get("message_id")} - {None}
        self._wipe_tasks[chat_id] = asyncio.ensure_future(self._wipe_run(call, chat_id, protected))

    async def _cb_wipe_stop(self, call: InlineCall, chat_id):
        task = self._wipe_tasks.get(chat_id)
        if task and not task.done():
            task.cancel()
        else:
            await call.delete()

    def _wipe_text(self, key, state, spool, chat=""):
        elapsed = max(time.monotonic() - state["started"], 0.001)
        chat_title = state.get("chat_title") or chat or "Chat"
        actual_key = key
        if key == "wipe_progress":
            actual_key = "wipe_progress_delete" if state.get("phase") == "delete" else "wipe_progress_collect"
        text = self.strings[actual_key].format(
            chat=chat,
            chat_title=utils.escape_html(chat_title),
            collected=state["collected"],
            deleted=state["deleted"],
            failed=state["failed"],
            disk=spool.on_disk if spool else 0,
            spilled=spool.spilled if spool else 0,
            speed=int(state["deleted"] / elapsed),
            elapsed=str(timedelta(seconds=int(elapsed))),
        )
        if state.get("error") and key != "wipe_stopped":
            text += self.strings["wipe_last_error"].format(error=utils.escape_html(state["error"][:200]))
        return text

    async def _wipe_collect(self, peer, spool, protected, state):
        offset = 0
        buf = array("q")
        while True:
            try:
                res = await self._client(GetHistoryRequest(
                    peer=peer,
                    offset_id=offset,
                    offset_date=None,
                    add_offset=0,
                    limit=WIPE_BATCH,
                    max_id=0,
                    min_id=0,
                    hash=0,
                ))
            except FloodWaitError as e:
                await asyncio.sleep(e.seconds + 1)
                continue
            ids = [m.id for m in res.messages if not isinstance(m, MessageEmpty)]
            del res
            if not ids:
                break
            offset = min(ids)
            state["collected"] += len(ids)
            buf.extend(i for i in ids if i not in protected)
            while len(buf) >= WIPE_CHUNK:
                spool.push(buf[:WIPE_CHUNK])
                buf = buf[WIPE_CHUNK:]
            if offset <= 1:
                break
        if buf:
            spool.push(buf)
        spool.queue.put_nowait(None)

    async def _wipe_delete_part(self, peer, part, sem, state):
        async with sem:
            for _ in range(6):
                try:
                    res = await self._client.delete_messages(peer, part, revoke=True)
                    affected = sum(getattr(r, "pts_count", 0) or 0 for r in (res or []))
                    state["deleted"] += affected or len(part)
                    return
                except FloodWaitError as e:
                    await asyncio.sleep(e.seconds + 1)
                except Exception as e:
                    state["error"] = str(e)
                    break
            state["failed"] += len(part)

    async def _wipe_delete(self, peer, spool, state):
        sem = asyncio.Semaphore(WIPE_PARALLEL)
        while True:
            item = await spool.queue.get()
            if item is None:
                break
            ids = spool.load(item)
            await asyncio.gather(*(
                self._wipe_delete_part(peer, ids[i:i + WIPE_BATCH].tolist(), sem, state)
                for i in range(0, len(ids), WIPE_BATCH)
            ))
            del ids
            await asyncio.sleep(WIPE_DELAY)

    async def _wipe_progress(self, call, chat_id, spool, state):
        markup = [[{"text": self.strings["btn_stop"], "callback": self._cb_wipe_stop, "args": (chat_id,), "style": "danger"}]]
        while True:
            try:
                await call.edit(self._wipe_text("wipe_progress", state, spool), reply_markup=markup)
            except asyncio.CancelledError:
                raise
            except Exception:
                pass
            await asyncio.sleep(WIPE_PROGRESS_EVERY)

    async def _wipe_run(self, call, chat_id, protected):
        state = {
            "phase": "collect",
            "collected": 0,
            "deleted": 0,
            "failed": 0,
            "error": None,
            "started": time.monotonic(),
        }
        spool = None
        ticker = None
        back = [[{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}]]
        try:
            peer = await self._client.get_input_entity(chat_id)
            _, title, cid, ctype = await self._get_chat_info(chat_id)
            state["chat_title"] = title

            uid = getattr(self, "_tg_id", None) or (self._owner.id if getattr(self, "_owner", None) else "common")
            user_dir = os.path.join(tempfile.gettempdir(), "Deleter", str(uid))
            os.makedirs(user_dir, exist_ok=True)
            spool = _IdSpool(int(self.config["WIPE_RAM_LIMIT_MB"]) * 1024 * 1024, base_dir=user_dir)
            ticker = asyncio.ensure_future(self._wipe_progress(call, chat_id, spool, state))

            await self._wipe_collect(peer, spool, protected, state)

            state["phase"] = "delete"
            await self._wipe_delete(peer, spool, state)

            ticker.cancel()
            text = self._wipe_text("wipe_done", state, spool, chat=utils.escape_html(title))
            try:
                await call.edit(text, reply_markup=back)
            except Exception:
                pass
            await self._send_log(text)
        except asyncio.CancelledError:
            try:
                await call.edit(self._wipe_text("wipe_stopped", state, spool), reply_markup=back)
            except Exception:
                pass
        except Exception as e:
            logger.error("[Deleter] wipe error: %s", e)
            try:
                await call.edit(self.strings["error"].format(error=utils.escape_html(str(e))), reply_markup=back)
            except Exception:
                pass
        finally:
            if ticker:
                ticker.cancel()
            if spool:
                spool.close()
            if self._wipe_tasks.get(chat_id) is asyncio.current_task():
                self._wipe_tasks.pop(chat_id, None)

    @loader.command(
        ru_doc="Быстрое удаление сообщений",
        en_doc="Swift message deletion",
    )
    async def delcmd(self, message: Message):
        """Swift message deletion"""
        args = utils.get_args_raw(message)
        args_list = args.split() if args else []

        if not args_list:
            await self.inline.form(
                text=self.strings["main_menu"],
                message=message,
                reply_markup=self._main_markup(),
                silent=True,
            )
            return

        cmd = args_list[0].lower()

        if cmd == "me":
            leave = "-f" in args_list
            await self._delete_me(message, leave=leave)
        elif cmd == "before":
            await self._delete_before(message)
        elif cmd == "after":
            await self._delete_after(message)
        elif cmd == "now":
            await self._delete_now(message)
        elif cmd == "today":
            await self._delete_today(message)
        elif cmd.startswith("@"):
            await self._delete_user(message, cmd)
        elif cmd.isdigit():
            await self._delete_own_n(message, int(cmd))
        else:
            try:
                entity = await message.client.get_entity(cmd)
                if isinstance(entity, User):
                    await self._delete_user_by_entity(message, entity)
                    return
            except Exception:
                pass

            await self.inline.form(
                text=self.strings["main_menu"],
                message=message,
                reply_markup=self._main_markup(),
                silent=True,
            )

    @loader.command(
        ru_doc="Включить Purger в текущем чате",
        en_doc="Enable Purger in current chat",
    )
    async def purgecmd(self, message: Message):
        """Enable Purger in current chat"""
        chat_id = message.chat_id
        async with self._purger_lock(chat_id):
            if chat_id not in self._purger_chats:
                self._purger_save(chat_id, 0, create=True)
            self._start_purger(chat_id)

            form = await self.inline.form(
                text=self.strings["purger_activated"],
                message=message,
                reply_markup=[
                    [{"text": self.strings["btn_stop"], "callback": self._cb_stop_purger, "args": (chat_id,), "style": "danger"}],
                ],
                silent=True,
            )

            form_id = (getattr(form, "form", None) or {}).get("message_id") if form else None
            if form_id:
                self._purger_protected.setdefault(chat_id, set()).add(form_id)

    async def _get_chat_name(self, message: Message) -> str:
        try:
            entity = await message.client.get_entity(message.chat_id)
            if hasattr(entity, "title") and entity.title:
                return entity.title
            if hasattr(entity, "first_name"):
                name = entity.first_name or ""
                if hasattr(entity, "last_name") and entity.last_name:
                    name = name + " " + entity.last_name
                return name.strip() or str(message.chat_id)
        except Exception:
            pass
        return str(message.chat_id)

    async def _delete_me(self, message: Message, leave: bool = False):
        chat_id = message.chat_id
        client = message.client
        cmd_msg_id = message.id

        chat_name = await self._get_chat_name(message)

        await message.delete()

        try:
            if leave:
                await self._delete_me_and_leave(client, chat_id, chat_name, cmd_msg_id)
            else:
                await self._delete_me_simple(client, chat_id, chat_name)
        except Exception as e:
            logger.error("[Deleter] del me error: %s", e)
            await self._send_log(self.strings["error"].format(error=str(e)))

    async def _delete_me_simple(self, client, chat_id, chat_name: str):
        try:
            ids = []
            async for msg in client.iter_messages(chat_id, from_user="me"):
                ids.append(msg.id)

            total_count = len(ids)

            if not ids:
                await self._send_log(
                    self.strings["done_me"].format(chat=chat_name, count=0)
                )
                return

            await self._bulk_delete(client, chat_id, ids)

            await self._send_log(
                self.strings["done_me"].format(chat=chat_name, count=total_count)
            )

        except Exception as e:
            logger.error("[Deleter] _delete_me_simple error: %s", e)
            await self._send_log(self.strings["error"].format(error=str(e)))

    async def _delete_me_and_leave(self, client, chat_id, chat_name: str, cmd_msg_id: int):
        try:
            ids_to_delete = []
            first_date = None
            last_date = None

            async for msg in client.iter_messages(chat_id, from_user="me"):
                if msg.id == cmd_msg_id:
                    continue
                ids_to_delete.append(msg.id)
                if last_date is None:
                    last_date = msg.date
                first_date = msg.date

            count = len(ids_to_delete)

            try:
                await client.send_file(chat_id, SAYONARA_URL)
            except Exception as e:
                logger.error("[Deleter] Failed to send Sayonara: %s", e)

            if ids_to_delete:
                await self._bulk_delete(client, chat_id, ids_to_delete)

            await self._leave_chat(client, chat_id)

            def fmt_date(d):
                if d is None:
                    return "-"
                return d.strftime("%Y-%m-%d %H:%M:%S UTC")

            log_text = self.strings["done_leave"].format(
                chat=chat_name,
                count=count,
                first_date=fmt_date(first_date),
                last_date=fmt_date(last_date),
            )
            await self._send_log(log_text)

        except Exception as e:
            logger.error("[Deleter] _delete_me_and_leave error: %s", e)
            await self._send_log(self.strings["error"].format(error=str(e)))

    async def _leave_chat(self, client, chat_id):
        try:
            await client(LeaveChannelRequest(chat_id))
        except Exception:
            try:
                await client.kick_participant(chat_id, "me")
            except Exception as e:
                logger.error("[Deleter] leave error: %s", e)

    async def _delete_own_n(self, message: Message, count: int):
        if count <= 0:
            return await utils.answer(message, self.strings["no_count"])

        chat_id = message.chat_id
        cmd_msg_id = message.id

        try:
            ids = [cmd_msg_id]
            kwargs = {"entity": chat_id, "from_user": "me"}
            if message.is_reply:
                kwargs["max_id"] = message.reply_to_msg_id

            async for msg in message.client.iter_messages(**kwargs):
                if msg.id != cmd_msg_id:
                    ids.append(msg.id)
                    if len(ids) - 1 >= count:
                        break

            deleted, failed = await self._bulk_delete(message.client, chat_id, ids)

            if failed:
                await message.client.send_message(chat_id, self.strings["no_perms"])
        except Exception as e:
            logger.error("[Deleter] del n error: %s", e)
            await message.client.send_message(
                chat_id, self.strings["error"].format(error=str(e))
            )

    async def _delete_before(self, message: Message):
        if not message.is_reply:
            return await utils.answer(message, self.strings["no_reply"])

        reply_id = message.reply_to_msg_id
        chat_id = message.chat_id
        cmd_msg_id = message.id

        try:
            ids = [cmd_msg_id]
            async for msg in message.client.iter_messages(chat_id, max_id=reply_id):
                ids.append(msg.id)

            deleted, failed = await self._bulk_delete(message.client, chat_id, ids)

            if failed:
                await message.client.send_message(chat_id, self.strings["no_perms"])
        except Exception as e:
            logger.error("[Deleter] del before error: %s", e)
            await message.client.send_message(
                chat_id, self.strings["error"].format(error=str(e))
            )

    async def _delete_after(self, message: Message):
        if not message.is_reply:
            return await utils.answer(message, self.strings["no_reply"])

        reply_id = message.reply_to_msg_id
        chat_id = message.chat_id
        cmd_msg_id = message.id

        try:
            ids = [cmd_msg_id]
            async for msg in message.client.iter_messages(chat_id, min_id=reply_id):
                if msg.id != cmd_msg_id:
                    ids.append(msg.id)

            deleted, failed = await self._bulk_delete(message.client, chat_id, ids)

            if failed:
                await message.client.send_message(chat_id, self.strings["no_perms"])
        except Exception as e:
            logger.error("[Deleter] del after error: %s", e)
            await message.client.send_message(
                chat_id, self.strings["error"].format(error=str(e))
            )

    async def _delete_now(self, message: Message):
        chat_id = message.chat_id
        cmd_msg_id = message.id
        cmd_time = message.date
        cutoff = cmd_time - timedelta(minutes=5)

        try:
            ids = [cmd_msg_id]
            async for msg in message.client.iter_messages(chat_id, from_user="me"):
                if msg.id == cmd_msg_id:
                    continue
                if msg.date < cutoff:
                    break
                ids.append(msg.id)

            deleted, failed = await self._bulk_delete(message.client, chat_id, ids)

            if failed:
                await message.client.send_message(chat_id, self.strings["no_perms"])
        except Exception as e:
            logger.error("[Deleter] del now error: %s", e)
            await message.client.send_message(
                chat_id, self.strings["error"].format(error=str(e))
            )

    async def _delete_today(self, message: Message):
        chat_id = message.chat_id
        cmd_msg_id = message.id

        offset = self.config["TIMEZONE_OFFSET"]
        tz = timezone(timedelta(hours=offset))
        now = datetime.now(tz)
        midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)

        try:
            ids = [cmd_msg_id]
            async for msg in message.client.iter_messages(chat_id, from_user="me"):
                if msg.id == cmd_msg_id:
                    continue
                msg_local = msg.date.astimezone(tz)
                if msg_local < midnight:
                    break
                ids.append(msg.id)

            deleted, failed = await self._bulk_delete(message.client, chat_id, ids)

            if failed:
                await message.client.send_message(chat_id, self.strings["no_perms"])
        except Exception as e:
            logger.error("[Deleter] del today error: %s", e)
            await message.client.send_message(
                chat_id, self.strings["error"].format(error=str(e))
            )

    async def _delete_user(self, message: Message, username: str):
        try:
            entity = await message.client.get_entity(username)
            if not isinstance(entity, User):
                return await utils.answer(message, self.strings["no_user"])
        except Exception:
            return await utils.answer(message, self.strings["no_user"])

        await self._delete_user_by_entity(message, entity)

    async def _delete_user_by_entity(self, message: Message, entity):
        chat_id = message.chat_id
        cmd_msg_id = message.id

        try:
            ids = [cmd_msg_id]
            async for msg in message.client.iter_messages(chat_id, from_user=entity.id):
                ids.append(msg.id)

            deleted, failed = await self._bulk_delete(message.client, chat_id, ids)

            if failed:
                await message.client.send_message(chat_id, self.strings["no_perms"])
        except Exception as e:
            logger.error("[Deleter] del user error: %s", e)
            await message.client.send_message(
                chat_id, self.strings["error"].format(error=str(e))
            )
