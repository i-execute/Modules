__version__ = (2, 3, 0)
# meta developer: I_execute.t.me
# meta banner: https://raw.githubusercontent.com/i-execute/Modules/main/Storage/RaidProtection/MetaBanner.jpeg

import asyncio
import contextlib
import logging
from datetime import datetime
from collections import Counter

from telethon.tl.functions.contacts import BlockRequest
from telethon.tl.functions.messages import DeleteHistoryRequest, ReportSpamRequest
from telethon.tl.types import (
    Message,
    PeerUser,
)
from telethon.errors import FloodWaitError
from telethon.utils import get_display_name

from .. import loader, utils
from ..inline.types import InlineCall

logger = logging.getLogger(__name__)


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
        ban_dates = self.get("ban_dates", [])
        today = datetime.now().strftime("%d.%m.%Y")
        ban_dates.append(today)
        self.set("ban_dates", ban_dates)
        total = self.get("total_bans", 0)
        self.set("total_bans", total + 1)

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
                logger.warning("[RaidProtection] heroku.forums channel_id not found in DB.")
                self._setup_failed = True
                return

            try:
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
        self._whitelist = self.get("whitelist", [])

        await self._ensure_log_topic()

        if self._storage_topic and self._asset_channel:
            try:
                await self._send_with_flood_wait(
                    self.inline.bot.send_message,
                    int(f"-100{self._asset_channel}"),
                    self.strings["reloaded"],
                    parse_mode="HTML",
                    message_thread_id=self._storage_topic.id,
                )
            except Exception as e:
                logger.warning(f"[RaidProtection] Failed to send reloaded message: {e}")

    async def _send_log(self, text):
        if not self._storage_topic or not self._asset_channel:
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
                message_thread_id=self._storage_topic.id,
            )
        except Exception as e:
            logger.error(f"[RaidProtection] Failed to send log: {e}")
            if not self._setup_failed:
                await self._ensure_log_topic()

    def _approve(self, user_id, reason="unknown"):
        if user_id not in self._whitelist:
            self._whitelist.append(user_id)
        self.set("whitelist", self._whitelist)
        self._processing.discard(user_id)
        logger.debug(f"[RaidProtection] Approved {user_id}, reason: {reason}")

    def _get_main_markup(self):
        return [
            [
                {"text": self.strings["btn_toggle"], "callback": self._cb_toggle, "style": "success" if not self.get("state", False) else "danger"},
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
        
        return self.strings["main_menu"].format(
            status=status,
            total=total
        )

    async def _cb_main_menu(self, call: InlineCall):
        await call.edit(
            self._format_main_text(),
            reply_markup=self._get_main_markup()
        )

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
            self.strings["stats_menu"].format(
                total=total,
                peak_date=peak_date,
                peak_count=peak_count
            ),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]]
        )

    async def _cb_close(self, call: InlineCall):
        await call.delete()

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

    @loader.loop(interval=0.01, autostart=True)
    async def queue_processor(self):
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
            entity = await self._client.get_entity(peer)
            if entity.bot:
                return self._approve(cid, "bot")
            if getattr(entity, "contact", False):
                return self._approve(cid, "contact")
        try:
            first_message = (
                await self._client.get_messages(peer, limit=1, reverse=True)
            )[0]
            if first_message.sender_id == self._tg_id:
                return self._approve(cid, "started_by_you")
        except Exception:
            pass
        q = 0
        async for msg in self._client.iter_messages(peer, limit=200):
            if msg.sender_id == self._tg_id:
                q += 1
            if q >= 1:
                return self._approve(cid, "you_wrote_before")
        self._ban_queue.append(message)

    @loader.loop(interval=0.05, autostart=True)
    async def ban_loop(self):
        if not self._ban_queue:
            return
        message = self._ban_queue.pop(0)
        sender_id = message.sender_id
        blocked = False
        report_status = self.strings["report_error"]
        try:
            try:
                entity = await self._client.get_entity(sender_id)
                name = self._escape(get_display_name(entity))
                username = self._get_username(entity)
            except Exception:
                name = str(sender_id)
                username = None

            try:
                await self._client.send_message(sender_id, self.strings["raid_message"])
            except Exception as e:
                logger.error(f"[RaidProtection] Failed to send raid message to {sender_id}: {e}")

            await asyncio.sleep(0.2)

            try:
                await self._client(ReportSpamRequest(peer=sender_id))
                report_status = self.strings["report_ok"]
            except Exception as e:
                report_status = self.strings["report_error"]
                logger.error(f"[RaidProtection] Failed to report spam {sender_id}: {e}")

            try:
                await self._client(DeleteHistoryRequest(
                    peer=sender_id,
                    max_id=0,
                    just_clear=True,
                    revoke=False,
                ))
            except Exception as e:
                logger.error(f"[RaidProtection] Failed to delete history with {sender_id}: {e}")

            try:
                await self._client(BlockRequest(id=sender_id))
                blocked = True
            except Exception as e:
                logger.error(f"[RaidProtection] Failed to block {sender_id}: {e}")

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

            if blocked:
                self._record_ban()
                logger.warning(f"[RaidProtection] Raider punished: {sender_id}")
                self._approve(sender_id, "banned")
            else:
                logger.warning(f"[RaidProtection] Raider partially handled: {sender_id}")
        finally:
            self._processing.discard(sender_id)