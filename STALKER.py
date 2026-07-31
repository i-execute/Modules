__version__ = (1, 0, 1)
# meta developer: I_execute.t.me

import asyncio
import logging
import time

from herokutl.tl.types import (
    PeerUser,
    PeerChat,
    PeerChannel,
    UpdateUserTyping,
    UpdateChatUserTyping,
    UpdateChannelUserTyping,
    SendMessageCancelAction,
    User,
    Channel,
)

from .. import loader, utils

logger = logging.getLogger(__name__)

CANCEL_TIMEOUT = 20
CANCEL_GRACE = 5
CHECK_INTERVAL = 2


@loader.tds
class STALKER(loader.Module):
    """Detects users who started typing but never sent anything"""

    strings = {
        "name": "STALKER",
        "log_entry": (
            "<blockquote><b>Stopped typing</b>\n"
            "<b>From:</b> {from_name}\n"
            "{from_uname}"
            "<b>Chat:</b> {chat_name}\n"
            "{chat_uname}"
            "<b>Typed for {duration}s, then stayed silent for {timeout} sec. The message was never sent.</blockquote>"
        ),
    }

    strings_ru = {
        "log_entry": (
            "<blockquote><b>Передумал писать</b>\n"
            "<b>От:</b> {from_name}\n"
            "{from_uname}"
            "<b>Чат:</b> {chat_name}\n"
            "{chat_uname}"
            "Печатал(а) {duration}с, затем молчание {timeout}сек. Сообщение так и не пришло</blockquote>"
        ),
    }

    def __init__(self):
        self._owner = None
        self._logger_topic = None
        self._asset_channel = None
        self._typing_state = {}
        self._checker_task = None

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

    def _format_username_row(self, entity):
        username = self._get_username(entity)
        if not username:
            return ""
        return f"@{username}\n"

    def _get_display_name(self, entity):
        if isinstance(entity, Channel):
            return self._escape(getattr(entity, "title", None) or "Channel")
        first = getattr(entity, "first_name", "") or ""
        last = getattr(entity, "last_name", "") or ""
        return self._escape(f"{first} {last}".strip() or "User")

    def _peer_key(self, peer):
        if isinstance(peer, PeerUser):
            return ("user", peer.user_id)
        if isinstance(peer, PeerChat):
            return ("chat", peer.chat_id)
        if isinstance(peer, PeerChannel):
            return ("channel", peer.channel_id)
        return None

    async def _send_with_flood_wait(self, coro_func, *args, **kwargs):
        max_retries = 5
        for attempt in range(max_retries):
            try:
                return await coro_func(*args, **kwargs)
            except Exception as e:
                error_str = str(e).lower()
                if "flood" in error_str:
                    await asyncio.sleep(5)
                    continue
                raise
        return None

    async def _send_log(self, text: str):
        if not self._logger_topic or not self._asset_channel:
            return
        try:
            await self._send_with_flood_wait(
                self.inline.bot.send_message,
                int(f"-100{self._asset_channel}"),
                text,
                disable_web_page_preview=True,
                parse_mode="HTML",
                message_thread_id=self._logger_topic.id,
            )
        except Exception as e:
            logger.error(f"[STALKER] Failed to send log: {e}")

    async def client_ready(self):
        self._owner = await self._client.get_me()
        self._asset_channel = self._db.get("heroku.forums", "channel_id", None)

        if not self._asset_channel:
            logger.warning("[STALKER] heroku.forums channel_id not found in DB, logging will be disabled.")
            return

        try:
            self._logger_topic = await utils.asset_forum_topic(
                self._client,
                self._db,
                self._asset_channel,
                "STALKER",
                description="Users who started typing but changed their mind.",
                icon_emoji_id=5303057349425013341,
            )
        except Exception as e:
            logger.error(f"[STALKER] Failed to create/get forum topic: {e}")
            return

        self._checker_task = asyncio.ensure_future(self._checker_loop())

    async def on_unload(self):
        if self._checker_task:
            self._checker_task.cancel()

    @loader.raw_handler(UpdateUserTyping, UpdateChatUserTyping, UpdateChannelUserTyping)
    async def typing_handler(self, update):
        try:
            if not self._logger_topic or not self._asset_channel:
                return

            if isinstance(update, UpdateUserTyping):
                chat_key = ("user", update.user_id)
                user_id = update.user_id
            elif isinstance(update, UpdateChatUserTyping):
                key = self._peer_key(update.from_id) if hasattr(update, "from_id") else None
                if key is None or key[0] != "user":
                    return
                user_id = key[1]
                chat_key = ("chat", update.chat_id)
            elif isinstance(update, UpdateChannelUserTyping):
                key = self._peer_key(update.from_id) if hasattr(update, "from_id") else None
                if key is None or key[0] != "user":
                    return
                user_id = key[1]
                chat_key = ("channel", update.channel_id)
            else:
                return

            if user_id == self._owner.id:
                return

            now = time.time()
            state_key = (chat_key, user_id)
            is_cancel = isinstance(getattr(update, "action", None), SendMessageCancelAction)

            if state_key not in self._typing_state:
                if is_cancel:
                    return
                self._typing_state[state_key] = {
                    "first_ts": now,
                    "last_ts": now,
                    "cancelled_ts": None,
                }
            else:
                state = self._typing_state[state_key]
                state["last_ts"] = now
                state["cancelled_ts"] = now if is_cancel else None
        except Exception as e:
            logger.error(f"[STALKER] typing_handler error: {e}")

    @loader.watcher()
    async def watcher(self, message):
        try:
            if not self._logger_topic or not self._asset_channel:
                return

            sender_id = getattr(message, "sender_id", None)
            if sender_id is None:
                return

            for state_key in list(self._typing_state.keys()):
                chat_key, user_id = state_key
                if user_id == sender_id:
                    if chat_key[0] == "user" and isinstance(message.peer_id, PeerUser):
                        del self._typing_state[state_key]
                    elif chat_key[0] == "chat" and isinstance(message.peer_id, PeerChat) and chat_key[1] == message.peer_id.chat_id:
                        del self._typing_state[state_key]
                    elif chat_key[0] == "channel" and isinstance(message.peer_id, PeerChannel) and chat_key[1] == message.peer_id.channel_id:
                        del self._typing_state[state_key]
        except Exception as e:
            logger.error(f"[STALKER] watcher error: {e}")

    async def _checker_loop(self):
        while True:
            try:
                await asyncio.sleep(CHECK_INTERVAL)
                now = time.time()

                for state_key, ts in list(self._typing_state.items()):
                    timed_out = now - ts["last_ts"] >= CANCEL_TIMEOUT
                    explicitly_cancelled = (
                        ts.get("cancelled_ts") is not None
                        and now - ts["cancelled_ts"] >= CANCEL_GRACE
                    )
                    if timed_out or explicitly_cancelled:
                        chat_key, user_id = state_key
                        del self._typing_state[state_key]
                        asyncio.ensure_future(self._trigger(chat_key, user_id, ts))
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[STALKER] checker_loop error: {e}")

    async def _trigger(self, chat_key, user_id, ts):
        try:
            sender = await self._client.get_entity(PeerUser(user_id))

            kind, chat_id = chat_key
            if kind == "user":
                chat = sender
            elif kind == "chat":
                chat = await self._client.get_entity(PeerChat(chat_id))
            else:
                chat = await self._client.get_entity(PeerChannel(chat_id))

            duration = max(int(ts["last_ts"] - ts["first_ts"]), 0)

            log_text = self.strings["log_entry"].format(
                from_name=self._get_display_name(sender),
                from_uname=self._format_username_row(sender),
                chat_name=self._get_display_name(chat),
                chat_uname=self._format_username_row(chat) if kind != "user" else "",
                duration=duration,
                timeout=CANCEL_TIMEOUT,
            )

            await self._send_log(log_text)
        except Exception as e:
            logger.error(f"[STALKER] trigger error: {e}")