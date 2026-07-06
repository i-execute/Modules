__version__ = (3, 2, 5)
# meta developer: I_execute.t.me
# meta banner: https://raw.githubusercontent.com/i-execute/Modules/main/Storage/Logger/MetaBanner.jpeg

import logging
import asyncio
import re
from telethon.tl.types import PeerUser, Channel, User
from telethon.tl.functions.messages import EditMessageRequest
from telethon.tl.types import InputMediaWebPage
from telethon.errors import FloodWaitError
from .. import loader, utils

logger = logging.getLogger(__name__)

GREETING_MEDIA_URL = "https://raw.githubusercontent.com/i-execute/Modules/main/Storage/Logger/Greetings.jpeg"


@loader.tds
class Logger(loader.Module):
    """Command logger for userbot"""

    strings = {
        "name": "Logger",
        "greeting_first": (
            "<blockquote><b>Yo!</b>\n"
            "Watchers are active. Now every command will be logged here, if some asshole uses your userbot I'll tag you</blockquote>"
        ),
        "reloaded": "<blockquote><b>Module successfully reloaded, everything works</b></blockquote>",
        "username_row": "@{uname}",
        "owner_attention": "<blockquote><b><a href='tg://user?id={owner_id}'>{owner_name}</a>, attention please</b></blockquote>\n",
        "log_dm_user": (
            "<blockquote><b>DIRECT MESSAGE</b>\n"
            "<b>Command:</b> <code>{cmd}</code>\n"
            "<b>From:</b> {from_name}\n"
            "{from_uname}"
            "<b>Chat:</b> {to_name}\n"
            "{to_uname}</blockquote>"
        ),
        "log_dm_bot": (
            "<blockquote><b>DIRECT MESSAGE (BOT)</b>\n"
            "<b>Command:</b> <code>{cmd}</code>\n"
            "<b>From:</b> {from_name}\n"
            "{from_uname}"
            "<b>Bot:</b> {to_name}\n"
            "{to_uname}</blockquote>"
        ),
        "log_group": (
            "<blockquote><b>GROUP</b>\n"
            "<b>Command:</b> <code>{cmd}</code>\n"
            "<b>From:</b> {from_name}\n"
            "{from_uname}"
            "<b>Group:</b> {chat_name} [<code>{chat_id}</code>]{chat_uname}\n"
            "<a href='{msg_link}'>Open message</a></blockquote>"
        ),
        "log_channel": (
            "<blockquote><b>CHANNEL</b>\n"
            "<b>Command:</b> <code>{cmd}</code>\n"
            "<b>Channel:</b> {chat_name} [<code>{chat_id}</code>]{chat_uname}\n"
            "<a href='{msg_link}'>Open message</a></blockquote>"
        ),
    }

    strings_ru = {
        "greeting_first": (
            "<blockquote><b>Ку!</b>\n"
            "Вотчеры активны. Теперь каждая команда будет залетать сюда, если какой-то хуй будет использовать твой юзербот то я тэгну тебя</blockquote>"
        ),
        "reloaded": "<blockquote><b>Модуль был успешно перезагружен, все воркает</b></blockquote>",
        "username_row": "@{uname}",
        "owner_attention": "<blockquote><b><a href='tg://user?id={owner_id}'>{owner_name}</a>, минуточку внимания</b></blockquote>\n",
        "log_dm_user": (
            "<blockquote><b>DIRECT MESSAGE</b>\n"
            "<b>Команда:</b> <code>{cmd}</code>\n"
            "<b>От:</b> {from_name}\n"
            "{from_uname}"
            "<b>Чат:</b> {to_name}\n"
            "{to_uname}</blockquote>"
        ),
        "log_dm_bot": (
            "<blockquote><b>DIRECT MESSAGE (BOT)</b>\n"
            "<b>Команда:</b> <code>{cmd}</code>\n"
            "<b>От:</b> {from_name}\n"
            "{from_uname}"
            "<b>Бот:</b> {to_name}\n"
            "{to_uname}</blockquote>"
        ),
        "log_group": (
            "<blockquote><b>GROUP</b>\n"
            "<b>Команда:</b> <code>{cmd}</code>\n"
            "<b>От:</b> {from_name}\n"
            "{from_uname}"
            "<b>Группа:</b> {chat_name} [<code>{chat_id}</code>]{chat_uname}\n"
            "<a href='{msg_link}'>Открыть сообщение</a></blockquote>"
        ),
        "log_channel": (
            "<blockquote><b>CHANNEL</b>\n"
            "<b>Команда:</b> <code>{cmd}</code>\n"
            "<b>Канал:</b> {chat_name} [<code>{chat_id}</code>]{chat_uname}\n"
            "<a href='{msg_link}'>Открыть сообщение</a></blockquote>"
        ),
    }

    def __init__(self):
        self._owner = None
        self._logger_topic = None
        self._asset_channel = None
        self._flood_lock = asyncio.Lock()
        self._owner_usernames = set()

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

    def _get_all_usernames(self, entity):
        usernames = []
        if hasattr(entity, "username") and entity.username:
            usernames.append(entity.username)
        if hasattr(entity, "usernames") and entity.usernames:
            for uname_obj in entity.usernames:
                if getattr(uname_obj, "active", False) and uname_obj.username:
                    usernames.append(uname_obj.username)
        return usernames

    def _get_display_name(self, entity):
        if isinstance(entity, Channel):
            return self._escape(getattr(entity, "title", None) or "Channel")
        first = getattr(entity, "first_name", "") or ""
        last = getattr(entity, "last_name", "") or ""
        return self._escape(f"{first} {last}".strip() or "User")

    def _get_full_name(self, entity) -> str:
        if not entity:
            return "Unknown"
        first = getattr(entity, "first_name", "") or ""
        last = getattr(entity, "last_name", "") or ""
        return f"{first} {last}".strip() or "Unknown"

    def _format_username_row(self, entity, is_owner=False):
        username = self._get_username(entity)
        if not username:
            return ""
        if is_owner:
            return f"<code>@{username}</code>\n"
        return self.strings["username_row"].format(uname=username) + "\n"

    def _get_topic_id(self, message):
        reply_to = getattr(message, "reply_to", None)
        if reply_to:
            if getattr(reply_to, "forum_topic", False):
                return getattr(reply_to, "reply_to_top_id", None) or getattr(
                    reply_to, "reply_to_msg_id", None
                )
        return None

    def _build_message_link(self, chat, message):
        username = self._get_username(chat)
        chat_id = chat.id
        msg_id = message.id
        topic_id = self._get_topic_id(message)
        if username:
            if topic_id:
                return f"https://t.me/{username}/{topic_id}/{msg_id}"
            return f"https://t.me/{username}/{msg_id}"
        else:
            if topic_id:
                return f"https://t.me/c/{chat_id}/{topic_id}/{msg_id}"
            return f"https://t.me/c/{chat_id}/{msg_id}"

    def _is_channel_post(self, message, sender):
        if message.post:
            return True
        if isinstance(sender, Channel) and not getattr(sender, "megagroup", False):
            return True
        return False

    async def _send_with_flood_wait(self, coro_func, *args, **kwargs):
        max_retries = 5
        for attempt in range(max_retries):
            try:
                return await coro_func(*args, **kwargs)
            except FloodWaitError as e:
                wait_time = e.seconds + 1
                logger.warning(f"[Logger] FloodWait {wait_time}s, retrying...")
                await asyncio.sleep(wait_time)
            except Exception as e:
                error_str = str(e).lower()
                if "flood" in error_str and "retry after" in error_str:
                    match = re.search(r"retry after (\d+)", error_str)
                    if match:
                        wait_time = int(match.group(1)) + 1
                        await asyncio.sleep(wait_time)
                        continue
                raise
        return None

    async def client_ready(self):
        self._owner = await self._client.get_me()
        self._owner_usernames = set(self._get_all_usernames(self._owner))

        self._asset_channel = self._db.get("heroku.forums", "channel_id", None)

        if not self._asset_channel:
            logger.warning("[Logger] heroku.forums channel_id not found in DB, logging will be disabled.")
            return

        try:
            self._logger_topic = await utils.asset_forum_topic(
                self._client,
                self._db,
                self._asset_channel,
                "Logger",
                description="All userbot commands will be logged here.",
                icon_emoji_id=5188466187448650036,
            )
        except Exception as e:
            logger.error(f"[Logger] Failed to create/get forum topic: {e}")
            return

        greeting_key = f"logger_greeted_{self._asset_channel}_{self._logger_topic.id}"
        already_greeted = self.get(greeting_key, False)
        chat_id = int(f"-100{self._asset_channel}")

        if already_greeted:
            try:
                await self._send_with_flood_wait(
                    self.inline.bot.send_message,
                    chat_id,
                    self.strings["reloaded"],
                    parse_mode="html",
                    message_thread_id=self._logger_topic.id,
                )
            except Exception as e:
                logger.error(f"[Logger] Failed to send reloaded message: {e}")
        else:
            self.set(greeting_key, True)
            try:
                msg_text, entities = await self.inline.bot._parse_message_text(
                    self.strings["greeting_first"], "html"
                )
                msg = await self._send_with_flood_wait(
                    self.inline.bot.send_message,
                    chat_id,
                    msg_text,
                    parse_mode=None,
                    entities=entities,
                    message_thread_id=self._logger_topic.id,
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
                                url=GREETING_MEDIA_URL,
                                optional=True,
                                force_large_media=True,
                            ),
                            invert_media=True,
                            reply_markup=reply_markup,
                            entities=entities,
                            no_webpage=False,
                        ))
                    except Exception as e:
                        logger.error(f"[Logger] Failed to add preview: {e}")
            except Exception as e:
                logger.error(f"[Logger] Failed to send greeting: {e}")

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
            logger.error(f"[Logger] Failed to send log: {e}")

    @loader.watcher(only_commands=True)
    async def watcher(self, message):
        try:
            if not self._logger_topic or not self._asset_channel:
                return

            if message.chat_id == self._asset_channel:
                return

            is_dm = isinstance(message.peer_id, PeerUser)
            sender = await message.get_sender()
            if not sender:
                return

            chat = await self._client.get_entity(message.peer_id)

            is_channel = self._is_channel_post(message, sender)
            is_owner = (not is_channel) and (sender.id == self._owner.id)

            sender_name = self._get_display_name(sender)
            sender_uname = self._format_username_row(sender, is_owner=is_owner)

            chat_name = self._get_display_name(chat)
            chat_username = self._get_username(chat)
            chat_uname = f" @{chat_username}" if chat_username else ""

            cmd_text = self._escape(message.raw_text)

            owner_prefix = ""
            if not is_owner and not is_channel:
                owner_name = self._get_full_name(self._owner)
                owner_prefix = self.strings["owner_attention"].format(
                    owner_id=self._owner.id,
                    owner_name=self._escape(owner_name),
                )

            if is_dm:
                is_bot_chat = isinstance(chat, User) and getattr(chat, "bot", False)
                template = (
                    self.strings["log_dm_bot"]
                    if is_bot_chat
                    else self.strings["log_dm_user"]
                )
                log_text = template.format(
                    cmd=cmd_text,
                    from_name=sender_name,
                    from_uname=sender_uname,
                    to_name=chat_name,
                    to_uname=self._format_username_row(chat),
                )
            elif is_channel:
                msg_link = self._build_message_link(chat, message)
                log_text = self.strings["log_channel"].format(
                    cmd=cmd_text,
                    chat_name=chat_name,
                    chat_id=chat.id,
                    chat_uname=chat_uname,
                    msg_link=msg_link,
                )
            else:
                msg_link = self._build_message_link(chat, message)
                log_text = self.strings["log_group"].format(
                    cmd=cmd_text,
                    from_name=sender_name,
                    from_uname=sender_uname,
                    chat_name=chat_name,
                    chat_id=chat.id,
                    chat_uname=chat_uname,
                    msg_link=msg_link,
                )

            log_text = owner_prefix + log_text
            await self._send_log(log_text)

        except Exception as e:
            logger.error(f"[Logger] Watcher error: {e}")