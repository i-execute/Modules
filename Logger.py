__version__ = (3, 0, 0)
# meta developer: I_execute.t.me

import logging
import asyncio
import re
from telethon.tl.types import PeerUser, Channel, User
from telethon.errors import FloodWaitError
from .. import loader, utils

logger = logging.getLogger(__name__)

GREETING_MEDIA_URL = "https://github.com/FireJester/Media/raw/main/Greeting_in_Logger.jpeg"


@loader.tds
class Logger(loader.Module):
    """Command logger for userbot"""

    strings = {
        "name": "Logger",
        "greeting_first": (
            "<b>Yo!</b>\n\n"
            "Watchers are active. "
            "Now every command will be logged here, if some asshole uses your userbot I'll tag you"
        ),
        "greeting_recovery": (
            "<b>Hey!</b>\n\n"
            "Everything went to shit\nOld group disappeared somewhere, who the fuck knows what happened"
        ),
        "error_info": (
            "<b>What happened:</b>\n"
            "Bot fucked up trying to send message to old group.\n"
            "Reason for the shitshow:\n<code>{error_text}</code>\n\n"
        ),
        "reloaded": "<b>Module successfully reloaded, everything works</b>",
        "username_row": "┗ <code>@{uname}</code>\n",
        "owner_attention": "<b>{owner_link},</b> attention please\n\n",
        "log_dm_user": (
            "<b>DIRECT MESSAGE</b>\n\n"
            "<b>Command:</b>\n<code>{cmd}</code>\n\n"
            "<b>From:</b> {from_name}\n"
            "{from_uname}"
            "<b>Chat:</b> {to_name}\n"
            "{to_uname}"
        ),
        "log_dm_bot": (
            "<b>DIRECT MESSAGE (BOT)</b>\n\n"
            "<b>Command:</b>\n<code>{cmd}</code>\n\n"
            "<b>From:</b> {from_name}\n"
            "{from_uname}"
            "<b>Bot:</b> {to_name}\n"
            "{to_uname}"
        ),
        "log_group": (
            "<b>GROUP</b>\n\n"
            "<b>Command:</b>\n<code>{cmd}</code>\n\n"
            "<b>From:</b> {from_name}\n"
            "{from_uname}"
            "<b>Group:</b> {chat_name} [<code>{chat_id}</code>]\n"
            "{chat_uname}"
            "<a href='{msg_link}'>Open message</a>"
        ),
        "log_channel": (
            "<b>CHANNEL</b>\n\n"
            "<b>Command:</b>\n<code>{cmd}</code>\n\n"
            "<b>Channel:</b> {chat_name} [<code>{chat_id}</code>]\n"
            "{chat_uname}"
            "<a href='{msg_link}'>Open message</a>"
        ),
        "help": (
            "<b>Logger Commands</b>\n\n"
            "<code>.logger</code> - show this help\n"
            "<code>.logger status</code> - current logger status\n"
        ),
        "status": (
            "<b>Logger Status</b>\n\n"
            "<b>Log topic:</b> {topic_name}\n"
            "<b>Channel ID:</b> <code>{channel_id}</code>\n"
            "<b>Topic ID:</b> <code>{topic_id}</code>\n"
            "<b>Status:</b> {status}\n"
        ),
        "status_no_topic": (
            "<b>Logger Status</b>\n\n"
            "<b>Log topic:</b> Not configured\n"
            "<b>Status:</b> Inactive\n\n"
            "Make sure heroku.forums channel_id is set in DB"
        ),
    }

    strings_ru = {
        "greeting_first": (
            "<b>Ку!</b>\n\n"
            "Вотчеры активны. "
            "Теперь каждая команда будет залетать сюда, если какой-то хуй будет использовать твой юзербот то я тэгну тебя"
        ),
        "greeting_recovery": (
            "<b>Прием!</b>\n\n"
            "Всё наебнулось к хуям\nСтарая группа куда-то съебалась, хер знает че такое"
        ),
        "error_info": (
            "<b>Чё произошло:</b>\n"
            "Бот обосрался при попытке отправить сообщение в старую группу.\n"
            "Причина тряски:\n<code>{error_text}</code>\n\n"
        ),
        "reloaded": "<b>Модуль был успешно перезагружен, все воркает</b>",
        "username_row": "┗ <code>@{uname}</code>\n",
        "owner_attention": "<b>{owner_link},</b> минуточку внимания\n\n",
        "log_dm_user": (
            "<b>DIRECT MESSAGE</b>\n\n"
            "<b>Команда:</b>\n<code>{cmd}</code>\n\n"
            "<b>От:</b> {from_name}\n"
            "{from_uname}"
            "<b>Чат:</b> {to_name}\n"
            "{to_uname}"
        ),
        "log_dm_bot": (
            "<b>DIRECT MESSAGE (BOT)</b>\n\n"
            "<b>Команда:</b>\n<code>{cmd}</code>\n\n"
            "<b>От:</b> {from_name}\n"
            "{from_uname}"
            "<b>Бот:</b> {to_name}\n"
            "{to_uname}"
        ),
        "log_group": (
            "<b>GROUP</b>\n\n"
            "<b>Команда:</b>\n<code>{cmd}</code>\n\n"
            "<b>От:</b> {from_name}\n"
            "{from_uname}"
            "<b>Группа:</b> {chat_name} [<code>{chat_id}</code>]\n"
            "{chat_uname}"
            "<a href='{msg_link}'>Открыть сообщение</a>"
        ),
        "log_channel": (
            "<b>CHANNEL</b>\n\n"
            "<b>Команда:</b>\n<code>{cmd}</code>\n\n"
            "<b>Канал:</b> {chat_name} [<code>{chat_id}</code>]\n"
            "{chat_uname}"
            "<a href='{msg_link}'>Открыть сообщение</a>"
        ),
        "help": (
            "<b>Команды Logger</b>\n\n"
            "<code>.logger</code> - показать эту справку\n"
            "<code>.logger status</code> - текущий статус логгера\n"
        ),
        "status": (
            "<b>Статус Logger</b>\n\n"
            "<b>Топик:</b> {topic_name}\n"
            "<b>ID канала:</b> <code>{channel_id}</code>\n"
            "<b>ID топика:</b> <code>{topic_id}</code>\n"
            "<b>Статус:</b> {status}\n"
        ),
        "status_no_topic": (
            "<b>Статус Logger</b>\n\n"
            "<b>Топик:</b> Не настроен\n"
            "<b>Статус:</b> Неактивен\n\n"
            "Убедись что heroku.forums channel_id установлен в БД"
        ),
    }

    def __init__(self):
        self._owner = None
        self._logger_topic = None
        self._asset_channel = None
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

    def _get_display_name(self, entity):
        if isinstance(entity, Channel):
            return self._escape(getattr(entity, "title", None) or "Channel")
        first = getattr(entity, "first_name", "") or ""
        last = getattr(entity, "last_name", "") or ""
        return self._escape(f"{first} {last}".strip() or "User")

    def _get_user_link(self, user_id: int, name: str) -> str:
        return f'<a href="tg://user?id={user_id}">{self._escape(name)}</a>'

    def _get_full_name(self, entity) -> str:
        if not entity:
            return "Unknown"
        first = getattr(entity, "first_name", "") or ""
        last = getattr(entity, "last_name", "") or ""
        return f"{first} {last}".strip() or "Unknown"

    def _format_username_row(self, entity):
        username = self._get_username(entity)
        return self.strings["username_row"].format(uname=username) if username else ""

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

        try:
            await self._send_with_flood_wait(
                self.inline.bot.send_photo,
                int(f"-100{self._asset_channel}"),
                photo=GREETING_MEDIA_URL,
                caption=self.strings["greeting_first"],
                parse_mode="HTML",
                message_thread_id=self._logger_topic.id,
            )
        except Exception:
            try:
                await self._send_with_flood_wait(
                    self.inline.bot.send_message,
                    int(f"-100{self._asset_channel}"),
                    self.strings["reloaded"],
                    parse_mode="HTML",
                    message_thread_id=self._logger_topic.id,
                )
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

    @loader.command(
        ru_doc="Показать справку по командам",
        en_doc="Show help for commands",
    )
    async def logger(self, message):
        """Show help for commands"""
        args = utils.get_args_raw(message).strip().lower()

        if not args:
            await utils.answer(message, self.strings["help"])
            return

        parts = args.split()
        cmd = parts[0]

        if cmd == "status":
            await self._cmd_status(message)
        else:
            await utils.answer(message, self.strings["help"])

    async def _cmd_status(self, message):
        if not self._logger_topic or not self._asset_channel:
            await utils.answer(message, self.strings["status_no_topic"])
            return

        await utils.answer(
            message,
            self.strings["status"].format(
                topic_name="Logger",
                channel_id=self._asset_channel,
                topic_id=self._logger_topic.id,
                status="Active",
            ),
        )

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

            sender_name = self._get_display_name(sender)
            sender_uname = self._format_username_row(sender)

            chat_name = self._get_display_name(chat)
            chat_uname = self._format_username_row(chat)

            cmd_text = self._escape(message.raw_text)

            is_channel = self._is_channel_post(message, sender)

            owner_prefix = ""
            if sender.id != self._owner.id and not is_channel:
                owner_name = self._get_full_name(self._owner)
                owner_link = self._get_user_link(self._owner.id, owner_name)
                owner_prefix = self.strings["owner_attention"].format(
                    owner_link=owner_link
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
                    to_uname=chat_uname,
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