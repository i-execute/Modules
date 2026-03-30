__version__ = (2, 0, 1)
# meta developer: FireJester.t.me

import asyncio
import contextlib
import logging

from telethon.tl.functions.contacts import BlockRequest
from telethon.tl.functions.messages import DeleteHistoryRequest, ReportSpamRequest
from telethon.tl.functions.channels import InviteToChannelRequest, EditAdminRequest
from telethon.tl.types import (
    Message,
    PeerUser,
    User,
    Channel,
    ChatAdminRights,
)
from telethon.errors import FloodWaitError, ChatAdminRequiredError
from telethon.utils import get_display_name

from .. import loader, utils

logger = logging.getLogger(__name__)

LOG_GROUP_AVATAR_URL = "https://github.com/FireJester/Media/raw/main/Group_avatar_in_RaidProtection.jpeg"


@loader.tds
class RaidProtection(loader.Module):
    """Raid protection for private messages - blocks and clears new unknown chats"""

    strings = {
        "name": "RaidProtection",
    }

    strings_en = {
        "help": (
            "<b>RaidProtection - PM raid shield</b>\n\n"
            "<code>{prefix}rp</code> - toggle raid protection on/off\n\n"
            "<b>How it works:</b>\n"
            "When enabled, if a new unknown user writes to you in DM "
            "(a chat that didn't exist before), the module will:\n"
            "1. Send them a message\n"
            "2. Report spam\n"
            "3. Delete the chat on your side\n"
            "4. Block the user\n"
            "5. Log the event to the log group\n\n"
            "<b>Ignores:</b> contacts, bots, chats you started yourself"
        ),
        "enabled": "<b>Raid protection enabled</b>",
        "disabled": "<b>Raid protection disabled</b>",
        "banned_log": (
            "<b>Raid protection triggered</b>\n\n"
            "<b>User:</b> <a href='tg://user?id={user_id}'>{name}</a>\n"
            "<b>ID:</b> <code>{user_id}</code>\n"
            "{username_line}"
            "<b>Message:</b>\n<code>{text}</code>"
        ),
        "raid_message": "Spam ban btw",
        "reloaded": "<b>RaidProtection module reloaded, active</b>",
        "inline_create_failed": (
            "<b>Failed to create/setup log group</b>\n\n"
            "The module will still work but without logging."
        ),
    }

    strings_ru = {
        "help": (
            "<b>RaidProtection - защита ЛС от рейдов</b>\n\n"
            "<code>{prefix}rp</code> - включить/выключить защиту от рейдов\n\n"
            "<b>Как работает:</b>\n"
            "При включении, если новый неизвестный пользователь пишет вам в ЛС "
            "(чат, которого раньше не существовало), модуль:\n"
            "1. Отправит ему сообщение\n"
            "2. Зарепортит спам\n"
            "3. Удалит чат только у вас\n"
            "4. Заблокирует пользователя\n"
            "5. Запишет событие в группу логов\n\n"
            "<b>Игнорирует:</b> контакты, ботов, чаты которые вы начали сами"
        ),
        "enabled": "<b>Защита от рейдов включена</b>",
        "disabled": "<b>Защита от рейдов выключена</b>",
        "banned_log": (
            "<b>Сработала защита от рейдов</b>\n\n"
            "<b>Пользователь:</b> <a href='tg://user?id={user_id}'>{name}</a>\n"
            "<b>ID:</b> <code>{user_id}</code>\n"
            "{username_line}"
            "<b>Сообщение:</b>\n<code>{text}</code>"
        ),
        "raid_message": "Spam ban btw",
        "reloaded": "<b>Модуль RaidProtection перезагружен, активен</b>",
        "inline_create_failed": (
            "<b>Не удалось создать/настроить группу логов</b>\n\n"
            "Модуль продолжит работать, но без логирования."
        ),
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "LOG_CHAT_ID",
                0,
                lambda: "ID of the log group",
                validator=loader.validators.Integer(),
            ),
        )
        self._owner = None
        self.chat_logs = None
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

    async def _set_bot_admin(self, chat_entity):
        try:
            bot_user = await self._client.get_entity(self.inline.bot_username)
            admin_rights = ChatAdminRights(
                post_messages=True,
                edit_messages=True,
                delete_messages=True,
                ban_users=False,
                invite_users=False,
                pin_messages=True,
                add_admins=False,
                anonymous=False,
                manage_call=False,
                other=False,
            )
            await self._client(EditAdminRequest(
                channel=chat_entity,
                user_id=bot_user,
                admin_rights=admin_rights,
                rank="Raid Guard",
            ))
            return True
        except ChatAdminRequiredError:
            return False
        except Exception as e:
            logger.error(f"[RaidProtection] Failed to set bot admin: {e}")
            return False

    async def _try_setup_group(self, chat_entity):
        try:
            try:
                bot_user = await self._client.get_entity(self.inline.bot_username)
                await self._send_with_flood_wait(
                    self._client,
                    InviteToChannelRequest(chat_entity, [bot_user]),
                )
            except Exception as e:
                logger.warning(f"[RaidProtection] Bot invite failed (maybe already in): {e}")
            admin_set = await self._set_bot_admin(chat_entity)
            if not admin_set:
                return False, "Failed to set bot admin rights"
            await asyncio.sleep(2)
            return True, None
        except Exception as e:
            return False, str(e)

    async def _ensure_log_chat(self):
        async with self._flood_lock:
            try:
                chat_entity, _ = await utils.asset_channel(
                    self._client,
                    "Raid logs",
                    "Raid protection logs. @FireJester with <3",
                    silent=True,
                    avatar=LOG_GROUP_AVATAR_URL,
                )
                self.config["LOG_CHAT_ID"] = chat_entity.id
                self.chat_logs = int(f"-100{chat_entity.id}")
                success, setup_error = await self._try_setup_group(chat_entity)
                if not success:
                    raise Exception(setup_error)
                self._setup_failed = False
            except Exception as e:
                logger.error(f"[RaidProtection] Failed to create/setup log group: {e}")
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
        saved_id = self.config["LOG_CHAT_ID"]
        if saved_id != 0:
            try:
                self.chat_logs = int(f"-100{saved_id}")
                chat_entity = await client.get_entity(self.chat_logs)
                try:
                    await self._send_with_flood_wait(
                        self.inline.bot.send_message,
                        self.chat_logs,
                        self.strings["reloaded"],
                        parse_mode="HTML",
                    )
                    self._setup_failed = False
                except Exception:
                    try:
                        bot_user = await self._client.get_entity(self.inline.bot_username)
                        await self._send_with_flood_wait(
                            self._client,
                            InviteToChannelRequest(chat_entity, [bot_user]),
                        )
                        await self._set_bot_admin(chat_entity)
                        await self._send_with_flood_wait(
                            self.inline.bot.send_message,
                            self.chat_logs,
                            self.strings["reloaded"],
                            parse_mode="HTML",
                        )
                        self._setup_failed = False
                    except Exception:
                        await self._ensure_log_chat()
            except Exception:
                await self._ensure_log_chat()
        else:
            await self._ensure_log_chat()

    async def _send_log(self, text):
        if not self.chat_logs:
            if not self._setup_failed:
                await self._ensure_log_chat()
            return
        try:
            await self._send_with_flood_wait(
                self.inline.bot.send_message,
                self.chat_logs,
                text,
                disable_web_page_preview=True,
                parse_mode="HTML",
            )
        except Exception as e:
            error_str = str(e).lower()
            if "flood" in error_str:
                return
            try:
                chat_entity = await self._client.get_entity(self.config["LOG_CHAT_ID"])
                bot_user = await self._client.get_entity(self.inline.bot_username)
                await self._send_with_flood_wait(
                    self._client,
                    InviteToChannelRequest(chat_entity, [bot_user]),
                )
                await self._set_bot_admin(chat_entity)
                await self._send_with_flood_wait(
                    self.inline.bot.send_message,
                    self.chat_logs,
                    text,
                    disable_web_page_preview=True,
                    parse_mode="HTML",
                )
            except Exception:
                if not self._setup_failed:
                    await self._ensure_log_chat()

    def _approve(self, user_id, reason="unknown"):
        if user_id not in self._whitelist:
            self._whitelist.append(user_id)
        self.set("whitelist", self._whitelist)
        self._processing.discard(user_id)
        logger.debug(f"[RaidProtection] Approved {user_id}, reason: {reason}")

    @loader.command(
        ru_doc="Включить/выключить защиту от рейдов",
        en_doc="Toggle raid protection on/off",
    )
    async def rp(self, message: Message):
        """Toggle raid protection on/off"""
        args = utils.get_args_raw(message)
        if not args:
            current = self.get("state", False)
            new = not current
            self.set("state", new)
            if new:
                await utils.answer(message, self.strings["enabled"])
            else:
                await utils.answer(message, self.strings["disabled"])
            return
        prefix = self.get_prefix()
        await utils.answer(
            message,
            self.strings["help"].format(prefix=prefix),
        )

    @loader.watcher()
    async def watcher(self, message: Message):
        if (
            getattr(message, "out", False)
            or not isinstance(message, Message)
            or not isinstance(message.peer_id, PeerUser)
            or not self.get("state", False)
            or utils.get_chat_id(message)
            in {
                777000,
                self._tg_id,
            }
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
                await self._client.get_messages(
                    peer,
                    limit=1,
                    reverse=True,
                )
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
            except Exception as e:
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
                "<sticker>"
                if message.sticker
                else (
                    "<photo>"
                    if message.photo
                    else (
                        "<video>"
                        if message.video
                        else (
                            "<file>"
                            if message.document
                            else raw[:3000]
                        )
                    )
                )
            )
            username_line = ""
            if username:
                username_line = f"<b>Username:</b> @{username}\n"
            log_text = self.strings["banned_log"].format(
                user_id=sender_id,
                name=name,
                username_line=username_line,
                text=msg_text,
            )
            await self._send_log(log_text)
            if blocked:
                logger.warning(f"[RaidProtection] Raider punished: {sender_id}")
                self._approve(sender_id, "banned")
            else:
                logger.warning(f"[RaidProtection] Raider partially handled: {sender_id}")
        finally:
            self._processing.discard(sender_id)