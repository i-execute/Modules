__version__ = (1, 0, 0)
# meta developer: FireJester.t.me

import logging
import asyncio
from telethon.tl.types import PeerUser, Channel, User
from telethon.tl.functions.channels import InviteToChannelRequest, EditAdminRequest
from telethon.tl.types import ChatAdminRights
from telethon.errors import FloodWaitError, ChatAdminRequiredError
from .. import loader, utils

logger = logging.getLogger(__name__)

LOG_GROUP_AVATAR_URL = "https://github.com/FireJester/Media/raw/main/Group_avatar_in_logger.jpeg"
GREETING_MEDIA_URL = "https://github.com/FireJester/Media/raw/main/Greeting_in_logger.jpeg"


@loader.tds
class Logger(loader.Module):
    """Command logger for userbot"""

    strings = {
        "name": "Logger",
        "greeting_first": (
            "<b>Yo!</b>\n\n"
            "Successfully joined this group and can write, watchers are active. "
            "Now every command will be logged here, if some asshole uses your userbot I'll tag you\n\n"
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
            "<code>.logger group</code> - set current group as log group\n"
        ),
        "status": (
            "<b>Logger Status</b>\n\n"
            "<b>Log group:</b> {group_name}\n"
            "<b>ID:</b> <code>{group_id}</code>\n"
            "<b>Username:</b> {group_username}\n"
            "<b>Status:</b> {status}\n"
        ),
        "status_no_group": (
            "<b>Logger Status</b>\n\n"
            "<b>Log group:</b> Not configured\n"
            "<b>Status:</b> Inactive\n\n"
            "Use <code>.logger group</code> in the desired group"
        ),
        "group_set": (
            "<b>Log group set!</b>\n\n"
            "<b>Group:</b> {group_name}\n"
            "<b>ID:</b> <code>{group_id}</code>\n"
        ),
        "group_not_group": "<b>Error:</b> This command only works in groups!",
        "group_no_rights": "<b>Error:</b> No rights to assign administrators in this group!",
        "group_error": "<b>Error:</b> {error}",
        "inline_create_failed": (
            "<b>Failed to create/setup log group</b>\n\n"
            "Use command <code>.logger group</code> in a group where you're admin to set it as log group."
        ),
        "inline_setup_failed": (
            "<b>Failed to setup log group</b>\n\n"
            "Error: <code>{error}</code>\n\n"
            "Use command <code>.logger group</code> in a group where you have proper admin rights."
        ),
    }

    strings_ru = {
        "greeting_first": (
            "<b>Ку!</b>\n\n"
            "Я успешно инвайтнулся в эту группу и могу писать, вотчеры активны. "
            "Теперь каждая команда будет залетать сюда, если какой-то хуй будет использовать твой юзербот то я тэгну тебя\n\n"
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
            "<code>.logger group</code> - установить текущую группу как лог-группу\n"
        ),
        "status": (
            "<b>Статус Logger</b>\n\n"
            "<b>Лог-группа:</b> {group_name}\n"
            "<b>ID:</b> <code>{group_id}</code>\n"
            "<b>Username:</b> {group_username}\n"
            "<b>Статус:</b> {status}\n"
        ),
        "status_no_group": (
            "<b>Статус Logger</b>\n\n"
            "<b>Лог-группа:</b> Не настроена\n"
            "<b>Статус:</b> Неактивен\n\n"
            "Используй <code>.logger group</code> в нужной группе"
        ),
        "group_set": (
            "<b>Лог-группа установлена!</b>\n\n"
            "<b>Группа:</b> {group_name}\n"
            "<b>ID:</b> <code>{group_id}</code>\n"
        ),
        "group_not_group": "<b>Ошибка:</b> Эта команда работает только в группах!",
        "group_no_rights": "<b>Ошибка:</b> Нет прав на назначение администраторов в этой группе!",
        "group_error": "<b>Ошибка:</b> {error}",
        "inline_create_failed": (
            "<b>Не удалось создать/настроить лог-группу</b>\n\n"
            "Юзай команду <code>.logger group</code> в группе, где ты админ, чтобы установить её как группу для логов."
        ),
        "inline_setup_failed": (
            "<b>Не удалось настроить лог-группу</b>\n\n"
            "Ошибка: <code>{error}</code>\n\n"
            "Юзай команду <code>.logger group</code> в группе, где у тебя есть нормальная админка."
        ),
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "LOG_CHAT_ID",
                0,
                lambda: "ID группы для логов команд",
                validator=loader.validators.Integer(),
            ),
            loader.ConfigValue(
                "LOG_CHAT_NAME",
                "",
                lambda: "Название группы логов",
            ),
        )
        self._owner = None
        self.chat_logs = None
        self._flood_lock = asyncio.Lock()
        self._setup_failed = False

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
        first = getattr(entity, 'first_name', '') or ''
        last = getattr(entity, 'last_name', '') or ''
        return f"{first} {last}".strip() or "Unknown"

    def _format_username_row(self, entity):
        username = self._get_username(entity)
        return self.strings["username_row"].format(uname=username) if username else ""

    def _get_topic_id(self, message):
        reply_to = getattr(message, 'reply_to', None)
        if reply_to:
            if getattr(reply_to, 'forum_topic', False):
                return getattr(reply_to, 'reply_to_top_id', None) or getattr(reply_to, 'reply_to_msg_id', None)
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
                rank="Logger"
            ))
            return True
        except ChatAdminRequiredError:
            return False
        except Exception as e:
            logger.error(f"[Logger] Failed to set bot admin: {e}")
            return False

    async def _send_with_flood_wait(self, coro_func, *args, **kwargs):
        max_retries = 5
        for attempt in range(max_retries):
            try:
                return await coro_func(*args, **kwargs)
            except FloodWaitError as e:
                wait_time = e.seconds + 1
                await asyncio.sleep(wait_time)
            except Exception as e:
                error_str = str(e).lower()
                if "flood" in error_str and "retry after" in error_str:
                    import re
                    match = re.search(r"retry after (\d+)", error_str)
                    if match:
                        wait_time = int(match.group(1)) + 1
                        await asyncio.sleep(wait_time)
                        continue
                raise
        return None

    async def _send_inline_notification(self, text: str):
        try:
            await self.inline.bot.send_message(
                self._owner.id,
                text,
                parse_mode="HTML"
            )
        except Exception as e:
            logger.error(f"[Logger] Failed to send inline notification: {e}")

    async def _send_greeting(self, is_recovery=False, error_text=None):
        greeting_text = (
            self.strings["greeting_recovery"]
            if is_recovery
            else self.strings["greeting_first"]
        )

        try:
            await self._send_with_flood_wait(
                self.inline.bot.send_photo,
                self.chat_logs,
                photo=GREETING_MEDIA_URL,
                caption=greeting_text,
                parse_mode="HTML"
            )

            if is_recovery and error_text:
                await self._send_with_flood_wait(
                    self.inline.bot.send_message,
                    self.chat_logs,
                    self.strings["error_info"].format(error_text=self._escape(str(error_text))),
                    parse_mode="HTML"
                )
        except Exception as e:
            logger.error(f"[Logger] Greeting failed: {e}")

    async def _try_setup_group(self, chat_entity, is_recovery=False, error_text=None):
        try:
            try:
                bot_user = await self._client.get_entity(self.inline.bot_username)
                await self._send_with_flood_wait(
                    self._client,
                    InviteToChannelRequest(chat_entity, [bot_user])
                )
            except Exception as e:
                logger.warning(f"[Logger] Bot invite failed (maybe already in): {e}")

            admin_set = await self._set_bot_admin(chat_entity)
            if not admin_set:
                return False, "Failed to set bot admin rights"

            await asyncio.sleep(2)
            await self._send_greeting(is_recovery=is_recovery, error_text=error_text)
            return True, None
            
        except Exception as e:
            return False, str(e)

    async def _ensure_log_chat(self, error_text=None):
        async with self._flood_lock:
            is_recovery = self.config["LOG_CHAT_ID"] != 0

            try:
                chat_entity, _ = await utils.asset_channel(
                    self._client,
                    "Command Logs",
                    "Command logs will appear here. @FireJester with ♡",
                    silent=True,
                    avatar=LOG_GROUP_AVATAR_URL,
                )

                self.config["LOG_CHAT_ID"] = chat_entity.id
                self.config["LOG_CHAT_NAME"] = getattr(chat_entity, 'title', 'Command Logs')
                self.chat_logs = int(f"-100{chat_entity.id}")

                success, setup_error = await self._try_setup_group(
                    chat_entity, 
                    is_recovery=is_recovery, 
                    error_text=error_text
                )
                
                if not success:
                    raise Exception(setup_error)
                    
                self._setup_failed = False
                
            except Exception as e:
                logger.error(f"[Logger] Failed to create/setup log group: {e}")
                self._setup_failed = True
                await self._send_inline_notification(
                    self.strings["inline_create_failed"]
                )

    async def _setup_existing_group(self, chat_entity):
        try:
            self.config["LOG_CHAT_ID"] = chat_entity.id
            self.config["LOG_CHAT_NAME"] = getattr(chat_entity, 'title', 'Log Group')
            self.chat_logs = int(f"-100{chat_entity.id}")

            success, setup_error = await self._try_setup_group(chat_entity)
            
            if success:
                self._setup_failed = False
                return True, None
            else:
                await self._ensure_log_chat(error_text=setup_error)
                if self._setup_failed:
                    return False, setup_error
                return True, None
                
        except Exception as e:
            return False, str(e)

    async def client_ready(self, client, _):
        self._client = client
        self._owner = await client.get_me()
        saved_id = self.config["LOG_CHAT_ID"]

        if saved_id != 0:
            try:
                self.chat_logs = int(f"-100{saved_id}")
                chat_entity = await client.get_entity(self.chat_logs)
                self.config["LOG_CHAT_NAME"] = getattr(chat_entity, 'title', 'Log Group')

                try:
                    await self._send_with_flood_wait(
                        self.inline.bot.send_message,
                        self.chat_logs,
                        self.strings["reloaded"],
                        parse_mode="HTML"
                    )
                    self._setup_failed = False
                except Exception as e:
                    try:
                        bot_user = await self._client.get_entity(self.inline.bot_username)
                        await self._send_with_flood_wait(
                            self._client,
                            InviteToChannelRequest(chat_entity, [bot_user])
                        )
                        await self._set_bot_admin(chat_entity)
                        await self._send_with_flood_wait(
                            self.inline.bot.send_message,
                            self.chat_logs,
                            self.strings["reloaded"],
                            parse_mode="HTML"
                        )
                        self._setup_failed = False
                    except Exception as invite_error:
                        await self._ensure_log_chat(error_text=f"Bot invite failed: {invite_error}")
            except Exception as e:
                await self._ensure_log_chat(error_text=f"Startup check failed: {e}")
        else:
            await self._ensure_log_chat()

    async def _send_log(self, text):
        if not self.chat_logs:
            if not self._setup_failed:
                await self._ensure_log_chat(error_text="Memory empty")
            return

        try:
            await self._send_with_flood_wait(
                self.inline.bot.send_message,
                self.chat_logs,
                text,
                disable_web_page_preview=True,
                parse_mode="HTML"
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
                    InviteToChannelRequest(chat_entity, [bot_user])
                )
                await self._set_bot_admin(chat_entity)
                await self._send_with_flood_wait(
                    self.inline.bot.send_message,
                    self.chat_logs,
                    text,
                    disable_web_page_preview=True,
                    parse_mode="HTML"
                )
            except Exception:
                if not self._setup_failed:
                    await self._ensure_log_chat(error_text=e)
                    try:
                        await self._send_with_flood_wait(
                            self.inline.bot.send_message,
                            self.chat_logs,
                            text,
                            disable_web_page_preview=True,
                            parse_mode="HTML"
                        )
                    except Exception:
                        pass

    @loader.command(
        ru_doc="Показать справку по командам",
        en_doc="Show help for commands"
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
        elif cmd == "group":
            await self._cmd_group(message)
        else:
            await utils.answer(message, self.strings["help"])

    async def _cmd_status(self, message):
        if self.config["LOG_CHAT_ID"] == 0:
            await utils.answer(message, self.strings["status_no_group"])
            return
        
        try:
            chat_entity = await self._client.get_entity(
                int(f"-100{self.config['LOG_CHAT_ID']}")
            )
            group_name = self._escape(getattr(chat_entity, 'title', 'Unknown'))
            username = self._get_username(chat_entity)
            username_str = f"@{username}" if username else "—"
            status = "Active"
            
        except Exception:
            group_name = self.config.get("LOG_CHAT_NAME", "Unknown")
            username_str = "—"
            status = "Group unavailable"
        
        await utils.answer(message, self.strings["status"].format(
            group_name=group_name,
            group_id=self.config["LOG_CHAT_ID"],
            group_username=username_str,
            status=status
        ))

    async def _cmd_group(self, message):
        if isinstance(message.peer_id, PeerUser):
            await utils.answer(message, self.strings["group_not_group"])
            return
        
        try:
            chat = await self._client.get_entity(message.peer_id)
            
            if not isinstance(chat, Channel) or not getattr(chat, 'megagroup', False):
                if not isinstance(chat, Channel):
                    await utils.answer(message, self.strings["group_not_group"])
                    return
            
            if hasattr(chat, 'admin_rights') and chat.admin_rights:
                if not chat.admin_rights.add_admins:
                    await utils.answer(message, self.strings["group_no_rights"])
                    return
            elif not getattr(chat, 'creator', False):
                try:
                    full = await self._client.get_permissions(chat, self._owner)
                    if not full.is_admin or not getattr(full, 'add_admins', False):
                        if not getattr(full, 'is_creator', False):
                            await utils.answer(message, self.strings["group_no_rights"])
                            return
                except Exception:
                    pass
            
            success, error = await self._setup_existing_group(chat)
            
            if success:
                await utils.answer(message, self.strings["group_set"].format(
                    group_name=self._escape(getattr(chat, 'title', 'Group')),
                    group_id=chat.id
                ))
            else:
                await utils.answer(message, self.strings["group_error"].format(error=error))
                
        except ChatAdminRequiredError:
            await utils.answer(message, self.strings["group_no_rights"])
        except Exception as e:
            await utils.answer(message, self.strings["group_error"].format(error=str(e)))

    def _is_channel_post(self, message, sender):
        if message.post:
            return True
        if isinstance(sender, Channel) and not getattr(sender, "megagroup", False):
            return True
        return False

    @loader.watcher(only_commands=True)
    async def watcher(self, message):
        try:
            is_dm = isinstance(message.peer_id, PeerUser)
            sender = await message.get_sender()
            if not sender:
                return
            if self.config["LOG_CHAT_ID"] and message.chat_id == self.config["LOG_CHAT_ID"]:
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
                owner_prefix = self.strings["owner_attention"].format(owner_link=owner_link)

            if is_dm:
                is_bot_chat = isinstance(chat, User) and getattr(chat, "bot", False)
                template = self.strings["log_dm_bot"] if is_bot_chat else self.strings["log_dm_user"]
                log_text = template.format(
                    cmd=cmd_text,
                    from_name=sender_name,
                    from_uname=sender_uname,
                    to_name=chat_name,
                    to_uname=chat_uname
                )
            elif is_channel:
                msg_link = self._build_message_link(chat, message)
                log_text = self.strings["log_channel"].format(
                    cmd=cmd_text,
                    chat_name=chat_name,
                    chat_id=chat.id,
                    chat_uname=chat_uname,
                    msg_link=msg_link
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
                    msg_link=msg_link
                )

            log_text = owner_prefix + log_text

            await self._send_log(log_text)
        except Exception as e:
            logger.error(f"[Logger] Watcher error: {e}")