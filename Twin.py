__version__ = (2, 0, 0)
# meta developer: FireJester.t.me

import asyncio
import logging
import re
from datetime import datetime, timezone, timedelta

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.functions.account import UpdateProfileRequest, UpdateStatusRequest
from telethon.tl.types import Message
from telethon.errors import AuthKeyUnregisteredError, UserDeactivatedBanError

from .. import loader, utils

logger = logging.getLogger(__name__)

STRING_SESSION_PATTERN = re.compile(r'1[A-Za-z0-9_-]{200,}={0,2}')


@loader.tds
class Twin(loader.Module):
    """Twin account manager with auto-reply and online status control"""

    strings = {
        "name": "Twin",
        "line": "--------------------",
        "help": (
            "<b>Twin Account Manager</b>\n\n"
            "<b>Session:</b>\n"
            "<code>{prefix}twin add [session]</code> - add session\n"
            "<code>{prefix}twin remove</code> - remove session\n"
            "<code>{prefix}twin status</code> - connection status\n\n"
            "<b>Settings:</b>\n"
            "<code>{prefix}twin timezone [-12..12]</code> - set timezone\n"
            "<code>{prefix}twin nick [name]</code> - set nickname (format: name | HH:MM)\n"
            "<code>{prefix}twin text [text]</code> - set auto-reply text (HTML supported)\n"
            "<code>{prefix}twin media [url]</code> - set media for auto-reply\n"
            "<code>{prefix}twin online</code> - set online status\n"
            "<code>{prefix}twin offline</code> - set offline status\n\n"
            "<b>Triggers:</b>\n"
            "<code>{prefix}twin target [words...]</code> - add triggers (max 10)\n"
            "<code>{prefix}twin target clear</code> - clear triggers\n"
            "<code>{prefix}twin target list</code> - list triggers\n"
        ),
        "no_session": "<b>Error:</b> No session added. Use <code>{prefix}twin add [session]</code>",
        "session_added": "<b>Session added successfully</b>\n{line}\nAccount: {name}\nID: <code>{user_id}</code>\n{line}",
        "session_removed": "<b>Session removed from memory</b>",
        "session_invalid": "<b>Error:</b> Invalid session\n{line}\nReason: {error}\n{line}",
        "session_exists": "<b>Error:</b> Session already exists. Remove first: <code>{prefix}twin remove</code>",
        "provide_session": "<b>Error:</b> Provide StringSession via argument or reply",
        "timezone_set": "<b>Timezone set:</b> UTC{tz}",
        "timezone_invalid": "<b>Error:</b> Invalid timezone. Range: -12 to +12",
        "nick_set": "<b>Nickname set:</b> <code>{nick}</code>\n{line}\nFormat: {nick} | HH:MM\n{line}",
        "nick_provide": "<b>Error:</b> Provide nickname",
        "text_set": "<b>Auto-reply text set:</b>\n{line}\n{text}\n{line}",
        "text_provide": "<b>Error:</b> Provide auto-reply text",
        "media_set": "<b>Media set successfully</b>",
        "media_removed": "<b>Media removed</b>",
        "media_invalid": "<b>Error:</b> Provide media URL or reply to media",
        "target_added": "<b>Triggers added:</b>\n{line}\n{targets}\n{line}",
        "target_cleared": "<b>Trigger list cleared</b>",
        "target_list": "<b>Current triggers ({count}/10):</b>\n{line}\n{targets}\n{line}",
        "target_empty": "<b>Trigger list is empty</b>",
        "target_max": "<b>Error:</b> Maximum 10 triggers. Current: {count}",
        "target_provide": "<b>Error:</b> Provide trigger words",
        "status_online": (
            "<b>Twin Status: Online</b>\n"
            "{line}\n"
            "Account: {name}\n"
            "ID: <code>{user_id}</code>\n"
            "Timezone: UTC{tz}\n"
            "Nickname: <code>{nick}</code>\n"
            "Triggers: {targets}\n"
            "Auto-reply: {has_text}\n"
            "Media: {has_media}\n"
            "Online mode: {online_mode}\n"
            "{line}"
        ),
        "status_offline": "<b>Twin Status: Offline</b>",
        "online_set": "<b>Twin account status:</b> Online\n{line}\nAccount will appear online\n{line}",
        "offline_set": "<b>Twin account status:</b> Offline\n{line}\nAccount will appear offline\n{line}",
        "online_error": "<b>Error:</b> Failed to update status",
    }

    strings_ru = {
        "line": "--------------------",
        "help": (
            "<b>Twin - Менеджер второго аккаунта</b>\n\n"
            "<b>Сессия:</b>\n"
            "<code>{prefix}twin add [сессия]</code> - добавить сессию\n"
            "<code>{prefix}twin remove</code> - удалить сессию\n"
            "<code>{prefix}twin status</code> - статус подключения\n\n"
            "<b>Настройки:</b>\n"
            "<code>{prefix}twin timezone [-12..12]</code> - установить часовой пояс\n"
            "<code>{prefix}twin nick [имя]</code> - установить никнейм (формат: имя | ЧЧ:ММ)\n"
            "<code>{prefix}twin text [текст]</code> - установить текст автоответа (HTML)\n"
            "<code>{prefix}twin media [url]</code> - установить медиа для автоответа\n"
            "<code>{prefix}twin online</code> - установить статус онлайн\n"
            "<code>{prefix}twin offline</code> - установить статус оффлайн\n\n"
            "<b>Триггеры:</b>\n"
            "<code>{prefix}twin target [слова...]</code> - добавить триггеры (макс 10)\n"
            "<code>{prefix}twin target clear</code> - очистить триггеры\n"
            "<code>{prefix}twin target list</code> - список триггеров\n"
        ),
        "no_session": "<b>Ошибка:</b> Нет сессии. Используйте <code>{prefix}twin add [сессия]</code>",
        "session_added": "<b>Сессия успешно добавлена</b>\n{line}\nАккаунт: {name}\nID: <code>{user_id}</code>\n{line}",
        "session_removed": "<b>Сессия удалена из памяти</b>",
        "session_invalid": "<b>Ошибка:</b> Недействительная сессия\n{line}\nПричина: {error}\n{line}",
        "session_exists": "<b>Ошибка:</b> Сессия уже существует. Сначала удалите: <code>{prefix}twin remove</code>",
        "provide_session": "<b>Ошибка:</b> Укажите StringSession аргументом или ответом",
        "timezone_set": "<b>Часовой пояс установлен:</b> UTC{tz}",
        "timezone_invalid": "<b>Ошибка:</b> Неверный часовой пояс. Диапазон: от -12 до +12",
        "nick_set": "<b>Никнейм установлен:</b> <code>{nick}</code>\n{line}\nФормат: {nick} | ЧЧ:ММ\n{line}",
        "nick_provide": "<b>Ошибка:</b> Укажите никнейм",
        "text_set": "<b>Текст автоответа установлен:</b>\n{line}\n{text}\n{line}",
        "text_provide": "<b>Ошибка:</b> Укажите текст автоответа",
        "media_set": "<b>Медиа успешно установлено</b>",
        "media_removed": "<b>Медиа удалено</b>",
        "media_invalid": "<b>Ошибка:</b> Укажите URL медиа или ответьте на медиа",
        "target_added": "<b>Триггеры добавлены:</b>\n{line}\n{targets}\n{line}",
        "target_cleared": "<b>Список триггеров очищен</b>",
        "target_list": "<b>Текущие триггеры ({count}/10):</b>\n{line}\n{targets}\n{line}",
        "target_empty": "<b>Список триггеров пуст</b>",
        "target_max": "<b>Ошибка:</b> Максимум 10 триггеров. Текущее количество: {count}",
        "target_provide": "<b>Ошибка:</b> Укажите слова-триггеры",
        "status_online": (
            "<b>Статус Twin: Онлайн</b>\n"
            "{line}\n"
            "Аккаунт: {name}\n"
            "ID: <code>{user_id}</code>\n"
            "Часовой пояс: UTC{tz}\n"
            "Никнейм: <code>{nick}</code>\n"
            "Триггеры: {targets}\n"
            "Автоответ: {has_text}\n"
            "Медиа: {has_media}\n"
            "Режим онлайн: {online_mode}\n"
            "{line}"
        ),
        "status_offline": "<b>Статус Twin: Оффлайн</b>",
        "online_set": "<b>Статус Twin аккаунта:</b> Онлайн\n{line}\nАккаунт будет отображаться онлайн\n{line}",
        "offline_set": "<b>Статус Twin аккаунта:</b> Оффлайн\n{line}\nАккаунт будет отображаться оффлайн\n{line}",
        "online_error": "<b>Ошибка:</b> Не удалось обновить статус",
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            "SESSION", "", "StringSession of second account",
            "TIMEZONE", 3, "Timezone UTC offset",
            "NICK_TEMPLATE", "", "Nickname template without time",
            "AUTO_TEXT", "", "Auto-reply text",
            "MEDIA_URL", "", "Media URL for auto-reply",
            "SAVED_MSG_ID", 0, "Saved media message ID",
            "TARGETS", [], "Trigger words for auto-reply",
            "KEEP_ONLINE", False, "Keep account online",
        )
        self._twin_client = None
        self._twin_me = None
        self._nick_task = None
        self._online_task = None
        self._connected = False

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        if self.config["SESSION"]:
            asyncio.create_task(self._connect_twin())

    async def on_unload(self):
        await self._disconnect_twin()

    async def _connect_twin(self):
        if not self.config["SESSION"]:
            return False

        try:
            self._twin_client = TelegramClient(
                StringSession(self.config["SESSION"]),
                api_id=2040,
                api_hash="b18441a1ff607e10a989891a5462e627"
            )
            await self._twin_client.connect()
            
            if not await self._twin_client.is_user_authorized():
                raise AuthKeyUnregisteredError("Session expired")
            
            self._twin_me = await self._twin_client.get_me()
            self._connected = True
            
            if self.config["NICK_TEMPLATE"]:
                await self._start_nick_loop()
            
            if self.config["KEEP_ONLINE"]:
                await self._start_online_loop()
            
            from telethon import events
            
            @self._twin_client.on(events.NewMessage(incoming=True))
            async def handler(event):
                await self._handle_twin_message(event)
            
            asyncio.create_task(self._twin_client.run_until_disconnected())
            
            logger.info(f"[TWIN] Connected: {self._twin_me.first_name}")
            return True
            
        except AuthKeyUnregisteredError:
            self._connected = False
            self.config["SESSION"] = ""
            logger.error("[TWIN] Session revoked")
            return False
        except UserDeactivatedBanError:
            self._connected = False
            self.config["SESSION"] = ""
            logger.error("[TWIN] Account banned")
            return False
        except Exception as e:
            self._connected = False
            logger.error(f"[TWIN] Connection error: {e}")
            return False

    async def _disconnect_twin(self):
        if self._nick_task:
            self._nick_task.cancel()
            try:
                await self._nick_task
            except asyncio.CancelledError:
                pass
            self._nick_task = None

        if self._online_task:
            self._online_task.cancel()
            try:
                await self._online_task
            except asyncio.CancelledError:
                pass
            self._online_task = None

        if self._twin_client:
            try:
                await self._twin_client.disconnect()
            except:
                pass
            self._twin_client = None
        
        self._twin_me = None
        self._connected = False

    async def _start_nick_loop(self):
        if self._nick_task and not self._nick_task.done():
            return
        self._nick_task = asyncio.create_task(self._nick_update_loop())

    async def _stop_nick_loop(self):
        if self._nick_task:
            self._nick_task.cancel()
            try:
                await self._nick_task
            except asyncio.CancelledError:
                pass
            self._nick_task = None

    async def _start_online_loop(self):
        if self._online_task and not self._online_task.done():
            return
        self._online_task = asyncio.create_task(self._online_update_loop())

    async def _stop_online_loop(self):
        if self._online_task:
            self._online_task.cancel()
            try:
                await self._online_task
            except asyncio.CancelledError:
                pass
            self._online_task = None

    async def _nick_update_loop(self):
        while self._connected and self.config["NICK_TEMPLATE"]:
            try:
                offset = self.config["TIMEZONE"]
                tz = timezone(timedelta(hours=offset))
                now = datetime.now(tz)
                time_str = now.strftime("%H:%M")
                
                new_nick = f"{self.config['NICK_TEMPLATE']} | {time_str}"
                
                await self._twin_client(UpdateProfileRequest(first_name=new_nick[:64]))
                
                seconds_to_wait = 60 - now.second - (now.microsecond / 1_000_000)
                if seconds_to_wait < 1:
                    seconds_to_wait = 60
                
                await asyncio.sleep(seconds_to_wait)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[TWIN] Nick update error: {e}")
                await asyncio.sleep(60)

    async def _online_update_loop(self):
        while self._connected and self.config["KEEP_ONLINE"]:
            try:
                await self._twin_client(UpdateStatusRequest(offline=False))
                await asyncio.sleep(15)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[TWIN] Online update error: {e}")
                await asyncio.sleep(30)

    async def _handle_twin_message(self, event):
        if not self._connected:
            return
        
        message = event.message
        
        if message.out:
            return
        
        targets = self.config["TARGETS"]
        auto_text = self.config["AUTO_TEXT"]
        
        if not targets or not auto_text:
            return
        
        msg_text = (message.text or "").lower()
        if not msg_text:
            return
        
        triggered = False
        for target in targets:
            if target.lower() in msg_text:
                triggered = True
                break
        
        if not triggered:
            return
        
        try:
            media = None
            media_url = self.config["MEDIA_URL"]
            
            if media_url:
                if media_url.startswith("saved:"):
                    msg_id = self.config["SAVED_MSG_ID"]
                    if msg_id:
                        saved_msg = await self._client.get_messages("me", ids=msg_id)
                        if saved_msg and saved_msg.media:
                            media = saved_msg.media
                else:
                    media = media_url
            
            if media:
                await message.reply(auto_text, file=media, parse_mode="html")
            else:
                await message.reply(auto_text, parse_mode="html")
                
        except Exception as e:
            logger.error(f"[TWIN] Auto-reply error: {e}")

    @loader.command(
        ru_doc="Управление Twin аккаунтом",
        en_doc="Twin account management",
    )
    async def twin(self, message: Message):
        """Twin account management"""
        args = utils.get_args_raw(message)
        args_list = args.split() if args else []
        prefix = self.get_prefix()
        
        if not args_list:
            await utils.answer(
                message,
                self.strings["help"].format(prefix=prefix),
            )
            return
        
        cmd = args_list[0].lower()
        
        if cmd == "add":
            await self._cmd_add(message, args_list)
        elif cmd == "remove":
            await self._cmd_remove(message)
        elif cmd == "status":
            await self._cmd_status(message)
        elif cmd == "timezone":
            await self._cmd_timezone(message, args_list)
        elif cmd == "nick":
            await self._cmd_nick(message, args, args_list)
        elif cmd == "text":
            await self._cmd_text(message, args)
        elif cmd == "media":
            await self._cmd_media(message, args_list)
        elif cmd == "target":
            await self._cmd_target(message, args_list)
        elif cmd == "online":
            await self._cmd_online(message)
        elif cmd == "offline":
            await self._cmd_offline(message)
        else:
            await utils.answer(
                message,
                self.strings["help"].format(prefix=prefix),
            )

    async def _cmd_add(self, message: Message, args):
        prefix = self.get_prefix()
        if self.config["SESSION"]:
            return await utils.answer(
                message,
                self.strings["session_exists"].format(prefix=prefix),
            )
        
        session_str = None
        
        if len(args) > 1:
            text = " ".join(args[1:])
            match = STRING_SESSION_PATTERN.search(text)
            if match:
                session_str = match.group(0)
        
        if not session_str:
            reply = await message.get_reply_message()
            if reply and reply.text:
                match = STRING_SESSION_PATTERN.search(reply.text)
                if match:
                    session_str = match.group(0)
        
        if not session_str:
            return await utils.answer(message, self.strings["provide_session"])
        
        self.config["SESSION"] = session_str
        
        try:
            success = await self._connect_twin()
            
            if success:
                name = self._twin_me.first_name or "Unknown"
                await message.delete()
                await self._client.send_message(
                    message.chat_id,
                    self.strings["session_added"].format(
                        line=self.strings["line"],
                        name=name,
                        user_id=self._twin_me.id
                    ),
                    reply_to=message.reply_to_msg_id
                )
            else:
                self.config["SESSION"] = ""
                await utils.answer(
                    message,
                    self.strings["session_invalid"].format(
                        line=self.strings["line"],
                        error="Connection failed"
                    )
                )
        except Exception as e:
            self.config["SESSION"] = ""
            await utils.answer(
                message,
                self.strings["session_invalid"].format(
                    line=self.strings["line"],
                    error=str(e)
                )
            )

    async def _cmd_remove(self, message: Message):
        await self._disconnect_twin()
        self.config["SESSION"] = ""
        self.config["NICK_TEMPLATE"] = ""
        self.config["KEEP_ONLINE"] = False
        await utils.answer(message, self.strings["session_removed"])

    async def _cmd_status(self, message: Message):
        if not self._connected or not self._twin_me:
            return await utils.answer(message, self.strings["status_offline"])
        
        tz = self.config["TIMEZONE"]
        tz_str = f"+{tz}" if tz >= 0 else str(tz)
        
        await utils.answer(
            message,
            self.strings["status_online"].format(
                line=self.strings["line"],
                name=self._twin_me.first_name or "Unknown",
                user_id=self._twin_me.id,
                tz=tz_str,
                nick=self.config["NICK_TEMPLATE"] or "not set",
                targets=len(self.config["TARGETS"]),
                has_text="Yes" if self.config["AUTO_TEXT"] else "No",
                has_media="Yes" if self.config["MEDIA_URL"] else "No",
                online_mode="Yes" if self.config["KEEP_ONLINE"] else "No"
            )
        )

    async def _cmd_timezone(self, message: Message, args):
        if len(args) < 2:
            return await utils.answer(message, self.strings["timezone_invalid"])
        
        try:
            tz = int(args[1].replace("+", ""))
            if not -12 <= tz <= 12:
                raise ValueError
            
            self.config["TIMEZONE"] = tz
            tz_str = f"+{tz}" if tz >= 0 else str(tz)
            await utils.answer(message, self.strings["timezone_set"].format(tz=tz_str))
            
        except ValueError:
            await utils.answer(message, self.strings["timezone_invalid"])

    async def _cmd_nick(self, message: Message, raw_args: str, args_list: list):
        if len(args_list) < 2:
            return await utils.answer(message, self.strings["nick_provide"])
        
        nick = raw_args[5:].strip()
        self.config["NICK_TEMPLATE"] = nick
        
        await utils.answer(
            message,
            self.strings["nick_set"].format(
                line=self.strings["line"],
                nick=nick
            )
        )
        
        if self._connected:
            await self._stop_nick_loop()
            await self._start_nick_loop()

    async def _cmd_text(self, message: Message, raw_args: str):
        if len(raw_args) <= 5:
            return await utils.answer(message, self.strings["text_provide"])
        
        text = raw_args[5:].strip()
        
        if not text:
            return await utils.answer(message, self.strings["text_provide"])
        
        self.config["AUTO_TEXT"] = text
        
        await utils.answer(
            message,
            self.strings["text_set"].format(
                line=self.strings["line"],
                text=text
            )
        )

    async def _cmd_media(self, message: Message, args):
        reply = await message.get_reply_message()
        
        if reply and reply.media:
            try:
                old_msg_id = self.config["SAVED_MSG_ID"]
                if old_msg_id:
                    try:
                        old_msg = await self._client.get_messages("me", ids=old_msg_id)
                        if old_msg:
                            await old_msg.delete()
                    except:
                        pass
                
                saved_msg = await self._client.send_file(
                    "me",
                    reply.media,
                    caption="Twin module media storage",
                    silent=True
                )
                
                self.config["SAVED_MSG_ID"] = saved_msg.id
                self.config["MEDIA_URL"] = f"saved:{saved_msg.id}"
                
                await utils.answer(message, self.strings["media_set"])
                return
                
            except Exception as e:
                logger.error(f"[TWIN] Media save error: {e}")
        
        if len(args) > 1:
            url = args[1]
            
            old_msg_id = self.config["SAVED_MSG_ID"]
            if old_msg_id:
                try:
                    old_msg = await self._client.get_messages("me", ids=old_msg_id)
                    if old_msg:
                        await old_msg.delete()
                except:
                    pass
            
            self.config["SAVED_MSG_ID"] = 0
            self.config["MEDIA_URL"] = url
            
            await utils.answer(message, self.strings["media_set"])
            return
        
        if len(args) == 1:
            old_msg_id = self.config["SAVED_MSG_ID"]
            if old_msg_id:
                try:
                    old_msg = await self._client.get_messages("me", ids=old_msg_id)
                    if old_msg:
                        await old_msg.delete()
                except:
                    pass
            
            self.config["SAVED_MSG_ID"] = 0
            self.config["MEDIA_URL"] = ""
            
            await utils.answer(message, self.strings["media_removed"])
            return
        
        await utils.answer(message, self.strings["media_invalid"])

    async def _cmd_target(self, message: Message, args):
        if len(args) < 2:
            return await utils.answer(message, self.strings["target_provide"])
        
        subcmd = args[1].lower()
        
        if subcmd == "clear":
            self.config["TARGETS"] = []
            await utils.answer(message, self.strings["target_cleared"])
            
        elif subcmd == "list":
            targets = self.config["TARGETS"]
            if not targets:
                await utils.answer(message, self.strings["target_empty"])
            else:
                await utils.answer(
                    message,
                    self.strings["target_list"].format(
                        line=self.strings["line"],
                        count=len(targets),
                        targets=", ".join(targets)
                    )
                )
        else:
            new_targets = args[1:]
            current = self.config["TARGETS"] or []
            
            available = 10 - len(current)
            if available <= 0:
                return await utils.answer(
                    message,
                    self.strings["target_max"].format(count=len(current))
                )
            
            added = []
            for t in new_targets[:available]:
                if t.lower() not in [x.lower() for x in current]:
                    current.append(t)
                    added.append(t)
            
            self.config["TARGETS"] = current
            
            await utils.answer(
                message,
                self.strings["target_added"].format(
                    line=self.strings["line"],
                    targets=", ".join(added) if added else "already exist"
                )
            )

    async def _cmd_online(self, message: Message):
        prefix = self.get_prefix()
        if not self._connected or not self._twin_client:
            return await utils.answer(
                message,
                self.strings["no_session"].format(prefix=prefix),
            )
        
        try:
            await self._twin_client(UpdateStatusRequest(offline=False))
            self.config["KEEP_ONLINE"] = True
            await self._start_online_loop()
            await utils.answer(
                message,
                self.strings["online_set"].format(line=self.strings["line"])
            )
        except Exception as e:
            logger.error(f"[TWIN] Online status error: {e}")
            await utils.answer(message, self.strings["online_error"])

    async def _cmd_offline(self, message: Message):
        prefix = self.get_prefix()
        if not self._connected or not self._twin_client:
            return await utils.answer(
                message,
                self.strings["no_session"].format(prefix=prefix),
            )
        
        try:
            self.config["KEEP_ONLINE"] = False
            await self._stop_online_loop()
            await self._twin_client(UpdateStatusRequest(offline=True))
            await utils.answer(
                message,
                self.strings["offline_set"].format(line=self.strings["line"])
            )
        except Exception as e:
            logger.error(f"[TWIN] Offline status error: {e}")
            await utils.answer(message, self.strings["online_error"])