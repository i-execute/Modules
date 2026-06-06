__version__ = (2, 1, 0)
# meta developer: I_execute.t.me

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
from ..inline.types import InlineCall

logger = logging.getLogger(__name__)

STRING_SESSION_PATTERN = re.compile(r'1[A-Za-z0-9_-]{200,}={0,2}')


@loader.tds
class Twin(loader.Module):
    """Twin account manager with auto-reply and online status control"""

    strings = {
        "name": "Twin",
        
        "session_success": (
            "<b>Session Connected Successfully</b>\n\n"
            "<b>Account:</b> {name}\n"
            "<b>ID:</b> <code>{id}</code>\n\n"
            "Click continue to access settings"
        ),
        
        "main_menu": (
            "<b>Twin Account Manager</b>\n\n"
            "<b>Status:</b> {status}\n"
            "<b>Account:</b> {account}\n"
            "<b>Nickname:</b> {nickname}\n"
            "<b>Timezone:</b> UTC{timezone}\n"
            "<b>Auto-reply:</b> {autoreply}\n"
            "<b>Triggers:</b> {triggers}\n"
            "<b>Online mode:</b> {online}"
        ),
        
        "session_menu": (
            "<b>Session Management</b>\n\n"
            "<b>Current status:</b> {status}\n"
            "{info}"
        ),
        
        "settings_menu": (
            "<b>Settings</b>\n\n"
            "<b>Nickname:</b> {nickname}\n"
            "<b>Timezone:</b> UTC{timezone}\n"
            "<b>Auto-reply text:</b> {text}\n"
            "<b>Media:</b> {media}"
        ),
        
        "nickname_menu": (
            "<b>Nickname Settings</b>\n\n"
            "<b>Current:</b> {nickname}\n"
            "<b>Format:</b> {nickname} | HH:MM"
        ),
        
        "timezone_menu": (
            "<b>Timezone Settings</b>\n\n"
            "<b>Current:</b> UTC{timezone}"
        ),
        
        "text_menu": (
            "<b>Auto-reply Text Settings</b>\n\n"
            "<b>Current:</b> {text}"
        ),
        
        "media_menu": (
            "<b>Media Settings</b>\n\n"
            "<b>Status:</b> {status}"
        ),
        
        "triggers_menu": (
            "<b>Trigger Words ({count}/10)</b>\n\n"
            "{triggers}"
        ),
        
        "online_menu": (
            "<b>Online Status Control</b>\n\n"
            "<b>Current mode:</b> {mode}\n"
            "<b>Account visibility:</b> {visibility}"
        ),
        
        "btn_continue": "Continue",
        "btn_session": "Session",
        "btn_settings": "Settings",
        "btn_triggers": "Triggers",
        "btn_online": "Online",
        "btn_back": "Back",
        "btn_close": "Close",
        
        "btn_remove_session": "Remove Session",
        
        "btn_nickname": "Nickname",
        "btn_timezone": "Timezone",
        "btn_text": "Text",
        "btn_media": "Media",
        
        "btn_set_nickname": "Set Nickname",
        "btn_set_timezone": "Set Timezone",
        "btn_set_text": "Set Text",
        "btn_set_media": "Set Media",
        "btn_remove_media": "Remove Media",
        
        "btn_add_trigger": "Add Trigger",
        "btn_clear_triggers": "Clear All",
        
        "btn_enable_online": "Enable Online",
        "btn_disable_online": "Disable Online",
        
        "input_nickname": "Enter nickname template (without time):",
        "input_timezone": "Enter timezone offset (-12 to +12):",
        "input_text": "Enter auto-reply text (HTML supported):",
        "input_media": "Enter media URL:",
        "input_trigger": "Enter trigger words (space separated):",
        
        "status_connected": "Connected",
        "status_disconnected": "Disconnected",
        "status_not_set": "Not set",
        "status_enabled": "Enabled",
        "status_disabled": "Disabled",
        "status_online": "Online",
        "status_offline": "Offline",
        
        "session_removed": "Session removed",
        "session_invalid": "Invalid session: {error}",
        "session_info": "Account: {name}\nID: {id}",
        "no_session": "<b>No session configured.</b> Reply to StringSession with <code>.twin</code> command",
        
        "nickname_set": "Nickname set: {nickname}",
        "timezone_set": "Timezone set: UTC{timezone}",
        "timezone_invalid": "Invalid timezone. Use range: -12 to +12",
        "text_set": "Auto-reply text set",
        "media_set": "Media set successfully",
        "media_removed": "Media removed",
        
        "triggers_added": "Triggers added: {triggers}",
        "triggers_cleared": "All triggers cleared",
        "triggers_max": "Maximum 10 triggers reached",
        "triggers_list": "{triggers}",
        "triggers_empty": "No triggers set",
        
        "online_enabling": "<b>Enabling online mode...</b>\n\nTwin account will appear online",
        "online_disabling": "<b>Disabling online mode...</b>\n\nTwin account will go offline",
        
        "error_no_session": "No session configured",
        "error_not_connected": "Session not connected",
    }

    strings_ru = {
        "session_success": (
            "<b>Сессия успешно подключена</b>\n\n"
            "<b>Аккаунт:</b> {name}\n"
            "<b>ID:</b> <code>{id}</code>\n\n"
            "Нажмите продолжить для доступа к настройкам"
        ),
        
        "main_menu": (
            "<b>Twin Account Manager</b>\n\n"
            "<b>Статус:</b> {status}\n"
            "<b>Аккаунт:</b> {account}\n"
            "<b>Никнейм:</b> {nickname}\n"
            "<b>Часовой пояс:</b> UTC{timezone}\n"
            "<b>Автоответ:</b> {autoreply}\n"
            "<b>Триггеры:</b> {triggers}\n"
            "<b>Режим онлайн:</b> {online}"
        ),
        
        "session_menu": (
            "<b>Управление сессией</b>\n\n"
            "<b>Текущий статус:</b> {status}\n"
            "{info}"
        ),
        
        "settings_menu": (
            "<b>Настройки</b>\n\n"
            "<b>Никнейм:</b> {nickname}\n"
            "<b>Часовой пояс:</b> UTC{timezone}\n"
            "<b>Текст автоответа:</b> {text}\n"
            "<b>Медиа:</b> {media}"
        ),
        
        "nickname_menu": (
            "<b>Настройки никнейма</b>\n\n"
            "<b>Текущий:</b> {nickname}\n"
            "<b>Формат:</b> {nickname} | ЧЧ:ММ"
        ),
        
        "timezone_menu": (
            "<b>Настройки часового пояса</b>\n\n"
            "<b>Текущий:</b> UTC{timezone}"
        ),
        
        "text_menu": (
            "<b>Настройки текста автоответа</b>\n\n"
            "<b>Текущий:</b> {text}"
        ),
        
        "media_menu": (
            "<b>Настройки медиа</b>\n\n"
            "<b>Статус:</b> {status}"
        ),
        
        "triggers_menu": (
            "<b>Слова-триггеры ({count}/10)</b>\n\n"
            "{triggers}"
        ),
        
        "online_menu": (
            "<b>Управление онлайн статусом</b>\n\n"
            "<b>Текущий режим:</b> {mode}\n"
            "<b>Видимость аккаунта:</b> {visibility}"
        ),
        
        "btn_continue": "Продолжить",
        "btn_session": "Сессия",
        "btn_settings": "Настройки",
        "btn_triggers": "Триггеры",
        "btn_online": "Онлайн",
        "btn_back": "Назад",
        "btn_close": "Закрыть",
        
        "btn_remove_session": "Удалить сессию",
        
        "btn_nickname": "Никнейм",
        "btn_timezone": "Часовой пояс",
        "btn_text": "Текст",
        "btn_media": "Медиа",
        
        "btn_set_nickname": "Установить никнейм",
        "btn_set_timezone": "Установить пояс",
        "btn_set_text": "Установить текст",
        "btn_set_media": "Установить медиа",
        "btn_remove_media": "Удалить медиа",
        
        "btn_add_trigger": "Добавить триггер",
        "btn_clear_triggers": "Очистить все",
        
        "btn_enable_online": "Включить онлайн",
        "btn_disable_online": "Выключить онлайн",
        
        "input_nickname": "Введите шаблон никнейма (без времени):",
        "input_timezone": "Введите смещение часового пояса (от -12 до +12):",
        "input_text": "Введите текст автоответа (поддерживается HTML):",
        "input_media": "Введите URL медиа:",
        "input_trigger": "Введите слова-триггеры (через пробел):",
        
        "status_connected": "Подключено",
        "status_disconnected": "Отключено",
        "status_not_set": "Не установлено",
        "status_enabled": "Включено",
        "status_disabled": "Выключено",
        "status_online": "Онлайн",
        "status_offline": "Оффлайн",
        
        "session_removed": "Сессия удалена",
        "session_invalid": "Неверная сессия: {error}",
        "session_info": "Аккаунт: {name}\nID: {id}",
        "no_session": "<b>Сессия не настроена.</b> Ответьте на StringSession командой <code>.twin</code>",
        
        "nickname_set": "Никнейм установлен: {nickname}",
        "timezone_set": "Часовой пояс установлен: UTC{timezone}",
        "timezone_invalid": "Неверный часовой пояс. Используйте диапазон: от -12 до +12",
        "text_set": "Текст автоответа установлен",
        "media_set": "Медиа успешно установлено",
        "media_removed": "Медиа удалено",
        
        "triggers_added": "Триггеры добавлены: {triggers}",
        "triggers_cleared": "Все триггеры очищены",
        "triggers_max": "Достигнут максимум в 10 триггеров",
        "triggers_list": "{triggers}",
        "triggers_empty": "Триггеры не установлены",
        
        "online_enabling": "<b>Включение режима онлайн...</b>\n\nTwin аккаунт появится в сети",
        "online_disabling": "<b>Выключение режима онлайн...</b>\n\nTwin аккаунт пропадет из сети",
        
        "error_no_session": "Сессия не настроена",
        "error_not_connected": "Сессия не подключена",
    }

    def __init__(self):
        self._twin_client = None
        self._twin_me = None
        self._nick_task = None
        self._online_task = None
        self._connected = False

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        
        if self._db.get(self.name, "session", None):
            asyncio.create_task(self._connect_twin())

    async def on_unload(self):
        await self._disconnect_twin()

    def _get_session(self):
        return self._db.get(self.name, "session", None)

    def _set_session(self, value):
        self._db.set(self.name, "session", value)

    def _get_timezone(self):
        return self._db.get(self.name, "timezone", 3)

    def _set_timezone(self, value):
        self._db.set(self.name, "timezone", value)

    def _get_nick_template(self):
        return self._db.get(self.name, "nick_template", "")

    def _set_nick_template(self, value):
        self._db.set(self.name, "nick_template", value)

    def _get_auto_text(self):
        return self._db.get(self.name, "auto_text", "")

    def _set_auto_text(self, value):
        self._db.set(self.name, "auto_text", value)

    def _get_media_url(self):
        return self._db.get(self.name, "media_url", "")

    def _set_media_url(self, value):
        self._db.set(self.name, "media_url", value)

    def _get_saved_msg_id(self):
        return self._db.get(self.name, "saved_msg_id", 0)

    def _set_saved_msg_id(self, value):
        self._db.set(self.name, "saved_msg_id", value)

    def _get_targets(self):
        return self._db.get(self.name, "targets", [])

    def _set_targets(self, value):
        self._db.set(self.name, "targets", value)

    def _get_keep_online(self):
        return self._db.get(self.name, "keep_online", False)

    def _set_keep_online(self, value):
        self._db.set(self.name, "keep_online", value)

    async def _connect_twin(self):
        session = self._get_session()
        if not session:
            return False

        try:
            self._twin_client = TelegramClient(
                StringSession(session),
                api_id=2040,
                api_hash="b18441a1ff607e10a989891a5462e627"
            )
            await self._twin_client.connect()
            
            if not await self._twin_client.is_user_authorized():
                raise AuthKeyUnregisteredError("Session expired")
            
            self._twin_me = await self._twin_client.get_me()
            self._connected = True
            
            if self._get_nick_template():
                await self._start_nick_loop()
            
            if self._get_keep_online():
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
            self._set_session(None)
            logger.error("[TWIN] Session revoked")
            return False
        except UserDeactivatedBanError:
            self._connected = False
            self._set_session(None)
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
        while self._connected and self._get_nick_template():
            try:
                offset = self._get_timezone()
                tz = timezone(timedelta(hours=offset))
                now = datetime.now(tz)
                time_str = now.strftime("%H:%M")
                
                new_nick = f"{self._get_nick_template()} | {time_str}"
                
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
        while self._connected and self._get_keep_online():
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
        
        targets = self._get_targets()
        auto_text = self._get_auto_text()
        
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
            media_url = self._get_media_url()
            
            if media_url:
                if media_url.startswith("saved:"):
                    msg_id = self._get_saved_msg_id()
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

    def _get_main_markup(self):
        return [
            [
                {"text": self.strings["btn_session"], "callback": self._cb_session_menu, "style": "primary"},
                {"text": self.strings["btn_settings"], "callback": self._cb_settings_menu, "style": "primary"},
            ],
            [
                {"text": self.strings["btn_triggers"], "callback": self._cb_triggers_menu, "style": "primary"},
                {"text": self.strings["btn_online"], "callback": self._cb_online_toggle, "style": "success" if not self._get_keep_online() else "danger"},
            ],
            [
                {"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"},
            ],
        ]

    def _format_main_text(self):
        status = self.strings["status_connected"] if self._connected else self.strings["status_disconnected"]
        account = f"{self._twin_me.first_name} ({self._twin_me.id})" if self._twin_me else self.strings["status_not_set"]
        nickname = self._get_nick_template() or self.strings["status_not_set"]
        
        tz = self._get_timezone()
        tz_str = f"+{tz}" if tz >= 0 else str(tz)
        
        autoreply = self.strings["status_enabled"] if self._get_auto_text() else self.strings["status_disabled"]
        triggers = f"{len(self._get_targets())}/10"
        online = self.strings["status_enabled"] if self._get_keep_online() else self.strings["status_disabled"]
        
        return self.strings["main_menu"].format(
            status=status,
            account=account,
            nickname=nickname,
            timezone=tz_str,
            autoreply=autoreply,
            triggers=triggers,
            online=online
        )

    async def _cb_main_menu(self, call: InlineCall):
        await call.edit(
            self._format_main_text(),
            reply_markup=self._get_main_markup()
        )

    async def _cb_session_menu(self, call: InlineCall):
        status = self.strings["status_connected"] if self._connected else self.strings["status_disconnected"]
        info = ""
        if self._twin_me:
            info = self.strings["session_info"].format(
                name=self._twin_me.first_name,
                id=self._twin_me.id
            )
        
        markup = [
            [
                {"text": self.strings["btn_remove_session"], "callback": self._cb_remove_session, "style": "danger"}
            ],
            [
                {"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}
            ],
        ]
        
        await call.edit(
            self.strings["session_menu"].format(status=status, info=info),
            reply_markup=markup
        )

    async def _cb_remove_session(self, call: InlineCall):
        await self._disconnect_twin()
        self._set_session(None)
        self._set_nick_template("")
        self._set_keep_online(False)
        await call.answer(self.strings["session_removed"], show_alert=True)
        await call.delete()

    async def _cb_settings_menu(self, call: InlineCall):
        nickname = self._get_nick_template() or self.strings["status_not_set"]
        
        tz = self._get_timezone()
        tz_str = f"+{tz}" if tz >= 0 else str(tz)
        
        text = self._get_auto_text()
        text = text[:50] + "..." if len(text) > 50 else (text or self.strings["status_not_set"])
        media = self.strings["status_enabled"] if self._get_media_url() else self.strings["status_disabled"]
        
        markup = [
            [
                {"text": self.strings["btn_nickname"], "callback": self._cb_nickname_menu, "style": "primary"},
            ],
            [
                {"text": self.strings["btn_timezone"], "callback": self._cb_timezone_menu, "style": "primary"},
            ],
            [
                {"text": self.strings["btn_text"], "callback": self._cb_text_menu, "style": "primary"},
            ],
            [
                {"text": self.strings["btn_media"], "callback": self._cb_media_menu, "style": "primary"},
            ],
            [
                {"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}
            ],
        ]
        
        await call.edit(
            self.strings["settings_menu"].format(
                nickname=nickname,
                timezone=tz_str,
                text=text,
                media=media
            ),
            reply_markup=markup
        )

    async def _cb_nickname_menu(self, call: InlineCall):
        nickname = self._get_nick_template() or self.strings["status_not_set"]
        
        markup = [
            [
                {"text": self.strings["btn_set_nickname"], "input": self.strings["input_nickname"], "handler": self._cb_set_nickname, "style": "success"}
            ],
            [
                {"text": self.strings["btn_back"], "callback": self._cb_settings_menu, "style": "danger"}
            ],
        ]
        
        await call.edit(
            self.strings["nickname_menu"].format(nickname=nickname),
            reply_markup=markup
        )

    async def _cb_timezone_menu(self, call: InlineCall):
        tz = self._get_timezone()
        tz_str = f"+{tz}" if tz >= 0 else str(tz)
        
        markup = [
            [
                {"text": self.strings["btn_set_timezone"], "input": self.strings["input_timezone"], "handler": self._cb_set_timezone, "style": "success"}
            ],
            [
                {"text": self.strings["btn_back"], "callback": self._cb_settings_menu, "style": "danger"}
            ],
        ]
        
        await call.edit(
            self.strings["timezone_menu"].format(timezone=tz_str),
            reply_markup=markup
        )

    async def _cb_text_menu(self, call: InlineCall):
        text = self._get_auto_text() or self.strings["status_not_set"]
        
        markup = [
            [
                {"text": self.strings["btn_set_text"], "input": self.strings["input_text"], "handler": self._cb_set_text, "style": "success"}
            ],
            [
                {"text": self.strings["btn_back"], "callback": self._cb_settings_menu, "style": "danger"}
            ],
        ]
        
        await call.edit(
            self.strings["text_menu"].format(text=text),
            reply_markup=markup
        )

    async def _cb_media_menu(self, call: InlineCall):
        status = self.strings["status_enabled"] if self._get_media_url() else self.strings["status_disabled"]
        
        markup = [
            [
                {"text": self.strings["btn_set_media"], "input": self.strings["input_media"], "handler": self._cb_set_media, "style": "success"}
            ],
        ]
        
        if self._get_media_url():
            markup.append([
                {"text": self.strings["btn_remove_media"], "callback": self._cb_remove_media, "style": "danger"}
            ])
        
        markup.append([
            {"text": self.strings["btn_back"], "callback": self._cb_settings_menu, "style": "danger"}
        ])
        
        await call.edit(
            self.strings["media_menu"].format(status=status),
            reply_markup=markup
        )

    async def _cb_set_nickname(self, call: InlineCall, nickname: str):
        self._set_nick_template(nickname.strip())
        
        if self._connected:
            await self._stop_nick_loop()
            if nickname.strip():
                await self._start_nick_loop()
        
        await call.answer(self.strings["nickname_set"].format(nickname=nickname.strip()), show_alert=True)
        await self._cb_nickname_menu(call)

    async def _cb_set_timezone(self, call: InlineCall, timezone: str):
        try:
            tz = int(timezone.replace("+", ""))
            if not -12 <= tz <= 12:
                raise ValueError
            
            self._set_timezone(tz)
            tz_str = f"+{tz}" if tz >= 0 else str(tz)
            await call.answer(self.strings["timezone_set"].format(timezone=tz_str), show_alert=True)
            await self._cb_timezone_menu(call)
        except ValueError:
            await call.answer(self.strings["timezone_invalid"], show_alert=True)

    async def _cb_set_text(self, call: InlineCall, text: str):
        self._set_auto_text(text.strip())
        await call.answer(self.strings["text_set"], show_alert=True)
        await self._cb_text_menu(call)

    async def _cb_set_media(self, call: InlineCall, url: str):
        old_msg_id = self._get_saved_msg_id()
        if old_msg_id:
            try:
                old_msg = await self._client.get_messages("me", ids=old_msg_id)
                if old_msg:
                    await old_msg.delete()
            except:
                pass
        
        self._set_saved_msg_id(0)
        self._set_media_url(url.strip())
        
        await call.answer(self.strings["media_set"], show_alert=True)
        await self._cb_media_menu(call)

    async def _cb_remove_media(self, call: InlineCall):
        old_msg_id = self._get_saved_msg_id()
        if old_msg_id:
            try:
                old_msg = await self._client.get_messages("me", ids=old_msg_id)
                if old_msg:
                    await old_msg.delete()
            except:
                pass
        
        self._set_saved_msg_id(0)
        self._set_media_url("")
        
        await call.answer(self.strings["media_removed"], show_alert=True)
        await self._cb_media_menu(call)

    async def _cb_triggers_menu(self, call: InlineCall):
        targets = self._get_targets()
        count = len(targets)
        
        if targets:
            triggers_text = self.strings["triggers_list"].format(triggers=", ".join(targets))
        else:
            triggers_text = self.strings["triggers_empty"]
        
        markup = []
        
        if count < 10:
            markup.append([
                {"text": self.strings["btn_add_trigger"], "input": self.strings["input_trigger"], "handler": self._cb_add_trigger, "style": "success"}
            ])
        
        if targets:
            markup.append([
                {"text": self.strings["btn_clear_triggers"], "callback": self._cb_clear_triggers, "style": "danger"}
            ])
        
        markup.append([
            {"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}
        ])
        
        await call.edit(
            self.strings["triggers_menu"].format(count=count, triggers=triggers_text),
            reply_markup=markup
        )

    async def _cb_add_trigger(self, call: InlineCall, triggers: str):
        new_triggers = triggers.strip().split()
        current = self._get_targets()
        
        available = 10 - len(current)
        if available <= 0:
            await call.answer(self.strings["triggers_max"], show_alert=True)
            return
        
        added = []
        for t in new_triggers[:available]:
            if t.lower() not in [x.lower() for x in current]:
                current.append(t)
                added.append(t)
        
        self._set_targets(current)
        
        await call.answer(
            self.strings["triggers_added"].format(triggers=", ".join(added) if added else "none"),
            show_alert=True
        )
        await self._cb_triggers_menu(call)

    async def _cb_clear_triggers(self, call: InlineCall):
        self._set_targets([])
        await call.answer(self.strings["triggers_cleared"], show_alert=True)
        await self._cb_triggers_menu(call)

    async def _cb_online_toggle(self, call: InlineCall):
        if not self._connected:
            await call.answer(self.strings["error_not_connected"], show_alert=True)
            return
        
        is_online = self._get_keep_online()
        
        if is_online:
            await call.edit(self.strings["online_disabling"])
            self._set_keep_online(False)
            await self._stop_online_loop()
            await self._twin_client(UpdateStatusRequest(offline=True))
            await asyncio.sleep(1)
            await call.edit(
                text=self._format_main_text(),
                reply_markup=self._get_main_markup()
            )
        else:
            await call.edit(self.strings["online_enabling"])
            await self._twin_client(UpdateStatusRequest(offline=False))
            self._set_keep_online(True)
            await self._start_online_loop()
            await asyncio.sleep(1)
            await call.edit(
                text=self._format_main_text(),
                reply_markup=self._get_main_markup()
            )

    async def _cb_close(self, call: InlineCall):
        await call.delete()

    @loader.command()
    async def twin(self, message: Message):
        """Twin account management"""
        reply = await message.get_reply_message()
        
        if reply and reply.text:
            match = STRING_SESSION_PATTERN.search(reply.text)
            if match:
                session_str = match.group(0)
                self._set_session(session_str)
                
                success = await self._connect_twin()
                
                if success:
                    await self.inline.form(
                        text=self.strings["session_success"].format(
                            name=self._twin_me.first_name,
                            id=self._twin_me.id
                        ),
                        message=message,
                        reply_markup=[
                            [
                                {"text": self.strings["btn_continue"], "callback": self._cb_main_menu, "style": "success"}
                            ],
                        ],
                        silent=True,
                    )
                    return
                else:
                    self._set_session(None)
                    await utils.answer(
                        message,
                        self.strings["session_invalid"].format(error="Connection failed")
                    )
                    return
        
        if not self._get_session():
            await utils.answer(message, self.strings["no_session"])
            return
        
        await self.inline.form(
            text=self._format_main_text(),
            message=message,
            reply_markup=self._get_main_markup(),
            silent=True,
        )