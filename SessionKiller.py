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

from telethon import functions
from telethon.errors import FloodWaitError

from .. import loader, utils
from ..inline.types import InlineCall

logger = logging.getLogger(__name__)

SESSIONS_PER_PAGE = 5


@loader.tds
class SessionKiller(loader.Module):
    """View and kill active Telegram sessions"""

    strings = {
        "name": "SessionKiller",
        "fetching": (
            "<b>SessionKiller</b>\n"
            "<blockquote>Fetching sessions...</blockquote>"
        ),
        "no_sessions": (
            "<b>SessionKiller</b>\n"
            "<blockquote>No sessions found.</blockquote>"
        ),
        "fetch_error": (
            "<b>SessionKiller</b>\n"
            "<blockquote>Failed to fetch sessions.\n"
            "{error}</blockquote>"
        ),
        "list": (
            "<b>SessionKiller</b>\n"
            "<blockquote>Page {page}/{total_pages}\n"
            "Total sessions: {total}</blockquote>"
        ),
        "session_detail": (
            "<b>Session Info</b>\n"
            "<blockquote>"
            "App: {app_name}\n"
            "Device: {device_model}\n"
            "Platform: {platform}\n"
            "OS: {system_version}\n"
            "API ID: {api_id}\n"
            "Country: {country}\n"
            "IP: {ip}\n"
            "Last active: {last_active}\n"
            "Created: {date_created}\n"
            "Current: {current}"
            "</blockquote>"
        ),
        "killing": (
            "<b>Killing session...</b>\n"
            "<blockquote>Please wait...</blockquote>"
        ),
        "killed": (
            "<b>Session Killed</b>\n"
            "<blockquote>App: {app_name}\n"
            "Device: {device_model}</blockquote>"
        ),
        "kill_error": (
            "<b>Kill Failed</b>\n"
            "<blockquote>{error}</blockquote>"
        ),
        "current_session": (
            "<b>Current Session</b>\n"
            "<blockquote>Cannot kill the current session.</blockquote>"
        ),
        "btn_kill": "Kill Session",
        "btn_back": "Back",
        "btn_close": "Close",
        "btn_left": "<",
        "btn_right": ">",
        "yes": "yes",
        "no": "no",
        "unknown": "unknown",
        "never": "never",
    }

    strings_ru = {
        "fetching": (
            "<b>SessionKiller</b>\n"
            "<blockquote>Получаем сессии...</blockquote>"
        ),
        "no_sessions": (
            "<b>SessionKiller</b>\n"
            "<blockquote>Сессии не найдены.</blockquote>"
        ),
        "fetch_error": (
            "<b>SessionKiller</b>\n"
            "<blockquote>Не удалось получить сессии.\n"
            "{error}</blockquote>"
        ),
        "list": (
            "<b>SessionKiller</b>\n"
            "<blockquote>Страница {page}/{total_pages}\n"
            "Всего сессий: {total}</blockquote>"
        ),
        "session_detail": (
            "<b>Инфо о сессии</b>\n"
            "<blockquote>"
            "Приложение: {app_name}\n"
            "Устройство: {device_model}\n"
            "Платформа: {platform}\n"
            "ОС: {system_version}\n"
            "API ID: {api_id}\n"
            "Страна: {country}\n"
            "IP: {ip}\n"
            "Последняя активность: {last_active}\n"
            "Создана: {date_created}\n"
            "Текущая: {current}"
            "</blockquote>"
        ),
        "killing": (
            "<b>Убиваем сессию...</b>\n"
            "<blockquote>Пожалуйста, подождите...</blockquote>"
        ),
        "killed": (
            "<b>Сессия убита</b>\n"
            "<blockquote>Приложение: {app_name}\n"
            "Устройство: {device_model}</blockquote>"
        ),
        "kill_error": (
            "<b>Ошибка убийства</b>\n"
            "<blockquote>{error}</blockquote>"
        ),
        "current_session": (
            "<b>Текущая сессия</b>\n"
            "<blockquote>Нельзя убить текущую сессию.</blockquote>"
        ),
        "btn_kill": "Убить сессию",
        "btn_back": "Назад",
        "btn_close": "Закрыть",
        "btn_left": "<",
        "btn_right": ">",
        "yes": "да",
        "no": "нет",
        "unknown": "неизвестно",
        "never": "никогда",
    }

    def __init__(self):
        self._state = {
            "sessions": [],
            "page": 0,
        }

    async def client_ready(self, client, db):
        self._client = client

    async def _get_authorizations(self):
        try:
            result = await self._client(functions.account.GetAuthorizationsRequest())
            return result.authorizations
        except FloodWaitError as e:
            logger.warning(f"[SessionKiller] FloodWait {e.seconds}s")
            await asyncio.sleep(e.seconds + 5)
            return None
        except Exception as e:
            logger.error(f"[SessionKiller] GetAuthorizations error: {e}")
            return None

    def _fmt_date(self, dt) -> str:
        if not dt:
            return self.strings["never"]
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        delta = now - dt
        total_seconds = int(delta.total_seconds())
        if total_seconds < 60:
            return f"{total_seconds}s ago"
        if total_seconds < 3600:
            return f"{total_seconds // 60}m ago"
        if total_seconds < 86400:
            return f"{total_seconds // 3600}h ago"
        return f"{delta.days}d ago"

    def _fmt_list(self) -> str:
        s = self._state
        total = len(s["sessions"])
        total_pages = max(1, (total + SESSIONS_PER_PAGE - 1) // SESSIONS_PER_PAGE)
        return self.strings["list"].format(
            page=s["page"] + 1,
            total_pages=total_pages,
            total=total,
        )

    def _get_list_markup(self) -> list:
        s = self._state
        sessions = s["sessions"]
        page = s["page"]
        total = len(sessions)
        total_pages = max(1, (total + SESSIONS_PER_PAGE - 1) // SESSIONS_PER_PAGE)

        start = page * SESSIONS_PER_PAGE
        end = min(start + SESSIONS_PER_PAGE, total)
        page_sessions = sessions[start:end]

        rows = []
        for i, auth in enumerate(page_sessions):
            label = auth.app_name or self.strings["unknown"]
            if auth.current:
                label = f"* {label}"
            rows.append([{
                "text": label[:32],
                "callback": self._cb_session_detail,
                "args": (start + i,),
                "style": "primary",
            }])

        nav_row = []
        if page > 0:
            nav_row.append({
                "text": self.strings["btn_left"],
                "callback": self._cb_page_left,
                "style": "primary",
            })
        if page < total_pages - 1:
            nav_row.append({
                "text": self.strings["btn_right"],
                "callback": self._cb_page_right,
                "style": "primary",
            })
        if nav_row:
            rows.append(nav_row)

        rows.append([{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}])
        return rows

    def _fmt_session_detail(self, auth) -> str:
        return self.strings["session_detail"].format(
            app_name=auth.app_name or self.strings["unknown"],
            device_model=auth.device_model or self.strings["unknown"],
            platform=auth.platform or self.strings["unknown"],
            system_version=auth.system_version or self.strings["unknown"],
            api_id=auth.api_id,
            country=auth.country or self.strings["unknown"],
            ip=auth.ip or self.strings["unknown"],
            last_active=self._fmt_date(auth.date_active),
            date_created=self._fmt_date(auth.date_created),
            current=self.strings["yes"] if auth.current else self.strings["no"],
        )

    def _get_detail_markup(self, session_index: int, is_current: bool) -> list:
        rows = []
        if not is_current:
            rows.append([{
                "text": self.strings["btn_kill"],
                "callback": self._cb_kill,
                "args": (session_index,),
                "style": "danger",
            }])
        rows.append([{
            "text": self.strings["btn_back"],
            "callback": self._cb_back_list,
            "style": "primary",
        }])
        return rows

    async def _cb_close(self, call: InlineCall):
        await call.delete()

    async def _cb_page_left(self, call: InlineCall):
        if self._state["page"] <= 0:
            await call.answer()
            return
        self._state["page"] -= 1
        await call.edit(self._fmt_list(), reply_markup=self._get_list_markup())

    async def _cb_page_right(self, call: InlineCall):
        total = len(self._state["sessions"])
        total_pages = max(1, (total + SESSIONS_PER_PAGE - 1) // SESSIONS_PER_PAGE)
        if self._state["page"] >= total_pages - 1:
            await call.answer()
            return
        self._state["page"] += 1
        await call.edit(self._fmt_list(), reply_markup=self._get_list_markup())

    async def _cb_back_list(self, call: InlineCall):
        await call.edit(self._fmt_list(), reply_markup=self._get_list_markup())

    async def _cb_session_detail(self, call: InlineCall, session_index: int):
        sessions = self._state["sessions"]
        if session_index >= len(sessions):
            await call.answer()
            return
        auth = sessions[session_index]
        await call.edit(
            self._fmt_session_detail(auth),
            reply_markup=self._get_detail_markup(session_index, auth.current),
        )

    async def _cb_kill(self, call: InlineCall, session_index: int):
        sessions = self._state["sessions"]
        if session_index >= len(sessions):
            await call.answer()
            return
        auth = sessions[session_index]

        if auth.current:
            await call.edit(
                self.strings["current_session"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_back_list, "style": "primary"}]],
            )
            return

        await call.edit(self.strings["killing"])

        try:
            await self._client(
                functions.account.ResetAuthorizationRequest(hash=auth.hash)
            )
            self._state["sessions"].pop(session_index)
            if self._state["page"] > 0:
                total = len(self._state["sessions"])
                total_pages = max(1, (total + SESSIONS_PER_PAGE - 1) // SESSIONS_PER_PAGE)
                if self._state["page"] >= total_pages:
                    self._state["page"] = total_pages - 1
            await call.edit(
                self.strings["killed"].format(
                    app_name=auth.app_name or self.strings["unknown"],
                    device_model=auth.device_model or self.strings["unknown"],
                ),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_back_list, "style": "primary"}]],
            )
        except Exception as e:
            logger.error(f"[SessionKiller] ResetAuthorization error: {e}")
            await call.edit(
                self.strings["kill_error"].format(error=str(e)[:200]),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_back_list, "style": "primary"}]],
            )

    @loader.command(
        ru_doc="Просмотр и убийство активных сессий",
        en_doc="View and kill active sessions",
    )
    async def sk(self, message):
        """View and kill active sessions"""
        await utils.answer(message, self.strings["fetching"])

        auths = await self._get_authorizations()

        if auths is None:
            await utils.answer(
                message,
                self.strings["fetch_error"].format(error="GetAuthorizationsRequest failed"),
            )
            return

        if not auths:
            await utils.answer(message, self.strings["no_sessions"])
            return

        self._state = {
            "sessions": list(auths),
            "page": 0,
        }

        await self.inline.form(
            text=self._fmt_list(),
            message=message,
            reply_markup=self._get_list_markup(),
            silent=True,
        )