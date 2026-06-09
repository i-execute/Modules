__version__ = (1, 2, 0)
# meta developer: I_execute.t.me
# meta banner: https://raw.githubusercontent.com/i-execute/Modules/main/Storage/VKCalls/MetaBanner.jpeg

import os
import re
import sys
import typing
import logging

from .. import loader, utils
from ..inline.types import InlineCall

logger = logging.getLogger(__name__)

DEPS = ["aiohttp"]

def _install_deps():
    import importlib
    import subprocess

    pip = os.path.join(os.path.dirname(sys.executable), "pip")
    if not os.path.exists(pip):
        pip = "pip"

    in_venv = sys.prefix != getattr(sys, "base_prefix", sys.prefix)
    lines = [f"venv: {'yes' if in_venv else 'no'} ({sys.prefix})"]

    for pkg in DEPS:
        try:
            subprocess.run(
                [pip, "install", "-U", pkg, "--break-system-packages", "-q"],
                capture_output=True,
                text=True,
                timeout=120,
            )
            try:
                mod = importlib.import_module(pkg)
                ver = getattr(mod, "__version__", "?")
                lines.append(f"{pkg}: OK ({ver})")
            except Exception:
                lines.append(f"{pkg}: FAIL (import error)")
        except Exception as e:
            lines.append(f"{pkg}: FAIL ({e})")

    return lines

_dep_log = _install_deps()

try:
    import aiohttp
    AIOHTTP_OK = True
except Exception:
    aiohttp = None
    AIOHTTP_OK = False

VK_API_VERSION = "5.199"
VK_API_BASE = "https://api.vk.ru/method"
VK_DEFAULT_APP_ID = 2685278
VK_REDIRECT = "https://oauth.vk.com/blank.html"
VK_DEFAULT_SCOPE = "offline"
VK_TOKEN_RE = re.compile(r"access_token=([A-Za-z0-9._-]+)")


def extract_vk_token(text: str) -> typing.Optional[str]:
    if not text:
        return None
    m = VK_TOKEN_RE.search(text)
    return m.group(1) if m else None


def build_vk_auth_url(app_id: int, scope: str, v: str) -> str:
    scope = (scope or "").strip()
    return (
        "https://oauth.vk.com/authorize"
        f"?client_id={int(app_id)}"
        f"&display=page"
        f"&redirect_uri={VK_REDIRECT}"
        f"&scope={scope}"
        f"&response_type=token"
        f"&v={v}"
    )


class VKAPIError(Exception):
    def __init__(self, error: dict):
        self.error = error or {}
        code = self.error.get("error_code")
        msg = self.error.get("error_msg")
        super().__init__(f"VK error {code}: {msg}")


class VKCallsAPIClient:
    def __init__(self):
        self._token: typing.Optional[str] = None
        self._session: typing.Optional["aiohttp.ClientSession"] = None
        self._user_id: typing.Optional[int] = None

    @property
    def ok(self) -> bool:
        return bool(self._token and self._user_id)

    @property
    def user_id(self) -> typing.Optional[int]:
        return self._user_id

    def reset(self):
        self._token = None
        self._user_id = None

    async def _get_session(self):
        if not AIOHTTP_OK:
            return None
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30)
            )
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
        self._session = None

    async def _post(self, method: str, params: dict) -> dict:
        s = await self._get_session()
        if not s:
            raise RuntimeError("Session error")
        async with s.post(f"{VK_API_BASE}/{method}", data=params) as r:
            return await r.json(content_type=None)

    async def api(self, method: str, **params) -> typing.Any:
        if not AIOHTTP_OK:
            raise RuntimeError("aiohttp is not available")
        if not self._token:
            raise RuntimeError("No VK token")

        params = dict(params)
        params["access_token"] = self._token
        params["v"] = VK_API_VERSION

        data = await self._post(method, params)
        if isinstance(data, dict) and "error" in data:
            raise VKAPIError(data["error"] or {})

        return data.get("response")

    async def auth(self, token: str) -> bool:
        token = (token or "").strip()
        if not token:
            self.reset()
            return False

        self._token = token
        try:
            resp = await self.api("users.get")
            if isinstance(resp, list) and resp and isinstance(resp[0], dict):
                self._user_id = int(resp[0].get("id") or 0) or None
            return bool(self._user_id)
        except Exception:
            self.reset()
            return False

    async def start_call(self, group_id: typing.Optional[int] = None) -> dict:
        params = {}
        if group_id:
            params["group_id"] = int(group_id)
        resp = await self.api("calls.start", **params)
        return resp if isinstance(resp, dict) else {}

    async def force_finish(self, call_id: str) -> bool:
        call_id = (call_id or "").strip()
        if not call_id:
            return False
        resp = await self.api("calls.forceFinish", call_id=call_id)
        return bool(resp)


@loader.tds
class VKCalls(loader.Module):
    """Create VK calls and get join links"""

    strings = {
        "name": "VKCalls",
        "main_menu": (
            "<b>VK Calls Manager</b>\n"
            "<blockquote>Select action:</blockquote>"
        ),
        "btn_create": "Create Call",
        "btn_active": "Active Calls",
        "btn_auth": "Authorization",
        "btn_back": "Back",
        "btn_finish": "Finish",
        "auth_menu": (
            "<b>VK Authorization</b>\n"
            "<blockquote>"
            "1) Open the link below\n"
            "2) Allow access\n"
            "3) Copy full URL from address bar\n"
            "4) Paste it in input field"
            "</blockquote>"
        ),
        "auth_input": "Paste authorization URL:",
        "auth_success": (
            "<b>Authorization successful</b>\n"
            "<blockquote>User ID: <code>{user_id}</code></blockquote>"
        ),
        "auth_failed": (
            "<b>Authorization failed</b>\n"
            "<blockquote>{error}</blockquote>"
        ),
        "auth_invalid": "<b>Invalid token format</b>",
        "not_authorized": (
            "<b>Not authorized</b>\n"
            "<blockquote>Please complete VK authorization first</blockquote>"
        ),
        "call_created": (
            "<b>Call Created</b>\n"
            "<blockquote>"
            "Call ID: <code>{call_id}</code>\n"
            "Join Link: <code>{join_link}</code>\n"
            "Short ID: <code>{short_id}</code>\n"
            "Short Password: <code>{short_password}</code>\n"
            "Short Link: <code>{short_link}</code>"
            "</blockquote>"
        ),
        "call_create_failed": (
            "<b>Failed to create call</b>\n"
            "<blockquote>{error}</blockquote>"
        ),
        "active_calls": (
            "<b>Active Calls</b>\n"
            "<blockquote>Total: {count}</blockquote>"
        ),
        "no_active_calls": (
            "<b>No active calls</b>\n"
            "<blockquote>Create a new call to get started</blockquote>"
        ),
        "call_info": (
            "<b>Call Info</b>\n"
            "<blockquote>"
            "Call ID: <code>{call_id}</code>\n"
            "Join Link: <code>{join_link}</code>\n"
            "Short ID: <code>{short_id}</code>\n"
            "Status: {status}"
            "</blockquote>"
        ),
        "call_finished": (
            "<b>Call finished</b>\n"
            "<blockquote>Call ID: <code>{call_id}</code></blockquote>"
        ),
        "call_finish_failed": (
            "<b>Failed to finish call</b>\n"
            "<blockquote>{error}</blockquote>"
        ),
        "loading": "<b>Loading...</b>",
    }

    strings_ru = {
        "main_menu": (
            "<b>Менеджер VK Звонков</b>\n"
            "<blockquote>Выберите действие:</blockquote>"
        ),
        "btn_create": "Создать звонок",
        "btn_active": "Активные звонки",
        "btn_auth": "Авторизация",
        "btn_back": "Назад",
        "btn_finish": "Завершить",
        "auth_menu": (
            "<b>Авторизация VK</b>\n"
            "<blockquote>"
            "1) Откройте ссылку ниже\n"
            "2) Разрешите доступ\n"
            "3) Скопируйте полный URL из адресной строки\n"
            "4) Вставьте его в поле ввода"
            "</blockquote>"
        ),
        "auth_input": "Вставьте URL авторизации:",
        "auth_success": (
            "<b>Авторизация успешна</b>\n"
            "<blockquote>User ID: <code>{user_id}</code></blockquote>"
        ),
        "auth_failed": (
            "<b>Ошибка авторизации</b>\n"
            "<blockquote>{error}</blockquote>"
        ),
        "auth_invalid": "<b>Неверный формат токена</b>",
        "not_authorized": (
            "<b>Не авторизован</b>\n"
            "<blockquote>Пожалуйста, сначала выполните авторизацию VK</blockquote>"
        ),
        "call_created": (
            "<b>Звонок создан</b>\n"
            "<blockquote>"
            "Call ID: <code>{call_id}</code>\n"
            "Ссылка: <code>{join_link}</code>\n"
            "Короткий ID: <code>{short_id}</code>\n"
            "Пароль: <code>{short_password}</code>\n"
            "Короткая ссылка: <code>{short_link}</code>"
            "</blockquote>"
        ),
        "call_create_failed": (
            "<b>Не удалось создать звонок</b>\n"
            "<blockquote>{error}</blockquote>"
        ),
        "active_calls": (
            "<b>Активные звонки</b>\n"
            "<blockquote>Всего: {count}</blockquote>"
        ),
        "no_active_calls": (
            "<b>Нет активных звонков</b>\n"
            "<blockquote>Создайте новый звонок для начала</blockquote>"
        ),
        "call_info": (
            "<b>Информация о звонке</b>\n"
            "<blockquote>"
            "Call ID: <code>{call_id}</code>\n"
            "Ссылка: <code>{join_link}</code>\n"
            "Короткий ID: <code>{short_id}</code>\n"
            "Статус: {status}"
            "</blockquote>"
        ),
        "call_finished": (
            "<b>Звонок завершён</b>\n"
            "<blockquote>Call ID: <code>{call_id}</code></blockquote>"
        ),
        "call_finish_failed": (
            "<b>Не удалось завершить звонок</b>\n"
            "<blockquote>{error}</blockquote>"
        ),
        "loading": "<b>Загрузка...</b>",
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "VK_TOKEN",
                "",
                "VK user access_token",
                validator=loader.validators.Hidden(),
            ),
            loader.ConfigValue(
                "VK_APP_ID",
                VK_DEFAULT_APP_ID,
                "VK OAuth app_id",
                validator=loader.validators.Integer(minimum=1),
            ),
            loader.ConfigValue(
                "VK_SCOPE",
                VK_DEFAULT_SCOPE,
                "OAuth scope string",
                validator=loader.validators.String(),
            ),
        )
        self._vk = VKCallsAPIClient()
        self._db = None

    def _get_calls(self) -> dict:
        return self._db.get("VKCalls", "calls", {})

    def _set_calls(self, calls: dict):
        self._db.set("VKCalls", "calls", calls)

    def _add_call(self, call_id: str, data: dict):
        calls = self._get_calls()
        calls[call_id] = data
        self._set_calls(calls)

    def _remove_call(self, call_id: str):
        calls = self._get_calls()
        calls.pop(call_id, None)
        self._set_calls(calls)

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        await self._ensure_vk()

    async def on_unload(self):
        await self._vk.close()

    async def _ensure_vk(self) -> bool:
        token = (self.config["VK_TOKEN"] or "").strip()
        if not token:
            self._vk.reset()
            return False
        if self._vk.ok and self._vk._token == token:
            return True
        return await self._vk.auth(token)

    async def _cb_main_menu(self, call: InlineCall):
        await call.edit(
            self.strings["main_menu"],
            reply_markup=[
                [{"text": self.strings["btn_create"], "callback": self._cb_create_call, "style": "success"}],
                [{"text": self.strings["btn_active"], "callback": self._cb_active_calls, "style": "primary"}],
                [{"text": self.strings["btn_auth"],   "callback": self._cb_auth_menu,   "style": "primary"}],
            ],
        )

    async def _cb_auth_menu(self, call: InlineCall):
        url = build_vk_auth_url(
            app_id=int(self.config["VK_APP_ID"]),
            scope=str(self.config["VK_SCOPE"]),
            v=VK_API_VERSION,
        )
        await call.edit(
            self.strings["auth_menu"],
            reply_markup=[
                [{"text": "Open VK Auth", "url": url}],
                [{"text": self.strings["auth_input"], "input": self.strings["auth_input"], "handler": self._input_auth, "style": "success"}],
                [{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "primary"}],
            ],
        )

    async def _input_auth(self, call: InlineCall, auth_url: str):
        token = extract_vk_token(auth_url)
        if not token:
            if not auth_url.startswith("http") and len(auth_url) > 20:
                token = auth_url
            else:
                await call.edit(self.strings["auth_invalid"])
                return

        ok = await self._vk.auth(token)
        if ok:
            self.config["VK_TOKEN"] = token
            await call.edit(
                self.strings["auth_success"].format(user_id=self._vk.user_id or "?"),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "primary"}]],
            )
        else:
            await call.edit(
                self.strings["auth_failed"].format(error="users.get failed"),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "primary"}]],
            )

    async def _cb_create_call(self, call: InlineCall):
        if not await self._ensure_vk():
            await call.edit(
                self.strings["not_authorized"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "primary"}]],
            )
            return

        await call.edit(self.strings["loading"])

        try:
            resp = await self._vk.start_call()
            call_id   = (resp.get("call_id")   or "").strip()
            join_link = (resp.get("join_link")  or "").strip()

            sc             = resp.get("short_credentials") if isinstance(resp, dict) else None
            short_id       = sc.get("id",                "N/A") if sc else "N/A"
            short_password = sc.get("password",          "N/A") if sc else "N/A"
            short_link     = sc.get("link_with_password","N/A") if sc else "N/A"

            if call_id:
                self._add_call(call_id, {
                    "call_id":           call_id,
                    "join_link":         join_link,
                    "short_credentials": sc,
                })

            await call.edit(
                self.strings["call_created"].format(
                    call_id=call_id or "?",
                    join_link=join_link or "?",
                    short_id=short_id,
                    short_password=short_password,
                    short_link=short_link,
                ),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "primary"}]],
            )
        except VKAPIError as e:
            code = e.error.get("error_code")
            msg  = e.error.get("error_msg")
            await call.edit(
                self.strings["call_create_failed"].format(error=f"{code}: {msg}"),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "primary"}]],
            )
        except Exception as e:
            await call.edit(
                self.strings["call_create_failed"].format(error=str(e)[:200]),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "primary"}]],
            )

    async def _cb_active_calls(self, call: InlineCall):
        # ── читаем из БД, никакого calls.get ──
        calls_dict = self._get_calls()
        calls      = list(calls_dict.values())

        if not calls:
            await call.edit(
                self.strings["no_active_calls"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "primary"}]],
            )
            return

        buttons = []
        for c in calls:
            call_id  = c.get("call_id", "")
            short_id = ""
            if isinstance(c.get("short_credentials"), dict):
                short_id = c["short_credentials"].get("id", "")
            label = short_id if short_id else call_id[:8]
            buttons.append([
                {"text": label, "callback": self._cb_call_detail, "args": (call_id,), "style": "primary"}
            ])

        buttons.append([{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "primary"}])

        await call.edit(
            self.strings["active_calls"].format(count=len(calls)),
            reply_markup=buttons,
        )

    async def _cb_call_detail(self, call: InlineCall, call_id: str):
        calls_dict = self._get_calls()
        call_data  = calls_dict.get(call_id)

        if not call_data:
            await call.edit(
                "<b>Call not found</b>",
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_active_calls, "style": "primary"}]],
            )
            return

        join_link = call_data.get("join_link", "N/A")
        short_id  = "N/A"
        if isinstance(call_data.get("short_credentials"), dict):
            short_id = call_data["short_credentials"].get("id", "N/A")

        await call.edit(
            self.strings["call_info"].format(
                call_id=call_id,
                join_link=join_link,
                short_id=short_id,
                status="active",
            ),
            reply_markup=[
                [{"text": self.strings["btn_finish"], "callback": self._cb_finish_call, "args": (call_id,), "style": "danger"}],
                [{"text": self.strings["btn_back"],   "callback": self._cb_active_calls, "style": "primary"}],
            ],
        )

    async def _cb_finish_call(self, call: InlineCall, call_id: str):
        if not await self._ensure_vk():
            await call.edit(
                self.strings["not_authorized"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "primary"}]],
            )
            return

        await call.edit(self.strings["loading"])

        try:
            ok = await self._vk.force_finish(call_id)
            if ok:
                self._remove_call(call_id)   
                await call.edit(
                    self.strings["call_finished"].format(call_id=call_id),
                    reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_active_calls, "style": "primary"}]],
                )
            else:
                await call.edit(
                    self.strings["call_finish_failed"].format(error="VK returned false"),
                    reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_active_calls, "style": "primary"}]],
                )
        except VKAPIError as e:
            code = e.error.get("error_code")
            msg  = e.error.get("error_msg")
            await call.edit(
                self.strings["call_finish_failed"].format(error=f"{code}: {msg}"),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_active_calls, "style": "primary"}]],
            )
        except Exception as e:
            await call.edit(
                self.strings["call_finish_failed"].format(error=str(e)[:200]),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_active_calls, "style": "primary"}]],
            )

    @loader.command(ru_doc="Менеджер VK звонков", en_doc="VK calls manager")
    async def vkcall(self, message):
        """VK calls manager"""
        await self.inline.form(
            text=self.strings["main_menu"],
            message=message,
            reply_markup=[
                [{"text": self.strings["btn_create"], "callback": self._cb_create_call, "style": "success"}],
                [{"text": self.strings["btn_active"], "callback": self._cb_active_calls, "style": "primary"}],
                [{"text": self.strings["btn_auth"],   "callback": self._cb_auth_menu,   "style": "primary"}],
            ],
            silent=True,
        )