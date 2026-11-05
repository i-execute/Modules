# CopyLeft 2026 github.com/i-execute
# Author: I_execute.t.me
# Licensed under AGPLv3.

__version__ = (1, 0, 0)
# meta developer: Execute_forge.t.me

import asyncio
import contextlib
import json
import logging
from datetime import datetime, timezone, timedelta

from .. import loader, utils
from ..inline.types import InlineCall

logger = logging.getLogger(__name__)

API = "https://api.github.com"


def _escape(text) -> str:
    if not text:
        return ""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _now_str(tz_offset: int) -> str:
    tz = timezone(timedelta(hours=tz_offset))
    now = datetime.now(tz)
    return now.strftime("%Y-%m-%d %H:%M:%S")


def _cfg_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n").replace("\r", "\\r")


async def _gh(method: str, path: str, token: str, timeout: int) -> dict:
    url = f"{API}{path}"
    cfg = [
        f'url = "{_cfg_escape(url)}"',
        f'request = "{method}"',
        f'header = "Authorization: Bearer {_cfg_escape(token)}"',
        'header = "Accept: application/vnd.github+json"',
        "silent",
        "show-error",
        "location",
        "compressed",
        "connect-timeout = 15",
        f"max-time = {int(timeout)}",
        'write-out = "\\n%{http_code}"',
    ]
    try:
        proc = await asyncio.create_subprocess_exec(
            "curl", "-K", "-",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except Exception as e:
        return {"ok": False, "status": 0, "data": {}, "error": str(e)[:200]}
    try:
        out, err = await asyncio.wait_for(proc.communicate("\n".join(cfg).encode()), timeout + 10)
    except asyncio.TimeoutError:
        with contextlib.suppress(Exception):
            proc.kill()
        return {"ok": False, "status": 0, "data": {}, "error": "timeout"}
    except Exception as e:
        return {"ok": False, "status": 0, "data": {}, "error": str(e)[:200]}

    text = out.decode("utf-8", errors="replace")
    payload, _, code = text.rpartition("\n")
    try:
        status = int(code.strip())
    except ValueError:
        status = 0
    if status == 0:
        msg = err.decode("utf-8", errors="replace").strip() or "no response"
        return {"ok": False, "status": 0, "data": {}, "error": msg[:200]}
    try:
        data = json.loads(payload) if payload.strip() else {}
    except Exception:
        data = {"message": payload.strip()[:200]}
    return {"ok": status < 400, "status": status, "data": data}


def _fmt_reset(ts, tz_offset: int) -> str:
    if not ts:
        return "n/a"
    reset = datetime.fromtimestamp(ts, tz=timezone(timedelta(hours=tz_offset)))
    left = int(ts - datetime.now(tz=timezone.utc).timestamp())
    if left <= 0:
        return "now"
    h, rem = divmod(left, 3600)
    m = rem // 60
    return f"{reset.strftime('%d.%m %H:%M')} ({h}h {m}m)"


def _fmt_bytes(value) -> str:
    try:
        value = float(value or 0)
    except Exception:
        return "0"
    for size, name in ((1024 ** 3, "GB"), (1024 ** 2, "MB"), (1024, "KB")):
        if value >= size:
            return f"{value / size:.1f} {name}"
    return f"{int(value)} B"


def _quota_line(res: dict) -> tuple:
    limit = res.get("limit", 0)
    remaining = res.get("remaining", 0)
    used = res.get("used", 0)
    if limit <= 0:
        return "n/a", "secondary"
    pct = int(round(used * 100 / limit))
    if pct >= 90:
        return f"{used}/{limit} ({pct}%)", "danger"
    if pct >= 70:
        return f"{used}/{limit} ({pct}%)", "warning"
    return f"{used}/{limit} ({pct}%)", "ok"


@loader.tds
class GitHubMonitor(loader.Module):
    """GitHub account monitor"""

    strings = {
        "name": "GitHubMonitor",
        "no_token": (
            "<b>GitHubMonitor</b>\n"
            "<blockquote>No token set\n"
            "Add a token in module config (.config)</blockquote>"
        ),
        "token_invalid": (
            "<b>GitHubMonitor</b>\n"
            "<blockquote>Token invalid: {error}</blockquote>"
        ),
        "fetching": "<b>Fetching GitHub data...</b>",
        "main": (
            "<b>GitHubMonitor</b>\n"
            "<blockquote>User: <b>{user}</b>\n"
            "Plan: {plan}\n"
            "Repos: {repos} (public: {public})\n"
            "Checked: {date}</blockquote>"
        ),
        "rate_limits": (
            "<b>Rate Limits</b>\n"
            "<blockquote>Core: {core}\n"
            "Core reset: {core_reset}\n"
            "Search: {search}\n"
            "GraphQL: {graphql}\n"
            "Code scanning: {scanning}</blockquote>"
        ),
        "actions": (
            "<b>Actions Usage</b>\n"
            "<blockquote>Included minutes: {included}\n"
            "Used minutes: {used}\n"
            "Paid minutes: {paid}</blockquote>"
        ),
        "storage": (
            "<b>Storage</b>\n"
            "<blockquote>Included: {included}\n"
            "Used: {used}</blockquote>"
        ),
        "error": (
            "<b>GitHubMonitor Error</b>\n"
            "<blockquote>{error}</blockquote>"
        ),
        "btn_refresh": "Refresh",
        "btn_rate": "Rate Limits",
        "btn_actions": "Actions",
        "btn_storage": "Storage",
        "btn_back": "Back",
        "btn_close": "Close",
        "no_data": "n/a",
    }

    strings_ru = {
        "no_token": (
            "<b>GitHubMonitor</b>\n"
            "<blockquote>Токен не задан\n"
            "Добавь токен в конфиг модуля (.config)</blockquote>"
        ),
        "token_invalid": (
            "<b>GitHubMonitor</b>\n"
            "<blockquote>Токен невалиден: {error}</blockquote>"
        ),
        "fetching": "<b>Загружаем данные GitHub...</b>",
        "main": (
            "<b>GitHubMonitor</b>\n"
            "<blockquote>Юзер: <b>{user}</b>\n"
            "План: {plan}\n"
            "Репо: {repos} (публичных: {public})\n"
            "Проверено: {date}</blockquote>"
        ),
        "rate_limits": (
            "<b>Лимиты API</b>\n"
            "<blockquote>Core: {core}\n"
            "Сброс core: {core_reset}\n"
            "Поиск: {search}\n"
            "GraphQL: {graphql}\n"
            "Скан кода: {scanning}</blockquote>"
        ),
        "actions": (
            "<b>Actions</b>\n"
            "<blockquote>Минут в плане: {included}\n"
            "Использовано: {used}\n"
            "Оплачено: {paid}</blockquote>"
        ),
        "storage": (
            "<b>Хранилище</b>\n"
            "<blockquote>В плане: {included}\n"
            "Использовано: {used}</blockquote>"
        ),
        "error": (
            "<b>Ошибка GitHubMonitor</b>\n"
            "<blockquote>{error}</blockquote>"
        ),
        "btn_refresh": "Обновить",
        "btn_rate": "Лимиты API",
        "btn_actions": "Actions",
        "btn_storage": "Хранилище",
        "btn_back": "Назад",
        "btn_close": "Закрыть",
        "no_data": "н/д",
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "token",
                "",
                "GitHub PAT",
                validator=loader.validators.Hidden(),
            ),
            loader.ConfigValue(
                "timezone",
                3,
                "Timezone offset (UTC)",
                validator=loader.validators.Integer(minimum=-12, maximum=12),
            ),
            loader.ConfigValue(
                "timeout",
                30,
                "Request timeout in seconds",
                validator=loader.validators.Integer(minimum=5, maximum=300),
            ),
        )
        self._cache: dict = {}

    async def client_ready(self, client, db):
        self._client = client
        self._db = db

    def _token(self) -> str:
        return (self.config["token"] or "").strip()

    def _nd(self) -> str:
        return self.strings["no_data"]

    async def _fetch_all(self) -> dict:
        token = self._token()
        timeout = self.config["timeout"]
        user, rate, actions, storage = await asyncio.gather(
            _gh("GET", "/user", token, timeout),
            _gh("GET", "/rate_limit", token, timeout),
            _gh("GET", "/users/i-execute/settings/billing/actions", token, timeout),
            _gh("GET", "/users/i-execute/settings/billing/shared-storage", token, timeout),
        )
        return {"user": user, "rate": rate, "actions": actions, "storage": storage}

    def _fmt_main(self, data: dict) -> str:
        u = data.get("user", {}).get("data", {})
        if not data.get("user", {}).get("ok"):
            return self.strings["error"].format(error=_escape(data["user"].get("error", "bad token")))
        repos = u.get("total_private_repos")
        public = u.get("public_repos")
        total = (repos if repos is not None else 0) + (public or 0)
        return self.strings["main"].format(
            user=_escape(u.get("login", "?")),
            plan=_escape((u.get("plan") or {}).get("name", "?")),
            repos=total,
            public=public or 0,
            date=_now_str(self.config["timezone"]),
        )

    def _fmt_rate(self, data: dict) -> str:
        r = data.get("rate", {})
        res = (r.get("data") or {}).get("resources", {})
        core = res.get("core", {})
        search = res.get("search", {})
        graphql = res.get("graphql", {})
        scanning = res.get("code_scanning", {})

        core_line, _ = _quota_line(core)
        search_line, _ = _quota_line(search)
        graphql_line, _ = _quota_line(graphql)
        scan_line, _ = _quota_line(scanning)

        return self.strings["rate_limits"].format(
            core=core_line,
            core_reset=_fmt_reset(core.get("reset"), self.config["timezone"]),
            search=search_line,
            graphql=graphql_line,
            scanning=scan_line,
        )

    def _fmt_actions(self, data: dict) -> str:
        a = (data.get("actions") or {}).get("data", {})
        if not data.get("actions", {}).get("ok"):
            return self.strings["error"].format(error=_escape((data.get("actions") or {}).get("error", "n/a")))
        return self.strings["actions"].format(
            included=a.get("included_minutes", self._nd()),
            used=a.get("total_minutes_used", self._nd()),
            paid=a.get("total_paid_minutes_used", 0),
        )

    async def _fetch_all(self) -> dict:
        token = self._token()
        timeout = self.config["timeout"]
        user = await _gh("GET", "/user", token, timeout)
        login = "i-execute"
        if user.get("ok"):
            login = (user.get("data") or {}).get("login", login)
        rate, actions, storage = await asyncio.gather(
            _gh("GET", "/rate_limit", token, timeout),
            _gh("GET", f"/users/{login}/settings/billing/actions", token, timeout),
            _gh("GET", f"/users/{login}/settings/billing/shared-storage", token, timeout),
        )
        return {"user": user, "rate": rate, "actions": actions, "storage": storage}

    def _fmt_storage(self, data: dict) -> str:
        s = (data.get("storage") or {}).get("data", {})
        if not data.get("storage", {}).get("ok"):
            return self.strings["error"].format(error=_escape((data.get("storage") or {}).get("error", "n/a")))
        return self.strings["storage"].format(
            included=_fmt_bytes(s.get("included_storage", 0)),
            used=_fmt_bytes(s.get("used_storage", 0)),
        )

    def _markup_main(self) -> list:
        return [
            [
                {"text": self.strings["btn_refresh"], "callback": self._cb_refresh, "style": "success"},
                {"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"},
            ],
        ]

    async def _cb_close(self, call: InlineCall):
        await call.delete()

    async def _cb_refresh(self, call: InlineCall):
        await call.edit(self.strings["fetching"], reply_markup=[])
        self._cache = await self._fetch_all()
        await self._cb_main(call)

    async def _cb_main(self, call: InlineCall):
        data = self._cache or await self._fetch_all()
        self._cache = data
        text = self._fmt_main(data)
        markup = [
            [
                {"text": self.strings["btn_rate"], "callback": self._cb_rate, "style": "primary"},
                {"text": self.strings["btn_actions"], "callback": self._cb_actions, "style": "primary"},
            ],
            [
                {"text": self.strings["btn_storage"], "callback": self._cb_storage, "style": "primary"},
                {"text": self.strings["btn_refresh"], "callback": self._cb_refresh, "style": "success"},
            ],
            [{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}],
        ]
        if data.get("user", {}).get("status") == 401:
            text = self.strings["token_invalid"].format(error=_escape(data["user"].get("error", "401")))
            markup = self._markup_main()
        await call.edit(text, reply_markup=markup)

    async def _cb_rate(self, call: InlineCall):
        data = self._cache or await self._fetch_all()
        self._cache = data
        await call.edit(
            self._fmt_rate(data),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main, "style": "danger"}]],
        )

    async def _cb_actions(self, call: InlineCall):
        data = self._cache or await self._fetch_all()
        self._cache = data
        await call.edit(
            self._fmt_actions(data),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main, "style": "danger"}]],
        )

    async def _cb_storage(self, call: InlineCall):
        data = self._cache or await self._fetch_all()
        self._cache = data
        await call.edit(
            self._fmt_storage(data),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main, "style": "danger"}]],
        )

    @loader.command(
        ru_doc="Монитор GitHub аккаунта",
        en_doc="GitHub account monitor",
    )
    async def ghm(self, message):
        """GitHub account monitor"""
        if not self._token():
            await utils.answer(message, self.strings["no_token"])
            return
        form = await self.inline.form(
            text=self.strings["fetching"],
            message=message,
            reply_markup=[],
            silent=True,
        )
        self._cache = await self._fetch_all()
        data = self._cache
        if form:
            await form.edit(
                self._fmt_main(data),
                reply_markup=[
                    [
                        {"text": self.strings["btn_rate"], "callback": self._cb_rate, "style": "primary"},
                        {"text": self.strings["btn_actions"], "callback": self._cb_actions, "style": "primary"},
                    ],
                    [
                        {"text": self.strings["btn_storage"], "callback": self._cb_storage, "style": "primary"},
                        {"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"},
                    ],
                ],
            )
