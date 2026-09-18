# CopyLeft 2026 github.com/i-execute
# Author: I_execute.t.me
# Licensed under AGPLv3.

__version__ = (1, 0, 0)
# meta developer: Execute_forge.t.me

import asyncio
import io
import json
import logging
import time
import uuid
from collections import Counter
from datetime import datetime

import aiohttp

from telethon.tl.types import Message

from .. import loader, utils
from ..inline.types import InlineCall

logger = logging.getLogger(__name__)

BASE_URL = "https://api.telescan-group.ru"
CHATS_PER_PAGE = 6
MSGS_PER_PAGE = 8
REQUEST_TIMEOUT = 120


def _escape(text):
    if not text:
        return ""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _num(n):
    try:
        return f"{int(n):,}".replace(",", " ")
    except Exception:
        return str(n)


def _dt(s, dash="—"):
    if not s:
        return dash
    try:
        return datetime.fromisoformat(s).strftime("%d.%m.%Y %H:%M")
    except Exception:
        return str(s)


def _date(s, dash="—"):
    if not s:
        return dash
    try:
        return datetime.fromisoformat(s).strftime("%d.%m.%Y")
    except Exception:
        return str(s)


def _bar(value, maxv, width=12):
    if not maxv or maxv <= 0:
        return "░" * width
    filled = int(round(width * value / maxv))
    filled = max(0, min(width, filled))
    return "█" * filled + "░" * (width - filled)


def _days_between(a, b):
    try:
        da = datetime.fromisoformat(a)
        db = datetime.fromisoformat(b)
        return max(1, (db - da).days + 1)
    except Exception:
        return None


@loader.tds
class TeleScan(loader.Module):
    """TeleScan API client - profile, chats, messages and reactions stats by user"""

    strings = {
        "name": "TeleScan",
        "weekdays": "Mon,Tue,Wed,Thu,Fri,Sat,Sun",
        "no_token": (
            "<b>TeleScan</b>\n"
            "<blockquote>Token is not set.\n"
            "Open <code>.config TeleScan</code> and paste it into <code>token</code></blockquote>"
        ),
        "ask_target": (
            "<b>TeleScan</b>\n"
            "<blockquote>Reply to a user or choose lookup method:</blockquote>"
        ),
        "loading": "<b>TeleScan</b>\n<blockquote>Loading...</blockquote>",
        "resolving": "<b>TeleScan</b>\n<blockquote>Resolving @{q}...</blockquote>",
        "not_found": (
            "<b>TeleScan</b>\n"
            "<blockquote>Nothing found for <code>{q}</code></blockquote>"
        ),
        "bad_id": "<b>TeleScan</b>\n<blockquote>ID must be a number</blockquote>",
        "api_error": "<b>TeleScan</b>\n<blockquote>API error: {e}</blockquote>",
        "quota": (
            "<b>TeleScan</b>\n"
            "<blockquote>Daily quota for resolve-username is exhausted</blockquote>"
        ),
        "overview": (
            "<b>{name}</b>\n"
            "<blockquote>"
            "ID: <code>{id}</code>\n"
            "Username: {username}\n"
            "Messages: <b>{messages}</b>\n"
            "Chats: <b>{chats}</b>\n"
            "Reactions: <b>+{rr}</b> / <b>-{rg}</b>\n"
            "First seen: <code>{first}</code>\n"
            "Last seen: <code>{last}</code>"
            "</blockquote>"
        ),
        "profile": (
            "<b>{name}</b>\n"
            "<blockquote>"
            "Type: <code>{type}</code>\n"
            "ID: <code>{id}</code>\n"
            "Username: {username}\n"
            "All usernames: {usernames}\n"
            "DC: <code>{dc}</code>\n"
            "Bio: {bio}\n"
            "Birthday: {birthday}\n"
            "Gifts: <b>{gifts}</b>\n"
            "Flags: {flags}\n"
            "Personal channel: {channel}"
            "</blockquote>"
        ),
        "names_menu": (
            "<b>Names and usernames</b>\n"
            "<blockquote>{body}</blockquote>"
        ),
        "chats_menu": (
            "<b>Chats {page}/{pages}</b>\n"
            "<blockquote>{body}</blockquote>"
        ),
        "chat_row": "{bar} <b>{count}</b>\n{title}\n<code>+{rr}/-{rg}</code> {first} - {last}",
        "msgs_menu": (
            "<b>Messages {page}/{pages}</b>\n"
            "<blockquote>Total: <b>{total}</b></blockquote>\n"
            "{body}"
        ),
        "msg_row": "<code>{ts}</code> <b>{title}</b>\n{text}",
        "reactions_menu": (
            "<b>Reactions</b>\n"
            "<blockquote>"
            "Received: <b>{rc}</b> from <b>{rp}</b> people\n"
            "{rfirst} - {rlast}\n"
            "{rtop}\n"
            "\n"
            "Given: <b>{gc}</b> to <b>{gp}</b> people\n"
            "{gfirst} - {glast}\n"
            "{gtop}"
            "</blockquote>"
        ),
        "analytics_menu": (
            "<b>Activity</b>\n"
            "<blockquote>"
            "Window: <code>{days}</code> days\n"
            "Avg per day: <b>{perday}</b>\n"
            "Media: <b>{media}</b> ({mediapct}%)\n"
            "Text: <b>{text}</b>\n"
            "Karma (recv/given): <b>{karma}</b>\n"
            "Top chat: {topchat}\n"
            "\n"
            "By hour:\n{hours}\n"
            "\n"
            "By weekday:\n{days_body}"
            "</blockquote>"
        ),
        "empty": "—",
        "no_messages": "<b>Messages</b>\n<blockquote>No messages found</blockquote>",
        "no_reactions": "<b>Reactions</b>\n<blockquote>No reactions found</blockquote>",
        "input_username": "Enter @username:",
        "input_id": "Enter user ID:",
        "btn_by_username": "By username",
        "btn_by_id": "By ID",
        "btn_overview": "Overview",
        "btn_profile": "Live profile (quota)",
        "btn_names": "Names history",
        "btn_chats": "Chats",
        "btn_messages": "Messages",
        "btn_reactions": "Reactions",
        "btn_analytics": "Activity",
        "btn_export": "Export JSON",
        "btn_back": "Back",
        "btn_close": "Close",
        "btn_left": "←",
        "btn_right": "→",
    }

    strings_ru = {
        "weekdays": "Пн,Вт,Ср,Чт,Пт,Сб,Вс",
        "no_token": (
            "<b>TeleScan</b>\n"
            "<blockquote>Токен не установлен.\n"
            "Открой <code>.config TeleScan</code> и вставь его в <code>token</code></blockquote>"
        ),
        "ask_target": (
            "<b>TeleScan</b>\n"
            "<blockquote>Ответь на пользователя или выбери способ поиска:</blockquote>"
        ),
        "loading": "<b>TeleScan</b>\n<blockquote>Загрузка...</blockquote>",
        "resolving": "<b>TeleScan</b>\n<blockquote>Резолвлю @{q}...</blockquote>",
        "not_found": (
            "<b>TeleScan</b>\n"
            "<blockquote>Ничего не найдено по <code>{q}</code></blockquote>"
        ),
        "bad_id": "<b>TeleScan</b>\n<blockquote>ID должен быть числом</blockquote>",
        "api_error": "<b>TeleScan</b>\n<blockquote>Ошибка API: {e}</blockquote>",
        "quota": (
            "<b>TeleScan</b>\n"
            "<blockquote>Суточная квота на resolve-username исчерпана</blockquote>"
        ),
        "overview": (
            "<b>{name}</b>\n"
            "<blockquote>"
            "ID: <code>{id}</code>\n"
            "Юзернейм: {username}\n"
            "Сообщений: <b>{messages}</b>\n"
            "Чатов: <b>{chats}</b>\n"
            "Реакции: <b>+{rr}</b> / <b>-{rg}</b>\n"
            "Первое: <code>{first}</code>\n"
            "Последнее: <code>{last}</code>"
            "</blockquote>"
        ),
        "profile": (
            "<b>{name}</b>\n"
            "<blockquote>"
            "Тип: <code>{type}</code>\n"
            "ID: <code>{id}</code>\n"
            "Юзернейм: {username}\n"
            "Все юзернеймы: {usernames}\n"
            "DC: <code>{dc}</code>\n"
            "Био: {bio}\n"
            "День рождения: {birthday}\n"
            "Подарков: <b>{gifts}</b>\n"
            "Флаги: {flags}\n"
            "Личный канал: {channel}"
            "</blockquote>"
        ),
        "names_menu": (
            "<b>Имена и юзернеймы</b>\n"
            "<blockquote>{body}</blockquote>"
        ),
        "chats_menu": (
            "<b>Чаты {page}/{pages}</b>\n"
            "<blockquote>{body}</blockquote>"
        ),
        "chat_row": "{bar} <b>{count}</b>\n{title}\n<code>+{rr}/-{rg}</code> {first} - {last}",
        "msgs_menu": (
            "<b>Сообщения {page}/{pages}</b>\n"
            "<blockquote>Всего: <b>{total}</b></blockquote>\n"
            "{body}"
        ),
        "msg_row": "<code>{ts}</code> <b>{title}</b>\n{text}",
        "reactions_menu": (
            "<b>Реакции</b>\n"
            "<blockquote>"
            "Получено: <b>{rc}</b> от <b>{rp}</b> людей\n"
            "{rfirst} - {rlast}\n"
            "{rtop}\n"
            "\n"
            "Поставлено: <b>{gc}</b> для <b>{gp}</b> людей\n"
            "{gfirst} - {glast}\n"
            "{gtop}"
            "</blockquote>"
        ),
        "analytics_menu": (
            "<b>Активность</b>\n"
            "<blockquote>"
            "Окно: <code>{days}</code> дней\n"
            "В среднем в день: <b>{perday}</b>\n"
            "Медиа: <b>{media}</b> ({mediapct}%)\n"
            "Текст: <b>{text}</b>\n"
            "Карма (получ/пост): <b>{karma}</b>\n"
            "Топ чат: {topchat}\n"
            "\n"
            "По часам:\n{hours}\n"
            "\n"
            "По дням недели:\n{days_body}"
            "</blockquote>"
        ),
        "empty": "—",
        "no_messages": "<b>Сообщения</b>\n<blockquote>Сообщений не найдено</blockquote>",
        "no_reactions": "<b>Реакции</b>\n<blockquote>Реакций не найдено</blockquote>",
        "input_username": "Введи @username:",
        "input_id": "Введи ID пользователя:",
        "btn_by_username": "По юзернейму",
        "btn_by_id": "По ID",
        "btn_overview": "Обзор",
        "btn_profile": "Актуальный профиль (квота)",
        "btn_names": "История имён",
        "btn_chats": "Чаты",
        "btn_messages": "Сообщения",
        "btn_reactions": "Реакции",
        "btn_analytics": "Активность",
        "btn_export": "Экспорт JSON",
        "btn_back": "Назад",
        "btn_close": "Закрыть",
        "btn_left": "←",
        "btn_right": "→",
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "token", "",
                "TeleScan API token",
                validator=loader.validators.Hidden(),
            ),
        )
        self._sessions = {}

    async def client_ready(self, client, db):
        self._client = client
        self._db = db

    def _token(self):
        return (self.config["token"] or "").strip()

    async def _api(self, path, payload):
        token = self._token()
        headers = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        body = dict(payload)
        if token:
            body.setdefault("token", token)
        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(f"{BASE_URL}{path}", json=body, headers=headers) as resp:
                raw = await resp.text()
                try:
                    data = json.loads(raw)
                except Exception:
                    data = None
                return resp.status, data

    async def _fetch_stats(self, sess):
        status, data = await self._api("/v2/get-stats", {"user_id": sess["id"]})
        if status != 200 or not isinstance(data, dict):
            return f"HTTP {status}"
        sess["stats"] = data
        return None

    async def _fetch_messages(self, sess):
        if sess.get("messages") is not None:
            return None
        status, data = await self._api("/v2/get-messages", {"user_id": sess["id"]})
        if status != 200 or not isinstance(data, dict):
            return f"HTTP {status}"
        sess["messages"] = data
        return None

    async def _fetch_reactions(self, sess):
        if sess.get("reactions") is not None:
            return None
        status, data = await self._api("/v2/get-reactions", {"user_id": sess["id"]})
        if status != 200 or not isinstance(data, dict):
            return f"HTTP {status}"
        sess["reactions"] = data
        return None

    async def _fetch_resolve(self, sess, username):
        status, data = await self._api("/v2/resolve-username", {"username": username})
        if status == 429:
            return "quota"
        if status != 200 or not isinstance(data, dict) or not data.get("id"):
            return "notfound"
        sess["resolve"] = data
        sess["id"] = data.get("id")
        return None

    def _new_session(self, chat_id, target_id=None, username=None):
        sid = uuid.uuid4().hex[:12]
        self._sessions[sid] = {
            "chat_id": chat_id,
            "id": target_id,
            "username": username,
            "stats": None,
            "messages": None,
            "reactions": None,
            "resolve": None,
            "chats_page": 0,
            "msgs_page": 0,
        }
        return sid

    def _display_name(self, sess):
        stats = sess.get("stats") or {}
        prof = stats.get("profile") or {}
        fn = prof.get("first_name") or ""
        ln = prof.get("last_name") or ""
        name = f"{fn} {ln}".strip()
        if not name:
            res = sess.get("resolve") or {}
            fn = res.get("first_name") or ""
            ln = res.get("last_name") or ""
            name = f"{fn} {ln}".strip()
        return name or "Unknown"

    def _overview_text(self, sess):
        stats = sess.get("stats") or {}
        prof = stats.get("profile") or {}
        act = stats.get("activity") or {}
        uname = prof.get("username")
        return self.strings["overview"].format(
            name=_escape(self._display_name(sess)),
            id=sess.get("id"),
            username=f"@{_escape(uname)}" if uname else self.strings["empty"],
            messages=_num(act.get("messages_count", 0)),
            chats=_num(act.get("chats_count", 0)),
            rr=_num(act.get("reactions_received", 0)),
            rg=_num(act.get("reactions_given", 0)),
            first=_dt(act.get("first_message")),
            last=_dt(act.get("last_message")),
        )

    def _main_markup(self, sid):
        return [
            [
                {"text": self.strings["btn_overview"], "callback": self._cb_overview, "args": (sid,), "style": "primary"},
                {"text": self.strings["btn_analytics"], "callback": self._cb_analytics, "args": (sid,), "style": "primary"},
            ],
            [
                {"text": self.strings["btn_chats"], "callback": self._cb_chats, "args": (sid,), "style": "primary"},
                {"text": self.strings["btn_messages"], "callback": self._cb_messages, "args": (sid,), "style": "primary"},
            ],
            [
                {"text": self.strings["btn_reactions"], "callback": self._cb_reactions, "args": (sid,), "style": "primary"},
                {"text": self.strings["btn_names"], "callback": self._cb_names, "args": (sid,), "style": "primary"},
            ],
            [
                {"text": self.strings["btn_profile"], "callback": self._cb_profile, "args": (sid,), "style": "primary"},
                {"text": self.strings["btn_export"], "callback": self._cb_export, "args": (sid,), "style": "primary"},
            ],
            [{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}],
        ]

    def _back_markup(self, sid):
        return [[{"text": self.strings["btn_back"], "callback": self._cb_overview, "args": (sid,), "style": "primary"}]]

    @loader.command(
        ru_doc="- Реплай на юзера или без аргументов форма поиска. Также .ts @username или .ts id",
        en_doc="- Reply to a user or open the lookup form. Also .ts @username or .ts id",
    )
    async def ts(self, message: Message):
        """- Reply to a user or open the lookup form. Also .ts @username or .ts id"""
        if not self._token():
            await utils.answer(message, self.strings["no_token"])
            return

        args = utils.get_args_raw(message).strip()
        reply = await message.get_reply_message()
        chat_id = utils.get_chat_id(message)

        if args:
            token = args.split()[0]
            if token.lstrip("-").isdigit():
                sid = self._new_session(chat_id, target_id=int(token))
                await self._open_main_from_message(message, sid)
            else:
                await self._open_by_username(message, token.lstrip("@"))
            return

        if reply and reply.sender_id:
            uname = None
            sender = getattr(reply, "sender", None)
            if sender is not None:
                uname = getattr(sender, "username", None)
            sid = self._new_session(chat_id, target_id=reply.sender_id, username=uname)
            await self._open_main_from_message(message, sid)
            return

        await self.inline.form(
            text=self.strings["ask_target"],
            message=message,
            reply_markup=[
                [
                    {"text": self.strings["btn_by_username"], "input": self.strings["input_username"], "handler": self._cb_input_username, "style": "primary"},
                    {"text": self.strings["btn_by_id"], "input": self.strings["input_id"], "handler": self._cb_input_id, "style": "primary"},
                ],
                [{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}],
            ],
            silent=True,
        )

    async def _open_by_username(self, message, username):
        m = await utils.answer(message, self.strings["resolving"].format(q=_escape(username)))
        if isinstance(m, list):
            m = m[0]
        chat_id = utils.get_chat_id(message)
        sid = self._new_session(chat_id, username=username)
        sess = self._sessions[sid]
        err = await self._fetch_resolve(sess, username)
        if err == "quota":
            await m.edit(self.strings["quota"], parse_mode="html")
            return
        if err:
            await m.edit(self.strings["not_found"].format(q=_escape(username)), parse_mode="html")
            return
        serr = await self._fetch_stats(sess)
        if serr:
            await m.edit(self.strings["api_error"].format(e=_escape(serr)), parse_mode="html")
            return
        await m.delete()
        await self.inline.form(
            text=self._overview_text(sess),
            message=message,
            reply_markup=self._main_markup(sid),
            silent=True,
        )

    async def _open_main_from_message(self, message, sid):
        m = await utils.answer(message, self.strings["loading"])
        if isinstance(m, list):
            m = m[0]
        sess = self._sessions[sid]
        err = await self._fetch_stats(sess)
        if err:
            await m.edit(self.strings["api_error"].format(e=_escape(err)), parse_mode="html")
            return
        if not (sess.get("stats") or {}).get("found"):
            await m.edit(self.strings["not_found"].format(q=sess.get("id")), parse_mode="html")
            return
        await m.delete()
        await self.inline.form(
            text=self._overview_text(sess),
            message=message,
            reply_markup=self._main_markup(sid),
            silent=True,
        )

    async def _cb_input_username(self, call: InlineCall, text: str):
        username = text.strip().lstrip("@")
        if not username:
            await call.answer()
            return
        await call.edit(self.strings["resolving"].format(q=_escape(username)))
        sid = self._new_session(call.form["chat"], username=username)
        sess = self._sessions[sid]
        err = await self._fetch_resolve(sess, username)
        if err == "quota":
            await call.edit(self.strings["quota"])
            return
        if err:
            await call.edit(self.strings["not_found"].format(q=_escape(username)))
            return
        serr = await self._fetch_stats(sess)
        if serr:
            await call.edit(self.strings["api_error"].format(e=_escape(serr)))
            return
        await call.edit(self._overview_text(sess), reply_markup=self._main_markup(sid))

    async def _cb_input_id(self, call: InlineCall, text: str):
        raw = text.strip()
        if not raw.lstrip("-").isdigit():
            await call.edit(self.strings["bad_id"])
            return
        await call.edit(self.strings["loading"])
        sid = self._new_session(call.form["chat"], target_id=int(raw))
        sess = self._sessions[sid]
        err = await self._fetch_stats(sess)
        if err:
            await call.edit(self.strings["api_error"].format(e=_escape(err)))
            return
        if not (sess.get("stats") or {}).get("found"):
            await call.edit(self.strings["not_found"].format(q=raw))
            return
        await call.edit(self._overview_text(sess), reply_markup=self._main_markup(sid))

    async def _cb_overview(self, call: InlineCall, sid: str):
        sess = self._sessions.get(sid)
        if not sess:
            await call.answer()
            return
        await call.edit(self._overview_text(sess), reply_markup=self._main_markup(sid))

    async def _cb_profile(self, call: InlineCall, sid: str):
        sess = self._sessions.get(sid)
        if not sess:
            await call.answer()
            return
        if sess.get("resolve") is None:
            uname = sess.get("username")
            if not uname:
                stats = sess.get("stats") or {}
                uname = (stats.get("profile") or {}).get("username")
            if not uname:
                await call.edit(self.strings["not_found"].format(q=sess.get("id")), reply_markup=self._back_markup(sid))
                return
            await call.edit(self.strings["resolving"].format(q=_escape(uname)))
            err = await self._fetch_resolve(sess, uname)
            if err == "quota":
                await call.edit(self.strings["quota"], reply_markup=self._back_markup(sid))
                return
            if err:
                await call.edit(self.strings["not_found"].format(q=_escape(uname)), reply_markup=self._back_markup(sid))
                return
        res = sess["resolve"]
        flags = []
        if res.get("is_verified"):
            flags.append("verified")
        if res.get("is_scam"):
            flags.append("scam")
        if res.get("is_fake"):
            flags.append("fake")
        if res.get("is_restricted"):
            flags.append("restricted")
        if res.get("is_premium"):
            flags.append("premium")
        usernames = res.get("usernames") or []
        bd = res.get("birthday") or {}
        bd_str = self.strings["empty"]
        if bd:
            day = bd.get("day")
            month = bd.get("month")
            year = bd.get("year")
            if day and month:
                bd_str = f"{day:02d}.{month:02d}" + (f".{year}" if year else "")
        pc = res.get("personal_channel") or {}
        pc_str = self.strings["empty"]
        if pc:
            pc_name = pc.get("username")
            pc_str = f"@{_escape(pc_name)}" if pc_name else _escape(pc.get("title") or "—")
        name = f"{res.get('first_name') or ''} {res.get('last_name') or ''}".strip() or "Unknown"
        await call.edit(
            self.strings["profile"].format(
                name=_escape(name),
                type=_escape(res.get("type") or "user"),
                id=res.get("id"),
                username=f"@{_escape(res.get('username'))}" if res.get("username") else self.strings["empty"],
                usernames=", ".join(f"@{_escape(u)}" for u in usernames) if usernames else self.strings["empty"],
                dc=res.get("dc_id") if res.get("dc_id") is not None else self.strings["empty"],
                bio=_escape(res.get("bio")) or self.strings["empty"],
                birthday=bd_str,
                gifts=_num(res.get("gifts_count") or 0),
                flags=", ".join(flags) if flags else self.strings["empty"],
                channel=pc_str,
            ),
            reply_markup=self._back_markup(sid),
        )

    async def _cb_names(self, call: InlineCall, sid: str):
        sess = self._sessions.get(sid)
        if not sess:
            await call.answer()
            return
        prof = (sess.get("stats") or {}).get("profile") or {}
        lines = []
        unames = prof.get("usernames") or []
        if unames:
            lines.append("<b>Usernames:</b>")
            for u in unames:
                lines.append(
                    f"@{_escape(u.get('username'))}  "
                    f"<code>{_date(u.get('first_seen'))} - {_date(u.get('last_seen'))}</code>"
                )
        names = prof.get("names") or []
        if names:
            lines.append("")
            lines.append("<b>Names:</b>")
            for n in names:
                nm = f"{n.get('first_name') or ''} {n.get('last_name') or ''}".strip() or "—"
                lines.append(
                    f"{_escape(nm)}  "
                    f"<code>{_date(n.get('first_seen'))} - {_date(n.get('last_seen'))}</code>"
                )
        body = "\n".join(lines) if lines else self.strings["empty"]
        await call.edit(self.strings["names_menu"].format(body=body), reply_markup=self._back_markup(sid))

    async def _cb_chats(self, call: InlineCall, sid: str):
        sess = self._sessions.get(sid)
        if not sess:
            await call.answer()
            return
        await self._render_chats(call, sid)

    async def _render_chats(self, call: InlineCall, sid: str):
        sess = self._sessions[sid]
        chats = (sess.get("stats") or {}).get("chats") or []
        if not chats:
            await call.edit(self.strings["names_menu"].format(body=self.strings["empty"]), reply_markup=self._back_markup(sid))
            return
        maxv = max((c.get("messages_count", 0) for c in chats), default=1)
        pages = max(1, (len(chats) + CHATS_PER_PAGE - 1) // CHATS_PER_PAGE)
        page = max(0, min(sess["chats_page"], pages - 1))
        sess["chats_page"] = page
        start = page * CHATS_PER_PAGE
        rows = []
        for c in chats[start:start + CHATS_PER_PAGE]:
            title = c.get("title")
            uname = c.get("username")
            label = _escape(title) if title else (f"@{_escape(uname)}" if uname else f"<code>{c.get('chat_id')}</code>")
            rows.append(self.strings["chat_row"].format(
                bar=_bar(c.get("messages_count", 0), maxv),
                count=_num(c.get("messages_count", 0)),
                title=label,
                rr=_num(c.get("reactions_received", 0)),
                rg=_num(c.get("reactions_given", 0)),
                first=_date(c.get("first_message")),
                last=_date(c.get("last_message")),
            ))
        body = "\n\n".join(rows)
        await call.edit(
            self.strings["chats_menu"].format(page=page + 1, pages=pages, body=body),
            reply_markup=self._pager_markup(sid, page, pages, self._cb_chats_page, self._cb_overview),
        )

    async def _cb_chats_page(self, call: InlineCall, sid: str, page: int):
        sess = self._sessions.get(sid)
        if not sess:
            await call.answer()
            return
        sess["chats_page"] = page
        await self._render_chats(call, sid)

    async def _cb_messages(self, call: InlineCall, sid: str):
        sess = self._sessions.get(sid)
        if not sess:
            await call.answer()
            return
        if sess.get("messages") is None:
            await call.edit(self.strings["loading"])
            err = await self._fetch_messages(sess)
            if err:
                await call.edit(self.strings["api_error"].format(e=_escape(err)), reply_markup=self._back_markup(sid))
                return
        await self._render_messages(call, sid)

    async def _render_messages(self, call: InlineCall, sid: str):
        sess = self._sessions[sid]
        data = sess.get("messages") or {}
        msgs = data.get("messages") or []
        if not msgs:
            await call.edit(self.strings["no_messages"], reply_markup=self._back_markup(sid))
            return
        pages = max(1, (len(msgs) + MSGS_PER_PAGE - 1) // MSGS_PER_PAGE)
        page = max(0, min(sess["msgs_page"], pages - 1))
        sess["msgs_page"] = page
        start = page * MSGS_PER_PAGE
        rows = []
        for m in msgs[start:start + MSGS_PER_PAGE]:
            title = m.get("title")
            uname = m.get("username")
            label = _escape(title) if title else (f"@{_escape(uname)}" if uname else f"{m.get('chat_id')}")
            text = m.get("text")
            text = _escape(text)[:120] if text else "[media]"
            rows.append(self.strings["msg_row"].format(
                ts=_dt(m.get("timestamp")),
                title=label,
                text=text,
            ))
        body = "\n".join(rows)
        await call.edit(
            self.strings["msgs_menu"].format(page=page + 1, pages=pages, total=_num(data.get("messages_count", len(msgs))), body=body),
            reply_markup=self._pager_markup(sid, page, pages, self._cb_msgs_page, self._cb_overview),
        )

    async def _cb_msgs_page(self, call: InlineCall, sid: str, page: int):
        sess = self._sessions.get(sid)
        if not sess:
            await call.answer()
            return
        sess["msgs_page"] = page
        await self._render_messages(call, sid)

    def _reaction_top(self, side, limit=8):
        emoji = (side or {}).get("emoji") or []
        lines = []
        for e in emoji[:limit]:
            if e.get("emoji"):
                key = e["emoji"]
            elif e.get("custom_emoji_id"):
                key = f"[custom {e['custom_emoji_id']}]"
            else:
                key = "?"
            lines.append(f"{key} {_num(e.get('count', 0))}")
        return "\n".join(lines) if lines else self.strings["empty"]

    async def _cb_reactions(self, call: InlineCall, sid: str):
        sess = self._sessions.get(sid)
        if not sess:
            await call.answer()
            return
        if sess.get("reactions") is None:
            await call.edit(self.strings["loading"])
            err = await self._fetch_reactions(sess)
            if err:
                await call.edit(self.strings["api_error"].format(e=_escape(err)), reply_markup=self._back_markup(sid))
                return
        data = sess.get("reactions") or {}
        if not data.get("found"):
            await call.edit(self.strings["no_reactions"], reply_markup=self._back_markup(sid))
            return
        rc = data.get("received") or {}
        gv = data.get("given") or {}
        await call.edit(
            self.strings["reactions_menu"].format(
                rc=_num(rc.get("count", 0)),
                rp=_num(rc.get("people", 0)),
                rfirst=_date(rc.get("first")),
                rlast=_date(rc.get("last")),
                rtop=self._reaction_top(rc),
                gc=_num(gv.get("count", 0)),
                gp=_num(gv.get("people", 0)),
                gfirst=_date(gv.get("first")),
                glast=_date(gv.get("last")),
                gtop=self._reaction_top(gv),
            ),
            reply_markup=self._back_markup(sid),
        )

    async def _cb_analytics(self, call: InlineCall, sid: str):
        sess = self._sessions.get(sid)
        if not sess:
            await call.answer()
            return
        if sess.get("messages") is None:
            await call.edit(self.strings["loading"])
            err = await self._fetch_messages(sess)
            if err:
                await call.edit(self.strings["api_error"].format(e=_escape(err)), reply_markup=self._back_markup(sid))
                return
        stats = sess.get("stats") or {}
        act = stats.get("activity") or {}
        chats = stats.get("chats") or []
        msgs = (sess.get("messages") or {}).get("messages") or []
        total = act.get("messages_count", len(msgs)) or 1
        media = sum(1 for m in msgs if not m.get("text"))
        text_cnt = len(msgs) - media
        media_pct = round(media / len(msgs) * 100) if msgs else 0
        days = _days_between(act.get("first_message"), act.get("last_message"))
        perday = round(total / days, 1) if days else total
        hours = Counter()
        dows = Counter()
        for m in msgs:
            try:
                d = datetime.fromisoformat(m.get("timestamp"))
                hours[d.hour] += 1
                dows[d.weekday()] += 1
            except Exception:
                pass
        hmax = max(hours.values(), default=1)
        hour_lines = []
        for h in range(0, 24, 3):
            block = sum(hours.get(hh, 0) for hh in (h, h + 1, h + 2))
            hour_lines.append(f"<code>{h:02d}-{h + 2:02d}</code> {_bar(block, hmax * 3, 10)} {_num(block)}")
        weekdays = self.strings["weekdays"].split(",")
        dmax = max(dows.values(), default=1)
        day_lines = []
        for i in range(7):
            day_lines.append(f"<code>{weekdays[i]}</code> {_bar(dows.get(i, 0), dmax, 10)} {_num(dows.get(i, 0))}")
        top_chat = self.strings["empty"]
        if chats:
            c = chats[0]
            title = c.get("title") or (f"@{c.get('username')}" if c.get("username") else str(c.get("chat_id")))
            top_chat = f"{_escape(title)} ({_num(c.get('messages_count', 0))})"
        karma = f"{_num(act.get('reactions_received', 0))}/{_num(act.get('reactions_given', 0))}"
        await call.edit(
            self.strings["analytics_menu"].format(
                days=days if days else self.strings["empty"],
                perday=perday,
                media=_num(media),
                mediapct=media_pct,
                text=_num(text_cnt),
                karma=karma,
                topchat=top_chat,
                hours="\n".join(hour_lines),
                days_body="\n".join(day_lines),
            ),
            reply_markup=self._back_markup(sid),
        )

    async def _cb_export(self, call: InlineCall, sid: str):
        sess = self._sessions.get(sid)
        if not sess:
            await call.answer()
            return
        await call.edit(self.strings["loading"])
        await self._fetch_messages(sess)
        await self._fetch_reactions(sess)
        dump = {
            "id": sess.get("id"),
            "username": sess.get("username"),
            "resolve": sess.get("resolve"),
            "stats": sess.get("stats"),
            "reactions": sess.get("reactions"),
            "messages": sess.get("messages"),
        }
        bio = io.BytesIO(json.dumps(dump, ensure_ascii=False, indent=2).encode("utf-8"))
        bio.name = f"telescan_{sess.get('id')}.json"
        await self._client.send_file(sess["chat_id"], bio)
        await call.edit(self._overview_text(sess), reply_markup=self._main_markup(sid))

    def _pager_markup(self, sid, page, pages, page_cb, back_cb):
        left = {"text": self.strings["btn_left"], "callback": page_cb, "args": (sid, page - 1)}
        right = {"text": self.strings["btn_right"], "callback": page_cb, "args": (sid, page + 1)}
        if page > 0:
            left["style"] = "primary"
        if page < pages - 1:
            right["style"] = "primary"
        return [
            [left, {"text": f"{page + 1}/{pages}", "callback": self._cb_noop, "style": "primary"}, right],
            [
                {"text": self.strings["btn_back"], "callback": back_cb, "args": (sid,), "style": "primary"},
                {"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"},
            ],
        ]

    async def _cb_noop(self, call: InlineCall):
        await call.answer()

    async def _cb_close(self, call: InlineCall):
        await call.delete()