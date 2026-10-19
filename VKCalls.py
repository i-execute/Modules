# CopyLeft 2026 github.com/i-execute
# Author: I_execute.t.me
# Licensed under AGPLv3.

__version__ = (1, 3, 0)
# meta developer: Execute_forge.t.me
# meta banner: https://raw.githubusercontent.com/i-execute/Modules/main/Storage/VKCalls/MetaBanner.jpeg

import asyncio
import logging
import os
import re
import sys
import typing

import aiohttp
from telethon import events

from .. import loader, utils
from ..inline.types import InlineCall

logger = logging.getLogger(__name__)

VK_API_VERSION = "5.199"
VK_API_BASE = "https://api.vk.ru/method"
VK_HOST_URL = "https://vkhost.github.io/"
VK_TOKEN_RE = re.compile(r"access_token=([A-Za-z0-9._-]+)")


def extract_vk_token(text: str) -> typing.Optional[str]:
    if not text:
        return None
    m = VK_TOKEN_RE.search(text)
    if m:
        return m.group(1)
    text = text.strip()
    if not text.startswith("http") and len(text) > 20:
        return text
    return None


class VKAPIError(Exception):
    def __init__(self, error: dict):
        self.error = error or {}
        code = self.error.get("error_code")
        msg = self.error.get("error_msg")
        super().__init__(f"VK error {code}: {msg}")


class VKCallsAPIClient:
    def __init__(self):
        self._token: typing.Optional[str] = None
        self._session: typing.Optional[aiohttp.ClientSession] = None
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
        async with s.post(f"{VK_API_BASE}/{method}", data=params) as r:
            return await r.json(content_type=None)

    async def api(self, method: str, **params) -> typing.Any:
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
            "<blockquote><b>VK Calls Manager</b></blockquote>\n"
            "<blockquote>Select action:</blockquote>"
        ),
        "btn_create": "Create Call",
        "btn_active": "Active Calls",
        "btn_auth": "Authorization",
        "btn_auth_bot": "Send token to Bot",
        "btn_back": "Back",
        "btn_finish": "Finish",
        "auth_menu": (
            "<blockquote><b>VK Authorization</b></blockquote>\n"
            "<blockquote>1. Open vkhost.github.io\n"
            "2. Choose Kate Mobile\n"
            "3. Allow access on oauth.vk.com\n"
            "4. Copy full URL or token\n"
            "5. Send to bot via button below</blockquote>"
        ),
        "auth_open": "Open vkhost.github.io",
        "auth_input": "Paste token here:",
        "auth_success": (
            "<blockquote><b>Authorization successful!</b></blockquote>\n"
            "<blockquote>User ID: <code>{user_id}</code></blockquote>"
        ),
        "auth_failed": (
            "<blockquote><b>Authorization failed</b></blockquote>\n"
            "<blockquote>{error}</blockquote>"
        ),
        "auth_invalid": "<blockquote><b>Invalid token format</b></blockquote>",
        "auth_bot_prompt": (
            "<blockquote><b>VKCalls | Authorization</b></blockquote>\n"
            "<blockquote>Send your VK token or full authorization URL:</blockquote>"
        ),
        "auth_success_bot": (
            "<blockquote><b>VKCalls | Authorization successful!</b></blockquote>\n"
            "<blockquote>User ID: <code>{user_id}</code></blockquote>\n"
            "<blockquote>Messages will be deleted in 10 seconds...</blockquote>"
        ),
        "auth_failed_bot": (
            "<blockquote><b>VKCalls | Authorization failed</b></blockquote>\n"
            "<blockquote>The token is invalid or expired.</blockquote>\n"
            "<blockquote>Messages will be deleted in 10 seconds...</blockquote>"
        ),
        "not_authorized": (
            "<blockquote><b>Not authorized</b></blockquote>\n"
            "<blockquote>Please complete VK authorization first</blockquote>"
        ),
        "call_created": (
            "<blockquote><b>Call Created</b></blockquote>\n"
            "<blockquote>Call ID: <code>{call_id}</code>\n"
            "Join Link: <code>{join_link}</code>\n"
            "Short ID: <code>{short_id}</code>\n"
            "Short Password: <code>{short_password}</code>\n"
            "Short Link: <code>{short_link}</code></blockquote>"
        ),
        "call_create_failed": (
            "<blockquote><b>Failed to create call</b></blockquote>\n"
            "<blockquote>{error}</blockquote>"
        ),
        "active_calls": (
            "<blockquote><b>Active Calls</b></blockquote>\n"
            "<blockquote>Total: <b>{count}</b></blockquote>"
        ),
        "no_active_calls": (
            "<blockquote><b>No active calls</b></blockquote>\n"
            "<blockquote>Create a new call to get started</blockquote>"
        ),
        "call_info": (
            "<blockquote><b>Call Info</b></blockquote>\n"
            "<blockquote>Call ID: <code>{call_id}</code>\n"
            "Join Link: <code>{join_link}</code>\n"
            "Short ID: <code>{short_id}</code>\n"
            "Status: {status}</blockquote>"
        ),
        "call_finished": (
            "<blockquote><b>Call finished</b></blockquote>\n"
            "<blockquote>Call ID: <code>{call_id}</code></blockquote>"
        ),
        "call_finish_failed": (
            "<blockquote><b>Failed to finish call</b></blockquote>\n"
            "<blockquote>{error}</blockquote>"
        ),
        "loading": "<blockquote><b>Loading...</b></blockquote>",
    }

    strings_ru = {
        "main_menu": (
            "<blockquote><b>Менеджер VK Звонков</b></blockquote>\n"
            "<blockquote>Выберите действие:</blockquote>"
        ),
        "btn_create": "Создать звонок",
        "btn_active": "Активные звонки",
        "btn_auth": "Авторизация",
        "btn_auth_bot": "Отправить токен боту",
        "btn_back": "Назад",
        "btn_finish": "Завершить",
        "auth_menu": (
            "<blockquote><b>Авторизация VK</b></blockquote>\n"
            "<blockquote>1. Откройте vkhost.github.io\n"
            "2. Выберите Kate Mobile\n"
            "3. Разрешите доступ на oauth.vk.com\n"
            "4. Скопируйте полный URL или токен\n"
            "5. Отправьте боту по кнопке ниже</blockquote>"
        ),
        "auth_open": "Открыть vkhost.github.io",
        "auth_input": "Вставить токен здесь:",
        "auth_success": (
            "<blockquote><b>Авторизация успешна!</b></blockquote>\n"
            "<blockquote>User ID: <code>{user_id}</code></blockquote>"
        ),
        "auth_failed": (
            "<blockquote><b>Ошибка авторизации</b></blockquote>\n"
            "<blockquote>{error}</blockquote>"
        ),
        "auth_invalid": "<blockquote><b>Неверный формат токена</b></blockquote>",
        "auth_bot_prompt": (
            "<blockquote><b>VKCalls | Авторизация</b></blockquote>\n"
            "<blockquote>Отправьте токен VK или полную ссылку авторизации:</blockquote>"
        ),
        "auth_success_bot": (
            "<blockquote><b>VKCalls | Авторизация успешна!</b></blockquote>\n"
            "<blockquote>User ID: <code>{user_id}</code></blockquote>\n"
            "<blockquote>Сообщения будут удалены через 10 секунд...</blockquote>"
        ),
        "auth_failed_bot": (
            "<blockquote><b>VKCalls | Ошибка авторизации</b></blockquote>\n"
            "<blockquote>Токен недействителен или устарел.</blockquote>\n"
            "<blockquote>Сообщения будут удалены через 10 секунд...</blockquote>"
        ),
        "not_authorized": (
            "<blockquote><b>Не авторизован</b></blockquote>\n"
            "<blockquote>Пожалуйста, сначала выполните авторизацию VK</blockquote>"
        ),
        "call_created": (
            "<blockquote><b>Звонок создан</b></blockquote>\n"
            "<blockquote>Call ID: <code>{call_id}</code>\n"
            "Ссылка: <code>{join_link}</code>\n"
            "Короткий ID: <code>{short_id}</code>\n"
            "Пароль: <code>{short_password}</code>\n"
            "Короткая ссылка: <code>{short_link}</code></blockquote>"
        ),
        "call_create_failed": (
            "<blockquote><b>Не удалось создать звонок</b></blockquote>\n"
            "<blockquote>{error}</blockquote>"
        ),
        "active_calls": (
            "<blockquote><b>Активные звонки</b></blockquote>\n"
            "<blockquote>Всего: <b>{count}</b></blockquote>"
        ),
        "no_active_calls": (
            "<blockquote><b>Нет активных звонков</b></blockquote>\n"
            "<blockquote>Создайте новый звонок для начала</blockquote>"
        ),
        "call_info": (
            "<blockquote><b>Информация о звонке</b></blockquote>\n"
            "<blockquote>Call ID: <code>{call_id}</code>\n"
            "Ссылка: <code>{join_link}</code>\n"
            "Короткий ID: <code>{short_id}</code>\n"
            "Статус: {status}</blockquote>"
        ),
        "call_finished": (
            "<blockquote><b>Звонок завершен</b></blockquote>\n"
            "<blockquote>Call ID: <code>{call_id}</code></blockquote>"
        ),
        "call_finish_failed": (
            "<blockquote><b>Не удалось завершить звонок</b></blockquote>\n"
            "<blockquote>{error}</blockquote>"
        ),
        "loading": "<blockquote><b>Загрузка...</b></blockquote>",
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "VK_TOKEN",
                "",
                "VK user access_token",
                validator=loader.validators.Hidden(),
            ),
        )
        self._vk = VKCallsAPIClient()
        self._db = None
        self._client = None
        self._auth_sessions = {}
        self._bot_handler_bound = False

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

    def _owners(self):
        owners = set()
        if self._client:
            if getattr(self._client, "tg_id", None):
                owners.add(self._client.tg_id)
            dispatcher = getattr(self._client, "dispatcher", None)
            security = getattr(dispatcher, "security", None)
            if security and hasattr(security, "_owner"):
                owners.update(security._owner or [])
        return owners

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        await self._ensure_vk()
        self._bind_bot_handler()

    def _bind_bot_handler(self):
        if self._bot_handler_bound:
            return
        bot = getattr(getattr(self, "inline", None), "_bot_client", None)
        if bot:
            bot.add_event_handler(
                self._on_bot_message,
                events.NewMessage(incoming=True, func=lambda e: e.is_private),
            )
            self._bot_handler_bound = True

    async def _on_bot_message(self, event):
        sender_id = event.sender_id
        if sender_id not in self._owners():
            return

        text = (event.raw_text or "").strip()
        if text.startswith(("/start vk_auth", "/start vkauth")):
            prompt = await event.respond(
                self.strings["auth_bot_prompt"], parse_mode="html"
            )
            self._auth_sessions[sender_id] = [event.id, prompt.id]
            return

        if sender_id in self._auth_sessions:
            token = extract_vk_token(text)
            if not token:
                resp = await event.respond(
                    self.strings["auth_invalid"], parse_mode="html"
                )
            else:
                ok = await self._vk.auth(token)
                if ok:
                    self.config["VK_TOKEN"] = token
                    resp = await event.respond(
                        self.strings["auth_success_bot"].format(
                            user_id=self._vk.user_id or "?"
                        ),
                        parse_mode="html",
                    )
                else:
                    resp = await event.respond(
                        self.strings["auth_failed_bot"], parse_mode="html"
                    )

            tracked = self._auth_sessions.pop(sender_id, [])
            all_ids = tracked + [event.id, resp.id]

            async def _cleanup():
                await asyncio.sleep(10)
                try:
                    await event.client.delete_messages(event.chat_id, all_ids)
                except Exception:
                    pass

            asyncio.ensure_future(_cleanup())

    async def on_unload(self):
        bot = getattr(getattr(self, "inline", None), "_bot_client", None)
        if bot and self._bot_handler_bound:
            try:
                bot.remove_event_handler(self._on_bot_message)
            except Exception:
                pass
            self._bot_handler_bound = False
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
                [
                    {
                        "text": self.strings["btn_create"],
                        "callback": self._cb_create_call,
                        "style": "success",
                    }
                ],
                [
                    {
                        "text": self.strings["btn_active"],
                        "callback": self._cb_active_calls,
                        "style": "primary",
                    }
                ],
                [
                    {
                        "text": self.strings["btn_auth"],
                        "callback": self._cb_auth_menu,
                        "style": "primary",
                    }
                ],
            ],
        )

    async def _cb_auth_menu(self, call: InlineCall):
        bot_uname = getattr(getattr(self, "inline", None), "bot_username", None) or "bot"
        deeplink = f"https://t.me/{bot_uname}?start=vk_auth"
        await call.edit(
            self.strings["auth_menu"],
            reply_markup=[
                [{"text": self.strings["auth_open"], "url": VK_HOST_URL}],
                [{"text": self.strings["btn_auth_bot"], "url": deeplink}],
                [
                    {
                        "text": self.strings["auth_input"],
                        "input": self.strings["auth_input"],
                        "handler": self._input_auth,
                        "style": "success",
                    }
                ],
                [
                    {
                        "text": self.strings["btn_back"],
                        "callback": self._cb_main_menu,
                        "style": "primary",
                    }
                ],
            ],
        )

    async def _input_auth(self, call: InlineCall, auth_url: str):
        token = extract_vk_token(auth_url)
        if not token:
            await call.edit(self.strings["auth_invalid"])
            return

        ok = await self._vk.auth(token)
        if ok:
            self.config["VK_TOKEN"] = token
            await call.edit(
                self.strings["auth_success"].format(user_id=self._vk.user_id or "?"),
                reply_markup=[
                    [
                        {
                            "text": self.strings["btn_back"],
                            "callback": self._cb_main_menu,
                            "style": "primary",
                        }
                    ]
                ],
            )
        else:
            await call.edit(
                self.strings["auth_failed"].format(error="users.get failed"),
                reply_markup=[
                    [
                        {
                            "text": self.strings["btn_back"],
                            "callback": self._cb_main_menu,
                            "style": "primary",
                        }
                    ]
                ],
            )

    async def _cb_create_call(self, call: InlineCall):
        if not await self._ensure_vk():
            await call.edit(
                self.strings["not_authorized"],
                reply_markup=[
                    [
                        {
                            "text": self.strings["btn_back"],
                            "callback": self._cb_main_menu,
                            "style": "primary",
                        }
                    ]
                ],
            )
            return

        await call.edit(self.strings["loading"])

        try:
            resp = await self._vk.start_call()
            call_id = (resp.get("call_id") or "").strip()
            join_link = (resp.get("join_link") or "").strip()

            sc = resp.get("short_credentials") if isinstance(resp, dict) else None
            short_id = sc.get("id", "N/A") if sc else "N/A"
            short_password = sc.get("password", "N/A") if sc else "N/A"
            short_link = sc.get("link_with_password", "N/A") if sc else "N/A"

            if call_id:
                self._add_call(
                    call_id,
                    {
                        "call_id": call_id,
                        "join_link": join_link,
                        "short_credentials": sc,
                    },
                )

            await call.edit(
                self.strings["call_created"].format(
                    call_id=call_id or "?",
                    join_link=join_link or "?",
                    short_id=short_id,
                    short_password=short_password,
                    short_link=short_link,
                ),
                reply_markup=[
                    [
                        {
                            "text": self.strings["btn_back"],
                            "callback": self._cb_main_menu,
                            "style": "primary",
                        }
                    ]
                ],
            )
        except VKAPIError as e:
            code = e.error.get("error_code")
            msg = e.error.get("error_msg")
            await call.edit(
                self.strings["call_create_failed"].format(error=f"{code}: {msg}"),
                reply_markup=[
                    [
                        {
                            "text": self.strings["btn_back"],
                            "callback": self._cb_main_menu,
                            "style": "primary",
                        }
                    ]
                ],
            )
        except Exception as e:
            await call.edit(
                self.strings["call_create_failed"].format(error=str(e)[:200]),
                reply_markup=[
                    [
                        {
                            "text": self.strings["btn_back"],
                            "callback": self._cb_main_menu,
                            "style": "primary",
                        }
                    ]
                ],
            )

    async def _cb_active_calls(self, call: InlineCall):
        calls_dict = self._get_calls()
        calls = list(calls_dict.values())

        if not calls:
            await call.edit(
                self.strings["no_active_calls"],
                reply_markup=[
                    [
                        {
                            "text": self.strings["btn_back"],
                            "callback": self._cb_main_menu,
                            "style": "primary",
                        }
                    ]
                ],
            )
            return

        buttons = []
        for c in calls:
            call_id = c.get("call_id", "")
            short_id = ""
            if isinstance(c.get("short_credentials"), dict):
                short_id = c["short_credentials"].get("id", "")
            label = short_id if short_id else call_id[:8]
            buttons.append(
                [
                    {
                        "text": label,
                        "callback": self._cb_call_detail,
                        "args": (call_id,),
                        "style": "primary",
                    }
                ]
            )

        buttons.append(
            [
                {
                    "text": self.strings["btn_back"],
                    "callback": self._cb_main_menu,
                    "style": "primary",
                }
            ]
        )

        await call.edit(
            self.strings["active_calls"].format(count=len(calls)),
            reply_markup=buttons,
        )

    async def _cb_call_detail(self, call: InlineCall, call_id: str):
        calls_dict = self._get_calls()
        call_data = calls_dict.get(call_id)

        if not call_data:
            await call.edit(
                "<blockquote><b>Call not found</b></blockquote>",
                reply_markup=[
                    [
                        {
                            "text": self.strings["btn_back"],
                            "callback": self._cb_active_calls,
                            "style": "primary",
                        }
                    ]
                ],
            )
            return

        join_link = call_data.get("join_link", "N/A")
        short_id = "N/A"
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
                [
                    {
                        "text": self.strings["btn_finish"],
                        "callback": self._cb_finish_call,
                        "args": (call_id,),
                        "style": "danger",
                    }
                ],
                [
                    {
                        "text": self.strings["btn_back"],
                        "callback": self._cb_active_calls,
                        "style": "primary",
                    }
                ],
            ],
        )

    async def _cb_finish_call(self, call: InlineCall, call_id: str):
        if not await self._ensure_vk():
            await call.edit(
                self.strings["not_authorized"],
                reply_markup=[
                    [
                        {
                            "text": self.strings["btn_back"],
                            "callback": self._cb_main_menu,
                            "style": "primary",
                        }
                    ]
                ],
            )
            return

        await call.edit(self.strings["loading"])

        try:
            ok = await self._vk.force_finish(call_id)
            if ok:
                self._remove_call(call_id)
                await call.edit(
                    self.strings["call_finished"].format(call_id=call_id),
                    reply_markup=[
                        [
                            {
                                "text": self.strings["btn_back"],
                                "callback": self._cb_active_calls,
                                "style": "primary",
                            }
                        ]
                    ],
                )
            else:
                await call.edit(
                    self.strings["call_finish_failed"].format(error="VK returned false"),
                    reply_markup=[
                        [
                            {
                                "text": self.strings["btn_back"],
                                "callback": self._cb_active_calls,
                                "style": "primary",
                            }
                        ]
                    ],
                )
        except VKAPIError as e:
            code = e.error.get("error_code")
            msg = e.error.get("error_msg")
            await call.edit(
                self.strings["call_finish_failed"].format(error=f"{code}: {msg}"),
                reply_markup=[
                    [
                        {
                            "text": self.strings["btn_back"],
                            "callback": self._cb_active_calls,
                            "style": "primary",
                        }
                    ]
                ],
            )
        except Exception as e:
            await call.edit(
                self.strings["call_finish_failed"].format(error=str(e)[:200]),
                reply_markup=[
                    [
                        {
                            "text": self.strings["btn_back"],
                            "callback": self._cb_active_calls,
                            "style": "primary",
                        }
                    ]
                ],
            )

    @loader.command(ru_doc="Менеджер VK звонков", en_doc="VK calls manager")
    async def vkcall(self, message):
        """VK calls manager"""
        await self.inline.form(
            text=self.strings["main_menu"],
            message=message,
            reply_markup=[
                [
                    {
                        "text": self.strings["btn_create"],
                        "callback": self._cb_create_call,
                        "style": "success",
                    }
                ],
                [
                    {
                        "text": self.strings["btn_active"],
                        "callback": self._cb_active_calls,
                        "style": "primary",
                    }
                ],
                [
                    {
                        "text": self.strings["btn_auth"],
                        "callback": self._cb_auth_menu,
                        "style": "primary",
                    }
                ],
            ],
            silent=True,
        )