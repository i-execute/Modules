__version__ = (1, 0, 0)
# meta developer: I_execute.t.me

import asyncio
import logging
import re
import time

import aiohttp
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import (
    Message,
    TextWithEntities,
)
from telethon.tl.functions.payments import (
    GetStarGiftsRequest,
    GetPaymentFormRequest,
    SendStarsFormRequest,
)
from telethon.tl.types import InputInvoiceStarGift
from telethon.errors import BadRequestError

from .. import loader, utils
from ..inline.types import InlineCall

logger = logging.getLogger(__name__)

BOT_TOKEN_PATTERN = re.compile(r'\b\d{8,10}:[A-Za-z0-9_-]{35}\b')
STRING_SESSION_PATTERN = re.compile(r'1[A-Za-z0-9_-]{200,}={0,2}')

MAX_ACCOUNTS = 10

DC_IP_MAP = {
    1: "149.154.175.53",
    2: "149.154.167.51",
    3: "149.154.175.100",
    4: "149.154.167.91",
    5: "91.108.56.130",
}


def _esc(text):
    if not text:
        return ""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _get_full_name(user):
    if not user:
        return "Unknown"
    first = getattr(user, "first_name", "") or ""
    last = getattr(user, "last_name", "") or ""
    return f"{first} {last}".strip() or "Unknown"


def _get_username(entity):
    if hasattr(entity, "username") and entity.username:
        return entity.username
    if hasattr(entity, "usernames") and entity.usernames:
        for u in entity.usernames:
            if getattr(u, "active", False):
                return u.username
    return None


@loader.tds
class Gift(loader.Module):
    """Send Telegram Star gifts via inline form"""

    strings = {
        "name": "Gift",
        "main_menu": (
            "<b>Gift</b>\n"
            "<blockquote>Select sender account type</blockquote>"
        ),
        "btn_user": "User",
        "btn_bot": "Bot",
        "btn_connected": "Connected",
        "btn_back": "Back",
        "btn_close": "Close",
        "bot_menu": (
            "<b>Bot Account</b>\n"
            "<blockquote>Enter bot token or select connected bot below</blockquote>"
        ),
        "btn_add_token": "Enter Token",
        "input_token": "Send bot token from @BotFather:",
        "bot_testing": (
            "<b>Validating token...</b>\n"
            "<blockquote>Please wait</blockquote>"
        ),
        "bot_invalid": (
            "<b>Invalid Token</b>\n"
            "<blockquote>Bot API rejected this token</blockquote>"
        ),
        "bot_valid": (
            "<b>Bot Connected</b>\n"
            "<blockquote>@{username} — ready to send gifts</blockquote>"
        ),
        "connected_menu": (
            "<b>Connected Accounts</b>\n"
            "<blockquote>Select account to send from</blockquote>"
        ),
        "connected_empty": (
            "<b>No Connected Accounts</b>\n"
            "<blockquote>Add sessions by replying to a session message with <code>.gift</code></blockquote>"
        ),
        "target_menu": (
            "<b>Select Recipient Type</b>\n"
            "<blockquote>Sending from: {from_info}</blockquote>"
        ),
        "btn_to_user": "User",
        "btn_to_channel": "Channel",
        "input_user_id": "Send user ID or @username:",
        "input_channel_id": "Send channel ID (with or without -100):",
        "resolving": (
            "<b>Resolving...</b>\n"
            "<blockquote>Looking up recipient</blockquote>"
        ),
        "not_found": (
            "<b>Not Found</b>\n"
            "<blockquote>Could not resolve this ID or username</blockquote>"
        ),
        "gift_id_menu": (
            "<b>Gift ID</b>\n"
            "<blockquote>Recipient: {target_name}\n"
            "Enter gift ID to send</blockquote>"
        ),
        "input_gift_id": "Send gift ID (numeric):",
        "gift_checking": (
            "<b>Checking gift...</b>\n"
            "<blockquote>Verifying gift ID exists</blockquote>"
        ),
        "gift_invalid": (
            "<b>Invalid Gift</b>\n"
            "<blockquote>Gift with this ID does not exist</blockquote>"
        ),
        "gift_found": (
            "<b>Gift Found</b>\n"
            "<blockquote>ID: <code>{gift_id}</code>\n"
            "Price: <b>{stars} stars</b></blockquote>"
        ),
        "count_menu": (
            "<b>Quantity</b>\n"
            "<blockquote>Gift: <code>{gift_id}</code> ({stars} stars)\n"
            "How many gifts to send?</blockquote>"
        ),
        "input_count": "Send quantity (number >= 1):",
        "count_invalid": (
            "<b>Invalid Quantity</b>\n"
            "<blockquote>Must be a positive integer</blockquote>"
        ),
        "comment_menu": (
            "<b>Comment</b>\n"
            "<blockquote>Add a message to the gift or skip</blockquote>"
        ),
        "input_comment": "Send comment text:",
        "btn_skip_comment": "Skip",
        "confirm_menu": (
            "<b>Confirm Gift</b>\n"
            "<blockquote>From: {from_info}\n"
            "To: {target_name} ({to_type})\n"
            "Gift ID: <code>{gift_id}</code>\n"
            "Price: <b>{stars} stars</b>\n"
            "Count: <b>{count}</b>\n"
            "Comment: {comment}</blockquote>"
        ),
        "btn_confirm": "Confirm",
        "btn_cancel": "Cancel",
        "sending": (
            "<b>Sending...</b>\n"
            "<blockquote>{current}/{total} gifts sent</blockquote>"
        ),
        "sent_ok": (
            "<b>Done</b>\n"
            "<blockquote>Successfully sent {count} gift(s) to {target_name}</blockquote>"
        ),
        "sent_partial": (
            "<b>Partially Sent</b>\n"
            "<blockquote>Sent {success}/{total}\n"
            "Error: {error}</blockquote>"
        ),
        "sent_fail": (
            "<b>Failed</b>\n"
            "<blockquote>Error: {error}</blockquote>"
        ),
        "balance_low": (
            "<b>Insufficient Balance</b>\n"
            "<blockquote>Not enough stars to send this gift</blockquote>"
        ),
        "connect_menu": (
            "<b>Connect Session</b>\n"
            "<blockquote>Detected: <code>{type}</code>\n"
            "Connect this session?</blockquote>"
        ),
        "connect_select_dc": (
            "<b>Select DC for HEX session</b>"
        ),
        "connecting": (
            "<b>Connecting...</b>\n"
            "<blockquote>Please wait</blockquote>"
        ),
        "connect_ok": (
            "<b>Connected</b>\n"
            "<blockquote>Slot: <code>{slot}</code>\n"
            "Name: {user}\n"
            "ID: <code>{uid}</code></blockquote>"
        ),
        "connect_fail_invalid": (
            "<b>Error</b>\n"
            "<blockquote>Session is invalid or not authorized</blockquote>"
        ),
        "connect_fail_banned": (
            "<b>Error</b>\n"
            "<blockquote>Account is banned</blockquote>"
        ),
        "connect_fail_revoked": (
            "<b>Error</b>\n"
            "<blockquote>Session was revoked</blockquote>"
        ),
        "connect_fail_error": (
            "<b>Error</b>\n"
            "<blockquote><code>{err}</code></blockquote>"
        ),
        "connect_slots_full": (
            "<b>Limit Reached</b>\n"
            "<blockquote>Maximum {max} accounts connected</blockquote>"
        ),
        "btn_dc_1": "DC 1",
        "btn_dc_2": "DC 2",
        "btn_dc_3": "DC 3",
        "btn_dc_4": "DC 4",
        "btn_dc_5": "DC 5",
        "btn_connect": "Connect",
    }

    strings_ru = {
        "main_menu": (
            "<b>Gift</b>\n"
            "<blockquote>Выберите тип аккаунта отправителя</blockquote>"
        ),
        "btn_user": "Пользователь",
        "btn_bot": "Бот",
        "btn_connected": "Подключённые",
        "btn_back": "Назад",
        "btn_close": "Закрыть",
        "bot_menu": (
            "<b>Бот-аккаунт</b>\n"
            "<blockquote>Введите токен бота или выберите подключённого ниже</blockquote>"
        ),
        "btn_add_token": "Ввести токен",
        "input_token": "Отправьте токен бота из @BotFather:",
        "bot_testing": (
            "<b>Проверяем токен...</b>\n"
            "<blockquote>Пожалуйста, подождите</blockquote>"
        ),
        "bot_invalid": (
            "<b>Неверный токен</b>\n"
            "<blockquote>Bot API отклонил этот токен</blockquote>"
        ),
        "bot_valid": (
            "<b>Бот подключён</b>\n"
            "<blockquote>@{username} — готов к отправке подарков</blockquote>"
        ),
        "connected_menu": (
            "<b>Подключённые аккаунты</b>\n"
            "<blockquote>Выберите аккаунт для отправки</blockquote>"
        ),
        "connected_empty": (
            "<b>Нет подключённых аккаунтов</b>\n"
            "<blockquote>Добавьте сессию, ответив на сообщение с сессией командой <code>.gift</code></blockquote>"
        ),
        "target_menu": (
            "<b>Тип получателя</b>\n"
            "<blockquote>Отправка с: {from_info}</blockquote>"
        ),
        "btn_to_user": "Пользователь",
        "btn_to_channel": "Канал",
        "input_user_id": "Отправьте ID пользователя или @username:",
        "input_channel_id": "Отправьте ID канала (с -100 или без):",
        "resolving": (
            "<b>Поиск...</b>\n"
            "<blockquote>Определяем получателя</blockquote>"
        ),
        "not_found": (
            "<b>Не найдено</b>\n"
            "<blockquote>Не удалось найти этот ID или username</blockquote>"
        ),
        "gift_id_menu": (
            "<b>ID подарка</b>\n"
            "<blockquote>Получатель: {target_name}\n"
            "Введите ID подарка</blockquote>"
        ),
        "input_gift_id": "Отправьте ID подарка (числовой):",
        "gift_checking": (
            "<b>Проверяем подарок...</b>\n"
            "<blockquote>Проверяем существование ID</blockquote>"
        ),
        "gift_invalid": (
            "<b>Неверный подарок</b>\n"
            "<blockquote>Подарок с таким ID не существует</blockquote>"
        ),
        "gift_found": (
            "<b>Подарок найден</b>\n"
            "<blockquote>ID: <code>{gift_id}</code>\n"
            "Цена: <b>{stars} звёзд</b></blockquote>"
        ),
        "count_menu": (
            "<b>Количество</b>\n"
            "<blockquote>Подарок: <code>{gift_id}</code> ({stars} звёзд)\n"
            "Сколько подарков отправить?</blockquote>"
        ),
        "input_count": "Отправьте количество (число >= 1):",
        "count_invalid": (
            "<b>Неверное количество</b>\n"
            "<blockquote>Должно быть положительным целым числом</blockquote>"
        ),
        "comment_menu": (
            "<b>Комментарий</b>\n"
            "<blockquote>Добавьте сообщение к подарку или пропустите</blockquote>"
        ),
        "input_comment": "Отправьте текст комментария:",
        "btn_skip_comment": "Пропустить",
        "confirm_menu": (
            "<b>Подтверждение</b>\n"
            "<blockquote>От: {from_info}\n"
            "Кому: {target_name} ({to_type})\n"
            "ID подарка: <code>{gift_id}</code>\n"
            "Цена: <b>{stars} звёзд</b>\n"
            "Количество: <b>{count}</b>\n"
            "Комментарий: {comment}</blockquote>"
        ),
        "btn_confirm": "Подтвердить",
        "btn_cancel": "Отмена",
        "sending": (
            "<b>Отправляем...</b>\n"
            "<blockquote>{current}/{total} подарков отправлено</blockquote>"
        ),
        "sent_ok": (
            "<b>Готово</b>\n"
            "<blockquote>Успешно отправлено {count} подарок(ов) — {target_name}</blockquote>"
        ),
        "sent_partial": (
            "<b>Частично отправлено</b>\n"
            "<blockquote>Отправлено {success}/{total}\n"
            "Ошибка: {error}</blockquote>"
        ),
        "sent_fail": (
            "<b>Ошибка</b>\n"
            "<blockquote>Ошибка: {error}</blockquote>"
        ),
        "balance_low": (
            "<b>Недостаточно звёзд</b>\n"
            "<blockquote>Не хватает баланса для отправки</blockquote>"
        ),
        "connect_menu": (
            "<b>Подключение сессии</b>\n"
            "<blockquote>Обнаружено: <code>{type}</code>\n"
            "Подключить эту сессию?</blockquote>"
        ),
        "connect_select_dc": (
            "<b>Выберите DC для HEX-сессии</b>"
        ),
        "connecting": (
            "<b>Подключаемся...</b>\n"
            "<blockquote>Пожалуйста, подождите</blockquote>"
        ),
        "connect_ok": (
            "<b>Подключено</b>\n"
            "<blockquote>Слот: <code>{slot}</code>\n"
            "Имя: {user}\n"
            "ID: <code>{uid}</code></blockquote>"
        ),
        "connect_fail_invalid": (
            "<b>Ошибка</b>\n"
            "<blockquote>Сессия невалидна или не авторизована</blockquote>"
        ),
        "connect_fail_banned": (
            "<b>Ошибка</b>\n"
            "<blockquote>Аккаунт заблокирован</blockquote>"
        ),
        "connect_fail_revoked": (
            "<b>Ошибка</b>\n"
            "<blockquote>Сессия отозвана</blockquote>"
        ),
        "connect_fail_error": (
            "<b>Ошибка</b>\n"
            "<blockquote><code>{err}</code></blockquote>"
        ),
        "connect_slots_full": (
            "<b>Лимит достигнут</b>\n"
            "<blockquote>Максимум {max} аккаунтов подключено</blockquote>"
        ),
        "btn_dc_1": "DC 1",
        "btn_dc_2": "DC 2",
        "btn_dc_3": "DC 3",
        "btn_dc_4": "DC 4",
        "btn_dc_5": "DC 5",
        "btn_connect": "Подключить",
    }

    def __init__(self):
        self._client = None
        self._db = None
        self._owner_id = None
        self._gifts_cache = None
        self._gifts_cache_time = 0
        self._sessions = {}
        self._next_slot = 1

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        me = await client.get_me()
        self._owner_id = me.id
        await self._load_sessions()

    def _save_sessions(self):
        data = []
        for slot, info in self._sessions.items():
            entry = (
                f"{info['user_id']}|{info['name']}|"
                f"{info.get('username') or 'None'}|"
                f"{info['type']}|{info['credential']}"
            )
            data.append(entry)
        self._db.set("Gift", "sessions", data)

    async def _load_sessions(self):
        self._sessions = {}
        self._next_slot = 1
        for raw in (self._db.get("Gift", "sessions", []) or []):
            try:
                parts = raw.split("|", 4)
                if len(parts) < 5:
                    continue
                user_id = int(parts[0])
                name = parts[1]
                username = parts[2] if parts[2] != "None" else None
                acc_type = parts[3]
                credential = parts[4]
                slot = self._next_slot
                self._next_slot += 1
                if acc_type == "user":
                    cl = TelegramClient(
                        StringSession(credential),
                        api_id=self._client.api_id,
                        api_hash=self._client.api_hash,
                    )
                    await cl.connect()
                    if not await cl.is_user_authorized():
                        await cl.disconnect()
                        continue
                    self._sessions[slot] = {
                        "type": "user",
                        "credential": credential,
                        "user_id": user_id,
                        "name": name,
                        "username": username,
                        "client": cl,
                    }
                else:
                    self._sessions[slot] = {
                        "type": "bot",
                        "credential": credential,
                        "user_id": user_id,
                        "name": name,
                        "username": username,
                    }
            except Exception:
                pass

    def _get_next_slot(self):
        for i in range(1, MAX_ACCOUNTS + 1):
            if i not in self._sessions:
                return i
        return None

    async def _get_gifts(self, force=False):
        if not force and self._gifts_cache and (time.time() - self._gifts_cache_time) < 300:
            return self._gifts_cache
        try:
            r = await self._client(GetStarGiftsRequest(hash=0))
            self._gifts_cache = r.gifts
            self._gifts_cache_time = time.time()
            return self._gifts_cache
        except Exception:
            return None

    async def _check_gift_id(self, gift_id: int):
        gifts = await self._get_gifts(force=True)
        if gifts:
            for g in gifts:
                if g.id == gift_id:
                    return True, g.stars
        try:
            from telethon.tl.types import InputPeerSelf
            me_input = await self._client.get_input_entity(self._owner_id)
            inv = InputInvoiceStarGift(me_input, gift_id, message=TextWithEntities("", []))
            form = await self._client(GetPaymentFormRequest(inv))
            stars = "?"
            if hasattr(form, "invoice") and form.invoice and form.invoice.prices:
                stars = form.invoice.prices[0].amount
            return True, stars
        except Exception:
            return False, None

    async def _send_gift_user(self, client, target_id, gift_id, text=None):
        try:
            target = await client.get_input_entity(target_id)
            inv = InputInvoiceStarGift(target, gift_id, message=TextWithEntities(text or "", []))
            form = await client(GetPaymentFormRequest(inv))
            await client(SendStarsFormRequest(form.form_id, inv))
            return True, None
        except BadRequestError as e:
            return False, "BALANCE_TOO_LOW" if "BALANCE_TOO_LOW" in str(e) else str(e)
        except Exception as e:
            return False, str(e)

    async def _send_gift_bot(self, token, target_id, gift_id, is_channel, text=None):
        try:
            p = {"gift_id": str(gift_id)}
            if is_channel:
                p["chat_id"] = int(f"-100{target_id}") if target_id > 0 else target_id
            else:
                p["user_id"] = target_id
            if text:
                p["text"] = text[:255]
            async with aiohttp.ClientSession() as s:
                async with s.post(
                    f"https://api.telegram.org/bot{token}/sendGift", json=p
                ) as r:
                    d = await r.json()
                    if d.get("ok"):
                        return True, None
                    return False, d.get("description", "Unknown")
        except Exception as e:
            return False, str(e)

    async def _check_bot_token(self, token):
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(
                    f"https://api.telegram.org/bot{token}/getMe",
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as r:
                    d = await r.json()
                    if d.get("ok"):
                        return (
                            d["result"].get("username"),
                            d["result"].get("id"),
                            d["result"].get("first_name", "Bot"),
                        )
        except Exception:
            pass
        return None, None, None

    async def _connect_string_session(self, call: InlineCall, string_session: str):
        await call.edit(self.strings["connecting"])
        slot = self._get_next_slot()
        if slot is None:
            await call.edit(self.strings["connect_slots_full"].format(max=MAX_ACCOUNTS))
            return
        try:
            cl = TelegramClient(
                StringSession(string_session),
                api_id=self._client.api_id,
                api_hash=self._client.api_hash,
            )
            await asyncio.wait_for(cl.connect(), timeout=15)
            me = await asyncio.wait_for(cl.get_me(), timeout=10)
            if not me:
                await cl.disconnect()
                await call.edit(self.strings["connect_fail_invalid"])
                return
            name = _get_full_name(me)
            username = _get_username(me)
            self._sessions[slot] = {
                "type": "user",
                "credential": string_session,
                "user_id": me.id,
                "name": name,
                "username": username,
                "client": cl,
            }
            self._next_slot = max(self._next_slot, slot + 1)
            self._save_sessions()
            await call.edit(
                self.strings["connect_ok"].format(
                    slot=slot, user=_esc(name), uid=me.id
                ),
                reply_markup=[[{
                    "text": self.strings["btn_back"],
                    "callback": self._cb_main,
                    "style": "danger",
                }]],
            )
        except Exception as e:
            err = str(e)
            if "AUTH_KEY_UNREGISTERED" in err or "SESSION_REVOKED" in err:
                await call.edit(self.strings["connect_fail_revoked"])
            elif "USER_DEACTIVATED" in err:
                await call.edit(self.strings["connect_fail_banned"])
            else:
                await call.edit(
                    self.strings["connect_fail_error"].format(err=_esc(err[:200]))
                )

    async def _connect_hex_dc(self, call: InlineCall, hex_key: str, dc_id: int):
        import base64
        import ipaddress
        import struct
        try:
            auth_key = bytes.fromhex(hex_key)
            if len(auth_key) != 256:
                await call.edit(self.strings["connect_fail_invalid"])
                return
            ip = ipaddress.IPv4Address(DC_IP_MAP[dc_id])
            data = struct.pack(">B4sH256s", dc_id, ip.packed, 443, auth_key)
            string_session = "1" + base64.urlsafe_b64encode(data).decode()
            await self._connect_string_session(call, string_session)
        except Exception:
            await call.edit(self.strings["connect_fail_invalid"])

    def _state_key(self, call):
        return f"gift_state_{call.form.get('uid', 'x')}"

    def _get_state(self, call):
        return self.get(self._state_key(call), {})

    def _set_state(self, call, data):
        self.set(self._state_key(call), data)

    def _clear_state(self, call):
        self.set(self._state_key(call), {})

    def _main_markup(self):
        return [
            [
                {"text": self.strings["btn_user"], "callback": self._cb_select_user, "style": "primary"},
                {"text": self.strings["btn_bot"], "callback": self._cb_bot_menu, "style": "primary"},
            ],
            [{"text": self.strings["btn_connected"], "callback": self._cb_connected_menu, "style": "success"}],
            [{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}],
        ]

    async def _cb_main(self, call: InlineCall):
        self._clear_state(call)
        await call.edit(self.strings["main_menu"], reply_markup=self._main_markup())

    async def _cb_close(self, call: InlineCall):
        self._clear_state(call)
        await call.delete()

    async def _cb_select_user(self, call: InlineCall):
        me = await self._client.get_me()
        state = {
            "from_type": "user",
            "from_info": _esc(_get_full_name(me)),
            "client_slot": None,
        }
        self._set_state(call, state)
        await self._show_target_menu(call)

    async def _cb_bot_menu(self, call: InlineCall):
        bots = {s: i for s, i in self._sessions.items() if i["type"] == "bot"}
        rows = []
        for slot, info in bots.items():
            label = f"@{info['username']}" if info.get("username") else info["name"]
            rows.append([{
                "text": label,
                "callback": self._cb_select_bot_slot,
                "args": (slot,),
                "style": "primary",
            }])
        rows.append([{
            "text": self.strings["btn_add_token"],
            "input": self.strings["input_token"],
            "handler": self._cb_handle_token,
            "style": "success",
        }])
        rows.append([{
            "text": self.strings["btn_back"],
            "callback": self._cb_main,
            "style": "danger",
        }])
        await call.edit(self.strings["bot_menu"], reply_markup=rows)

    async def _cb_handle_token(self, call: InlineCall, token: str):
        token = token.strip()
        await call.edit(self.strings["bot_testing"])
        username, uid, name = await self._check_bot_token(token)
        if not username:
            await call.edit(
                self.strings["bot_invalid"],
                reply_markup=[[{
                    "text": self.strings["btn_back"],
                    "callback": self._cb_bot_menu,
                    "style": "danger",
                }]],
            )
            return
        slot = self._get_next_slot()
        if slot:
            self._sessions[slot] = {
                "type": "bot",
                "credential": token,
                "user_id": uid,
                "name": name,
                "username": username,
            }
            self._next_slot = max(self._next_slot, slot + 1)
            self._save_sessions()
        state = {
            "from_type": "bot",
            "from_info": f"@{username}",
            "bot_token": token,
            "client_slot": None,
        }
        self._set_state(call, state)
        await call.edit(
            self.strings["bot_valid"].format(username=username),
            reply_markup=[[{
                "text": self.strings["btn_back"],
                "callback": self._cb_bot_menu,
                "style": "danger",
            }]],
        )
        await asyncio.sleep(1)
        await self._show_target_menu(call)

    async def _cb_select_bot_slot(self, call: InlineCall, slot: int):
        info = self._sessions.get(slot)
        if not info:
            await self._cb_bot_menu(call)
            return
        state = {
            "from_type": "bot",
            "from_info": f"@{info.get('username') or info['name']}",
            "bot_token": info["credential"],
            "client_slot": slot,
        }
        self._set_state(call, state)
        await self._show_target_menu(call)

    async def _cb_connected_menu(self, call: InlineCall):
        if not self._sessions:
            await call.edit(
                self.strings["connected_empty"],
                reply_markup=[[{
                    "text": self.strings["btn_back"],
                    "callback": self._cb_main,
                    "style": "danger",
                }]],
            )
            return
        rows = []
        for slot, info in self._sessions.items():
            t = "Bot" if info["type"] == "bot" else "User"
            label = f"[{t}] {info['name']}"
            rows.append([{
                "text": label,
                "callback": self._cb_select_connected_slot,
                "args": (slot,),
                "style": "primary",
            }])
        rows.append([{
            "text": self.strings["btn_back"],
            "callback": self._cb_main,
            "style": "danger",
        }])
        await call.edit(self.strings["connected_menu"], reply_markup=rows)

    async def _cb_select_connected_slot(self, call: InlineCall, slot: int):
        info = self._sessions.get(slot)
        if not info:
            await self._cb_connected_menu(call)
            return
        label = f"@{info.get('username') or info['name']}"
        if info["type"] == "user":
            state = {
                "from_type": "user_slot",
                "from_info": _esc(label),
                "client_slot": slot,
            }
        else:
            state = {
                "from_type": "bot",
                "from_info": _esc(label),
                "bot_token": info["credential"],
                "client_slot": slot,
            }
        self._set_state(call, state)
        await self._show_target_menu(call)

    async def _show_target_menu(self, call: InlineCall):
        state = self._get_state(call)
        await call.edit(
            self.strings["target_menu"].format(from_info=state.get("from_info", "?")),
            reply_markup=[
                [
                    {
                        "text": self.strings["btn_to_user"],
                        "input": self.strings["input_user_id"],
                        "handler": self._cb_got_user_target,
                        "style": "primary",
                    },
                    {
                        "text": self.strings["btn_to_channel"],
                        "input": self.strings["input_channel_id"],
                        "handler": self._cb_got_channel_target,
                        "style": "primary",
                    },
                ],
                [{"text": self.strings["btn_back"], "callback": self._cb_main, "style": "danger"}],
            ],
        )

    async def _cb_got_user_target(self, call: InlineCall, raw: str):
        await call.edit(self.strings["resolving"])
        target_id, target_name = await self._resolve(raw.strip())
        if not target_id:
            await call.edit(
                self.strings["not_found"],
                reply_markup=[[{
                    "text": self.strings["btn_back"],
                    "callback": self._show_target_menu,
                    "style": "danger",
                }]],
            )
            return
        state = self._get_state(call)
        state.update({
            "to_type": "user",
            "target_id": target_id,
            "target_name": _esc(target_name),
        })
        self._set_state(call, state)
        await self._show_gift_id_menu(call)

    async def _cb_got_channel_target(self, call: InlineCall, raw: str):
        await call.edit(self.strings["resolving"])
        raw = raw.strip()
        if re.match(r'^\d+$', raw):
            raw = f"-100{raw}"
        target_id, target_name = await self._resolve(raw)
        if not target_id:
            await call.edit(
                self.strings["not_found"],
                reply_markup=[[{
                    "text": self.strings["btn_back"],
                    "callback": self._show_target_menu,
                    "style": "danger",
                }]],
            )
            return
        chan_id = abs(target_id)
        state = self._get_state(call)
        state.update({
            "to_type": "channel",
            "target_id": chan_id,
            "target_name": _esc(target_name),
        })
        self._set_state(call, state)
        await self._show_gift_id_menu(call)

    async def _resolve(self, raw: str):
        try:
            entity = await self._client.get_entity(
                int(raw) if raw.lstrip("-").isdigit() else raw
            )
            name = getattr(entity, "title", None) or _get_full_name(entity)
            return entity.id, name
        except Exception:
            return None, None

    async def _show_gift_id_menu(self, call: InlineCall):
        state = self._get_state(call)
        await call.edit(
            self.strings["gift_id_menu"].format(target_name=state.get("target_name", "?")),
            reply_markup=[
                [{
                    "text": "Enter Gift ID",
                    "input": self.strings["input_gift_id"],
                    "handler": self._cb_got_gift_id,
                    "style": "success",
                }],
                [{"text": self.strings["btn_back"], "callback": self._show_target_menu, "style": "danger"}],
            ],
        )

    async def _cb_got_gift_id(self, call: InlineCall, raw: str):
        raw = raw.strip()
        try:
            gift_id = int(raw)
        except ValueError:
            await call.edit(
                self.strings["gift_invalid"],
                reply_markup=[[{
                    "text": self.strings["btn_back"],
                    "callback": self._show_gift_id_menu,
                    "style": "danger",
                }]],
            )
            return
        await call.edit(self.strings["gift_checking"])
        exists, stars = await self._check_gift_id(gift_id)
        if not exists:
            await call.edit(
                self.strings["gift_invalid"],
                reply_markup=[[{
                    "text": self.strings["btn_back"],
                    "callback": self._show_gift_id_menu,
                    "style": "danger",
                }]],
            )
            return
        state = self._get_state(call)
        state.update({"gift_id": gift_id, "gift_stars": stars})
        self._set_state(call, state)
        await call.edit(
            self.strings["gift_found"].format(gift_id=gift_id, stars=stars),
            reply_markup=[[{
                "text": "Continue",
                "callback": self._show_count_menu,
                "style": "success",
            }]],
        )
        await asyncio.sleep(0.8)
        await self._show_count_menu(call)

    async def _show_count_menu(self, call: InlineCall):
        state = self._get_state(call)
        await call.edit(
            self.strings["count_menu"].format(
                gift_id=state.get("gift_id", "?"),
                stars=state.get("gift_stars", "?"),
            ),
            reply_markup=[
                [{
                    "text": "Enter Count",
                    "input": self.strings["input_count"],
                    "handler": self._cb_got_count,
                    "style": "success",
                }],
                [{"text": self.strings["btn_back"], "callback": self._show_gift_id_menu, "style": "danger"}],
            ],
        )

    async def _cb_got_count(self, call: InlineCall, raw: str):
        try:
            count = int(raw.strip())
            assert count >= 1
        except Exception:
            await call.edit(
                self.strings["count_invalid"],
                reply_markup=[[{
                    "text": self.strings["btn_back"],
                    "callback": self._show_count_menu,
                    "style": "danger",
                }]],
            )
            return
        state = self._get_state(call)
        state["count"] = count
        self._set_state(call, state)
        await self._show_comment_menu(call)

    async def _show_comment_menu(self, call: InlineCall):
        await call.edit(
            self.strings["comment_menu"],
            reply_markup=[
                [{
                    "text": "Enter Comment",
                    "input": self.strings["input_comment"],
                    "handler": self._cb_got_comment,
                    "style": "primary",
                }],
                [{"text": self.strings["btn_skip_comment"], "callback": self._cb_skip_comment, "style": "success"}],
                [{"text": self.strings["btn_back"], "callback": self._show_count_menu, "style": "danger"}],
            ],
        )

    async def _cb_got_comment(self, call: InlineCall, raw: str):
        state = self._get_state(call)
        state["comment"] = raw.strip()[:255]
        self._set_state(call, state)
        await self._show_confirm(call)

    async def _cb_skip_comment(self, call: InlineCall):
        state = self._get_state(call)
        state["comment"] = None
        self._set_state(call, state)
        await self._show_confirm(call)

    async def _show_confirm(self, call: InlineCall):
        state = self._get_state(call)
        comment_display = _esc(state.get("comment")) if state.get("comment") else "-"
        await call.edit(
            self.strings["confirm_menu"].format(
                from_info=state.get("from_info", "?"),
                target_name=state.get("target_name", "?"),
                to_type=state.get("to_type", "?"),
                gift_id=state.get("gift_id", "?"),
                stars=state.get("gift_stars", "?"),
                count=state.get("count", 1),
                comment=comment_display,
            ),
            reply_markup=[
                [
                    {"text": self.strings["btn_confirm"], "callback": self._cb_do_send, "style": "success"},
                    {"text": self.strings["btn_cancel"], "callback": self._cb_close, "style": "danger"},
                ],
            ],
        )

    async def _cb_do_send(self, call: InlineCall):
        state = self._get_state(call)
        from_type = state.get("from_type")
        target_id = state.get("target_id")
        gift_id = state.get("gift_id")
        count = state.get("count", 1)
        comment = state.get("comment")
        to_type = state.get("to_type", "user")
        is_channel = to_type == "channel"
        target_name = state.get("target_name", str(target_id))

        success, errors = 0, []

        for i in range(count):
            await call.edit(self.strings["sending"].format(current=i, total=count))

            if from_type == "bot":
                token = state.get("bot_token")
                ok, err = await self._send_gift_bot(token, target_id, gift_id, is_channel, comment)
            elif from_type == "user":
                ok, err = await self._send_gift_user(self._client, target_id, gift_id, comment)
            elif from_type == "user_slot":
                slot = state.get("client_slot")
                info = self._sessions.get(slot)
                cl = info.get("client") if info else None
                if not cl:
                    await call.edit(
                        self.strings["sent_fail"].format(error="Session not connected"),
                        reply_markup=[[{
                            "text": self.strings["btn_close"],
                            "callback": self._cb_close,
                            "style": "danger",
                        }]],
                    )
                    return
                ok, err = await self._send_gift_user(cl, target_id, gift_id, comment)
            else:
                ok, err = False, "Unknown sender type"

            if ok:
                success += 1
            else:
                errors.append(err or "Unknown")
                if err and "BALANCE_TOO_LOW" in err:
                    break

            if count > 1 and i < count - 1:
                await asyncio.sleep(0.5)

        self._clear_state(call)

        back_row = [[{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}]]

        if success == count:
            await call.edit(
                self.strings["sent_ok"].format(count=success, target_name=target_name),
                reply_markup=back_row,
            )
        elif success > 0:
            await call.edit(
                self.strings["sent_partial"].format(
                    success=success,
                    total=count,
                    error=_esc(errors[0] if errors else "Unknown"),
                ),
                reply_markup=back_row,
            )
        else:
            err = errors[0] if errors else "Unknown"
            if "BALANCE_TOO_LOW" in err:
                await call.edit(self.strings["balance_low"], reply_markup=back_row)
            else:
                await call.edit(
                    self.strings["sent_fail"].format(error=_esc(err[:200])),
                    reply_markup=back_row,
                )

    @loader.command(
        ru_doc="Открыть форму отправки подарка",
        en_doc="Open gift sending form",
    )
    async def gift(self, message: Message):
        """Open gift sending form"""
        reply = await message.get_reply_message()
        if reply:
            text = reply.text or ""

            ms = STRING_SESSION_PATTERN.search(text)
            if ms:
                string_session = ms.group(0)
                slot = self._get_next_slot()
                if slot is None:
                    await self.inline.form(
                        text=self.strings["connect_slots_full"].format(max=MAX_ACCOUNTS),
                        message=message,
                        reply_markup=[[{
                            "text": self.strings["btn_close"],
                            "callback": self._cb_close,
                            "style": "danger",
                        }]],
                        silent=True,
                    )
                    return
                await self.inline.form(
                    text=self.strings["connect_menu"].format(type="String"),
                    message=message,
                    reply_markup=[
                        [{
                            "text": self.strings["btn_connect"],
                            "callback": self._cb_connect_string_wrap,
                            "args": (string_session,),
                            "style": "success",
                        }],
                        [{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}],
                    ],
                    silent=True,
                )
                return

            hex_m = re.search(r'[0-9a-fA-F]{512}', text)
            if hex_m:
                hex_key = hex_m.group(0)
                slot = self._get_next_slot()
                if slot is None:
                    await self.inline.form(
                        text=self.strings["connect_slots_full"].format(max=MAX_ACCOUNTS),
                        message=message,
                        reply_markup=[[{
                            "text": self.strings["btn_close"],
                            "callback": self._cb_close,
                            "style": "danger",
                        }]],
                        silent=True,
                    )
                    return
                await self.inline.form(
                    text=self.strings["connect_select_dc"],
                    message=message,
                    reply_markup=[
                        [
                            {"text": self.strings["btn_dc_1"], "callback": self._cb_connect_hex_wrap, "args": (hex_key, 1), "style": "primary"},
                            {"text": self.strings["btn_dc_2"], "callback": self._cb_connect_hex_wrap, "args": (hex_key, 2), "style": "primary"},
                            {"text": self.strings["btn_dc_3"], "callback": self._cb_connect_hex_wrap, "args": (hex_key, 3), "style": "primary"},
                        ],
                        [
                            {"text": self.strings["btn_dc_4"], "callback": self._cb_connect_hex_wrap, "args": (hex_key, 4), "style": "primary"},
                            {"text": self.strings["btn_dc_5"], "callback": self._cb_connect_hex_wrap, "args": (hex_key, 5), "style": "primary"},
                        ],
                        [{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}],
                    ],
                    silent=True,
                )
                return

        await self.inline.form(
            text=self.strings["main_menu"],
            message=message,
            reply_markup=self._main_markup(),
            silent=True,
        )

    async def _cb_connect_string_wrap(self, call: InlineCall, string_session: str):
        await self._connect_string_session(call, string_session)

    async def _cb_connect_hex_wrap(self, call: InlineCall, hex_key: str, dc_id: int):
        await self._connect_hex_dc(call, hex_key, dc_id)

    async def on_unload(self):
        for info in self._sessions.values():
            cl = info.get("client")
            if cl:
                try:
                    await cl.disconnect()
                except Exception:
                    pass