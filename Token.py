__version__ = (1, 1, 0)
# meta developer: I_execute.t.me

import os
import re
import io
import logging
import tempfile
import shutil
import asyncio

import aiohttp
from telethon import TelegramClient, events
from telethon.tl.types import Message

from .. import loader, utils
from ..inline.types import InlineCall

logger = logging.getLogger(__name__)

BOT_TOKEN_PATTERN = re.compile(r'\b\d{8,10}:[A-Za-z0-9_-]{35}\b')
EMOJI_PATTERN = re.compile(r'<emoji document_id=(\d+)>([^<]*)</emoji>')


def escape_html(text):
    if not text:
        return ""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def convert_emoji_for_bot(text):
    if not text:
        return text
    return EMOJI_PATTERN.sub(r'<tg-emoji emoji-id=\1>\2</tg-emoji>', text)


def get_full_name(user):
    if not user:
        return "Unknown"
    first = getattr(user, "first_name", "") or ""
    last = getattr(user, "last_name", "") or ""
    return f"{first} {last}".strip() or "Unknown"


def get_username(entity):
    if hasattr(entity, "username") and entity.username:
        return entity.username
    if hasattr(entity, "usernames") and entity.usernames:
        for u in entity.usernames:
            if getattr(u, "active", False):
                return u.username
        return entity.usernames[0].username
    return None


@loader.tds
class Token(loader.Module):
    """Bot token manager - info, dialogs and broadcast via bot tokens"""

    strings = {
        "name": "Token",
        "main_menu": (
            "<b>Token Manager</b>\n"
            "<blockquote>Connected bots: {count}\n"
            "{bot_list}</blockquote>"
        ),
        "main_menu_empty": (
            "<b>Token Manager</b>\n"
            "<blockquote>No bots connected yet</blockquote>"
        ),
        "btn_add_token": "Add Token",
        "btn_refresh": "Refresh",
        "btn_back": "Back",
        "btn_close": "Close",
        "btn_info": "Users Info",
        "btn_adv": "Broadcast",
        "btn_remove": "Remove",
        "btn_adv_set_text": "Set Text",
        "btn_adv_set_link": "Set Media URL",
        "btn_adv_test": "Test Send",
        "btn_adv_start": "Start Broadcast",
        "btn_adv_clear": "Clear",
        "input_token": "Send bot token (from @BotFather):",
        "input_adv_text": "Send broadcast text (HTML supported):",
        "input_adv_link": "Send media URL for broadcast:",
        "token_added": (
            "<b>Token Added</b>\n"
            "<blockquote>Bot: @{username}\n"
            "ID: <code>{bot_id}</code>\n"
            "Number: [{num}]</blockquote>"
        ),
        "token_invalid": (
            "<b>Invalid Token</b>\n"
            "<blockquote>The token was not accepted by Telegram API</blockquote>"
        ),
        "token_already": (
            "<b>Already Added</b>\n"
            "<blockquote>This bot is already connected as [{num}]</blockquote>"
        ),
        "token_not_found": (
            "<b>Token Not Found</b>\n"
            "<blockquote>No valid token found in the text</blockquote>"
        ),
        "list_empty": (
            "<b>No Bots</b>\n"
            "<blockquote>No bots connected yet</blockquote>"
        ),
        "bot_not_found": (
            "<b>Not Found</b>\n"
            "<blockquote>Bot [{num}] not found</blockquote>"
        ),
        "bot_view": (
            "<b>Bot: @{username}</b>\n"
            "<blockquote>ID: <code>{bot_id}</code>\n"
            "Number: [{num}]\n"
            "Known users: {user_count}</blockquote>"
        ),
        "info_processing": (
            "<b>Collecting Info</b>\n"
            "<blockquote>Bot [{num}] - fetching user list...</blockquote>"
        ),
        "info_no_dialogs": (
            "<b>No Users</b>\n"
            "<blockquote>Bot [{num}] has no known users yet.\n"
            "Users appear after they send /start to the bot.</blockquote>"
        ),
        "bot_removed": (
            "<b>Bot Removed</b>\n"
            "<blockquote>Bot [{num}] has been removed</blockquote>"
        ),
        "adv_menu": (
            "<b>Broadcast - Bot [{num}] @{username}</b>\n"
            "<blockquote>Text: {text_status}\n"
            "Media URL: {link_status}\n"
            "Known users: {user_count}</blockquote>"
        ),
        "adv_text_set": (
            "<b>Text Set</b>\n"
            "<blockquote>Broadcast text has been saved</blockquote>"
        ),
        "adv_link_set": (
            "<b>Media URL Set</b>\n"
            "<blockquote>{url}</blockquote>"
        ),
        "adv_link_no_url": (
            "<b>No URL Found</b>\n"
            "<blockquote>No valid URL found in the text</blockquote>"
        ),
        "adv_no_text": (
            "<b>No Text</b>\n"
            "<blockquote>Set broadcast text first</blockquote>"
        ),
        "adv_test_sent": (
            "<b>Test Sent</b>\n"
            "<blockquote>Test message sent to you</blockquote>"
        ),
        "adv_test_error": (
            "<b>Test Failed</b>\n"
            "<blockquote>{error}</blockquote>"
        ),
        "adv_sending": (
            "<b>Broadcasting</b>\n"
            "<blockquote>{current}/{total} sent...</blockquote>"
        ),
        "adv_done": (
            "<b>Broadcast Complete</b>\n"
            "<blockquote>Sent: {success}/{total}\n"
            "Failed: {failed}</blockquote>"
        ),
        "adv_no_users": (
            "<b>No Users</b>\n"
            "<blockquote>No known users to send to.\n"
            "Users appear after they send /start to the bot.</blockquote>"
        ),
        "adv_cleared": (
            "<b>Cleared</b>\n"
            "<blockquote>Broadcast state cleared</blockquote>"
        ),
        "error": (
            "<b>Error</b>\n"
            "<blockquote>{error}</blockquote>"
        ),
        "status_set": "Set",
        "status_not_set": "Not set",
        "collecting_users": (
            "<b>Collecting Users</b>\n"
            "<blockquote>Fetching updates from bot API...\n"
            "This may take a moment.</blockquote>"
        ),
    }

    strings_ru = {
        "main_menu": (
            "<b>Token Manager</b>\n"
            "<blockquote>Подключено ботов: {count}\n"
            "{bot_list}</blockquote>"
        ),
        "main_menu_empty": (
            "<b>Token Manager</b>\n"
            "<blockquote>Ботов пока не добавлено</blockquote>"
        ),
        "btn_add_token": "Добавить токен",
        "btn_refresh": "Обновить",
        "btn_back": "Назад",
        "btn_close": "Закрыть",
        "btn_info": "Инфо о юзерах",
        "btn_adv": "Рассылка",
        "btn_remove": "Удалить",
        "btn_adv_set_text": "Задать текст",
        "btn_adv_set_link": "Задать URL медиа",
        "btn_adv_test": "Тест",
        "btn_adv_start": "Запустить рассылку",
        "btn_adv_clear": "Очистить",
        "input_token": "Отправьте токен бота (из @BotFather):",
        "input_adv_text": "Отправьте текст рассылки (поддерживается HTML):",
        "input_adv_link": "Отправьте URL медиа для рассылки:",
        "token_added": (
            "<b>Токен добавлен</b>\n"
            "<blockquote>Бот: @{username}\n"
            "ID: <code>{bot_id}</code>\n"
            "Номер: [{num}]</blockquote>"
        ),
        "token_invalid": (
            "<b>Невалидный токен</b>\n"
            "<blockquote>Telegram API не принял токен</blockquote>"
        ),
        "token_already": (
            "<b>Уже добавлен</b>\n"
            "<blockquote>Этот бот уже подключен как [{num}]</blockquote>"
        ),
        "token_not_found": (
            "<b>Токен не найден</b>\n"
            "<blockquote>В тексте не найден валидный токен</blockquote>"
        ),
        "list_empty": (
            "<b>Нет ботов</b>\n"
            "<blockquote>Ботов пока не добавлено</blockquote>"
        ),
        "bot_not_found": (
            "<b>Не найдено</b>\n"
            "<blockquote>Бот [{num}] не найден</blockquote>"
        ),
        "bot_view": (
            "<b>Бот: @{username}</b>\n"
            "<blockquote>ID: <code>{bot_id}</code>\n"
            "Номер: [{num}]\n"
            "Известных юзеров: {user_count}</blockquote>"
        ),
        "info_processing": (
            "<b>Сбор информации</b>\n"
            "<blockquote>Бот [{num}] - получаем список юзеров...</blockquote>"
        ),
        "info_no_dialogs": (
            "<b>Нет юзеров</b>\n"
            "<blockquote>У бота [{num}] пока нет известных юзеров.\n"
            "Юзеры появляются после отправки /start боту.</blockquote>"
        ),
        "bot_removed": (
            "<b>Бот удален</b>\n"
            "<blockquote>Бот [{num}] удален</blockquote>"
        ),
        "adv_menu": (
            "<b>Рассылка - Бот [{num}] @{username}</b>\n"
            "<blockquote>Текст: {text_status}\n"
            "URL медиа: {link_status}\n"
            "Известных юзеров: {user_count}</blockquote>"
        ),
        "adv_text_set": (
            "<b>Текст задан</b>\n"
            "<blockquote>Текст рассылки сохранен</blockquote>"
        ),
        "adv_link_set": (
            "<b>URL задан</b>\n"
            "<blockquote>{url}</blockquote>"
        ),
        "adv_link_no_url": (
            "<b>URL не найден</b>\n"
            "<blockquote>В тексте не найден валидный URL</blockquote>"
        ),
        "adv_no_text": (
            "<b>Нет текста</b>\n"
            "<blockquote>Сначала задайте текст рассылки</blockquote>"
        ),
        "adv_test_sent": (
            "<b>Тест отправлен</b>\n"
            "<blockquote>Тестовое сообщение отправлено вам</blockquote>"
        ),
        "adv_test_error": (
            "<b>Тест не удался</b>\n"
            "<blockquote>{error}</blockquote>"
        ),
        "adv_sending": (
            "<b>Рассылка</b>\n"
            "<blockquote>Отправлено {current}/{total}...</blockquote>"
        ),
        "adv_done": (
            "<b>Рассылка завершена</b>\n"
            "<blockquote>Отправлено: {success}/{total}\n"
            "Ошибок: {failed}</blockquote>"
        ),
        "adv_no_users": (
            "<b>Нет юзеров</b>\n"
            "<blockquote>Нет известных юзеров для рассылки.\n"
            "Юзеры появляются после отправки /start боту.</blockquote>"
        ),
        "adv_cleared": (
            "<b>Очищено</b>\n"
            "<blockquote>Состояние рассылки очищено</blockquote>"
        ),
        "error": (
            "<b>Ошибка</b>\n"
            "<blockquote>{error}</blockquote>"
        ),
        "status_set": "Задан",
        "status_not_set": "Не задан",
        "collecting_users": (
            "<b>Сбор юзеров</b>\n"
            "<blockquote>Запрашиваем обновления из Bot API...\n"
            "Это может занять момент.</blockquote>"
        ),
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "tokens",
                "",
                lambda: "Bot tokens (hidden, managed by UI)",
                validator=loader.validators.Hidden(),
            ),
        )
        self._owner_id = None
        self._temp_dir = None
        self._adv_states = {}

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        me = await client.get_me()
        self._owner_id = me.id
        self._temp_dir = os.path.join(
            tempfile.gettempdir(), f"Token_{self._owner_id}"
        )
        os.makedirs(self._temp_dir, exist_ok=True)
        logger.info(f"[Token] Module ready. Owner ID: {self._owner_id}. Temp dir: {self._temp_dir}")

    def _get_tokens(self):
        raw = self.config["tokens"]
        if not raw or not raw.strip():
            return []
        return [t.strip() for t in raw.split("\n") if t.strip()]

    def _set_tokens(self, tokens):
        self.config["tokens"] = "\n".join(tokens)

    def _get_known_users(self, token_id):
        key = f"known_users_{token_id}"
        raw = self._db.get("Token", key, "")
        if not raw:
            return {}
        result = {}
        for line in raw.split("\n"):
            if not line.strip():
                continue
            parts = line.split("|", 2)
            if len(parts) >= 1:
                try:
                    uid = int(parts[0])
                    name = parts[1] if len(parts) > 1 else "Unknown"
                    username = parts[2] if len(parts) > 2 else ""
                    result[uid] = {"name": name, "username": username}
                except Exception:
                    pass
        return result

    def _set_known_users(self, token_id, users: dict):
        key = f"known_users_{token_id}"
        lines = []
        for uid, data in users.items():
            name = data.get("name", "Unknown")
            username = data.get("username", "")
            lines.append(f"{uid}|{name}|{username}")
        self._db.set("Token", key, "\n".join(lines))

    def _add_known_user(self, token_id, uid, name, username):
        users = self._get_known_users(token_id)
        users[uid] = {"name": name, "username": username or ""}
        self._set_known_users(token_id, users)

    def _get_token_id(self, token):
        return token.split(":")[0]

    def _clean_session(self, path):
        for ext in ("", ".session", ".session-journal"):
            p = path + ext if ext else path
            if os.path.exists(p):
                try:
                    os.remove(p)
                except Exception:
                    pass

    async def _create_bot_client(self, token):
        session_name = token.split(":")[0]
        session_path = os.path.join(self._temp_dir, f"bot_{session_name}")
        self._clean_session(session_path)
        logger.info(f"[Token] Creating bot client for token id {session_name}, session path: {session_path}")
        bot_client = TelegramClient(
            session_path,
            api_id=self._client.api_id,
            api_hash=self._client.api_hash,
        )
        await bot_client.start(bot_token=token)
        me = await bot_client.get_me()
        logger.info(f"[Token] Bot client started: @{me.username} (id={me.id})")
        return bot_client, session_path

    async def _destroy_bot_client(self, bot_client, session_path):
        try:
            await bot_client.disconnect()
            logger.info(f"[Token] Bot client disconnected, session: {session_path}")
        except Exception as e:
            logger.warning(f"[Token] Error disconnecting bot client: {e}")
        self._clean_session(session_path)

    async def _validate_token_api(self, token):
        logger.info(f"[Token] Validating token via HTTP API: {token[:10]}...")
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(f"https://api.telegram.org/bot{token}/getMe") as r:
                    d = await r.json()
                    if d.get("ok"):
                        logger.info(f"[Token] Token valid: @{d['result'].get('username')}")
                        return d["result"]
                    else:
                        logger.warning(f"[Token] Token invalid, API response: {d}")
        except Exception as e:
            logger.error(f"[Token] Token validation HTTP error: {e}")
        return None

    async def _fetch_users_via_api(self, token):
        logger.info(f"[Token] Fetching users via getUpdates for token {token[:10]}...")
        token_id = self._get_token_id(token)
        users = self._get_known_users(token_id)
        logger.info(f"[Token] Already known users from DB: {len(users)}")

        try:
            async with aiohttp.ClientSession() as s:
                offset = -100
                fetched = 0
                while True:
                    url = f"https://api.telegram.org/bot{token}/getUpdates?offset={offset}&limit=100&timeout=0"
                    logger.info(f"[Token] getUpdates request offset={offset}")
                    async with s.get(url) as r:
                        data = await r.json()
                        if not data.get("ok"):
                            logger.warning(f"[Token] getUpdates failed: {data}")
                            break
                        updates = data.get("result", [])
                        logger.info(f"[Token] getUpdates returned {len(updates)} updates")
                        if not updates:
                            break
                        for upd in updates:
                            update_id = upd.get("update_id", 0)
                            if update_id >= offset:
                                offset = update_id + 1
                            for key in ("message", "callback_query", "inline_query", "my_chat_member"):
                                obj = upd.get(key)
                                if not obj:
                                    continue
                                sender = obj.get("from") or obj.get("chat")
                                if not sender:
                                    continue
                                uid = sender.get("id")
                                if not uid:
                                    continue
                                if sender.get("is_bot"):
                                    continue
                                first = sender.get("first_name", "") or ""
                                last = sender.get("last_name", "") or ""
                                name = f"{first} {last}".strip() or "Unknown"
                                username = sender.get("username", "") or ""
                                if uid not in users:
                                    logger.info(f"[Token] New user found: id={uid} name={name} username={username}")
                                    fetched += 1
                                users[uid] = {"name": name, "username": username}
                        if len(updates) < 100:
                            break
                logger.info(f"[Token] getUpdates done. Total known users: {len(users)}, newly found: {fetched}")
        except Exception as e:
            logger.error(f"[Token] getUpdates error: {e}")

        self._set_known_users(token_id, users)
        return users

    def _remove_token(self, index):
        tokens = self._get_tokens()
        if 0 <= index < len(tokens):
            tokens.pop(index)
            self._set_tokens(tokens)
            logger.info(f"[Token] Token at index {index} removed")

    def _format_bot_list(self, bots):
        if not bots:
            return ""
        lines = []
        for i, (username, valid) in enumerate(bots):
            mark = "OK" if valid else "INVALID"
            lines.append(f"{mark} [{i+1}] @{username}")
        return "\n".join(lines)

    async def _build_main_text_and_markup(self, call=None):
        tokens = self._get_tokens()
        if not tokens:
            text = self.strings["main_menu_empty"]
            markup = [
                [{"text": self.strings["btn_add_token"], "input": self.strings["input_token"], "handler": self._cb_add_token, "style": "success"}],
                [{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}],
            ]
            return text, markup

        bot_lines = []
        for i, token in enumerate(tokens):
            info = await self._validate_token_api(token)
            if info:
                bot_lines.append(f"OK [{i+1}] @{info.get('username', 'unknown')}")
            else:
                bot_lines.append(f"INVALID [{i+1}]")

        bot_list = "\n".join(bot_lines)
        text = self.strings["main_menu"].format(count=len(tokens), bot_list=bot_list)

        rows = []
        for i, token in enumerate(tokens):
            rows.append([{"text": f"[{i+1}]", "callback": self._cb_bot_view, "args": (i,), "style": "primary"}])

        rows.append([{"text": self.strings["btn_add_token"], "input": self.strings["input_token"], "handler": self._cb_add_token, "style": "success"}])
        rows.append([{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}])
        return text, rows

    async def _cb_main_menu(self, call: InlineCall):
        text, markup = await self._build_main_text_and_markup(call)
        await call.edit(text, reply_markup=markup)

    async def _cb_close(self, call: InlineCall):
        await call.delete()

    async def _cb_add_token(self, call: InlineCall, query: str):
        text = query.strip()
        m = BOT_TOKEN_PATTERN.search(text)
        if not m:
            await call.edit(
                self.strings["token_not_found"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]],
            )
            return

        token = m.group(0)
        logger.info(f"[Token] Attempting to add token: {token[:10]}...")
        info = await self._validate_token_api(token)
        if not info:
            await call.edit(
                self.strings["token_invalid"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]],
            )
            return

        bot_id = info["id"]
        tokens = self._get_tokens()
        for i, t in enumerate(tokens):
            t_info = await self._validate_token_api(t)
            if t_info and t_info["id"] == bot_id:
                await call.edit(
                    self.strings["token_already"].format(num=i + 1),
                    reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]],
                )
                return

        tokens.append(token)
        self._set_tokens(tokens)
        logger.info(f"[Token] Token added: @{info.get('username')} as [{len(tokens)}]")

        await call.edit(
            self.strings["token_added"].format(
                num=len(tokens),
                username=info.get("username", "unknown"),
                bot_id=bot_id,
            ),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]],
        )

    async def _cb_bot_view(self, call: InlineCall, index: int):
        tokens = self._get_tokens()
        if index < 0 or index >= len(tokens):
            await call.edit(
                self.strings["bot_not_found"].format(num=index + 1),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]],
            )
            return

        token = tokens[index]
        info = await self._validate_token_api(token)
        if not info:
            await call.edit(
                self.strings["token_invalid"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]],
            )
            self._remove_token(index)
            return

        token_id = self._get_token_id(token)
        known_users = self._get_known_users(token_id)
        logger.info(f"[Token] Bot view for [{index+1}] @{info.get('username')}, known users: {len(known_users)}")

        await call.edit(
            self.strings["bot_view"].format(
                username=info.get("username", "unknown"),
                bot_id=info["id"],
                num=index + 1,
                user_count=len(known_users),
            ),
            reply_markup=[
                [
                    {"text": self.strings["btn_info"], "callback": self._cb_bot_info, "args": (index,), "style": "primary"},
                    {"text": self.strings["btn_adv"], "callback": self._cb_adv_menu, "args": (index,), "style": "primary"},
                ],
                [{"text": self.strings["btn_remove"], "callback": self._cb_bot_remove, "args": (index,), "style": "danger"}],
                [{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}],
            ],
        )

    async def _cb_bot_remove(self, call: InlineCall, index: int):
        num = index + 1
        self._remove_token(index)
        await call.edit(
            self.strings["bot_removed"].format(num=num),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]],
        )

    async def _cb_bot_info(self, call: InlineCall, index: int):
        tokens = self._get_tokens()
        if index < 0 or index >= len(tokens):
            await call.edit(
                self.strings["bot_not_found"].format(num=index + 1),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]],
            )
            return

        token = tokens[index]
        info = await self._validate_token_api(token)
        if not info:
            await call.edit(
                self.strings["token_invalid"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]],
            )
            self._remove_token(index)
            return

        await call.edit(self.strings["collecting_users"])
        logger.info(f"[Token] Starting user collection for bot [{index+1}] @{info.get('username')}")

        token_id = self._get_token_id(token)
        users = await self._fetch_users_via_api(token)
        logger.info(f"[Token] User collection done. Total users: {len(users)}")

        if not users:
            await call.edit(
                self.strings["info_no_dialogs"].format(num=index + 1),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_bot_view, "args": (index,), "style": "danger"}]],
            )
            return

        bot_username = info.get("username", "unknown")
        file_lines = [
            f"Bot: @{bot_username}",
            f"Total known users: {len(users)}",
            "=" * 50,
            "",
        ]
        for uid, udata in users.items():
            uname_display = f"@{udata['username']}" if udata["username"] else "no_username"
            file_lines.append(f"ID: {uid} | Name: {udata['name']} | Username: {uname_display}")

        content = "\n".join(file_lines)
        f = io.BytesIO(content.encode("utf-8"))
        f.name = f"bot_{index+1}_users.txt"

        logger.info(f"[Token] Sending user info file for bot [{index+1}]")
        try:
            chat_id = call.form.get("chat") or self._owner_id
            await self._client.send_file(
                chat_id,
                f,
                caption=f"<b>Bot [{index+1}] @{bot_username} - {len(users)} known users</b>",
                parse_mode="html",
            )
        except Exception as e:
            logger.error(f"[Token] Failed to send info file: {e}")

        await call.edit(
            self.strings["bot_view"].format(
                username=bot_username,
                bot_id=info["id"],
                num=index + 1,
                user_count=len(users),
            ),
            reply_markup=[
                [
                    {"text": self.strings["btn_info"], "callback": self._cb_bot_info, "args": (index,), "style": "primary"},
                    {"text": self.strings["btn_adv"], "callback": self._cb_adv_menu, "args": (index,), "style": "primary"},
                ],
                [{"text": self.strings["btn_remove"], "callback": self._cb_bot_remove, "args": (index,), "style": "danger"}],
                [{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}],
            ],
        )

    async def _cb_adv_menu(self, call: InlineCall, index: int):
        tokens = self._get_tokens()
        if index < 0 or index >= len(tokens):
            await call.edit(
                self.strings["bot_not_found"].format(num=index + 1),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]],
            )
            return

        token = tokens[index]
        info = await self._validate_token_api(token)
        if not info:
            await call.edit(
                self.strings["token_invalid"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]],
            )
            self._remove_token(index)
            return

        if index not in self._adv_states:
            self._adv_states[index] = {"text": None, "media_url": None}

        state = self._adv_states[index]
        token_id = self._get_token_id(token)
        known_users = self._get_known_users(token_id)

        text_status = self.strings["status_set"] if state.get("text") else self.strings["status_not_set"]
        link_status = self.strings["status_set"] if state.get("media_url") else self.strings["status_not_set"]

        await call.edit(
            self.strings["adv_menu"].format(
                num=index + 1,
                username=info.get("username", "unknown"),
                text_status=text_status,
                link_status=link_status,
                user_count=len(known_users),
            ),
            reply_markup=[
                [
                    {"text": self.strings["btn_adv_set_text"], "input": self.strings["input_adv_text"], "handler": self._cb_adv_set_text, "args": (index,), "style": "primary"},
                    {"text": self.strings["btn_adv_set_link"], "input": self.strings["input_adv_link"], "handler": self._cb_adv_set_link, "args": (index,), "style": "primary"},
                ],
                [
                    {"text": self.strings["btn_adv_test"], "callback": self._cb_adv_test, "args": (index,), "style": "primary"},
                    {"text": self.strings["btn_adv_start"], "callback": self._cb_adv_start, "args": (index,), "style": "success"},
                ],
                [{"text": self.strings["btn_adv_clear"], "callback": self._cb_adv_clear, "args": (index,), "style": "danger"}],
                [{"text": self.strings["btn_back"], "callback": self._cb_bot_view, "args": (index,), "style": "danger"}],
            ],
        )

    async def _cb_adv_set_text(self, call: InlineCall, query: str, index: int):
        if index not in self._adv_states:
            self._adv_states[index] = {"text": None, "media_url": None}
        self._adv_states[index]["text"] = query.strip()
        logger.info(f"[Token] Broadcast text set for bot [{index+1}]")
        await call.edit(
            self.strings["adv_text_set"],
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_adv_menu, "args": (index,), "style": "danger"}]],
        )

    async def _cb_adv_set_link(self, call: InlineCall, query: str, index: int):
        text = query.strip()
        url_match = re.search(r'https?://[^\s<>"\']+', text, re.IGNORECASE)
        if not url_match:
            await call.edit(
                self.strings["adv_link_no_url"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_adv_menu, "args": (index,), "style": "danger"}]],
            )
            return
        url = url_match.group(0)
        if index not in self._adv_states:
            self._adv_states[index] = {"text": None, "media_url": None}
        self._adv_states[index]["media_url"] = url
        logger.info(f"[Token] Broadcast media URL set for bot [{index+1}]: {url}")
        await call.edit(
            self.strings["adv_link_set"].format(url=url),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_adv_menu, "args": (index,), "style": "danger"}]],
        )

    async def _cb_adv_clear(self, call: InlineCall, index: int):
        if index in self._adv_states:
            del self._adv_states[index]
        logger.info(f"[Token] Broadcast state cleared for bot [{index+1}]")
        await call.edit(
            self.strings["adv_cleared"],
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_adv_menu, "args": (index,), "style": "danger"}]],
        )

    async def _cb_adv_test(self, call: InlineCall, index: int):
        state = self._adv_states.get(index)
        if not state or not state.get("text"):
            await call.edit(
                self.strings["adv_no_text"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_adv_menu, "args": (index,), "style": "danger"}]],
            )
            return

        tokens = self._get_tokens()
        if index < 0 or index >= len(tokens):
            await call.edit(
                self.strings["bot_not_found"].format(num=index + 1),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]],
            )
            return

        token = tokens[index]
        text = convert_emoji_for_bot(state["text"])
        media_url = state.get("media_url")

        logger.info(f"[Token] Test broadcast for bot [{index+1}] to owner {self._owner_id}")
        bot_client = None
        session_path = None
        try:
            bot_client, session_path = await self._create_bot_client(token)
            if media_url:
                await bot_client.send_file(self._owner_id, media_url, caption=text, parse_mode="html")
            else:
                await bot_client.send_message(self._owner_id, text, parse_mode="html")
            await self._destroy_bot_client(bot_client, session_path)
            logger.info(f"[Token] Test message sent successfully")
            await call.edit(
                self.strings["adv_test_sent"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_adv_menu, "args": (index,), "style": "danger"}]],
            )
        except Exception as e:
            logger.error(f"[Token] Test send error: {e}")
            if bot_client and session_path:
                await self._destroy_bot_client(bot_client, session_path)
            await call.edit(
                self.strings["adv_test_error"].format(error=str(e)[:200]),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_adv_menu, "args": (index,), "style": "danger"}]],
            )

    async def _cb_adv_start(self, call: InlineCall, index: int):
        state = self._adv_states.get(index)
        if not state or not state.get("text"):
            await call.edit(
                self.strings["adv_no_text"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_adv_menu, "args": (index,), "style": "danger"}]],
            )
            return

        tokens = self._get_tokens()
        if index < 0 or index >= len(tokens):
            await call.edit(
                self.strings["bot_not_found"].format(num=index + 1),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]],
            )
            return

        token = tokens[index]
        info = await self._validate_token_api(token)
        if not info:
            await call.edit(
                self.strings["token_invalid"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]],
            )
            self._remove_token(index)
            return

        token_id = self._get_token_id(token)
        await call.edit(self.strings["collecting_users"])
        logger.info(f"[Token] Collecting users before broadcast for bot [{index+1}]")
        users = await self._fetch_users_via_api(token)
        logger.info(f"[Token] Users collected: {len(users)}")

        if not users:
            await call.edit(
                self.strings["adv_no_users"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_adv_menu, "args": (index,), "style": "danger"}]],
            )
            return

        text = convert_emoji_for_bot(state["text"])
        media_url = state.get("media_url")
        user_ids = list(users.keys())
        total = len(user_ids)

        logger.info(f"[Token] Starting broadcast for bot [{index+1}] to {total} users")
        bot_client = None
        session_path = None
        try:
            bot_client, session_path = await self._create_bot_client(token)
            success = 0
            failed = 0

            for i, uid in enumerate(user_ids):
                try:
                    if media_url:
                        await bot_client.send_file(uid, media_url, caption=text, parse_mode="html")
                    else:
                        await bot_client.send_message(uid, text, parse_mode="html")
                    success += 1
                    logger.debug(f"[Token] Broadcast sent to {uid} ({i+1}/{total})")
                except Exception as e:
                    failed += 1
                    logger.warning(f"[Token] Broadcast failed to {uid}: {e}")

                if (i + 1) % 5 == 0:
                    try:
                        await call.edit(self.strings["adv_sending"].format(current=i + 1, total=total))
                    except Exception:
                        pass
                    await asyncio.sleep(0.5)

            await self._destroy_bot_client(bot_client, session_path)
            logger.info(f"[Token] Broadcast done. success={success}, failed={failed}, total={total}")

            await call.edit(
                self.strings["adv_done"].format(success=success, total=total, failed=failed),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_adv_menu, "args": (index,), "style": "danger"}]],
            )

        except Exception as e:
            logger.error(f"[Token] Broadcast error: {e}")
            if bot_client and session_path:
                await self._destroy_bot_client(bot_client, session_path)
            await call.edit(
                self.strings["error"].format(error=str(e)[:200]),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_adv_menu, "args": (index,), "style": "danger"}]],
            )

    @loader.command(
        ru_doc="Открыть менеджер токенов",
        en_doc="Open token manager",
    )
    async def token(self, message: Message):
        """Open token manager"""
        text, markup = await self._build_main_text_and_markup()
        await self.inline.form(
            text=text,
            message=message,
            reply_markup=markup,
            silent=True,
        )

    async def on_unload(self):
        self._adv_states.clear()
        if self._temp_dir and os.path.exists(self._temp_dir):
            shutil.rmtree(self._temp_dir, ignore_errors=True)