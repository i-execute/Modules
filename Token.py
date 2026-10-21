# CopyLeft 2026 github.com/i-execute
# Author: I_execute.t.me
# Licensed under AGPLv3.

__version__ = (2, 0, 0)
# meta developer: Execute_forge.t.me

import asyncio
import base64
import json
import logging
import os
import random
import re
import shutil
import subprocess
import urllib.request
from telethon import TelegramClient, types
from telethon.tl.functions.messages import GetMessagesRequest
from telethon.tl.types import (
    Message,
    MessageEmpty,
    PeerUser,
    PeerChat,
    PeerChannel,
    MessageEntityBold,
    MessageEntityItalic,
    MessageEntityCode,
    MessageEntityPre,
    MessageEntityTextUrl,
    MessageEntityUrl,
    MessageEntityUnderline,
    MessageEntityStrike,
    MessageEntitySpoiler,
    MessageEntityBlockquote,
    MessageEntityCustomEmoji,
    MessageEntityMention,
    MessageEntityBotCommand,
)
from telethon.errors import FloodWaitError

from .. import loader, utils
from ..inline.types import InlineCall

logger = logging.getLogger(__name__)

BOT_TOKEN_PATTERN = re.compile(r"(\d{8,12}:[A-Za-z0-9_-]{35})")
GITHUB_WEB_URL = "https://raw.githubusercontent.com/i-execute/Modules/main/Storage/Token/web/index.html"
PHOTO_DOWNLOAD_URL = "https://n.uguu.se/FQAHtVVH.jpg"
EMBEDDED_PHOTO_B64 = "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"


def _escape(text: str) -> str:
    if not text:
        return ""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _encrypt_payload(data_str: str, key: int | str) -> str:
    key_bytes = str(key).encode("utf-8")
    data_bytes = data_str.encode("utf-8")
    enc = bytearray(b ^ key_bytes[i % len(key_bytes)] for i, b in enumerate(data_bytes))
    return base64.b64encode(enc).decode("ascii")


def _format_layer229_message(text: str, entities: list) -> str:
    """Format Telegram message text with Layer 229 entities into safe rich HTML."""
    if not text:
        return ""
    if not entities:
        return _escape(text)

    u16 = text.encode("utf-16-le")
    insertions = []

    for e in entities:
        start = getattr(e, "offset", 0) * 2
        end = start + (getattr(e, "length", 0) * 2)

        open_tag = ""
        close_tag = ""

        if isinstance(e, MessageEntityBold):
            open_tag, close_tag = "<b>", "</b>"
        elif isinstance(e, MessageEntityItalic):
            open_tag, close_tag = "<i>", "</i>"
        elif isinstance(e, MessageEntityCode):
            open_tag, close_tag = "<code class=\"tg-code\">", "</code>"
        elif isinstance(e, MessageEntityPre):
            lang = getattr(e, "language", "") or ""
            cls = f" class=\"language-{lang} tg-pre\"" if lang else " class=\"tg-pre\""
            open_tag, close_tag = f"<pre><code{cls}>", "</code></pre>"
        elif isinstance(e, MessageEntityTextUrl):
            open_tag, close_tag = f"<a class=\"tg-link\" href=\"{_escape(e.url)}\" target=\"_blank\" rel=\"noopener\">", "</a>"
        elif isinstance(e, MessageEntityUrl):
            open_tag, close_tag = "<a class=\"tg-link\" href=\"", "\" target=\"_blank\" rel=\"noopener\">"
        elif isinstance(e, MessageEntityUnderline):
            open_tag, close_tag = "<u>", "</u>"
        elif isinstance(e, MessageEntityStrike):
            open_tag, close_tag = "<s>", "</s>"
        elif isinstance(e, MessageEntitySpoiler):
            open_tag, close_tag = "<span class=\"spoiler\">", "</span>"
        elif isinstance(e, MessageEntityBlockquote):
            open_tag, close_tag = "<blockquote>", "</blockquote>"
        elif isinstance(e, MessageEntityCustomEmoji):
            doc_id = getattr(e, "document_id", 0)
            open_tag, close_tag = f"<span class=\"tg-custom-emoji\" data-emoji-id=\"{doc_id}\">", "</span>"
        elif isinstance(e, (MessageEntityMention, MessageEntityBotCommand)):
            open_tag, close_tag = "<span class=\"tg-link\">", "</span>"
        else:
            continue

        insertions.append((start, 0, open_tag))
        insertions.append((end, -1, close_tag))

    insertions.sort(key=lambda x: (x[0], x[1]))

    res = []
    last_idx = 0
    for pos, _, tag in insertions:
        if pos > last_idx:
            chunk = u16[last_idx:pos].decode("utf-16-le", errors="ignore")
            res.append(_escape(chunk))
            last_idx = pos
        res.append(tag)
    if last_idx < len(u16):
        res.append(_escape(u16[last_idx:].decode("utf-16-le", errors="ignore")))

    return "".join(res)


@loader.tds
class Token(loader.Module):
    """Менеджер токенов ботов, дампа истории сообщений и Telegram Serverless Mini App"""

    strings = {
        "name": "Token",
        "_cls_doc": "Bot dumper and Serverless Mini App manager",
        "_cmd_doc_t": "Open Bot Dumper and Serverless Mini App manager",
        "_cfg_tgcloud_token": "CLI access token from @BotFather for tgcloud serverless deploy (app<id>:<secret>)",
        "_cfg_api_id": "Custom Telegram API ID for MTProto sessions (optional, falls back to client)",
        "_cfg_api_hash": "Custom Telegram API Hash for MTProto sessions (optional, falls back to client)",
        "_cfg_app_short_name": "Mini App short name in @BotFather to use t.me/<bot>/<app> links",
        "main_menu": (
            "<b>Token Dumper</b>\n"
            "<blockquote>Dumped bots: {count}\n"
            "{bot_list}</blockquote>"
        ),
        "main_menu_empty": (
            "<b>Token Dumper</b>\n"
            "<blockquote>No bots dumped yet. Press button below to add bot token.</blockquote>"
        ),
        "btn_add_token": "Add Bot Token",
        "btn_refresh": "Refresh",
        "btn_back": "Back",
        "btn_close": "Close",
        "btn_open_app": "Open Mini App",
        "btn_redump": "Re-dump",
        "btn_delete": "Delete",
        "btn_cancel": "Cancel",
        "input_token": "Send bot API token (from @BotFather):",
        "cli_token_needed": (
            "<b>CLI Token Not Configured</b>\n"
            "<blockquote>Configure your tgcloud CLI token first:\n"
            "<code>.cfg Token TGCLOUD_TOKEN app&lt;id&gt;:&lt;secret&gt;</code></blockquote>"
        ),
        "token_invalid": (
            "<b>Invalid Token</b>\n"
            "<blockquote>The token was not accepted by Telegram API</blockquote>"
        ),
        "bot_auth_failed": (
            "<b>Auth Error</b>\n"
            "<blockquote>Could not authorize bot via MTProto / Bot API</blockquote>"
        ),
        "flood_wait": (
            "<b>Telegram FloodWait</b>\n"
            "<blockquote>Need to wait {seconds} sec for MTProto session authorization</blockquote>"
        ),
        "dumping_progress": (
            "<b>Dumping History</b>\n"
            "<blockquote>Collecting messages and dialogs for @{username}...\n"
            "Please wait.</blockquote>"
        ),
        "dump_complete": (
            "<b>Dump Completed</b>\n"
            "<blockquote>Bot: <b>{name}</b> (@{username})\n"
            "ID: <code>{bot_id}</code>\n"
            "Dialogs: <b>{chats_count}</b>\n"
            "Messages: <b>{messages_count}</b>\n"
            "Access: <b>Owner Only (Encrypted)</b>\n"
            "PIN fallback: <code>{pin}</code>\n"
            "Mini App: <code>{app_url}</code></blockquote>"
        ),
        "bot_view": (
            "<b>Bot: {name} (@{username})</b>\n"
            "<blockquote>ID: <code>{bot_id}</code>\n"
            "Dialogs: <b>{chats_count}</b>\n"
            "Messages: <b>{messages_count}</b>\n"
            "Access: <b>Owner Only (Encrypted)</b>\n"
            "PIN fallback: <code>{pin}</code>\n"
            "Mini App: <code>{app_url}</code></blockquote>"
        ),
        "deleted": (
            "<b>Dump Deleted</b>\n"
            "<blockquote>Dump data and bot session removed successfully.</blockquote>"
        ),
    }

    strings_ru = {
        "name": "Token",
        "_cls_doc": "Менеджер дампа ботов и Telegram Serverless Mini App",
        "_cmd_doc_t": "Открыть менеджер дампа ботов и Mini App",
        "_cfg_tgcloud_token": "CLI Access токен от @BotFather для tgcloud деплоя (app<id>:<secret>)",
        "_cfg_api_id": "Telegram API ID для авторизации MTProto сессий (опционально)",
        "_cfg_api_hash": "Telegram API Hash для авторизации MTProto сессий (опционально)",
        "_cfg_app_short_name": "Short name приложения из @BotFather для ссылок t.me/<bot>/<app>",
        "main_menu": (
            "<b>Token Dumper</b>\n"
            "<blockquote>Сохраненные дампы: {count}\n"
            "{bot_list}</blockquote>"
        ),
        "main_menu_empty": (
            "<b>Token Dumper</b>\n"
            "<blockquote>Нет сохраненных ботов. Нажмите кнопку ниже для ввода токена.</blockquote>"
        ),
        "btn_add_token": "Ввести токен бота",
        "btn_refresh": "Обновить",
        "btn_back": "Назад",
        "btn_close": "Закрыть",
        "btn_open_app": "Открыть Mini App",
        "btn_redump": "Передампить",
        "btn_delete": "Удалить",
        "btn_cancel": "Отмена",
        "input_token": "Введите токен бота (например, 123456789:ABCdef...):",
        "cli_token_needed": (
            "<b>Не задан CLI токен</b>\n"
            "<blockquote>Для деплоя на Serverless задайте CLI токен в конфиге:\n"
            "<code>.cfg Token TGCLOUD_TOKEN app&lt;id&gt;:&lt;secret&gt;</code></blockquote>"
        ),
        "token_invalid": (
            "<b>Некорректный токен</b>\n"
            "<blockquote>Токен не найден или отклонен Telegram API</blockquote>"
        ),
        "bot_auth_failed": (
            "<b>Ошибка авторизации</b>\n"
            "<blockquote>Не удалось подключиться к боту через MTProto/API</blockquote>"
        ),
        "flood_wait": (
            "<b>Telegram FloodWait</b>\n"
            "<blockquote>Требуется подождать {seconds} сек. для авторизации новой сессии MTProto.</blockquote>"
        ),
        "dumping_progress": (
            "<b>Дамп истории сообщений</b>\n"
            "<blockquote>Сбор диалогов бота @{username}...\n"
            "Пожалуйста, подождите.</blockquote>"
        ),
        "dump_complete": (
            "<b>Дамп бота завершен</b>\n"
            "<blockquote>Бот: <b>{name}</b> (@{username})\n"
            "ID: <code>{bot_id}</code>\n\n"
            "Диалогов: <b>{chats_count}</b>\n"
            "Сообщений: <b>{messages_count}</b>\n"
            "Доступ: <b>Только владелец (Зашифровано)</b>\n"
            "PIN fallback: <code>{pin}</code>\n"
            "Mini App: <code>{app_url}</code></blockquote>"
        ),
        "bot_view": (
            "<b>Бот: {name} (@{username})</b>\n"
            "<blockquote>ID: <code>{bot_id}</code>\n"
            "Диалогов: <b>{chats_count}</b>\n"
            "Сообщений: <b>{messages_count}</b>\n"
            "Доступ: <b>Только владелец (Зашифровано)</b>\n"
            "PIN fallback: <code>{pin}</code>\n"
            "Mini App: <code>{app_url}</code></blockquote>"
        ),
        "deleted": (
            "<b>Дамп удален</b>\n"
            "<blockquote>Данные дампа и сессия бота успешно удалены.</blockquote>"
        ),
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "TGCLOUD_TOKEN",
                "",
                lambda: self._s("_cfg_tgcloud_token"),
                validator=loader.validators.Hidden(),
            ),
            loader.ConfigValue(
                "API_ID",
                None,
                lambda: self._s("_cfg_api_id"),
                validator=loader.validators.Integer(),
            ),
            loader.ConfigValue(
                "API_HASH",
                None,
                lambda: self._s("_cfg_api_hash"),
                validator=loader.validators.Hidden(),
            ),
            loader.ConfigValue(
                "APP_SHORT_NAME",
                "",
                lambda: self._s("_cfg_app_short_name"),
            ),
        )

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        asyncio.create_task(self._auto_setup_botfather())

    def _s(self, key, **kwargs):
        try:
            return self.strings[key].format(prefix=self.get_prefix(), **kwargs)
        except (KeyError, IndexError):
            return self.strings.get(key, key)

    def _get_api_creds(self) -> tuple[int, str]:
        api_id = self.config.get("API_ID")
        api_hash = self.config.get("API_HASH")

        if not api_id and hasattr(self, "_client") and self._client:
            api_id = getattr(self._client, "api_id", None)
        if not api_hash and hasattr(self, "_client") and self._client:
            api_hash = getattr(self._client, "api_hash", None)

        if not api_id:
            api_id = 2040
        if not api_hash:
            api_hash = "b18441a1ff607e10a989891a5462e627"

        return int(api_id), str(api_hash)

    def _get_user_tmp_dir(self, user_id: int, bot_id: str = "") -> str:
        base = f"/tmp/Token/{user_id}"
        if bot_id:
            path = os.path.join(base, str(bot_id))
        else:
            path = base
        os.makedirs(path, exist_ok=True)
        return path

    def _get_cli_token(self) -> str:
        token = self.config.get("TGCLOUD_TOKEN")
        if not token and hasattr(self, "_db") and self._db:
            token = self._db.get("Token", "tgcloud_token", "")
        if not token:
            token = os.environ.get("TGCLOUD_TOKEN", "")
        return (token or "").strip()

    def _get_inline_bot_id(self) -> str:
        cli_token = self._get_cli_token()
        if cli_token and cli_token.startswith("app"):
            return cli_token.split(":")[0].replace("app", "")
        if hasattr(self, "inline") and getattr(self.inline, "bot_id", None):
            return str(self.inline.bot_id)
        return "8288899778"

    def _get_inline_bot_username(self) -> str:
        if hasattr(self, "inline") and getattr(self.inline, "bot_username", None):
            return self.inline.bot_username
        if hasattr(self, "_db") and self._db:
            uname = (
                self._db.get("Token", "inline_bot_username", "")
                or self._db.get("friendly-telegram", "bot_username", "")
                or self._db.get("main", "bot_username", "")
            )
            if uname:
                return uname
        return ""

    def _get_app_url(self, bot_id: str) -> str:
        inline_bot_id = self._get_inline_bot_id()
        short_name = self.config.get("APP_SHORT_NAME", "").strip()
        if not short_name and hasattr(self, "_db") and self._db:
            short_name = self._db.get("Token", "app_short_name", "")
        bot_uname = self._get_inline_bot_username()
        if short_name and bot_uname:
            return f"https://t.me/{bot_uname}/{short_name}?startapp={bot_id}"
        return f"https://app{inline_bot_id}.tgcloud.ai/{bot_id}"

    def _find_storage_file(self, rel_name: str) -> str:
        candidates = [
            os.path.join("Storage", "Token", rel_name),
            os.path.join(os.getcwd(), "Storage", "Token", rel_name),
            os.path.expanduser(f"~/Feroku/Storage/Token/{rel_name}"),
            os.path.expanduser(f"~/Storage/Token/{rel_name}"),
            f"/home/user/Storage/Token/{rel_name}",
        ]
        if hasattr(utils, "get_base_dir"):
            try:
                base = utils.get_base_dir()
                if base:
                    candidates.insert(0, os.path.join(base, "Storage", "Token", rel_name))
            except Exception:
                pass
        for path in candidates:
            if path and os.path.isfile(path):
                return path
        return ""

    def _find_web_file(self, rel_name: str) -> str:
        return self._find_storage_file(os.path.join("web", rel_name))

    def _ensure_webapp_photo(self) -> str:
        """Ensure 640x360 WebApp.jpg photo is present, with auto-download fallback."""
        local_path = self._find_storage_file("WebApp.jpg")
        if local_path and os.path.isfile(local_path) and os.path.getsize(local_path) > 1000:
            return local_path

        tmp_dir = "/tmp/Token"
        os.makedirs(tmp_dir, exist_ok=True)
        tmp_path = os.path.join(tmp_dir, "WebApp.jpg")

        if os.path.isfile(tmp_path) and os.path.getsize(tmp_path) > 1000:
            return tmp_path

        try:
            req = urllib.request.Request(PHOTO_DOWNLOAD_URL, headers={"User-Agent": "TelegramBotDumper/2.0"})
            with urllib.request.urlopen(req, timeout=10) as r:
                if r.status == 200:
                    data = r.read()
                    if len(data) > 1000:
                        with open(tmp_path, "wb") as f:
                            f.write(data)
                        return tmp_path
        except Exception as e:
            logger.warning(f"Failed to download WebApp.jpg: {e}")

        try:
            if EMBEDDED_PHOTO_B64:
                data = base64.b64decode(EMBEDDED_PHOTO_B64)
                with open(tmp_path, "wb") as f:
                    f.write(data)
                return tmp_path
        except Exception as e:
            logger.warning(f"Failed to write embedded WebApp photo: {e}")

        return ""

    async def _auto_setup_botfather(self):
        """Automatically configure the Mini App in @BotFather on first load with 1.5s pauses."""
        if not self._client:
            return
        if self._db and self._db.get("Token", "botfather_configured", False):
            return

        bot_username = self._get_inline_bot_username()
        if not bot_username:
            for _ in range(10):
                await asyncio.sleep(1)
                bot_username = self._get_inline_bot_username()
                if bot_username:
                    break

        if not bot_username:
            logger.info("BotFather setup skipped: no inline bot username identified yet")
            return

        photo_path = self._ensure_webapp_photo()
        if not photo_path or not os.path.isfile(photo_path):
            logger.warning("BotFather setup skipped: WebApp.jpg not available")
            return

        inline_bot_id = self._get_inline_bot_id()
        webapp_url = f"https://app{inline_bot_id}.tgcloud.ai"
        short_name = "test3"

        botfather = "@BotFather"
        try:
            # Step 0: Cancel any previous hanging conversation
            await self._client.send_message(botfather, "/cancel")
            await asyncio.sleep(1.5)

            # Step 1: /newapp
            await self._client.send_message(botfather, "/newapp")
            await asyncio.sleep(1.5)

            # Step 2: Target Bot username
            target_uname = bot_username if bot_username.startswith("@") else f"@{bot_username}"
            await self._client.send_message(botfather, target_uname)
            await asyncio.sleep(1.5)

            # Step 3: Enter Title
            await self._client.send_message(botfather, "test")
            await asyncio.sleep(1.5)

            # Step 4: Enter Description
            await self._client.send_message(botfather, "test2")
            await asyncio.sleep(1.5)

            # Step 5: Upload photo (640x360) as a photo (force_document=False)
            await self._client.send_file(botfather, photo_path, force_document=False)
            await asyncio.sleep(1.5)

            # Step 6: Skip Demo GIF
            await self._client.send_message(botfather, "/empty")
            await asyncio.sleep(1.5)

            # Step 7: Web App URL
            await self._client.send_message(botfather, webapp_url)
            await asyncio.sleep(1.5)

            # Step 8: Short name
            await self._client.send_message(botfather, short_name)
            await asyncio.sleep(1.5)

            # Save configuration
            self.config["APP_SHORT_NAME"] = short_name
            if self._db:
                self._db.set("Token", "app_short_name", short_name)
                self._db.set("Token", "botfather_configured", True)
            logger.info("BotFather Mini App auto-configuration complete!")
        except Exception as e:
            logger.warning(f"BotFather auto-setup exception: {e}")

    def _get_bots(self) -> dict:
        return self._db.get("Token", "bots", {})

    def _set_bots(self, bots: dict):
        self._db.set("Token", "bots", bots)

    def _save_bot(self, bot_id: str, data: dict):
        bots = self._get_bots()
        bots[str(bot_id)] = data
        self._set_bots(bots)

    def _remove_bot(self, bot_id: str):
        bots = self._get_bots()
        if str(bot_id) in bots:
            del bots[str(bot_id)]
            self._set_bots(bots)

    def _validate_token_api(self, token: str) -> dict:
        url = f"https://api.telegram.org/bot{token}/getMe"
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "TelegramBotDumper/2.0"})
            with urllib.request.urlopen(req, timeout=10) as r:
                if r.status != 200:
                    return None
                data = json.loads(r.read().decode("utf-8"))
                if data.get("ok"):
                    return data.get("result")
        except Exception:
            pass
        return None

    def _get_chat_id(self, m) -> int:
        if isinstance(m.peer_id, PeerUser):
            return m.peer_id.user_id
        if isinstance(m.peer_id, PeerChat):
            return -m.peer_id.chat_id
        if isinstance(m.peer_id, PeerChannel):
            return int(f"-100{m.peer_id.channel_id}")
        return 0

    async def _resolve_entities(self, client: TelegramClient, chat_ids: set) -> dict:
        entities = {}
        for cid in chat_ids:
            try:
                ent = await client.get_entity(cid)
                first = getattr(ent, "first_name", "") or ""
                last = getattr(ent, "last_name", "") or ""
                title = getattr(ent, "title", "") or ""
                display_name = (first + " " + last).strip() or title or f"Dialog {cid}"
                username = getattr(ent, "username", "") or ""
                entities[str(cid)] = {
                    "name": display_name,
                    "first_name": first,
                    "last_name": last,
                    "username": username,
                    "id": cid,
                }
            except Exception:
                entities[str(cid)] = {
                    "name": f"Dialog {cid}",
                    "first_name": f"Dialog {cid}",
                    "last_name": "",
                    "username": "",
                    "id": cid,
                }
        return entities

    async def _dump_messages(self, client: TelegramClient) -> list:
        curr = 1
        step = 100
        empty_streak = 0
        all_msgs = []
        while empty_streak < 2 and curr < 50000:
            chunk = list(range(curr, curr + step))
            try:
                res = await client(GetMessagesRequest(chunk))
                found = [m for m in res.messages if not isinstance(m, MessageEmpty)]
                if not found:
                    empty_streak += 1
                else:
                    empty_streak = 0
                    all_msgs.extend(found)
            except Exception:
                break
            curr += step
        return all_msgs

    def _get_html_template(self) -> str:
        local_path = self._find_web_file("index.html")
        if local_path:
            try:
                with open(local_path, "r", encoding="utf-8") as f:
                    return f.read()
            except Exception:
                pass
        try:
            req = urllib.request.Request(GITHUB_WEB_URL, headers={"User-Agent": "TelegramBotDumper/2.0"})
            with urllib.request.urlopen(req, timeout=10) as r:
                if r.status == 200:
                    return r.read().decode("utf-8")
        except Exception:
            pass
        return ""

    def _deploy_serverless(self, project_dir: str) -> bool:
        tgcloud_token = self._get_cli_token()
        if not tgcloud_token:
            return False
        env = os.environ.copy()
        env["TGCLOUD_TOKEN"] = tgcloud_token
        try:
            res = subprocess.run(
                ["npx", "--yes", "@tgcloud/cli", "push", "--force"],
                cwd=project_dir,
                env=env,
                capture_output=True,
                text=True,
                timeout=120,
            )
            return res.returncode == 0
        except Exception as e:
            logger.warning(f"Serverless push error: {e}")
            return False

    async def _build_main_menu(self):
        bots = self._get_bots()
        if not bots:
            text = self._s("main_menu_empty")
        else:
            lines = []
            for bid, binfo in bots.items():
                uname = binfo.get("username", "")
                name = binfo.get("name", f"Bot {bid}")
                ccnt = binfo.get("chats_count", 0)
                mcnt = binfo.get("messages_count", 0)
                lines.append(f"- <b>{name}</b> (@{uname}): {ccnt} chats, {mcnt} msgs")
            text = self._s(
                "main_menu",
                count=len(bots),
                bot_list="\n".join(lines),
            )

        markup = [
            [
                {
                    "text": self._s("btn_add_token"),
                    "input": self._s("input_token"),
                    "handler": self._cb_token_input,
                    "style": "success",
                }
            ]
        ]

        if bots:
            for bid, binfo in bots.items():
                uname = binfo.get("username") or bid
                markup.append(
                    [
                        {
                            "text": f"@{uname}",
                            "callback": self._cb_bot_view,
                            "args": (bid,),
                            "style": "primary",
                        }
                    ]
                )

        markup.append(
            [
                {
                    "text": self._s("btn_close"),
                    "callback": self._cb_close,
                    "style": "danger",
                }
            ]
        )
        return text, markup

    async def _cb_close(self, call: InlineCall):
        await call.delete()

    async def _cb_main_menu(self, call: InlineCall):
        text, markup = await self._build_main_menu()
        await call.edit(text, reply_markup=markup)

    async def _cb_token_input(self, call: InlineCall, query: str):
        match = BOT_TOKEN_PATTERN.search(query.strip())
        if not match:
            await call.edit(
                self._s("token_invalid"),
                reply_markup=[
                    [
                        {
                            "text": self._s("btn_back"),
                            "callback": self._cb_main_menu,
                            "style": "danger",
                        }
                    ]
                ],
            )
            return

        token = match.group(1)
        info = await asyncio.to_thread(self._validate_token_api, token)
        if not info:
            await call.edit(
                self._s("bot_auth_failed"),
                reply_markup=[
                    [
                        {
                            "text": self._s("btn_back"),
                            "callback": self._cb_main_menu,
                            "style": "danger",
                        }
                    ]
                ],
            )
            return

        bot_id = str(info.get("id"))
        name = info.get("first_name") or "Bot"
        username = info.get("username") or ""

        await self._start_dump(call, token, bot_id, name, username)

    async def _start_dump(
        self,
        call: InlineCall,
        token: str,
        bot_id: str,
        name: str,
        username: str,
    ):
        user_id = call.from_user.id
        await call.edit(self._s("dumping_progress", username=_escape(username)))

        bot_dir = self._get_user_tmp_dir(user_id, bot_id)
        session_file = os.path.join(bot_dir, f"sess_{bot_id}")

        serverless_dir = os.path.join(self._get_user_tmp_dir(user_id), "serverless")
        dist_dir = os.path.join(serverless_dir, "dist")
        bot_dist_dir = os.path.join(dist_dir, bot_id)
        endpoints_dir = os.path.join(serverless_dir, "tgcloud", "endpoints")
        os.makedirs(bot_dist_dir, exist_ok=True)
        os.makedirs(endpoints_dir, exist_ok=True)

        api_id, api_hash = self._get_api_creds()
        client = TelegramClient(session_file, api_id, api_hash)
        raw_msgs = []
        entities = {}
        messages_by_chat = {}
        chats_list = []
        total_msgs_count = 0

        try:
            await client.start(bot_token=token)
            raw_msgs = await self._dump_messages(client)

            chat_ids = set()
            for m in raw_msgs:
                cid = self._get_chat_id(m)
                if cid:
                    chat_ids.add(cid)

            entities = await self._resolve_entities(client, chat_ids)
            await client.disconnect()
        except FloodWaitError as e:
            try:
                await client.disconnect()
            except Exception:
                pass
            found_cache = False
            candidates = [
                os.path.join(bot_dist_dir, "data.json"),
                os.path.join(dist_dir, "data.json"),
            ]
            for c_path in candidates:
                if c_path and os.path.isfile(c_path):
                    try:
                        with open(c_path, "r", encoding="utf-8") as f:
                            cached = json.load(f)
                        if str(cached.get("bot", {}).get("id")) == str(bot_id):
                            chats_list = cached.get("chats", [])
                            messages_by_chat = cached.get("messages", {})
                            total_msgs_count = cached.get("total_messages", 0)
                            found_cache = True
                            break
                    except Exception:
                        pass
            if not found_cache:
                await call.edit(
                    self._s("flood_wait", seconds=e.seconds),
                    reply_markup=[
                        [
                            {
                                "text": self._s("btn_back"),
                                "callback": self._cb_main_menu,
                                "style": "danger",
                            }
                        ]
                    ],
                )
                return
        except Exception as e:
            logger.error(f"Dump error: {e}")
            try:
                await client.disconnect()
            except Exception:
                pass
            await call.edit(
                self._s("bot_auth_failed"),
                reply_markup=[
                    [
                        {
                            "text": self._s("btn_back"),
                            "callback": self._cb_main_menu,
                            "style": "danger",
                        }
                    ]
                ],
            )
            return

        pin = str(random.randint(100000, 999999))

        if not messages_by_chat:
            for m in raw_msgs:
                cid = str(self._get_chat_id(m))
                if not cid or cid == "0":
                    continue
                if cid not in messages_by_chat:
                    messages_by_chat[cid] = []

                m_text = m.message or ""
                m_date = m.date.strftime("%Y-%m-%d %H:%M:%S") if m.date else ""
                m_media = type(m.media).__name__ if m.media else ""

                m_html = _format_layer229_message(m_text, getattr(m, "entities", None))

                reply_id = None
                if getattr(m, "reply_to", None) and getattr(m.reply_to, "reply_to_msg_id", None):
                    reply_id = m.reply_to.reply_to_msg_id

                messages_by_chat[cid].append(
                    {
                        "id": m.id,
                        "out": bool(m.out),
                        "text": m_text,
                        "html": m_html,
                        "date": m_date,
                        "media": m_media,
                        "reply_to": reply_id,
                    }
                )

            for cid, msgs in messages_by_chat.items():
                info = entities.get(cid, {"name": f"Dialog {cid}", "username": ""})
                last_msg = msgs[-1] if msgs else None
                last_text = last_msg["text"] if last_msg else ""
                if not last_text and last_msg and last_msg["media"]:
                    last_text = f"[{last_msg['media']}]"
                chats_list.append(
                    {
                        "id": cid,
                        "name": info["name"],
                        "first_name": info.get("first_name", ""),
                        "last_name": info.get("last_name", ""),
                        "username": info["username"],
                        "count": len(msgs),
                        "last": last_text[:60],
                        "date": last_msg["date"] if last_msg else "",
                    }
                )
            chats_list.sort(key=lambda x: x["date"], reverse=True)
            total_msgs_count = len(raw_msgs)

        dump_data = {
            "bot": {"id": bot_id, "name": name, "username": username},
            "owner_id": user_id,
            "pin": pin,
            "total_messages": total_msgs_count,
            "total_chats": len(chats_list),
            "chats": chats_list,
            "messages": messages_by_chat,
        }

        # Encrypt dump payload using user_id
        dump_json = json.dumps(dump_data, ensure_ascii=False)
        enc_payload = _encrypt_payload(dump_json, user_id)

        # Write data to bot directory and dist
        with open(os.path.join(bot_dist_dir, "data.json"), "w", encoding="utf-8") as f:
            json.dump({"encrypted": enc_payload, "owner_id": user_id, "pin": pin}, f, ensure_ascii=False, indent=2)

        with open(os.path.join(dist_dir, "data.json"), "w", encoding="utf-8") as f:
            json.dump({"encrypted": enc_payload, "owner_id": user_id, "pin": pin}, f, ensure_ascii=False, indent=2)

        template = self._get_html_template()
        if template:
            injected_script = (
                f"<script>\n"
                f"window.ENCRYPTED_DUMP = {json.dumps(enc_payload)};\n"
                f"window.DUMP_OWNER_ID = {user_id};\n"
                f"</script>"
            )
            if "</head>" in template:
                final_html = template.replace("</head>", f"{injected_script}\n</head>", 1)
            else:
                final_html = f"{injected_script}\n{template}"

            with open(os.path.join(bot_dist_dir, "index.html"), "w", encoding="utf-8") as f:
                f.write(final_html)

            with open(os.path.join(dist_dir, "index.html"), "w", encoding="utf-8") as f:
                f.write(final_html)

        # Register serverless endpoint
        all_bots = self._get_bots()
        bots_to_endpoint = set(all_bots.keys())
        bots_to_endpoint.add(str(bot_id))

        for bid in bots_to_endpoint:
            endpoint_file = os.path.join(endpoints_dir, f"bot_{bid}.js")
            endpoint_code = (
                "import { EndpointError } from 'sdk'\n\n"
                "export default async function (input, ctx) {\n"
                "  const user = ctx.initData?.user\n"
                f"  if (!user || user.id !== {user_id}) {{\n"
                "    throw new EndpointError('Access denied', { code: 'FORBIDDEN' })\n"
                "  }\n"
                f"  return {{ bot_id: '{bid}', status: 'active' }}\n"
                "}\n"
            )
            with open(endpoint_file, "w", encoding="utf-8") as f:
                f.write(endpoint_code)

        tgcloud_jsonc = os.path.join(serverless_dir, "tgcloud.jsonc")
        with open(tgcloud_jsonc, "w", encoding="utf-8") as f:
            f.write(
                '{\n'
                '  "$schema": "https://raw.githubusercontent.com/telegram-cloud/tgcloud/main/schema.json",\n'
                '  "static": {\n'
                '    "source": "dist",\n'
                '    "spa": true\n'
                '  }\n'
                '}\n'
            )

        cli_token = self._get_cli_token()
        if not cli_token:
            await call.edit(
                self._s("cli_token_needed"),
                reply_markup=[
                    [
                        {
                            "text": self._s("btn_back"),
                            "callback": self._cb_main_menu,
                            "style": "primary",
                        }
                    ]
                ],
            )
            return

        self._deploy_serverless(serverless_dir)

        app_url = self._get_app_url(bot_id)
        bot_info = {
            "token": token,
            "bot_id": bot_id,
            "name": name,
            "username": username,
            "owner_id": user_id,
            "pin": pin,
            "chats_count": len(chats_list),
            "messages_count": total_msgs_count,
            "app_url": app_url,
            "bot_dist_dir": bot_dist_dir,
        }
        self._save_bot(bot_id, bot_info)

        text = self._s(
            "dump_complete",
            name=_escape(name),
            username=_escape(username),
            bot_id=bot_id,
            chats_count=len(chats_list),
            messages_count=total_msgs_count,
            pin=pin,
            app_url=app_url,
        )

        markup = [
            [
                {
                    "text": self._s("btn_open_app"),
                    "url": app_url,
                    "style": "success",
                }
            ],
            [
                {
                    "text": self._s("btn_back"),
                    "callback": self._cb_main_menu,
                    "style": "secondary",
                }
            ],
        ]
        await call.edit(text, reply_markup=markup)

    async def _cb_bot_view(self, call: InlineCall, bot_id: str):
        bots = self._get_bots()
        info = bots.get(str(bot_id))
        if not info:
            await call.edit(
                self._s("deleted"),
                reply_markup=[
                    [
                        {
                            "text": self._s("btn_back"),
                            "callback": self._cb_main_menu,
                            "style": "primary",
                        }
                    ]
                ],
            )
            return

        name = info.get("name", f"Bot {bot_id}")
        username = info.get("username", "")
        chats_count = info.get("chats_count", 0)
        messages_count = info.get("messages_count", 0)
        pin = info.get("pin", "------")
        app_url = info.get("app_url", self._get_app_url(bot_id))

        text = self._s(
            "bot_view",
            name=_escape(name),
            username=_escape(username),
            bot_id=bot_id,
            chats_count=chats_count,
            messages_count=messages_count,
            pin=pin,
            app_url=app_url,
        )
        markup = [
            [
                {
                    "text": self._s("btn_open_app"),
                    "url": app_url,
                    "style": "success",
                }
            ],
            [
                {
                    "text": self._s("btn_redump"),
                    "callback": self._cb_redump,
                    "args": (bot_id,),
                    "style": "secondary",
                },
                {
                    "text": self._s("btn_delete"),
                    "callback": self._cb_bot_delete,
                    "args": (bot_id,),
                    "style": "danger",
                },
            ],
            [
                {
                    "text": self._s("btn_back"),
                    "callback": self._cb_main_menu,
                    "style": "primary",
                }
            ],
        ]
        await call.edit(text, reply_markup=markup)

    async def _cb_redump(self, call: InlineCall, bot_id: str):
        bots = self._get_bots()
        info = bots.get(str(bot_id))
        if not info or not info.get("token"):
            await self._cb_main_menu(call)
            return
        await self._start_dump(
            call,
            info["token"],
            bot_id,
            info.get("name", "Bot"),
            info.get("username", ""),
        )

    async def _cb_bot_delete(self, call: InlineCall, bot_id: str):
        user_id = call.from_user.id
        self._remove_bot(bot_id)
        bot_dir = self._get_user_tmp_dir(user_id, bot_id)
        serverless_dir = os.path.join(self._get_user_tmp_dir(user_id), "serverless")
        serverless_bot_dir = os.path.join(serverless_dir, "dist", bot_id)
        endpoint_file = os.path.join(serverless_dir, "tgcloud", "endpoints", f"bot_{bot_id}.js")
        for d in (bot_dir, serverless_bot_dir):
            if os.path.isdir(d):
                try:
                    shutil.rmtree(d)
                except Exception:
                    pass
        if os.path.isfile(endpoint_file):
            try:
                os.remove(endpoint_file)
            except Exception:
                pass
        await call.edit(
            self._s("deleted"),
            reply_markup=[
                [
                    {
                        "text": self._s("btn_back"),
                        "callback": self._cb_main_menu,
                        "style": "primary",
                    }
                ]
            ],
        )

    @loader.command()
    async def t(self, message: Message):
        """Open Bot Dumper and Serverless Mini App manager"""
        text, markup = await self._build_main_menu()
        await self.inline.form(
            text=text,
            reply_markup=markup,
            message=message,
        )
