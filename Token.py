__version__ = (1, 0, 0)
# meta developer: FireJester.t.me

import os
import re
import io
import logging
import tempfile
import shutil
import asyncio

import aiohttp
from telethon import TelegramClient
from telethon.tl.types import Message

from .. import loader, utils

logger = logging.getLogger(__name__)

BOT_TOKEN_PATTERN = re.compile(r'\b\d{8,10}:[A-Za-z0-9_-]{35}\b')
EMOJI_PATTERN = re.compile(
    r'<emoji document_id=(\d+)>([^<]*)</emoji>'
)


def escape_html(text):
    if not text:
        return ""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def convert_emoji_for_bot(text):
    if not text:
        return text
    return EMOJI_PATTERN.sub(
        r'<tg-emoji emoji-id=\1>\2</tg-emoji>',
        text,
    )


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
        "help": (
            "<b>Token - Bot token manager</b>\n\n"
            "<code>{prefix}tkadd [token]</code> - add bot token (args or reply)\n"
            "<code>{prefix}tklist</code> - list connected bots\n"
            "<code>{prefix}tkinfo [N]</code> - bot users info (txt file)\n"
            "<code>{prefix}tkdm [N] [user_id]</code> - dialog with user (txt file)\n"
            "<code>{prefix}tkadv [N]</code> - start broadcast process\n"
            "<code>{prefix}tktxt</code> - set broadcast text (reply)\n"
            "<code>{prefix}tklink</code> - set broadcast media url (reply)\n"
            "<code>{prefix}tkadv test</code> - test broadcast (send to yourself)\n"
            "<code>{prefix}tkadv start</code> - start broadcast to all users\n"
        ),
        "token_added": "<b>Token added as [{num}]</b>\n\nBot: @{username}\nID: <code>{bot_id}</code>",
        "token_invalid": "<b>Invalid or expired token</b>",
        "token_no_args": "<b>Provide a token (args or reply)</b>",
        "token_already": "<b>This bot is already added as [{num}]</b>",
        "token_not_found": "<b>No token found in text</b>",
        "list_empty": "<b>No bots added</b>",
        "list_processing": "Processing...",
        "list_invalid_removed": "<b>Token [{num}] is invalid, removed. Bots renumbered.</b>",
        "bot_not_found": "<b>Bot [{num}] not found</b>",
        "info_processing": "<b>Collecting info for bot [{num}]...</b>",
        "info_no_dialogs": "<b>Bot [{num}] has no user dialogs</b>",
        "dm_processing": "<b>Loading dialog with {user_id} in bot [{num}]...</b>",
        "dm_no_messages": "<b>No messages found with {user_id}</b>",
        "dm_user_not_found": "<b>User {user_id} not found in bot dialogs</b>",
        "adv_started": (
            "<b>Broadcast process started for bot [{num}] (@{username})</b>\n\n"
            "Use <code>{prefix}tktxt</code> in reply to set broadcast text\n"
            "Use <code>{prefix}tklink</code> in reply to set media URL\n\n"
            "Then use:\n"
            "<code>{prefix}tkadv test</code> - preview (send to yourself)\n"
            "<code>{prefix}tkadv start</code> - send to all users"
        ),
        "adv_no_process": "<b>No active broadcast process. Start with</b> <code>{prefix}tkadv [N]</code>",
        "adv_txt_set": "<b>Broadcast text set</b>",
        "adv_txt_no_reply": "<b>Reply to a message to set broadcast text</b>",
        "adv_link_set": "<b>Broadcast media URL set:</b> {url}",
        "adv_link_no_reply": "<b>Reply to a message containing a URL</b>",
        "adv_link_no_url": "<b>No URL found in replied message</b>",
        "adv_no_text": "<b>Set broadcast text first with</b> <code>{prefix}tktxt</code>",
        "adv_test_sent": "<b>Test message sent to you</b>",
        "adv_test_error": "<b>Test send error:</b> {error}",
        "adv_sending": "<b>Broadcasting... {current}/{total}</b>",
        "adv_done": "<b>Broadcast complete!</b>\n\nSent: {success}/{total}\nFailed: {failed}",
        "adv_cancelled": "<b>Broadcast process cleared</b>",
        "error": "<b>Error:</b> {error}",
    }

    strings_ru = {
        "help": (
            "<b>Token - Менеджер токенов ботов</b>\n\n"
            "<code>{prefix}tkadd [токен]</code> - добавить токен бота (аргумент или реплай)\n"
            "<code>{prefix}tklist</code> - список подключенных ботов\n"
            "<code>{prefix}tkinfo [N]</code> - информация о пользователях бота (txt файл)\n"
            "<code>{prefix}tkdm [N] [user_id]</code> - диалог с пользователем (txt файл)\n"
            "<code>{prefix}tkadv [N]</code> - начать процесс рассылки\n"
            "<code>{prefix}tktxt</code> - задать текст рассылки (реплай)\n"
            "<code>{prefix}tklink</code> - задать URL медиа для рассылки (реплай)\n"
            "<code>{prefix}tkadv test</code> - тест рассылки (отправить себе)\n"
            "<code>{prefix}tkadv start</code> - отправить рассылку всем пользователям\n"
        ),
        "token_added": "<b>Токен добавлен как [{num}]</b>\n\nБот: @{username}\nID: <code>{bot_id}</code>",
        "token_invalid": "<b>Невалидный или просроченный токен</b>",
        "token_no_args": "<b>Укажите токен (аргументом или реплаем)</b>",
        "token_already": "<b>Этот бот уже добавлен как [{num}]</b>",
        "token_not_found": "<b>Токен не найден в тексте</b>",
        "list_empty": "<b>Нет добавленных ботов</b>",
        "list_processing": "Processing...",
        "list_invalid_removed": "<b>Токен [{num}] невалидный, удален. Боты перенумерованы.</b>",
        "bot_not_found": "<b>Бот [{num}] не найден</b>",
        "info_processing": "<b>Сбор информации о боте [{num}]...</b>",
        "info_no_dialogs": "<b>У бота [{num}] нет диалогов с пользователями</b>",
        "dm_processing": "<b>Загрузка диалога с {user_id} в боте [{num}]...</b>",
        "dm_no_messages": "<b>Сообщений с {user_id} не найдено</b>",
        "dm_user_not_found": "<b>Пользователь {user_id} не найден в диалогах бота</b>",
        "adv_started": (
            "<b>Процесс рассылки запущен для бота [{num}] (@{username})</b>\n\n"
            "Используйте <code>{prefix}tktxt</code> в реплай для задания текста\n"
            "Используйте <code>{prefix}tklink</code> в реплай для задания URL медиа\n\n"
            "Затем:\n"
            "<code>{prefix}tkadv test</code> - предпросмотр (отправить себе)\n"
            "<code>{prefix}tkadv start</code> - отправить всем пользователям"
        ),
        "adv_no_process": "<b>Нет активного процесса рассылки. Начните с</b> <code>{prefix}tkadv [N]</code>",
        "adv_txt_set": "<b>Текст рассылки задан</b>",
        "adv_txt_no_reply": "<b>Сделайте реплай на сообщение для задания текста рассылки</b>",
        "adv_link_set": "<b>URL медиа для рассылки задан:</b> {url}",
        "adv_link_no_reply": "<b>Сделайте реплай на сообщение с URL</b>",
        "adv_link_no_url": "<b>URL не найден в сообщении</b>",
        "adv_no_text": "<b>Сначала задайте текст рассылки через</b> <code>{prefix}tktxt</code>",
        "adv_test_sent": "<b>Тестовое сообщение отправлено вам</b>",
        "adv_test_error": "<b>Ошибка тестовой отправки:</b> {error}",
        "adv_sending": "<b>Рассылка... {current}/{total}</b>",
        "adv_done": "<b>Рассылка завершена!</b>\n\nОтправлено: {success}/{total}\nОшибок: {failed}",
        "adv_cancelled": "<b>Процесс рассылки очищен</b>",
        "error": "<b>Ошибка:</b> {error}",
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "tokens",
                "",
                lambda: "Bot tokens (hidden, managed by commands)",
                validator=loader.validators.Hidden(),
            ),
        )
        self._owner_id = None
        self._temp_dir = None
        self._adv_state = None

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        me = await client.get_me()
        self._owner_id = me.id
        self._temp_dir = os.path.join(
            tempfile.gettempdir(), f"Token_{self._owner_id}"
        )
        os.makedirs(self._temp_dir, exist_ok=True)

    def _get_tokens(self):
        raw = self.config["tokens"]
        if not raw or not raw.strip():
            return []
        return [t.strip() for t in raw.split("\n") if t.strip()]

    def _set_tokens(self, tokens):
        self.config["tokens"] = "\n".join(tokens)

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
        bot_client = TelegramClient(
            session_path,
            api_id=self._client.api_id,
            api_hash=self._client.api_hash,
        )
        await bot_client.start(bot_token=token)
        return bot_client, session_path

    async def _destroy_bot_client(self, bot_client, session_path):
        try:
            await bot_client.disconnect()
        except Exception:
            pass
        self._clean_session(session_path)

    async def _validate_token_api(self, token):
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(
                    f"https://api.telegram.org/bot{token}/getMe"
                ) as r:
                    d = await r.json()
                    if d.get("ok"):
                        return d["result"]
        except Exception:
            pass
        return None

    def _remove_token_and_renumber(self, index):
        tokens = self._get_tokens()
        if 0 <= index < len(tokens):
            tokens.pop(index)
            self._set_tokens(tokens)

    async def _get_bot_users(self, bot_client):
        users = {}
        async for dialog in bot_client.iter_dialogs():
            entity = dialog.entity
            if not entity:
                continue
            if not getattr(entity, "bot", False) and hasattr(entity, "first_name"):
                uid = entity.id
                msg_count = 0
                async for _ in bot_client.iter_messages(uid):
                    msg_count += 1
                users[uid] = {
                    "name": get_full_name(entity),
                    "username": get_username(entity),
                    "msg_count": msg_count,
                }
        return users

    @loader.command(
        ru_doc="Добавить токен бота",
        en_doc="Add bot token",
    )
    async def tkadd(self, message: Message):
        """Add bot token"""
        token = None
        args = utils.get_args_raw(message)
        if args:
            m = BOT_TOKEN_PATTERN.search(args)
            if m:
                token = m.group(0)
        if not token:
            reply = await message.get_reply_message()
            if reply and reply.raw_text:
                m = BOT_TOKEN_PATTERN.search(reply.raw_text)
                if m:
                    token = m.group(0)
        if not token:
            await utils.answer(message, self.strings["token_not_found"])
            return

        info = await self._validate_token_api(token)
        if not info:
            await utils.answer(message, self.strings["token_invalid"])
            return

        bot_id = info["id"]
        tokens = self._get_tokens()
        for i, t in enumerate(tokens):
            t_info = await self._validate_token_api(t)
            if t_info and t_info["id"] == bot_id:
                await utils.answer(
                    message,
                    self.strings["token_already"].format(num=i + 1),
                )
                return

        tokens.append(token)
        self._set_tokens(tokens)

        await utils.answer(
            message,
            self.strings["token_added"].format(
                num=len(tokens),
                username=info.get("username", "unknown"),
                bot_id=bot_id,
            ),
        )

    @loader.command(
        ru_doc="Список подключенных ботов",
        en_doc="List connected bots",
    )
    async def tklist(self, message: Message):
        """List connected bots"""
        tokens = self._get_tokens()
        if not tokens:
            await utils.answer(message, self.strings["list_empty"])
            return

        status_msg = await utils.answer(
            message, self.strings["list_processing"]
        )

        lines = []
        invalid_indices = []

        for i, token in enumerate(tokens):
            info = await self._validate_token_api(token)
            if info:
                username = info.get("username", "unknown")
                lines.append(f"[{i + 1}] @{username}")
            else:
                invalid_indices.append(i)
                lines.append(f"[{i + 1}] INVALID")

        text = (
            self.strings["list_processing"]
            + "\n<blockquote expandable>"
            + "\n".join(lines)
            + "</blockquote>"
        )
        await utils.answer(status_msg, text, parse_mode="html")

        if invalid_indices:
            for offset, idx in enumerate(invalid_indices):
                real_idx = idx - offset
                self._remove_token_and_renumber(real_idx)
            removed_nums = ", ".join(str(idx + 1) for idx in invalid_indices)
            await self._client.send_message(
                message.chat_id,
                self.strings["list_invalid_removed"].format(num=removed_nums),
                parse_mode="html",
            )

    @loader.command(
        ru_doc="Информация о пользователях бота (txt файл)",
        en_doc="Bot users info (txt file)",
    )
    async def tkinfo(self, message: Message):
        """Bot users info (txt file)"""
        args = utils.get_args_raw(message).strip()
        if not args or not args.isdigit():
            prefix = self.get_prefix()
            await utils.answer(
                message, self.strings["help"].format(prefix=prefix)
            )
            return

        num = int(args)
        tokens = self._get_tokens()
        if num < 1 or num > len(tokens):
            await utils.answer(
                message, self.strings["bot_not_found"].format(num=num)
            )
            return

        token = tokens[num - 1]
        info = await self._validate_token_api(token)
        if not info:
            await utils.answer(message, self.strings["token_invalid"])
            self._remove_token_and_renumber(num - 1)
            await self._client.send_message(
                message.chat_id,
                self.strings["list_invalid_removed"].format(num=num),
                parse_mode="html",
            )
            return

        status_msg = await utils.answer(
            message, self.strings["info_processing"].format(num=num)
        )

        bot_client = None
        session_path = None
        try:
            bot_client, session_path = await self._create_bot_client(token)
            users = await self._get_bot_users(bot_client)
        except Exception as e:
            if bot_client and session_path:
                await self._destroy_bot_client(bot_client, session_path)
            await utils.answer(
                status_msg, self.strings["error"].format(error=str(e)[:200])
            )
            return

        await self._destroy_bot_client(bot_client, session_path)

        if not users:
            await utils.answer(
                status_msg, self.strings["info_no_dialogs"].format(num=num)
            )
            return

        bot_username = info.get("username", "unknown")
        file_lines = [
            f"Bot: @{bot_username}",
            f"Total users: {len(users)}",
            "=" * 50,
            "",
        ]
        for uid, udata in users.items():
            uname_display = f"@{udata['username']}" if udata["username"] else "no_username"
            file_lines.append(
                f"Name: {udata['name']} | Username: {uname_display} "
                f"| ID: {uid} | Messages: {udata['msg_count']}"
            )

        content = "\n".join(file_lines)
        f = io.BytesIO(content.encode("utf-8"))
        f.name = f"bot_{num}_users.txt"

        try:
            await status_msg.delete()
        except Exception:
            pass

        await self._client.send_file(
            message.chat_id,
            f,
            caption=f"<b>Bot [{num}] @{bot_username} - {len(users)} users</b>",
            parse_mode="html",
        )

    @loader.command(
        ru_doc="Диалог с пользователем бота (txt файл)",
        en_doc="Dialog with bot user (txt file)",
    )
    async def tkdm(self, message: Message):
        """Dialog with bot user (txt file)"""
        args = utils.get_args_raw(message).strip().split()
        if len(args) < 2 or not args[0].isdigit() or not args[1].isdigit():
            prefix = self.get_prefix()
            await utils.answer(
                message, self.strings["help"].format(prefix=prefix)
            )
            return

        num = int(args[0])
        user_id = int(args[1])
        tokens = self._get_tokens()

        if num < 1 or num > len(tokens):
            await utils.answer(
                message, self.strings["bot_not_found"].format(num=num)
            )
            return

        token = tokens[num - 1]
        info = await self._validate_token_api(token)
        if not info:
            await utils.answer(message, self.strings["token_invalid"])
            self._remove_token_and_renumber(num - 1)
            await self._client.send_message(
                message.chat_id,
                self.strings["list_invalid_removed"].format(num=num),
                parse_mode="html",
            )
            return

        status_msg = await utils.answer(
            message,
            self.strings["dm_processing"].format(user_id=user_id, num=num),
        )

        bot_client = None
        session_path = None
        try:
            bot_client, session_path = await self._create_bot_client(token)

            messages_list = []
            try:
                async for msg in bot_client.iter_messages(user_id, limit=None):
                    sender = "BOT" if msg.out else "USER"
                    date_str = msg.date.strftime("%Y-%m-%d %H:%M:%S") if msg.date else "?"
                    text = msg.raw_text or ""
                    if msg.media and not text:
                        text = "[media]"
                    messages_list.append(f"[{date_str}] {sender}: {text}")
            except Exception:
                await self._destroy_bot_client(bot_client, session_path)
                await utils.answer(
                    status_msg,
                    self.strings["dm_user_not_found"].format(user_id=user_id),
                )
                return

            await self._destroy_bot_client(bot_client, session_path)

            if not messages_list:
                await utils.answer(
                    status_msg,
                    self.strings["dm_no_messages"].format(user_id=user_id),
                )
                return

            messages_list.reverse()

            bot_username = info.get("username", "unknown")
            file_lines = [
                f"Bot: @{bot_username}",
                f"Dialog with: {user_id}",
                f"Total messages: {len(messages_list)}",
                "=" * 50,
                "",
            ]
            file_lines.extend(messages_list)

            content = "\n".join(file_lines)
            f = io.BytesIO(content.encode("utf-8"))
            f.name = f"bot_{num}_dm_{user_id}.txt"

            try:
                await status_msg.delete()
            except Exception:
                pass

            await self._client.send_file(
                message.chat_id,
                f,
                caption=(
                    f"<b>Bot [{num}] @{bot_username} - dialog with"
                    f" {user_id} ({len(messages_list)} messages)</b>"
                ),
                parse_mode="html",
            )

        except Exception as e:
            if bot_client and session_path:
                await self._destroy_bot_client(bot_client, session_path)
            await utils.answer(
                status_msg, self.strings["error"].format(error=str(e)[:200])
            )

    @loader.command(
        ru_doc="Управление рассылкой",
        en_doc="Broadcast management",
    )
    async def tkadv(self, message: Message):
        """Broadcast management"""
        args = utils.get_args_raw(message).strip()
        prefix = self.get_prefix()

        if not args:
            await utils.answer(
                message, self.strings["help"].format(prefix=prefix)
            )
            return

        if args.lower() == "test":
            await self._adv_test(message)
            return

        if args.lower() == "start":
            await self._adv_start(message)
            return

        if not args.isdigit():
            await utils.answer(
                message, self.strings["help"].format(prefix=prefix)
            )
            return

        num = int(args)
        tokens = self._get_tokens()
        if num < 1 or num > len(tokens):
            await utils.answer(
                message, self.strings["bot_not_found"].format(num=num)
            )
            return

        token = tokens[num - 1]
        info = await self._validate_token_api(token)
        if not info:
            await utils.answer(message, self.strings["token_invalid"])
            self._remove_token_and_renumber(num - 1)
            await self._client.send_message(
                message.chat_id,
                self.strings["list_invalid_removed"].format(num=num),
                parse_mode="html",
            )
            return

        self._adv_state = {
            "num": num,
            "token": token,
            "username": info.get("username", "unknown"),
            "text": None,
            "media_url": None,
        }

        await utils.answer(
            message,
            self.strings["adv_started"].format(
                num=num,
                username=info.get("username", "unknown"),
                prefix=prefix,
            ),
        )

    @loader.command(
        ru_doc="Задать текст рассылки (реплай)",
        en_doc="Set broadcast text (reply)",
    )
    async def tktxt(self, message: Message):
        """Set broadcast text (reply)"""
        prefix = self.get_prefix()
        if not self._adv_state:
            await utils.answer(
                message,
                self.strings["adv_no_process"].format(prefix=prefix),
            )
            return

        reply = await message.get_reply_message()
        if not reply:
            await utils.answer(message, self.strings["adv_txt_no_reply"])
            return

        html_text = reply.text
        if not html_text:
            await utils.answer(message, self.strings["adv_txt_no_reply"])
            return

        self._adv_state["text"] = html_text
        await utils.answer(message, self.strings["adv_txt_set"])

    @loader.command(
        ru_doc="Задать URL медиа для рассылки (реплай)",
        en_doc="Set broadcast media URL (reply)",
    )
    async def tklink(self, message: Message):
        """Set broadcast media URL (reply)"""
        prefix = self.get_prefix()
        if not self._adv_state:
            await utils.answer(
                message,
                self.strings["adv_no_process"].format(prefix=prefix),
            )
            return

        reply = await message.get_reply_message()
        if not reply:
            await utils.answer(message, self.strings["adv_link_no_reply"])
            return

        raw = reply.raw_text or ""
        url_match = re.search(r'https?://[^\s<>"\']+', raw, re.IGNORECASE)
        if not url_match:
            await utils.answer(message, self.strings["adv_link_no_url"])
            return

        url = url_match.group(0)
        self._adv_state["media_url"] = url
        await utils.answer(
            message, self.strings["adv_link_set"].format(url=url)
        )

    async def _adv_test(self, message):
        prefix = self.get_prefix()
        if not self._adv_state:
            await utils.answer(
                message,
                self.strings["adv_no_process"].format(prefix=prefix),
            )
            return

        if not self._adv_state.get("text"):
            await utils.answer(
                message,
                self.strings["adv_no_text"].format(prefix=prefix),
            )
            return

        token = self._adv_state["token"]
        text = convert_emoji_for_bot(self._adv_state["text"])
        media_url = self._adv_state.get("media_url")

        bot_client = None
        session_path = None
        try:
            bot_client, session_path = await self._create_bot_client(token)

            if media_url:
                await bot_client.send_file(
                    self._owner_id,
                    media_url,
                    caption=text,
                    parse_mode="html",
                )
            else:
                await bot_client.send_message(
                    self._owner_id,
                    text,
                    parse_mode="html",
                )

            await self._destroy_bot_client(bot_client, session_path)
            await utils.answer(message, self.strings["adv_test_sent"])

        except Exception as e:
            if bot_client and session_path:
                await self._destroy_bot_client(bot_client, session_path)
            await utils.answer(
                message,
                self.strings["adv_test_error"].format(error=str(e)[:200]),
            )

    async def _adv_start(self, message):
        prefix = self.get_prefix()
        if not self._adv_state:
            await utils.answer(
                message,
                self.strings["adv_no_process"].format(prefix=prefix),
            )
            return

        if not self._adv_state.get("text"):
            await utils.answer(
                message,
                self.strings["adv_no_text"].format(prefix=prefix),
            )
            return

        token = self._adv_state["token"]
        text = convert_emoji_for_bot(self._adv_state["text"])
        media_url = self._adv_state.get("media_url")

        info = await self._validate_token_api(token)
        if not info:
            await utils.answer(message, self.strings["token_invalid"])
            self._adv_state = None
            return

        bot_client = None
        session_path = None
        try:
            bot_client, session_path = await self._create_bot_client(token)

            user_ids = []
            async for dialog in bot_client.iter_dialogs():
                entity = dialog.entity
                if not entity:
                    continue
                if not getattr(entity, "bot", False) and hasattr(entity, "first_name"):
                    user_ids.append(entity.id)

            total = len(user_ids)
            if total == 0:
                await self._destroy_bot_client(bot_client, session_path)
                await utils.answer(
                    message,
                    self.strings["info_no_dialogs"].format(
                        num=self._adv_state["num"]
                    ),
                )
                self._adv_state = None
                return

            status_msg = await utils.answer(
                message,
                self.strings["adv_sending"].format(current=0, total=total),
            )

            success = 0
            failed = 0

            for i, uid in enumerate(user_ids):
                try:
                    if media_url:
                        await bot_client.send_file(
                            uid,
                            media_url,
                            caption=text,
                            parse_mode="html",
                        )
                    else:
                        await bot_client.send_message(
                            uid,
                            text,
                            parse_mode="html",
                        )
                    success += 1
                except Exception:
                    failed += 1

                if (i + 1) % 5 == 0:
                    try:
                        await utils.answer(
                            status_msg,
                            self.strings["adv_sending"].format(
                                current=i + 1, total=total
                            ),
                        )
                    except Exception:
                        pass
                    await asyncio.sleep(0.5)

            await self._destroy_bot_client(bot_client, session_path)

            await utils.answer(
                status_msg,
                self.strings["adv_done"].format(
                    success=success, total=total, failed=failed
                ),
            )

            self._adv_state = None

        except Exception as e:
            if bot_client and session_path:
                await self._destroy_bot_client(bot_client, session_path)
            await utils.answer(
                message, self.strings["error"].format(error=str(e)[:200])
            )
            self._adv_state = None

    async def on_unload(self):
        self._adv_state = None
        if self._temp_dir and os.path.exists(self._temp_dir):
            shutil.rmtree(self._temp_dir, ignore_errors=True)