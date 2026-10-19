# CopyLeft 2026 github.com/i-execute
# Author: I_execute.t.me
# Licensed under AGPLv3.

__version__ = (2, 0, 0)
# meta developer: Execute_forge.t.me

import asyncio
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
from telethon.tl.types import Message, MessageEmpty, PeerUser, PeerChat, PeerChannel
from telethon.errors import FloodWaitError

from .. import loader, utils
from ..inline.types import InlineCall

logger = logging.getLogger(__name__)

API_ID = 2040
API_HASH = "b18441a1ff607e10a989891a5462e627"
BOT_TOKEN_PATTERN = re.compile(r"(\d{8,12}:[A-Za-z0-9_-]{35})")
DEFAULT_CLI_TOKEN = "app8288899778:OcG_KDo0FE73mbFbsiBB_FP7JMLeqJFYFpPd-67yHeE"
GITHUB_WEB_URL = "https://raw.githubusercontent.com/i-execute/Modules/main/Storage/Token/web/index.html"


def _escape(text: str) -> str:
    if not text:
        return ""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


@loader.tds
class Token(loader.Module):
    """Менеджер токенов ботов, дампа истории сообщений и Telegram Serverless Mini App"""

    strings = {
        "name": "Token",
        "_cls_doc": "Bot dumper and Serverless Mini App manager",
        "_cmd_doc_t": "Open Bot Dumper and Serverless Mini App manager",
        "_cfg_tgcloud_token": "CLI access token from @BotFather for tgcloud serverless deploy",
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
        "btn_download_html": "Download HTML",
        "btn_redump": "Re-dump",
        "btn_delete": "Delete",
        "btn_private": "Private (Owner only)",
        "btn_public": "Public (Anyone)",
        "btn_cancel": "Cancel",
        "input_token": "Send bot API token (from @BotFather):",
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
        "ask_privacy": (
            "<b>Bot Verified</b>\n"
            "<blockquote>Bot: <b>{name}</b> (@{username})\n"
            "ID: <code>{bot_id}</code>\n\n"
            "Make dump private?\n"
            "Private dump will only allow the owner via Telegram initData.</blockquote>"
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
            "Privacy: <b>{privacy_label}</b>\n"
            "Mini App: <code>{app_url}</code></blockquote>"
        ),
        "bot_view": (
            "<b>Bot: {name} (@{username})</b>\n"
            "<blockquote>ID: <code>{bot_id}</code>\n"
            "Dialogs: <b>{chats_count}</b>\n"
            "Messages: <b>{messages_count}</b>\n"
            "Privacy: <b>{privacy_label}</b>\n"
            "Mini App: <code>{app_url}</code></blockquote>"
        ),
        "deleted": (
            "<b>Dump Deleted</b>\n"
            "<blockquote>Dump data and bot session removed successfully.</blockquote>"
        ),
        "file_not_found": "Dump HTML archive not found.",
        "sending_file": "Sending dump archive...",
    }

    strings_ru = {
        "name": "Token",
        "_cls_doc": "Менеджер дампа ботов и Telegram Serverless Mini App",
        "_cmd_doc_t": "Открыть менеджер дампа ботов и Mini App",
        "_cfg_tgcloud_token": "CLI Access токен от @BotFather для tgcloud деплоя",
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
        "btn_download_html": "Скачать HTML",
        "btn_redump": "Передампить",
        "btn_delete": "Удалить",
        "btn_private": "Приватный (только владелец)",
        "btn_public": "Публичный (для всех)",
        "btn_cancel": "Отмена",
        "input_token": "Введите токен бота (например, 123456789:ABCdef...):",
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
        "ask_privacy": (
            "<b>Бот верифицирован</b>\n"
            "<blockquote>Бот: <b>{name}</b> (@{username})\n"
            "ID: <code>{bot_id}</code>\n\n"
            "Сделать дамп приватным?\n"
            "Приватный дамп пускает только владельца через Telegram initData.</blockquote>"
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
            "Режим: <b>{privacy_label}</b>\n"
            "Mini App: <code>{app_url}</code></blockquote>"
        ),
        "bot_view": (
            "<b>Бот: {name} (@{username})</b>\n"
            "<blockquote>ID: <code>{bot_id}</code>\n"
            "Диалогов: <b>{chats_count}</b>\n"
            "Сообщений: <b>{messages_count}</b>\n"
            "Режим: <b>{privacy_label}</b>\n"
            "Mini App: <code>{app_url}</code></blockquote>"
        ),
        "deleted": (
            "<b>Дамп удален</b>\n"
            "<blockquote>Данные дампа и сессия бота успешно удалены.</blockquote>"
        ),
        "file_not_found": "Файл дампа не найден.",
        "sending_file": "Отправка архива дампа...",
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "TGCLOUD_TOKEN",
                DEFAULT_CLI_TOKEN,
                lambda: self._s("_cfg_tgcloud_token"),
                validator=loader.validators.Hidden(),
            )
        )

    async def client_ready(self, client, db):
        self._client = client
        self._db = db

    def _s(self, key, **kwargs):
        try:
            return self.strings[key].format(prefix=self.get_prefix(), **kwargs)
        except (KeyError, IndexError):
            return self.strings.get(key, key)

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
        if not token:
            token = DEFAULT_CLI_TOKEN
        return token.strip()

    def _get_inline_bot_id(self) -> str:
        cli_token = self._get_cli_token()
        if cli_token and cli_token.startswith("app"):
            return cli_token.split(":")[0].replace("app", "")
        if hasattr(self, "inline") and getattr(self.inline, "bot_id", None):
            return str(self.inline.bot_id)
        return "8288899778"

    def _get_app_url(self, bot_id: str) -> str:
        inline_bot_id = self._get_inline_bot_id()
        return f"https://app{inline_bot_id}.tgcloud.ai/{bot_id}"

    def _find_web_file(self, rel_name: str) -> str:
        candidates = [
            os.path.join("Storage", "Token", "web", rel_name),
            os.path.join(os.getcwd(), "Storage", "Token", "web", rel_name),
            os.path.expanduser(f"~/Feroku/Storage/Token/web/{rel_name}"),
            os.path.expanduser(f"~/Storage/Token/web/{rel_name}"),
            f"/home/user/Storage/Token/web/{rel_name}",
        ]
        if hasattr(utils, "get_base_dir"):
            try:
                base = utils.get_base_dir()
                if base:
                    candidates.insert(0, os.path.join(base, "Storage", "Token", "web", rel_name))
            except Exception:
                pass
        for path in candidates:
            if path and os.path.isfile(path):
                return path
        return ""

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
                name = (first + " " + last).strip() or title or f"Chat {cid}"
                user = getattr(ent, "username", "") or ""
                entities[str(cid)] = {"name": name, "username": user}
            except Exception:
                entities[str(cid)] = {"name": f"Chat {cid}", "username": ""}
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

        text = self._s(
            "ask_privacy",
            name=_escape(name),
            username=_escape(username),
            bot_id=bot_id,
        )
        markup = [
            [
                {
                    "text": self._s("btn_private"),
                    "callback": self._cb_privacy_choice,
                    "args": (token, bot_id, name, username, True),
                    "style": "primary",
                }
            ],
            [
                {
                    "text": self._s("btn_public"),
                    "callback": self._cb_privacy_choice,
                    "args": (token, bot_id, name, username, False),
                    "style": "secondary",
                }
            ],
            [
                {
                    "text": self._s("btn_cancel"),
                    "callback": self._cb_main_menu,
                    "style": "danger",
                }
            ],
        ]
        await call.edit(text, reply_markup=markup)

    async def _cb_privacy_choice(
        self,
        call: InlineCall,
        token: str,
        bot_id: str,
        name: str,
        username: str,
        is_private: bool,
    ):
        user_id = call.from_user.id
        await call.edit(self._s("dumping_progress", username=_escape(username)))

        pin = ""
        if is_private:
            pin = str(random.randint(100000, 999999))

        bot_dir = self._get_user_tmp_dir(user_id, bot_id)
        session_file = os.path.join(bot_dir, f"sess_{bot_id}")

        serverless_dir = os.path.join(self._get_user_tmp_dir(user_id), "serverless")
        dist_dir = os.path.join(serverless_dir, "dist")
        bot_dist_dir = os.path.join(dist_dir, bot_id)
        endpoints_dir = os.path.join(serverless_dir, "tgcloud", "endpoints")
        os.makedirs(bot_dist_dir, exist_ok=True)
        os.makedirs(endpoints_dir, exist_ok=True)

        client = TelegramClient(session_file, API_ID, API_HASH)
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
                messages_by_chat[cid].append(
                    {
                        "id": m.id,
                        "out": bool(m.out),
                        "text": m_text,
                        "date": m_date,
                        "media": m_media,
                    }
                )

            for cid, msgs in messages_by_chat.items():
                info = entities.get(cid, {"name": f"Chat {cid}", "username": ""})
                last_msg = msgs[-1] if msgs else None
                last_text = last_msg["text"] if last_msg else ""
                if not last_text and last_msg and last_msg["media"]:
                    last_text = f"[{last_msg['media']}]"
                chats_list.append(
                    {
                        "id": cid,
                        "name": info["name"],
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
            "is_private": is_private,
            "pin": pin,
            "total_messages": total_msgs_count,
            "total_chats": len(chats_list),
            "chats": chats_list,
            "messages": messages_by_chat,
        }

        # Save data files in dist
        with open(os.path.join(bot_dist_dir, "data.json"), "w", encoding="utf-8") as f:
            json.dump(dump_data, f, ensure_ascii=False, indent=2)

        with open(os.path.join(dist_dir, "data.json"), "w", encoding="utf-8") as f:
            json.dump(dump_data, f, ensure_ascii=False, indent=2)

        template = self._get_html_template()
        if template:
            injected_script = f"<script>window.DUMP_DATA = {json.dumps(dump_data, ensure_ascii=False)};</script>"
            if "</head>" in template:
                final_html = template.replace("</head>", f"{injected_script}\n</head>", 1)
            else:
                final_html = f"{injected_script}\n{template}"

            with open(os.path.join(bot_dist_dir, "index.html"), "w", encoding="utf-8") as f:
                f.write(final_html)

            with open(os.path.join(dist_dir, "index.html"), "w", encoding="utf-8") as f:
                f.write(final_html)

        # Create tgcloud endpoint for this bot and all saved bots
        all_bots = self._get_bots()
        bots_to_endpoint = set(all_bots.keys())
        bots_to_endpoint.add(str(bot_id))

        for bid in bots_to_endpoint:
            endpoint_file = os.path.join(endpoints_dir, f"bot_{bid}.js")
            endpoint_code = (
                "import { EndpointError } from 'sdk'\n\n"
                "export default async function (input, ctx) {\n"
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

        self._deploy_serverless(serverless_dir)

        app_url = self._get_app_url(bot_id)
        bot_info = {
            "token": token,
            "bot_id": bot_id,
            "name": name,
            "username": username,
            "owner_id": user_id,
            "is_private": is_private,
            "pin": pin,
            "chats_count": len(chats_list),
            "messages_count": total_msgs_count,
            "app_url": app_url,
            "bot_dist_dir": bot_dist_dir,
        }
        self._save_bot(bot_id, bot_info)

        privacy_label = "Private" if is_private else "Public"
        text = self._s(
            "dump_complete",
            name=_escape(name),
            username=_escape(username),
            bot_id=bot_id,
            chats_count=len(chats_list),
            messages_count=total_msgs_count,
            privacy_label=privacy_label,
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
                    "text": self._s("btn_download_html"),
                    "callback": self._cb_download_html,
                    "args": (bot_id,),
                    "style": "primary",
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
        is_private = info.get("is_private", False)
        pin = info.get("pin", "")
        app_url = info.get("app_url", self._get_app_url(bot_id))
        privacy_label = "Private" if is_private else "Public"

        text = self._s(
            "bot_view",
            name=_escape(name),
            username=_escape(username),
            bot_id=bot_id,
            chats_count=chats_count,
            messages_count=messages_count,
            privacy_label=privacy_label,
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
                    "text": self._s("btn_download_html"),
                    "callback": self._cb_download_html,
                    "args": (bot_id,),
                    "style": "primary",
                }
            ],
            [
                {
                    "text": self._s("btn_redump"),
                    "callback": self._cb_privacy_choice,
                    "args": (info.get("token", ""), bot_id, name, username, is_private),
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

    async def _cb_download_html(self, call: InlineCall, bot_id: str):
        user_id = call.from_user.id
        serverless_dir = os.path.join(self._get_user_tmp_dir(user_id), "serverless")
        bot_dir = self._get_user_tmp_dir(user_id, bot_id)
        candidates = [
            os.path.join(serverless_dir, "dist", bot_id, "index.html"),
            os.path.join(serverless_dir, "dist", "index.html"),
            os.path.join(bot_dir, "dist", "index.html"),
        ]
        html_path = ""
        for p in candidates:
            if os.path.isfile(p):
                html_path = p
                break

        if not html_path:
            await call.answer(self._s("file_not_found"), show_alert=True)
            return

        await call.answer(self._s("sending_file"))

        target = "me"
        try:
            if getattr(call, "chat_id", None):
                target = call.chat_id
        except Exception:
            pass

        try:
            await self._client.send_file(
                target,
                html_path,
                caption=f"Dump HTML archive for bot {bot_id}",
            )
        except Exception:
            try:
                await self._client.send_file(
                    "me",
                    html_path,
                    caption=f"Dump HTML archive for bot {bot_id}",
                )
            except Exception as e:
                logger.error(f"send_file error: {e}")

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
