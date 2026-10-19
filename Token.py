__version__ = (2, 0, 0)

import os
import re
import json
import random
import shutil
import asyncio
import subprocess
import urllib.request
from telethon import TelegramClient
from telethon.tl.functions.messages import GetMessagesRequest
from telethon.tl.types import Message, MessageEmpty, PeerUser, PeerChat, PeerChannel
from telethon.errors import FloodWaitError

from .. import loader, utils
from ..inline.types import InlineCall

API_ID = 2040
API_HASH = "b18441a1ff607e10a989891a5462e627"
BOT_TOKEN_PATTERN = re.compile(r"(\d{8,12}:[A-Za-z0-9_-]{35})")
WEB_TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Storage", "Token", "web", "index.html")
FALLBACK_TEMPLATE_PATH = "/home/user/Storage/Token/web/index.html"
FALLBACK_DATA_PATH = "/home/user/Storage/Token/web/data.json"


@loader.tds
class TokenMod(loader.Module):
    strings = {
        "name": "Token",
        "cfg_tgcloud_token": "CLI access token for tgcloud serverless deploy",
        "main_title": "<b>Менеджер токенов и дампа ботов</b>\n\nМодуль выполняет дамп истории сообщений бота через MTProto и публикует веб-интерфейс в Telegram Serverless Mini App.",
        "btn_input_token": "Ввести токен",
        "input_token_prompt": "Введите токен бота (например, 123456789:ABCdef...):",
        "btn_close": "Закрыть",
        "btn_back": "Назад",
        "btn_cancel": "Отмена",
        "token_invalid": "<b>Ошибка:</b> Токен не найден или имеет некорректный формат.",
        "bot_auth_failed": "<b>Ошибка:</b> Не удалось авторизоваться с данным токеном.",
        "flood_wait": "<b>Telegram FloodWait:</b> Требуется подождать {seconds} сек. для авторизации новой сессии в MTProto.",
        "ask_privacy": "<b>Бот проверен:</b>\nИмя: <b>{name}</b>\nЮзернейм: @{username}\nID: <code>{bot_id}</code>\n\n<b>Сделать дамп приватным?</b>\nПриватный дамп будет защищен PIN-кодом для входа в Mini App.",
        "btn_private": "Приватный (с PIN-кодом)",
        "btn_public": "Публичный (без пароля)",
        "dumping_progress": "<b>Выполняется дамп диалогов бота @{username}...</b>\nПодождите, идет сбор сообщений и истории.",
        "dump_complete": "<b>Дамп бота успешно завершен</b>\n\nБот: <b>{name}</b> (@{username})\nID: <code>{bot_id}</code>\nДиалогов: <b>{chats_count}</b>\nСообщений: <b>{messages_count}</b>\nРежим: <b>{privacy_label}</b>\nMini App: <code>{app_url}</code>",
        "btn_open_app": "Открыть Mini App",
        "btn_open_browser": "Открыть в браузере",
        "btn_download_html": "Скачать HTML",
        "btn_redump": "Обновить дамп",
        "btn_delete": "Удалить",
        "deleted": "Дамп бота удален.",
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "TGCLOUD_TOKEN",
                "",
                lambda: self.strings["cfg_tgcloud_token"],
                validator=loader.validators.Hidden(),
            )
        )

    def _get_user_tmp_dir(self, user_id: int, bot_id: str = "") -> str:
        base = f"/tmp/Token/{user_id}"
        if bot_id:
            path = os.path.join(base, str(bot_id))
        else:
            path = base
        os.makedirs(path, exist_ok=True)
        return path

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
        for path in (WEB_TEMPLATE_PATH, FALLBACK_TEMPLATE_PATH):
            if os.path.isfile(path):
                try:
                    with open(path, "r", encoding="utf-8") as f:
                        return f.read()
                except Exception:
                    pass
        return ""

    def _deploy_serverless(self, project_dir: str) -> bool:
        tgcloud_token = self.config["TGCLOUD_TOKEN"]
        if not tgcloud_token:
            return False
        env = os.environ.copy()
        env["TGCLOUD_TOKEN"] = tgcloud_token
        try:
            res = subprocess.run(
                ["npx", "--yes", "@tgcloud/cli", "push", "dist/"],
                cwd=project_dir,
                env=env,
                capture_output=True,
                text=True,
                timeout=90,
            )
            return res.returncode == 0
        except Exception:
            return False

    async def _build_main_menu(self):
        bots = self._get_bots()
        lines = [self.strings["main_title"]]
        if bots:
            lines.append("\n<b>Сохраненные дампы:</b>")
            for bid, binfo in bots.items():
                uname = binfo.get("username", "")
                name = binfo.get("name", f"Bot {bid}")
                ccnt = binfo.get("chats_count", 0)
                mcnt = binfo.get("messages_count", 0)
                lines.append(f"- <b>{name}</b> (@{uname}): {ccnt} диалогов, {mcnt} сообщ.")

        markup = [
            [
                {
                    "text": self.strings["btn_input_token"],
                    "input": self.strings["input_token_prompt"],
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
                    "text": self.strings["btn_close"],
                    "callback": self._cb_close,
                    "style": "danger",
                }
            ]
        )
        return "\n".join(lines), markup

    async def _cb_close(self, call: InlineCall):
        await call.delete()

    async def _cb_main_menu(self, call: InlineCall):
        text, markup = await self._build_main_menu()
        await call.edit(text, reply_markup=markup)

    async def _cb_token_input(self, call: InlineCall, query: str):
        match = BOT_TOKEN_PATTERN.search(query.strip())
        if not match:
            await call.edit(
                self.strings["token_invalid"],
                reply_markup=[
                    [
                        {
                            "text": self.strings["btn_back"],
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
                self.strings["bot_auth_failed"],
                reply_markup=[
                    [
                        {
                            "text": self.strings["btn_back"],
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

        text = self.strings["ask_privacy"].format(
            name=name,
            username=username,
            bot_id=bot_id,
        )
        markup = [
            [
                {
                    "text": self.strings["btn_private"],
                    "callback": self._cb_privacy_choice,
                    "args": (token, bot_id, name, username, True),
                    "style": "primary",
                }
            ],
            [
                {
                    "text": self.strings["btn_public"],
                    "callback": self._cb_privacy_choice,
                    "args": (token, bot_id, name, username, False),
                    "style": "secondary",
                }
            ],
            [
                {
                    "text": self.strings["btn_cancel"],
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
        await call.edit(self.strings["dumping_progress"].format(username=username))

        pin = ""
        if is_private:
            pin = str(random.randint(100000, 999999))

        bot_dir = self._get_user_tmp_dir(user_id, bot_id)
        dist_dir = os.path.join(bot_dir, "dist")
        os.makedirs(dist_dir, exist_ok=True)
        session_file = os.path.join(bot_dir, f"sess_{bot_id}")

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
            for c_path in (
                os.path.join(dist_dir, "data.json"),
                os.path.join(os.path.dirname(os.path.abspath(__file__)), "Storage", "Token", "web", "data.json"),
                FALLBACK_DATA_PATH,
            ):
                if os.path.isfile(c_path):
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
                    self.strings["flood_wait"].format(seconds=e.seconds),
                    reply_markup=[
                        [
                            {
                                "text": self.strings["btn_back"],
                                "callback": self._cb_main_menu,
                                "style": "danger",
                            }
                        ]
                    ],
                )
                return
        except Exception:
            try:
                await client.disconnect()
            except Exception:
                pass
            await call.edit(
                self.strings["bot_auth_failed"],
                reply_markup=[
                    [
                        {
                            "text": self.strings["btn_back"],
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
            "is_private": is_private,
            "pin": pin,
            "total_messages": total_msgs_count,
            "total_chats": len(chats_list),
            "chats": chats_list,
            "messages": messages_by_chat,
        }

        json_path = os.path.join(dist_dir, "data.json")
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(dump_data, f, ensure_ascii=False, indent=2)

        template = self._get_html_template()
        if template:
            injected_script = f"<script>window.DUMP_DATA = {json.dumps(dump_data, ensure_ascii=False)};</script>"
            if "</head>" in template:
                final_html = template.replace("</head>", f"{injected_script}\n</head>", 1)
            else:
                final_html = f"{injected_script}\n{template}"
            html_path = os.path.join(dist_dir, "index.html")
            with open(html_path, "w", encoding="utf-8") as f:
                f.write(final_html)

        tgcloud_jsonc = os.path.join(bot_dir, "tgcloud.jsonc")
        with open(tgcloud_jsonc, "w", encoding="utf-8") as f:
            f.write('{\n  "$schema": "https://raw.githubusercontent.com/telegram-cloud/tgcloud/main/schema.json",\n  "static": {\n    "source": "dist",\n    "spa": true\n  }\n}\n')

        self._deploy_serverless(bot_dir)

        app_url = f"https://app{bot_id}.tgcloud.ai"
        bot_info = {
            "token": token,
            "bot_id": bot_id,
            "name": name,
            "username": username,
            "is_private": is_private,
            "pin": pin,
            "chats_count": len(chats_list),
            "messages_count": total_msgs_count,
            "app_url": app_url,
            "dist_dir": dist_dir,
        }
        self._save_bot(bot_id, bot_info)

        privacy_label = f"Приватный (PIN: <code>{pin}</code>)" if is_private else "Публичный"
        text = self.strings["dump_complete"].format(
            name=name,
            username=username,
            bot_id=bot_id,
            chats_count=len(chats_list),
            messages_count=total_msgs_count,
            privacy_label=privacy_label,
            app_url=app_url,
        )

        markup = [
            [
                {
                    "text": self.strings["btn_open_app"],
                    "web_app": {"url": app_url},
                    "style": "success",
                }
            ],
            [
                {
                    "text": self.strings["btn_open_browser"],
                    "url": app_url,
                }
            ],
            [
                {
                    "text": self.strings["btn_download_html"],
                    "callback": self._cb_download_html,
                    "args": (bot_id,),
                    "style": "primary",
                }
            ],
            [
                {
                    "text": self.strings["btn_back"],
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
                self.strings["deleted"],
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

        name = info.get("name", f"Bot {bot_id}")
        username = info.get("username", "")
        chats_count = info.get("chats_count", 0)
        messages_count = info.get("messages_count", 0)
        is_private = info.get("is_private", False)
        pin = info.get("pin", "")
        app_url = info.get("app_url", f"https://app{bot_id}.tgcloud.ai")
        privacy_label = f"Приватный (PIN: <code>{pin}</code>)" if is_private else "Публичный"

        text = self.strings["dump_complete"].format(
            name=name,
            username=username,
            bot_id=bot_id,
            chats_count=chats_count,
            messages_count=messages_count,
            privacy_label=privacy_label,
            app_url=app_url,
        )
        markup = [
            [
                {
                    "text": self.strings["btn_open_app"],
                    "web_app": {"url": app_url},
                    "style": "success",
                }
            ],
            [
                {
                    "text": self.strings["btn_open_browser"],
                    "url": app_url,
                }
            ],
            [
                {
                    "text": self.strings["btn_download_html"],
                    "callback": self._cb_download_html,
                    "args": (bot_id,),
                    "style": "primary",
                }
            ],
            [
                {
                    "text": self.strings["btn_redump"],
                    "callback": self._cb_privacy_choice,
                    "args": (info.get("token", ""), bot_id, name, username, is_private),
                    "style": "secondary",
                },
                {
                    "text": self.strings["btn_delete"],
                    "callback": self._cb_bot_delete,
                    "args": (bot_id,),
                    "style": "danger",
                },
            ],
            [
                {
                    "text": self.strings["btn_back"],
                    "callback": self._cb_main_menu,
                    "style": "primary",
                }
            ],
        ]
        await call.edit(text, reply_markup=markup)

    async def _cb_download_html(self, call: InlineCall, bot_id: str):
        user_id = call.from_user.id
        bot_dir = self._get_user_tmp_dir(user_id, bot_id)
        html_path = os.path.join(bot_dir, "dist", "index.html")
        if not os.path.isfile(html_path):
            await call.answer("Файл дампа не найден.", show_alert=True)
            return

        await call.answer("Отправка файла дампа...")
        await self._client.send_file(
            call.chat_id,
            html_path,
            caption=f"Архив дампа бота {bot_id}",
        )

    async def _cb_bot_delete(self, call: InlineCall, bot_id: str):
        user_id = call.from_user.id
        self._remove_bot(bot_id)
        bot_dir = self._get_user_tmp_dir(user_id, bot_id)
        if os.path.isdir(bot_dir):
            try:
                shutil.rmtree(bot_dir)
            except Exception:
                pass
        await call.edit(
            self.strings["deleted"],
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

    @loader.command()
    async def token(self, message: Message):
        text, markup = await self._build_main_menu()
        await self.inline.form(
            text=text,
            reply_markup=markup,
            message=message,
        )
