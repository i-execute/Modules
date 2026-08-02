__version__ = (1, 1, 0)
# meta developer: I_execute.t.me

import asyncio
import base64
import json
import logging
import time
from typing import Optional

import aiohttp

from telethon.tl.types import Message
from .. import loader, utils
from ..inline.types import InlineCall

logger = logging.getLogger(__name__)

GITHUB_API = "https://api.github.com"
DB_PATH = "/DATABASE.json"


def _esc(text):
    if not text:
        return ""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


async def _github_get_file(token: str, repo: str, path: str):
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                f"{GITHUB_API}/repos/{repo}/contents{path}",
                headers={
                    "Authorization": f"token {token}",
                    "Accept": "application/vnd.github.v3+json",
                },
                timeout=aiohttp.ClientTimeout(total=15),
            ) as r:
                if r.status == 404:
                    return None, None
                if r.status != 200:
                    return None, None
                data = await r.json()
                content = base64.b64decode(data["content"]).decode()
                return json.loads(content), data["sha"]
    except Exception:
        return None, None


async def _github_push_file(token: str, repo: str, path: str, content: dict, sha: Optional[str], message: str):
    try:
        encoded = base64.b64encode(json.dumps(content, indent=2).encode()).decode()
        payload = {
            "message": message,
            "content": encoded,
        }
        if sha:
            payload["sha"] = sha
        async with aiohttp.ClientSession() as s:
            async with s.put(
                f"{GITHUB_API}/repos/{repo}/contents{path}",
                headers={
                    "Authorization": f"token {token}",
                    "Accept": "application/vnd.github.v3+json",
                },
                json=payload,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as r:
                return r.status in (200, 201)
    except Exception:
        return False


async def _parse_chat_members(client, chat_id):
    ids = set()
    calls = 0
    scanned = 0
    started = time.monotonic()

    try:
        entity = await client.get_entity(chat_id)
    except Exception as e:
        return None, str(e)

    offset_id = 0
    while True:
        try:
            batch = []
            async for msg in client.iter_messages(entity, limit=100, offset_id=offset_id):
                batch.append(msg)
            calls += 1
            if not batch:
                break
            for msg in batch:
                scanned += 1
                sender_id = None
                if msg.sender_id:
                    sender_id = msg.sender_id
                elif hasattr(msg, "from_id") and msg.from_id:
                    fid = msg.from_id
                    if hasattr(fid, "user_id"):
                        sender_id = fid.user_id
                    elif hasattr(fid, "channel_id"):
                        sender_id = fid.channel_id
                    elif hasattr(fid, "chat_id"):
                        sender_id = fid.chat_id
                if sender_id:
                    ids.add(sender_id)
            offset_id = batch[-1].id
            if len(batch) < 100:
                break
            await asyncio.sleep(0.05)
        except Exception as e:
            logger.warning("parse error: %s", e)
            break

    elapsed = round(time.monotonic() - started, 2)
    return {
        "ids": list(ids),
        "calls": calls,
        "scanned": scanned,
        "unique": len(ids),
        "elapsed": elapsed,
    }, None


@loader.tds
class RestrictAPI(loader.Module):
    """RestrictAPI - chat member parser and restrict database manager"""

    strings = {
        "name": "RestrictAPI",

        "main_menu": (
            "<b>RestrictAPI</b>\n"
            "<blockquote>Select action</blockquote>"
        ),

        "parsing_input_chat": (
            "<b>Parsing: Enter Chat ID</b>\n"
            "<blockquote>Enter chat ID or username to parse</blockquote>"
        ),

        "parsing_select_reason": (
            "<b>Parsing: Select Reason</b>\n"
            "<blockquote>Chat: <code>{chat_id}</code></blockquote>"
        ),

        "parsing_running": (
            "<b>Parsing in progress...</b>\n"
            "<blockquote>Chat: <code>{chat_id}</code>\n"
            "Please wait, fetching all messages</blockquote>"
        ),

        "parsing_confirm": (
            "<b>Parsing Complete</b>\n"
            "<blockquote>"
            "Chat: <code>{chat_id}</code>\n"
            "Reason: {reason}\n"
            "GetHistory calls: {calls}\n"
            "Messages scanned: {scanned}\n"
            "Unique user IDs: {unique}\n"
            "Elapsed: {elapsed}s\n\n"
            "Push to GitHub or export JSON?"
            "</blockquote>"
        ),

        "parsing_error": (
            "<b>Parse Error</b>\n"
            "<blockquote>{error}</blockquote>"
        ),

        "push_running": "<b>Pushing to GitHub...</b>",

        "push_ok": (
            "<b>Push Complete</b>\n"
            "<blockquote>"
            "Added: {added} new IDs\n"
            "Skipped: {skipped} already in DB\n"
            "Total in DB: {total}"
            "</blockquote>"
        ),

        "push_fail": (
            "<b>Push Failed</b>\n"
            "<blockquote>Check GitHub token and repository settings</blockquote>"
        ),

        "no_settings": (
            "<b>Settings Required</b>\n"
            "<blockquote>Configure GitHub token and repository in settings first</blockquote>"
        ),

        "cache_empty": (
            "<b>Cache Empty</b>\n"
            "<blockquote>No parsed data in cache. Run parsing first</blockquote>"
        ),

        "cache_info": (
            "<b>Cache</b>\n"
            "<blockquote>"
            "Unique IDs: {unique}\n"
            "Reason: {reason}\n"
            "Chat: {chat_id}"
            "</blockquote>"
        ),

        "export_sent": (
            "<b>Export Sent</b>\n"
            "<blockquote>cache.json sent to this chat</blockquote>"
        ),

        "import_prompt": (
            "<b>Import JSON</b>\n"
            "<blockquote>Reply to a .json file or a message containing JSON with .rapi command, or use this button after replying</blockquote>"
        ),

        "import_ok": (
            "<b>Import Complete</b>\n"
            "<blockquote>"
            "Loaded IDs: {unique}\n"
            "Reason: {reason}"
            "</blockquote>"
        ),

        "import_fail": (
            "<b>Import Failed</b>\n"
            "<blockquote>Could not parse JSON from reply</blockquote>"
        ),

        "import_push_confirm": (
            "<b>Imported Cache Ready</b>\n"
            "<blockquote>"
            "IDs loaded: {unique}\n"
            "Reason: {reason}\n\n"
            "Push to GitHub?"
            "</blockquote>"
        ),

        "direct_input_id": (
            "<b>Direct: Enter User ID</b>\n"
            "<blockquote>Enter user ID to add directly</blockquote>"
        ),

        "direct_select_reason": (
            "<b>Direct: Select Reason</b>\n"
            "<blockquote>User ID: <code>{user_id}</code></blockquote>"
        ),

        "direct_confirm": (
            "<b>Direct: Confirm</b>\n"
            "<blockquote>"
            "User ID: <code>{user_id}</code>\n"
            "Reason: {reason}\n\n"
            "Push to GitHub?"
            "</blockquote>"
        ),

        "direct_ok": (
            "<b>Done</b>\n"
            "<blockquote>"
            "User ID: <code>{user_id}</code>\n"
            "Status: {status}\n"
            "Total in DB: {total}"
            "</blockquote>"
        ),

        "settings_menu": (
            "<b>Settings</b>\n"
            "<blockquote>"
            "Repository: {repo}\n"
            "Token: {token}"
            "</blockquote>"
        ),

        "btn_parsing": "Parsing",
        "btn_direct": "Direct",
        "btn_settings": "Settings",
        "btn_back": "Back",
        "btn_close": "Close",
        "btn_push": "Push",
        "btn_export": "Export JSON",
        "btn_import": "Import JSON",
        "btn_cache": "Cache",
        "btn_cancel": "Cancel",
        "btn_set_repo": "Set Repository",
        "btn_set_token": "Set Token",

        "input_chat_id": "Enter chat ID or @username:",
        "input_user_id": "Enter user ID:",
        "input_repo": "Enter repository (owner/repo):",
        "input_token": "Enter GitHub token:",

        "reason_savemode": "Save Mode",
        "reason_extrascam": "Extra Scam",
        "reason_ordinaryscam": "Ordinary Scam",

        "status_added": "Added to DB",
        "status_skipped": "Already in DB (skipped)",
    }

    strings_ru = {
        "main_menu": (
            "<b>RestrictAPI</b>\n"
            "<blockquote>Выберите действие</blockquote>"
        ),

        "parsing_input_chat": (
            "<b>Парсинг: Введите ID чата</b>\n"
            "<blockquote>Введите ID чата или username</blockquote>"
        ),

        "parsing_select_reason": (
            "<b>Парсинг: Выберите причину</b>\n"
            "<blockquote>Чат: <code>{chat_id}</code></blockquote>"
        ),

        "parsing_running": (
            "<b>Парсинг запущен...</b>\n"
            "<blockquote>Чат: <code>{chat_id}</code>\n"
            "Подождите, идёт сбор сообщений</blockquote>"
        ),

        "parsing_confirm": (
            "<b>Парсинг завершён</b>\n"
            "<blockquote>"
            "Чат: <code>{chat_id}</code>\n"
            "Причина: {reason}\n"
            "GetHistory вызовов: {calls}\n"
            "Сообщений просканировано: {scanned}\n"
            "Уникальных ID: {unique}\n"
            "Время: {elapsed}s\n\n"
            "Запушить на GitHub или экспортировать JSON?"
            "</blockquote>"
        ),

        "parsing_error": (
            "<b>Ошибка парсинга</b>\n"
            "<blockquote>{error}</blockquote>"
        ),

        "push_running": "<b>Отправка на GitHub...</b>",

        "push_ok": (
            "<b>Готово</b>\n"
            "<blockquote>"
            "Добавлено: {added} новых ID\n"
            "Пропущено: {skipped} уже в БД\n"
            "Всего в БД: {total}"
            "</blockquote>"
        ),

        "push_fail": (
            "<b>Ошибка отправки</b>\n"
            "<blockquote>Проверьте токен GitHub и настройки репозитория</blockquote>"
        ),

        "no_settings": (
            "<b>Требуются настройки</b>\n"
            "<blockquote>Сначала настройте токен GitHub и репозиторий</blockquote>"
        ),

        "cache_empty": (
            "<b>Кеш пуст</b>\n"
            "<blockquote>Нет данных в кеше. Сначала запустите парсинг</blockquote>"
        ),

        "cache_info": (
            "<b>Кеш</b>\n"
            "<blockquote>"
            "Уникальных ID: {unique}\n"
            "Причина: {reason}\n"
            "Чат: {chat_id}"
            "</blockquote>"
        ),

        "export_sent": (
            "<b>Экспорт отправлен</b>\n"
            "<blockquote>cache.json отправлен в этот чат</blockquote>"
        ),

        "import_prompt": (
            "<b>Импорт JSON</b>\n"
            "<blockquote>Ответьте на .json файл или сообщение с JSON командой .rapi, или используйте кнопку после реплая</blockquote>"
        ),

        "import_ok": (
            "<b>Импорт завершён</b>\n"
            "<blockquote>"
            "Загружено ID: {unique}\n"
            "Причина: {reason}"
            "</blockquote>"
        ),

        "import_fail": (
            "<b>Ошибка импорта</b>\n"
            "<blockquote>Не удалось разобрать JSON из реплая</blockquote>"
        ),

        "import_push_confirm": (
            "<b>Импортированный кеш готов</b>\n"
            "<blockquote>"
            "Загружено ID: {unique}\n"
            "Причина: {reason}\n\n"
            "Запушить на GitHub?"
            "</blockquote>"
        ),

        "direct_input_id": (
            "<b>Прямой: Введите ID пользователя</b>\n"
            "<blockquote>Введите ID пользователя для добавления</blockquote>"
        ),

        "direct_select_reason": (
            "<b>Прямой: Выберите причину</b>\n"
            "<blockquote>ID пользователя: <code>{user_id}</code></blockquote>"
        ),

        "direct_confirm": (
            "<b>Прямой: Подтверждение</b>\n"
            "<blockquote>"
            "ID пользователя: <code>{user_id}</code>\n"
            "Причина: {reason}\n\n"
            "Запушить на GitHub?"
            "</blockquote>"
        ),

        "direct_ok": (
            "<b>Готово</b>\n"
            "<blockquote>"
            "ID пользователя: <code>{user_id}</code>\n"
            "Статус: {status}\n"
            "Всего в БД: {total}"
            "</blockquote>"
        ),

        "settings_menu": (
            "<b>Настройки</b>\n"
            "<blockquote>"
            "Репозиторий: {repo}\n"
            "Токен: {token}"
            "</blockquote>"
        ),

        "btn_parsing": "Парсинг",
        "btn_direct": "Прямой",
        "btn_settings": "Настройки",
        "btn_back": "Назад",
        "btn_close": "Закрыть",
        "btn_push": "Запушить",
        "btn_export": "Экспорт JSON",
        "btn_import": "Импорт JSON",
        "btn_cache": "Кеш",
        "btn_cancel": "Отмена",
        "btn_set_repo": "Репозиторий",
        "btn_set_token": "Токен",

        "input_chat_id": "Введите ID чата или @username:",
        "input_user_id": "Введите ID пользователя:",
        "input_repo": "Введите репозиторий (owner/repo):",
        "input_token": "Введите GitHub токен:",

        "reason_savemode": "Save Mode",
        "reason_extrascam": "Extra Scam",
        "reason_ordinaryscam": "Ordinary Scam",

        "status_added": "Добавлен в БД",
        "status_skipped": "Уже в БД (пропущен)",
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "savemode_label",
                "savemode",
                "Label for savemode reason",
                validator=loader.validators.String(),
            ),
            loader.ConfigValue(
                "extrascam_label",
                "extrascam",
                "Label for extrascam reason",
                validator=loader.validators.String(),
            ),
            loader.ConfigValue(
                "ordinaryscam_label",
                "ordinaryscam",
                "Label for ordinaryscam reason",
                validator=loader.validators.String(),
            ),
        )
        self._local_cache = {}

    async def client_ready(self, client, db):
        self._client = client
        self._db = db

    def _get_repo(self):
        return self._db.get("RestrictAPI", "repo", None)

    def _get_token(self):
        return self._db.get("RestrictAPI", "token", None)

    def _set_repo(self, repo):
        self._db.set("RestrictAPI", "repo", repo)

    def _set_token(self, token):
        self._db.set("RestrictAPI", "token", token)

    def _state_key(self, call):
        return f"rapi_state_{call.form.get('uid', 'x')}"

    def _get_state(self, call):
        return self.get(self._state_key(call), {})

    def _set_state(self, call, data):
        self.set(self._state_key(call), data)

    def _clear_state(self, call):
        self.set(self._state_key(call), {})

    def _reason_label(self, reason_key: str) -> str:
        return {
            "savemode": self.strings["reason_savemode"],
            "extrascam": self.strings["reason_extrascam"],
            "ordinaryscam": self.strings["reason_ordinaryscam"],
        }.get(reason_key, reason_key)

    def _reason_config_key(self, reason_key: str) -> str:
        return {
            "savemode": self.config["savemode_label"],
            "extrascam": self.config["extrascam_label"],
            "ordinaryscam": self.config["ordinaryscam_label"],
        }.get(reason_key, reason_key)

    def _main_markup(self):
        return [
            [
                {"text": self.strings["btn_parsing"], "callback": self._cb_parsing_start, "style": "primary"},
                {"text": self.strings["btn_direct"], "callback": self._cb_direct_start, "style": "primary"},
            ],
            [
                {"text": self.strings["btn_cache"], "callback": self._cb_cache_menu, "style": "primary"},
                {"text": self.strings["btn_settings"], "callback": self._cb_settings, "style": "primary"},
            ],
            [{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}],
        ]

    async def _cb_main(self, call: InlineCall):
        self._clear_state(call)
        await call.edit(self.strings["main_menu"], reply_markup=self._main_markup())

    async def _cb_close(self, call: InlineCall):
        self._clear_state(call)
        await call.delete()

    async def _cb_parsing_start(self, call: InlineCall):
        await call.edit(
            self.strings["parsing_input_chat"],
            reply_markup=[[
                {
                    "text": self.strings["input_chat_id"],
                    "input": self.strings["input_chat_id"],
                    "handler": self._cb_parsing_got_chat,
                    "style": "primary",
                }
            ], [
                {"text": self.strings["btn_back"], "callback": self._cb_main, "style": "danger"}
            ]],
        )

    async def _cb_parsing_got_chat(self, call: InlineCall, chat_id: str):
        chat_id = chat_id.strip()
        state = {"chat_id": chat_id}
        self._set_state(call, state)
        await self._show_parsing_reason(call)

    async def _show_parsing_reason(self, call: InlineCall):
        state = self._get_state(call)
        chat_id = state.get("chat_id", "?")
        await call.edit(
            self.strings["parsing_select_reason"].format(chat_id=_esc(str(chat_id))),
            reply_markup=[
                [
                    {"text": self.strings["reason_savemode"], "callback": self._cb_parsing_reason, "args": ("savemode",), "style": "primary"},
                    {"text": self.strings["reason_extrascam"], "callback": self._cb_parsing_reason, "args": ("extrascam",), "style": "primary"},
                ],
                [
                    {"text": self.strings["reason_ordinaryscam"], "callback": self._cb_parsing_reason, "args": ("ordinaryscam",), "style": "primary"},
                ],
                [{"text": self.strings["btn_back"], "callback": self._cb_parsing_start, "style": "danger"}],
            ],
        )

    async def _cb_parsing_reason(self, call: InlineCall, reason_key: str):
        state = self._get_state(call)
        state["reason"] = reason_key
        self._set_state(call, state)

        chat_id = state["chat_id"]
        await call.edit(
            self.strings["parsing_running"].format(chat_id=_esc(str(chat_id))),
            reply_markup=[],
        )

        try:
            raw_id = int(chat_id)
        except ValueError:
            raw_id = chat_id

        result, error = await _parse_chat_members(self._client, raw_id)

        if error:
            await call.edit(
                self.strings["parsing_error"].format(error=_esc(error)),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main, "style": "danger"}]],
            )
            return

        self._local_cache = {
            "ids": result["ids"],
            "reason": reason_key,
            "chat_id": chat_id,
            "calls": result["calls"],
            "scanned": result["scanned"],
            "unique": result["unique"],
            "elapsed": result["elapsed"],
        }

        state["parse_result"] = result
        self._set_state(call, state)

        await call.edit(
            self.strings["parsing_confirm"].format(
                chat_id=_esc(str(chat_id)),
                reason=self._reason_label(reason_key),
                calls=result["calls"],
                scanned=result["scanned"],
                unique=result["unique"],
                elapsed=result["elapsed"],
            ),
            reply_markup=[
                [
                    {"text": self.strings["btn_push"], "callback": self._cb_parsing_push, "style": "success"},
                    {"text": self.strings["btn_export"], "callback": self._cb_export_cache, "style": "primary"},
                ],
                [{"text": self.strings["btn_cancel"], "callback": self._cb_main, "style": "danger"}],
            ],
        )

    async def _cb_parsing_push(self, call: InlineCall):
        token = self._get_token()
        repo = self._get_repo()
        if not token or not repo:
            await call.edit(
                self.strings["no_settings"],
                reply_markup=[[{"text": self.strings["btn_settings"], "callback": self._cb_settings, "style": "primary"}]],
            )
            return

        state = self._get_state(call)
        result = state.get("parse_result", {})
        reason_key = state.get("reason", "savemode")
        reason_config = self._reason_config_key(reason_key)
        new_ids = result.get("ids", [])

        await call.edit(self.strings["push_running"], reply_markup=[])

        db_data, sha = await _github_get_file(token, repo, DB_PATH)
        if db_data is None:
            db_data = {}

        existing = set(str(i) for i in db_data.keys())
        added = 0
        skipped = 0

        for uid in new_ids:
            key = str(uid)
            if key in existing:
                skipped += 1
            else:
                db_data[key] = {"reason": reason_config}
                added += 1

        ok = await _github_push_file(
            token, repo, DB_PATH, db_data, sha,
            f"RestrictAPI: add {added} IDs reason={reason_config}",
        )

        if ok:
            await call.edit(
                self.strings["push_ok"].format(
                    added=added,
                    skipped=skipped,
                    total=len(db_data),
                ),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main, "style": "primary"}]],
            )
        else:
            await call.edit(
                self.strings["push_fail"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main, "style": "danger"}]],
            )

    async def _cb_cache_menu(self, call: InlineCall):
        if not self._local_cache:
            await call.edit(
                self.strings["cache_empty"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main, "style": "danger"}]],
            )
            return

        await call.edit(
            self.strings["cache_info"].format(
                unique=self._local_cache.get("unique", len(self._local_cache.get("ids", []))),
                reason=self._reason_label(self._local_cache.get("reason", "?")),
                chat_id=_esc(str(self._local_cache.get("chat_id", "?"))),
            ),
            reply_markup=[
                [
                    {"text": self.strings["btn_export"], "callback": self._cb_export_cache, "style": "primary"},
                    {"text": self.strings["btn_push"], "callback": self._cb_cache_push, "style": "success"},
                ],
                [{"text": self.strings["btn_back"], "callback": self._cb_main, "style": "danger"}],
            ],
        )

    async def _cb_export_cache(self, call: InlineCall):
        if not self._local_cache:
            await call.edit(
                self.strings["cache_empty"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main, "style": "danger"}]],
            )
            return

        export = {
            "ids": self._local_cache.get("ids", []),
            "reason": self._local_cache.get("reason", ""),
            "chat_id": self._local_cache.get("chat_id", ""),
            "unique": self._local_cache.get("unique", 0),
        }

        raw = json.dumps(export, indent=2).encode()

        import io
        buf = io.BytesIO(raw)
        buf.name = "cache.json"

        try:
            await self._client.send_file(
                call.form["chat"],
                buf,
                force_document=True,
                file_name="cache.json",
            )
        except Exception as e:
            logger.exception("export send_file failed: %s", e)

        await call.edit(
            self.strings["export_sent"],
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main, "style": "primary"}]],
        )

    async def _cb_cache_push(self, call: InlineCall):
        if not self._local_cache:
            await call.edit(
                self.strings["cache_empty"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main, "style": "danger"}]],
            )
            return

        token = self._get_token()
        repo = self._get_repo()
        if not token or not repo:
            await call.edit(
                self.strings["no_settings"],
                reply_markup=[[{"text": self.strings["btn_settings"], "callback": self._cb_settings, "style": "primary"}]],
            )
            return

        reason_key = self._local_cache.get("reason", "savemode")
        reason_config = self._reason_config_key(reason_key)
        new_ids = self._local_cache.get("ids", [])

        await call.edit(self.strings["push_running"], reply_markup=[])

        db_data, sha = await _github_get_file(token, repo, DB_PATH)
        if db_data is None:
            db_data = {}

        existing = set(str(i) for i in db_data.keys())
        added = 0
        skipped = 0

        for uid in new_ids:
            key = str(uid)
            if key in existing:
                skipped += 1
            else:
                db_data[key] = {"reason": reason_config}
                added += 1

        ok = await _github_push_file(
            token, repo, DB_PATH, db_data, sha,
            f"RestrictAPI: add {added} IDs reason={reason_config}",
        )

        if ok:
            await call.edit(
                self.strings["push_ok"].format(
                    added=added,
                    skipped=skipped,
                    total=len(db_data),
                ),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main, "style": "primary"}]],
            )
        else:
            await call.edit(
                self.strings["push_fail"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main, "style": "danger"}]],
            )

    async def _try_import_from_reply(self, message: Message) -> Optional[dict]:
        reply = await message.get_reply_message()
        if not reply:
            return None

        raw_text = None

        if reply.media and hasattr(reply.media, "document"):
            try:
                data = await self._client.download_media(reply, bytes)
                raw_text = data.decode()
            except Exception:
                return None
        elif reply.text:
            raw_text = reply.text

        if not raw_text:
            return None

        try:
            parsed = json.loads(raw_text)
            if "ids" in parsed and isinstance(parsed["ids"], list):
                return parsed
        except Exception:
            pass

        return None

    async def _cb_direct_start(self, call: InlineCall):
        await call.edit(
            self.strings["direct_input_id"],
            reply_markup=[[
                {
                    "text": self.strings["input_user_id"],
                    "input": self.strings["input_user_id"],
                    "handler": self._cb_direct_got_id,
                    "style": "primary",
                }
            ], [
                {"text": self.strings["btn_back"], "callback": self._cb_main, "style": "danger"}
            ]],
        )

    async def _cb_direct_got_id(self, call: InlineCall, user_id: str):
        user_id = user_id.strip()
        try:
            int(user_id)
        except ValueError:
            await call.edit(
                self.strings["direct_input_id"],
                reply_markup=[[
                    {
                        "text": self.strings["input_user_id"],
                        "input": self.strings["input_user_id"],
                        "handler": self._cb_direct_got_id,
                        "style": "primary",
                    }
                ], [
                    {"text": self.strings["btn_back"], "callback": self._cb_main, "style": "danger"}
                ]],
            )
            return

        state = {"user_id": user_id}
        self._set_state(call, state)
        await self._show_direct_reason(call)

    async def _show_direct_reason(self, call: InlineCall):
        state = self._get_state(call)
        user_id = state.get("user_id", "?")
        await call.edit(
            self.strings["direct_select_reason"].format(user_id=_esc(user_id)),
            reply_markup=[
                [
                    {"text": self.strings["reason_savemode"], "callback": self._cb_direct_reason, "args": ("savemode",), "style": "primary"},
                    {"text": self.strings["reason_extrascam"], "callback": self._cb_direct_reason, "args": ("extrascam",), "style": "primary"},
                ],
                [
                    {"text": self.strings["reason_ordinaryscam"], "callback": self._cb_direct_reason, "args": ("ordinaryscam",), "style": "primary"},
                ],
                [{"text": self.strings["btn_back"], "callback": self._cb_direct_start, "style": "danger"}],
            ],
        )

    async def _cb_direct_reason(self, call: InlineCall, reason_key: str):
        state = self._get_state(call)
        state["reason"] = reason_key
        self._set_state(call, state)

        await call.edit(
            self.strings["direct_confirm"].format(
                user_id=_esc(state["user_id"]),
                reason=self._reason_label(reason_key),
            ),
            reply_markup=[
                [
                    {"text": self.strings["btn_push"], "callback": self._cb_direct_push, "style": "success"},
                    {"text": self.strings["btn_cancel"], "callback": self._cb_main, "style": "danger"},
                ]
            ],
        )

    async def _cb_direct_push(self, call: InlineCall):
        token = self._get_token()
        repo = self._get_repo()
        if not token or not repo:
            await call.edit(
                self.strings["no_settings"],
                reply_markup=[[{"text": self.strings["btn_settings"], "callback": self._cb_settings, "style": "primary"}]],
            )
            return

        state = self._get_state(call)
        user_id = state.get("user_id")
        reason_key = state.get("reason", "savemode")
        reason_config = self._reason_config_key(reason_key)

        await call.edit(self.strings["push_running"], reply_markup=[])

        db_data, sha = await _github_get_file(token, repo, DB_PATH)
        if db_data is None:
            db_data = {}

        key = str(user_id)
        if key in db_data:
            status = self.strings["status_skipped"]
        else:
            db_data[key] = {"reason": reason_config}
            ok = await _github_push_file(
                token, repo, DB_PATH, db_data, sha,
                f"RestrictAPI: add {key} reason={reason_config}",
            )
            if not ok:
                await call.edit(
                    self.strings["push_fail"],
                    reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main, "style": "danger"}]],
                )
                return
            status = self.strings["status_added"]

        await call.edit(
            self.strings["direct_ok"].format(
                user_id=_esc(user_id),
                status=status,
                total=len(db_data),
            ),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main, "style": "primary"}]],
        )

    async def _cb_settings(self, call: InlineCall):
        token = self._get_token()
        repo = self._get_repo()

        token_display = (token[:8] + "..." + token[-4:]) if token and len(token) > 12 else (token or "Not set")
        repo_display = repo or "Not set"

        await call.edit(
            self.strings["settings_menu"].format(
                repo=_esc(repo_display),
                token=_esc(token_display),
            ),
            reply_markup=[
                [
                    {
                        "text": self.strings["btn_set_repo"],
                        "input": self.strings["input_repo"],
                        "handler": self._cb_settings_got_repo,
                        "style": "primary",
                    },
                    {
                        "text": self.strings["btn_set_token"],
                        "input": self.strings["input_token"],
                        "handler": self._cb_settings_got_token,
                        "style": "primary",
                    },
                ],
                [{"text": self.strings["btn_back"], "callback": self._cb_main, "style": "danger"}],
            ],
        )

    async def _cb_settings_got_repo(self, call: InlineCall, repo: str):
        repo = repo.strip()
        self._set_repo(repo)
        await call.edit(
            self.strings["settings_saved"].format(field="Repository"),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_settings, "style": "primary"}]],
        )

    async def _cb_settings_got_token(self, call: InlineCall, token: str):
        token = token.strip()
        self._set_token(token)
        await call.edit(
            self.strings["settings_saved"].format(field="Token"),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_settings, "style": "primary"}]],
        )

    @loader.command(
        ru_doc="Открыть меню RestrictAPI",
        en_doc="Open RestrictAPI menu",
    )
    async def rapi(self, message: Message):
        """Open RestrictAPI menu"""
        imported = await self._try_import_from_reply(message)

        if imported:
            ids = imported.get("ids", [])
            reason_key = imported.get("reason", "savemode")
            chat_id = imported.get("chat_id", "unknown")

            self._local_cache = {
                "ids": ids,
                "reason": reason_key,
                "chat_id": chat_id,
                "unique": len(ids),
            }

            await self.inline.form(
                text=self.strings["import_push_confirm"].format(
                    unique=len(ids),
                    reason=self._reason_label(reason_key),
                ),
                message=message,
                reply_markup=[
                    [
                        {"text": self.strings["btn_push"], "callback": self._cb_cache_push, "style": "success"},
                        {"text": self.strings["btn_cancel"], "callback": self._cb_main, "style": "danger"},
                    ]
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