# CopyLeft 2026 github.com/i-execute
# Author: I_execute.t.me
# Licensed under AGPLv3.

__version__ = (3, 0, 0)
# meta developer: Execute_forge.t.me

import json
import logging
import os
import asyncio
import tempfile
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse

import aiohttp

from .. import loader, utils
from ..inline.types import InlineCall

logger = logging.getLogger(__name__)

ITEMS_PER_PAGE = 5


def _escape(text) -> str:
    if not text:
        return ""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _mask(key: str) -> str:
    if len(key) > 12:
        return key[:8] + "..." + key[-4:]
    return key


def _domain_from_url(url: str) -> str:
    try:
        host = urlparse(url).hostname or url
        parts = host.split(".")
        if len(parts) >= 2:
            return parts[-2]
        return host
    except Exception:
        return url


def _now_str(tz_offset: int) -> str:
    tz = timezone(timedelta(hours=tz_offset))
    now = datetime.now(tz)
    sign = "+" if tz_offset >= 0 else "-"
    return f"{now.strftime('%Y-%m-%d %H:%M:%S')} UTC{sign}{abs(tz_offset)}"


async def _api_get(url: str, api_key: str, timeout: int) -> dict:
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                url,
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=timeout),
            ) as r:
                data = await r.json(content_type=None)
                return {"ok": r.status < 400, "status": r.status, "data": data}
    except Exception as e:
        return {"ok": False, "status": 0, "data": {}, "error": str(e)}


async def _api_post(url: str, api_key: str, body: dict, timeout: int) -> dict:
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(
                url,
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                json=body,
                timeout=aiohttp.ClientTimeout(total=timeout),
            ) as r:
                data = await r.json(content_type=None)
                return {"ok": r.status < 400, "status": r.status, "data": data}
    except Exception as e:
        return {"ok": False, "status": 0, "data": {}, "error": str(e)}


async def _fetch_models(base_url: str, api_key: str, timeout: int) -> tuple[bool, list, str]:
    r = await _api_get(f"{base_url.rstrip('/')}/models", api_key, timeout)
    if not r["ok"]:
        err = r.get("error") or r["data"].get("error", {}).get("message", str(r["data"]))
        return False, [], str(err)[:200]
    data = r["data"]
    if "data" not in data:
        return False, [], str(data)[:200]
    ids = sorted(m.get("id", "") for m in data["data"] if m.get("id"))
    return True, ids, ""


async def _validate_key(base_url: str, api_key: str, model: str, timeout: int) -> dict:
    body = {
        "model": model,
        "stream": False,
        "messages": [{"role": "user", "content": "Hello"}],
    }
    r = await _api_post(f"{base_url.rstrip('/')}/chat/completions", api_key, body, timeout)
    data = r["data"]

    if "error" in data:
        code = data["error"].get("code", "")
        msg = data["error"].get("message", "")
        if code == "rate_limit_exceeded":
            return {"status": "rate-limited", "message": msg, "raw": r}
        return {"status": "invalid", "message": msg, "raw": r}

    if "choices" in data:
        return {"status": "valid", "message": "", "raw": r}

    err = r.get("error", str(data)[:200])
    return {"status": "invalid", "message": str(err)[:200], "raw": r}


@loader.tds
class KeyKeeper(loader.Module):
    """OpenAI-compatible API key manager"""

    strings = {
        "name": "KeyKeeper",
        "main_menu": (
            "<b>KeyKeeper</b>\n"
            "<blockquote>Providers: {providers}\n"
            "Total keys: {keys}</blockquote>"
        ),
        "btn_providers": "Providers",
        "btn_add_provider": "Add Provider",
        "btn_validate_all": "Validate All Keys",
        "btn_export_all": "Export All",
        "btn_close": "Close",
        "btn_back": "Back",
        "btn_left": "<",
        "btn_right": ">",

        "add_url": (
            "<b>Add Provider</b>\n"
            "<blockquote>Enter base URL\n"
            "<i>e.g. https://api.openai.com/v1</i></blockquote>"
        ),
        "add_key": (
            "<b>Add Provider</b>\n"
            "<blockquote>URL: <code>{url}</code>\n"
            "Enter API key:</blockquote>"
        ),
        "fetching_models": (
            "<b>Fetching models...</b>\n"
            "<blockquote>URL: <code>{url}</code></blockquote>"
        ),
        "select_model": (
            "<b>Select Test Model</b>\n"
            "<blockquote>Provider: <b>{name}</b>\n"
            "Page {page}/{total_pages}</blockquote>"
        ),
        "provider_saved": (
            "<b>Provider Added</b>\n"
            "<blockquote>Name: <b>{name}</b>\n"
            "URL: <code>{url}</code>\n"
            "Model: <code>{model}</code></blockquote>"
        ),
        "fetch_models_error": (
            "<b>Failed to fetch models</b>\n"
            "<blockquote>{error}</blockquote>"
        ),

        "providers_list": (
            "<b>Providers</b>\n"
            "<blockquote>Page {page}/{total_pages}\n"
            "Total: {total}</blockquote>"
        ),
        "provider_menu": (
            "<b>{name}</b>\n"
            "<blockquote>URL: <code>{url}</code>\n"
            "Model: <code>{model}</code>\n"
            "Keys: {keys}</blockquote>"
        ),
        "btn_add_key": "Add Key",
        "btn_list_keys": "Keys",
        "btn_validate_provider": "Validate All",
        "btn_export_provider": "Export",
        "btn_change_model": "Change Model",
        "btn_delete_provider": "Delete Provider",

        "add_key_input": (
            "<b>Add Key</b>\n"
            "<blockquote>Provider: <b>{name}</b>\n"
            "Enter API key:</blockquote>"
        ),
        "validating": "<b>Validating...</b>",
        "key_valid": (
            "<b>Key Valid</b>\n"
            "<blockquote>Status: {status}\n"
            "Save with optional comment or skip:</blockquote>"
        ),
        "key_invalid": (
            "<b>Key Invalid</b>\n"
            "<blockquote>Reason: {reason}</blockquote>"
        ),
        "key_duplicate": (
            "<b>Duplicate</b>\n"
            "<blockquote>This key already exists</blockquote>"
        ),
        "btn_save_key": "Save",
        "btn_save_comment": "Save with Comment",
        "input_key": "API key:",
        "input_comment": "Comment:",
        "input_new_model": "New test model:",

        "keys_list": (
            "<b>{name} - Keys</b>\n"
            "<blockquote>Page {page}/{total_pages}\n"
            "Total: {total}</blockquote>"
        ),
        "no_keys": (
            "<b>No Keys</b>\n"
            "<blockquote>Add keys first</blockquote>"
        ),
        "key_detail": (
            "<b>Key #{num} - {name}</b>\n"
            "<blockquote>Masked: <code>{masked}</code>\n"
            "Status: {status}\n"
            "Comment: {comment}\n"
            "Checked: {date}</blockquote>"
        ),
        "btn_show_key": "Show Key",
        "btn_check_key": "Check",
        "btn_models_key": "Models",
        "btn_delete_key": "Delete",
        "key_full": (
            "<b>Key #{num}</b>\n"
            "<blockquote><code>{value}</code></blockquote>"
        ),
        "key_deleted": (
            "<b>Key #{num} deleted</b>"
        ),
        "key_saved": (
            "<b>Key Saved</b>\n"
            "<blockquote>Provider: <b>{name}</b>\n"
            "Key #: {num}</blockquote>"
        ),

        "validate_provider_running": "<b>Validating {name}...</b>",
        "validate_provider_done": (
            "<b>Validation Done - {name}</b>\n"
            "<blockquote>Total: {total}\n"
            "Valid: {valid}\n"
            "Rate limited: {rate_limited}\n"
            "Invalid: {invalid}</blockquote>"
        ),
        "validate_all_running": "<b>Validating all keys...</b>",
        "validate_all_done": (
            "<b>Validation Done - All</b>\n"
            "<blockquote>Total: {total}\n"
            "Valid: {valid}\n"
            "Rate limited: {rate_limited}\n"
            "Invalid: {invalid}\n"
            "Results sent as JSON</blockquote>"
        ),
        "models_sent": (
            "<b>Models</b>\n"
            "<blockquote>Key #{num}\n"
            "Total: {total}\n"
            "Sent to chat</blockquote>"
        ),
        "models_error": (
            "<b>Models Error</b>\n"
            "<blockquote>{error}</blockquote>"
        ),
        "export_sent": (
            "<b>Export Sent</b>\n"
            "<blockquote>{count} keys</blockquote>"
        ),
        "provider_deleted": "<b>Provider {name} deleted</b>",
        "model_updated": (
            "<b>Model Updated</b>\n"
            "<blockquote>Provider: <b>{name}</b>\n"
            "Model: <code>{model}</code></blockquote>"
        ),
        "change_model_menu": (
            "<b>Change Model - {name}</b>\n"
            "<blockquote>Current: <code>{model}</code>\n"
            "Page {page}/{total_pages}</blockquote>"
        ),

        "status_valid": "Valid",
        "status_rate_limited": "Rate Limited",
        "status_invalid": "Invalid",
        "status_unknown": "Not checked",
        "no_comment": "-",
    }

    strings_ru = {
        "main_menu": (
            "<b>KeyKeeper</b>\n"
            "<blockquote>Провайдеры: {providers}\n"
            "Ключей всего: {keys}</blockquote>"
        ),
        "btn_providers": "Провайдеры",
        "btn_add_provider": "Добавить провайдера",
        "btn_validate_all": "Проверить все ключи",
        "btn_export_all": "Экспорт всего",
        "btn_close": "Закрыть",
        "btn_back": "Назад",
        "btn_left": "<",
        "btn_right": ">",

        "add_url": (
            "<b>Добавить провайдера</b>\n"
            "<blockquote>Введите base URL\n"
            "<i>например https://api.openai.com/v1</i></blockquote>"
        ),
        "add_key": (
            "<b>Добавить провайдера</b>\n"
            "<blockquote>URL: <code>{url}</code>\n"
            "Введите API ключ:</blockquote>"
        ),
        "fetching_models": (
            "<b>Загружаем модели...</b>\n"
            "<blockquote>URL: <code>{url}</code></blockquote>"
        ),
        "select_model": (
            "<b>Выбери тестовую модель</b>\n"
            "<blockquote>Провайдер: <b>{name}</b>\n"
            "Страница {page}/{total_pages}</blockquote>"
        ),
        "provider_saved": (
            "<b>Провайдер добавлен</b>\n"
            "<blockquote>Имя: <b>{name}</b>\n"
            "URL: <code>{url}</code>\n"
            "Модель: <code>{model}</code></blockquote>"
        ),
        "fetch_models_error": (
            "<b>Не удалось получить модели</b>\n"
            "<blockquote>{error}</blockquote>"
        ),

        "providers_list": (
            "<b>Провайдеры</b>\n"
            "<blockquote>Страница {page}/{total_pages}\n"
            "Всего: {total}</blockquote>"
        ),
        "provider_menu": (
            "<b>{name}</b>\n"
            "<blockquote>URL: <code>{url}</code>\n"
            "Модель: <code>{model}</code>\n"
            "Ключей: {keys}</blockquote>"
        ),
        "btn_add_key": "Добавить ключ",
        "btn_list_keys": "Ключи",
        "btn_validate_provider": "Проверить все",
        "btn_export_provider": "Экспорт",
        "btn_change_model": "Сменить модель",
        "btn_delete_provider": "Удалить провайдера",

        "add_key_input": (
            "<b>Добавить ключ</b>\n"
            "<blockquote>Провайдер: <b>{name}</b>\n"
            "Введите API ключ:</blockquote>"
        ),
        "validating": "<b>Проверяем...</b>",
        "key_valid": (
            "<b>Ключ валиден</b>\n"
            "<blockquote>Статус: {status}\n"
            "Сохрани с комментарием или без:</blockquote>"
        ),
        "key_invalid": (
            "<b>Ключ невалиден</b>\n"
            "<blockquote>Причина: {reason}</blockquote>"
        ),
        "key_duplicate": (
            "<b>Дубликат</b>\n"
            "<blockquote>Такой ключ уже есть</blockquote>"
        ),
        "btn_save_key": "Сохранить",
        "btn_save_comment": "Сохранить с комментарием",
        "input_key": "API ключ:",
        "input_comment": "Комментарий:",
        "input_new_model": "Новая тестовая модель:",

        "keys_list": (
            "<b>{name} - Ключи</b>\n"
            "<blockquote>Страница {page}/{total_pages}\n"
            "Всего: {total}</blockquote>"
        ),
        "no_keys": (
            "<b>Нет ключей</b>\n"
            "<blockquote>Сначала добавьте ключи</blockquote>"
        ),
        "key_detail": (
            "<b>Ключ #{num} - {name}</b>\n"
            "<blockquote>Маска: <code>{masked}</code>\n"
            "Статус: {status}\n"
            "Комментарий: {comment}\n"
            "Проверен: {date}</blockquote>"
        ),
        "btn_show_key": "Показать ключ",
        "btn_check_key": "Проверить",
        "btn_models_key": "Модели",
        "btn_delete_key": "Удалить",
        "key_full": (
            "<b>Ключ #{num}</b>\n"
            "<blockquote><code>{value}</code></blockquote>"
        ),
        "key_deleted": "<b>Ключ #{num} удалён</b>",
        "key_saved": (
            "<b>Ключ сохранён</b>\n"
            "<blockquote>Провайдер: <b>{name}</b>\n"
            "Номер: {num}</blockquote>"
        ),

        "validate_provider_running": "<b>Проверяем {name}...</b>",
        "validate_provider_done": (
            "<b>Проверка завершена - {name}</b>\n"
            "<blockquote>Всего: {total}\n"
            "Валидных: {valid}\n"
            "Лимит: {rate_limited}\n"
            "Невалидных: {invalid}</blockquote>"
        ),
        "validate_all_running": "<b>Проверяем все ключи...</b>",
        "validate_all_done": (
            "<b>Проверка завершена - все</b>\n"
            "<blockquote>Всего: {total}\n"
            "Валидных: {valid}\n"
            "Лимит: {rate_limited}\n"
            "Невалидных: {invalid}\n"
            "Результаты отправлены в JSON</blockquote>"
        ),
        "models_sent": (
            "<b>Модели</b>\n"
            "<blockquote>Ключ #{num}\n"
            "Всего: {total}\n"
            "Отправлено в чат</blockquote>"
        ),
        "models_error": (
            "<b>Ошибка моделей</b>\n"
            "<blockquote>{error}</blockquote>"
        ),
        "export_sent": (
            "<b>Экспорт отправлен</b>\n"
            "<blockquote>{count} ключей</blockquote>"
        ),
        "provider_deleted": "<b>Провайдер {name} удалён</b>",
        "model_updated": (
            "<b>Модель обновлена</b>\n"
            "<blockquote>Провайдер: <b>{name}</b>\n"
            "Модель: <code>{model}</code></blockquote>"
        ),
        "change_model_menu": (
            "<b>Сменить модель - {name}</b>\n"
            "<blockquote>Текущая: <code>{model}</code>\n"
            "Страница {page}/{total_pages}</blockquote>"
        ),

        "status_valid": "Валиден",
        "status_rate_limited": "Лимит",
        "status_invalid": "Невалиден",
        "status_unknown": "Не проверен",
        "no_comment": "-",
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
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
        self._providers: list = []
        self._pending: dict = {}

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        self._providers = self.get("providers", [])

    def _save(self):
        self.set("providers", self._providers)

    def _find_provider(self, pid: int) -> dict | None:
        for p in self._providers:
            if p.get("id") == pid:
                return p
        return None

    def _status_label(self, status: str) -> str:
        return {
            "valid": self.strings["status_valid"],
            "rate-limited": self.strings["status_rate_limited"],
            "invalid": self.strings["status_invalid"],
        }.get(status, self.strings["status_unknown"])

    def _total_keys(self) -> int:
        return sum(len(p.get("keys", [])) for p in self._providers)

    def _next_key_num(self, provider: dict) -> int:
        existing = {k.get("num") for k in provider.get("keys", [])}
        n = 1
        while n in existing:
            n += 1
        return n

    async def _send_json(self, chat_id, data: dict | list, filename: str):
        tmp = tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        )
        json.dump(data, tmp, indent=2, ensure_ascii=False)
        tmp.close()
        try:
            await self._client.send_file(
                chat_id,
                tmp.name,
                force_document=True,
                file_name=filename,
            )
        finally:
            os.unlink(tmp.name)

    def _fmt_main(self) -> str:
        return self.strings["main_menu"].format(
            providers=len(self._providers),
            keys=self._total_keys(),
        )

    def _markup_main(self) -> list:
        return [
            [
                {"text": self.strings["btn_providers"], "callback": self._cb_providers_list, "args": (0,), "style": "primary"},
                {"text": self.strings["btn_add_provider"], "callback": self._cb_add_provider_start, "style": "primary"},
            ],
            [
                {"text": self.strings["btn_validate_all"], "callback": self._cb_validate_all, "style": "success"},
                {"text": self.strings["btn_export_all"], "callback": self._cb_export_all, "style": "primary"},
            ],
            [{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}],
        ]

    async def _cb_close(self, call: InlineCall):
        await call.delete()

    async def _cb_main(self, call: InlineCall):
        await call.edit(self._fmt_main(), reply_markup=self._markup_main())

    async def _cb_add_provider_start(self, call: InlineCall):
        await call.edit(
            self.strings["add_url"],
            reply_markup=[
                [{"text": self.strings["input_key"], "input": self.strings["add_url"], "handler": self._ih_add_url, "style": "primary"}],
                [{"text": self.strings["btn_back"], "callback": self._cb_main, "style": "danger"}],
            ],
        )

    async def _ih_add_url(self, call: InlineCall, url: str):
        url = url.strip().rstrip("/")
        await call.edit(
            self.strings["add_key"].format(url=_escape(url)),
            reply_markup=[
                [{"text": self.strings["input_key"], "input": self.strings["input_key"], "handler": self._ih_add_api_key, "args": (url,), "style": "primary"}],
                [{"text": self.strings["btn_back"], "callback": self._cb_add_provider_start, "style": "danger"}],
            ],
        )

    async def _ih_add_api_key(self, call: InlineCall, api_key: str, url: str):
        api_key = api_key.strip()
        await call.edit(self.strings["fetching_models"].format(url=_escape(url)), reply_markup=[])
        ok, models, err = await _fetch_models(url, api_key, self.config["timeout"])
        if not ok:
            await call.edit(
                self.strings["fetch_models_error"].format(error=_escape(err)),
                reply_markup=[
                    [{"text": self.strings["btn_back"], "callback": self._cb_add_provider_start, "style": "danger"}],
                ],
            )
            return
        name = _domain_from_url(url)
        pending_id = utils.rand(12)
        self._pending[pending_id] = {"url": url, "api_key": api_key, "name": name, "models": models}
        await self._show_model_select(call, pending_id, 0, mode="add_provider")

    async def _show_model_select(self, call: InlineCall, pending_id: str, page: int, mode: str):
        pending = self._pending.get(pending_id)
        if not pending:
            await call.answer()
            return
        models = pending["models"]
        name = pending["name"]
        total = len(models)
        total_pages = max(1, (total + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE)
        page = max(0, min(page, total_pages - 1))
        start = page * ITEMS_PER_PAGE
        chunk = models[start:start + ITEMS_PER_PAGE]

        if mode == "add_provider":
            text = self.strings["select_model"].format(name=_escape(name), page=page + 1, total_pages=total_pages)
            handler = self._cb_model_selected_add
        else:
            pid = pending.get("pid")
            provider = self._find_provider(pid)
            text = self.strings["change_model_menu"].format(
                name=_escape(provider["name"] if provider else name),
                model=_escape(provider["model"] if provider else ""),
                page=page + 1,
                total_pages=total_pages,
            )
            handler = self._cb_model_selected_change

        rows = []
        for m in chunk:
            rows.append([{"text": m, "callback": handler, "args": (pending_id, m), "style": "primary"}])

        nav_row = []
        if page > 0:
            nav_row.append({"text": self.strings["btn_left"], "callback": self._cb_model_page, "args": (pending_id, page - 1, mode), "style": "primary"})
        if page < total_pages - 1:
            nav_row.append({"text": self.strings["btn_right"], "callback": self._cb_model_page, "args": (pending_id, page + 1, mode), "style": "primary"})
        if nav_row:
            rows.append(nav_row)

        rows.append([{"text": self.strings["btn_back"], "callback": self._cb_main, "style": "danger"}])
        await call.edit(text, reply_markup=rows)

    async def _cb_model_page(self, call: InlineCall, pending_id: str, page: int, mode: str):
        await self._show_model_select(call, pending_id, page, mode)

    async def _cb_model_selected_add(self, call: InlineCall, pending_id: str, model: str):
        pending = self._pending.pop(pending_id, None)
        if not pending:
            await call.answer()
            return
        pid = (max((p.get("id", 0) for p in self._providers), default=0)) + 1
        self._providers.append({
            "id": pid,
            "name": pending["name"],
            "url": pending["url"],
            "api_key": pending["api_key"],
            "model": model,
            "keys": [],
        })
        self._save()
        await call.edit(
            self.strings["provider_saved"].format(
                name=_escape(pending["name"]),
                url=_escape(pending["url"]),
                model=_escape(model),
            ),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main, "style": "danger"}]],
        )

    async def _cb_model_selected_change(self, call: InlineCall, pending_id: str, model: str):
        pending = self._pending.pop(pending_id, None)
        if not pending:
            await call.answer()
            return
        pid = pending.get("pid")
        provider = self._find_provider(pid)
        if provider:
            provider["model"] = model
            self._save()
        await call.edit(
            self.strings["model_updated"].format(
                name=_escape(provider["name"] if provider else ""),
                model=_escape(model),
            ),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_provider_menu, "args": (pid,), "style": "danger"}]],
        )

    async def _cb_providers_list(self, call: InlineCall, page: int):
        total = len(self._providers)
        if total == 0:
            await call.edit(
                self._fmt_main(),
                reply_markup=self._markup_main(),
            )
            return
        total_pages = max(1, (total + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE)
        page = max(0, min(page, total_pages - 1))
        start = page * ITEMS_PER_PAGE
        chunk = self._providers[start:start + ITEMS_PER_PAGE]

        rows = []
        for p in chunk:
            key_count = len(p.get("keys", []))
            rows.append([{
                "text": f"{p['name']} ({key_count})",
                "callback": self._cb_provider_menu,
                "args": (p["id"],),
                "style": "primary",
            }])

        nav_row = []
        if page > 0:
            nav_row.append({"text": self.strings["btn_left"], "callback": self._cb_providers_list, "args": (page - 1,), "style": "primary"})
        if page < total_pages - 1:
            nav_row.append({"text": self.strings["btn_right"], "callback": self._cb_providers_list, "args": (page + 1,), "style": "primary"})
        if nav_row:
            rows.append(nav_row)

        rows.append([{"text": self.strings["btn_back"], "callback": self._cb_main, "style": "danger"}])

        await call.edit(
            self.strings["providers_list"].format(page=page + 1, total_pages=total_pages, total=total),
            reply_markup=rows,
        )

    async def _cb_provider_menu(self, call: InlineCall, pid: int):
        provider = self._find_provider(pid)
        if not provider:
            await call.answer()
            return
        await call.edit(
            self.strings["provider_menu"].format(
                name=_escape(provider["name"]),
                url=_escape(provider["url"]),
                model=_escape(provider["model"]),
                keys=len(provider.get("keys", [])),
            ),
            reply_markup=[
                [
                    {"text": self.strings["btn_add_key"], "callback": self._cb_add_key_start, "args": (pid,), "style": "primary"},
                    {"text": self.strings["btn_list_keys"], "callback": self._cb_keys_list, "args": (pid, 0), "style": "primary"},
                ],
                [
                    {"text": self.strings["btn_validate_provider"], "callback": self._cb_validate_provider, "args": (pid,), "style": "success"},
                    {"text": self.strings["btn_export_provider"], "callback": self._cb_export_provider, "args": (pid,), "style": "primary"},
                ],
                [
                    {"text": self.strings["btn_change_model"], "callback": self._cb_change_model_start, "args": (pid,), "style": "primary"},
                    {"text": self.strings["btn_delete_provider"], "callback": self._cb_delete_provider, "args": (pid,), "style": "danger"},
                ],
                [{"text": self.strings["btn_back"], "callback": self._cb_providers_list, "args": (0,), "style": "danger"}],
            ],
        )

    async def _cb_change_model_start(self, call: InlineCall, pid: int):
        provider = self._find_provider(pid)
        if not provider:
            await call.answer()
            return
        await call.edit(self.strings["fetching_models"].format(url=_escape(provider["url"])), reply_markup=[])
        ok, models, err = await _fetch_models(provider["url"], provider["api_key"], self.config["timeout"])
        if not ok:
            await call.edit(
                self.strings["fetch_models_error"].format(error=_escape(err)),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_provider_menu, "args": (pid,), "style": "danger"}]],
            )
            return
        pending_id = utils.rand(12)
        self._pending[pending_id] = {"pid": pid, "name": provider["name"], "models": models}
        await self._show_model_select(call, pending_id, 0, mode="change_model")

    async def _cb_delete_provider(self, call: InlineCall, pid: int):
        provider = self._find_provider(pid)
        name = provider["name"] if provider else str(pid)
        self._providers = [p for p in self._providers if p.get("id") != pid]
        self._save()
        await call.edit(
            self.strings["provider_deleted"].format(name=_escape(name)),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main, "style": "danger"}]],
        )

    async def _cb_add_key_start(self, call: InlineCall, pid: int):
        provider = self._find_provider(pid)
        if not provider:
            await call.answer()
            return
        await call.edit(
            self.strings["add_key_input"].format(name=_escape(provider["name"])),
            reply_markup=[
                [{"text": self.strings["input_key"], "input": self.strings["input_key"], "handler": self._ih_validate_new_key, "args": (pid,), "style": "primary"}],
                [{"text": self.strings["btn_back"], "callback": self._cb_provider_menu, "args": (pid,), "style": "danger"}],
            ],
        )

    async def _ih_validate_new_key(self, call: InlineCall, key: str, pid: int):
        key = key.strip()
        provider = self._find_provider(pid)
        if not provider:
            await call.answer()
            return

        all_values = [k.get("value") for k in provider.get("keys", [])]
        if key in all_values:
            await call.edit(
                self.strings["key_duplicate"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_provider_menu, "args": (pid,), "style": "danger"}]],
            )
            return

        await call.edit(self.strings["validating"], reply_markup=[])
        result = await _validate_key(provider["url"], key, provider["model"], self.config["timeout"])

        if result["status"] in ("valid", "rate-limited"):
            await call.edit(
                self.strings["key_valid"].format(status=self._status_label(result["status"])),
                reply_markup=[
                    [{"text": self.strings["btn_save_key"], "callback": self._cb_save_key, "args": (pid, key, result["status"], ""), "style": "success"}],
                    [{"text": self.strings["btn_save_comment"], "input": self.strings["input_comment"], "handler": self._ih_save_key_comment, "args": (pid, key, result["status"]), "style": "primary"}],
                    [{"text": self.strings["btn_back"], "callback": self._cb_provider_menu, "args": (pid,), "style": "danger"}],
                ],
            )
        else:
            await call.edit(
                self.strings["key_invalid"].format(reason=_escape(result["message"])),
                reply_markup=[
                    [{"text": self.strings["btn_back"], "callback": self._cb_provider_menu, "args": (pid,), "style": "danger"}],
                ],
            )

    async def _cb_save_key(self, call: InlineCall, pid: int, key: str, status: str, comment: str):
        provider = self._find_provider(pid)
        if not provider:
            await call.answer()
            return
        num = self._next_key_num(provider)
        provider.setdefault("keys", []).append({
            "num": num,
            "value": key,
            "status": status,
            "comment": comment,
            "date": _now_str(self.config["timezone"]),
        })
        self._save()
        await call.edit(
            self.strings["key_saved"].format(name=_escape(provider["name"]), num=num),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_provider_menu, "args": (pid,), "style": "danger"}]],
        )

    async def _ih_save_key_comment(self, call: InlineCall, comment: str, pid: int, key: str, status: str):
        await self._cb_save_key(call, pid, key, status, comment.strip())

    async def _cb_keys_list(self, call: InlineCall, pid: int, page: int):
        provider = self._find_provider(pid)
        if not provider:
            await call.answer()
            return
        keys = provider.get("keys", [])
        if not keys:
            await call.edit(
                self.strings["no_keys"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_provider_menu, "args": (pid,), "style": "danger"}]],
            )
            return
        total = len(keys)
        total_pages = max(1, (total + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE)
        page = max(0, min(page, total_pages - 1))
        start = page * ITEMS_PER_PAGE
        chunk = keys[start:start + ITEMS_PER_PAGE]

        rows = []
        for k in chunk:
            num = k.get("num")
            masked = _mask(k.get("value", ""))
            status = self._status_label(k.get("status", ""))
            rows.append([{
                "text": f"#{num} {masked} [{status}]",
                "callback": self._cb_key_detail,
                "args": (pid, num),
                "style": "primary",
            }])

        nav_row = []
        if page > 0:
            nav_row.append({"text": self.strings["btn_left"], "callback": self._cb_keys_list, "args": (pid, page - 1), "style": "primary"})
        if page < total_pages - 1:
            nav_row.append({"text": self.strings["btn_right"], "callback": self._cb_keys_list, "args": (pid, page + 1), "style": "primary"})
        if nav_row:
            rows.append(nav_row)

        rows.append([{"text": self.strings["btn_back"], "callback": self._cb_provider_menu, "args": (pid,), "style": "danger"}])

        await call.edit(
            self.strings["keys_list"].format(
                name=_escape(provider["name"]),
                page=page + 1,
                total_pages=total_pages,
                total=total,
            ),
            reply_markup=rows,
        )

    async def _cb_key_detail(self, call: InlineCall, pid: int, num: int):
        provider = self._find_provider(pid)
        if not provider:
            await call.answer()
            return
        key_entry = next((k for k in provider.get("keys", []) if k.get("num") == num), None)
        if not key_entry:
            await call.answer()
            return
        comment = key_entry.get("comment") or self.strings["no_comment"]
        await call.edit(
            self.strings["key_detail"].format(
                num=num,
                name=_escape(provider["name"]),
                masked=_escape(_mask(key_entry.get("value", ""))),
                status=self._status_label(key_entry.get("status", "")),
                comment=_escape(comment),
                date=key_entry.get("date", "-"),
            ),
            reply_markup=[
                [
                    {"text": self.strings["btn_show_key"], "callback": self._cb_show_key, "args": (pid, num), "style": "primary"},
                    {"text": self.strings["btn_check_key"], "callback": self._cb_check_key, "args": (pid, num), "style": "success"},
                ],
                [
                    {"text": self.strings["btn_models_key"], "callback": self._cb_models_key, "args": (pid, num), "style": "primary"},
                    {"text": self.strings["btn_delete_key"], "callback": self._cb_delete_key, "args": (pid, num), "style": "danger"},
                ],
                [{"text": self.strings["btn_back"], "callback": self._cb_keys_list, "args": (pid, 0), "style": "danger"}],
            ],
        )

    async def _cb_show_key(self, call: InlineCall, pid: int, num: int):
        provider = self._find_provider(pid)
        if not provider:
            await call.answer()
            return
        key_entry = next((k for k in provider.get("keys", []) if k.get("num") == num), None)
        if not key_entry:
            await call.answer()
            return
        await call.edit(
            self.strings["key_full"].format(num=num, value=_escape(key_entry.get("value", ""))),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_key_detail, "args": (pid, num), "style": "danger"}]],
        )

    async def _cb_check_key(self, call: InlineCall, pid: int, num: int):
        provider = self._find_provider(pid)
        if not provider:
            await call.answer()
            return
        key_entry = next((k for k in provider.get("keys", []) if k.get("num") == num), None)
        if not key_entry:
            await call.answer()
            return
        await call.edit(self.strings["validating"], reply_markup=[])
        result = await _validate_key(provider["url"], key_entry["value"], provider["model"], self.config["timeout"])
        key_entry["status"] = result["status"]
        key_entry["date"] = _now_str(self.config["timezone"])
        self._save()
        await self._cb_key_detail(call, pid, num)

    async def _cb_models_key(self, call: InlineCall, pid: int, num: int):
        provider = self._find_provider(pid)
        if not provider:
            await call.answer()
            return
        key_entry = next((k for k in provider.get("keys", []) if k.get("num") == num), None)
        if not key_entry:
            await call.answer()
            return
        await call.edit(self.strings["fetching_models"].format(url=_escape(provider["url"])), reply_markup=[])
        ok, models, err = await _fetch_models(provider["url"], key_entry["value"], self.config["timeout"])
        if not ok:
            await call.edit(
                self.strings["models_error"].format(error=_escape(err)),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_key_detail, "args": (pid, num), "style": "danger"}]],
            )
            return
        export = {
            "provider": provider["name"],
            "base_url": provider["url"],
            "key_num": num,
            "masked": _mask(key_entry.get("value", "")),
            "total": len(models),
            "models": models,
        }
        await self._send_json(call.form["chat"], export, f"models_{provider['name']}_key{num}.json")
        await call.edit(
            self.strings["models_sent"].format(num=num, total=len(models)),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_key_detail, "args": (pid, num), "style": "danger"}]],
        )

    async def _cb_delete_key(self, call: InlineCall, pid: int, num: int):
        provider = self._find_provider(pid)
        if not provider:
            await call.answer()
            return
        provider["keys"] = [k for k in provider.get("keys", []) if k.get("num") != num]
        self._save()
        await call.edit(
            self.strings["key_deleted"].format(num=num),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_keys_list, "args": (pid, 0), "style": "danger"}]],
        )

    async def _cb_validate_provider(self, call: InlineCall, pid: int):
        provider = self._find_provider(pid)
        if not provider:
            await call.answer()
            return
        keys = provider.get("keys", [])
        if not keys:
            await call.edit(
                self.strings["no_keys"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_provider_menu, "args": (pid,), "style": "danger"}]],
            )
            return
        await call.edit(self.strings["validate_provider_running"].format(name=_escape(provider["name"])), reply_markup=[])
        tasks = [_validate_key(provider["url"], k["value"], provider["model"], self.config["timeout"]) for k in keys]
        results = await asyncio.gather(*tasks)
        valid = rate_limited = invalid = 0
        now = _now_str(self.config["timezone"])
        for k, r in zip(keys, results):
            k["status"] = r["status"]
            k["date"] = now
            if r["status"] == "valid":
                valid += 1
            elif r["status"] == "rate-limited":
                rate_limited += 1
            else:
                invalid += 1
        self._save()
        await call.edit(
            self.strings["validate_provider_done"].format(
                name=_escape(provider["name"]),
                total=len(keys),
                valid=valid,
                rate_limited=rate_limited,
                invalid=invalid,
            ),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_provider_menu, "args": (pid,), "style": "danger"}]],
        )

    async def _cb_validate_all(self, call: InlineCall):
        await call.edit(self.strings["validate_all_running"], reply_markup=[])
        all_tasks = []
        key_refs = []
        for p in self._providers:
            for k in p.get("keys", []):
                all_tasks.append(_validate_key(p["url"], k["value"], p["model"], self.config["timeout"]))
                key_refs.append((p, k))
        if not all_tasks:
            await call.edit(self._fmt_main(), reply_markup=self._markup_main())
            return
        results = await asyncio.gather(*all_tasks)
        valid = rate_limited = invalid = 0
        now = _now_str(self.config["timezone"])
        export_rows = []
        for (p, k), r in zip(key_refs, results):
            k["status"] = r["status"]
            k["date"] = now
            if r["status"] == "valid":
                valid += 1
            elif r["status"] == "rate-limited":
                rate_limited += 1
            else:
                invalid += 1
            export_rows.append({
                "provider": p["name"],
                "base_url": p["url"],
                "key_num": k["num"],
                "masked": _mask(k.get("value", "")),
                "status": r["status"],
                "date": now,
            })
        self._save()
        await self._send_json(call.form["chat"], export_rows, "validate_all_results.json")
        await call.edit(
            self.strings["validate_all_done"].format(
                total=len(all_tasks),
                valid=valid,
                rate_limited=rate_limited,
                invalid=invalid,
            ),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main, "style": "danger"}]],
        )

    async def _cb_export_provider(self, call: InlineCall, pid: int):
        provider = self._find_provider(pid)
        if not provider:
            await call.answer()
            return
        keys = provider.get("keys", [])
        export = [
            {
                "num": k.get("num"),
                "value": k.get("value"),
                "base_url": provider["url"],
                "provider": provider["name"],
                "status": k.get("status", "unknown"),
                "comment": k.get("comment", ""),
                "date": k.get("date", "-"),
            }
            for k in keys
        ]
        await self._send_json(call.form["chat"], export, f"keys_{provider['name']}.json")
        await call.edit(
            self.strings["export_sent"].format(count=len(export)),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_provider_menu, "args": (pid,), "style": "danger"}]],
        )

    async def _cb_export_all(self, call: InlineCall):
        export = []
        for p in self._providers:
            for k in p.get("keys", []):
                export.append({
                    "num": k.get("num"),
                    "value": k.get("value"),
                    "base_url": p["url"],
                    "provider": p["name"],
                    "status": k.get("status", "unknown"),
                    "comment": k.get("comment", ""),
                    "date": k.get("date", "-"),
                })
        await self._send_json(call.form["chat"], export, "keys_all.json")
        await call.edit(
            self.strings["export_sent"].format(count=len(export)),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main, "style": "danger"}]],
        )

    @loader.command(
        ru_doc="Менеджер API ключей",
        en_doc="API key manager",
    )
    async def kk(self, message):
        """API key manager"""
        await self.inline.form(
            text=self._fmt_main(),
            message=message,
            reply_markup=self._markup_main(),
            silent=True,
        )