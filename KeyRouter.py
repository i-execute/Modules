__version__ = (2, 1, 0)
# meta developer: I_execute.t.me

import logging
import json
import tempfile
import os
import asyncio
from datetime import datetime, timezone, timedelta

import aiohttp

from telethon.tl.types import Message
from .. import loader, utils
from ..inline.types import InlineCall

logger = logging.getLogger(__name__)


def _escape(text):
    if not text:
        return ""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _mask(key: str) -> str:
    if len(key) > 12:
        return key[:8] + "..." + key[-4:]
    return key


def _now(tz_offset: int) -> str:
    tz = timezone(timedelta(hours=tz_offset))
    now = datetime.now(tz)
    sign = "+" if tz_offset >= 0 else "-"
    return f"{now.strftime('%Y.%m.%d')}|{now.strftime('%H:%M:%S')}|{sign}{abs(tz_offset)} UTC"


async def _validate_key(api_key: str, base_url: str, model: str, timeout: int) -> dict:
    logs = []
    request_body = {
        "model": model,
        "stream": False,
        "messages": [{"role": "user", "content": "Hello"}],
    }
    request_headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    logs.append({
        "request": {
            "url": f"{base_url.rstrip('/')}/chat/completions",
            "method": "POST",
            "headers": {k: (v if k != "Authorization" else f"Bearer {_mask(api_key)}") for k, v in request_headers.items()},
            "body": request_body,
        }
    })

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{base_url.rstrip('/')}/chat/completions",
                headers=request_headers,
                json=request_body,
                timeout=aiohttp.ClientTimeout(total=timeout),
            ) as resp:
                data = await resp.json(content_type=None)
                logs.append({"response": {"status": resp.status, "body": data}})

                if "error" in data:
                    code = data["error"].get("code", "")
                    msg = data["error"].get("message", "")
                    if code == "rate_limit_exceeded":
                        return {"valid": True, "status": "rate-limited", "message": msg, "logs": logs}
                    else:
                        return {"valid": False, "status": "invalid", "message": msg, "logs": logs}

                if "choices" in data:
                    return {"valid": True, "status": "valid", "message": "", "logs": logs}

                return {"valid": False, "status": "invalid", "message": str(data)[:200], "logs": logs}

    except Exception as e:
        logs.append({"error": str(e)})
        return {"valid": False, "status": "invalid", "message": str(e)[:200], "logs": logs}


async def _fetch_models(api_key: str, base_url: str, timeout: int) -> dict:
    url = f"{base_url.rstrip('/')}/models"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url,
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                timeout=aiohttp.ClientTimeout(total=timeout),
            ) as resp:
                data = await resp.json(content_type=None)
                if "error" in data:
                    return {"ok": False, "message": data["error"].get("message", str(data["error"]))[:200], "models": []}
                if "data" in data and isinstance(data["data"], list):
                    ids = [m.get("id", "") for m in data["data"] if m.get("id")]
                    return {"ok": True, "message": "", "models": ids}
                return {"ok": False, "message": str(data)[:200], "models": []}
    except Exception as e:
        return {"ok": False, "message": str(e)[:200], "models": []}


@loader.tds
class KeyRouter(loader.Module):
    """OpenAI-compatible API key manager for multiple providers"""

    strings = {
        "name": "KeyRouter",

        "main_menu": (
            "<b>KeyRouter</b>\n"
            "<blockquote>Providers: {providers} | Keys: {keys}</blockquote>"
        ),

        "no_provider_for_key": (
            "<b>No Providers</b>\n"
            "<blockquote>Add a provider first</blockquote>"
        ),

        "add_provider_url": (
            "<b>Add Provider</b>\n"
            "<blockquote>Enter base URL</blockquote>"
        ),

        "add_provider_name": (
            "<b>Add Provider</b>\n"
            "<blockquote>URL: <code>{url}</code>\n"
            "Enter display name</blockquote>"
        ),

        "add_provider_model": (
            "<b>Add Provider</b>\n"
            "<blockquote>URL: <code>{url}</code>\n"
            "Name: <b>{name}</b>\n"
            "Enter test model</blockquote>"
        ),

        "provider_saved": (
            "<b>Provider Added</b>\n"
            "<blockquote>Name: <b>{name}</b>\n"
            "URL: <code>{url}</code>\n"
            "Model: <code>{model}</code></blockquote>"
        ),

        "select_provider_for_key": (
            "<b>Add Key</b>\n"
            "<blockquote>Select provider:</blockquote>"
        ),

        "add_key": (
            "<b>Add Key</b>\n"
            "<blockquote>Provider: <b>{provider}</b>\n"
            "Enter API key:</blockquote>"
        ),

        "validating": "<b>Validating key...</b>",

        "key_valid_comment": (
            "<b>Key Valid</b>\n"
            "<blockquote>"
            "Key: <code>{key}</code>\n"
            "Status: {status}\n"
            "Add a comment or skip:</blockquote>"
        ),

        "key_invalid": (
            "<b>Key Invalid</b>\n"
            "<blockquote>"
            "Key: <code>{key}</code>\n"
            "Reason: {reason}"
            "</blockquote>"
        ),

        "key_saved": (
            "<b>Key Saved</b>\n"
            "<blockquote>Provider: <b>{provider}</b>\n"
            "<code>{key}</code></blockquote>"
        ),

        "manage_select_provider": (
            "<b>Manage</b>\n"
            "<blockquote>Select provider:</blockquote>"
        ),

        "manage_provider": (
            "<b>{provider}</b>\n"
            "<blockquote>Keys: {keys}</blockquote>"
        ),

        "provider_settings": (
            "<b>{provider} Settings</b>\n"
            "<blockquote>URL: <code>{url}</code>\n"
            "Model: <code>{model}</code></blockquote>"
        ),

        "provider_settings_model": (
            "<b>{provider}</b>\n"
            "<blockquote>Enter new test model:</blockquote>"
        ),

        "provider_model_updated": (
            "<b>Model Updated</b>\n"
            "<blockquote>Provider: <b>{provider}</b>\n"
            "Model: <code>{model}</code></blockquote>"
        ),

        "provider_deleted": "<b>Provider <b>{name}</b> deleted</b>",

        "keys_list": "<b>{provider} - Keys</b>",

        "no_keys": (
            "<b>No Keys</b>\n"
            "<blockquote>Add keys first</blockquote>"
        ),

        "validate_all_running": "<b>Validating all keys...</b>",

        "validate_all_results": (
            "<b>Validation Results</b>\n"
            "<blockquote>"
            "Provider: <b>{provider}</b>\n"
            "Total: {total}\n"
            "Valid: {valid}\n"
            "Rate limited: {rate_limited}\n"
            "Invalid: {invalid}"
            "</blockquote>"
        ),

        "export_done": (
            "<b>Export Complete</b>\n"
            "<blockquote>keys.json sent to chat</blockquote>"
        ),

        "logs_sent": (
            "<b>Logs Sent</b>\n"
            "<blockquote>Provider: <b>{provider}</b></blockquote>"
        ),

        "no_logs": (
            "<b>No Logs</b>\n"
            "<blockquote>Validate keys first to generate logs</blockquote>"
        ),

        "key_detail": (
            "<b>Key #{num}</b>\n"
            "<blockquote>"
            "Masked: <code>{masked}</code>\n"
            "Comment: {comment}\n"
            "Status: {status}\n"
            "Checked: {date}"
            "</blockquote>"
        ),

        "key_full": (
            "<b>Key #{num}</b>\n"
            "<blockquote><code>{value}</code></blockquote>"
        ),

        "key_deleted": "<b>Key #{num} deleted</b>",

        "clean_done": (
            "<b>Cleanup Done</b>\n"
            "<blockquote>Removed {count} invalid key(s)</blockquote>"
        ),

        "fetching_models": "<b>Fetching models...</b>",

        "models_list": (
            "<b>Models - Key #{num}</b>\n"
            "<blockquote>Sent to chat ({total} models)</blockquote>"
        ),

        "models_error": (
            "<b>Failed to fetch models</b>\n"
            "<blockquote>{reason}</blockquote>"
        ),

        "btn_models": "List Models",

        "btn_add_provider": "Add Provider",
        "btn_add_key": "Add Key",
        "btn_manage": "Manage",
        "btn_back": "Back",
        "btn_save": "Save Key",
        "btn_skip_comment": "Skip Comment",
        "btn_validate_all": "Validate All",
        "btn_export": "Export Keys",
        "btn_send_logs": "Send Logs",
        "btn_list": "List Keys",
        "btn_show": "Show Key",
        "btn_check": "Check Key",
        "btn_delete": "Delete Key",
        "btn_clean": "Clean Invalid",
        "btn_settings": "Provider Settings",
        "btn_delete_provider": "Delete Provider",
        "btn_change_model": "Change Test Model",

        "input_url": "Enter base URL (e.g. https://your.provider.domain/v1):",
        "input_name": "Enter provider display name:",
        "input_model": "Enter test model name:",
        "input_key": "Enter API key:",
        "input_comment": "Enter comment:",
        "input_new_model": "Enter new test model:",

        "status_valid": "Valid",
        "status_rate_limited": "Rate Limited",
        "status_invalid": "Invalid",
        "status_unknown": "Not checked",
    }

    strings_ru = {
        "main_menu": (
            "<b>KeyRouter</b>\n"
            "<blockquote>Провайдеры: {providers} | Ключи: {keys}</blockquote>"
        ),

        "no_provider_for_key": (
            "<b>Нет провайдеров</b>\n"
            "<blockquote>Сначала добавьте провайдера</blockquote>"
        ),

        "add_provider_url": (
            "<b>Добавить провайдера</b>\n"
            "<blockquote>Введите base URL</blockquote>"
        ),

        "add_provider_name": (
            "<b>Добавить провайдера</b>\n"
            "<blockquote>URL: <code>{url}</code>\n"
            "Введите отображаемое имя</blockquote>"
        ),

        "add_provider_model": (
            "<b>Добавить провайдера</b>\n"
            "<blockquote>URL: <code>{url}</code>\n"
            "Имя: <b>{name}</b>\n"
            "Введите тестовую модель</blockquote>"
        ),

        "provider_saved": (
            "<b>Провайдер добавлен</b>\n"
            "<blockquote>Имя: <b>{name}</b>\n"
            "URL: <code>{url}</code>\n"
            "Модель: <code>{model}</code></blockquote>"
        ),

        "select_provider_for_key": (
            "<b>Добавить ключ</b>\n"
            "<blockquote>Выберите провайдера:</blockquote>"
        ),

        "add_key": (
            "<b>Добавить ключ</b>\n"
            "<blockquote>Провайдер: <b>{provider}</b>\n"
            "Введите API ключ:</blockquote>"
        ),

        "validating": "<b>Проверка ключа...</b>",

        "key_valid_comment": (
            "<b>Ключ валиден</b>\n"
            "<blockquote>"
            "Ключ: <code>{key}</code>\n"
            "Статус: {status}\n"
            "Добавьте комментарий или пропустите:</blockquote>"
        ),

        "key_invalid": (
            "<b>Ключ невалиден</b>\n"
            "<blockquote>"
            "Ключ: <code>{key}</code>\n"
            "Причина: {reason}"
            "</blockquote>"
        ),

        "key_saved": (
            "<b>Ключ сохранён</b>\n"
            "<blockquote>Провайдер: <b>{provider}</b>\n"
            "<code>{key}</code></blockquote>"
        ),

        "manage_select_provider": (
            "<b>Управление</b>\n"
            "<blockquote>Выберите провайдера:</blockquote>"
        ),

        "manage_provider": (
            "<b>{provider}</b>\n"
            "<blockquote>Ключей: {keys}</blockquote>"
        ),

        "provider_settings": (
            "<b>Настройки {provider}</b>\n"
            "<blockquote>URL: <code>{url}</code>\n"
            "Модель: <code>{model}</code></blockquote>"
        ),

        "provider_settings_model": (
            "<b>{provider}</b>\n"
            "<blockquote>Введите новую тестовую модель:</blockquote>"
        ),

        "provider_model_updated": (
            "<b>Модель обновлена</b>\n"
            "<blockquote>Провайдер: <b>{provider}</b>\n"
            "Модель: <code>{model}</code></blockquote>"
        ),

        "provider_deleted": "<b>Провайдер <b>{name}</b> удалён</b>",

        "keys_list": "<b>{provider} - Ключи</b>",

        "no_keys": (
            "<b>Нет ключей</b>\n"
            "<blockquote>Сначала добавьте ключи</blockquote>"
        ),

        "validate_all_running": "<b>Проверка всех ключей...</b>",

        "validate_all_results": (
            "<b>Результаты проверки</b>\n"
            "<blockquote>"
            "Провайдер: <b>{provider}</b>\n"
            "Всего: {total}\n"
            "Валидных: {valid}\n"
            "Лимит запросов: {rate_limited}\n"
            "Невалидных: {invalid}"
            "</blockquote>"
        ),

        "export_done": (
            "<b>Экспорт завершён</b>\n"
            "<blockquote>keys.json отправлен в чат</blockquote>"
        ),

        "logs_sent": (
            "<b>Логи отправлены</b>\n"
            "<blockquote>Провайдер: <b>{provider}</b></blockquote>"
        ),

        "no_logs": (
            "<b>Нет логов</b>\n"
            "<blockquote>Сначала проверьте ключи</blockquote>"
        ),

        "key_detail": (
            "<b>Ключ #{num}</b>\n"
            "<blockquote>"
            "Маска: <code>{masked}</code>\n"
            "Комментарий: {comment}\n"
            "Статус: {status}\n"
            "Проверен: {date}"
            "</blockquote>"
        ),

        "key_full": (
            "<b>Ключ #{num}</b>\n"
            "<blockquote><code>{value}</code></blockquote>"
        ),

        "key_deleted": "<b>Ключ #{num} удалён</b>",

        "clean_done": (
            "<b>Очистка завершена</b>\n"
            "<blockquote>Удалено {count} невалидных ключей</blockquote>"
        ),

        "fetching_models": "<b>Получение списка моделей...</b>",

        "models_list": (
            "<b>Модели - Ключ #{num}</b>\n"
            "<blockquote>Отправлено в чат ({total} моделей)</blockquote>"
        ),

        "models_error": (
            "<b>Не удалось получить модели</b>\n"
            "<blockquote>{reason}</blockquote>"
        ),

        "btn_models": "Список моделей",

        "btn_add_provider": "Добавить провайдера",
        "btn_add_key": "Добавить ключ",
        "btn_manage": "Управление",
        "btn_back": "Назад",
        "btn_save": "Сохранить ключ",
        "btn_skip_comment": "Пропустить",
        "btn_validate_all": "Проверить все",
        "btn_export": "Экспорт ключей",
        "btn_send_logs": "Отправить логи",
        "btn_list": "Список ключей",
        "btn_show": "Показать ключ",
        "btn_check": "Проверить ключ",
        "btn_delete": "Удалить ключ",
        "btn_clean": "Очистить невалидные",
        "btn_settings": "Настройки провайдера",
        "btn_delete_provider": "Удалить провайдера",
        "btn_change_model": "Сменить модель",

        "input_url": "Введите base URL (например https://your.provider.domain/v1):",
        "input_name": "Введите отображаемое имя провайдера:",
        "input_model": "Введите название тестовой модели:",
        "input_key": "Введите API ключ:",
        "input_comment": "Введите комментарий:",
        "input_new_model": "Введите новую тестовую модель:",

        "status_valid": "Валиден",
        "status_rate_limited": "Лимит запросов",
        "status_invalid": "Невалиден",
        "status_unknown": "Не проверен",
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "timezone",
                3,
                "Timezone offset (UTC), e.g. 3 for UTC+3",
                validator=loader.validators.Integer(minimum=-12, maximum=12),
            ),
            loader.ConfigValue(
                "timeout",
                30,
                "Request timeout in seconds for key validation",
                validator=loader.validators.Integer(minimum=10, maximum=3600),
            ),
        )
        self._providers: list = []
        self._keys: list = []
        self._logs: dict = {}

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        self._providers = self._db.get("KeyRouter", "providers", [])
        raw_keys = self._db.get("KeyRouter", "keys", [])
        self._keys = self._migrate_keys(raw_keys)
        self._logs = self._db.get("KeyRouter", "logs", {})
        self._save_all()

    def _migrate_keys(self, raw: list) -> list:
        normalized = []
        for entry in raw:
            if isinstance(entry, dict):
                if "comment" not in entry:
                    entry["comment"] = ""
                if "provider_id" not in entry:
                    entry["provider_id"] = 0
                normalized.append(entry)
        for i, entry in enumerate(normalized, start=1):
            entry["key"] = i
        return normalized

    def _save_all(self):
        self._db.set("KeyRouter", "providers", self._providers)
        self._db.set("KeyRouter", "keys", self._keys)
        self._db.set("KeyRouter", "logs", self._logs)

    def _status_label(self, status: str) -> str:
        mapping = {
            "valid": self.strings["status_valid"],
            "rate-limited": self.strings["status_rate_limited"],
            "invalid": self.strings["status_invalid"],
        }
        return mapping.get(status, self.strings["status_unknown"])

    def _find_key_index(self, num: int) -> int:
        for i, entry in enumerate(self._keys):
            if entry.get("key") == num:
                return i
        return -1

    def _find_provider(self, pid: int) -> dict:
        for p in self._providers:
            if p.get("id") == pid:
                return p
        return {}

    def _keys_for_provider(self, pid: int) -> list:
        return [e for e in self._keys if e.get("provider_id") == pid]

    def _total_keys(self) -> int:
        return len(self._keys)

    async def _cb_main_menu(self, call: InlineCall):
        await call.edit(
            self.strings["main_menu"].format(
                providers=len(self._providers),
                keys=self._total_keys(),
            ),
            reply_markup=[
                [{"text": self.strings["btn_add_provider"], "callback": self._cb_add_provider_start, "style": "primary"}],
                [{"text": self.strings["btn_add_key"], "callback": self._cb_select_provider_for_key, "style": "primary"}],
                [{"text": self.strings["btn_manage"], "callback": self._cb_manage_select_provider, "style": "primary"}],
            ],
        )

    async def _cb_add_provider_start(self, call: InlineCall):
        await call.edit(
            self.strings["add_provider_url"],
            reply_markup=[[{
                "text": self.strings["input_url"],
                "input": self.strings["input_url"],
                "handler": self._cb_add_provider_got_url,
                "style": "primary",
            }]],
        )

    async def _cb_add_provider_got_url(self, call: InlineCall, url: str):
        url = url.strip().rstrip("/")
        await call.edit(
            self.strings["add_provider_name"].format(url=_escape(url)),
            reply_markup=[[{
                "text": self.strings["input_name"],
                "input": self.strings["input_name"],
                "handler": self._cb_add_provider_got_name,
                "args": (url,),
                "style": "primary",
            }]],
        )

    async def _cb_add_provider_got_name(self, call: InlineCall, name: str, url: str):
        name = name.strip()
        await call.edit(
            self.strings["add_provider_model"].format(url=_escape(url), name=_escape(name)),
            reply_markup=[[{
                "text": self.strings["input_model"],
                "input": self.strings["input_model"],
                "handler": self._cb_add_provider_got_model,
                "args": (url, name),
                "style": "primary",
            }]],
        )

    async def _cb_add_provider_got_model(self, call: InlineCall, model: str, url: str, name: str):
        model = model.strip()
        pid = (max((p.get("id", 0) for p in self._providers), default=0)) + 1
        self._providers.append({
            "id": pid,
            "name": name,
            "url": url,
            "model": model,
        })
        self._save_all()

        await call.edit(
            self.strings["provider_saved"].format(
                name=_escape(name),
                url=_escape(url),
                model=_escape(model),
            ),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]],
        )

    async def _cb_select_provider_for_key(self, call: InlineCall):
        if not self._providers:
            await call.edit(
                self.strings["no_provider_for_key"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]],
            )
            return

        rows = []
        for p in self._providers:
            rows.append([{
                "text": p["name"],
                "callback": self._cb_add_key_for_provider,
                "args": (p["id"],),
                "style": "primary",
            }])
        rows.append([{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}])

        await call.edit(
            self.strings["select_provider_for_key"],
            reply_markup=rows,
        )

    async def _cb_add_key_for_provider(self, call: InlineCall, pid: int):
        provider = self._find_provider(pid)
        if not provider:
            await call.answer("Provider not found", show_alert=True)
            return

        await call.edit(
            self.strings["add_key"].format(provider=_escape(provider["name"])),
            reply_markup=[[{
                "text": self.strings["input_key"],
                "input": self.strings["input_key"],
                "handler": self._cb_validate_key_for_provider,
                "args": (pid,),
                "style": "primary",
            }]],
        )

    async def _cb_validate_key_for_provider(self, call: InlineCall, key: str, pid: int):
        key = key.strip()
        masked = _mask(key)
        provider = self._find_provider(pid)
        if not provider:
            await call.answer("Provider not found", show_alert=True)
            return

        await call.edit(self.strings["validating"], reply_markup=[])

        result = await _validate_key(key, provider["url"], provider["model"], self.config["timeout"])

        if result["valid"]:
            status_str = self._status_label(result["status"])
            await call.edit(
                self.strings["key_valid_comment"].format(
                    key=_escape(masked),
                    status=status_str,
                ),
                reply_markup=[
                    [{
                        "text": self.strings["input_comment"],
                        "input": self.strings["input_comment"],
                        "handler": self._cb_save_key_with_comment,
                        "args": (key, result["status"], pid, result["logs"]),
                        "style": "primary",
                    }],
                    [{"text": self.strings["btn_skip_comment"], "callback": self._cb_save_key_no_comment, "args": (key, result["status"], pid, result["logs"]), "style": "success"}],
                    [{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}],
                ],
            )
        else:
            await call.edit(
                self.strings["key_invalid"].format(
                    key=_escape(masked),
                    reason=_escape(result["message"]),
                ),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]],
            )

    async def _do_save_key(self, call: InlineCall, key: str, status: str, pid: int, logs: list, comment: str):
        existing_values = [e.get("value") for e in self._keys if e.get("provider_id") == pid]
        provider = self._find_provider(pid)
        if not provider:
            await call.answer("Provider not found", show_alert=True)
            return

        if key not in existing_values:
            num = (max((e.get("key", 0) for e in self._keys), default=0)) + 1
            self._keys.append({
                "key": num,
                "value": key,
                "status": status,
                "comment": comment,
                "date": _now(self.config["timezone"]),
                "provider_id": pid,
            })
            log_key = f"{pid}:{num}"
            self._logs[log_key] = logs
            self._save_all()

        masked = _mask(key)
        await call.edit(
            self.strings["key_saved"].format(
                provider=_escape(provider["name"]),
                key=_escape(masked),
            ),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "primary"}]],
        )

    async def _cb_save_key_with_comment(self, call: InlineCall, comment: str, key: str, status: str, pid: int, logs: list):
        await self._do_save_key(call, key, status, pid, logs, comment.strip())

    async def _cb_save_key_no_comment(self, call: InlineCall, key: str, status: str, pid: int, logs: list):
        await self._do_save_key(call, key, status, pid, logs, "")

    async def _cb_manage_select_provider(self, call: InlineCall):
        if not self._providers:
            await call.edit(
                self.strings["no_provider_for_key"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]],
            )
            return

        rows = []
        for p in self._providers:
            key_count = len(self._keys_for_provider(p["id"]))
            rows.append([{
                "text": f"{p['name']} ({key_count})",
                "callback": self._cb_manage_provider,
                "args": (p["id"],),
                "style": "primary",
            }])
        rows.append([{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}])

        await call.edit(
            self.strings["manage_select_provider"],
            reply_markup=rows,
        )

    async def _cb_manage_provider(self, call: InlineCall, pid: int):
        provider = self._find_provider(pid)
        if not provider:
            await call.answer("Provider not found", show_alert=True)
            return

        pkeys = self._keys_for_provider(pid)
        rows = [
            [{"text": self.strings["btn_settings"], "callback": self._cb_provider_settings, "args": (pid,), "style": "primary"}],
            [{"text": self.strings["btn_list"], "callback": self._cb_list_keys, "args": (pid,), "style": "primary"}],
            [{"text": self.strings["btn_validate_all"], "callback": self._cb_validate_all, "args": (pid,), "style": "success"}],
            [{"text": self.strings["btn_send_logs"], "callback": self._cb_send_logs, "args": (pid,), "style": "primary"}],
            [{"text": self.strings["btn_clean"], "callback": self._cb_clean_invalid, "args": (pid,), "style": "danger"}],
            [{"text": self.strings["btn_export"], "callback": self._cb_export, "args": (pid,), "style": "success"}],
            [{"text": self.strings["btn_back"], "callback": self._cb_manage_select_provider, "style": "danger"}],
        ]

        await call.edit(
            self.strings["manage_provider"].format(
                provider=_escape(provider["name"]),
                keys=len(pkeys),
            ),
            reply_markup=rows,
        )

    async def _cb_provider_settings(self, call: InlineCall, pid: int):
        provider = self._find_provider(pid)
        if not provider:
            await call.answer("Provider not found", show_alert=True)
            return

        await call.edit(
            self.strings["provider_settings"].format(
                provider=_escape(provider["name"]),
                url=_escape(provider["url"]),
                model=_escape(provider["model"]),
            ),
            reply_markup=[
                [{"text": self.strings["btn_change_model"], "callback": self._cb_change_model_start, "args": (pid,), "style": "primary"}],
                [{"text": self.strings["btn_delete_provider"], "callback": self._cb_delete_provider, "args": (pid,), "style": "danger"}],
                [{"text": self.strings["btn_back"], "callback": self._cb_manage_provider, "args": (pid,), "style": "danger"}],
            ],
        )

    async def _cb_change_model_start(self, call: InlineCall, pid: int):
        provider = self._find_provider(pid)
        if not provider:
            await call.answer("Provider not found", show_alert=True)
            return

        await call.edit(
            self.strings["provider_settings_model"].format(provider=_escape(provider["name"])),
            reply_markup=[[{
                "text": self.strings["input_new_model"],
                "input": self.strings["input_new_model"],
                "handler": self._cb_change_model_save,
                "args": (pid,),
                "style": "primary",
            }]],
        )

    async def _cb_change_model_save(self, call: InlineCall, model: str, pid: int):
        model = model.strip()
        for p in self._providers:
            if p.get("id") == pid:
                p["model"] = model
                break
        self._save_all()

        provider = self._find_provider(pid)
        await call.edit(
            self.strings["provider_model_updated"].format(
                provider=_escape(provider.get("name", "")),
                model=_escape(model),
            ),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_provider_settings, "args": (pid,), "style": "danger"}]],
        )

    async def _cb_delete_provider(self, call: InlineCall, pid: int):
        provider = self._find_provider(pid)
        name = provider.get("name", str(pid)) if provider else str(pid)

        self._providers = [p for p in self._providers if p.get("id") != pid]
        self._keys = [e for e in self._keys if e.get("provider_id") != pid]

        for i, entry in enumerate(self._keys, start=1):
            entry["key"] = i

        self._save_all()

        await call.edit(
            self.strings["provider_deleted"].format(name=_escape(name)),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]],
        )

    async def _cb_list_keys(self, call: InlineCall, pid: int):
        provider = self._find_provider(pid)
        if not provider:
            await call.answer("Provider not found", show_alert=True)
            return

        pkeys = self._keys_for_provider(pid)
        if not pkeys:
            await call.edit(
                self.strings["no_keys"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_manage_provider, "args": (pid,), "style": "danger"}]],
            )
            return

        rows = []
        for entry in pkeys:
            num = entry.get("key")
            masked = _mask(entry.get("value", ""))
            rows.append([{
                "text": f"{num}. {masked}",
                "callback": self._cb_key_detail,
                "args": (num, pid),
                "style": "primary",
            }])

        rows.append([{"text": self.strings["btn_back"], "callback": self._cb_manage_provider, "args": (pid,), "style": "danger"}])

        await call.edit(
            self.strings["keys_list"].format(provider=_escape(provider["name"])),
            reply_markup=rows,
        )

    async def _cb_key_detail(self, call: InlineCall, num: int, pid: int):
        idx = self._find_key_index(num)
        if idx == -1:
            await call.answer("Key not found", show_alert=True)
            return

        entry = self._keys[idx]
        masked = _mask(entry.get("value", ""))
        status = self._status_label(entry.get("status", ""))
        date = entry.get("date", "-")
        comment = entry.get("comment", "") or "-"

        await call.edit(
            self.strings["key_detail"].format(
                num=num,
                masked=_escape(masked),
                comment=_escape(comment),
                status=status,
                date=date,
            ),
            reply_markup=[
                [{"text": self.strings["btn_show"], "callback": self._cb_show_key, "args": (num, pid), "style": "primary"}],
                [{"text": self.strings["btn_check"], "callback": self._cb_check_single, "args": (num, pid), "style": "success"}],
                [{"text": self.strings["btn_models"], "callback": self._cb_list_models, "args": (num, pid), "style": "primary"}],
                [{"text": self.strings["btn_delete"], "callback": self._cb_delete_key, "args": (num, pid), "style": "danger"}],
                [{"text": self.strings["btn_back"], "callback": self._cb_list_keys, "args": (pid,), "style": "danger"}],
            ],
        )

    async def _cb_show_key(self, call: InlineCall, num: int, pid: int):
        idx = self._find_key_index(num)
        if idx == -1:
            await call.answer("Key not found", show_alert=True)
            return

        entry = self._keys[idx]
        await call.edit(
            self.strings["key_full"].format(
                num=num,
                value=_escape(entry.get("value", "")),
            ),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_key_detail, "args": (num, pid), "style": "danger"}]],
        )

    async def _cb_list_models(self, call: InlineCall, num: int, pid: int):
        idx = self._find_key_index(num)
        if idx == -1:
            await call.answer("Key not found", show_alert=True)
            return

        provider = self._find_provider(pid)
        if not provider:
            await call.answer("Provider not found", show_alert=True)
            return

        await call.edit(self.strings["fetching_models"], reply_markup=[])

        entry = self._keys[idx]
        key = entry.get("value", "")
        result = await _fetch_models(key, provider["url"], self.config["timeout"])

        if result["ok"]:
            export = {
                "provider": provider["name"],
                "base_url": provider["url"],
                "key_num": num,
                "masked": _mask(key),
                "models": result["models"],
                "total": len(result["models"]),
            }
            tmp = tempfile.NamedTemporaryFile(
                mode="w",
                suffix=".json",
                prefix=f"kr_models_{provider['name'].lower()}_",
                delete=False,
            )
            json.dump(export, tmp, indent=2, ensure_ascii=False)
            tmp.close()
            try:
                await self._client.send_file(
                    call.form["chat"],
                    tmp.name,
                    force_document=True,
                    file_name=f"models_{provider['name']}_key{num}.json",
                )
            except Exception as e:
                logger.exception("send_file failed: %s", e)
            finally:
                os.unlink(tmp.name)

            await call.edit(
                self.strings["models_list"].format(num=num, total=len(result["models"])),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_key_detail, "args": (num, pid), "style": "danger"}]],
            )
        else:
            await call.edit(
                self.strings["models_error"].format(reason=_escape(result["message"])),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_key_detail, "args": (num, pid), "style": "danger"}]],
            )

    async def _cb_check_single(self, call: InlineCall, num: int, pid: int):
        idx = self._find_key_index(num)
        if idx == -1:
            await call.answer("Key not found", show_alert=True)
            return

        provider = self._find_provider(pid)
        if not provider:
            await call.answer("Provider not found", show_alert=True)
            return

        await call.edit(self.strings["validating"], reply_markup=[])

        entry = self._keys[idx]
        key = entry.get("value", "")
        result = await _validate_key(key, provider["url"], provider["model"], self.config["timeout"])

        self._keys[idx]["status"] = result["status"]
        self._keys[idx]["date"] = _now(self.config["timezone"])

        log_key = f"{pid}:{num}"
        self._logs[log_key] = result["logs"]
        self._save_all()

        await self._cb_key_detail(call, num, pid)

    async def _cb_delete_key(self, call: InlineCall, num: int, pid: int):
        idx = self._find_key_index(num)
        if idx == -1:
            await call.answer("Key not found", show_alert=True)
            return

        self._keys.pop(idx)
        for i, entry in enumerate(self._keys, start=1):
            entry["key"] = i
        self._save_all()

        await call.edit(
            self.strings["key_deleted"].format(num=num),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_list_keys, "args": (pid,), "style": "danger"}]],
        )

    async def _cb_validate_all(self, call: InlineCall, pid: int):
        provider = self._find_provider(pid)
        if not provider:
            await call.answer("Provider not found", show_alert=True)
            return

        pkeys = self._keys_for_provider(pid)
        if not pkeys:
            await call.edit(
                self.strings["no_keys"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_manage_provider, "args": (pid,), "style": "danger"}]],
            )
            return

        await call.edit(self.strings["validate_all_running"], reply_markup=[])

        tasks = [_validate_key(e.get("value", ""), provider["url"], provider["model"], self.config["timeout"]) for e in pkeys]
        results = await asyncio.gather(*tasks)

        valid = 0
        rate_limited = 0
        invalid = 0

        for entry, result in zip(pkeys, results):
            idx = self._find_key_index(entry["key"])
            if idx != -1:
                self._keys[idx]["status"] = result["status"]
                self._keys[idx]["date"] = _now(self.config["timezone"])
                log_key = f"{pid}:{entry['key']}"
                self._logs[log_key] = result["logs"]

            if result["status"] == "valid":
                valid += 1
            elif result["status"] == "rate-limited":
                rate_limited += 1
            else:
                invalid += 1

        self._save_all()

        await call.edit(
            self.strings["validate_all_results"].format(
                provider=_escape(provider["name"]),
                total=len(pkeys),
                valid=valid,
                rate_limited=rate_limited,
                invalid=invalid,
            ),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_manage_provider, "args": (pid,), "style": "danger"}]],
        )

    async def _cb_clean_invalid(self, call: InlineCall, pid: int):
        before = len(self._keys_for_provider(pid))
        self._keys = [
            e for e in self._keys
            if not (e.get("provider_id") == pid and e.get("status") == "invalid")
        ]
        for i, entry in enumerate(self._keys, start=1):
            entry["key"] = i
        self._save_all()

        after = len(self._keys_for_provider(pid))
        removed = before - after

        await call.edit(
            self.strings["clean_done"].format(count=removed),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_manage_provider, "args": (pid,), "style": "danger"}]],
        )

    async def _cb_send_logs(self, call: InlineCall, pid: int):
        provider = self._find_provider(pid)
        if not provider:
            await call.answer("Provider not found", show_alert=True)
            return

        pkeys = self._keys_for_provider(pid)
        has_logs = any(f"{pid}:{e['key']}" in self._logs for e in pkeys)

        if not pkeys or not has_logs:
            await call.edit(
                self.strings["no_logs"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_manage_provider, "args": (pid,), "style": "danger"}]],
            )
            return

        export = {
            "provider": provider["name"],
            "base_url": provider["url"],
            "test_model": provider["model"],
            "keys": [],
        }

        for entry in pkeys:
            log_key = f"{pid}:{entry['key']}"
            export["keys"].append({
                "key_num": entry["key"],
                "masked": _mask(entry.get("value", "")),
                "status": entry.get("status", "unknown"),
                "comment": entry.get("comment", ""),
                "date": entry.get("date", "-"),
                "full_request_logs": self._logs.get(log_key, []),
            })

        tmp = tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".json",
            prefix=f"kr_logs_{provider['name'].lower()}_",
            delete=False,
        )
        json.dump(export, tmp, indent=2, ensure_ascii=False)
        tmp.close()

        try:
            await self._client.send_file(
                call.form["chat"],
                tmp.name,
                force_document=True,
                file_name=f"logs_{provider['name']}.json",
            )
        except Exception as e:
            logger.exception("send_file failed: %s", e)
        finally:
            os.unlink(tmp.name)

        await call.edit(
            self.strings["logs_sent"].format(provider=_escape(provider["name"])),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_manage_provider, "args": (pid,), "style": "danger"}]],
        )

    async def _cb_export(self, call: InlineCall, pid: int):
        provider = self._find_provider(pid)
        if not provider:
            await call.answer("Provider not found", show_alert=True)
            return

        pkeys = self._keys_for_provider(pid)
        if not pkeys:
            await call.answer(self.strings["no_keys"], show_alert=True)
            return

        export_data = []
        for entry in pkeys:
            export_data.append({
                "key": entry.get("key"),
                "value": entry.get("value", ""),
                "status": entry.get("status", "unknown"),
                "comment": entry.get("comment", ""),
                "date": entry.get("date", "-"),
                "provider": provider["name"],
            })

        tmp = tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".json",
            prefix=f"kr_keys_{provider['name'].lower()}_",
            delete=False,
        )
        json.dump(export_data, tmp, indent=2, ensure_ascii=False)
        tmp.close()

        try:
            await self._client.send_file(
                call.form["chat"],
                tmp.name,
                force_document=True,
                file_name=f"keys_{provider['name']}.json",
            )
        except Exception as e:
            logger.exception("send_file failed: %s", e)
        finally:
            os.unlink(tmp.name)

        await call.edit(
            self.strings["export_done"],
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_manage_provider, "args": (pid,), "style": "danger"}]],
        )

    @loader.command(
        ru_doc="Менеджер API ключей для OpenAI-совместимых провайдеров",
        en_doc="API key manager for OpenAI-compatible providers",
    )
    async def kr(self, message: Message):
        """API key manager for OpenAI-compatible providers"""
        await self.inline.form(
            text=self.strings["main_menu"].format(
                providers=len(self._providers),
                keys=self._total_keys(),
            ),
            message=message,
            reply_markup=[
                [{"text": self.strings["btn_add_provider"], "callback": self._cb_add_provider_start, "style": "primary"}],
                [{"text": self.strings["btn_add_key"], "callback": self._cb_select_provider_for_key, "style": "primary"}],
                [{"text": self.strings["btn_manage"], "callback": self._cb_manage_select_provider, "style": "primary"}],
            ],
            silent=True,
        )
