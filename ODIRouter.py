__version__ = (1, 0, 1)
# meta developer: I_execute.t.me

import logging
import json
import tempfile
import os
import asyncio
from datetime import datetime

import aiohttp

from telethon.tl.types import Message
from .. import loader, utils
from ..inline.types import InlineCall

logger = logging.getLogger(__name__)

ODIROUTER_BASE = "https://api.odirouter.ai/v1"


def _escape(text):
    if not text:
        return ""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _mask(key: str) -> str:
    if len(key) > 12:
        return key[:8] + "..." + key[-4:]
    return key


def _now() -> str:
    return datetime.now().strftime("%Y.%m.%d")


async def _validate_key(api_key: str, model: str) -> dict:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{ODIROUTER_BASE}/chat/completions",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": model,
                    "stream": False,
                    "messages": [{"role": "user", "content": "Hello"}],
                },
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                data = await resp.json(content_type=None)

                if "error" in data:
                    code = data["error"].get("code", "")
                    msg = data["error"].get("message", "")

                    if code == "rate_limit_exceeded":
                        return {"valid": True, "status": "rate-limited", "message": msg}
                    elif code == "upstream_rejected":
                        return {"valid": False, "status": "invalid", "message": msg}
                    else:
                        return {"valid": False, "status": "invalid", "message": msg}

                if "choices" in data:
                    return {"valid": True, "status": "valid", "message": ""}

                return {"valid": False, "status": "invalid", "message": str(data)[:100]}

    except Exception as e:
        return {"valid": False, "status": "invalid", "message": str(e)[:100]}


@loader.tds
class ODIRouter(loader.Module):
    """ODIRouter API key validator and manager"""

    strings = {
        "name": "ODIRouter",

        "main_menu": (
            "<b>ODIRouter Key Manager</b>\n"
            "<blockquote>Total keys: {total}</blockquote>"
        ),

        "add_key": (
            "<b>Add API Key</b>\n"
            "<blockquote>Enter your ODIRouter API key:</blockquote>"
        ),

        "validating": "<b>Validating key...</b>",

        "key_valid": (
            "<b>Key Valid</b>\n"
            "<blockquote>"
            "Key: <code>{key}</code>\n"
            "Status: {status}"
            "</blockquote>"
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
            "<blockquote><code>{key}</code></blockquote>"
        ),

        "manage_menu": (
            "<b>Manage Keys</b>\n"
            "<blockquote>Total: {total}</blockquote>"
        ),

        "keys_list": "<b>Keys</b>",

        "no_keys": (
            "<b>No Keys</b>\n"
            "<blockquote>Add keys first</blockquote>"
        ),

        "validate_all_running": "<b>Validating all keys...</b>",

        "validate_all_results": (
            "<b>Validation Results</b>\n"
            "<blockquote>"
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

        "key_detail": (
            "<b>Key #{num}</b>\n"
            "<blockquote>"
            "Masked: <code>{masked}</code>\n"
            "Status: {status}\n"
            "Checked: {date}"
            "</blockquote>"
        ),

        "key_full": (
            "<b>Key #{num}</b>\n"
            "<blockquote><code>{value}</code></blockquote>"
        ),

        "key_deleted": (
            "<b>Key #{num} deleted</b>"
        ),

        "clean_done": (
            "<b>Cleanup Done</b>\n"
            "<blockquote>Removed {count} invalid key(s)</blockquote>"
        ),

        "btn_add": "Add Key",
        "btn_manage": "Manage",
        "btn_back": "Back",
        "btn_save": "Save",
        "btn_validate_all": "Validate All",
        "btn_export": "Export",
        "btn_list": "List Keys",
        "btn_show": "Show Key",
        "btn_check": "Check Key",
        "btn_delete": "Delete Key",
        "btn_clean": "Clean Invalid",

        "input_key": "Enter API key:",

        "status_valid": "Valid",
        "status_rate_limited": "Rate Limited",
        "status_invalid": "Invalid",
        "status_unknown": "Not checked",
    }

    strings_ru = {
        "main_menu": (
            "<b>ODIRouter Key Manager</b>\n"
            "<blockquote>Всего ключей: {total}</blockquote>"
        ),

        "add_key": (
            "<b>Добавить API ключ</b>\n"
            "<blockquote>Введите ваш ODIRouter API ключ:</blockquote>"
        ),

        "validating": "<b>Проверка ключа...</b>",

        "key_valid": (
            "<b>Ключ валиден</b>\n"
            "<blockquote>"
            "Ключ: <code>{key}</code>\n"
            "Статус: {status}"
            "</blockquote>"
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
            "<blockquote><code>{key}</code></blockquote>"
        ),

        "manage_menu": (
            "<b>Управление ключами</b>\n"
            "<blockquote>Всего: {total}</blockquote>"
        ),

        "keys_list": "<b>Ключи</b>",

        "no_keys": (
            "<b>Нет ключей</b>\n"
            "<blockquote>Сначала добавьте ключи</blockquote>"
        ),

        "validate_all_running": "<b>Проверка всех ключей...</b>",

        "validate_all_results": (
            "<b>Результаты проверки</b>\n"
            "<blockquote>"
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

        "key_detail": (
            "<b>Ключ #{num}</b>\n"
            "<blockquote>"
            "Маска: <code>{masked}</code>\n"
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

        "btn_add": "Добавить ключ",
        "btn_manage": "Управление",
        "btn_back": "Назад",
        "btn_save": "Сохранить",
        "btn_validate_all": "Проверить все",
        "btn_export": "Экспорт",
        "btn_list": "Список ключей",
        "btn_show": "Показать ключ",
        "btn_check": "Проверить ключ",
        "btn_delete": "Удалить ключ",
        "btn_clean": "Очистить невалидные",

        "input_key": "Введите API ключ:",

        "status_valid": "Валиден",
        "status_rate_limited": "Лимит запросов",
        "status_invalid": "Невалиден",
        "status_unknown": "Не проверен",
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "model",
                "free-gpt-5.6-terra",
                "Model used for key validation",
                validator=loader.validators.String(),
            ),
        )
        self._keys: list = []

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        raw = self._db.get("ODIRouter", "keys", [])
        self._keys = self._normalize_keys(raw)
        self._save_keys()

    def _normalize_keys(self, raw: list) -> list:
        normalized = []
        for entry in raw:
            if isinstance(entry, str):
                normalized.append({
                    "key": 0,
                    "value": entry,
                    "status": "unknown",
                    "date": "-",
                })
            elif isinstance(entry, dict):
                normalized.append(entry)
        for i, entry in enumerate(normalized, start=1):
            entry["key"] = i
        return normalized

    def _save_keys(self):
        self._db.set("ODIRouter", "keys", self._keys)

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

    async def _cb_main_menu(self, call: InlineCall):
        await call.edit(
            self.strings["main_menu"].format(total=len(self._keys)),
            reply_markup=[
                [{"text": self.strings["btn_add"], "callback": self._cb_add_key, "style": "primary"}],
                [{"text": self.strings["btn_manage"], "callback": self._cb_manage_menu, "style": "primary"}],
            ],
        )

    async def _cb_add_key(self, call: InlineCall):
        await call.edit(
            self.strings["add_key"],
            reply_markup=[[
                {
                    "text": self.strings["input_key"],
                    "input": self.strings["input_key"],
                    "handler": self._cb_validate_key,
                    "style": "primary",
                }
            ]],
        )

    async def _cb_validate_key(self, call: InlineCall, key: str):
        key = key.strip()
        masked = _mask(key)
        await call.edit(self.strings["validating"], reply_markup=[])

        result = await _validate_key(key, self.config["model"])

        if result["valid"]:
            status_str = self._status_label(result["status"])
            text = self.strings["key_valid"].format(
                key=_escape(masked),
                status=status_str,
            )
            markup = [
                [{"text": self.strings["btn_save"], "callback": self._cb_save_key, "args": (key, result["status"]), "style": "success"}],
                [{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}],
            ]
        else:
            text = self.strings["key_invalid"].format(
                key=_escape(masked),
                reason=_escape(result["message"]),
            )
            markup = [[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]]

        await call.edit(text, reply_markup=markup)

    async def _cb_save_key(self, call: InlineCall, key: str, status: str):
        existing_values = [e.get("value") for e in self._keys]
        if key not in existing_values:
            num = (max((e.get("key", 0) for e in self._keys), default=0)) + 1
            self._keys.append({
                "key": num,
                "value": key,
                "status": status,
                "date": _now(),
            })
            self._save_keys()

        masked = _mask(key)
        await call.edit(
            self.strings["key_saved"].format(key=_escape(masked)),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "primary"}]],
        )

    async def _cb_manage_menu(self, call: InlineCall):
        if not self._keys:
            await call.edit(
                self.strings["no_keys"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "primary"}]],
            )
            return

        await call.edit(
            self.strings["manage_menu"].format(total=len(self._keys)),
            reply_markup=[
                [{"text": self.strings["btn_list"], "callback": self._cb_list_keys, "style": "primary"}],
                [{"text": self.strings["btn_validate_all"], "callback": self._cb_validate_all, "style": "primary"}],
                [{"text": self.strings["btn_clean"], "callback": self._cb_clean_invalid, "style": "danger"}],
                [{"text": self.strings["btn_export"], "callback": self._cb_export, "style": "success"}],
                [{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}],
            ],
        )

    async def _cb_list_keys(self, call: InlineCall):
        if not self._keys:
            await call.edit(
                self.strings["no_keys"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_manage_menu, "style": "danger"}]],
            )
            return

        rows = []
        for entry in self._keys:
            num = entry.get("key")
            masked = _mask(entry.get("value", ""))
            rows.append([{
                "text": f"{num}. {masked}",
                "callback": self._cb_key_detail,
                "args": (num,),
                "style": "primary",
            }])

        rows.append([{"text": self.strings["btn_back"], "callback": self._cb_manage_menu, "style": "danger"}])

        await call.edit(
            self.strings["keys_list"],
            reply_markup=rows,
        )

    async def _cb_key_detail(self, call: InlineCall, num: int):
        idx = self._find_key_index(num)
        if idx == -1:
            await call.answer("Key not found", show_alert=True)
            return

        entry = self._keys[idx]
        masked = _mask(entry.get("value", ""))
        status = self._status_label(entry.get("status", ""))
        date = entry.get("date", "-")

        await call.edit(
            self.strings["key_detail"].format(
                num=num,
                masked=_escape(masked),
                status=status,
                date=date,
            ),
            reply_markup=[
                [{"text": self.strings["btn_show"], "callback": self._cb_show_key, "args": (num,), "style": "primary"}],
                [{"text": self.strings["btn_check"], "callback": self._cb_check_single, "args": (num,), "style": "success"}],
                [{"text": self.strings["btn_delete"], "callback": self._cb_delete_key, "args": (num,), "style": "danger"}],
                [{"text": self.strings["btn_back"], "callback": self._cb_list_keys, "style": "danger"}],
            ],
        )

    async def _cb_show_key(self, call: InlineCall, num: int):
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
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_key_detail, "args": (num,), "style": "danger"}]],
        )

    async def _cb_check_single(self, call: InlineCall, num: int):
        idx = self._find_key_index(num)
        if idx == -1:
            await call.answer("Key not found", show_alert=True)
            return

        await call.edit(self.strings["validating"], reply_markup=[])

        entry = self._keys[idx]
        key = entry.get("value", "")
        result = await _validate_key(key, self.config["model"])

        self._keys[idx]["status"] = result["status"]
        self._keys[idx]["date"] = _now()
        self._save_keys()

        await self._cb_key_detail(call, num)

    async def _cb_delete_key(self, call: InlineCall, num: int):
        idx = self._find_key_index(num)
        if idx == -1:
            await call.answer("Key not found", show_alert=True)
            return

        self._keys.pop(idx)
        self._save_keys()

        await call.edit(
            self.strings["key_deleted"].format(num=num),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_list_keys, "style": "danger"}]],
        )

    async def _cb_validate_all(self, call: InlineCall):
        await call.edit(self.strings["validate_all_running"], reply_markup=[])

        tasks = [_validate_key(e.get("value", ""), self.config["model"]) for e in self._keys]
        results = await asyncio.gather(*tasks)

        valid = 0
        rate_limited = 0
        invalid = 0

        for i, result in enumerate(results):
            self._keys[i]["status"] = result["status"]
            self._keys[i]["date"] = _now()

            if result["status"] == "valid":
                valid += 1
            elif result["status"] == "rate-limited":
                rate_limited += 1
            else:
                invalid += 1

        self._save_keys()

        await call.edit(
            self.strings["validate_all_results"].format(
                total=len(self._keys),
                valid=valid,
                rate_limited=rate_limited,
                invalid=invalid,
            ),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_manage_menu, "style": "danger"}]],
        )

    async def _cb_clean_invalid(self, call: InlineCall):
        before = len(self._keys)
        self._keys = [e for e in self._keys if e.get("status") != "invalid"]

        for i, entry in enumerate(self._keys):
            entry["key"] = i + 1

        self._save_keys()
        removed = before - len(self._keys)

        await call.edit(
            self.strings["clean_done"].format(count=removed),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_manage_menu, "style": "danger"}]],
        )

    async def _cb_export(self, call: InlineCall):
        if not self._keys:
            await call.answer(self.strings["no_keys"], show_alert=True)
            return

        export_data = []
        for entry in self._keys:
            export_data.append({
                "key": entry.get("key"),
                "status": entry.get("status", "unknown"),
                "value": entry.get("value", ""),
                "date": entry.get("date", "-"),
            })

        tmp = tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".json",
            prefix="odi_keys_",
            delete=False,
        )
        json.dump(export_data, tmp, indent=2, ensure_ascii=False)
        tmp.close()

        try:
            await self._client.send_file(
                call.form["chat"],
                tmp.name,
                force_document=True,
                file_name="keys.json",
            )
        except Exception as e:
            logger.exception("send_file failed: %s", e)
        finally:
            os.unlink(tmp.name)

        await call.edit(
            self.strings["export_done"],
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_manage_menu, "style": "danger"}]],
        )

    @loader.command(
        ru_doc="Менеджер API ключей ODIRouter",
        en_doc="ODIRouter API key manager",
    )
    async def odi(self, message: Message):
        """ODIRouter API key manager"""
        await self.inline.form(
            text=self.strings["main_menu"].format(total=len(self._keys)),
            message=message,
            reply_markup=[
                [{"text": self.strings["btn_add"], "callback": self._cb_add_key, "style": "primary"}],
                [{"text": self.strings["btn_manage"], "callback": self._cb_manage_menu, "style": "primary"}],
            ],
            silent=True,
        )