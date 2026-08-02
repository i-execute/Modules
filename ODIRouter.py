__version__ = (1, 0, 0)
# meta developer: I_execute.t.me

import logging
import json
import tempfile
import os

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
                        return {"valid": True, "status": "rate_limited", "message": msg}
                    elif code == "upstream_rejected":
                        return {"valid": False, "status": "upstream_rejected", "message": msg}
                    else:
                        return {"valid": False, "status": "error", "message": msg}

                if "choices" in data:
                    return {"valid": True, "status": "ok", "message": ""}

                return {"valid": False, "status": "unknown", "message": str(data)[:100]}

    except Exception as e:
        return {"valid": False, "status": "exception", "message": str(e)[:100]}


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

        "validating": (
            "<b>Validating key...</b>"
        ),

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

        "keys_list": (
            "<b>Keys</b>\n"
            "<blockquote>{keys}</blockquote>"
        ),

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

        "btn_add": "Add Key",
        "btn_manage": "Manage",
        "btn_back": "Back",
        "btn_save": "Save",
        "btn_validate_all": "Validate All",
        "btn_export": "Export",
        "btn_list": "List Keys",

        "input_key": "Enter API key:",

        "status_ok": "Working",
        "status_rate_limited": "Valid (rate limited)",
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

        "validating": (
            "<b>Проверка ключа...</b>"
        ),

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

        "keys_list": (
            "<b>Ключи</b>\n"
            "<blockquote>{keys}</blockquote>"
        ),

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

        "btn_add": "Добавить ключ",
        "btn_manage": "Управление",
        "btn_back": "Назад",
        "btn_save": "Сохранить",
        "btn_validate_all": "Проверить все",
        "btn_export": "Экспорт",
        "btn_list": "Список ключей",

        "input_key": "Введите API ключ:",

        "status_ok": "Работает",
        "status_rate_limited": "Валиден (лимит запросов)",
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
        self._keys = self._db.get("ODIRouter", "keys", [])

    def _save_keys(self):
        self._db.set("ODIRouter", "keys", self._keys)

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
        await call.edit(self.strings["validating"], reply_markup=[])

        result = await _validate_key(key, self.config["model"])

        if result["valid"]:
            if result["status"] == "rate_limited":
                status_str = self.strings["status_rate_limited"]
            else:
                status_str = self.strings["status_ok"]

            text = self.strings["key_valid"].format(
                key=_escape(key),
                status=status_str,
            )

            markup = [
                [{"text": self.strings["btn_save"], "callback": self._cb_save_key, "args": (key,), "style": "success"}],
                [{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}],
            ]
        else:
            text = self.strings["key_invalid"].format(
                key=_escape(key),
                reason=_escape(result["message"]),
            )

            markup = [[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]]

        await call.edit(text, reply_markup=markup)

    async def _cb_save_key(self, call: InlineCall, key: str):
        if key not in self._keys:
            self._keys.append(key)
            self._save_keys()

        await call.edit(
            self.strings["key_saved"].format(key=_escape(key)),
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
                [{"text": self.strings["btn_export"], "callback": self._cb_export, "style": "primary"}],
                [{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "primary"}],
            ],
        )

    async def _cb_list_keys(self, call: InlineCall):
        if not self._keys:
            await call.edit(
                self.strings["no_keys"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_manage_menu, "style": "primary"}]],
            )
            return

        lines = []
        for i, key in enumerate(self._keys, 1):
            masked = key[:8] + "..." + key[-4:] if len(key) > 12 else key
            lines.append(f"{i}. <code>{_escape(masked)}</code>")

        await call.edit(
            self.strings["keys_list"].format(keys="\n".join(lines)),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_manage_menu, "style": "primary"}]],
        )

    async def _cb_validate_all(self, call: InlineCall):
        await call.edit(self.strings["validate_all_running"], reply_markup=[])

        import asyncio
        tasks = [_validate_key(k, self.config["model"]) for k in self._keys]
        results = await asyncio.gather(*tasks)

        valid = sum(1 for r in results if r["valid"] and r["status"] == "ok")
        rate_limited = sum(1 for r in results if r["valid"] and r["status"] == "rate_limited")
        invalid = sum(1 for r in results if not r["valid"])

        await call.edit(
            self.strings["validate_all_results"].format(
                total=len(self._keys),
                valid=valid,
                rate_limited=rate_limited,
                invalid=invalid,
            ),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_manage_menu, "style": "primary"}]],
        )

    async def _cb_export(self, call: InlineCall):
        if not self._keys:
            await call.answer(self.strings["no_keys"], show_alert=True)
            return

        export_data = [{"key": k} for k in self._keys]

        tmp = tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".json",
            prefix="odi_keys_",
            delete=False,
        )
        json.dump(export_data, tmp, indent=2)
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
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_manage_menu, "style": "primary"}]],
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