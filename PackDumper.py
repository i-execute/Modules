__version__ = (1, 0, 0)
# meta developer: I_execute.t.me

import json
import logging
import os
import random
import re
import tempfile

from .. import loader, utils
from ..inline.types import InlineCall

logger = logging.getLogger(__name__)

ADDSTICKERS_RE = re.compile(r'^https://t\.me/addstickers/([A-Za-z0-9_]+)$')
ADDEMOJI_RE = re.compile(r'^https://t\.me/addemoji/([A-Za-z0-9_]+)$')


@loader.tds
class PackDumper(loader.Module):
    """Sticker and emoji pack dumper to JSON"""

    strings = {
        "name": "PackDumper",
        "main_menu": (
            "<b>PackDumper</b>\n"
            "<blockquote>Dump sticker or emoji pack to JSON.\n"
            "Supports t.me/addstickers/ and t.me/addemoji/</blockquote>"
        ),
        "btn_enter_url": "Enter URL",
        "btn_close": "Close",
        "input_url": "Send pack link (https://t.me/addstickers/... or https://t.me/addemoji/...):",
        "resolving": (
            "<b>Resolving</b>\n"
            "<blockquote>Fetching pack info...</blockquote>"
        ),
        "invalid_url": (
            "<b>Invalid URL</b>\n"
            "<blockquote>Link must start with:\n"
            "https://t.me/addstickers/\n"
            "or\n"
            "https://t.me/addemoji/</blockquote>"
        ),
        "not_found": (
            "<b>Pack Not Found</b>\n"
            "<blockquote>Could not resolve pack at this link.\n"
            "Make sure the link is correct and the pack exists.</blockquote>"
        ),
        "done": (
            "<b>Done</b>\n"
            "<blockquote>Pack: <b>{title}</b>\n"
            "Type: {pack_type}\n"
            "Items: {count}</blockquote>"
        ),
        "error": (
            "<b>Error</b>\n"
            "<blockquote>{error}</blockquote>"
        ),
        "btn_back": "Back",
        "type_stickers": "Stickers",
        "type_emoji": "Emoji",
    }

    strings_ru = {
        "main_menu": (
            "<b>PackDumper</b>\n"
            "<blockquote>Дамп стикерпака или эмодзи-пака в JSON.\n"
            "Поддерживает t.me/addstickers/ и t.me/addemoji/</blockquote>"
        ),
        "btn_enter_url": "Ввести ссылку",
        "btn_close": "Закрыть",
        "input_url": "Отправьте ссылку на пак (https://t.me/addstickers/... или https://t.me/addemoji/...):",
        "resolving": (
            "<b>Загружаем</b>\n"
            "<blockquote>Получаем информацию о паке...</blockquote>"
        ),
        "invalid_url": (
            "<b>Неверная ссылка</b>\n"
            "<blockquote>Ссылка должна начинаться с:\n"
            "https://t.me/addstickers/\n"
            "или\n"
            "https://t.me/addemoji/</blockquote>"
        ),
        "not_found": (
            "<b>Пак не найден</b>\n"
            "<blockquote>Не удалось найти пак по этой ссылке.\n"
            "Убедитесь что ссылка правильная и пак существует.</blockquote>"
        ),
        "done": (
            "<b>Готово</b>\n"
            "<blockquote>Пак: <b>{title}</b>\n"
            "Тип: {pack_type}\n"
            "Элементов: {count}</blockquote>"
        ),
        "error": (
            "<b>Ошибка</b>\n"
            "<blockquote>{error}</blockquote>"
        ),
        "btn_back": "Назад",
        "type_stickers": "Стикеры",
        "type_emoji": "Эмодзи",
    }

    async def client_ready(self, client, db):
        self._client = client

    def _parse_url(self, url: str):
        url = url.strip()
        m = ADDSTICKERS_RE.match(url)
        if m:
            return m.group(1), "stickers"
        m = ADDEMOJI_RE.match(url)
        if m:
            return m.group(1), "emoji"
        return None, None

    def _get_alt(self, doc) -> str:
        try:
            from telethon.tl.types import DocumentAttributeCustomEmoji, DocumentAttributeSticker
            for attr in doc.attributes:
                if isinstance(attr, (DocumentAttributeCustomEmoji, DocumentAttributeSticker)):
                    return attr.alt or ""
        except Exception:
            pass
        return ""

    async def _resolve_pack(self, short_name: str):
        from telethon.tl.functions.messages import GetStickerSetRequest
        from telethon.tl.types import InputStickerSetShortName
        try:
            result = await self._client(GetStickerSetRequest(
                stickerset=InputStickerSetShortName(short_name=short_name),
                hash=random.randint(-2147483647, 2147483647),
            ))
            return result
        except Exception as e:
            logger.info(f"[PackDumper] _resolve_pack '{short_name}': {e}")
            return None

    def _build_dump(self, result, pack_type: str, short_name: str) -> dict:
        pack_set = result.set
        documents = result.documents

        items = []
        for doc in documents:
            alt = self._get_alt(doc)
            item = {
                "id": doc.id,
                "access_hash": doc.access_hash,
                "file_reference": doc.file_reference.hex(),
                "mime_type": doc.mime_type or "",
                "size": doc.size,
                "alt": alt,
            }
            items.append(item)

        return {
            "pack": {
                "id": pack_set.id,
                "access_hash": pack_set.access_hash,
                "title": pack_set.title,
                "short_name": pack_set.short_name,
                "count": pack_set.count,
                "type": pack_type,
                "link": (
                    f"https://t.me/addstickers/{short_name}"
                    if pack_type == "stickers"
                    else f"https://t.me/addemoji/{short_name}"
                ),
                "animated": getattr(pack_set, "animated", False),
                "videos": getattr(pack_set, "videos", False),
                "emojis": getattr(pack_set, "emojis", False),
            },
            "items": items,
        }

    async def _cb_close(self, call: InlineCall):
        await call.delete()

    async def _cb_main_menu(self, call: InlineCall):
        await call.edit(
            self.strings["main_menu"],
            reply_markup=[
                [{"text": self.strings["btn_enter_url"], "input": self.strings["input_url"], "handler": self._cb_got_url, "style": "primary"}],
                [{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}],
            ],
        )

    async def _cb_got_url(self, call: InlineCall, query: str):
        short_name, pack_type = self._parse_url(query)

        if not short_name:
            await call.edit(
                self.strings["invalid_url"],
                reply_markup=[
                    [{"text": self.strings["btn_enter_url"], "input": self.strings["input_url"], "handler": self._cb_got_url, "style": "primary"}],
                    [{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}],
                ],
            )
            return

        await call.edit(self.strings["resolving"], reply_markup=[])

        try:
            result = await self._resolve_pack(short_name)
        except Exception as e:
            await call.edit(
                self.strings["error"].format(error=str(e)),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]],
            )
            return

        if not result or not result.documents:
            await call.edit(
                self.strings["not_found"],
                reply_markup=[
                    [{"text": self.strings["btn_enter_url"], "input": self.strings["input_url"], "handler": self._cb_got_url, "style": "primary"}],
                    [{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}],
                ],
            )
            return

        try:
            dump = self._build_dump(result, pack_type, short_name)

            title = dump["pack"]["title"]
            count = len(dump["items"])
            pack_type_label = (
                self.strings["type_emoji"]
                if pack_type == "emoji"
                else self.strings["type_stickers"]
            )

            tmp = tempfile.NamedTemporaryFile(
                mode="w",
                suffix=".json",
                prefix=f"pack_dump_{short_name}_",
                delete=False,
            )
            json.dump(dump, tmp, indent=2, ensure_ascii=False)
            tmp.close()

            try:
                await self._client.send_file(
                    call.form["chat"],
                    tmp.name,
                    force_document=True,
                    file_name=f"{short_name}.json",
                )
            finally:
                os.unlink(tmp.name)

            await call.edit(
                self.strings["done"].format(
                    title=title,
                    pack_type=pack_type_label,
                    count=count,
                ),
                reply_markup=[
                    [{"text": self.strings["btn_enter_url"], "input": self.strings["input_url"], "handler": self._cb_got_url, "style": "primary"}],
                    [{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}],
                ],
            )

        except Exception as e:
            logger.exception("[PackDumper] build/send error")
            await call.edit(
                self.strings["error"].format(error=str(e)),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]],
            )

    @loader.command(
        ru_doc="Открыть меню дампа стикер/эмодзи пака",
        en_doc="Open pack dumper menu",
    )
    async def pd(self, message):
        """Open pack dumper menu"""
        await self.inline.form(
            text=self.strings["main_menu"],
            message=message,
            reply_markup=[
                [{"text": self.strings["btn_enter_url"], "input": self.strings["input_url"], "handler": self._cb_got_url, "style": "primary"}],
                [{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}],
            ],
            silent=True,
        )