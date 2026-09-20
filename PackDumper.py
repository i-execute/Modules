# CopyLeft 2026 github.com/i-execute
# Author: I_execute.t.me
# Licensed under AGPLv3.

__version__ = (1, 1, 0)
# meta developer: Execute_forge.t.me

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


def _escape(t: str) -> str:
    return (t or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _fmt_val(v) -> str:
    if v is None:
        return "<i>null</i>"
    if isinstance(v, bool):
        return f"<code>{'true' if v else 'false'}</code>"
    if isinstance(v, (int, float)):
        return f"<code>{v}</code>"
    if isinstance(v, bytes):
        return f"<code>{v.hex()}</code>"
    return f"<code>{_escape(str(v))}</code>"


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
        "input_url": "Paste pack link:",
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
        "no_reply": (
            "<b>Error</b>\n"
            "<blockquote>Reply to a message</blockquote>"
        ),
        "rd_title": "<b>Message Dump</b>",
        "rd_btn_close": "Close",
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
        "input_url": "Вставьте ссылку на пак:",
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
        "no_reply": (
            "<b>Ошибка</b>\n"
            "<blockquote>Ответьте на сообщение</blockquote>"
        ),
        "rd_title": "<b>Дамп сообщения</b>",
        "rd_btn_close": "Закрыть",
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
            items.append({
                "id": doc.id,
                "access_hash": doc.access_hash,
                "file_reference": doc.file_reference.hex(),
                "mime_type": doc.mime_type or "",
                "size": doc.size,
                "alt": alt,
            })
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

    def _build_rd_text(self, reply) -> str:
        from telethon.tl.types import (
            MessageMediaDocument,
            MessageMediaPhoto,
            DocumentAttributeSticker,
            DocumentAttributeCustomEmoji,
            DocumentAttributeAnimated,
            DocumentAttributeVideo,
            InputStickerSetID,
            InputStickerSetShortName,
            InputStickerSetEmpty,
        )

        inner = []

        from_id = getattr(reply, "sender_id", None) or getattr(reply, "from_id", None)
        date_str = str(reply.date) if reply.date else "N/A"
        inner.append(
            "<b>Message</b>\n"
            f"ID: {reply.id}\n"
            f"Date: {_escape(date_str)}\n"
            f"From: {_fmt_val(from_id)}"
        )

        text = reply.message or ""
        if text.strip():
            inner.append(f"<b>Text</b>\n{_escape(text[:512])}")
        else:
            inner.append("<b>Text</b>\n<i>empty</i>")

        media = reply.media
        if media is None:
            inner.append("<b>Media</b>\n<i>none</i>")
        else:
            media_type = type(media).__name__
            media_block = [f"<b>Media</b>\nType: {_escape(media_type)}\n"]

            if isinstance(media, MessageMediaDocument):
                doc = media.document
                if doc:
                    media_block.append(
                        f"<b>Document</b>\n"
                        f"ID: {doc.id}\n"
                        f"Access hash: {doc.access_hash}\n"
                        f"File ref: {doc.file_reference.hex() if doc.file_reference else ''}\n"
                        f"MIME: {_escape(doc.mime_type or '')}\n"
                        f"Size: {doc.size} bytes"
                    )

                    is_sticker = False
                    is_custom_emoji = False
                    is_animated = False
                    sticker_alt = ""
                    sticker_set = None

                    for attr in doc.attributes:
                        if isinstance(attr, DocumentAttributeSticker):
                            is_sticker = True
                            sticker_alt = attr.alt or ""
                            sticker_set = attr.stickerset
                        elif isinstance(attr, DocumentAttributeCustomEmoji):
                            is_custom_emoji = True
                            sticker_alt = attr.alt or ""
                            sticker_set = attr.stickerset
                        elif isinstance(attr, DocumentAttributeAnimated):
                            is_animated = True

                    pack_short = ""
                    pack_id = ""
                    if sticker_set and not isinstance(sticker_set, InputStickerSetEmpty):
                        if isinstance(sticker_set, InputStickerSetShortName):
                            pack_short = sticker_set.short_name
                        elif isinstance(sticker_set, InputStickerSetID):
                            pack_id = str(sticker_set.id)

                    if is_sticker:
                        media_block.append(
                            f"\n<b>Sticker</b>\n"
                            f"Alt: {_escape(sticker_alt)}\n"
                            f"Pack short: {_escape(pack_short) or '<i>N/A</i>'}\n"
                            f"Pack ID: {_escape(pack_id) or '<i>N/A</i>'}"
                        )
                    elif is_custom_emoji:
                        media_block.append(
                            f"\n<b>Custom Emoji</b>\n"
                            f"Alt: {_escape(sticker_alt)}\n"
                            f"Pack short: {_escape(pack_short) or '<i>N/A</i>'}\n"
                            f"Pack ID: {_escape(pack_id) or '<i>N/A</i>'}"
                        )
                    elif is_animated or (doc.mime_type in ("image/gif", "video/mp4") and not is_sticker):
                        media_block.append(
                            f"\n<b>GIF / Animation</b>\n"
                            f"ID: {doc.id}\n"
                            f"MIME: {_escape(doc.mime_type or '')}\n"
                            f"Size: {doc.size} bytes"
                        )

            elif isinstance(media, MessageMediaPhoto):
                photo = media.photo
                if photo:
                    media_block.append(
                        f"<b>Photo</b>\n"
                        f"ID: {photo.id}\n"
                        f"Access hash: {photo.access_hash}\n"
                        f"File ref: {photo.file_reference.hex() if photo.file_reference else ''}"
                    )

            inner.append("".join(media_block))

        entities = reply.entities or []
        ent_lines = ["<b>Entities</b>"]
        if not entities:
            ent_lines.append("<i>none</i>")
        else:
            for idx, ent in enumerate(entities, 1):
                etype = type(ent).__name__
                extra = ""
                if hasattr(ent, "url") and ent.url:
                    extra = f"\nurl={_escape(ent.url)}"
                elif hasattr(ent, "language") and ent.language:
                    extra = f"\nlang={_escape(ent.language)}"
                elif hasattr(ent, "document_id"):
                    extra = f"\ndoc_id={ent.document_id}"
                ent_lines.append(
                    f"{idx}. {_escape(etype)}: offset={ent.offset} length={ent.length}{extra}"
                )
        inner.append("\n".join(ent_lines))

        body = "\n\n".join(inner)
        return (
            self.strings["rd_title"] + "\n"
            "<blockquote expandable>" + body + "</blockquote>"
        )

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

            json_path = os.path.join(tempfile.gettempdir(), f"{short_name}.json")
            if os.path.exists(json_path):
                try:
                    os.unlink(json_path)
                except Exception:
                    pass

            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(dump, f, indent=2, ensure_ascii=False)

            try:
                await self._client.send_file(
                    call.form["chat"],
                    json_path,
                    force_document=True,
                    file_name=f"{short_name}.json",
                )
            finally:
                try:
                    os.unlink(json_path)
                except Exception:
                    pass

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
        ru_doc="Открыть меню дампа",
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

    @loader.command(
        ru_doc="в реплай - дамп сообщения в инлайн форму",
        en_doc="reply dump - message to inline form",
    )
    async def rd(self, message):
        """reply dump - message to inline form"""
        reply = await message.get_reply_message()
        if not reply:
            await utils.answer(message, self.strings["no_reply"])
            return

        try:
            text = self._build_rd_text(reply)
        except Exception as e:
            logger.exception("[PackDumper] rd build error")
            await utils.answer(message, self.strings["error"].format(error=str(e)))
            return

        await self.inline.form(
            text=text,
            message=message,
            reply_markup=[
                [{"text": self.strings["rd_btn_close"], "callback": self._cb_close, "style": "danger"}],
            ],
            silent=True,
        )