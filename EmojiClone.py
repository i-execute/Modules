# CopyLeft 2026 github.com/i-execute
# Author: I_execute.t.me
# Licensed under AGPLv3.

__version__ = (1, 1, 0)
# meta developer: Execute_forge.t.me

import asyncio
import io
import logging
import os
import random
import re
import sys

from .. import loader, utils
from ..inline.types import InlineCall

DEPS = ["Pillow"]

ADDEMOJI_RE = re.compile(r'^https://t\.me/addemoji/([A-Za-z0-9_]+)$')

PACKS_PER_PAGE = 5


def _install_deps():
    import subprocess

    pip = os.path.join(os.path.dirname(sys.executable), "pip")
    if not os.path.exists(pip):
        pip = "pip"

    for pkg in DEPS:
        try:
            subprocess.run(
                [pip, "install", "-U", pkg, "--break-system-packages", "-q"],
                capture_output=True,
                text=True,
                timeout=120,
            )
        except Exception as e:
            logger.error(f"[EmojiClone] dep install {pkg}: {e}")


logger = logging.getLogger(__name__)


@loader.tds
class EmojiClone(loader.Module):
    """Emoji pack cloner"""

    strings = {
        "name": "EmojiClone",
        "state_menu": (
            "<b>EmojiClone</b>\n"
            "<blockquote>Source pack: {source_status}\n"
            "New pack link: {short_status}\n"
            "Pack name: {name_status}</blockquote>"
        ),
        "btn_set_source": "Source Pack Link",
        "btn_set_short": "New Pack Link",
        "btn_set_name": "Pack Name",
        "btn_start": "Start Copy",
        "btn_back": "Back",
        "btn_close": "Close",
        "btn_retry": "Try Again",
        "input_source": "Send source emoji pack link:",
        "input_short": "Send new pack link - must be free:",
        "input_name": "Send the name for the new emoji pack:",
        "source_set": (
            "<b>Source Pack Set</b>\n"
            "<blockquote>{link}\n"
            "Emoji: {count}</blockquote>"
        ),
        "source_invalid_format": (
            "<b>Invalid Format</b>\n"
            "<blockquote>Link must start with https://t.me/addemoji/\n"
            "Example: https://t.me/addemoji/MyPack</blockquote>"
        ),
        "source_invalid_resolve": (
            "<b>Pack Not Found</b>\n"
            "<blockquote>Could not find emoji pack at this link.\n"
            "Make sure the link is correct and the pack exists.</blockquote>"
        ),
        "short_set": (
            "<b>New Pack Link Set</b>\n"
            "<blockquote>{link}</blockquote>"
        ),
        "short_invalid_format": (
            "<b>Invalid Format</b>\n"
            "<blockquote>Link must start with https://t.me/addemoji/\n"
            "Example: https://t.me/addemoji/MyNewPack</blockquote>"
        ),
        "short_occupied": (
            "<b>Link Already Taken</b>\n"
            "<blockquote>This short name is already in use.\n"
            "Choose a different link.</blockquote>"
        ),
        "name_set": (
            "<b>Pack Name Set</b>\n"
            "<blockquote>{name}</blockquote>"
        ),
        "no_source": (
            "<b>No Source Pack</b>\n"
            "<blockquote>Set the source pack link first</blockquote>"
        ),
        "no_short": (
            "<b>No New Pack Link</b>\n"
            "<blockquote>Set the new pack link first</blockquote>"
        ),
        "no_name": (
            "<b>No Pack Name</b>\n"
            "<blockquote>Set the pack name first</blockquote>"
        ),
        "copying": (
            "<b>Copying</b>\n"
            "<blockquote>Progress: {current}/{total}\n"
            "Pack: {name}{flood}</blockquote>"
        ),
        "done": (
            "<b>Done</b>\n"
            "<blockquote>Pack: <b>{name}</b>\n"
            "Emoji: {count}</blockquote>\n"
            "<blockquote><a href='https://t.me/addemoji/{short}'>Open pack</a></blockquote>"
        ),
        "done_partial": (
            "<b>Done with errors</b>\n"
            "<blockquote>Pack: <b>{name}</b>\n"
            "Copied: {copied}/{total}\n"
            "Failed: {failed}</blockquote>\n"
            "<blockquote><a href='https://t.me/addemoji/{short}'>Open pack</a></blockquote>"
        ),
        "copy_failed": (
            "<b>Copy Failed</b>\n"
            "<blockquote>Could not copy any emoji.\n"
            "Errors: {failed}</blockquote>"
        ),
        "error": (
            "<b>Error</b>\n"
            "<blockquote>{error}</blockquote>"
        ),
        "no_reply_emoji": (
            "<b>No Custom Emoji</b>\n"
            "<blockquote>Reply to a message containing a custom emoji</blockquote>"
        ),
        "status_not_set": "Not set",
        "checking": "Checking...",
        "btn_left": "<",
        "btn_right": ">",
        "eadd_no_packs": (
            "<b>No Packs Found</b>\n"
            "<blockquote>You have not created any emoji packs yet.</blockquote>"
        ),
        "eadd_list": (
            "<b>Eadd - Choose Target Pack</b>\n"
            "<blockquote>Page {page}/{total_pages}\n"
            "Total packs: {total}</blockquote>"
        ),
        "eadd_pack_detail": (
            "<b>Eadd - Pack Selected</b>\n"
            "<blockquote>{title}\n"
            "@{short}\n"
            "Emoji: {count}</blockquote>"
        ),
        "eadd_btn_select": "Add Here",
        "eadd_input_source": "Send source emoji pack link (https://t.me/addemoji/PackName):",
        "eadd_ask_source": (
            "<b>Target Pack: {title}</b>\n"
            "<blockquote>Send the source emoji pack link to resolve emoji from.</blockquote>"
        ),
        "eadd_source_set": (
            "<b>Source Pack Resolved</b>\n"
            "<blockquote>{link}\n"
            "Emoji: {count}</blockquote>\n"
            "Now send the emoji ID to add."
        ),
        "eadd_btn_id": "Emoji ID",
        "eadd_input_id": "Send the numeric emoji document ID from the resolved source pack:",
        "eadd_id_invalid": (
            "<b>Invalid ID</b>\n"
            "<blockquote>Emoji ID must be a number.</blockquote>"
        ),
        "eadd_id_not_found": (
            "<b>Emoji Not Found</b>\n"
            "<blockquote>No emoji with ID {id} in the resolved source pack.</blockquote>"
        ),
        "eadd_adding": (
            "<b>Adding Emoji</b>\n"
            "<blockquote>Please wait...</blockquote>"
        ),
        "eadd_done": (
            "<b>Emoji Added</b>\n"
            "<blockquote>Pack: <b>{title}</b>\n"
            "Emoji ID: {id}</blockquote>\n"
            "<blockquote><a href='https://t.me/addemoji/{short}'>Open pack</a></blockquote>"
        ),
        "eadd_done_reply": (
            "<b>Emoji Added</b>\n"
            "<blockquote>Pack: <b>{title}</b></blockquote>\n"
            "<blockquote><a href='https://t.me/addemoji/{short}'>Open pack</a></blockquote>"
        ),
        "eadd_fail": (
            "<b>Add Failed</b>\n"
            "<blockquote>{error}</blockquote>"
        ),
    }

    strings_ru = {
        "state_menu": (
            "<b>EmojiClone</b>\n"
            "<blockquote>Исходный пак: {source_status}\n"
            "Ссылка нового пака: {short_status}\n"
            "Название пака: {name_status}</blockquote>"
        ),
        "btn_set_source": "Ссылка исходного пака",
        "btn_set_short": "Ссылка нового пака",
        "btn_set_name": "Название пака",
        "btn_start": "Начать копирование",
        "btn_back": "Назад",
        "btn_close": "Закрыть",
        "btn_retry": "Попробовать снова",
        "input_source": "Отправьте ссылку на исходный пак:",
        "input_short": "Отправьте ссылку для нового пака:",
        "input_name": "Отправьте название для нового эмодзи-пака:",
        "source_set": (
            "<b>Исходный пак задан</b>\n"
            "<blockquote>{link}\n"
            "Эмодзи: {count}</blockquote>"
        ),
        "source_invalid_format": (
            "<b>Неверный формат</b>\n"
            "<blockquote>Ссылка должна начинаться с https://t.me/addemoji/\n"
            "Пример: https://t.me/addemoji/MyPack</blockquote>"
        ),
        "source_invalid_resolve": (
            "<b>Пак не найден</b>\n"
            "<blockquote>Не удалось найти эмодзи-пак по этой ссылке.\n"
            "Убедитесь что ссылка правильная и пак существует.</blockquote>"
        ),
        "short_set": (
            "<b>Ссылка нового пака задана</b>\n"
            "<blockquote>{link}</blockquote>"
        ),
        "short_invalid_format": (
            "<b>Неверный формат</b>\n"
            "<blockquote>Ссылка должна начинаться с https://t.me/addemoji/\n"
            "Пример: https://t.me/addemoji/MyNewPack</blockquote>"
        ),
        "short_occupied": (
            "<b>Ссылка занята</b>\n"
            "<blockquote>Это короткое имя уже используется.\n"
            "Выберите другую ссылку.</blockquote>"
        ),
        "name_set": (
            "<b>Название задано</b>\n"
            "<blockquote>{name}</blockquote>"
        ),
        "no_source": (
            "<b>Нет исходного пака</b>\n"
            "<blockquote>Сначала укажите ссылку на исходный пак</blockquote>"
        ),
        "no_short": (
            "<b>Нет ссылки нового пака</b>\n"
            "<blockquote>Сначала укажите ссылку для нового пака</blockquote>"
        ),
        "no_name": (
            "<b>Нет названия</b>\n"
            "<blockquote>Сначала укажите название пака</blockquote>"
        ),
        "copying": (
            "<b>Копирование</b>\n"
            "<blockquote>Прогресс: {current}/{total}\n"
            "Пак: {name}{flood}</blockquote>"
        ),
        "done": (
            "<b>Готово</b>\n"
            "<blockquote>Пак: <b>{name}</b>\n"
            "Эмодзи: {count}</blockquote>\n"
            "<blockquote><a href='https://t.me/addemoji/{short}'>Открыть пак</a></blockquote>"
        ),
        "done_partial": (
            "<b>Готово с ошибками</b>\n"
            "<blockquote>Пак: <b>{name}</b>\n"
            "Скопировано: {copied}/{total}\n"
            "Ошибок: {failed}</blockquote>\n"
            "<blockquote><a href='https://t.me/addemoji/{short}'>Открыть пак</a></blockquote>"
        ),
        "copy_failed": (
            "<b>Ошибка копирования</b>\n"
            "<blockquote>Не удалось скопировать ни одного эмодзи.\n"
            "Ошибок: {failed}</blockquote>"
        ),
        "error": (
            "<b>Ошибка</b>\n"
            "<blockquote>{error}</blockquote>"
        ),
        "no_reply_emoji": (
            "<b>Нет кастомного эмодзи</b>\n"
            "<blockquote>Ответьте на сообщение с кастомным эмодзи</blockquote>"
        ),
        "status_not_set": "Не задано",
        "checking": "Проверяем...",
        "btn_left": "<",
        "btn_right": ">",
        "eadd_no_packs": (
            "<b>Паки не найдены</b>\n"
            "<blockquote>У вас пока нет созданных эмодзи-паков.</blockquote>"
        ),
        "eadd_list": (
            "<b>Eadd - Выбор пака</b>\n"
            "<blockquote>Страница {page}/{total_pages}\n"
            "Всего паков: {total}</blockquote>"
        ),
        "eadd_pack_detail": (
            "<b>Eadd - Пак выбран</b>\n"
            "<blockquote>{title}\n"
            "@{short}\n"
            "Эмодзи: {count}</blockquote>"
        ),
        "eadd_btn_select": "Добавить сюда",
        "eadd_input_source": "Отправьте ссылку на исходный пак (https://t.me/addemoji/PackName):",
        "eadd_ask_source": (
            "<b>Целевой пак: {title}</b>\n"
            "<blockquote>Отправьте ссылку на исходный эмодзи-пак для получения эмодзи.</blockquote>"
        ),
        "eadd_source_set": (
            "<b>Исходный пак получен</b>\n"
            "<blockquote>{link}\n"
            "Эмодзи: {count}</blockquote>\n"
            "Теперь отправьте ID эмодзи для добавления."
        ),
        "eadd_btn_id": "ID эмодзи",
        "eadd_input_id": "Отправьте числовой ID эмодзи из полученного исходного пака:",
        "eadd_id_invalid": (
            "<b>Неверный ID</b>\n"
            "<blockquote>ID эмодзи должен быть числом.</blockquote>"
        ),
        "eadd_id_not_found": (
            "<b>Эмодзи не найден</b>\n"
            "<blockquote>Эмодзи с ID {id} не найден в полученном исходном паке.</blockquote>"
        ),
        "eadd_adding": (
            "<b>Добавление эмодзи</b>\n"
            "<blockquote>Пожалуйста, подождите...</blockquote>"
        ),
        "eadd_done": (
            "<b>Эмодзи добавлен</b>\n"
            "<blockquote>Пак: <b>{title}</b>\n"
            "ID эмодзи: {id}</blockquote>\n"
            "<blockquote><a href='https://t.me/addemoji/{short}'>Открыть пак</a></blockquote>"
        ),
        "eadd_done_reply": (
            "<b>Эмодзи добавлен</b>\n"
            "<blockquote>Пак: <b>{title}</b></blockquote>\n"
            "<blockquote><a href='https://t.me/addemoji/{short}'>Открыть пак</a></blockquote>"
        ),
        "eadd_fail": (
            "<b>Ошибка добавления</b>\n"
            "<blockquote>{error}</blockquote>"
        ),
    }

    def __init__(self):
        self._state = {
            "source_link": None,
            "source_short": None,
            "source_documents": None,
            "new_short": None,
            "name": None,
        }
        self._eadd_state = {
            "packs": [],
            "page": 0,
            "target_short": None,
            "target_title": None,
            "target_count": None,
            "source_link": None,
            "source_documents": None,
            "reply_doc": None,
        }

    async def client_ready(self, client, db):
        self._client = client
        _install_deps()

    def _get_emoji_alt(self, doc) -> str:
        try:
            from telethon.tl.types import DocumentAttributeCustomEmoji, DocumentAttributeSticker
            for attr in doc.attributes:
                if isinstance(attr, (DocumentAttributeCustomEmoji, DocumentAttributeSticker)):
                    return attr.alt or "⭐"
        except Exception:
            pass
        return "⭐"

    def _extract_reply_emoji_doc_id(self, message) -> int | None:
        from telethon.tl.types import MessageEntityCustomEmoji
        if not message:
            return None
        entities = getattr(message, "entities", None) or []
        for ent in entities:
            if isinstance(ent, MessageEntityCustomEmoji):
                return ent.document_id
        return None

    async def _resolve_doc_by_id(self, document_id: int):
        from telethon.tl.functions.messages import GetCustomEmojiDocumentsRequest
        try:
            docs = await self._client(GetCustomEmojiDocumentsRequest(document_id=[document_id]))
            if docs:
                return docs[0]
        except Exception as e:
            logger.error(f"[EmojiClone] _resolve_doc_by_id {document_id}: {e}")
        return None

    async def _with_floodwait(self, coro, call, current, total, name):
        from telethon.errors import FloodWaitError
        while True:
            try:
                return await coro
            except FloodWaitError as e:
                wait = e.seconds
                extra = random.randint(1, 10)
                flood_line = f"\nGot floodwait, waiting {wait} + {extra} seconds"
                try:
                    await call.edit(
                        self.strings["copying"].format(
                            current=current,
                            total=total,
                            name=name,
                            flood=flood_line,
                        )
                    )
                except Exception:
                    pass
                await asyncio.sleep(wait + extra)

    async def _upload_emoji_doc(self, doc, call, current, total, name):
        from telethon.tl.functions.messages import UploadMediaRequest
        from telethon.tl.types import (
            InputPeerSelf,
            InputMediaUploadedDocument,
            DocumentAttributeFilename,
            DocumentAttributeCustomEmoji,
            InputStickerSetEmpty,
            InputDocument,
        )
        try:
            buf = io.BytesIO()
            await self._client.download_file(doc, buf)
            raw = buf.getvalue()
            mime = doc.mime_type or "application/x-tgsticker"
            alt = self._get_emoji_alt(doc)

            if mime == "application/x-tgsticker":
                fname = "emoji.tgs"
            elif mime == "video/webm":
                fname = "emoji.webm"
            else:
                fname = "emoji.webp"

            file_buf = io.BytesIO(raw)
            file_buf.name = fname
            uploaded = await self._client.upload_file(file_buf)

            media = InputMediaUploadedDocument(
                file=uploaded,
                mime_type=mime,
                attributes=[
                    DocumentAttributeFilename(file_name=fname),
                    DocumentAttributeCustomEmoji(
                        alt=alt,
                        stickerset=InputStickerSetEmpty(),
                        free=False,
                        text_color=False,
                    ),
                ],
            )

            result = await self._with_floodwait(
                self._client(UploadMediaRequest(peer=InputPeerSelf(), media=media)),
                call, current, total, name,
            )
            d = result.document
            return InputDocument(d.id, d.access_hash, d.file_reference), alt

        except Exception as e:
            logger.error(f"[EmojiClone] _upload_emoji_doc: {e}")
            return None, None

    async def _try_resolve_pack(self, short_name: str):
        from telethon.tl.functions.messages import GetStickerSetRequest
        from telethon.tl.types import InputStickerSetShortName
        try:
            result = await self._client(GetStickerSetRequest(
                stickerset=InputStickerSetShortName(short_name=short_name),
                hash=random.randint(-2147483647, 2147483647),
            ))
            return result
        except Exception as e:
            logger.info(f"[EmojiClone] _try_resolve_pack '{short_name}': {e}")
            return None

    def _extract_short_name(self, link: str):
        m = ADDEMOJI_RE.match(link.strip())
        if m:
            return m.group(1)
        return None

    def _format_state_menu(self):
        s = self._state
        source_status = s["source_link"] if s["source_link"] else self.strings["status_not_set"]
        short_status = f"https://t.me/addemoji/{s['new_short']}" if s["new_short"] else self.strings["status_not_set"]
        name_status = s["name"] if s["name"] else self.strings["status_not_set"]
        return self.strings["state_menu"].format(
            source_status=source_status,
            short_status=short_status,
            name_status=name_status,
        )

    def _get_state_markup(self):
        return [
            [
                {"text": self.strings["btn_set_source"], "input": self.strings["input_source"], "handler": self._cb_set_source, "style": "primary"},
            ],
            [
                {"text": self.strings["btn_set_short"], "input": self.strings["input_short"], "handler": self._cb_set_short, "style": "primary"},
                {"text": self.strings["btn_set_name"], "input": self.strings["input_name"], "handler": self._cb_set_name, "style": "primary"},
            ],
            [{"text": self.strings["btn_start"], "callback": self._cb_start, "style": "success"}],
            [{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}],
        ]

    def _format_eadd_list(self):
        s = self._eadd_state
        packs = s["packs"]
        page = s["page"]
        total = len(packs)
        total_pages = max(1, (total + PACKS_PER_PAGE - 1) // PACKS_PER_PAGE)
        return self.strings["eadd_list"].format(
            page=page + 1,
            total_pages=total_pages,
            total=total,
        )

    def _get_eadd_list_markup(self):
        s = self._eadd_state
        packs = s["packs"]
        page = s["page"]
        total = len(packs)
        total_pages = max(1, (total + PACKS_PER_PAGE - 1) // PACKS_PER_PAGE)

        start = page * PACKS_PER_PAGE
        end = min(start + PACKS_PER_PAGE, total)
        page_packs = packs[start:end]

        pack_buttons = []
        for i, pack in enumerate(page_packs):
            pack_buttons.append({
                "text": pack.title[:32],
                "callback": self._cb_eadd_pack_btn,
                "args": (start + i,),
                "style": "primary",
            })

        rows = [pack_buttons]

        nav_row = []
        if page > 0:
            nav_row.append({"text": self.strings["btn_left"], "callback": self._cb_eadd_page_left, "style": "primary"})
        if page < total_pages - 1:
            nav_row.append({"text": self.strings["btn_right"], "callback": self._cb_eadd_page_right, "style": "primary"})
        if nav_row:
            rows.append(nav_row)

        rows.append([{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}])
        return rows

    async def _cb_state_menu(self, call: InlineCall):
        await call.edit(self._format_state_menu(), reply_markup=self._get_state_markup())

    async def _cb_close(self, call: InlineCall):
        await call.delete()

    async def _cb_set_source(self, call: InlineCall, query: str):
        link = query.strip()
        short_name = self._extract_short_name(link)

        if not short_name:
            await call.edit(
                self.strings["source_invalid_format"],
                reply_markup=[
                    [{"text": self.strings["btn_retry"], "input": self.strings["input_source"], "handler": self._cb_set_source, "style": "primary"}],
                    [{"text": self.strings["btn_back"], "callback": self._cb_state_menu, "style": "danger"}],
                ],
            )
            return

        await call.edit(self.strings["checking"])
        result = await self._try_resolve_pack(short_name)

        if not result or not result.documents:
            await call.edit(
                self.strings["source_invalid_resolve"],
                reply_markup=[
                    [{"text": self.strings["btn_retry"], "input": self.strings["input_source"], "handler": self._cb_set_source, "style": "primary"}],
                    [{"text": self.strings["btn_back"], "callback": self._cb_state_menu, "style": "danger"}],
                ],
            )
            return

        self._state["source_link"] = link
        self._state["source_short"] = short_name
        self._state["source_documents"] = result.documents

        await call.edit(
            self.strings["source_set"].format(link=link, count=len(result.documents)),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_state_menu, "style": "danger"}]],
        )

    async def _cb_set_short(self, call: InlineCall, query: str):
        link = query.strip()
        short_name = self._extract_short_name(link)

        if not short_name:
            await call.edit(
                self.strings["short_invalid_format"],
                reply_markup=[
                    [{"text": self.strings["btn_retry"], "input": self.strings["input_short"], "handler": self._cb_set_short, "style": "primary"}],
                    [{"text": self.strings["btn_back"], "callback": self._cb_state_menu, "style": "danger"}],
                ],
            )
            return

        await call.edit(self.strings["checking"])
        result = await self._try_resolve_pack(short_name)

        if result is not None:
            await call.edit(
                self.strings["short_occupied"],
                reply_markup=[
                    [{"text": self.strings["btn_retry"], "input": self.strings["input_short"], "handler": self._cb_set_short, "style": "primary"}],
                    [{"text": self.strings["btn_back"], "callback": self._cb_state_menu, "style": "danger"}],
                ],
            )
            return

        self._state["new_short"] = short_name

        await call.edit(
            self.strings["short_set"].format(link=link),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_state_menu, "style": "danger"}]],
        )

    async def _cb_set_name(self, call: InlineCall, query: str):
        name = query.strip()
        self._state["name"] = name if name else None
        await call.edit(
            self.strings["name_set"].format(name=name),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_state_menu, "style": "danger"}]],
        )

    async def _cb_start(self, call: InlineCall):
        if not self._state["source_documents"]:
            await call.edit(
                self.strings["no_source"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_state_menu, "style": "danger"}]],
            )
            return

        if not self._state["new_short"]:
            await call.edit(
                self.strings["no_short"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_state_menu, "style": "danger"}]],
            )
            return

        if not self._state["name"]:
            await call.edit(
                self.strings["no_name"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_state_menu, "style": "danger"}]],
            )
            return

        from telethon.tl.functions.stickers import CreateStickerSetRequest, AddStickerToSetRequest
        from telethon.tl.functions.messages import UninstallStickerSetRequest
        from telethon.tl.types import InputStickerSetShortName, InputStickerSetItem, InputUserSelf
        from telethon.errors.rpcerrorlist import PackShortNameOccupiedError

        documents = self._state["source_documents"]
        pack_title = self._state["name"]
        short_name = self._state["new_short"]
        total = len(documents)

        pack_created = False
        copied = 0
        failed = 0

        for i, doc in enumerate(documents, 1):
            try:
                await call.edit(
                    self.strings["copying"].format(
                        current=i,
                        total=total,
                        name=pack_title,
                        flood="",
                    )
                )
            except Exception:
                pass

            input_doc, alt = await self._upload_emoji_doc(doc, call, i, total, pack_title)
            if input_doc is None:
                failed += 1
                continue

            try:
                if not pack_created:
                    await self._with_floodwait(
                        self._client(CreateStickerSetRequest(
                            user_id=InputUserSelf(),
                            title=pack_title,
                            short_name=short_name,
                            stickers=[InputStickerSetItem(document=input_doc, emoji=alt)],
                            emojis=True,
                        )),
                        call, i, total, pack_title,
                    )
                    pack_created = True
                    copied += 1
                else:
                    await self._with_floodwait(
                        self._client(AddStickerToSetRequest(
                            stickerset=InputStickerSetShortName(short_name=short_name),
                            sticker=InputStickerSetItem(document=input_doc, emoji=alt),
                        )),
                        call, i, total, pack_title,
                    )
                    copied += 1
            except PackShortNameOccupiedError:
                await call.edit(
                    self.strings["short_occupied"],
                    reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_state_menu, "style": "danger"}]],
                )
                return
            except Exception as e:
                logger.error(f"[EmojiClone] add emoji {i}/{total}: {e}")
                failed += 1

        if not pack_created:
            await call.edit(
                self.strings["copy_failed"].format(failed=failed),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_state_menu, "style": "danger"}]],
            )
            return

        try:
            await self._client(UninstallStickerSetRequest(
                stickerset=InputStickerSetShortName(short_name=short_name)
            ))
        except Exception:
            pass

        self._state = {
            "source_link": None,
            "source_short": None,
            "source_documents": None,
            "new_short": None,
            "name": None,
        }

        if failed == 0:
            await call.edit(
                self.strings["done"].format(name=pack_title, count=copied, short=short_name),
                reply_markup=[[{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}]],
            )
        else:
            await call.edit(
                self.strings["done_partial"].format(
                    name=pack_title, copied=copied, total=total, failed=failed, short=short_name
                ),
                reply_markup=[[{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}]],
            )

    async def _cb_eadd_page_left(self, call: InlineCall):
        s = self._eadd_state
        if s["page"] <= 0:
            await call.answer()
            return
        s["page"] -= 1
        await call.edit(self._format_eadd_list(), reply_markup=self._get_eadd_list_markup())

    async def _cb_eadd_page_right(self, call: InlineCall):
        s = self._eadd_state
        total = len(s["packs"])
        total_pages = max(1, (total + PACKS_PER_PAGE - 1) // PACKS_PER_PAGE)
        if s["page"] >= total_pages - 1:
            await call.answer()
            return
        s["page"] += 1
        await call.edit(self._format_eadd_list(), reply_markup=self._get_eadd_list_markup())

    async def _cb_eadd_pack_btn(self, call: InlineCall, pack_index: int):
        s = self._eadd_state
        pack = s["packs"][pack_index]
        await call.edit(
            self.strings["eadd_pack_detail"].format(
                title=pack.title,
                short=pack.short_name,
                count=pack.count,
            ),
            reply_markup=[
                [{"text": self.strings["eadd_btn_select"], "callback": self._cb_eadd_confirm_select, "args": (pack_index,), "style": "success"}],
                [{"text": self.strings["btn_back"], "callback": self._cb_eadd_back_list, "style": "danger"}],
            ],
        )

    async def _cb_eadd_back_list(self, call: InlineCall):
        await call.edit(self._format_eadd_list(), reply_markup=self._get_eadd_list_markup())

    async def _cb_eadd_confirm_select(self, call: InlineCall, pack_index: int):
        s = self._eadd_state
        pack = s["packs"][pack_index]
        s["target_short"] = pack.short_name
        s["target_title"] = pack.title
        s["target_count"] = pack.count
        logger.info(f"[EmojiClone] eadd target selected: {pack.short_name}")

        reply_doc = s.get("reply_doc")
        if reply_doc is not None:
            await self._eadd_do_add_reply(call, reply_doc)
            return

        await call.edit(
            self.strings["eadd_ask_source"].format(title=pack.title),
            reply_markup=[
                [{"text": self.strings["btn_set_source"], "input": self.strings["eadd_input_source"], "handler": self._cb_eadd_set_source, "style": "primary"}],
                [{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}],
            ],
        )

    async def _eadd_do_add_reply(self, call: InlineCall, doc):
        from telethon.tl.functions.stickers import AddStickerToSetRequest
        from telethon.tl.types import InputStickerSetShortName, InputStickerSetItem, InputDocument
        from telethon.errors import FloodWaitError

        s = self._eadd_state
        target_short = s["target_short"]
        target_title = s["target_title"]
        alt = self._get_emoji_alt(doc)

        await call.edit(self.strings["eadd_adding"])

        try:
            input_doc, alt = await self._upload_emoji_doc(doc, call, 1, 1, target_title)
            if input_doc is None:
                raise Exception("upload failed")

            await self._client(AddStickerToSetRequest(
                stickerset=InputStickerSetShortName(short_name=target_short),
                sticker=InputStickerSetItem(
                    document=input_doc,
                    emoji=alt,
                ),
            ))
        except FloodWaitError as e:
            logger.info(f"[EmojiClone] eadd reply FloodWait {e.seconds}s")
            await call.edit(
                self.strings["eadd_fail"].format(error=f"FloodWait {e.seconds}s"),
                reply_markup=[[{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}]],
            )
            return
        except Exception as e:
            logger.error(f"[EmojiClone] eadd reply add error: {e}")
            await call.edit(
                self.strings["eadd_fail"].format(error=str(e)),
                reply_markup=[[{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}]],
            )
            return

        logger.info(f"[EmojiClone] eadd reply: added emoji {doc.id} to {target_short}")
        s["reply_doc"] = None

        await call.edit(
            self.strings["eadd_done_reply"].format(title=target_title, short=target_short),
            reply_markup=[[{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}]],
        )

    async def _cb_eadd_set_source(self, call: InlineCall, query: str):
        link = query.strip()
        short_name = self._extract_short_name(link)

        if not short_name:
            await call.edit(
                self.strings["source_invalid_format"],
                reply_markup=[
                    [{"text": self.strings["btn_retry"], "input": self.strings["eadd_input_source"], "handler": self._cb_eadd_set_source, "style": "primary"}],
                    [{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}],
                ],
            )
            return

        await call.edit(self.strings["checking"])
        result = await self._try_resolve_pack(short_name)

        if not result or not result.documents:
            await call.edit(
                self.strings["source_invalid_resolve"],
                reply_markup=[
                    [{"text": self.strings["btn_retry"], "input": self.strings["eadd_input_source"], "handler": self._cb_eadd_set_source, "style": "primary"}],
                    [{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}],
                ],
            )
            return

        s = self._eadd_state
        s["source_link"] = link
        s["source_documents"] = {doc.id: doc for doc in result.documents}
        logger.info(f"[EmojiClone] eadd source resolved: {short_name}, {len(result.documents)} emoji")

        await call.edit(
            self.strings["eadd_source_set"].format(link=link, count=len(result.documents)),
            reply_markup=[
                [{"text": self.strings["eadd_btn_id"], "input": self.strings["eadd_input_id"], "handler": self._cb_eadd_set_id, "style": "primary"}],
                [{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}],
            ],
        )

    async def _cb_eadd_set_id(self, call: InlineCall, query: str):
        s = self._eadd_state
        raw_id = query.strip()

        try:
            emoji_id = int(raw_id)
        except ValueError:
            await call.edit(
                self.strings["eadd_id_invalid"],
                reply_markup=[
                    [{"text": self.strings["btn_retry"], "input": self.strings["eadd_input_id"], "handler": self._cb_eadd_set_id, "style": "primary"}],
                    [{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}],
                ],
            )
            return

        doc = (s["source_documents"] or {}).get(emoji_id)
        if not doc:
            await call.edit(
                self.strings["eadd_id_not_found"].format(id=emoji_id),
                reply_markup=[
                    [{"text": self.strings["btn_retry"], "input": self.strings["eadd_input_id"], "handler": self._cb_eadd_set_id, "style": "primary"}],
                    [{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}],
                ],
            )
            return

        await call.edit(self.strings["eadd_adding"])

        from telethon.tl.functions.stickers import AddStickerToSetRequest
        from telethon.tl.types import InputStickerSetShortName, InputStickerSetItem
        from telethon.errors import FloodWaitError

        target_short = s["target_short"]

        try:
            input_doc, alt = await self._upload_emoji_doc(doc, call, 1, 1, s["target_title"])
            if input_doc is None:
                raise Exception("upload failed")

            await self._client(AddStickerToSetRequest(
                stickerset=InputStickerSetShortName(short_name=target_short),
                sticker=InputStickerSetItem(document=input_doc, emoji=alt),
            ))
        except FloodWaitError as e:
            logger.info(f"[EmojiClone] eadd FloodWait {e.seconds}s")
            await call.edit(
                self.strings["eadd_fail"].format(error=f"FloodWait {e.seconds}s"),
                reply_markup=[[{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}]],
            )
            return
        except Exception as e:
            logger.error(f"[EmojiClone] eadd add error: {e}")
            await call.edit(
                self.strings["eadd_fail"].format(error=str(e)),
                reply_markup=[[{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}]],
            )
            return

        logger.info(f"[EmojiClone] eadd: added emoji {emoji_id} to {target_short}")

        await call.edit(
            self.strings["eadd_done"].format(title=s["target_title"], id=emoji_id, short=target_short),
            reply_markup=[[{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}]],
        )

    @loader.command(
        ru_doc="Открыть меню клонирования эмодзи-пака",
        en_doc="Open emoji pack cloner menu",
    )
    async def eclone(self, message):
        """Open emoji pack cloner menu"""
        await self.inline.form(
            text=self._format_state_menu(),
            message=message,
            reply_markup=self._get_state_markup(),
            silent=True,
        )

    @loader.command(
        ru_doc="Добавить эмодзи в свой пак - без реплая открывает меню выбора пака и исходника, в реплай на эмодзи добавляет напрямую",
        en_doc="Add emoji to your own pack - without reply opens pack and source menu, reply to a custom emoji adds it directly",
    )
    async def eadd(self, message):
        """Add emoji to your own pack - without reply opens pack and source menu, reply to a custom emoji adds it directly"""
        from telethon.tl.functions.messages import GetMyStickersRequest

        reply = await message.get_reply_message()
        reply_doc = None

        if reply:
            doc_id = self._extract_reply_emoji_doc_id(reply)
            if doc_id is not None:
                reply_doc = await self._resolve_doc_by_id(doc_id)
                if reply_doc:
                    logger.info(f"[EmojiClone] eadd reply emoji doc_id={doc_id} resolved")
                else:
                    logger.warning(f"[EmojiClone] eadd reply emoji doc_id={doc_id} resolve failed")

        result = await self._client(GetMyStickersRequest(offset_id=0, limit=100))
        packs = [item.set for item in result.sets if getattr(item.set, "emojis", False)]
        logger.info(f"[EmojiClone] eadd: {len(packs)} own emoji packs found, reply_doc={'yes' if reply_doc else 'no'}")

        self._eadd_state = {
            "packs": packs,
            "page": 0,
            "target_short": None,
            "target_title": None,
            "target_count": None,
            "source_link": None,
            "source_documents": None,
            "reply_doc": reply_doc,
        }

        if not packs:
            await self.inline.form(
                text=self.strings["eadd_no_packs"],
                message=message,
                reply_markup=[[{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}]],
                silent=True,
            )
            return

        await self.inline.form(
            text=self._format_eadd_list(),
            message=message,
            reply_markup=self._get_eadd_list_markup(),
            silent=True,
        )