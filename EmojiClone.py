__version__ = (1, 0, 0)
# meta developer: I_execute.t.me

import asyncio
import io
import logging
import os
import random
import re
import sys
import tempfile

from .. import loader, utils
from ..inline.types import InlineCall

DEPS = ["Pillow"]

ADDEMOJI_RE = re.compile(r'^https://t\.me/addemoji/([A-Za-z0-9_]+)$')


def _install_deps():
    import importlib
    import subprocess

    pip = os.path.join(os.path.dirname(sys.executable), "pip")
    if not os.path.exists(pip):
        pip = "pip"

    imp_map = {"Pillow": "PIL"}

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
        "status_not_set": "Not set",
        "checking": "Checking...",
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
        "status_not_set": "Не задано",
        "checking": "Проверяем...",
    }

    def __init__(self):
        self._state = {
            "source_link": None,
            "source_short": None,
            "source_documents": None,
            "new_short": None,
            "name": None,
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