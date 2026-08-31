__version__ = (1, 2, 0)
# meta developer: I_execute.t.me froked from @elisartix

import asyncio
import io
import os
import re
import random
import string
import logging
import tempfile
import sys

from .. import loader, utils
from ..inline.types import InlineCall

DEPS = ["Pillow"]

ADDSTICKERS_RE = re.compile(r'^https://t\.me/addstickers/([A-Za-z0-9_]+)$')


def _install_deps():
    import importlib
    import subprocess

    pip = os.path.join(os.path.dirname(sys.executable), "pip")
    if not os.path.exists(pip):
        pip = "pip"

    in_venv = sys.prefix != sys.base_prefix
    imp_map = {"Pillow": "PIL"}
    lines = [f"venv: {'yes' if in_venv else 'no'} ({sys.prefix})"]

    for pkg in DEPS:
        try:
            subprocess.run(
                [pip, "install", "-U", pkg, "--break-system-packages", "-q"],
                capture_output=True,
                text=True,
                timeout=120,
            )
            try:
                imp_name = imp_map.get(pkg, pkg)
                mod = importlib.import_module(imp_name)
                ver = getattr(mod, "__version__", "?")
                lines.append(f"{pkg}: OK ({ver})")
            except ImportError:
                lines.append(f"{pkg}: FAIL (import error)")
        except Exception as e:
            lines.append(f"{pkg}: FAIL ({e})")
    return lines


logger = logging.getLogger(__name__)


@loader.tds
class StickerClone(loader.Module):
    """Sticker pack cloner"""

    strings = {
        "name": "Stickerclone",
        "state_menu": (
            "<b>Stickerclone</b>\n"
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
        "input_source": "Send source pack link:",
        "input_short": "Send new pack link - must be free:",
        "input_name": "Send the name for the new sticker pack:",
        "source_set": (
            "<b>Source Pack Set</b>\n"
            "<blockquote>{link}\n"
            "Stickers: {count}</blockquote>"
        ),
        "source_invalid_format": (
            "<b>Invalid Format</b>\n"
            "<blockquote>Link must start with https://t.me/addstickers/\n"
            "Example: https://t.me/addstickers/Journey_of_Elaina\n"
            "This pack was not found - try a different link.</blockquote>"
        ),
        "source_invalid_resolve": (
            "<b>Pack Not Found</b>\n"
            "<blockquote>Could not find sticker pack at this link.\n"
            "Make sure the link is correct and the pack exists.\n"
            "Example: https://t.me/addstickers/Journey_of_Elaina</blockquote>"
        ),
        "short_set": (
            "<b>New Pack Link Set</b>\n"
            "<blockquote>{link}</blockquote>"
        ),
        "short_invalid_format": (
            "<b>Invalid Format</b>\n"
            "<blockquote>Link must start with https://t.me/addstickers/\n"
            "Example: https://t.me/addstickers/MyNewPack</blockquote>"
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
        "fetching": (
            "<b>Fetching Pack</b>\n"
            "<blockquote>Getting sticker pack info...</blockquote>"
        ),
        "pack_empty": (
            "<b>Empty Pack</b>\n"
            "<blockquote>This sticker pack has no stickers</blockquote>"
        ),
        "copying": (
            "<b>Copying</b>\n"
            "<blockquote>Progress: {current}/{total}\n"
            "Pack: {name}</blockquote>"
        ),
        "done": (
            "<b>Done</b>\n"
            "<blockquote>Pack: <b>{name}</b>\n"
            "Stickers: {count}</blockquote>\n"
            "<blockquote><a href='https://t.me/addstickers/{short}'>Open pack</a></blockquote>"
        ),
        "done_partial": (
            "<b>Done with errors</b>\n"
            "<blockquote>Pack: <b>{name}</b>\n"
            "Copied: {copied}/{total}\n"
            "Failed: {failed}</blockquote>\n"
            "<blockquote><a href='https://t.me/addstickers/{short}'>Open pack</a></blockquote>"
        ),
        "copy_failed": (
            "<b>Copy Failed</b>\n"
            "<blockquote>Could not copy any sticker.\n"
            "Errors: {failed}</blockquote>"
        ),
        "error": (
            "<b>Error</b>\n"
            "<blockquote>{error}</blockquote>"
        ),
        "status_set": "Set",
        "status_not_set": "Not set",
        "checking": "Checking...",
        "btn_left": "⬅️",
        "btn_right": "➡️",
        "sadd_no_packs": (
            "<b>No Packs Found</b>\n"
            "<blockquote>You have not created any sticker packs yet.</blockquote>"
        ),
        "sadd_fetching_packs": (
            "<b>Fetching Packs</b>\n"
            "<blockquote>Getting your sticker packs...</blockquote>"
        ),
        "sadd_pack_item": (
            "<b>Sadd - Choose Target Pack</b>\n"
            "<blockquote>{title}\n"
            "@{short}\n"
            "Stickers: {count}</blockquote>\n"
            "Pack {index}/{total}"
        ),
        "sadd_btn_select": "Add Here",
        "sadd_ask_source": (
            "<b>Target Pack: {title}</b>\n"
            "<blockquote>Now send the source pack link to resolve stickers from.</blockquote>"
        ),
        "sadd_input_source": "Send source pack link (https://t.me/addstickers/PackName):",
        "sadd_source_set": (
            "<b>Source Pack Resolved</b>\n"
            "<blockquote>{link}\n"
            "Stickers: {count}</blockquote>\n"
            "Now send the sticker ID to add."
        ),
        "sadd_btn_id": "Sticker ID",
        "sadd_input_id": "Send the numeric sticker ID from the resolved source pack:",
        "sadd_id_invalid": (
            "<b>Invalid ID</b>\n"
            "<blockquote>Sticker ID must be a number.</blockquote>"
        ),
        "sadd_id_not_found": (
            "<b>Sticker Not Found</b>\n"
            "<blockquote>No sticker with ID {id} in the resolved source pack.</blockquote>"
        ),
        "sadd_adding": (
            "<b>Adding Sticker</b>\n"
            "<blockquote>Please wait...</blockquote>"
        ),
        "sadd_done": (
            "<b>Sticker Added</b>\n"
            "<blockquote>Pack: <b>{title}</b>\n"
            "Sticker ID: {id}</blockquote>\n"
            "<blockquote><a href='https://t.me/addstickers/{short}'>Open pack</a></blockquote>"
        ),
        "sadd_fail": (
            "<b>Add Failed</b>\n"
            "<blockquote>{error}</blockquote>"
        ),
    }

    strings_ru = {
        "state_menu": (
            "<b>Stickerclone</b>\n"
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
        "input_source": "Вставьте ссылку на исходный пак:",
        "input_short": "Вставьте ссылку для нового пака",
        "input_name": "Отправьте название для нового стикерпака:",
        "source_set": (
            "<b>Исходный пак задан</b>\n"
            "<blockquote>{link}\n"
            "Стикеров: {count}</blockquote>"
        ),
        "source_invalid_format": (
            "<b>Неверный формат</b>\n"
            "<blockquote>Ссылка должна начинаться с https://t.me/addstickers/\n"
            "Пример: https://t.me/addstickers/Journey_of_Elaina\n"
            "Пак не найден - попробуйте другую ссылку.</blockquote>"
        ),
        "source_invalid_resolve": (
            "<b>Пак не найден</b>\n"
            "<blockquote>Не удалось найти стикерпак по этой ссылке.\n"
            "Убедитесь что ссылка правильная и пак существует.\n"
            "Пример: https://t.me/addstickers/Journey_of_Elaina</blockquote>"
        ),
        "short_set": (
            "<b>Ссылка нового пака задана</b>\n"
            "<blockquote>{link}</blockquote>"
        ),
        "short_invalid_format": (
            "<b>Неверный формат</b>\n"
            "<blockquote>Ссылка должна начинаться с https://t.me/addstickers/\n"
            "Пример: https://t.me/addstickers/MyNewPack</blockquote>"
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
        "fetching": (
            "<b>Получаем пак</b>\n"
            "<blockquote>Запрашиваем информацию о стикерпаке...</blockquote>"
        ),
        "pack_empty": (
            "<b>Пустой пак</b>\n"
            "<blockquote>В этом стикерпаке нет стикеров</blockquote>"
        ),
        "copying": (
            "<b>Копирование</b>\n"
            "<blockquote>Прогресс: {current}/{total}\n"
            "Пак: {name}</blockquote>"
        ),
        "done": (
            "<b>Готово</b>\n"
            "<blockquote>Пак: <b>{name}</b>\n"
            "Стикеров: {count}</blockquote>\n"
            "<blockquote><a href='https://t.me/addstickers/{short}'>Открыть пак</a></blockquote>"
        ),
        "done_partial": (
            "<b>Готово с ошибками</b>\n"
            "<blockquote>Пак: <b>{name}</b>\n"
            "Скопировано: {copied}/{total}\n"
            "Ошибок: {failed}</blockquote>\n"
            "<blockquote><a href='https://t.me/addstickers/{short}'>Открыть пак</a></blockquote>"
        ),
        "copy_failed": (
            "<b>Ошибка копирования</b>\n"
            "<blockquote>Не удалось скопировать ни одного стикера.\n"
            "Ошибок: {failed}</blockquote>"
        ),
        "error": (
            "<b>Ошибка</b>\n"
            "<blockquote>{error}</blockquote>"
        ),
        "status_set": "Задано",
        "status_not_set": "Не задано",
        "checking": "Проверяем...",
        "btn_left": "⬅️",
        "btn_right": "➡️",
        "sadd_no_packs": (
            "<b>Паки не найдены</b>\n"
            "<blockquote>У вас пока нет созданных стикерпаков.</blockquote>"
        ),
        "sadd_fetching_packs": (
            "<b>Получаем паки</b>\n"
            "<blockquote>Запрашиваем ваши стикерпаки...</blockquote>"
        ),
        "sadd_pack_item": (
            "<b>Sadd - Выбор пака</b>\n"
            "<blockquote>{title}\n"
            "@{short}\n"
            "Стикеров: {count}</blockquote>\n"
            "Пак {index}/{total}"
        ),
        "sadd_btn_select": "Добавить сюда",
        "sadd_ask_source": (
            "<b>Целевой пак: {title}</b>\n"
            "<blockquote>Теперь отправьте ссылку на исходный пак для получения стикеров.</blockquote>"
        ),
        "sadd_input_source": "Отправьте ссылку на исходный пак (https://t.me/addstickers/PackName):",
        "sadd_source_set": (
            "<b>Исходный пак получен</b>\n"
            "<blockquote>{link}\n"
            "Стикеров: {count}</blockquote>\n"
            "Теперь отправьте ID стикера для добавления."
        ),
        "sadd_btn_id": "ID стикера",
        "sadd_input_id": "Отправьте числовой ID стикера из полученного исходного пака:",
        "sadd_id_invalid": (
            "<b>Неверный ID</b>\n"
            "<blockquote>ID стикера должен быть числом.</blockquote>"
        ),
        "sadd_id_not_found": (
            "<b>Стикер не найден</b>\n"
            "<blockquote>Стикер с ID {id} не найден в полученном исходном паке.</blockquote>"
        ),
        "sadd_adding": (
            "<b>Добавление стикера</b>\n"
            "<blockquote>Пожалуйста, подождите...</blockquote>"
        ),
        "sadd_done": (
            "<b>Стикер добавлен</b>\n"
            "<blockquote>Пак: <b>{title}</b>\n"
            "ID стикера: {id}</blockquote>\n"
            "<blockquote><a href='https://t.me/addstickers/{short}'>Открыть пак</a></blockquote>"
        ),
        "sadd_fail": (
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
            "is_emoji": False,
        }
        self._sadd_state = {
            "packs": [],
            "index": 0,
            "target_short": None,
            "target_title": None,
            "source_link": None,
            "source_documents": None,
        }

    async def client_ready(self, client, db):
        self._client = client
        try:
            lines = _install_deps()
            logger.info("[Stickerclone] Deps:\n" + "\n".join(lines))
        except Exception as e:
            logger.error(f"[Stickerclone] Deps error: {e}")

    def _get_sticker_emoji(self, doc) -> str:
        try:
            from telethon.tl.types import DocumentAttributeSticker, DocumentAttributeCustomEmoji
            for attr in doc.attributes:
                if isinstance(attr, (DocumentAttributeSticker, DocumentAttributeCustomEmoji)):
                    return attr.alt or "⭐"
        except Exception:
            pass
        return "⭐"

    async def _resize_static(self, raw: bytes, size: int = 512):
        try:
            from PIL import Image
            try:
                resample = Image.Resampling.LANCZOS
            except AttributeError:
                resample = Image.LANCZOS
            img = Image.open(io.BytesIO(raw))
            if img.mode != "RGBA":
                img = img.convert("RGBA")
            img.thumbnail((size, size), resample)
            bg = Image.new("RGBA", (size, size), (0, 0, 0, 0))
            offset = ((size - img.width) // 2, (size - img.height) // 2)
            bg.paste(img, offset, img)
            buf = io.BytesIO()
            bg.save(buf, format="PNG", optimize=True)
            return buf.getvalue()
        except Exception as e:
            logger.error(f"[Stickerclone] _resize_static: {e}")
            return None

    async def _to_webm(self, raw: bytes, mime: str, size: int = 512):
        import subprocess
        ext_map = {
            "video/webm": ".webm",
            "image/gif": ".gif",
            "video/mp4": ".mp4",
        }
        suffix = ext_map.get(mime)
        if not suffix:
            return None
        fin = fout = None
        try:
            with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as f:
                f.write(raw)
                fin = f.name
            fout = fin + "_out.webm"
            r = subprocess.run(
                [
                    "ffmpeg", "-y", "-i", fin,
                    "-vf",
                    f"scale={size}:{size}:force_original_aspect_ratio=decrease,"
                    f"pad={size}:{size}:(ow-iw)/2:(oh-ih)/2:color=0x00000000,fps=30",
                    "-t", "3", "-c:v", "libvpx-vp9", "-pix_fmt", "yuva420p",
                    "-b:v", "400k", "-an", fout,
                ],
                capture_output=True,
                timeout=60,
            )
            if r.returncode != 0:
                logger.warning(f"[Stickerclone] ffmpeg returned {r.returncode}: {r.stderr.decode()[:200]}")
                return None
            with open(fout, "rb") as f:
                data = f.read()
            return data if len(data) <= 256 * 1024 else None
        except Exception as e:
            logger.error(f"[Stickerclone] _to_webm: {e}")
            return None
        finally:
            for p in (fin, fout):
                if p:
                    try:
                        os.unlink(p)
                    except Exception:
                        pass

    async def _upload_doc(self, data: bytes, fname: str, sticker_type: str, size: int, is_emoji: bool = False):
        from telethon.tl.functions.messages import UploadMediaRequest
        from telethon.tl.types import (
            InputPeerSelf,
            InputMediaUploadedDocument,
            DocumentAttributeFilename,
            DocumentAttributeVideo,
            DocumentAttributeSticker,
            DocumentAttributeCustomEmoji,
            DocumentAttributeImageSize,
            InputStickerSetEmpty,
            InputDocument,
        )
        buf = io.BytesIO(data)
        buf.name = fname
        uploaded = await self._client.upload_file(buf)

        if sticker_type == "tgs":
            if is_emoji:
                attr = DocumentAttributeCustomEmoji(alt="", stickerset=InputStickerSetEmpty(), free=True)
            else:
                attr = DocumentAttributeSticker(alt="", stickerset=InputStickerSetEmpty())
            media = InputMediaUploadedDocument(
                file=uploaded,
                mime_type="application/x-tgsticker",
                attributes=[
                    DocumentAttributeFilename(file_name=fname),
                    attr,
                ],
            )
        elif sticker_type == "webm":
            media = InputMediaUploadedDocument(
                file=uploaded,
                mime_type="video/webm",
                attributes=[
                    DocumentAttributeFilename(file_name=fname),
                    DocumentAttributeVideo(
                        duration=3, w=size, h=size,
                        round_message=False, supports_streaming=True,
                    ),
                    DocumentAttributeSticker(alt="", stickerset=InputStickerSetEmpty()),
                ],
                nosound_video=True,
            )
        else:
            media = InputMediaUploadedDocument(
                file=uploaded,
                mime_type="image/png",
                attributes=[
                    DocumentAttributeFilename(file_name=fname),
                    DocumentAttributeImageSize(w=size, h=size),
                    DocumentAttributeSticker(alt="", stickerset=InputStickerSetEmpty()),
                ],
            )
        result = await self._client(UploadMediaRequest(peer=InputPeerSelf(), media=media))
        doc = result.document
        return InputDocument(doc.id, doc.access_hash, doc.file_reference)

    async def _process_sticker(self, doc, is_emoji: bool):
        from telethon.errors import FloodWaitError
        try:
            mime = doc.mime_type or ""
            buf = io.BytesIO()
            await self._client.download_file(doc, buf)
            raw = buf.getvalue()
            logger.debug(f"[Stickerclone] Processing sticker mime={mime} size={len(raw)}")

            if mime == "application/x-tgsticker":
                if len(raw) > 512 * 1024:
                    logger.warning(f"[Stickerclone] TGS too large: {len(raw)} bytes")
                    return None, None
                uploaded = await self._upload_doc(raw, "s.tgs", "tgs", 512, is_emoji)
                return uploaded, "tgs"

            if mime == "video/webm":
                if len(raw) <= 256 * 1024:
                    uploaded = await self._upload_doc(raw, "s.webm", "webm", 512, is_emoji)
                    return uploaded, "webm"
                data = await self._to_webm(raw, mime, 512)
                if data:
                    uploaded = await self._upload_doc(data, "s.webm", "webm", 512, is_emoji)
                    return uploaded, "webm"
                return None, None

            if mime in ("image/gif", "video/mp4"):
                data = await self._to_webm(raw, mime, 512)
                if data:
                    uploaded = await self._upload_doc(data, "s.webm", "webm", 512, is_emoji)
                    return uploaded, "webm"
                data = await self._resize_static(raw, 512)
                if data:
                    uploaded = await self._upload_doc(data, "s.png", "png", 512, is_emoji)
                    return uploaded, "png"
                return None, None

            data = await self._resize_static(raw, 512)
            if not data:
                logger.warning(f"[Stickerclone] _resize_static returned None for mime={mime}")
                return None, None
            uploaded = await self._upload_doc(data, "s.png", "png", 512, is_emoji)
            return uploaded, "png"

        except FloodWaitError:
            raise
        except Exception as e:
            logger.error(f"[Stickerclone] _process_sticker error: {e}")
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
            logger.info(f"[Stickerclone] _try_resolve_pack '{short_name}': {e}")
            return None

    def _extract_short_name(self, link: str):
        m = ADDSTICKERS_RE.match(link.strip())
        if m:
            return m.group(1)
        return None

    def _format_state_menu(self):
        s = self._state
        source_status = s["source_link"] if s["source_link"] else self.strings["status_not_set"]
        short_status = f"https://t.me/addstickers/{s['new_short']}" if s["new_short"] else self.strings["status_not_set"]
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
        from telethon.tl.types import DocumentAttributeCustomEmoji
        link = query.strip()
        short_name = self._extract_short_name(link)

        if not short_name:
            await call.edit(
                self.strings["source_invalid_format"],
                reply_markup=[[{"text": self.strings["btn_retry"], "input": self.strings["input_source"], "handler": self._cb_set_source, "style": "primary"}],
                              [{"text": self.strings["btn_back"], "callback": self._cb_state_menu, "style": "danger"}]],
            )
            return

        await call.edit(self.strings["checking"])
        logger.info(f"[Stickerclone] Checking source pack: {short_name}")
        result = await self._try_resolve_pack(short_name)

        if not result or not result.documents:
            await call.edit(
                self.strings["source_invalid_resolve"],
                reply_markup=[[{"text": self.strings["btn_retry"], "input": self.strings["input_source"], "handler": self._cb_set_source, "style": "primary"}],
                              [{"text": self.strings["btn_back"], "callback": self._cb_state_menu, "style": "danger"}]],
            )
            return

        is_emoji = any(
            isinstance(attr, DocumentAttributeCustomEmoji)
            for doc in result.documents
            for attr in doc.attributes
        )

        self._state["source_link"] = link
        self._state["source_short"] = short_name
        self._state["source_documents"] = result.documents
        self._state["is_emoji"] = is_emoji
        logger.info(f"[Stickerclone] Source set: {short_name}, {len(result.documents)} stickers, is_emoji={is_emoji}")

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
                reply_markup=[[{"text": self.strings["btn_retry"], "input": self.strings["input_short"], "handler": self._cb_set_short, "style": "primary"}],
                              [{"text": self.strings["btn_back"], "callback": self._cb_state_menu, "style": "danger"}]],
            )
            return

        await call.edit(self.strings["checking"])
        logger.info(f"[Stickerclone] Checking if short name is free: {short_name}")
        result = await self._try_resolve_pack(short_name)

        if result is not None:
            await call.edit(
                self.strings["short_occupied"],
                reply_markup=[[{"text": self.strings["btn_retry"], "input": self.strings["input_short"], "handler": self._cb_set_short, "style": "primary"}],
                              [{"text": self.strings["btn_back"], "callback": self._cb_state_menu, "style": "danger"}]],
            )
            return

        self._state["new_short"] = short_name
        logger.info(f"[Stickerclone] New short name set: {short_name}")

        await call.edit(
            self.strings["short_set"].format(link=link),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_state_menu, "style": "danger"}]],
        )

    async def _cb_set_name(self, call: InlineCall, query: str):
        name = query.strip()
        self._state["name"] = name if name else None
        logger.info(f"[Stickerclone] Name set: {name}")
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
        from telethon.errors import FloodWaitError, PackShortNameOccupiedError

        documents = self._state["source_documents"]
        pack_title = self._state["name"]
        short_name = self._state["new_short"]
        is_emoji = self._state.get("is_emoji", False)
        total = len(documents)

        logger.info(f"[Stickerclone] Starting copy: title='{pack_title}' short='{short_name}' total={total} emojis={is_emoji}")

        pack_created = False
        copied = 0
        failed = 0

        for i, doc in enumerate(documents, 1):
            while True:
                try:
                    await call.edit(self.strings["copying"].format(current=i, total=total, name=pack_title))
                except Exception:
                    pass

                emoji = self._get_sticker_emoji(doc)
                logger.debug(f"[Stickerclone] Sticker {i}/{total} emoji={emoji} mime={doc.mime_type}")

                try:
                    input_doc, sticker_type = await self._process_sticker(doc, is_emoji)
                    if input_doc is None:
                        failed += 1
                        logger.warning(f"[Stickerclone] Sticker {i}/{total} process failed")
                        break

                    if not pack_created:
                        await self._client(CreateStickerSetRequest(
                            user_id=InputUserSelf(),
                            title=pack_title,
                            short_name=short_name,
                            stickers=[InputStickerSetItem(document=input_doc, emoji=emoji)],
                            emojis=is_emoji,
                        ))
                        pack_created = True
                        copied += 1
                        logger.info(f"[Stickerclone] Pack created: {short_name}")
                    else:
                        await self._client(AddStickerToSetRequest(
                            stickerset=InputStickerSetShortName(short_name=short_name),
                            sticker=InputStickerSetItem(document=input_doc, emoji=emoji),
                        ))
                        copied += 1
                    
                    break

                except FloodWaitError as e:
                    wait_secs = e.seconds
                    rand_secs = random.randint(1, 10)
                    total_wait = wait_secs + rand_secs
                    
                    try:
                        await call.edit(
                            self.strings["copying"].format(current=i, total=total, name=pack_title) + 
                            f"\n<blockquote>Got floodwait, waiting {wait_secs} + {rand_secs} seconds</blockquote>"
                        )
                    except Exception:
                        pass
                    
                    logger.info(f"[Stickerclone] FloodWait {wait_secs}s, sleeping {total_wait}s")
                    await asyncio.sleep(total_wait)
                    continue

                except PackShortNameOccupiedError:
                    logger.warning(f"[Stickerclone] Short name occupied on creation: {short_name}")
                    await call.edit(
                        self.strings["short_occupied"],
                        reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_state_menu, "style": "danger"}]],
                    )
                    return

                except Exception as e:
                    logger.error(f"[Stickerclone] Sticker {i}/{total} add error: {e}")
                    failed += 1
                    break

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
            "is_emoji": False,
        }

        logger.info(f"[Stickerclone] Done. copied={copied} failed={failed} total={total}")

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
        ru_doc="Открыть меню клонирования стикерпака",
        en_doc="Open sticker pack cloner menu",
    )
    async def sclone(self, message):
        """Open sticker pack cloner menu"""
        await self.inline.form(
            text=self._format_state_menu(),
            message=message,
            reply_markup=self._get_state_markup(),
            silent=True,
        )

    def _format_sadd_pack(self):
        s = self._sadd_state
        pack = s["packs"][s["index"]]
        return self.strings["sadd_pack_item"].format(
            title=pack.title,
            short=pack.short_name,
            count=pack.count,
            index=s["index"] + 1,
            total=len(s["packs"]),
        )

    def _get_sadd_markup(self):
        s = self._sadd_state
        idx = s["index"]
        total = len(s["packs"])
        left = {"text": self.strings["btn_left"], "callback": self._cb_sadd_left}
        right = {"text": self.strings["btn_right"], "callback": self._cb_sadd_right}
        if idx > 0:
            left["style"] = "primary"
        if idx < total - 1:
            right["style"] = "primary"
        return [
            [{"text": self.strings["sadd_btn_select"], "callback": self._cb_sadd_select, "style": "success"}],
            [left, right],
            [{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}],
        ]

    async def _cb_sadd_left(self, call: InlineCall):
        s = self._sadd_state
        if s["index"] <= 0:
            await call.answer()
            return
        s["index"] -= 1
        await call.edit(self._format_sadd_pack(), reply_markup=self._get_sadd_markup())

    async def _cb_sadd_right(self, call: InlineCall):
        s = self._sadd_state
        if s["index"] >= len(s["packs"]) - 1:
            await call.answer()
            return
        s["index"] += 1
        await call.edit(self._format_sadd_pack(), reply_markup=self._get_sadd_markup())

    async def _cb_sadd_select(self, call: InlineCall):
        s = self._sadd_state
        pack = s["packs"][s["index"]]
        s["target_short"] = pack.short_name
        s["target_title"] = pack.title
        s["source_link"] = None
        s["source_documents"] = None
        logger.info(f"[Stickerclone] sadd target selected: {pack.short_name}")

        await call.edit(
            self.strings["sadd_ask_source"].format(title=pack.title),
            reply_markup=[
                [{"text": self.strings["btn_set_source"], "input": self.strings["sadd_input_source"], "handler": self._cb_sadd_set_source, "style": "primary"}],
                [{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}],
            ],
        )

    async def _cb_sadd_set_source(self, call: InlineCall, query: str):
        link = query.strip()
        short_name = self._extract_short_name(link)

        if not short_name:
            await call.edit(
                self.strings["source_invalid_format"],
                reply_markup=[
                    [{"text": self.strings["btn_retry"], "input": self.strings["sadd_input_source"], "handler": self._cb_sadd_set_source, "style": "primary"}],
                    [{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}],
                ],
            )
            return

        await call.edit(self.strings["checking"])
        logger.info(f"[Stickerclone] sadd checking source pack: {short_name}")
        result = await self._try_resolve_pack(short_name)

        if not result or not result.documents:
            await call.edit(
                self.strings["source_invalid_resolve"],
                reply_markup=[
                    [{"text": self.strings["btn_retry"], "input": self.strings["sadd_input_source"], "handler": self._cb_sadd_set_source, "style": "primary"}],
                    [{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}],
                ],
            )
            return

        s = self._sadd_state
        s["source_link"] = link
        s["source_documents"] = {doc.id: doc for doc in result.documents}
        logger.info(f"[Stickerclone] sadd source resolved: {short_name}, {len(result.documents)} stickers")

        await call.edit(
            self.strings["sadd_source_set"].format(link=link, count=len(result.documents)),
            reply_markup=[
                [{"text": self.strings["sadd_btn_id"], "input": self.strings["sadd_input_id"], "handler": self._cb_sadd_set_id, "style": "primary"}],
                [{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}],
            ],
        )

    async def _cb_sadd_set_id(self, call: InlineCall, query: str):
        s = self._sadd_state
        raw_id = query.strip()

        try:
            sticker_id = int(raw_id)
        except ValueError:
            await call.edit(
                self.strings["sadd_id_invalid"],
                reply_markup=[
                    [{"text": self.strings["btn_retry"], "input": self.strings["sadd_input_id"], "handler": self._cb_sadd_set_id, "style": "primary"}],
                    [{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}],
                ],
            )
            return

        doc = (s["source_documents"] or {}).get(sticker_id)
        if not doc:
            await call.edit(
                self.strings["sadd_id_not_found"].format(id=sticker_id),
                reply_markup=[
                    [{"text": self.strings["btn_retry"], "input": self.strings["sadd_input_id"], "handler": self._cb_sadd_set_id, "style": "primary"}],
                    [{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}],
                ],
            )
            return

        await call.edit(self.strings["sadd_adding"])

        from telethon.tl.functions.stickers import AddStickerToSetRequest
        from telethon.tl.types import InputStickerSetShortName, InputStickerSetItem, InputDocument
        from telethon.errors import FloodWaitError

        emoji = self._get_sticker_emoji(doc)
        target_short = s["target_short"]

        try:
            await self._client(AddStickerToSetRequest(
                stickerset=InputStickerSetShortName(short_name=target_short),
                sticker=InputStickerSetItem(
                    document=InputDocument(doc.id, doc.access_hash, doc.file_reference),
                    emoji=emoji,
                ),
            ))
        except FloodWaitError as e:
            logger.info(f"[Stickerclone] sadd FloodWait {e.seconds}s")
            await call.edit(
                self.strings["sadd_fail"].format(error=f"FloodWait {e.seconds}s"),
                reply_markup=[[{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}]],
            )
            return
        except Exception as e:
            logger.error(f"[Stickerclone] sadd add error: {e}")
            await call.edit(
                self.strings["sadd_fail"].format(error=str(e)),
                reply_markup=[[{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}]],
            )
            return

        logger.info(f"[Stickerclone] sadd: added sticker {sticker_id} to {target_short}")

        await call.edit(
            self.strings["sadd_done"].format(title=s["target_title"], id=sticker_id, short=target_short),
            reply_markup=[[{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}]],
        )

    @loader.command(
        ru_doc="Добавить стикер в свой пак по ID",
        en_doc="Add a sticker to your own pack by ID",
    )
    async def sadd(self, message):
        """Add a sticker to your own pack by ID"""
        from telethon.tl.functions.messages import GetMyStickersRequest

        result = await self._client(GetMyStickersRequest(offset_id=0, limit=100))
        packs = [item.set for item in result.sets]
        logger.info(f"[Stickerclone] sadd: {len(packs)} own packs found")

        self._sadd_state = {
            "packs": packs,
            "index": 0,
            "target_short": None,
            "target_title": None,
            "source_link": None,
            "source_documents": None,
        }

        if not packs:
            await self.inline.form(
                text=self.strings["sadd_no_packs"],
                message=message,
                reply_markup=[[{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}]],
                silent=True,
            )
            return

        await self.inline.form(
            text=self._format_sadd_pack(),
            message=message,
            reply_markup=self._get_sadd_markup(),
            silent=True,
        )