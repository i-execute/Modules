__version__ = (1, 1, 0)
# meta developer: I_execute.t.me froked from @elisartix

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
        "input_source": "Send source pack link (https://t.me/addstickers/PackName):",
        "input_short": "Send new pack link (https://t.me/addstickers/MyNewPack) - must be free:",
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
            "<blockquote><a href='tg://addstickers?set={short}'>Open pack</a></blockquote>"
        ),
        "done_partial": (
            "<b>Done with errors</b>\n"
            "<blockquote>Pack: <b>{name}</b>\n"
            "Copied: {copied}/{total}\n"
            "Failed: {failed}</blockquote>\n"
            "<blockquote><a href='tg://addstickers?set={short}'>Open pack</a></blockquote>"
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
        "tgs_unsupported": (
            "<b>Unsupported Pack</b>\n"
            "<blockquote>This pack contains .tgs stickers.\n"
            "This module does not support .tgs sticker packs.</blockquote>"
        ),
        "status_set": "Set",
        "status_not_set": "Not set",
        "checking": "Checking...",
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
        "input_source": "Отправьте ссылку на исходный пак (https://t.me/addstickers/PackName):",
        "input_short": "Отправьте ссылку для нового пака (https://t.me/addstickers/MyNewPack) - должна быть свободной:",
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
            "<blockquote><a href='tg://addstickers?set={short}'>Открыть пак</a></blockquote>"
        ),
        "done_partial": (
            "<b>Готово с ошибками</b>\n"
            "<blockquote>Пак: <b>{name}</b>\n"
            "Скопировано: {copied}/{total}\n"
            "Ошибок: {failed}</blockquote>\n"
            "<blockquote><a href='tg://addstickers?set={short}'>Открыть пак</a></blockquote>"
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
        "tgs_unsupported": (
            "<b>Пак не поддерживается</b>\n"
            "<blockquote>Этот пак содержит .tgs стикеры.\n"
            "Модуль не работает с такими наборами стикеров.</blockquote>"
        ),
        "status_set": "Задано",
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
        try:
            lines = _install_deps()
            logger.info("[Stickerclone] Deps:\n" + "\n".join(lines))
        except Exception as e:
            logger.error(f"[Stickerclone] Deps error: {e}")

    def _get_sticker_emoji(self, doc) -> str:
        try:
            from telethon.tl.types import DocumentAttributeSticker
            for attr in doc.attributes:
                if isinstance(attr, DocumentAttributeSticker):
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

    async def _upload_doc(self, data: bytes, fname: str, animated: bool, size: int):
        from telethon.tl.functions.messages import UploadMediaRequest
        from telethon.tl.types import (
            InputPeerSelf,
            InputMediaUploadedDocument,
            DocumentAttributeFilename,
            DocumentAttributeVideo,
            DocumentAttributeSticker,
            DocumentAttributeImageSize,
            InputStickerSetEmpty,
            InputDocument,
        )
        buf = io.BytesIO(data)
        buf.name = fname
        uploaded = await self._client.upload_file(buf)
        if animated:
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

    async def _process_sticker(self, doc):
        try:
            mime = doc.mime_type or ""
            buf = io.BytesIO()
            await self._client.download_file(doc, buf)
            raw = buf.getvalue()
            logger.debug(f"[Stickerclone] Processing sticker mime={mime} size={len(raw)}")

            if mime == "video/webm":
                if len(raw) <= 256 * 1024:
                    uploaded = await self._upload_doc(raw, "s.webm", True, 512)
                    return uploaded, True
                data = await self._to_webm(raw, mime, 512)
                if data:
                    uploaded = await self._upload_doc(data, "s.webm", True, 512)
                    return uploaded, True
                return None, False

            if mime in ("image/gif", "video/mp4"):
                data = await self._to_webm(raw, mime, 512)
                if data:
                    uploaded = await self._upload_doc(data, "s.webm", True, 512)
                    return uploaded, True
                data = await self._resize_static(raw, 512)
                if data:
                    uploaded = await self._upload_doc(data, "s.png", False, 512)
                    return uploaded, False
                return None, False

            data = await self._resize_static(raw, 512)
            if not data:
                logger.warning(f"[Stickerclone] _resize_static returned None for mime={mime}")
                return None, False
            uploaded = await self._upload_doc(data, "s.png", False, 512)
            return uploaded, False

        except Exception as e:
            logger.error(f"[Stickerclone] _process_sticker error: {e}")
            return None, False

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

        if any(doc.mime_type == "application/x-tgsticker" for doc in result.documents):
            await call.edit(
                self.strings["tgs_unsupported"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_state_menu, "style": "danger"}]],
            )
            return

        self._state["source_link"] = link
        self._state["source_short"] = short_name
        self._state["source_documents"] = result.documents
        logger.info(f"[Stickerclone] Source set: {short_name}, {len(result.documents)} stickers")

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
        from telethon.errors.rpcerrorlist import PackShortNameOccupiedError

        documents = self._state["source_documents"]
        pack_title = self._state["name"]
        short_name = self._state["new_short"]
        total = len(documents)

        logger.info(f"[Stickerclone] Starting copy: title='{pack_title}' short='{short_name}' total={total}")

        pack_created = False
        copied = 0
        failed = 0

        for i, doc in enumerate(documents, 1):
            try:
                await call.edit(self.strings["copying"].format(current=i, total=total, name=pack_title))
            except Exception:
                pass

            emoji = self._get_sticker_emoji(doc)
            logger.debug(f"[Stickerclone] Sticker {i}/{total} emoji={emoji} mime={doc.mime_type}")

            input_doc, animated = await self._process_sticker(doc)
            if input_doc is None:
                failed += 1
                logger.warning(f"[Stickerclone] Sticker {i}/{total} process failed")
                continue

            try:
                if not pack_created:
                    await self._client(CreateStickerSetRequest(
                        user_id=InputUserSelf(),
                        title=pack_title,
                        short_name=short_name,
                        stickers=[InputStickerSetItem(document=input_doc, emoji=emoji)],
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