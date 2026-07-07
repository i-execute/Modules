__version__ = (1, 0, 0)
# meta developer: I_execute.t.me forked from @elisartix

import io
import os
import random
import string
import logging
import tempfile
import sys

from .. import loader, utils
from ..inline.types import InlineCall

DEPS = ["Pillow"]


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

_TR = {
    'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'yo',
    'ж': 'zh', 'з': 'z', 'и': 'i', 'й': 'j', 'к': 'k', 'л': 'l', 'м': 'm',
    'н': 'n', 'о': 'o', 'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u',
    'ф': 'f', 'х': 'kh', 'ц': 'ts', 'ч': 'ch', 'ш': 'sh', 'щ': 'sch',
    'ъ': '', 'ы': 'y', 'ь': '', 'э': 'e', 'ю': 'yu', 'я': 'ya',
}


def _translit(text: str) -> str:
    out = []
    for ch in text.lower():
        if ch in _TR:
            out.append(_TR[ch])
        elif ch.isascii() and (ch.isalnum() or ch == '_'):
            out.append(ch)
        else:
            out.append('_')
    return "".join(out).strip("_") or "pack"


@loader.tds
class StickerClone(loader.Module):
    """Sticker pack cloner"""

    strings = {
        "name": "Stickerclone",
        "main_menu": (
            "<b>Stickerclone</b>\n"
            "<blockquote>Copy any sticker pack to a new one with custom name.\n"
            "Enter sticker link and pack name to start.</blockquote>"
        ),
        "btn_set_link": "Set Sticker Link",
        "btn_set_name": "Set Pack Name",
        "btn_start": "Start Copy",
        "btn_back": "Back",
        "btn_close": "Close",
        "input_link": "Send a link to any sticker from the pack (t.me/addstickers/... or direct message link with sticker):",
        "input_name": "Send the name for the new sticker pack:",
        "link_set": (
            "<b>Sticker Link Set</b>\n"
            "<blockquote>{link}</blockquote>"
        ),
        "name_set": (
            "<b>Pack Name Set</b>\n"
            "<blockquote>{name}</blockquote>"
        ),
        "state_menu": (
            "<b>Stickerclone</b>\n"
            "<blockquote>Sticker link: {link_status}\n"
            "Pack name: {name_status}</blockquote>"
        ),
        "no_link": (
            "<b>No Sticker Link</b>\n"
            "<blockquote>Set sticker link first</blockquote>"
        ),
        "no_name": (
            "<b>No Pack Name</b>\n"
            "<blockquote>Set pack name first</blockquote>"
        ),
        "link_invalid": (
            "<b>Invalid Link</b>\n"
            "<blockquote>Could not resolve sticker pack from this link.\n"
            "Use t.me/addstickers/packname format.</blockquote>"
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
        "status_set": "Set",
        "status_not_set": "Not set",
    }

    strings_ru = {
        "main_menu": (
            "<b>Stickerclone</b>\n"
            "<blockquote>Копирует стикерпак в новый с заданным именем.\n"
            "Укажите ссылку на стикер и название нового пака.</blockquote>"
        ),
        "btn_set_link": "Ссылка на стикер",
        "btn_set_name": "Название пака",
        "btn_start": "Начать копирование",
        "btn_back": "Назад",
        "btn_close": "Закрыть",
        "input_link": "Отправьте ссылку на любой стикер из пака (t.me/addstickers/... или прямая ссылка на сообщение со стикером):",
        "input_name": "Отправьте название для нового стикерпака:",
        "link_set": (
            "<b>Ссылка задана</b>\n"
            "<blockquote>{link}</blockquote>"
        ),
        "name_set": (
            "<b>Название задано</b>\n"
            "<blockquote>{name}</blockquote>"
        ),
        "state_menu": (
            "<b>Stickerclone</b>\n"
            "<blockquote>Ссылка на стикер: {link_status}\n"
            "Название пака: {name_status}</blockquote>"
        ),
        "no_link": (
            "<b>Нет ссылки</b>\n"
            "<blockquote>Сначала укажите ссылку на стикер</blockquote>"
        ),
        "no_name": (
            "<b>Нет названия</b>\n"
            "<blockquote>Сначала укажите название пака</blockquote>"
        ),
        "link_invalid": (
            "<b>Невалидная ссылка</b>\n"
            "<blockquote>Не удалось получить стикерпак по этой ссылке.\n"
            "Используйте формат t.me/addstickers/packname.</blockquote>"
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
        "status_set": "Задано",
        "status_not_set": "Не задано",
    }

    def __init__(self):
        self._state = {"link": None, "name": None}

    async def client_ready(self, client, db):
        self._client = client
        try:
            lines = _install_deps()
            logger.info("[Stickerclone] Deps:\n" + "\n".join(lines))
        except Exception as e:
            logger.error(f"[Stickerclone] Deps error: {e}")

    def _gen_short_name(self, title: str) -> str:
        base = _translit(title)
        suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=8))
        me_username = getattr(self, "_me_username", "user")
        return f"{base}_{suffix}_by_{me_username}"[:64]

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
            "application/x-tgsticker": ".tgs",
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
                    DocumentAttributeVideo(duration=3, w=size, h=size, round_message=False, supports_streaming=True),
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

            if mime == "video/webm":
                if len(raw) <= 256 * 1024:
                    return await self._upload_doc(raw, "s.webm", True, 512), True
                data = await self._to_webm(raw, mime, 512)
                if data:
                    return await self._upload_doc(data, "s.webm", True, 512), True
                return None, False

            if mime == "application/x-tgsticker":
                data = await self._to_webm(raw, mime, 512)
                if data:
                    return await self._upload_doc(data, "s.webm", True, 512), True
                return None, False

            if mime in ("image/gif", "video/mp4"):
                data = await self._to_webm(raw, mime, 512)
                if data:
                    return await self._upload_doc(data, "s.webm", True, 512), True
                data = await self._resize_static(raw, 512)
                if data:
                    return await self._upload_doc(data, "s.png", False, 512), False
                return None, False

            data = await self._resize_static(raw, 512)
            if not data:
                return None, False
            return await self._upload_doc(data, "s.png", False, 512), False

        except Exception as e:
            logger.error(f"[Stickerclone] _process_sticker: {e}")
            return None, False

    async def _resolve_pack(self, link: str):
        from telethon.tl.functions.messages import GetStickerSetRequest
        from telethon.tl.types import InputStickerSetShortName
        import re

        link = link.strip()
        short_name = None

        m = re.search(r't\.me/addstickers/([A-Za-z0-9_]+)', link)
        if m:
            short_name = m.group(1)

        if not short_name:
            m = re.search(r'addstickers[=?/]([A-Za-z0-9_]+)', link)
            if m:
                short_name = m.group(1)

        if not short_name:
            short_name = link.strip().split("/")[-1].split("?")[0]

        if not short_name:
            return None

        try:
            result = await self._client(GetStickerSetRequest(
                stickerset=InputStickerSetShortName(short_name=short_name),
                hash=random.randint(-2147483647, 2147483647),
            ))
            return result
        except Exception as e:
            logger.error(f"[Stickerclone] _resolve_pack error for '{short_name}': {e}")
            return None

    def _format_state_menu(self):
        link_status = self.strings["status_set"] if self._state["link"] else self.strings["status_not_set"]
        name_status = self.strings["status_set"] if self._state["name"] else self.strings["status_not_set"]
        return self.strings["state_menu"].format(link_status=link_status, name_status=name_status)

    def _get_state_markup(self):
        return [
            [
                {"text": self.strings["btn_set_link"], "input": self.strings["input_link"], "handler": self._cb_set_link, "style": "primary"},
                {"text": self.strings["btn_set_name"], "input": self.strings["input_name"], "handler": self._cb_set_name, "style": "primary"},
            ],
            [{"text": self.strings["btn_start"], "callback": self._cb_start, "style": "success"}],
            [{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}],
        ]

    async def _cb_set_link(self, call: InlineCall, query: str):
        link = query.strip()
        self._state["link"] = link if link else None
        logger.info(f"[Stickerclone] Link set: {link}")
        await call.edit(
            self.strings["link_set"].format(link=link) if link else self.strings["no_link"],
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_state_menu, "style": "danger"}]],
        )

    async def _cb_set_name(self, call: InlineCall, query: str):
        name = query.strip()
        self._state["name"] = name if name else None
        logger.info(f"[Stickerclone] Name set: {name}")
        await call.edit(
            self.strings["name_set"].format(name=name) if name else self.strings["no_name"],
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_state_menu, "style": "danger"}]],
        )

    async def _cb_state_menu(self, call: InlineCall):
        await call.edit(self._format_state_menu(), reply_markup=self._get_state_markup())

    async def _cb_close(self, call: InlineCall):
        await call.delete()

    async def _cb_start(self, call: InlineCall):
        if not self._state["link"]:
            await call.edit(
                self.strings["no_link"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_state_menu, "style": "danger"}]],
            )
            return

        if not self._state["name"]:
            await call.edit(
                self.strings["no_name"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_state_menu, "style": "danger"}]],
            )
            return

        await call.edit(self.strings["fetching"])
        logger.info(f"[Stickerclone] Resolving pack from link: {self._state['link']}")

        full_set = await self._resolve_pack(self._state["link"])
        if not full_set:
            await call.edit(
                self.strings["link_invalid"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_state_menu, "style": "danger"}]],
            )
            return

        if not full_set.documents:
            await call.edit(
                self.strings["pack_empty"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_state_menu, "style": "danger"}]],
            )
            return

        pack_title = self._state["name"]
        documents = full_set.documents
        total = len(documents)
        logger.info(f"[Stickerclone] Starting copy: '{pack_title}', {total} stickers")

        from telethon.tl.functions.stickers import CreateStickerSetRequest, AddStickerToSetRequest
        from telethon.tl.functions.messages import UninstallStickerSetRequest
        from telethon.tl.types import InputStickerSetShortName, InputStickerSetItem
        from telethon.errors.rpcerrorlist import PackShortNameOccupiedError

        me = await self._client.get_me()
        self._me_username = me.username or str(me.id)
        short_name = self._gen_short_name(pack_title)
        pack_created = False
        copied = 0
        failed = 0

        for i, doc in enumerate(documents, 1):
            try:
                await call.edit(self.strings["copying"].format(current=i, total=total, name=pack_title))
            except Exception:
                pass

            emoji = self._get_sticker_emoji(doc)
            input_doc, _ = await self._process_sticker(doc)
            if input_doc is None:
                failed += 1
                logger.warning(f"[Stickerclone] Sticker {i}/{total} failed to process")
                continue

            try:
                if not pack_created:
                    await self._client(CreateStickerSetRequest(
                        user_id="me",
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
                short_name = self._gen_short_name(pack_title)
                logger.warning(f"[Stickerclone] Short name occupied, retrying with {short_name}")
                try:
                    await self._client(CreateStickerSetRequest(
                        user_id="me",
                        title=pack_title,
                        short_name=short_name,
                        stickers=[InputStickerSetItem(document=input_doc, emoji=emoji)],
                    ))
                    pack_created = True
                    copied += 1
                except Exception as e2:
                    logger.error(f"[Stickerclone] Pack creation retry failed: {e2}")
                    await call.edit(
                        self.strings["error"].format(error=str(e2)[:200]),
                        reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_state_menu, "style": "danger"}]],
                    )
                    return
            except Exception as e:
                logger.error(f"[Stickerclone] Sticker {i} add error: {e}")
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

        self._state = {"link": None, "name": None}
        logger.info(f"[Stickerclone] Done. copied={copied}, failed={failed}, total={total}")

        if failed == 0:
            await call.edit(
                self.strings["done"].format(name=pack_title, count=copied, short=short_name),
                reply_markup=[[{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}]],
            )
        else:
            await call.edit(
                self.strings["done_partial"].format(name=pack_title, copied=copied, total=total, failed=failed, short=short_name),
                reply_markup=[[{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}]],
            )

    @loader.command(
        ru_doc="Открыть меню клонирования стикерпака",
        en_doc="Open sticker pack cloner menu",
    )
    async def stickerclone(self, message):
        """Open sticker pack cloner menu"""
        await self.inline.form(
            text=self._format_state_menu(),
            message=message,
            reply_markup=self._get_state_markup(),
            silent=True,
        )