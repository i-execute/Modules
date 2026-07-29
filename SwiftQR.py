__version__ = (1, 0, 1)
# meta developer: I_execute.t.me

import logging
import io
import os
import sys
import subprocess
import importlib

logger = logging.getLogger(__name__)

DEPS = ["qrcode", "Pillow", "pyzbar", "aiohttp"]


def _install_deps():
    pip = os.path.join(os.path.dirname(sys.executable), "pip")
    if not os.path.exists(pip):
        pip = "pip"
    for pkg in DEPS:
        try:
            subprocess.run(
                [pip, "install", "-U", pkg, "--break-system-packages", "-q"],
                capture_output=True, text=True, timeout=120,
            )
        except Exception:
            pass


_install_deps()

try:
    import qrcode
    import qrcode.image.pil
    QRCODE_OK = True
except ImportError:
    qrcode = None
    QRCODE_OK = False

try:
    from PIL import Image
    PIL_OK = True
except ImportError:
    Image = None
    PIL_OK = False

try:
    from pyzbar.pyzbar import decode as pyzbar_decode
    PYZBAR_OK = True
except ImportError:
    pyzbar_decode = None
    PYZBAR_OK = False

try:
    import aiohttp
    AIOHTTP_OK = True
except ImportError:
    aiohttp = None
    AIOHTTP_OK = False

from telethon.tl.types import Message
from .. import loader, utils
from ..inline.types import InlineCall


def _escape(text):
    if not text:
        return ""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


async def _generate_qr(data: str):
    if not QRCODE_OK or not PIL_OK:
        return None
    try:
        qr = qrcode.QRCode(
            version=None,
            error_correction=qrcode.constants.ERROR_CORRECT_H,
            box_size=20,
            border=2,
        )
        qr.add_data(data)
        qr.make(fit=True)

        img = qr.make_image(fill_color="black", back_color="white").convert("RGB")

        w, h = img.size
        target = 1024
        if w < target:
            img = img.resize((target, target), Image.NEAREST)

        buf = io.BytesIO()
        img.save(buf, format="PNG", optimize=True)
        return buf.getvalue()
    except Exception as e:
        logger.exception("_generate_qr error: %s", e)
        return None


async def _decode_qr(image_data: bytes):
    if not PYZBAR_OK or not PIL_OK:
        logger.error(
            "SwiftQR: pyzbar_ok=%s pil_ok=%s (pyzbar needs the system libzbar0 "
            "shared library, not just the pip package — run: apt install -y libzbar0)",
            PYZBAR_OK, PIL_OK,
        )
        return None
    try:
        img = Image.open(io.BytesIO(image_data))

        if img.mode not in ("L", "RGB"):
            img = img.convert("RGB")

        attempts = [img]

        w, h = img.size
        if max(w, h) < 800:
            scale = 800 / max(w, h)
            attempts.append(img.resize((int(w * scale), int(h * scale)), Image.LANCZOS))

        attempts.append(img.convert("L"))

        for candidate in attempts:
            try:
                decoded = pyzbar_decode(candidate)
            except Exception:
                decoded = None
            if decoded:
                return decoded[0].data.decode("utf-8", errors="replace")

        return None
    except Exception as e:
        logger.exception("_decode_qr error: %s", e)
        return None


async def _upload_to_x0(data: bytes, filename: str, content_type: str = "image/png") -> str:
    if not AIOHTTP_OK:
        return ""
    try:
        form = aiohttp.FormData()
        form.add_field("file", data, filename=filename, content_type=content_type)
        async with aiohttp.ClientSession() as s:
            async with s.post(
                "https://x0.at",
                data=form,
                timeout=aiohttp.ClientTimeout(total=60),
            ) as r:
                text = (await r.text()).strip()
                if text.startswith("http"):
                    return text
    except Exception as e:
        logger.exception("_upload_to_x0 error: %s", e)
    return ""


@loader.tds
class SwiftQR(loader.Module):
    """QR code generator and analyzer"""

    strings = {
        "name": "SwiftQR",

        "qr_menu": (
            "<b>SwiftQR</b>\n"
            "<blockquote>QR code generated</blockquote>"
        ),

        "generating": "<b>Generating QR code...</b>",

        "analyzing": "<b>Analyzing QR code...</b>",

        "decoded": (
            "<b>QR Code Content</b>\n"
            "<blockquote>{content}</blockquote>"
        ),

        "no_qr_found": (
            "<b>QR Code Not Found</b>\n"
            "<blockquote>No QR code detected in image</blockquote>\n"
            "<blockquote>If this happens on every image, install the missing "
            "system library first: <code>apt install -y libzbar0</code></blockquote>"
        ),

        "no_reply": (
            "<b>Error</b>\n"
            "<blockquote>Reply to an image with QR code</blockquote>"
        ),

        "no_text": (
            "<b>Error</b>\n"
            "<blockquote>Provide text to generate QR code</blockquote>"
        ),

        "generation_failed": (
            "<b>Generation Failed</b>\n"
            "<blockquote>Could not generate QR code</blockquote>"
        ),

        "analysis_failed": (
            "<b>Analysis Failed</b>\n"
            "<blockquote>Could not decode QR code</blockquote>"
        ),

        "btn_new_qr": "New QR Code",
        "btn_close": "Close",

        "input_new_text": "Enter text for new QR code:",
    }

    strings_ru = {
        "qr_menu": (
            "<b>SwiftQR</b>\n"
            "<blockquote>QR код сгенерирован</blockquote>"
        ),

        "generating": "<b>Генерация QR кода...</b>",

        "analyzing": "<b>Анализ QR кода...</b>",

        "decoded": (
            "<b>Содержимое QR кода</b>\n"
            "<blockquote>{content}</blockquote>"
        ),

        "no_qr_found": (
            "<b>QR код не найден</b>\n"
            "<blockquote>QR код не обнаружен на изображении</blockquote>\n"
            "<blockquote>Если это происходит на любом изображении, сначала "
            "установите системную библиотеку: <code>apt install -y libzbar0</code></blockquote>"
        ),

        "no_reply": (
            "<b>Ошибка</b>\n"
            "<blockquote>Ответьте на изображение с QR кодом</blockquote>"
        ),

        "no_text": (
            "<b>Ошибка</b>\n"
            "<blockquote>Укажите текст для генерации QR кода</blockquote>"
        ),

        "generation_failed": (
            "<b>Генерация не удалась</b>\n"
            "<blockquote>Не удалось создать QR код</blockquote>"
        ),

        "analysis_failed": (
            "<b>Анализ не удался</b>\n"
            "<blockquote>Не удалось декодировать QR код</blockquote>"
        ),

        "btn_new_qr": "Новый QR код",
        "btn_close": "Закрыть",

        "input_new_text": "Введите текст для нового QR кода:",
    }

    def __init__(self):
        self._sessions = {}

    async def client_ready(self, client, db):
        self._client = client
        self._db = db

    def _markup(self, session_id: str):
        return [
            [{
                "text": self.strings["btn_new_qr"],
                "input": self.strings["input_new_text"],
                "handler": self._cb_new_qr_input,
                "args": (session_id,),
                "style": "primary",
            }],
            [{"text": self.strings["btn_close"], "callback": self._cb_close, "args": (session_id,), "style": "danger"}],
        ]

    async def _cb_new_qr_input(self, call: InlineCall, text: str, session_id: str):
        text = text.strip()

        if not text:
            await call.edit(self.strings["no_text"])
            return

        await call.edit(self.strings["generating"])

        qr_data = await _generate_qr(text)

        if not qr_data:
            await call.edit(
                self.strings["generation_failed"],
                reply_markup=self._markup(session_id),
            )
            return

        qr_url = await _upload_to_x0(qr_data, "qr.png", "image/png")

        edit_kwargs = dict(
            text=self.strings["qr_menu"],
            reply_markup=self._markup(session_id),
        )

        if qr_url:
            edit_kwargs["photo"] = qr_url

        await call.edit(**edit_kwargs)

    async def _cb_close(self, call: InlineCall, session_id: str):
        self._sessions.pop(session_id, None)
        await call.delete()

    @loader.command(
        ru_doc="[текст] - сгенерировать QR код",
        en_doc="[text] - generate QR code",
    )
    async def gqr(self, message: Message):
        """[text] - generate QR code"""
        text = utils.get_args_raw(message).strip()

        if not text:
            await utils.answer(message, self.strings["no_text"])
            return

        status = await utils.answer(message, self.strings["generating"])

        qr_data = await _generate_qr(text)

        if not qr_data:
            await utils.answer(status, self.strings["generation_failed"])
            return

        qr_url = await _upload_to_x0(qr_data, "qr.png", "image/png")

        session_id = str(id(message))
        self._sessions[session_id] = {"chat_id": message.chat_id}

        try:
            await status.delete()
        except Exception:
            pass

        form_kwargs = dict(
            text=self.strings["qr_menu"],
            message=message,
            reply_markup=self._markup(session_id),
            silent=True,
        )

        if qr_url:
            form_kwargs["photo"] = qr_url

        await self.inline.form(**form_kwargs)

    @loader.command(
        ru_doc="Анализировать QR код (реплай на изображение)",
        en_doc="Analyze QR code (reply to image)",
    )
    async def aqr(self, message: Message):
        """Analyze QR code (reply to image)"""
        reply = await message.get_reply_message()

        if not reply or not reply.photo:
            await utils.answer(message, self.strings["no_reply"])
            return

        status = await utils.answer(message, self.strings["analyzing"])

        try:
            image_data = await reply.download_media(bytes)
        except Exception as e:
            logger.exception("download_media error: %s", e)
            await utils.answer(status, self.strings["analysis_failed"])
            return

        if not image_data:
            await utils.answer(status, self.strings["analysis_failed"])
            return

        content = await _decode_qr(image_data)

        if not content:
            await utils.answer(status, self.strings["no_qr_found"])
            return

        await utils.answer(
            status,
            self.strings["decoded"].format(content=_escape(content))
        )