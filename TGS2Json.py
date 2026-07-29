__version__ = (1, 0, 0)
# meta developer: I_execute.t.me

import gzip
import io
import json

from telethon.tl.types import Message

from .. import loader, utils


def _tgs_bytes_to_json_bytes(data: bytes) -> bytes:
    raw = gzip.decompress(data)
    parsed = json.loads(raw)  # валидируем, что это действительно json/lottie
    return json.dumps(parsed, ensure_ascii=False).encode("utf-8")


def _json_bytes_to_tgs_bytes(data: bytes) -> bytes:
    parsed = json.loads(data)  # валидируем, что это валидный json/lottie
    raw = json.dumps(parsed, ensure_ascii=False).encode("utf-8")
    return gzip.compress(raw)


@loader.tds
class TGS2Json(loader.Module):
    """Convert .tgs sticker (gzip Lottie) to plain .json"""

    strings = {
        "name": "TGS2Json",

        "loading": "<b>Converting...</b>",

        "err_no_reply": (
            "<b>Error</b>\n"
            "<blockquote>Reply to a message with a sticker/.tgs file</blockquote>"
        ),

        "err_no_doc": (
            "<b>Error</b>\n"
            "<blockquote>No file found in the message</blockquote>"
        ),

        "err_download_failed": (
            "<b>Error</b>\n"
            "<blockquote>Failed to download the file</blockquote>"
        ),

        "err_not_tgs": (
            "<b>Error</b>\n"
            "<blockquote>File doesn't look like .tgs (gzip Lottie): {error}</blockquote>"
        ),

        "err_not_json": (
            "<b>Error</b>\n"
            "<blockquote>File doesn't look like valid json/Lottie: {error}</blockquote>"
        ),

        "success": "<b>Done, here's your .json</b>",
        "success_tgs": "<b>Done, here's your .tgs</b>",
    }

    strings_ru = {
        "loading": "<b>Конвертирую...</b>",

        "err_no_reply": (
            "<b>Ошибка</b>\n"
            "<blockquote>Ответь на сообщение со стикером/.tgs файлом</blockquote>"
        ),

        "err_no_doc": (
            "<b>Ошибка</b>\n"
            "<blockquote>В сообщении нет файла</blockquote>"
        ),

        "err_download_failed": (
            "<b>Ошибка</b>\n"
            "<blockquote>Не удалось скачать файл</blockquote>"
        ),

        "err_not_tgs": (
            "<b>Ошибка</b>\n"
            "<blockquote>Файл не похож на .tgs (gzip Lottie): {error}</blockquote>"
        ),

        "err_not_json": (
            "<b>Ошибка</b>\n"
            "<blockquote>Файл не похож на валидный json/Lottie: {error}</blockquote>"
        ),

        "success": "<b>Готово, держи .json</b>",
        "success_tgs": "<b>Готово, держи .tgs</b>",
    }

    async def client_ready(self, client, db):
        self._client = client

    @loader.command(
        ru_doc="(в ответ на .tgs/стикер) - сконвертировать в .json",
        en_doc="(reply to .tgs/sticker) - convert to .json",
    )
    async def tgstj(self, message: Message):
        """(reply to .tgs/sticker) - convert to .json"""
        reply = await message.get_reply_message()
        if not reply:
            await utils.answer(message, self.strings["err_no_reply"])
            return

        doc = getattr(reply, "document", None)
        if not doc:
            await utils.answer(message, self.strings["err_no_doc"])
            return

        status = await utils.answer(message, self.strings["loading"])

        try:
            data = await self._client.download_media(reply, bytes)
        except Exception:
            data = None

        if not data:
            await utils.answer(status, self.strings["err_download_failed"])
            return

        try:
            json_bytes = _tgs_bytes_to_json_bytes(data)
        except Exception as e:
            await utils.answer(status, self.strings["err_not_tgs"].format(error=str(e)))
            return

        base_name = "animation"
        for attr in getattr(doc, "attributes", []):
            fname = getattr(attr, "file_name", None)
            if fname:
                base_name = fname.rsplit(".", 1)[0]
                break

        f = io.BytesIO(json_bytes)
        f.name = f"{base_name}.json"

        await self._client.send_file(
            message.chat_id,
            f,
            reply_to=reply.id,
            force_document=True,
        )

        await utils.answer(status, self.strings["success"])

    @loader.command(
        ru_doc="(в ответ на .json) - сконвертировать в .tgs",
        en_doc="(reply to .json) - convert to .tgs",
    )
    async def jttgs(self, message: Message):
        """(reply to .json) - convert to .tgs"""
        reply = await message.get_reply_message()
        if not reply:
            await utils.answer(message, self.strings["err_no_reply"])
            return

        doc = getattr(reply, "document", None)
        if not doc:
            await utils.answer(message, self.strings["err_no_doc"])
            return

        status = await utils.answer(message, self.strings["loading"])

        try:
            data = await self._client.download_media(reply, bytes)
        except Exception:
            data = None

        if not data:
            await utils.answer(status, self.strings["err_download_failed"])
            return

        try:
            tgs_bytes = _json_bytes_to_tgs_bytes(data)
        except Exception as e:
            await utils.answer(status, self.strings["err_not_json"].format(error=str(e)))
            return

        base_name = "animation"
        for attr in getattr(doc, "attributes", []):
            fname = getattr(attr, "file_name", None)
            if fname:
                base_name = fname.rsplit(".", 1)[0]
                break

        f = io.BytesIO(tgs_bytes)
        f.name = f"{base_name}.tgs"

        await self._client.send_file(
            message.chat_id,
            f,
            reply_to=reply.id,
            force_document=True,
        )

        await utils.answer(status, self.strings["success_tgs"])