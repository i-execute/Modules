# CopyLeft 2026 github.com/i-execute
# Author: I_execute.t.me
# Licensed under AGPLv3.

__version__ = (1, 0, 0)
# meta developer: Execute_forge.t.me

import asyncio
import html
import logging

from telethon import functions
from telethon.tl.types import DocumentAttributeAudio, DocumentAttributeVideo, MessageMediaDocument, Message

from .. import loader, utils

logger = logging.getLogger("Voice2Text")

POLL_STEP = 1.5
POLL_LIMIT = 24


@loader.tds
class Voice2Text(loader.Module):
    strings = {
        "name": "Voice2Text",
        "_cls_doc": "Transcribe voice and round video messages with the built-in premium engine",
        "_cmd_doc_vtt": "Reply to a voice or round video message - transcribe it. Premium accounts only",
        "premium_only": (
            "<b>Telegram Premium required</b>\n"
            "<blockquote>Voice transcription is a premium account feature.</blockquote>"
        ),
        "need_reply": (
            "<b>No media</b>\n"
            "<blockquote>Reply to a voice or round video message first.</blockquote>"
        ),
        "wrong_media": (
            "<b>Nothing to transcribe</b>\n"
            "<blockquote>The reply contains neither a voice message nor a round video.</blockquote>"
        ),
        "working": "<b>Voice2Text</b>\n<blockquote>Transcribing, please wait...</blockquote>",
        "result": "<b>Voice2Text</b>\n<blockquote expandable>{text}</blockquote>",
        "empty_result": "<b>Voice2Text</b>\n<blockquote>The engine returned an empty transcript.</blockquote>",
        "timed_out": "<b>Voice2Text</b>\n<blockquote>Transcription took too long. Try again in a moment.</blockquote>",
        "failed": "<b>Voice2Text</b>\n<blockquote>Failed: <code>{error}</code></blockquote>",
    }

    strings_ru = {
        "name": "Voice2Text",
        "_cls_doc": "Расшифровка голосовых и кружков встроенным премиум-движком",
        "_cmd_doc_vtt": "Реплай на голосовое или кружок - расшифровать. Только для премиум-аккаунтов",
        "premium_only": (
            "<b>Нужен Telegram Premium</b>\n"
            "<blockquote>Расшифровка голосовых - функция премиум-аккаунтов.</blockquote>"
        ),
        "need_reply": (
            "<b>Нет медиа</b>\n"
            "<blockquote>Сначала ответьте на голосовое или кружок.</blockquote>"
        ),
        "wrong_media": (
            "<b>Нечего расшифровывать</b>\n"
            "<blockquote>В ответе нет ни голосового, ни кружка.</blockquote>"
        ),
        "working": "<b>Voice2Text</b>\n<blockquote>Расшифровываю, подождите...</blockquote>",
        "result": "<b>Voice2Text</b>\n<blockquote expandable>{text}</blockquote>",
        "empty_result": "<b>Voice2Text</b>\n<blockquote>Движок вернул пустую расшифровку.</blockquote>",
        "timed_out": "<b>Voice2Text</b>\n<blockquote>Расшифровка затянулась. Попробуйте чуть позже.</blockquote>",
        "failed": "<b>Voice2Text</b>\n<blockquote>Ошибка: <code>{error}</code></blockquote>",
    }

    async def client_ready(self, client, db):
        self._client = client
        self._db = db

    def _pick_media_type(self, msg):
        media = getattr(msg, "media", None)
        doc = getattr(media, "document", None) if isinstance(media, MessageMediaDocument) else None
        if doc is None:
            return None
        for a in doc.attributes or []:
            if isinstance(a, DocumentAttributeAudio) and a.voice:
                return "voice"
            if isinstance(a, DocumentAttributeVideo) and getattr(a, "round_message", False):
                return "round"
        return None

    async def _transcribe_msg(self, reply):
        peer = await self._client.get_input_entity(utils.get_chat_id(reply))
        try:
            req_cls = functions.account.TranscribeAudioRequest
        except AttributeError:
            req_cls = functions.messages.TranscribeAudioRequest
        for _ in range(POLL_LIMIT):
            resp = await self._client(req_cls(peer=peer, msg_id=reply.id))
            text = getattr(resp, "text", "") or ""
            if text.strip() and not getattr(resp, "pending", False):
                return text.strip()
            if not getattr(resp, "pending", False) and text.strip():
                return text.strip()
            await asyncio.sleep(POLL_STEP)
        return None

    @loader.command(
        ru_doc="Реплай на голосовое или кружок - расшифровать премиум-движком. Доступно только премиум-аккаунтам",
        en_doc="Reply to a voice or round video message - transcribe it with the premium engine. Premium accounts only",
    )
    async def vttcmd(self, message: Message):
        """Reply to a voice or round video message - transcribe it with the premium engine. Premium accounts only"""
        me = await self._client.get_me()
        if not getattr(me, "premium", False):
            await utils.answer(message, self.strings["premium_only"])
            return
        reply = await message.get_reply_message()
        if not reply or not reply.media:
            await utils.answer(message, self.strings["need_reply"])
            return
        if not self._pick_media_type(reply):
            await utils.answer(message, self.strings["wrong_media"])
            return
        message = await utils.answer(message, self.strings["working"])
        try:
            text = await self._transcribe_msg(reply)
        except Exception as e:
            logger.exception("[V2T] transcribe failed")
            await utils.answer(message, self.strings["failed"].format(error=html.escape(str(e)[:300], quote=True)))
            return
        if text is None:
            await utils.answer(message, self.strings["timed_out"])
            return
        if not text:
            await utils.answer(message, self.strings["empty_result"])
            return
        await utils.answer(message, self.strings["result"].format(text=html.escape(text)))
