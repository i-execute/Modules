__version__ = (1, 0, 0)
# meta developer: I_execute.t.me

from telethon.tl.types import Message
from telethon.tl.functions.payments import GetUniqueStarGiftRequest
import io
import re

from .. import loader, utils

NFT_LINK_PATTERN = re.compile(r'(?:https?://t\.me/nft/)?([A-Za-z][\w-]+-\d+)$')


def _escape(text):
    if not text:
        return ""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _parse_nft_slug(text):
    text = text.strip()
    m = NFT_LINK_PATTERN.match(text)
    if m:
        return m.group(1)
    if re.match(r'^[A-Za-z][\w-]+-\d+$', text):
        return text
    return None


def _is_custom_emoji_doc(doc):
    from telethon.tl.types import DocumentAttributeCustomEmoji
    for attr in getattr(doc, 'attributes', []):
        if isinstance(attr, DocumentAttributeCustomEmoji):
            return True
    return False


@loader.tds
class Gift2Sticker(loader.Module):
    """Convert NFT gift link to sticker"""

    strings = {
        "name": "Gift2Sticker",

        "loading": "<b>Loading...</b>",

        "gift_info": (
            "<b>NFT Gift: {title}</b>\n"
            "<blockquote>"
            "Slug: {slug}\n"
            "Model: {model}\n"
            "Pattern: {pattern}\n"
            "Backdrop: {backdrop}"
            "</blockquote>"
        ),

        "err_no_arg": (
            "<b>Error</b>\n"
            "<blockquote>Provide a gift link or slug</blockquote>"
        ),

        "err_invalid_link": (
            "<b>Error</b>\n"
            "<blockquote>Invalid gift link or slug</blockquote>"
        ),

        "err_not_found": (
            "<b>Error</b>\n"
            "<blockquote>Gift not found</blockquote>"
        ),

        "err_no_sticker": (
            "<b>Error</b>\n"
            "<blockquote>No sticker found for this gift</blockquote>"
        ),

        "err_send_failed": (
            "<b>Error</b>\n"
            "<blockquote>Failed to send sticker</blockquote>"
        ),
    }

    strings_ru = {
        "loading": "<b>Загрузка...</b>",

        "gift_info": (
            "<b>NFT подарок: {title}</b>\n"
            "<blockquote>"
            "Slug: {slug}\n"
            "Модель: {model}\n"
            "Паттерн: {pattern}\n"
            "Фон: {backdrop}"
            "</blockquote>"
        ),

        "err_no_arg": (
            "<b>Ошибка</b>\n"
            "<blockquote>Укажите ссылку или slug подарка</blockquote>"
        ),

        "err_invalid_link": (
            "<b>Ошибка</b>\n"
            "<blockquote>Неверная ссылка или slug подарка</blockquote>"
        ),

        "err_not_found": (
            "<b>Ошибка</b>\n"
            "<blockquote>Подарок не найден</blockquote>"
        ),

        "err_no_sticker": (
            "<b>Ошибка</b>\n"
            "<blockquote>Стикер для этого подарка не найден</blockquote>"
        ),

        "err_send_failed": (
            "<b>Ошибка</b>\n"
            "<blockquote>Не удалось отправить стикер</blockquote>"
        ),
    }

    async def client_ready(self, client, db):
        self._client = client

    async def _send_sticker_doc(self, chat_id, doc, reply_id=None):
        from telethon.tl.types import (
            DocumentAttributeCustomEmoji,
            DocumentAttributeImageSize,
            DocumentAttributeSticker,
            InputStickerSetEmpty,
        )

        is_ce = _is_custom_emoji_doc(doc)

        if not is_ce:
            try:
                data = await self._client.download_media(doc, bytes)
                if not data:
                    return False
                f = io.BytesIO(data)
                f.name = "sticker.tgs"
                await self._client.send_file(chat_id, f, reply_to=reply_id)
                return True
            except Exception:
                return False

        try:
            data = await self._client.download_media(doc, bytes)
            if not data:
                return False

            f = io.BytesIO(data)
            f.name = "sticker.tgs"

            alt_emoji = ""
            for attr in getattr(doc, 'attributes', []):
                if isinstance(attr, DocumentAttributeCustomEmoji):
                    alt_emoji = getattr(attr, 'alt', '') or ''

            attrs = [
                DocumentAttributeImageSize(w=512, h=512),
                DocumentAttributeSticker(
                    alt=alt_emoji or '',
                    stickerset=InputStickerSetEmpty(),
                    mask=None,
                    mask_coords=None,
                ),
            ]

            await self._client.send_file(
                chat_id, f,
                reply_to=reply_id,
                attributes=attrs,
                force_document=False,
            )
            return True
        except Exception:
            try:
                data = await self._client.download_media(doc, bytes)
                if data:
                    f = io.BytesIO(data)
                    f.name = "sticker.tgs"
                    await self._client.send_file(
                        chat_id, f,
                        reply_to=reply_id,
                        force_document=True,
                    )
                    return True
            except Exception:
                pass
            return False

    @loader.command(
        ru_doc="[ссылка] - конвертировать NFT подарок в стикер",
        en_doc="[link] - convert NFT gift to sticker",
    )
    async def gts(self, message: Message):
        """[link] - convert NFT gift to sticker"""
        args = utils.get_args_raw(message).strip()

        if not args:
            await utils.answer(message, self.strings["err_no_arg"])
            return

        slug = _parse_nft_slug(args)
        if not slug:
            await utils.answer(message, self.strings["err_invalid_link"])
            return

        status = await utils.answer(message, self.strings["loading"])

        try:
            r = await self._client(GetUniqueStarGiftRequest(slug=slug))
            gift = r.gift if hasattr(r, 'gift') else r
        except Exception:
            await utils.answer(status, self.strings["err_not_found"])
            return

        model_name = "?"
        pattern_name = "?"
        backdrop_name = "?"
        model_doc = None

        for attr in (getattr(gift, 'attributes', None) or []):
            atype = type(attr).__name__
            if atype == "StarGiftAttributeModel":
                model_name = getattr(attr, 'name', '?')
                model_doc = getattr(attr, 'document', None)
            elif atype == "StarGiftAttributePattern":
                pattern_name = getattr(attr, 'name', '?')
            elif atype == "StarGiftAttributeBackdrop":
                backdrop_name = getattr(attr, 'name', '?')

        if not model_doc:
            await utils.answer(status, self.strings["err_no_sticker"])
            return

        reply_id = None
        try:
            reply = await message.get_reply_message()
            if reply:
                reply_id = reply.id
        except Exception:
            pass

        ok = await self._send_sticker_doc(message.chat_id, model_doc, reply_id)

        if not ok:
            await utils.answer(status, self.strings["err_send_failed"])
            return

        await utils.answer(
            status,
            self.strings["gift_info"].format(
                title=_escape(getattr(gift, 'title', '?')),
                slug=_escape(slug),
                model=_escape(model_name),
                pattern=_escape(pattern_name),
                backdrop=_escape(backdrop_name),
            ),
        )