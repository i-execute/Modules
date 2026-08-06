__version__ = (1, 1, 0)
# meta developer: I_execute.t.me

from telethon.tl.types import Message
from telethon.tl.functions.payments import GetUniqueStarGiftRequest, GetSavedStarGiftsRequest, GetStarGiftsRequest
from telethon.tl.functions.payments import GetPaymentFormRequest
from telethon.tl.types import InputPeerSelf, TextWithEntities
from telethon.tl.types import InputInvoiceStarGift
import io
import re
import asyncio

from .. import loader, utils
from ..inline.types import InlineCall

NFT_LINK_PATTERN = re.compile(r'(?:https?://)?(?:t\.me/nft/)?([A-Za-z][\w-]+-\d+)$')
GIFT_ID_PATTERN = re.compile(r'^\d{19}$')


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

        "main_menu": (
            "<b>Gift2Sticker</b>\n"
            "<blockquote>Choose input method</blockquote>"
        ),

        "loading": "<b>Loading...</b>",

        "sending_progress": (
            "<b>Sending stickers</b>\n"
            "<blockquote>{current}/{total} sent</blockquote>"
        ),

        "done": (
            "<b>Done</b>\n"
            "<blockquote>{sent} sticker(s) sent</blockquote>"
        ),

        "gift_info": (
            "<b>NFT Gift: {title}</b>\n"
            "<blockquote>"
            "Slug: {slug}\n"
            "Model: {model}\n"
            "Pattern: {pattern}\n"
            "Backdrop: {backdrop}"
            "</blockquote>"
        ),

        "hidden_gift_info": (
            "<b>Hidden Gift</b>\n"
            "<blockquote>"
            "ID: <code>{id}</code>\n"
            "Price: {stars} stars"
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

        "err_no_gifts": (
            "<b>Error</b>\n"
            "<blockquote>No gifts found</blockquote>"
        ),

        "err_invalid_index": (
            "<b>Error</b>\n"
            "<blockquote>Invalid index. Valid range: 0-{max}</blockquote>"
        ),

        "err_invalid_id": (
            "<b>Error</b>\n"
            "<blockquote>Invalid gift ID (must be 19 digits)</blockquote>"
        ),

        "err_user_not_found": (
            "<b>Error</b>\n"
            "<blockquote>User not found</blockquote>"
        ),

        "err_no_saved_gifts": (
            "<b>Error</b>\n"
            "<blockquote>User has no saved NFT gifts</blockquote>"
        ),

        "stopped": (
            "<b>Stopped</b>\n"
            "<blockquote>{sent} sticker(s) sent</blockquote>"
        ),

        "input_nft_link": "Enter NFT link or slug:",
        "input_index": "Enter gift index:",
        "input_gift_id": "Enter gift ID:",
        "input_username": "Enter username or ID:",

        "btn_nft_link": "NFT Link",
        "btn_index": "Index",
        "btn_gift_id": "Gift ID",
        "btn_from_profile": "From Profile",
        "btn_back": "Back",
        "btn_kill": "Kill",
        "btn_new_sticker": "New Sticker",
        "btn_close": "Close",
    }

    strings_ru = {
        "main_menu": (
            "<b>Gift2Sticker</b>\n"
            "<blockquote>Выберите способ ввода</blockquote>"
        ),

        "loading": "<b>Загрузка...</b>",

        "sending_progress": (
            "<b>Отправка стикеров</b>\n"
            "<blockquote>Отправлено {current}/{total}</blockquote>"
        ),

        "done": (
            "<b>Готово</b>\n"
            "<blockquote>Отправлено {sent} стикер(ов)</blockquote>"
        ),

        "gift_info": (
            "<b>NFT подарок: {title}</b>\n"
            "<blockquote>"
            "Slug: {slug}\n"
            "Модель: {model}\n"
            "Паттерн: {pattern}\n"
            "Фон: {backdrop}"
            "</blockquote>"
        ),

        "hidden_gift_info": (
            "<b>Скрытый подарок</b>\n"
            "<blockquote>"
            "ID: <code>{id}</code>\n"
            "Цена: {stars} звезд"
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

        "err_no_gifts": (
            "<b>Ошибка</b>\n"
            "<blockquote>Подарки не найдены</blockquote>"
        ),

        "err_invalid_index": (
            "<b>Ошибка</b>\n"
            "<blockquote>Неверный индекс. Допустимый диапазон: 0-{max}</blockquote>"
        ),

        "err_invalid_id": (
            "<b>Ошибка</b>\n"
            "<blockquote>Неверный ID подарка (должен быть 19 цифр)</blockquote>"
        ),

        "err_user_not_found": (
            "<b>Ошибка</b>\n"
            "<blockquote>Пользователь не найден</blockquote>"
        ),

        "err_no_saved_gifts": (
            "<b>Ошибка</b>\n"
            "<blockquote>У пользователя нет сохранённых NFT подарков</blockquote>"
        ),

        "stopped": (
            "<b>Остановлено</b>\n"
            "<blockquote>Отправлено {sent} стикер(ов)</blockquote>"
        ),

        "input_nft_link": "Введите NFT ссылку или slug:",
        "input_index": "Введите индекс подарка:",
        "input_gift_id": "Введите ID подарка:",
        "input_username": "Введите username или ID:",

        "btn_nft_link": "NFT Link",
        "btn_index": "Index",
        "btn_gift_id": "Gift ID",
        "btn_from_profile": "From Profile",
        "btn_back": "Назад",
        "btn_kill": "Стоп",
        "btn_new_sticker": "Новый стикер",
        "btn_close": "Закрыть",
    }

    def __init__(self):
        self._kill_flag = False
        self._chat_id = None
        self._reply_id = None

    async def client_ready(self, client, db):
        self._client = client

    def _main_menu_markup(self):
        return [
            [
                {"text": self.strings["btn_nft_link"], "callback": self._cb_nft_link, "style": "primary"},
                {"text": self.strings["btn_index"], "callback": self._cb_index, "style": "primary"},
            ],
            [
                {"text": self.strings["btn_gift_id"], "callback": self._cb_gift_id, "style": "primary"},
                {"text": self.strings["btn_from_profile"], "callback": self._cb_from_profile, "style": "primary"},
            ],
        ]

    async def _cb_main_menu(self, call: InlineCall):
        self._kill_flag = False
        await call.edit(
            self.strings["main_menu"],
            reply_markup=self._main_menu_markup(),
        )

    async def _cb_nft_link(self, call: InlineCall):
        await call.edit(
            self.strings["main_menu"],
            reply_markup=[
                [{"text": self.strings["input_nft_link"], "input": self.strings["input_nft_link"], "handler": self._handle_nft_link, "style": "primary"}],
                [{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}],
            ],
        )

    async def _cb_index(self, call: InlineCall):
        await call.edit(
            self.strings["main_menu"],
            reply_markup=[
                [{"text": self.strings["input_index"], "input": self.strings["input_index"], "handler": self._handle_index, "style": "primary"}],
                [{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}],
            ],
        )

    async def _cb_gift_id(self, call: InlineCall):
        await call.edit(
            self.strings["main_menu"],
            reply_markup=[
                [{"text": self.strings["input_gift_id"], "input": self.strings["input_gift_id"], "handler": self._handle_gift_id, "style": "primary"}],
                [{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}],
            ],
        )

    async def _cb_from_profile(self, call: InlineCall):
        await call.edit(
            self.strings["main_menu"],
            reply_markup=[
                [{"text": self.strings["input_username"], "input": self.strings["input_username"], "handler": self._handle_from_profile, "style": "primary"}],
                [{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}],
            ],
        )

    async def _cb_kill(self, call: InlineCall):
        self._kill_flag = True
        await call.answer("Stopping...")

    def _done_markup(self):
        return [
            [
                {"text": self.strings["btn_new_sticker"], "callback": self._cb_main_menu, "style": "success"},
                {"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"},
            ],
        ]

    async def _cb_close(self, call: InlineCall):
        await call.delete()

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

    async def _handle_nft_link(self, call: InlineCall, value: str):
        value = value.strip()
        slug = _parse_nft_slug(value)
        if not slug:
            await call.edit(
                self.strings["err_invalid_link"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]],
            )
            return

        await call.edit(
            self.strings["loading"],
            reply_markup=[],
        )

        try:
            r = await self._client(GetUniqueStarGiftRequest(slug=slug))
            gift = r.gift if hasattr(r, 'gift') else r
        except Exception:
            await call.edit(
                self.strings["err_not_found"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]],
            )
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
            await call.edit(
                self.strings["err_no_sticker"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]],
            )
            return

        self._kill_flag = False
        await call.edit(
            self.strings["sending_progress"].format(current=0, total=1),
            reply_markup=[[{"text": self.strings["btn_kill"], "callback": self._cb_kill, "style": "danger"}]],
        )

        ok = await self._send_sticker_doc(self._chat_id, model_doc, self._reply_id)

        if not ok:
            await call.edit(
                self.strings["err_send_failed"],
                reply_markup=self._done_markup(),
            )
            return

        await call.edit(
            self.strings["gift_info"].format(
                title=_escape(getattr(gift, 'title', '?')),
                slug=_escape(slug),
                model=_escape(model_name),
                pattern=_escape(pattern_name),
                backdrop=_escape(backdrop_name),
            ),
            reply_markup=self._done_markup(),
        )

    async def _handle_index(self, call: InlineCall, value: str):
        value = value.strip()
        try:
            idx = int(value)
        except ValueError:
            await call.edit(
                self.strings["err_invalid_index"].format(max="?"),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]],
            )
            return

        await call.edit(self.strings["loading"], reply_markup=[])

        try:
            result = await self._client(GetStarGiftsRequest(hash=0))
            gifts = result.gifts
        except Exception:
            await call.edit(
                self.strings["err_no_gifts"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]],
            )
            return

        if not gifts:
            await call.edit(
                self.strings["err_no_gifts"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]],
            )
            return

        max_idx = len(gifts) - 1
        if idx < 0 or idx > max_idx:
            await call.edit(
                self.strings["err_invalid_index"].format(max=max_idx),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]],
            )
            return

        selected = gifts[idx]
        sticker = getattr(selected, 'sticker', None)
        if not sticker:
            await call.edit(
                self.strings["err_no_sticker"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]],
            )
            return

        self._kill_flag = False
        await call.edit(
            self.strings["sending_progress"].format(current=0, total=1),
            reply_markup=[[{"text": self.strings["btn_kill"], "callback": self._cb_kill, "style": "danger"}]],
        )

        ok = await self._send_sticker_doc(self._chat_id, sticker, self._reply_id)

        if not ok:
            await call.edit(
                self.strings["err_send_failed"],
                reply_markup=self._done_markup(),
            )
            return

        await call.edit(
            self.strings["done"].format(sent=1),
            reply_markup=self._done_markup(),
        )

    async def _handle_gift_id(self, call: InlineCall, value: str):
        value = value.strip()
        if not GIFT_ID_PATTERN.match(value):
            await call.edit(
                self.strings["err_invalid_id"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]],
            )
            return

        gift_id = int(value)
        await call.edit(self.strings["loading"], reply_markup=[])

        try:
            result = await self._client(GetStarGiftsRequest(hash=0))
            gifts = result.gifts or []
        except Exception:
            gifts = []

        sticker = None
        stars = None
        for g in gifts:
            if g.id == gift_id:
                sticker = getattr(g, 'sticker', None)
                stars = getattr(g, 'stars', None)
                break

        if sticker:
            self._kill_flag = False
            await call.edit(
                self.strings["sending_progress"].format(current=0, total=1),
                reply_markup=[[{"text": self.strings["btn_kill"], "callback": self._cb_kill, "style": "danger"}]],
            )
            ok = await self._send_sticker_doc(self._chat_id, sticker, self._reply_id)
            if not ok:
                await call.edit(
                    self.strings["err_send_failed"],
                    reply_markup=self._done_markup(),
                )
                return
            await call.edit(
                self.strings["done"].format(sent=1),
                reply_markup=self._done_markup(),
            )
            return

        try:
            me_input = await self._client.get_input_entity(self._client._self_id)
            inv = InputInvoiceStarGift(me_input, gift_id, message=TextWithEntities("", []))
            form = await self._client(GetPaymentFormRequest(inv))
            price = None
            if hasattr(form, 'invoice') and form.invoice and form.invoice.prices:
                price = form.invoice.prices[0].amount
            await call.edit(
                self.strings["hidden_gift_info"].format(id=gift_id, stars=price if price is not None else "?"),
                reply_markup=self._done_markup(),
            )
            return
        except Exception as e:
            err = str(e)
            if "STARGIFT_INVALID" in err:
                await call.edit(
                    self.strings["err_not_found"],
                    reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]],
                )
                return

        await call.edit(
            self.strings["err_not_found"],
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]],
        )

    async def _handle_from_profile(self, call: InlineCall, value: str):
        value = value.strip()
        await call.edit(self.strings["loading"], reply_markup=[])

        try:
            target = int(value) if value.lstrip("-").isdigit() else value
            entity = await self._client.get_entity(target)
        except Exception:
            await call.edit(
                self.strings["err_user_not_found"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]],
            )
            return

        try:
            peer = await self._client.get_input_entity(entity.id)
        except Exception:
            await call.edit(
                self.strings["err_user_not_found"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]],
            )
            return

        all_gifts = []
        offset = ""
        while True:
            try:
                r = await self._client(GetSavedStarGiftsRequest(peer=peer, offset=offset, limit=10))
                if not r.gifts:
                    break
                all_gifts.extend(r.gifts)
                if len(r.gifts) < 10:
                    break
                offset = getattr(r, 'next_offset', "") or ""
                if not offset:
                    break
                await asyncio.sleep(0.3)
            except Exception:
                break

        unique_gifts = []
        for sg in all_gifts:
            gift = sg.gift
            if type(gift).__name__ == "StarGiftUnique":
                unique_gifts.append(gift)

        if not unique_gifts:
            await call.edit(
                self.strings["err_no_saved_gifts"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]],
            )
            return

        total = len(unique_gifts)
        self._kill_flag = False

        await call.edit(
            self.strings["sending_progress"].format(current=0, total=total),
            reply_markup=[[{"text": self.strings["btn_kill"], "callback": self._cb_kill, "style": "danger"}]],
        )

        sent = 0
        sent_ids = set()

        for gift in unique_gifts:
            if self._kill_flag:
                await call.edit(
                    self.strings["stopped"].format(sent=sent),
                    reply_markup=self._done_markup(),
                )
                self._kill_flag = False
                return

            gid = getattr(gift, 'id', None)
            if gid in sent_ids:
                continue
            sent_ids.add(gid)

            slug = getattr(gift, 'slug', None)
            if not slug:
                continue

            try:
                r = await self._client(GetUniqueStarGiftRequest(slug=slug))
                g = r.gift if hasattr(r, 'gift') else r
                attrs = getattr(g, 'attributes', None) or []
                for attr in attrs:
                    if type(attr).__name__ == "StarGiftAttributeModel":
                        doc = getattr(attr, 'document', None)
                        if doc:
                            ok = await self._send_sticker_doc(self._chat_id, doc, self._reply_id)
                            if ok:
                                sent += 1
                        break
            except Exception:
                pass

            await call.edit(
                self.strings["sending_progress"].format(current=sent, total=total),
                reply_markup=[[{"text": self.strings["btn_kill"], "callback": self._cb_kill, "style": "danger"}]],
            )

            if sent % 5 == 0 and sent > 0:
                await asyncio.sleep(1)
            else:
                await asyncio.sleep(0.3)

        await call.edit(
            self.strings["done"].format(sent=sent),
            reply_markup=self._done_markup(),
        )

    @loader.command(
        ru_doc="Конвертировать NFT подарок в стикер",
        en_doc="Convert NFT gift to sticker",
    )
    async def gts(self, message: Message):
        """Convert NFT gift to sticker"""
        self._kill_flag = False
        self._chat_id = message.chat_id

        try:
            reply = await message.get_reply_message()
            self._reply_id = reply.id if reply else None
        except Exception:
            self._reply_id = None

        await self.inline.form(
            text=self.strings["main_menu"],
            message=message,
            reply_markup=self._main_menu_markup(),
            silent=True,
        )