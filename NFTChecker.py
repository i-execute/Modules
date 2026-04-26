__version__ = (1, 3, 2)
# meta developer: I_execute.t.me
# requires: aiohttp, Pillow

import re
import io
import time
import logging

import aiohttp
from PIL import Image

from aiogram.types import (
    InlineQuery,
    InlineQueryResultArticle,
    InputTextMessageContent,
    LinkPreviewOptions,
)

from .. import loader, utils

logger = logging.getLogger(__name__)

REQUEST_OK = 200

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
}

BANNER = "https://raw.githubusercontent.com/i-execute/Modules/main/Assets/NFTChecker/Inline_query.png"

NFT_LINK_RE = re.compile(
    r"(?:https?://)?(?:t\.me/nft/|fragment\.com/gift/)"
    r"([A-Za-z][A-Za-z0-9]*)-(\d+)",
    re.IGNORECASE,
)


def _parse_nft_url(text: str) -> tuple[str, int] | None:
    text = text.strip()
    m = NFT_LINK_RE.search(text)
    if m:
        return m.group(1), int(m.group(2))
    parts = text.rsplit("-", 1)
    if len(parts) == 2 and parts[1].isdigit():
        return parts[0], int(parts[1])
    return None


async def _check_fragment(slug: str, num: int) -> tuple[bool | None, str]:
    url = f"https://fragment.com/gift/{slug.lower()}-{num}"
    try:
        async with aiohttp.ClientSession(headers=HEADERS) as s:
            async with s.get(
                url,
                timeout=aiohttp.ClientTimeout(total=15),
                allow_redirects=False,
            ) as r:
                if r.status == REQUEST_OK:
                    return True, url
                elif r.status in (301, 302, 303, 307, 308):
                    return False, url
                else:
                    return None, url
    except Exception as e:
        logger.error(f"[NFTCheck] fragment check error: {e}")
        return None, url


async def _download_webp(slug: str, num: int) -> bytes | None:
    url = f"https://nft.fragment.com/gift/{slug.lower()}-{num}.webp"
    try:
        async with aiohttp.ClientSession(headers=HEADERS) as s:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=20)) as r:
                if r.status != REQUEST_OK:
                    return None
                data = await r.read()
                return data if len(data) > 500 else None
    except Exception as e:
        logger.error(f"[NFTCheck] webp download error: {e}")
        return None


def _webp_to_jpeg(data: bytes) -> bytes | None:
    try:
        img = Image.open(io.BytesIO(data)).convert("RGB")
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=95)
        result = buf.getvalue()
        return result if len(result) > 500 else None
    except Exception as e:
        logger.error(f"[NFTCheck] webp->jpeg error: {e}")
        return None


async def _upload_to_x0(data: bytes, filename: str) -> str:
    try:
        form = aiohttp.FormData()
        form.add_field("file", data, filename=filename, content_type="image/jpeg")
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
        logger.error(f"[NFTCheck] x0.at upload error: {e}")
    return ""


async def _get_preview_url(slug: str, num: int) -> str:
    webp = await _download_webp(slug, num)
    if not webp:
        return ""
    jpeg = _webp_to_jpeg(webp)
    if not jpeg:
        return ""
    return await _upload_to_x0(jpeg, f"{slug.lower()}-{num}.jpg")


@loader.tds
class NFTChecker(loader.Module):
    """NFT Gift blockchain checker"""

    strings = {
        "name": "NFTChecker",

        "hint_title": "NFTChecker",
        "hint_desc": "Paste a t.me/nft/... or fragment.com/gift/... link",
        "hint_msg": (
            "<blockquote><b>NFTCheck</b></blockquote>\n"
            "<blockquote>Paste a NFT gift link to check if it was on blockchain.</blockquote>\n"
            "<blockquote>Supported formats:\n"
            "<code>t.me/nft/Name-123</code>\n"
            "<code>fragment.com/gift/name-123</code></blockquote>"
        ),

        "bad_url_title": "Cannot parse NFT link",
        "bad_url_desc": "Expected: t.me/nft/Name-123",
        "bad_url_msg": (
            "<blockquote><b>NFTCheck</b></blockquote>\n"
            "<blockquote>Cannot parse link.</blockquote>\n"
            "<blockquote>Expected format:\n"
            "<code>t.me/nft/Name-123</code></blockquote>"
        ),

        "result_yes_title": "{name} #{num} - WAS on blockchain",
        "result_yes_desc": "Cannot be used in crafting",
        "result_no_title": "{name} #{num} - NOT on blockchain",
        "result_no_desc": "Can be used in crafting",
        "result_unknown_title": "{name} #{num} - check failed",
        "result_unknown_desc": "Could not reach Fragment",

        "caption_yes": (
            "<blockquote><b>{name} #{num}</b></blockquote>\n"
            "<blockquote>On blockchain: <b>YES</b>\n"
            "Cannot be used in crafting</blockquote>\n"
            "<blockquote>View: <a href=\"https://t.me/nft/{name}-{num}\">Telegram</a></blockquote>"
        ),
        "caption_no": (
            "<blockquote><b>{name} #{num}</b></blockquote>\n"
            "<blockquote>On blockchain: <b>NO</b>\n"
            "Can be used in crafting</blockquote>\n"
            "<blockquote>View: <a href=\"https://t.me/nft/{name}-{num}\">Telegram</a></blockquote>"
        ),
        "caption_unknown": (
            "<blockquote><b>{name} #{num}</b></blockquote>\n"
            "<blockquote>Blockchain check: <b>FAILED</b>\n"
            "Could not reach Fragment</blockquote>\n"
            "<blockquote>View: <a href=\"https://t.me/nft/{name}-{num}\">Telegram</a></blockquote>"
        ),
    }

    strings_ru = {
        "hint_title": "NFTCheck",
        "hint_desc": "Вставь ссылку t.me/nft/... или fragment.com/gift/...",
        "hint_msg": (
            "<blockquote><b>NFTCheck</b></blockquote>\n"
            "<blockquote>Вставь ссылку на NFT подарок чтобы проверить был ли он в блокчейне.</blockquote>\n"
            "<blockquote>Поддерживаемые форматы:\n"
            "<code>t.me/nft/Name-123</code>\n"
            "<code>fragment.com/gift/name-123</code></blockquote>"
        ),

        "bad_url_title": "Не могу распарсить ссылку",
        "bad_url_desc": "Ожидается: t.me/nft/Name-123",
        "bad_url_msg": (
            "<blockquote><b>NFTCheck</b></blockquote>\n"
            "<blockquote>Не могу распарсить ссылку.</blockquote>\n"
            "<blockquote>Ожидаемый формат:\n"
            "<code>t.me/nft/Name-123</code></blockquote>"
        ),

        "result_yes_title": "{name} #{num} - БЫЛ в блокчейне",
        "result_yes_desc": "Нельзя использовать в крафте",
        "result_no_title": "{name} #{num} - НЕ был в блокчейне",
        "result_no_desc": "Можно использовать в крафте",
        "result_unknown_title": "{name} #{num} - проверка не удалась",
        "result_unknown_desc": "Не удалось связаться с Fragment",

        "caption_yes": (
            "<blockquote><b>{name} #{num}</b></blockquote>\n"
            "<blockquote>Был в блокчейне: <b>ДА</b>\n"
            "Нельзя использовать в крафте</blockquote>\n"
            "<blockquote>Смотреть: <a href=\"https://t.me/nft/{name}-{num}\">Telegram</a></blockquote>"
        ),
        "caption_no": (
            "<blockquote><b>{name} #{num}</b></blockquote>\n"
            "<blockquote>Был в блокчейне: <b>НЕТ</b>\n"
            "Можно использовать в крафте</blockquote>\n"
            "<blockquote>Смотреть: <a href=\"https://t.me/nft/{name}-{num}\">Telegram</a></blockquote>"
        ),
        "caption_unknown": (
            "<blockquote><b>{name} #{num}</b></blockquote>\n"
            "<blockquote>Проверка блокчейна: <b>ОШИБКА</b>\n"
            "Не удалось связаться с Fragment</blockquote>\n"
            "<blockquote>Смотреть: <a href=\"https://t.me/nft/{name}-{num}\">Telegram</a></blockquote>"
        ),
    }

    def __init__(self):
        self._preview_cache: dict[str, str] = {}

    async def client_ready(self, client, db):
        self._client = client
        self.inline_bot = self.inline.bot

    @loader.inline_handler(ru_doc="Проверка NFT подарка", en_doc="NFT gift blockchain check")
    async def nft_inline_handler(self, query: InlineQuery):
        """NFT gift blockchain check"""
        raw    = query.query.strip()
        prefix = "nft"
        text   = raw[len(prefix):].strip() if raw.lower().startswith(prefix) else raw.strip()

        if not text:
            await self._answer(query, [self._make_article(
                uid=f"hint_{int(time.time())}",
                title=self.strings["hint_title"],
                desc=self.strings["hint_desc"],
                text=self.strings["hint_msg"],
                thumb_url=BANNER,
            )])
            return

        parsed = _parse_nft_url(text)
        if not parsed:
            await self._answer(query, [self._make_article(
                uid=f"bad_{int(time.time())}",
                title=self.strings["bad_url_title"],
                desc=self.strings["bad_url_desc"],
                text=self.strings["bad_url_msg"],
                thumb_url=BANNER,
            )])
            return

        slug, num = parsed
        cache_key = f"{slug.lower()}-{num}"

        on_chain, _ = await _check_fragment(slug, num)

        if cache_key in self._preview_cache:
            preview_url = self._preview_cache[cache_key]
        else:
            preview_url = await _get_preview_url(slug, num)
            if preview_url:
                self._preview_cache[cache_key] = preview_url

        fmt = dict(name=slug, num=num)

        if on_chain is True:
            title   = self.strings["result_yes_title"].format(**fmt)
            desc    = self.strings["result_yes_desc"]
            caption = self.strings["caption_yes"].format(**fmt)
        elif on_chain is False:
            title   = self.strings["result_no_title"].format(**fmt)
            desc    = self.strings["result_no_desc"]
            caption = self.strings["caption_no"].format(**fmt)
        else:
            title   = self.strings["result_unknown_title"].format(**fmt)
            desc    = self.strings["result_unknown_desc"]
            caption = self.strings["caption_unknown"].format(**fmt)

        await self._answer(query, [self._make_article(
            uid=f"nft_{cache_key}_{int(time.time())}",
            title=title,
            desc=desc,
            text=caption,
            preview_url=preview_url if preview_url else None,
            thumb_url=preview_url if preview_url else BANNER,
        )])

    def _make_article(
        self,
        uid: str,
        title: str,
        desc: str,
        text: str,
        preview_url: str | None = None,
        thumb_url: str | None = None,
    ) -> InlineQueryResultArticle:
        lp = None
        if preview_url:
            lp = LinkPreviewOptions(
                url=preview_url,
                prefer_large_media=True,
                show_above_text=True,
                is_disabled=False,
            )
        kwargs = dict(
            id=uid,
            title=title,
            description=desc,
            input_message_content=InputTextMessageContent(
                message_text=text,
                parse_mode="HTML",
                link_preview_options=lp,
            ),
        )
        if thumb_url:
            kwargs["thumbnail_url"]    = thumb_url
            kwargs["thumbnail_width"]  = 100
            kwargs["thumbnail_height"] = 100
        return InlineQueryResultArticle(**kwargs)

    async def _answer(self, query: InlineQuery, results: list):
        try:
            await self.inline_bot.answer_inline_query(
                inline_query_id=query.id,
                results=results,
                cache_time=0,
                is_personal=True,
            )
        except Exception as e:
            logger.error(f"[NFTCheck] answer_inline_query failed: {e}")
