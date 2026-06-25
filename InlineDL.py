__version__ = (2, 4, 0)
# meta developer: I_execute.t.me
# meta banner: https://raw.githubusercontent.com/i-execute/Modules/main/Storage/InlineDL/MetaBannerNew.jpeg

import re
import time
import logging
import aiohttp

from telethon.tl.types import (
    InputBotInlineResult,
    InputBotInlineMessageMediaAuto,
    InputBotInlineMessageMediaWebPage,
    InputBotInlineMessageText,
    InputWebDocument,
    DocumentAttributeVideo,
)
from telethon.utils import html as tl_html

from .. import loader, utils

logger = logging.getLogger(__name__)

BANNER   = "https://raw.githubusercontent.com/i-execute/Modules/main/Storage/InlineDL/InlineQuery.png"
THUMB_IG = "https://raw.githubusercontent.com/i-execute/Modules/main/Storage/InlineDL/Instagram.png"
THUMB_TT = "https://raw.githubusercontent.com/i-execute/Modules/main/Storage/InlineDL/TikTok.png"
THUMB_PT = "https://raw.githubusercontent.com/i-execute/Modules/main/Storage/InlineDL/Pinterest.png"

INSTAGRAM_RE = re.compile(
    r"https?://(?:www\.)?instagram\.com/"
    r"(?:p|reel|reels|tv)/([A-Za-z0-9_-]+)"
)
INSTAGRAM_SHORT_RE = re.compile(
    r"https?://(?:www\.)?instagr\.am/"
    r"(?:p|reel|reels|tv)/([A-Za-z0-9_-]+)"
)
TIKTOK_FULL_RE = re.compile(
    r"https?://(?:www\.)?tiktok\.com/@[^/]+/video/(\d+)"
)
TIKTOK_SHORT_RE = re.compile(
    r"https?://(?:(?:vm|vt|www)\.)?tiktok\.com/(?:t/)?([A-Za-z0-9_-]+)"
)
PINTEREST_FULL_RE = re.compile(
    r"https?://(?:[\w-]+\.)?pinterest\.com/pin/[\w-]+"
)
PINTEREST_SHORT_RE = re.compile(
    r"https?://pin\.it/[A-Za-z0-9_-]+"
)

PINIMG_PRIORITY = [
    r"https://i\.pinimg\.com/1200x/[^\s\"'\\]+",
    r"https://i\.pinimg\.com/originals/[^\s\"'\\]+",
    r"https://i\.pinimg\.com/736x/[^\s\"'\\]+",
    r"https://i\.pinimg\.com/564x/[^\s\"'\\]+",
    r"https://i\.pinimg\.com/474x/[^\s\"'\\]+",
]


def detect_platform(text):
    if not text:
        return None, None
    text = text.strip()
    for pat in [PINTEREST_SHORT_RE, PINTEREST_FULL_RE]:
        m = pat.search(text)
        if m:
            return "pinterest", m.group(0)
    for pat in [INSTAGRAM_RE, INSTAGRAM_SHORT_RE]:
        m = pat.search(text)
        if m:
            return "instagram", m.group(0)
    for pat in [TIKTOK_FULL_RE, TIKTOK_SHORT_RE]:
        m = pat.search(text)
        if m:
            return "tiktok", m.group(0)
    return None, None


def make_kk_url(platform, url):
    if platform == "instagram":
        return re.sub(
            r"https?://(?:www\.)?instagram\.com",
            "https://kkclip.com",
            url,
        )
    if platform == "tiktok":
        return re.sub(
            r"https?://(?:(?:vm|vt|www)\.)?tiktok\.com",
            "https://kkclip.com",
            url,
        )
    return url


async def fetch_pinterest_image(url: str) -> str | None:
    try:
        async with aiohttp.ClientSession(
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/137.0 Safari/537.36"
                )
            }
        ) as s:
            async with s.get(url, allow_redirects=True) as r:
                html = await r.text()

        for pattern in PINIMG_PRIORITY:
            matches = re.findall(pattern, html)
            if matches:
                clean = sorted(set(m.replace("\\/", "/") for m in matches))
                return clean[0]

        all_imgs = re.findall(r"https://i\.pinimg\.com/[^\s\"'\\]+", html)
        all_imgs += [
            m.replace("\\/", "/")
            for m in re.findall(r"https:\\/\\/i\.pinimg\.com\\/[^\"]+", html)
        ]
        if all_imgs:
            return sorted(set(all_imgs))[-1]

    except Exception as e:
        logger.error(f"[InlineDL] Pinterest fetch error: {e}", exc_info=True)
    return None


async def download_image(url: str) -> bytes | None:
    try:
        async with aiohttp.ClientSession(
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/137.0 Safari/537.36"
                )
            }
        ) as s:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=30)) as r:
                if r.status != 200:
                    return None
                data = await r.read()
                return data if len(data) > 500 else None
    except Exception as e:
        logger.error(f"[InlineDL] Image download error: {e}", exc_info=True)
    return None


async def upload_to_x0(data: bytes, filename: str = "image.jpg") -> str:
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
        logger.error(f"[InlineDL] x0.at upload error: {e}", exc_info=True)
    return ""


async def get_pinterest_preview(url: str) -> str:
    img_url = await fetch_pinterest_image(url)
    if not img_url:
        return ""
    data = await download_image(img_url)
    if not data:
        return ""
    filename = img_url.split("/")[-1].split("?")[0] or "image.jpg"
    if not filename.endswith((".jpg", ".jpeg", ".png", ".webp")):
        filename = "image.jpg"
    return await upload_to_x0(data, filename)


@loader.tds
class InlineDL(loader.Module):
    """Instagram & TikTok & Pinterest video/photo downloader via inline query"""

    strings = {
        "name": "InlineDL",

        "hint_title": "InlineDL",
        "hint_desc": "Paste Instagram, TikTok or Pinterest link",
        "hint_msg": (
            "<blockquote><b>InlineDL</b></blockquote>\n"
            "<blockquote>Paste an Instagram, TikTok or Pinterest link to download media.</blockquote>\n"
            "<blockquote>Supported:\n"
            "<code>instagram.com/reel/...</code>\n"
            "<code>tiktok.com/@.../video/...</code>\n"
            "<code>pinterest.com/pin/...</code>\n"
            "<code>pin.it/...</code></blockquote>"
        ),

        "invalid_title": "Invalid link",
        "invalid_desc": "Could not detect Instagram, TikTok or Pinterest link",
        "invalid_msg": (
            "<blockquote><b>InlineDL</b></blockquote>\n"
            "<blockquote>Could not detect a valid link.</blockquote>\n"
            "<blockquote>Supported: Instagram, TikTok, Pinterest</blockquote>"
        ),

        "ready_ig": "Instagram",
        "ready_tt": "TikTok",
        "ready_pt": "Pinterest",
        "ready_desc": "Tap to download",

        "err_title": "Error",
        "pt_not_found": "Could not find image on Pinterest",
        "pt_not_found_msg": (
            "<blockquote><b>InlineDL</b></blockquote>\n"
            "<blockquote>Could not find image on Pinterest</blockquote>"
        ),
    }

    strings_ru = {
        "hint_title": "InlineDL",
        "hint_desc": "Вставьте ссылку Instagram, TikTok или Pinterest",
        "hint_msg": (
            "<blockquote><b>InlineDL</b></blockquote>\n"
            "<blockquote>Вставьте ссылку Instagram, TikTok или Pinterest для скачивания медиа.</blockquote>\n"
            "<blockquote>Поддерживаются:\n"
            "<code>instagram.com/reel/...</code>\n"
            "<code>tiktok.com/@.../video/...</code>\n"
            "<code>pinterest.com/pin/...</code>\n"
            "<code>pin.it/...</code></blockquote>"
        ),

        "invalid_title": "Неверная ссылка",
        "invalid_desc": "Не удалось распознать ссылку Instagram, TikTok или Pinterest",
        "invalid_msg": (
            "<blockquote><b>InlineDL</b></blockquote>\n"
            "<blockquote>Не удалось распознать ссылку.</blockquote>\n"
            "<blockquote>Поддерживаются: Instagram, TikTok, Pinterest</blockquote>"
        ),

        "ready_ig": "Instagram",
        "ready_tt": "TikTok",
        "ready_pt": "Pinterest",
        "ready_desc": "Нажмите чтобы скачать",

        "err_title": "Ошибка",
        "pt_not_found": "Не удалось найти изображение на Pinterest",
        "pt_not_found_msg": (
            "<blockquote><b>InlineDL</b></blockquote>\n"
            "<blockquote>Не удалось найти изображение на Pinterest</blockquote>"
        ),
    }

    def __init__(self):
        self._preview_cache: dict[str, str] = {}

    async def client_ready(self, client, db):
        self._client = client
        self._db = db

    def _make_web_document(self, url, mime_type="image/png"):
        return InputWebDocument(
            url=url,
            size=0,
            mime_type=mime_type,
            attributes=[],
        )

    def _make_article(
        self,
        uid: str,
        title: str,
        desc: str,
        text: str,
        preview_url: str | None = None,
        thumb_url: str | None = None,
    ) -> InputBotInlineResult:
        plain, entities = tl_html.parse(text)
        if preview_url:
            send_message = InputBotInlineMessageMediaWebPage(
                message=plain,
                url=preview_url,
                force_large_media=True,
                invert_media=True,
                entities=entities or None,
            )
        else:
            send_message = InputBotInlineMessageText(
                message=plain,
                no_webpage=True,
                entities=entities or None,
            )
        return InputBotInlineResult(
            id=uid,
            type="article",
            title=title,
            description=desc,
            thumb=self._make_web_document(thumb_url or BANNER),
            send_message=send_message,
        )

    @loader.inline_handler(
        ru_doc="Скачать видео/фото из Instagram, TikTok и Pinterest",
        en_doc="Download video/photo from Instagram, TikTok and Pinterest",
    )
    async def dl_inline_handler(self, query):
        """Download video/photo from Instagram, TikTok and Pinterest"""
        text = query.query.strip()
        if text.lower().startswith("dl"):
            text = text[2:].strip()

        if not text:
            await query.answer(
                results=[self._make_article(
                    uid=f"hint_{int(time.time())}",
                    title=self.strings["hint_title"],
                    desc=self.strings["hint_desc"],
                    text=self.strings["hint_msg"],
                    thumb_url=BANNER,
                )],
                cache_time=0,
                private=True,
            )
            return

        platform, matched = detect_platform(text)
        if not platform or not matched:
            await query.answer(
                results=[self._make_article(
                    uid=f"inv_{int(time.time())}",
                    title=self.strings["invalid_title"],
                    desc=self.strings["invalid_desc"],
                    text=self.strings["invalid_msg"],
                    thumb_url=BANNER,
                )],
                cache_time=0,
                private=True,
            )
            return

        if platform == "pinterest":
            cache_key = matched
            if cache_key in self._preview_cache:
                preview_url = self._preview_cache[cache_key]
            else:
                preview_url = await get_pinterest_preview(matched)
                if preview_url:
                    self._preview_cache[cache_key] = preview_url

            if not preview_url:
                await query.answer(
                    results=[self._make_article(
                        uid=f"pt_err_{int(time.time())}",
                        title=self.strings["err_title"],
                        desc=self.strings["pt_not_found"],
                        text=self.strings["pt_not_found_msg"],
                        thumb_url=THUMB_PT,
                    )],
                    cache_time=0,
                    private=True,
                )
                return

            await query.answer(
                results=[self._make_article(
                    uid=f"pt_{cache_key}_{int(time.time())}",
                    title=self.strings["ready_pt"],
                    desc=self.strings["ready_desc"],
                    text=f'<blockquote><a href="{preview_url}">Download image</a></blockquote>',
                    preview_url=preview_url,
                    thumb_url=preview_url,
                )],
                cache_time=0,
                private=True,
            )
            return

        kk_url = make_kk_url(platform, matched)
        title  = self.strings["ready_ig"] if platform == "instagram" else self.strings["ready_tt"]
        thumb  = THUMB_IG if platform == "instagram" else THUMB_TT

        try:
            result = InputBotInlineResult(
                id=f"v_{int(time.time())}",
                type="video",
                title=title,
                description=self.strings["ready_desc"],
                url=kk_url,
                thumb=self._make_web_document(thumb, mime_type="image/png"),
                content=self._make_web_document(
                    kk_url,
                    mime_type="video/mp4",
                    attributes=[
                        DocumentAttributeVideo(duration=0, w=1080, h=1920)
                    ],
                ),
                send_message=InputBotInlineMessageMediaAuto(message=""),
            )
            await query.answer(results=[result], cache_time=0, private=True)
        except Exception as e:
            logger.exception("[InlineDL] Failed to answer inline query")
            await query.answer(
                results=[self._make_article(
                    uid=f"e_{int(time.time())}",
                    title=self.strings["err_title"],
                    desc=str(e)[:100],
                    text=f"<blockquote><b>InlineDL:</b> {utils.escape_html(str(e))}</blockquote>",
                    thumb_url=thumb,
                )],
                cache_time=0,
                private=True,
            )

    async def on_unload(self):
        pass