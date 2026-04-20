__version__ = (1, 6, 0)
# meta developer: FireJester.t.me

import re
import time
import logging

from aiogram.types import (
    InlineQuery,
    InlineQueryResultVideo,
    InlineQueryResultArticle,
    InputTextMessageContent,
)

from .. import loader

logger = logging.getLogger(__name__)

BANNER = "https://github.com/FireJester/Modules/raw/main/Assets/InlineDL/Inline_query.png"
THUMB_IG = "https://github.com/FireJester/Modules/raw/main/Assets/InlineDL/Instagram.png"
THUMB_TT = "https://github.com/FireJester/Modules/raw/main/Assets/InlineDL/TikTok.png"

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


def escape_html(t):
    return (t or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def detect_platform(text):
    if not text:
        return None, None
    text = text.strip()
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
            r"(https?://(?:www\.)?)instagram\.com",
            r"\1kksave.com",
            url,
        )
    if platform == "tiktok":
        return re.sub(
            r"(https?://(?:(?:vm|vt|www)\.)?)tiktok\.com",
            r"\1kksave.com",
            url,
        )
    return url


@loader.tds
class InlineDL(loader.Module):
    """Instagram & TikTok video/photo downloader via inline query"""

    strings = {
        "name": "InlineDL",
        "hint_title": "InlineDL",
        "hint_desc": "Paste Instagram or TikTok link",
        "hint_msg": "<b>InlineDL:</b> Paste an Instagram or TikTok link",
        "invalid_title": "Invalid link",
        "invalid_desc": "Could not detect Instagram or TikTok link",
        "invalid_msg": "<b>InlineDL:</b> Invalid link. Supported: Instagram, TikTok",
        "ready_ig": "Instagram",
        "ready_tt": "TikTok",
        "ready_desc": "Tap to download",
        "err_title": "Error",
    }

    strings_ru = {
        "hint_title": "InlineDL",
        "hint_desc": "Вставьте ссылку Instagram или TikTok",
        "hint_msg": "<b>InlineDL:</b> Вставьте ссылку Instagram или TikTok",
        "invalid_title": "Неверная ссылка",
        "invalid_desc": "Не удалось распознать ссылку Instagram или TikTok",
        "invalid_msg": "<b>InlineDL:</b> Неверная ссылка. Поддерживаются: Instagram, TikTok",
        "ready_ig": "Instagram",
        "ready_tt": "TikTok",
        "ready_desc": "Нажмите чтобы скачать",
        "err_title": "Ошибка",
    }

    def __init__(self):
        self.inline_bot = None

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        if hasattr(self, "inline") and hasattr(self.inline, "bot"):
            self.inline_bot = self.inline.bot

    @loader.inline_handler(
        ru_doc="Скачать видео/фото из Instagram и TikTok",
        en_doc="Download video/photo from Instagram and TikTok",
    )
    async def dl_inline_handler(self, query: InlineQuery):
        """Download video/photo from Instagram and TikTok"""
        text = query.query.strip()
        if text.lower().startswith("dl"):
            text = text[2:].strip()

        if not text:
            await self._hint(query)
            return

        platform, matched = detect_platform(text)
        if not platform:
            await self._invalid(query)
            return

        kk_url = make_kk_url(platform, matched)
        title = self.strings["ready_ig"] if platform == "instagram" else self.strings["ready_tt"]
        thumb = THUMB_IG if platform == "instagram" else THUMB_TT

        try:
            await self.inline_bot.answer_inline_query(
                inline_query_id=query.id,
                results=[
                    InlineQueryResultVideo(
                        id=f"v_{int(time.time())}",
                        video_url=kk_url,
                        mime_type="video/mp4",
                        thumbnail_url=thumb,
                        title=title,
                        description=self.strings["ready_desc"],
                    )
                ],
                cache_time=0,
                is_personal=True,
            )
        except Exception as e:
            await self._error(query, str(e))

    async def _hint(self, query):
        try:
            await self.inline_bot.answer_inline_query(
                inline_query_id=query.id,
                results=[
                    InlineQueryResultArticle(
                        id=f"h_{int(time.time())}",
                        title=self.strings["hint_title"],
                        description=self.strings["hint_desc"],
                        input_message_content=InputTextMessageContent(
                            message_text=self.strings["hint_msg"],
                            parse_mode="HTML",
                        ),
                        thumbnail_url=BANNER,
                        thumbnail_width=640,
                        thumbnail_height=360,
                    )
                ],
                cache_time=0,
                is_personal=True,
            )
        except Exception:
            pass

    async def _invalid(self, query):
        try:
            await self.inline_bot.answer_inline_query(
                inline_query_id=query.id,
                results=[
                    InlineQueryResultArticle(
                        id=f"inv_{int(time.time())}",
                        title=self.strings["invalid_title"],
                        description=self.strings["invalid_desc"],
                        input_message_content=InputTextMessageContent(
                            message_text=self.strings["invalid_msg"],
                            parse_mode="HTML",
                        ),
                        thumbnail_url=BANNER,
                        thumbnail_width=640,
                        thumbnail_height=360,
                    )
                ],
                cache_time=0,
                is_personal=True,
            )
        except Exception:
            pass

    async def _error(self, query, err):
        try:
            await self.inline_bot.answer_inline_query(
                inline_query_id=query.id,
                results=[
                    InlineQueryResultArticle(
                        id=f"e_{int(time.time())}",
                        title=self.strings["err_title"],
                        description=str(err)[:100],
                        input_message_content=InputTextMessageContent(
                            message_text=f"<b>InlineDL:</b> {escape_html(str(err))}",
                            parse_mode="HTML",
                        ),
                        thumbnail_url=BANNER,
                        thumbnail_width=640,
                        thumbnail_height=640,
                    )
                ],
                cache_time=0,
                is_personal=True,
            )
        except Exception:
            pass

    async def on_unload(self):
        pass
