__version__ = (2, 0, 2)
# meta developer: I_execute.t.me
# meta banner: https://raw.githubusercontent.com/i-execute/Modules/main/Storage/InlineDL/MetaBanner.jpeg

import re
import time
import logging

from telethon.tl.types import (
    InputBotInlineResult,
    InputBotInlineMessageMediaAuto,
    InputWebDocument,
)

from .. import loader

logger = logging.getLogger(__name__)

BANNER = "https://raw.githubusercontent.com/i-execute/Modules/main/Storage/InlineDL/Inline_query.png"
THUMB_IG = "https://raw.githubusercontent.com/i-execute/Modules/main/Storage/InlineDL/Instagram.png"
THUMB_TT = "https://raw.githubusercontent.com/i-execute/Modules/main/Storage/InlineDL/TikTok.png"

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

    async def client_ready(self, client, db):
        self._client = client
        self._db = db

    def _make_web_document(self, url, width=640, height=640):
        return InputWebDocument(
            url=url,
            size=0,
            mime_type="image/png",
            attributes=[],
        )

    @loader.inline_handler(
        ru_doc="Скачать видео/фото из Instagram и TikTok",
        en_doc="Download video/photo from Instagram and TikTok",
    )
    async def dl_inline_handler(self, query):
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
            result = InputBotInlineResult(
                id=f"v_{int(time.time())}",
                type="video",
                title=title,
                description=self.strings["ready_desc"],
                thumb=self._make_web_document(thumb, 1080, 1920),
                content=self._make_web_document(kk_url, 1080, 1920),
                send_message=InputBotInlineMessageMediaAuto(
                    message="",
                ),
            )
            
            await query.answer(
                results=[result],
                cache_time=0,
                private=True,
            )
        except Exception as e:
            logger.error(f"[InlineDL] Error: {e}", exc_info=True)
            await self._error(query, str(e))

    async def _hint(self, query):
        try:
            result = InputBotInlineResult(
                id=f"h_{int(time.time())}",
                type="article",
                title=self.strings["hint_title"],
                description=self.strings["hint_desc"],
                thumb=self._make_web_document(BANNER),
                send_message=InputBotInlineMessageMediaAuto(
                    message=self.strings["hint_msg"],
                ),
            )
            
            await query.answer(
                results=[result],
                cache_time=0,
                private=True,
            )
        except Exception as e:
            logger.error(f"[InlineDL] Hint error: {e}", exc_info=True)

    async def _invalid(self, query):
        try:
            result = InputBotInlineResult(
                id=f"inv_{int(time.time())}",
                type="article",
                title=self.strings["invalid_title"],
                description=self.strings["invalid_desc"],
                thumb=self._make_web_document(BANNER),
                send_message=InputBotInlineMessageMediaAuto(
                    message=self.strings["invalid_msg"],
                ),
            )
            
            await query.answer(
                results=[result],
                cache_time=0,
                private=True,
            )
        except Exception as e:
            logger.error(f"[InlineDL] Invalid error: {e}", exc_info=True)

    async def _error(self, query, err):
        try:
            result = InputBotInlineResult(
                id=f"e_{int(time.time())}",
                type="article",
                title=self.strings["err_title"],
                description=str(err)[:100],
                thumb=self._make_web_document(BANNER),
                send_message=InputBotInlineMessageMediaAuto(
                    message=f"<b>InlineDL:</b> {escape_html(str(err))}",
                ),
            )
            
            await query.answer(
                results=[result],
                cache_time=0,
                private=True,
            )
        except Exception as e:
            logger.error(f"[InlineDL] Error display error: {e}", exc_info=True)

    async def on_unload(self):
        pass