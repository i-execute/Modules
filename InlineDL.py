__version__ = (2, 0, 2)
# meta developer: I_execute.t.me
# meta banner: https://raw.githubusercontent.com/i-execute/Modules/main/Storage/InlineDL/MetaBanner.jpeg

import re
import time
import logging

from .. import loader, utils

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
    return utils.escape_html(t or "")

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
            await self._answer_article(
                query,
                title=self.strings["hint_title"],
                description=self.strings["hint_desc"],
                message=self.strings["hint_msg"],
                thumb=BANNER,
                result_id=f"h_{int(time.time())}",
            )
            return

        platform, matched = detect_platform(text)
        if not platform or not matched:
            await self._answer_article(
                query,
                title=self.strings["invalid_title"],
                description=self.strings["invalid_desc"],
                message=self.strings["invalid_msg"],
                thumb=BANNER,
                result_id=f"inv_{int(time.time())}",
            )
            return

        kk_url = make_kk_url(platform, matched)
        title = self.strings["ready_ig"] if platform == "instagram" else self.strings["ready_tt"]
        thumb = THUMB_IG if platform == "instagram" else THUMB_TT

        try:
            await query.answer(
                [
                    await query.builder.document(
                        kk_url,
                        title=title,
                        description=self.strings["ready_desc"],
                        type="video",
                        mime_type="video/mp4",
                        text=f'<a href="{escape_html(kk_url)}">{title}</a>',
                        parse_mode="HTML",
                        link_preview=False,
                        id=f"v_{int(time.time())}",
                    )
                ],
                cache_time=0,
                private=True,
            )
        except Exception as e:
            logger.exception("[InlineDL] Failed to answer inline query")
            await self._answer_article(
                query,
                title=self.strings["err_title"],
                description=str(e)[:100],
                message=f"<b>InlineDL:</b> {escape_html(str(e))}",
                thumb=thumb,
                result_id=f"e_{int(time.time())}",
            )

    async def _answer_article(
        self,
        query,
        title,
        description,
        message,
        thumb,
        result_id,
    ):
        try:
            await query.answer(
                [
                    await query.builder.article(
                        title=title,
                        description=description,
                        text=message,
                        parse_mode="HTML",
                        link_preview=False,
                        thumb=self.inline._web_document(thumb, width=640, height=640),
                        id=result_id,
                    )
                ],
                cache_time=0,
                private=True,
            )
        except Exception as e:
            logger.error(f"[InlineDL] Article answer error: {e}", exc_info=True)

    async def on_unload(self):
        pass