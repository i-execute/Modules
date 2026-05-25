__version__ = (2, 1, 0)
# meta developer: I_execute.t.me
# meta banner: https://raw.githubusercontent.com/i-execute/Modules/main/Storage/InlineDL/MetaBanner.jpeg

import re
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

    @loader.inline_handler(
        ru_doc="Скачать видео/фото из Instagram и TikTok",
        en_doc="Download video/photo from Instagram and TikTok",
    )
    async def dl_inline_handler(self, query: loader.InlineQuery):
        """Download video/photo from Instagram and TikTok"""
        text = query.query.strip()
        if text.lower().startswith("dl"):
            text = text[2:].strip()

        if not text:
            return await self._hint(query)

        platform, matched = detect_platform(text)
        if not platform:
            return await self._invalid(query)

        kk_url = make_kk_url(platform, matched)
        title = self.strings["ready_ig"] if platform == "instagram" else self.strings["ready_tt"]
        thumb = THUMB_IG if platform == "instagram" else THUMB_TT

        try:
            await query.answer(
                [
                    {
                        "type": "video",
                        "id": utils.hash_url(kk_url),
                        "video_url": kk_url,
                        "mime_type": "video/mp4",
                        "thumb_url": thumb,
                        "title": title,
                        "description": self.strings["ready_desc"],
                        "width": 1080,
                        "height": 1920,
                    }
                ],
                cache_time=0,
            )
        except Exception as e:
            await self._error(query, str(e))

    async def _hint(self, query):
        await query.answer(
            [
                {
                    "type": "article",
                    "id": "hint",
                    "title": self.strings["hint_title"],
                    "description": self.strings["hint_desc"],
                    "input_message_content": {
                        "message_text": self.strings["hint_msg"],
                        "parse_mode": "HTML",
                    },
                    "thumb_url": BANNER,
                }
            ],
            cache_time=0,
        )

    async def _invalid(self, query):
        await query.answer(
            [
                {
                    "type": "article",
                    "id": "invalid",
                    "title": self.strings["invalid_title"],
                    "description": self.strings["invalid_desc"],
                    "input_message_content": {
                        "message_text": self.strings["invalid_msg"],
                        "parse_mode": "HTML",
                    },
                    "thumb_url": BANNER,
                }
            ],
            cache_time=0,
        )

    async def _error(self, query, err):
        await query.answer(
            [
                {
                    "type": "article",
                    "id": "error",
                    "title": self.strings["err_title"],
                    "description": str(err)[:100],
                    "input_message_content": {
                        "message_text": f"<b>InlineDL:</b> {utils.escape_html(str(err))}",
                        "parse_mode": "HTML",
                    },
                    "thumb_url": BANNER,
                }
            ],
            cache_time=0,
        )