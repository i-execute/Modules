__version__ = (1, 2, 0)
# meta developer: FireJester.t.me

import re
import io
import time
import logging
import asyncio

from aiogram.types import (
    InlineQuery,
    InlineQueryResultCachedVideo,
    InlineQueryResultCachedPhoto,
    InlineQueryResultArticle,
    InputTextMessageContent,
    BufferedInputFile,
)

from telethon.tl.types import (
    MessageMediaWebPage,
    WebPage,
    WebPagePending,
    DocumentAttributeVideo,
)
from telethon import functions

from .. import loader, utils

logger = logging.getLogger(__name__)

BANNER = "https://github.com/FireJester/Media/raw/main/Banner_for_inline_query_in_InlineDL.jpeg"

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

MAX_FILE_SIZE = 50 * 1024 * 1024
CACHE_TTL = 600
WP_POLL_INTERVAL = 0.8
WP_MAX_ATTEMPTS = 15


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
            r"\1kktiktok.com",
            url,
        )
    return url


def make_cache_key(platform, url):
    for pat in [INSTAGRAM_RE, INSTAGRAM_SHORT_RE, TIKTOK_FULL_RE, TIKTOK_SHORT_RE]:
        m = pat.search(url)
        if m:
            pfx = "ig" if platform == "instagram" else "tt"
            return f"{pfx}_{m.group(1)}"
    return None


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
        "dl_title": "Downloading...",
        "dl_desc_ig": "Instagram - wait a few seconds and repeat query",
        "dl_desc_tt": "TikTok - wait a few seconds and repeat query",
        "dl_msg": "<b>InlineDL:</b> Downloading… Try again in a few seconds.",
        "ready_ig": "Instagram",
        "ready_tt": "TikTok",
        "ready_desc": "Tap on inline query to send",
        "err_title": "Error",
    }

    strings_ru = {
        "hint_title": "InlineDL",
        "hint_desc": "Вставьте ссылку Instagram или TikTok",
        "hint_msg": "<b>InlineDL:</b> Вставьте ссылку Instagram или TikTok",
        "invalid_title": "Неверная ссылка",
        "invalid_desc": "Не удалось распознать ссылку Instagram или TikTok",
        "invalid_msg": "<b>InlineDL:</b> Неверная ссылка. Поддерживаются: Instagram, TikTok",
        "dl_title": "Загрузка...",
        "dl_desc_ig": "Instagram - подождите несколько секунд и повторите запрос",
        "dl_desc_tt": "TikTok - подождите несколько секунд и повторите запрос",
        "dl_msg": "<b>InlineDL:</b> Загружаю… Повторите запрос через несколько секунд.",
        "ready_ig": "Instagram",
        "ready_tt": "TikTok",
        "ready_desc": "Нажмите на инлайн запрос чтобы отправить",
        "err_title": "Ошибка",
    }

    def __init__(self):
        self.inline_bot = None
        self._pending = {}
        self._upload_lock = None

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        self._upload_lock = asyncio.Lock()
        if hasattr(self, "inline") and hasattr(self.inline, "bot"):
            self.inline_bot = self.inline.bot
        self._cleanup_cache()

    def _get_cache(self):
        return self._db.get("InlineDL", "cache", {})

    def _save_cache(self, cache):
        self._db.set("InlineDL", "cache", cache)

    def _cache_get(self, key):
        cache = self._get_cache()
        entry = cache.get(key)
        if not entry:
            return None
        if time.time() - entry.get("ts", 0) > CACHE_TTL:
            cache.pop(key, None)
            self._save_cache(cache)
            return None
        entry["ts"] = time.time()
        self._save_cache(cache)
        return entry.get("data")

    def _cache_set(self, key, data):
        cache = self._get_cache()
        cache[key] = {"data": data, "ts": time.time()}
        self._save_cache(cache)

    def _cleanup_cache(self):
        cache = self._get_cache()
        now = time.time()
        dead = [k for k, v in cache.items() if now - v.get("ts", 0) > CACHE_TTL]
        if dead:
            for k in dead:
                cache.pop(k, None)
            self._save_cache(cache)

    async def _prefetch_webpage(self, kk_url: str):
        try:
            await self._client(
                functions.messages.GetWebPageRequest(url=kk_url, hash=0)
            )
        except Exception:
            pass

        await asyncio.sleep(0.5)

    async def _fetch_wp(self, kk_url):
        await self._prefetch_webpage(kk_url)
        sent = await self._client.send_message("me", kk_url)
        msg = None
        wp = None

        for _ in range(WP_MAX_ATTEMPTS):
            await asyncio.sleep(WP_POLL_INTERVAL)
            try:
                msg = await self._client.get_messages("me", ids=sent.id)
            except Exception:
                continue

            if not msg or not msg.media:
                continue
            if not isinstance(msg.media, MessageMediaWebPage):
                continue

            w = msg.media.webpage
            if isinstance(w, WebPagePending):
                continue
            if isinstance(w, WebPage):
                wp = w
                break

        return wp, msg, sent.id

    async def _dl_and_upload(self, kk_url, platform, cache_key, user_id):
        sent_id = None
        try:
            wp, msg, sent_id = await self._fetch_wp(kk_url)

            if not wp:
                self._cache_set(cache_key, {"error": "Telegram could not parse the link"})
                return {"error": "Telegram could not parse the link"}

            has_video = wp.document is not None
            has_photo = wp.photo is not None and not has_video

            if not has_video and not has_photo:
                self._cache_set(cache_key, {"error": "No media in this link"})
                return {"error": "No media in this link"}

            if has_video:
                result = await self._handle_video(wp, msg, platform, cache_key, user_id)
            else:
                result = await self._handle_photo(wp, msg, platform, cache_key, user_id)

            return result

        except Exception as e:
            err = {"error": str(e)[:80]}
            self._cache_set(cache_key, err)
            return err
        finally:
            if sent_id:
                try:
                    await self._client.delete_messages("me", [sent_id])
                except Exception:
                    pass

    async def _handle_video(self, wp, msg, platform, cache_key, user_id):
        doc = wp.document
        if doc.size and doc.size > MAX_FILE_SIZE:
            self._cache_set(cache_key, {"error": "Video > 50 MB"})
            return {"error": "Video > 50 MB"}

        w, h, dur = 0, 0, 0
        for attr in (doc.attributes or []):
            if isinstance(attr, DocumentAttributeVideo):
                w = attr.w or 0
                h = attr.h or 0
                dur = int(attr.duration or 0)

        buf = io.BytesIO()
        await self._client.download_media(msg, buf)
        buf.seek(0)
        video_bytes = buf.read()

        if not video_bytes or len(video_bytes) < 1000:
            self._cache_set(cache_key, {"error": "Downloaded file is empty"})
            return {"error": "Downloaded file is empty"}

        thumb_bytes = await self._download_photo(wp.photo)

        file_id = await self._upload_video(video_bytes, thumb_bytes, w, h, dur, user_id)
        if not file_id:
            self._cache_set(cache_key, {"error": "Telegram upload failed"})
            return {"error": "Telegram upload failed"}

        result = {"file_id": file_id, "platform": platform, "media_type": "video"}
        self._cache_set(cache_key, result)
        return result

    async def _handle_photo(self, wp, msg, platform, cache_key, user_id):
        photo_bytes = await self._download_photo_full(msg)

        if not photo_bytes or len(photo_bytes) < 1000:
            self._cache_set(cache_key, {"error": "Downloaded photo is empty"})
            return {"error": "Downloaded photo is empty"}

        if len(photo_bytes) > MAX_FILE_SIZE:
            self._cache_set(cache_key, {"error": "Photo > 50 MB"})
            return {"error": "Photo > 50 MB"}

        file_id = await self._upload_photo(photo_bytes, user_id)
        if not file_id:
            self._cache_set(cache_key, {"error": "Telegram upload failed"})
            return {"error": "Telegram upload failed"}

        result = {"file_id": file_id, "platform": platform, "media_type": "photo"}
        self._cache_set(cache_key, result)
        return result

    async def _download_photo(self, photo_obj):
        if not photo_obj:
            return None
        try:
            buf = io.BytesIO()
            await self._client.download_media(photo_obj, buf)
            buf.seek(0)
            raw = buf.read()
            return raw if raw and len(raw) > 500 else None
        except Exception:
            return None

    async def _download_photo_full(self, msg):
        if not msg:
            return None
        try:
            buf = io.BytesIO()
            await self._client.download_media(msg, buf)
            buf.seek(0)
            raw = buf.read()
            return raw if raw and len(raw) > 500 else None
        except Exception:
            return None

    async def _upload_video(self, video_bytes, thumb_bytes, w, h, dur, user_id):
        async with self._upload_lock:
            vid = BufferedInputFile(video_bytes, filename="video.mp4")
            thb = (
                BufferedInputFile(thumb_bytes, filename="thumb.jpg")
                if thumb_bytes
                else None
            )
            try:
                sent = await self.inline_bot.send_video(
                    chat_id=user_id,
                    video=vid,
                    width=w if w > 0 else None,
                    height=h if h > 0 else None,
                    duration=dur if dur > 0 else None,
                    thumbnail=thb,
                    supports_streaming=True,
                )
            except Exception:
                return None

            if not sent or not sent.video:
                return None

            file_id = sent.video.file_id
            mid = sent.message_id
            await asyncio.sleep(0.5)
            for att in range(5):
                try:
                    await self.inline_bot.delete_message(chat_id=user_id, message_id=mid)
                    break
                except Exception:
                    await asyncio.sleep(1.0 * (att + 1))
            return file_id

    async def _upload_photo(self, photo_bytes, user_id):
        async with self._upload_lock:
            ph = BufferedInputFile(photo_bytes, filename="photo.jpg")
            try:
                sent = await self.inline_bot.send_photo(
                    chat_id=user_id,
                    photo=ph,
                )
            except Exception:
                return None

            if not sent or not sent.photo:
                return None

            file_id = sent.photo[-1].file_id
            mid = sent.message_id
            await asyncio.sleep(0.5)
            for att in range(5):
                try:
                    await self.inline_bot.delete_message(chat_id=user_id, message_id=mid)
                    break
                except Exception:
                    await asyncio.sleep(1.0 * (att + 1))
            return file_id

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

        cache_key = make_cache_key(platform, matched)
        if not cache_key:
            await self._invalid(query)
            return

        self._cleanup_cache()

        cached = self._cache_get(cache_key)
        if cached:
            if "error" in cached:
                await self._error(query, cached["error"])
                return
            if "file_id" in cached:
                await self._result(query, cached)
                return

        if cache_key in self._pending:
            fut = self._pending[cache_key]
            if fut.done():
                self._pending.pop(cache_key, None)
                try:
                    res = fut.result()
                except Exception:
                    res = {"error": "Internal error"}
                if "error" in res:
                    await self._error(query, res["error"])
                elif "file_id" in res:
                    await self._result(query, res)
                else:
                    await self._error(query, "Unknown error")
                return
            await self._downloading(query, platform)
            return

        kk_url = make_kk_url(platform, matched)
        self._pending[cache_key] = asyncio.ensure_future(
            self._dl_and_upload(kk_url, platform, cache_key, query.from_user.id)
        )
        await self._downloading(query, platform)

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

    async def _downloading(self, query, platform):
        desc_key = "dl_desc_ig" if platform == "instagram" else "dl_desc_tt"
        try:
            await self.inline_bot.answer_inline_query(
                inline_query_id=query.id,
                results=[
                    InlineQueryResultArticle(
                        id=f"dl_{int(time.time())}",
                        title=self.strings["dl_title"],
                        description=self.strings[desc_key],
                        input_message_content=InputTextMessageContent(
                            message_text=self.strings["dl_msg"],
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

    async def _result(self, query, data):
        plat = data.get("platform", "")
        media_type = data.get("media_type", "video")
        title = (
            self.strings["ready_ig"]
            if plat == "instagram"
            else self.strings["ready_tt"]
        )
        try:
            if media_type == "photo":
                await self.inline_bot.answer_inline_query(
                    inline_query_id=query.id,
                    results=[
                        InlineQueryResultCachedPhoto(
                            id=f"p_{int(time.time())}",
                            photo_file_id=data["file_id"],
                            title=title,
                            description=self.strings["ready_desc"],
                        )
                    ],
                    cache_time=0,
                    is_personal=True,
                )
            else:
                await self.inline_bot.answer_inline_query(
                    inline_query_id=query.id,
                    results=[
                        InlineQueryResultCachedVideo(
                            id=f"v_{int(time.time())}",
                            video_file_id=data["file_id"],
                            title=title,
                            description=self.strings["ready_desc"],
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
                        thumbnail_height=360,
                    )
                ],
                cache_time=0,
                is_personal=True,
            )
        except Exception:
            pass

    async def on_unload(self):
        for fut in self._pending.values():
            fut.cancel()
        self._pending.clear()