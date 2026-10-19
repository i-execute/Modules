# CopyLeft 2026 github.com/i-execute
# Author: I_execute.t.me
# Licensed under AGPLv3.

__version__ = (3, 3, 0)
# meta developer: Execute_forge.t.me
# meta banner: https://raw.githubusercontent.com/i-execute/Modules/main/Storage/InlineDL/MetaBannerNew.jpeg

import asyncio
import copy
import logging
import mimetypes
import os
import re
import secrets
import shutil
import subprocess
import sys
import time
from collections import OrderedDict


def _ensure_deps():
    for mod, pkg in (("yt_dlp", "yt-dlp"), ("imageio_ffmpeg", "imageio-ffmpeg")):
        try:
            __import__(mod)
        except ImportError:
            subprocess.check_call(
                [sys.executable, "-m", "pip", "install", pkg, "-q"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )


_ensure_deps()

import aiohttp
import yt_dlp


def _patch_instagram():
    try:
        from yt_dlp.extractor.instagram import InstagramIE

        if getattr(InstagramIE, "_patched_no_formats", False):
            return
        orig_extract = InstagramIE._real_extract

        def patched_extract(self, url):
            orig_raise = self.raise_no_formats

            def noop_raise(*args, **kwargs):
                pass

            self.raise_no_formats = noop_raise
            try:
                return orig_extract(self, url)
            finally:
                self.raise_no_formats = orig_raise

        InstagramIE._real_extract = patched_extract
        InstagramIE._patched_no_formats = True
    except Exception:
        pass


_patch_instagram()
from telethon import utils as tl_utils
from telethon.errors import FloodWaitError, MessageNotModifiedError
from telethon.tl import functions
from telethon.tl.types import (
    DocumentAttributeAudio,
    DocumentAttributeFilename,
    DocumentAttributeVideo,
    InputBotInlineMessageMediaWebPage,
    InputBotInlineMessageText,
    InputBotInlineResult,
    InputMediaUploadedDocument,
    InputMediaWebPage,
    InputPeerSelf,
    InputWebDocument,
)
from telethon.utils import html as tl_html

from .. import loader, utils

logger = logging.getLogger(__name__)

BANNER = "https://raw.githubusercontent.com/i-execute/Modules/main/Storage/InlineDL/InlineQuery.png"
THUMB_IG = "https://raw.githubusercontent.com/i-execute/Modules/main/Storage/InlineDL/Instagram.png"
THUMB_TT = "https://raw.githubusercontent.com/i-execute/Modules/main/Storage/InlineDL/TikTok.png"
THUMB_PT = "https://raw.githubusercontent.com/i-execute/Modules/main/Storage/InlineDL/Pinterest.png"
RELOAD_BANNER = "https://raw.githubusercontent.com/i-execute/Modules/main/Storage/InlineDL/Reloading.jpeg"
TOPIC_TITLE = "InlineDL"
TOPIC_ICON = 5379758279851289726


MB = 1024 * 1024
BOT_LIMIT = 2000 * MB
USER_LIMIT = 4096 * MB
CACHE_TTL = 1800
CACHE_MAX = 30
MAX_HEIGHTS = 6
MAX_ENTRIES = 10
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/137.0 Safari/537.36"
)

URL_RE = re.compile(r"https?://[^\s<>\"']+")
PINTEREST_RE = re.compile(r"https?://(?:[\w-]+\.)?(?:pinterest\.[a-z.]+/pin/|pin\.it/)", re.I)
PINTEREST_SHORT_RE = re.compile(r"https?://pin\.it/[A-Za-z0-9_-]+", re.I)
PINTEREST_FULL_RE = re.compile(r"https?://(?:[\w-]+\.)?pinterest\.[a-z.]+/pin/[\w-]+", re.I)
PIN_BUDGET = 8
PIN_GRACE = 2
JPG_RE = re.compile(r"\.jpe?g(?:\?|$)", re.I)

PINIMG_PRIORITY = [
    r"https://i\.pinimg\.com/1200x/[^\s\"'\\]+",
    r"https://i\.pinimg\.com/originals/[^\s\"'\\]+",
    r"https://i\.pinimg\.com/736x/[^\s\"'\\]+",
    r"https://i\.pinimg\.com/564x/[^\s\"'\\]+",
    r"https://i\.pinimg\.com/474x/[^\s\"'\\]+",
]

CODECS = (
    ("avc", "H.264"),
    ("h264", "H.264"),
    ("hev", "H.265"),
    ("hvc", "H.265"),
    ("h265", "H.265"),
    ("vp09", "VP9"),
    ("vp9", "VP9"),
    ("vp8", "VP8"),
    ("av01", "AV1"),
    ("mp4a", "AAC"),
    ("aac", "AAC"),
    ("opus", "Opus"),
    ("mp3", "MP3"),
    ("vorbis", "Vorbis"),
)


class _YtLogger:
    def debug(self, msg):
        pass

    def info(self, msg):
        pass

    def warning(self, msg):
        logger.debug("[InlineDL] yt-dlp: %s", msg)

    def error(self, msg):
        logger.debug("[InlineDL] yt-dlp: %s", msg)


def _codec(name):
    if not name or name == "none":
        return ""
    low = name.lower()
    for prefix, label in CODECS:
        if low.startswith(prefix):
            return label
    return name.split(".")[0].upper()


def _platform_thumb(url):
    low = (url or "").lower()
    if "instagram.com" in low or "instagr.am" in low:
        return THUMB_IG
    if "tiktok.com" in low:
        return THUMB_TT
    if "pinterest." in low or "pin.it" in low:
        return THUMB_PT
    return BANNER


def _pick_thumb(entry):
    for t in reversed(entry.get("thumbnails") or []):
        u = t.get("url") or ""
        if JPG_RE.search(u):
            return u
    u = entry.get("thumbnail") or ""
    return u if JPG_RE.search(u) else None


def _sanitize_fn(name, default="video"):
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", name or "").strip(". ")
    return name[:120] or default


def _fmt_speed(b):
    if not b:
        return "..."
    if b < 1024:
        return f"{b:.0f} B/s"
    if b < MB:
        return f"{b / 1024:.1f} KB/s"
    return f"{b / MB:.1f} MB/s"


def _fmt_elapsed(s):
    s = int(s or 0)
    if s < 60:
        return f"{s}s"
    if s < 3600:
        m, sec = divmod(s, 60)
        return f"{m}m {sec}s" if sec else f"{m}m"
    h, rem = divmod(s, 3600)
    m, sec = divmod(rem, 60)
    return f"{h}h {m}m {sec}s"


def _fmt_dur(s):
    s = int(s or 0)
    if s < 3600:
        m, sec = divmod(s, 60)
        return f"{m}:{sec:02d}"
    h, rem = divmod(s, 3600)
    m, sec = divmod(rem, 60)
    return f"{h}:{m:02d}:{sec:02d}"


def _summary(fmts):
    video = next((f for f in fmts if f.get("vcodec") != "none"), None)
    audio = next((f for f in fmts if f.get("acodec") not in (None, "none")), None)
    size = sum(f.get("filesize") or f.get("filesize_approx") or 0 for f in fmts)
    w = (video or {}).get("width")
    h = (video or {}).get("height")
    return {
        "res": f"{w}x{h}" if w and h else "",
        "vcodec": _codec((video or {}).get("vcodec")),
        "acodec": _codec((audio or {}).get("acodec")),
        "size": size,
    }


async def fetch_pinterest_image(url):
    try:
        async with aiohttp.ClientSession(headers={"User-Agent": UA}) as s:
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


async def download_image(url):
    try:
        async with aiohttp.ClientSession(headers={"User-Agent": UA}) as s:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=30)) as r:
                if r.status != 200:
                    return None
                data = await r.read()
                return data if len(data) > 500 else None
    except Exception as e:
        logger.error(f"[InlineDL] Image download error: {e}", exc_info=True)
    return None


async def upload_to_x0(data, filename="image.jpg"):
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


async def get_pinterest_preview(url):
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


async def resolve_short(url, timeout=15):
    try:
        async with aiohttp.ClientSession(headers={"User-Agent": UA}) as s:
            async with s.get(
                url,
                allow_redirects=True,
                timeout=aiohttp.ClientTimeout(total=timeout),
            ) as r:
                return str(r.url)
    except Exception:
        return url


def detect_pinterest(text):
    for pat in (PINTEREST_SHORT_RE, PINTEREST_FULL_RE):
        m = pat.search(text or "")
        if m:
            return m.group(0)
    return None


@loader.tds
class InlineDL(loader.Module):
    """Inline video downloader: YouTube, Instagram, TikTok, Pinterest and more"""

    strings = {
        "name": "InlineDL",

        "hint_title": "InlineDL",
        "hint_desc": "Paste a YouTube, Instagram, TikTok, Pinterest or other link",
        "hint_msg": (
            "<blockquote><b>InlineDL</b></blockquote>\n"
            "<blockquote>Paste a link after <code>dl</code> to download media.</blockquote>\n"
            "<blockquote>Supported:\n"
            "<code>youtube.com/watch?v=...</code>\n"
            "<code>youtu.be/...</code>\n"
            "<code>instagram.com/reel/...</code>\n"
            "<code>tiktok.com/@.../video/...</code>\n"
            "<code>pinterest.com/pin/...</code>\n"
            "<code>pin.it/...</code>\n"
            "and other sites supported by yt-dlp</blockquote>"
        ),

        "invalid_title": "Invalid link",
        "invalid_desc": "Could not find a link in the query",
        "invalid_msg": (
            "<blockquote><b>InlineDL</b></blockquote>\n"
            "<blockquote>Could not find a valid link.</blockquote>"
        ),

        "err_title": "Error",
        "err_msg": (
            "<blockquote><b>InlineDL</b></blockquote>\n"
            "<blockquote>{error}</blockquote>"
        ),
        "no_video": "No video found by this link",
        "pt_not_found": "Could not find image on Pinterest",
        "ready_pt": "Pinterest",
        "ready_desc": "Tap to send",
        "pt_download": "Download image",

        "res_title": "Video {h}p",
        "best_title": "Video",
        "item_title": "{n}. {title}",
        "photo_title": "Photo (JPG)",
        "item_photo": "{n}. Photo: {title}",
        "desc_photo": "sent as photo",
        "btn_photo": "Photo",
        "ready_ig": "Instagram Photo",
        "ig_download": "Download photo",
        "raw_title": "RAW (max quality, file)",
        "audio_title": "Audio MP3 {br} kbps",
        "cobalt_video": "Video (cobalt)",
        "cobalt_audio": "Audio (cobalt)",
        "desc_convert": "will be converted to H.264",
        "desc_doc": "sent as file, no conversion",
        "desc_unknown": "best available",

        "pending": (
            "<b>{title}</b>\n"
            "<blockquote>{mode}</blockquote>\n"
            "<blockquote>Starting... If nothing happens, press <b>Download</b>.</blockquote>"
        ),
        "progress": (
            "<b>{title}</b>\n"
            "<blockquote>{mode}</blockquote>\n"
            "<pre><code class=\"language-inlinedl\">{body}</code></pre>"
        ),
        "caption": (
            "<blockquote><b><a href=\"{url}\">{title}</a></b></blockquote>\n"
            "<pre><code class=\"language-inlinedl\">{info}</code></pre>"
        ),
        "error": (
            "<b>{title}</b>\n"
            "<blockquote>Download failed</blockquote>\n"
            "<pre><code class=\"language-inlinedl\">{error}</code></pre>"
        ),
        "cancelled": (
            "<b>{title}</b>\n"
            "<blockquote>Download cancelled</blockquote>"
        ),

        "dl_wait": "Download: waiting...",
        "dl_prog": "Download: {pct:.1f}% ({cur:.1f}/{total:.1f} MB) | {speed}",
        "dl_prog_nt": "Download: {cur:.1f} MB | {speed}",
        "dl_done": "Download: done {size:.1f} MB",
        "ff_wait": "FFmpeg: waiting...",
        "ff_merge": "FFmpeg: merging streams...",
        "ff_mp3": "FFmpeg: encoding MP3...",
        "ff_prog": "FFmpeg: {kind} {pct:.1f}%",
        "ff_done": "FFmpeg: done {size:.1f} MB",
        "ff_skip": "FFmpeg: not needed",
        "ff_copy": "remux",
        "ff_x264": "H.264",
        "up_wait": "Telegram: waiting...",
        "up_prog": "Telegram: {pct:.1f}% ({cur:.1f}/{total:.1f} MB) | {speed}",
        "up_done": "Telegram: done {size:.1f} MB",
        "time_line": "Time: {elapsed} | {stage}",
        "stage_queue": "In queue...",
        "stage_extract": "Fetching info...",
        "stage_download": "Downloading...",
        "stage_merge": "Merging...",
        "stage_ffmpeg": "Converting...",
        "stage_upload": "Uploading to Telegram...",
        "stage_finish": "Finishing...",

        "too_large": "File is too large: {size:.1f} MB, Telegram limit is 4096 MB",
        "need_premium": "File is {size:.1f} MB, bot limit is 2000 MB. Telegram Premium on the userbot account is required for up to 4096 MB",
        "no_channel": "Asset channel not found, it is required for files over {limit} MB",
        "no_media": "Telegram did not return media",
        "cobalt_fail": "cobalt: {code}",

        "btn_download": "Download",
        "btn_cancel": "Cancel",
        "btn_retry": "Retry",
        "btn_raw": "RAW",
        "btn_audio": "MP3",
        "btn_video": "Video",
        "cb_started": "Started",
        "cb_running": "Already in progress",
        "cb_cancel": "Cancelling...",
        "cb_no_id": "Inline message id is missing",
        "cb_expired": "This download is no longer available, send the link again",

        "topic_desc": "InlineDL large files storage.",
        "greeting": (
            "<blockquote><b>InlineDL installed</b></blockquote>\n"
            "<blockquote>Files larger than <b>{direct} MB</b> are uploaded to this topic first and then sent into the inline message.</blockquote>\n"
            "<blockquote><b>Upload limits:</b>\n"
            "Inline bot: up to <b>2000 MB</b>\n"
            "Userbot account: up to <b>4096 MB</b> with Telegram Premium\n"
            "Premium: <b>{premium}</b></blockquote>\n"
            "<blockquote><b>Usage:</b> <code>@{bot} dl link</code></blockquote>"
        ),
        "reloaded": (
            "<blockquote><b>InlineDL reloaded, everything works</b></blockquote>\n"
            "<blockquote>Version: <b>{version}</b>\n"
            "yt-dlp: <b>{ytdlp}</b>\n"
            "Direct upload: up to <b>{direct} MB</b>\n"
            "Topic upload: up to <b>{limit} MB</b></blockquote>"
        ),
        "yes": "yes",
        "no": "no",

        "upd_running": "<b>InlineDL</b>\n<blockquote>Updating yt-dlp...</blockquote>",
        "upd_done": (
            "<b>InlineDL</b>\n"
            "<blockquote>yt-dlp: <code>{old}</code> -> <code>{new}</code></blockquote>\n"
            "<blockquote>Restart the userbot to apply the new version.</blockquote>"
        ),
        "upd_same": "<b>InlineDL</b>\n<blockquote>yt-dlp is up to date: <code>{ver}</code></blockquote>",
        "upd_fail": "<b>InlineDL</b>\n<blockquote>Update failed:</blockquote>\n<pre>{error}</pre>",

        "_cfg_direct": "Max file size in MB to upload directly into the inline message. Larger files go through the asset channel topic",
        "_cfg_jobs": "How many downloads can run at the same time",
        "_cfg_interval": "Progress message update interval in seconds",
        "_cfg_bitrate": "MP3 bitrate in kbps",
        "_cfg_cookies": "Path to cookies.txt for yt-dlp (needed for private or age restricted content)",
        "_cfg_proxy": "Proxy for yt-dlp, for example socks5://127.0.0.1:1080",
        "_cfg_cobalt": "Self hosted cobalt API URL, used as fallback when yt-dlp fails",
        "_cfg_cobalt_key": "cobalt API key (Api-Key auth)",
        "_cfg_feedback": "Automatically enable inline feedback for the inline bot via @BotFather",
    }

    strings_ru = {
        "hint_title": "InlineDL",
        "hint_desc": "Вставьте ссылку YouTube, Instagram, TikTok, Pinterest или другую",
        "hint_msg": (
            "<blockquote><b>InlineDL</b></blockquote>\n"
            "<blockquote>Вставьте ссылку после <code>dl</code> для скачивания медиа.</blockquote>\n"
            "<blockquote>Поддерживаются:\n"
            "<code>youtube.com/watch?v=...</code>\n"
            "<code>youtu.be/...</code>\n"
            "<code>instagram.com/reel/...</code>\n"
            "<code>tiktok.com/@.../video/...</code>\n"
            "<code>pinterest.com/pin/...</code>\n"
            "<code>pin.it/...</code>\n"
            "и другие сайты, поддерживаемые yt-dlp</blockquote>"
        ),

        "invalid_title": "Неверная ссылка",
        "invalid_desc": "Не удалось найти ссылку в запросе",
        "invalid_msg": (
            "<blockquote><b>InlineDL</b></blockquote>\n"
            "<blockquote>Не удалось найти ссылку.</blockquote>"
        ),

        "err_title": "Ошибка",
        "err_msg": (
            "<blockquote><b>InlineDL</b></blockquote>\n"
            "<blockquote>{error}</blockquote>"
        ),
        "no_video": "По этой ссылке видео не найдено",
        "pt_not_found": "Не удалось найти изображение на Pinterest",
        "ready_pt": "Pinterest",
        "ready_desc": "Нажмите чтобы отправить",
        "pt_download": "Скачать изображение",

        "res_title": "Видео {h}p",
        "best_title": "Видео",
        "item_title": "{n}. {title}",
        "photo_title": "Фото (JPG)",
        "item_photo": "{n}. Фото: {title}",
        "desc_photo": "отправка как фото",
        "btn_photo": "Фото",
        "ready_ig": "Фото Instagram",
        "ig_download": "Скачать фото",
        "raw_title": "RAW (макс. качество, файлом)",
        "audio_title": "Аудио MP3 {br} kbps",
        "cobalt_video": "Видео (cobalt)",
        "cobalt_audio": "Аудио (cobalt)",
        "desc_convert": "будет сконвертировано в H.264",
        "desc_doc": "файлом, без конвертации",
        "desc_unknown": "лучшее доступное",

        "pending": (
            "<b>{title}</b>\n"
            "<blockquote>{mode}</blockquote>\n"
            "<blockquote>Запуск... Если ничего не происходит, нажмите <b>Скачать</b>.</blockquote>"
        ),
        "progress": (
            "<b>{title}</b>\n"
            "<blockquote>{mode}</blockquote>\n"
            "<pre><code class=\"language-inlinedl\">{body}</code></pre>"
        ),
        "caption": (
            "<blockquote><b><a href=\"{url}\">{title}</a></b></blockquote>\n"
            "<pre><code class=\"language-inlinedl\">{info}</code></pre>"
        ),
        "error": (
            "<b>{title}</b>\n"
            "<blockquote>Ошибка загрузки</blockquote>\n"
            "<pre><code class=\"language-inlinedl\">{error}</code></pre>"
        ),
        "cancelled": (
            "<b>{title}</b>\n"
            "<blockquote>Загрузка отменена</blockquote>"
        ),

        "dl_wait": "Загрузка: ожидание...",
        "dl_prog": "Загрузка: {pct:.1f}% ({cur:.1f}/{total:.1f} MB) | {speed}",
        "dl_prog_nt": "Загрузка: {cur:.1f} MB | {speed}",
        "dl_done": "Загрузка: готово {size:.1f} MB",
        "ff_wait": "FFmpeg: ожидание...",
        "ff_merge": "FFmpeg: слияние потоков...",
        "ff_mp3": "FFmpeg: кодирование MP3...",
        "ff_prog": "FFmpeg: {kind} {pct:.1f}%",
        "ff_done": "FFmpeg: готово {size:.1f} MB",
        "ff_skip": "FFmpeg: не требуется",
        "ff_copy": "ремукс",
        "ff_x264": "H.264",
        "up_wait": "Telegram: ожидание...",
        "up_prog": "Telegram: {pct:.1f}% ({cur:.1f}/{total:.1f} MB) | {speed}",
        "up_done": "Telegram: готово {size:.1f} MB",
        "time_line": "Время: {elapsed} | {stage}",
        "stage_queue": "В очереди...",
        "stage_extract": "Получение информации...",
        "stage_download": "Скачивание...",
        "stage_merge": "Слияние...",
        "stage_ffmpeg": "Конвертация...",
        "stage_upload": "Загрузка в Telegram...",
        "stage_finish": "Завершение...",

        "too_large": "Файл слишком большой: {size:.1f} MB, лимит Telegram 4096 MB",
        "need_premium": "Файл {size:.1f} MB, лимит бота 2000 MB. Для файлов до 4096 MB нужен Telegram Premium на аккаунте юзербота",
        "no_channel": "Не найден канал ассетов, он нужен для файлов больше {limit} MB",
        "no_media": "Telegram не вернул медиа",
        "cobalt_fail": "cobalt: {code}",

        "btn_download": "Скачать",
        "btn_cancel": "Отмена",
        "btn_retry": "Повторить",
        "btn_raw": "RAW",
        "btn_audio": "MP3",
        "btn_video": "Видео",
        "cb_started": "Запущено",
        "cb_running": "Уже выполняется",
        "cb_cancel": "Отменяю...",
        "cb_no_id": "Нет id инлайн сообщения",
        "cb_expired": "Загрузка больше недоступна, отправьте ссылку заново",

        "topic_desc": "Хранилище больших файлов InlineDL.",
        "greeting": (
            "<blockquote><b>InlineDL установлен</b></blockquote>\n"
            "<blockquote>Файлы больше <b>{direct} MB</b> сначала загружаются в этот топик, а затем отправляются в инлайн сообщение.</blockquote>\n"
            "<blockquote><b>Лимиты загрузки:</b>\n"
            "Инлайн бот: до <b>2000 MB</b>\n"
            "Аккаунт юзербота: до <b>4096 MB</b> с Telegram Premium\n"
            "Premium: <b>{premium}</b></blockquote>\n"
            "<blockquote><b>Использование:</b> <code>@{bot} dl ссылка</code></blockquote>"
        ),
        "reloaded": (
            "<blockquote><b>InlineDL перезагружен, все работает</b></blockquote>\n"
            "<blockquote>Версия: <b>{version}</b>\n"
            "yt-dlp: <b>{ytdlp}</b>\n"
            "Прямая загрузка: до <b>{direct} MB</b>\n"
            "Загрузка через топик: до <b>{limit} MB</b></blockquote>"
        ),
        "yes": "да",
        "no": "нет",

        "upd_running": "<b>InlineDL</b>\n<blockquote>Обновляю yt-dlp...</blockquote>",
        "upd_done": (
            "<b>InlineDL</b>\n"
            "<blockquote>yt-dlp: <code>{old}</code> -> <code>{new}</code></blockquote>\n"
            "<blockquote>Перезапустите юзербот, чтобы применить новую версию.</blockquote>"
        ),
        "upd_same": "<b>InlineDL</b>\n<blockquote>yt-dlp уже последней версии: <code>{ver}</code></blockquote>",
        "upd_fail": "<b>InlineDL</b>\n<blockquote>Не удалось обновить:</blockquote>\n<pre>{error}</pre>",

        "_cfg_direct": "Макс. размер файла в MB для прямой загрузки в инлайн сообщение. Файлы больше идут через топик канала ассетов",
        "_cfg_jobs": "Сколько загрузок может идти одновременно",
        "_cfg_interval": "Интервал обновления прогресса в секундах",
        "_cfg_bitrate": "Битрейт MP3 в kbps",
        "_cfg_cookies": "Путь к cookies.txt для yt-dlp (нужно для приватного или 18+ контента)",
        "_cfg_proxy": "Прокси для yt-dlp, например socks5://127.0.0.1:1080",
        "_cfg_cobalt": "URL своего cobalt API, используется как запасной вариант, если yt-dlp не справился",
        "_cfg_cobalt_key": "Ключ cobalt API (Api-Key)",
        "_cfg_feedback": "Автоматически включить inline feedback у инлайн бота через @BotFather",
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "DIRECT_LIMIT_MB",
                50,
                lambda: self.strings["_cfg_direct"],
                validator=loader.validators.Integer(minimum=1, maximum=2000),
            ),
            loader.ConfigValue(
                "MAX_JOBS",
                2,
                lambda: self.strings["_cfg_jobs"],
                validator=loader.validators.Integer(minimum=1, maximum=10),
            ),
            loader.ConfigValue(
                "PROGRESS_INTERVAL",
                3,
                lambda: self.strings["_cfg_interval"],
                validator=loader.validators.Integer(minimum=1, maximum=10),
            ),
            loader.ConfigValue(
                "MP3_BITRATE",
                320,
                lambda: self.strings["_cfg_bitrate"],
                validator=loader.validators.Choice([128, 192, 256, 320]),
            ),
            loader.ConfigValue(
                "COOKIES_FILE",
                "",
                lambda: self.strings["_cfg_cookies"],
                validator=loader.validators.String(),
            ),
            loader.ConfigValue(
                "PROXY",
                "",
                lambda: self.strings["_cfg_proxy"],
                validator=loader.validators.String(),
            ),
            loader.ConfigValue(
                "COBALT_API",
                "",
                lambda: self.strings["_cfg_cobalt"],
                validator=loader.validators.String(),
            ),
            loader.ConfigValue(
                "COBALT_KEY",
                "",
                lambda: self.strings["_cfg_cobalt_key"],
                validator=loader.validators.Hidden(loader.validators.String()),
            ),
            loader.ConfigValue(
                "AUTO_FEEDBACK",
                True,
                lambda: self.strings["_cfg_feedback"],
                validator=loader.validators.Boolean(),
            ),
        )
        self._cache = OrderedDict()
        self._by_url = {}
        self._preview_cache = {}
        self._tasks = {}
        self._sem = None
        self._sem_size = 0
        self._ffmpeg = None
        self._bg = set()
        self._topic_id = None
        self._topic_lock = None

    @property
    def _work_dir(self):
        uid = getattr(self, "_tg_id", None)
        if not uid and getattr(self, "_client", None):
            uid = getattr(self._client, "tg_id", None)
        if not uid:
            uid = "common"
        p = os.path.join(tempfile.gettempdir(), "InlineDL", str(uid))
        os.makedirs(p, exist_ok=True)
        return p

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        try:
            me = await client.get_me()
            self._tg_id = me.id
        except Exception:
            self._tg_id = getattr(client, "tg_id", None)
        os.makedirs(self._work_dir, exist_ok=True)
        self._spawn(self._startup())
        if self.config["AUTO_FEEDBACK"]:
            self._spawn(self._ensure_feedback())

    async def _startup(self):
        topic_id = None
        for attempt in range(6):
            try:
                topic_id = await self._ensure_topic()
                break
            except Exception as e:
                logger.warning(f"[InlineDL] topic setup failed: {e}")
                await asyncio.sleep(10 * (attempt + 1))
        if topic_id:
            await self._notify(topic_id)

    async def _wait_channel(self, limit=120):
        channel = self._asset_channel()
        waited = 0
        while not channel and waited < limit:
            await asyncio.sleep(5)
            waited += 5
            channel = self._asset_channel()
        return channel

    async def _ensure_topic(self, fresh=False):
        if self._topic_lock is None:
            self._topic_lock = asyncio.Lock()
        async with self._topic_lock:
            if self._topic_id and not fresh:
                return self._topic_id
            channel = await self._wait_channel(0 if fresh else 120)
            if not channel:
                raise RuntimeError(self.strings["no_channel"].format(limit=self.config["DIRECT_LIMIT_MB"]))
            topic = await utils.asset_forum_topic(
                self._client,
                self._db,
                channel,
                TOPIC_TITLE,
                description=self.strings["topic_desc"],
                icon_emoji_id=TOPIC_ICON,
                invite_bot=True,
            )
            if getattr(topic, "icon_emoji_id", None) != TOPIC_ICON:
                try:
                    await self._client(functions.messages.EditForumTopicRequest(
                        peer=channel,
                        topic_id=topic.id,
                        icon_emoji_id=TOPIC_ICON,
                    ))
                except Exception as e:
                    logger.debug(f"[InlineDL] topic icon not set: {e}")
            self._topic_id = topic.id
            return topic.id

    async def _notify(self, topic_id):
        channel = self._asset_channel()
        if not channel:
            return
        key = f"inlinedl_greeted_{str(channel)[4:]}_{topic_id}"
        premium = self._premium()
        values = dict(
            version=".".join(map(str, __version__)),
            ytdlp=utils.escape_html(yt_dlp.version.__version__),
            direct=self.config["DIRECT_LIMIT_MB"],
            limit=USER_LIMIT // MB if premium else BOT_LIMIT // MB,
            premium=self.strings["yes"] if premium else self.strings["no"],
            bot=utils.escape_html(getattr(self.inline, "bot_username", None) or "bot"),
        )
        if self.get(key, False):
            text = self.strings["reloaded"].format(**values)
        else:
            self.set(key, True)
            text = self.strings["greeting"].format(**values)
        await self._send_with_preview(channel, topic_id, text)

    async def _send_with_preview(self, channel, topic_id, text):
        bot = self._bot()
        try:
            msg_text, entities = await bot._parse_message_text(text, "html")
            msg = await bot.send_message(
                channel,
                msg_text,
                formatting_entities=entities,
                reply_to=topic_id,
                link_preview=False,
            )
            try:
                peer = await bot.get_input_entity(channel)
                current = await bot.get_messages(channel, ids=msg.id)
                reply_markup = current.reply_markup if current else None
                await bot(functions.messages.EditMessageRequest(
                    peer=peer,
                    id=msg.id,
                    message=msg_text,
                    media=InputMediaWebPage(
                        url=RELOAD_BANNER,
                        optional=True,
                        force_large_media=True,
                    ),
                    invert_media=True,
                    reply_markup=reply_markup,
                    entities=entities,
                    no_webpage=False,
                ))
            except Exception as e:
                logger.error(f"[InlineDL] Failed to add preview: {e}")
        except Exception as e:
            logger.error(f"[InlineDL] Failed to send message with preview: {e}")

    async def on_unload(self):
        for job in list(self._tasks.values()):
            job["cancel"] = True
            task = job.get("task")
            if task and not task.done():
                task.cancel()
        for task in list(self._bg):
            task.cancel()

    def _spawn(self, coro):
        task = asyncio.ensure_future(coro)
        self._bg.add(task)
        task.add_done_callback(self._bg.discard)
        return task

    def _bot(self):
        return getattr(self.inline, "_bot_client", None) or self.inline.bot.client

    def _semaphore(self):
        size = int(self.config["MAX_JOBS"])
        if self._sem is None or self._sem_size != size:
            self._sem = asyncio.Semaphore(size)
            self._sem_size = size
        return self._sem

    def _ffmpeg_bin(self):
        if self._ffmpeg and os.path.exists(self._ffmpeg):
            return self._ffmpeg
        path = shutil.which("ffmpeg")
        if not path:
            import imageio_ffmpeg

            exe = imageio_ffmpeg.get_ffmpeg_exe()
            bin_dir = os.path.join(self._work_dir, "bin")
            os.makedirs(bin_dir, exist_ok=True)
            path = os.path.join(bin_dir, "ffmpeg")
            if os.path.realpath(path) != os.path.realpath(exe):
                try:
                    if os.path.lexists(path):
                        os.remove(path)
                    os.symlink(exe, path)
                except OSError:
                    path = exe
        self._ffmpeg = path
        return path

    def _premium(self):
        me = getattr(self._client, "feroku_me", None) or getattr(self._client, "heroku_me", None)
        return bool(getattr(me, "premium", False))

    def _owners(self):
        sec = getattr(getattr(self._client, "dispatcher", None), "security", None)
        owners = set(getattr(sec, "_owner", None) or [])
        owners.add(getattr(self._client, "tg_id", None))
        return owners

    def _asset_channel(self):
        cid = self._db.get("feroku.forums", "channel_id", None) or self._db.get(
            "heroku.forums", "channel_id", None
        )
        if not cid:
            return None
        cid = abs(int(cid))
        text = str(cid)
        if text.startswith("100") and len(text) > 12:
            cid = int(text[3:])
        return int(f"-100{cid}")

    async def _ensure_feedback(self):
        await asyncio.sleep(5)
        username = getattr(self.inline, "bot_username", None)
        if not username or self.get("feedback_for") == username:
            return
        try:
            async with self._client.conversation("@BotFather", timeout=30) as conv:
                reply = None
                for text in ("/setinlinefeedback", f"@{username}", "Enabled"):
                    sent = await conv.send_message(text)
                    reply = await conv.get_response()
                    await asyncio.sleep(1)
                    for m in (sent, reply):
                        try:
                            await m.delete()
                        except Exception:
                            pass
            if reply and "success" in (reply.raw_text or "").lower():
                self.set("feedback_for", username)
        except Exception as e:
            logger.warning(f"[InlineDL] inline feedback setup failed: {e}")

    def _cache_put(self, url, info, user_id):
        now = time.time()
        for key in [k for k, v in self._cache.items() if now - v["ts"] > CACHE_TTL]:
            self._by_url.pop(self._cache.pop(key)["url"], None)
        while len(self._cache) >= CACHE_MAX:
            _, old = self._cache.popitem(last=False)
            self._by_url.pop(old["url"], None)
        cid = secrets.token_hex(4)
        self._cache[cid] = {"url": url, "info": info, "user": user_id, "ts": now}
        self._by_url[url] = cid
        return cid

    def _cache_get(self, cid):
        entry = self._cache.get(cid)
        if entry and time.time() - entry["ts"] <= CACHE_TTL:
            return entry
        return None

    def _ydl_opts(self, mode=None, workdir=None):
        opts = {
            "quiet": True,
            "no_warnings": True,
            "noprogress": True,
            "noplaylist": True,
            "socket_timeout": 20,
            "retries": 5,
            "fragment_retries": 5,
            "concurrent_fragment_downloads": 4,
            "ffmpeg_location": self._ffmpeg_bin(),
            "logger": _YtLogger(),
        }
        cookies = (self.config["COOKIES_FILE"] or "").strip()
        if cookies and os.path.isfile(cookies):
            opts["cookiefile"] = cookies
        proxy = (self.config["PROXY"] or "").strip()
        if proxy:
            opts["proxy"] = proxy
        if mode and mode.startswith("v"):
            height = int(mode[1:] or 0)
            opts["format"] = "bv*+ba/b"
            opts["format_sort"] = (
                [f"res:{height}", "vcodec:h264", "acodec:aac"]
                if height
                else ["vcodec:h264", "acodec:aac"]
            )
            opts["merge_output_format"] = "mkv"
        elif mode == "raw":
            opts["format"] = "bv*+ba/b"
            opts["merge_output_format"] = "mkv"
        elif mode == "audio":
            opts["format"] = "ba/b"
            opts["postprocessors"] = [{
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": str(self.config["MP3_BITRATE"]),
            }]
        if workdir:
            opts["outtmpl"] = os.path.join(workdir, "media.%(ext)s")
        return opts

    def _extract_sync(self, url):
        is_ig = bool(re.search(r"instagram\.com|instagr\.am", url, re.I))
        opts = self._ydl_opts()
        if is_ig:
            opts["noplaylist"] = False
        with yt_dlp.YoutubeDL(opts) as y:
            info = y.extract_info(url, download=False, process=not is_ig)
            return y.sanitize_info(info)

    async def _extract(self, url):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._extract_sync, url)

    def _entries(self, info):
        if not info:
            return []
        if info.get("_type") == "playlist" or "entries" in info:
            raw = [
                e
                for e in (info.get("entries") or [])
                if e and (e.get("formats") or e.get("thumbnails") or e.get("url"))
            ]
            return raw[:MAX_ENTRIES]
        return [info] if (info.get("formats") or info.get("thumbnails") or info.get("url")) else []

    def _heights(self, entry):
        found = set()
        for f in entry.get("formats") or []:
            if f.get("vcodec") == "none":
                continue
            w, h = f.get("width"), f.get("height")
            if w and h:
                found.add(min(w, h))
        return sorted(found, reverse=True)[:MAX_HEIGHTS]

    def _fresh(self, entry):
        entry = copy.deepcopy(entry)
        for key in ("requested_formats", "requested_downloads", "requested_subtitles"):
            entry.pop(key, None)
        return entry

    def _select_sync(self, entry, modes):
        result = {}
        for mode in modes:
            try:
                with yt_dlp.YoutubeDL(self._ydl_opts(mode)) as y:
                    r = y.process_ie_result(self._fresh(entry), download=False)
                result[mode] = r.get("requested_formats") or [
                    f for f in r.get("formats") or [] if f.get("format_id") == r.get("format_id")
                ] or [r]
            except Exception:
                result[mode] = []
        return result

    def _mode_title(self, mode):
        if mode == "raw":
            return self.strings["raw_title"]
        if mode == "photo":
            return self.strings["photo_title"]
        if mode == "audio":
            return self.strings["audio_title"].format(br=self.config["MP3_BITRATE"])
        if mode == "cv":
            return self.strings["cobalt_video"]
        if mode == "ca":
            return self.strings["cobalt_audio"]
        height = int(mode[1:] or 0)
        return self.strings["res_title"].format(h=height) if height else self.strings["best_title"]

    def _mode_desc(self, mode, fmts):
        if not fmts:
            return self.strings["desc_unknown"]
        s = _summary(fmts)
        parts = []
        if mode == "audio":
            if s["acodec"]:
                parts.append(f"{s['acodec']} -> MP3")
        else:
            if s["res"]:
                parts.append(s["res"])
            codecs = " + ".join(c for c in (s["vcodec"], s["acodec"]) if c)
            if codecs:
                parts.append(codecs)
        if s["size"]:
            parts.append(f"~{s['size'] / MB:.1f} MB")
        if mode == "raw":
            parts.append(self.strings["desc_doc"])
        elif mode.startswith("v") and s["vcodec"] and s["vcodec"] != "H.264":
            parts.append(self.strings["desc_convert"])
        return " | ".join(parts) or self.strings["desc_unknown"]

    def _web_doc(self, url, mime_type="image/png"):
        return InputWebDocument(url=url, size=0, mime_type=mime_type, attributes=[])

    def _make_article(self, uid, title, desc, text, preview_url=None, thumb_url=None, markup=None):
        plain, entities = tl_html.parse(text)
        if preview_url:
            send_message = InputBotInlineMessageMediaWebPage(
                message=plain,
                url=preview_url,
                force_large_media=True,
                invert_media=True,
                entities=entities or None,
                reply_markup=markup,
            )
        else:
            send_message = InputBotInlineMessageText(
                message=plain,
                no_webpage=True,
                entities=entities or None,
                reply_markup=markup,
            )
        thumb_url = thumb_url or BANNER
        mime = "image/jpeg" if JPG_RE.search(thumb_url) else "image/png"
        return InputBotInlineResult(
            id=uid[:64],
            type="article",
            title=title[:200],
            description=desc[:200],
            thumb=self._web_doc(thumb_url, mime),
            send_message=send_message,
        )

    async def _answer_one(self, query, key, title, desc, text, thumb=None):
        await query.answer(
            results=[self._make_article(
                uid=f"{key}_{int(time.time())}",
                title=title,
                desc=desc,
                text=text,
                thumb_url=thumb or BANNER,
            )],
            cache_time=0,
            private=True,
        )

    def _start_markup(self, cid, mode, idx, url, user_id):
        rows = self.inline.generate_markup([[{
            "text": self.strings["btn_download"],
            "callback": self._cb_start,
            "args": (cid, mode, idx, url),
            "always_allow": [user_id],
            "style": "success",
        }]])
        return self._bot().build_reply_markup(rows)

    def _result(self, cid, mode, idx, url, title, desc, thumb, user_id, label=None):
        label = label or self._mode_title(mode)
        text = self.strings["pending"].format(
            title=utils.escape_html(title[:200]),
            mode=utils.escape_html(label),
        )
        return self._make_article(
            uid=f"idl|{cid}|{mode}|{idx}",
            title=label,
            desc=desc,
            text=text,
            thumb_url=thumb,
            markup=self._start_markup(cid, mode, idx, url, user_id),
        )

    async def _pinterest_preview(self, url):
        if url in self._preview_cache:
            return self._preview_cache[url]
        preview_url = await get_pinterest_preview(url)
        if preview_url:
            self._preview_cache[url] = preview_url
        return preview_url

    async def _warm_preview(self, preview_url):
        try:
            await asyncio.wait_for(
                self._client(functions.messages.GetWebPageRequest(url=preview_url, hash=0)),
                timeout=3,
            )
        except Exception:
            pass

    def _pinterest_article(self, preview_url):
        return self._make_article(
            uid=f"pt_{int(time.time())}",
            title=self.strings["ready_pt"],
            desc=self.strings["ready_desc"],
            text=f'<blockquote><a href="{preview_url}"><b>{self.strings["pt_download"]}</b></a></blockquote>',
            preview_url=preview_url,
            thumb_url=THUMB_PT,
        )

    async def _pinterest_video(self, matched, user_id):
        url = await resolve_short(matched, 5) if PINTEREST_SHORT_RE.match(matched) else matched
        if not PINTEREST_RE.match(url):
            return None
        cid = self._by_url.get(url)
        cached = self._cache_get(cid) if cid else None
        info = cached["info"] if cached else await self._extract(url)
        if not info or (info.get("extractor_key") or "Generic") == "Generic":
            return None
        entries = [e for e in self._entries(info) if self._heights(e)]
        if not entries:
            return None
        if not cached:
            cid = self._cache_put(url, info, user_id)
        return cid, url, entries

    async def _pinterest_answer(self, query, matched, user_id):
        image = asyncio.ensure_future(self._pinterest_preview(matched))
        video = asyncio.ensure_future(self._pinterest_video(matched, user_id))
        self._bg.add(image)
        image.add_done_callback(self._bg.discard)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + PIN_BUDGET
        await asyncio.wait({image}, timeout=PIN_BUDGET)
        preview_url = image.result() if image.done() and not image.exception() else ""
        if not video.done():
            left = max(0, deadline - loop.time())
            await asyncio.wait({video}, timeout=min(left, PIN_GRACE) if preview_url else left)
        if not video.done():
            video.cancel()
        found = None
        if video.done() and not video.cancelled() and not video.exception():
            found = video.result()

        results = []
        if preview_url:
            await self._warm_preview(preview_url)
            results.append(self._pinterest_article(preview_url))
        if found:
            cid, url, entries = found
            results += await self._video_results(cid, url, entries, user_id, THUMB_PT)

        if not results:
            await self._answer_one(
                query, "pt_err", self.strings["err_title"], self.strings["pt_not_found"],
                self.strings["err_msg"].format(error=self.strings["pt_not_found"]), THUMB_PT,
            )
            return
        await query.answer(results=results, cache_time=0, private=True)

    async def _video_results(self, cid, url, entries, user_id, fallback_thumb):
        results = []
        loop = asyncio.get_running_loop()
        img_match = re.search(r"[?&]img_index=(\d+)", url)
        if img_match and len(entries) > 1:
            target = int(img_match.group(1)) - 1
            if 0 <= target < len(entries):
                entries = [entries[target]] + [e for i, e in enumerate(entries) if i != target]
        if len(entries) > 1:
            for idx, entry in enumerate(entries):
                title = entry.get("title") or entry.get("id") or url
                is_photo = not bool(entry.get("formats"))
                mode = "photo" if is_photo else "v0"
                thumb = _pick_thumb(entry) or fallback_thumb
                if is_photo:
                    desc = self.strings["photo_title"]
                    label = self.strings["item_photo"].format(n=idx + 1, title=title[:60])
                else:
                    desc = self._mode_desc("v0", (await loop.run_in_executor(None, self._select_sync, entry, ["v0"]))["v0"])
                    label = self.strings["item_title"].format(n=idx + 1, title=title[:60])
                results.append(self._result(
                    cid, mode, idx, url, title,
                    desc, thumb, user_id,
                    label=label,
                ))
            return results
        entry = entries[0]
        title = entry.get("title") or entry.get("id") or url
        thumb = _pick_thumb(entry) or fallback_thumb
        is_photo = not bool(entry.get("formats"))
        if is_photo:
            results.append(self._result(
                cid, "photo", 0, url, title,
                self.strings["desc_photo"],
                thumb, user_id,
            ))
            results.append(self._result(
                cid, "raw", 0, url, title,
                self.strings["desc_doc"],
                thumb, user_id,
            ))
            return results
        modes = [f"v{h}" for h in self._heights(entry)] or ["v0"]
        modes += ["raw", "audio"]
        selected = await loop.run_in_executor(None, self._select_sync, entry, modes)
        for mode in modes:
            if mode != "audio" and mode != "v0" and not selected.get(mode):
                continue
            results.append(self._result(
                cid, mode, 0, url, title,
                self._mode_desc(mode, selected.get(mode)),
                thumb, user_id,
            ))
        return results

    @loader.inline_handler(
        ru_doc="Скачать видео или аудио из YouTube, Instagram, TikTok, Pinterest и других сайтов",
        en_doc="Download video or audio from YouTube, Instagram, TikTok, Pinterest and other sites",
    )
    async def dl_inline_handler(self, query):
        """Download video or audio from YouTube, Instagram, TikTok, Pinterest and other sites"""
        text = query.query.strip()
        if text.lower().startswith("dl"):
            text = text[2:].strip()

        if not text:
            await self._answer_one(
                query, "hint", self.strings["hint_title"], self.strings["hint_desc"],
                self.strings["hint_msg"],
            )
            return

        match = URL_RE.search(text)
        if not match:
            await self._answer_one(
                query, "inv", self.strings["invalid_title"], self.strings["invalid_desc"],
                self.strings["invalid_msg"],
            )
            return

        user_id = query.from_user.id
        pin = detect_pinterest(text)
        if pin:
            await self._pinterest_answer(query, pin, user_id)
            return

        url = match.group(0)
        fallback_thumb = _platform_thumb(url)

        cid = self._by_url.get(url)
        cached = self._cache_get(cid) if cid else None
        info = cached["info"] if cached else None
        error = None
        if info is None:
            try:
                info = await asyncio.wait_for(self._extract(url), timeout=15)
            except Exception as e:
                error = re.sub(r"^ERROR:\s*", "", str(e)).strip() or type(e).__name__
                info = None

        entries = self._entries(info)
        if not entries:
            if (self.config["COBALT_API"] or "").strip():
                cid = self._cache_put(url, None, user_id)
                title = url
                await query.answer(
                    results=[
                        self._result(cid, m, 0, url, title, url, fallback_thumb, user_id)
                        for m in ("cv", "ca")
                    ],
                    cache_time=0,
                    private=True,
                )
                return
            msg = error or self.strings["no_video"]
            await self._answer_one(
                query, "err", self.strings["err_title"], msg[:200],
                self.strings["err_msg"].format(error=utils.escape_html(msg[:900])),
                fallback_thumb,
            )
            return

        if not cached:
            cid = self._cache_put(url, info, user_id)

        results = await self._video_results(cid, url, entries, user_id, fallback_thumb)
        await query.answer(results=results, cache_time=0, private=True)

    @loader.need_update("chosen_inline_result")
    async def _on_chosen(self, update):
        rid = getattr(update, "id", "") or ""
        if not rid.startswith("idl|") or not getattr(update, "msg_id", None):
            return
        try:
            _, cid, mode, idx = rid.split("|")
            idx = int(idx)
        except ValueError:
            return
        entry = self._cache_get(cid)
        if entry:
            if update.user_id != entry["user"] and update.user_id not in self._owners():
                return
            url = entry["url"]
        else:
            if update.user_id not in self._owners():
                return
            match = URL_RE.search(getattr(update, "query", "") or "")
            if not match:
                return
            url = match.group(0)
        self._start(update.msg_id, cid, mode, idx, url, update.user_id)

    async def _cb_start(self, call, cid, mode, idx, url):
        mid = call.inline_message_id
        if not mid:
            await call.answer(self.strings["cb_no_id"], show_alert=True)
            return
        key = bytes(mid)
        if key in self._tasks:
            await call.answer(self.strings["cb_running"])
            return
        self._start(mid, cid, mode, idx, url, call.sender_id)
        await call.answer(self.strings["cb_started"])

    async def _cb_cancel(self, call, key):
        job = self._tasks.get(key)
        if job:
            job["cancel"] = True
            task = job.get("task")
            if task and not task.done():
                task.cancel()
        await call.answer(self.strings["cb_cancel"])

    def _start(self, mid, cid, mode, idx, url, user_id):
        key = bytes(mid)
        if key in self._tasks:
            return None
        entry = self._cache_get(cid)
        info = entry["info"] if entry else None
        entries = self._entries(info)
        item = entries[idx] if idx < len(entries) else None
        title = (item or {}).get("title") or url
        job = {
            "key": key,
            "mid": mid,
            "cid": cid,
            "url": url,
            "mode": mode,
            "idx": idx,
            "user": user_id,
            "entry": item,
            "title": title,
            "label": self._mode_title(mode),
            "dir": os.path.join(self._work_dir, secrets.token_hex(6)),
            "cancel": False,
            "done": asyncio.Event(),
            "t0": time.time(),
            "last": None,
            "st": {
                "stage": "queue",
                "dl_cur": 0,
                "dl_total": 0,
                "dl_base": 0,
                "dl_est": 0,
                "dl_speed": 0,
                "dl_done": False,
                "ff": "wait",
                "ff_kind": "",
                "ff_pct": 0.0,
                "ff_size": 0,
                "up_cur": 0,
                "up_total": 0,
                "up_t0": 0.0,
                "up_done": False,
            },
        }
        job["cancel_markup"] = self.inline.generate_markup([[{
            "text": self.strings["btn_cancel"],
            "callback": self._cb_cancel,
            "args": (key,),
            "always_allow": [user_id],
            "style": "danger",
        }]])
        self._tasks[key] = job
        job["task"] = asyncio.ensure_future(self._run_job(job))
        return job

    async def _edit(self, mid, text, file=None, buttons=None, retries=3):
        for attempt in range(retries):
            try:
                await self._bot().edit_message(
                    mid,
                    text,
                    file=file,
                    buttons=buttons,
                    parse_mode="html",
                    link_preview=False,
                )
                return True
            except MessageNotModifiedError:
                return True
            except FloodWaitError as e:
                if attempt == retries - 1:
                    raise
                await asyncio.sleep(min(int(e.seconds) + 1, 60))
        return False

    def _render(self, job):
        s = job["st"]
        if s["dl_done"]:
            dl = self.strings["dl_done"].format(size=s["dl_cur"] / MB)
        elif s["dl_cur"] and s["dl_total"]:
            dl = self.strings["dl_prog"].format(
                pct=min(s["dl_cur"] / s["dl_total"] * 100, 100.0),
                cur=s["dl_cur"] / MB,
                total=s["dl_total"] / MB,
                speed=_fmt_speed(s["dl_speed"]),
            )
        elif s["dl_cur"]:
            dl = self.strings["dl_prog_nt"].format(cur=s["dl_cur"] / MB, speed=_fmt_speed(s["dl_speed"]))
        else:
            dl = self.strings["dl_wait"]

        ff_state = s["ff"]
        if ff_state == "merge":
            ff = self.strings["ff_merge"]
        elif ff_state == "mp3":
            ff = self.strings["ff_mp3"]
        elif ff_state == "run":
            ff = self.strings["ff_prog"].format(kind=s["ff_kind"], pct=s["ff_pct"])
        elif ff_state == "done":
            ff = self.strings["ff_done"].format(size=s["ff_size"] / MB)
        elif ff_state == "skip":
            ff = self.strings["ff_skip"]
        else:
            ff = self.strings["ff_wait"]

        if s["up_done"]:
            up = self.strings["up_done"].format(size=s["up_total"] / MB)
        elif s["up_total"]:
            spent = max(time.time() - s["up_t0"], 0.001)
            up = self.strings["up_prog"].format(
                pct=s["up_cur"] / s["up_total"] * 100,
                cur=s["up_cur"] / MB,
                total=s["up_total"] / MB,
                speed=_fmt_speed(s["up_cur"] / spent),
            )
        else:
            up = self.strings["up_wait"]

        line = self.strings["time_line"].format(
            elapsed=_fmt_elapsed(time.time() - job["t0"]),
            stage=self.strings[f"stage_{s['stage']}"],
        )
        return self.strings["progress"].format(
            title=utils.escape_html(job["title"][:200]),
            mode=utils.escape_html(job["label"]),
            body=utils.escape_html("\n".join((dl, ff, up, line))),
        )

    async def _render_loop(self, job):
        interval = int(self.config["PROGRESS_INTERVAL"])
        while not job["done"].is_set():
            try:
                await asyncio.wait_for(job["done"].wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass
            if job["done"].is_set():
                break
            text = self._render(job)
            if text == job["last"]:
                continue
            job["last"] = text
            try:
                await self._bot().edit_message(
                    job["mid"],
                    text,
                    buttons=job["cancel_markup"],
                    parse_mode="html",
                    link_preview=False,
                )
            except Exception:
                pass

    def _after_markup(self, job):
        buttons = []
        if job["mode"] == "photo":
            buttons.append({
                "text": self.strings["btn_raw"],
                "callback": self._cb_start,
                "args": (job["cid"], "raw", job["idx"], job["url"]),
                "always_allow": [job["user"]],
                "style": "primary",
            })
            return self.inline.generate_markup([buttons]) if buttons else None
        if job["mode"] == "raw" and job.get("entry") and not job["entry"].get("formats"):
            buttons.append({
                "text": self.strings["btn_photo"],
                "callback": self._cb_start,
                "args": (job["cid"], "photo", job["idx"], job["url"]),
                "always_allow": [job["user"]],
                "style": "primary",
            })
            return self.inline.generate_markup([buttons]) if buttons else None
        for mode, key in (("v0", "btn_video"), ("raw", "btn_raw"), ("audio", "btn_audio")):
            same = job["mode"] == mode or (mode == "v0" and job["mode"].startswith("v"))
            if same or job["mode"] in ("cv", "ca"):
                continue
            buttons.append({
                "text": self.strings[key],
                "callback": self._cb_start,
                "args": (job["cid"], mode, job["idx"], job["url"]),
                "always_allow": [job["user"]],
                "style": "primary",
            })
        return self.inline.generate_markup([buttons]) if buttons else None

    def _retry_markup(self, job):
        return self.inline.generate_markup([[{
            "text": self.strings["btn_retry"],
            "callback": self._cb_start,
            "args": (job["cid"], job["mode"], job["idx"], job["url"]),
            "always_allow": [job["user"]],
            "style": "primary",
        }]])

    async def _run_job(self, job):
        render = None
        final = None
        try:
            try:
                await self._edit(job["mid"], self._render(job), buttons=job["cancel_markup"], retries=1)
            except Exception:
                pass
            render = asyncio.ensure_future(self._render_loop(job))
            async with self._semaphore():
                final = await self._process(job)
        except asyncio.CancelledError:
            job["cancel"] = True
            final = (
                self.strings["cancelled"].format(title=utils.escape_html(job["title"][:200])),
                None,
                self._retry_markup(job),
            )
        except Exception as e:
            logger.exception("[InlineDL] job failed")
            err = re.sub(r"^ERROR:\s*", "", str(e)).strip() or type(e).__name__
            final = (
                self.strings["error"].format(
                    title=utils.escape_html(job["title"][:200]),
                    error=utils.escape_html(err[:700]),
                ),
                None,
                self._retry_markup(job),
            )
        finally:
            job["done"].set()
            if render:
                await asyncio.gather(render, return_exceptions=True)
            try:
                if final:
                    text, media, buttons = final
                    try:
                        await self._edit(job["mid"], text, file=media, buttons=buttons)
                    except Exception as e:
                        if media is None:
                            raise
                        logger.exception("[InlineDL] media edit failed")
                        await self._edit(
                            job["mid"],
                            self.strings["error"].format(
                                title=utils.escape_html(job["title"][:200]),
                                error=utils.escape_html(str(e)[:700]),
                            ),
                            buttons=self._retry_markup(job),
                        )
            except Exception:
                logger.exception("[InlineDL] final edit failed")
            shutil.rmtree(job["dir"], ignore_errors=True)
            self._tasks.pop(job["key"], None)

    async def _resolve_entry(self, job):
        if job["mode"] in ("cv", "ca"):
            return None
        if job["entry"] is not None:
            return job["entry"]
        job["st"]["stage"] = "extract"
        info = await self._extract(job["url"])
        entries = self._entries(info)
        if not entries:
            raise RuntimeError(self.strings["no_video"])
        item = entries[job["idx"]] if job["idx"] < len(entries) else entries[0]
        job["entry"] = item
        job["title"] = item.get("title") or job["title"]
        return item

    def _hooks(self, job):
        s = job["st"]

        def progress(d):
            if job["cancel"]:
                raise yt_dlp.utils.DownloadCancelled()
            status = d.get("status")
            if status == "downloading":
                cur = d.get("downloaded_bytes") or 0
                total = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
                s["stage"] = "download"
                s["dl_cur"] = s["dl_base"] + cur
                s["dl_total"] = max(s["dl_est"], s["dl_base"] + total)
                s["dl_speed"] = d.get("speed") or 0
            elif status == "finished":
                s["dl_base"] += d.get("total_bytes") or d.get("downloaded_bytes") or 0
                s["dl_cur"] = s["dl_base"]

        def postprocess(d):
            if job["cancel"]:
                raise yt_dlp.utils.DownloadCancelled()
            name = d.get("postprocessor") or ""
            if d.get("status") != "started":
                return
            if name == "Merger":
                s["stage"] = "merge"
                s["ff"] = "merge"
            elif name == "ExtractAudio":
                s["stage"] = "ffmpeg"
                s["ff"] = "mp3"

        return progress, postprocess

    def _download_sync(self, job, entry):
        progress, postprocess = self._hooks(job)
        opts = self._ydl_opts(job["mode"], job["dir"])
        opts["progress_hooks"] = [progress]
        opts["postprocessor_hooks"] = [postprocess]
        with yt_dlp.YoutubeDL(opts) as y:
            result = y.process_ie_result(self._fresh(entry), download=True)
        for d in result.get("requested_downloads") or []:
            path = d.get("filepath")
            if path and os.path.isfile(path):
                return path
        files = [
            os.path.join(job["dir"], f)
            for f in os.listdir(job["dir"])
            if not f.endswith((".part", ".ytdl", ".jpg", ".webp", ".png"))
        ]
        if not files:
            raise RuntimeError(self.strings["no_video"])
        return max(files, key=os.path.getsize)

    async def _ytdl_download(self, job, entry):
        loop = asyncio.get_running_loop()
        selected = await loop.run_in_executor(None, self._select_sync, entry, [job["mode"]])
        job["st"]["dl_est"] = _summary(selected.get(job["mode"]) or [])["size"]
        self._check_size(job["st"]["dl_est"] / 1.1)
        job["st"]["stage"] = "download"
        try:
            return await loop.run_in_executor(None, self._download_sync, job, entry)
        except yt_dlp.utils.DownloadCancelled:
            raise asyncio.CancelledError()
        except yt_dlp.utils.DownloadError:
            if job["cancel"]:
                raise asyncio.CancelledError()
            job["st"].update(dl_cur=0, dl_total=0, dl_base=0, dl_speed=0, ff="wait")
            job["st"]["stage"] = "extract"
            info = await self._extract(job["url"])
            entries = self._entries(info)
            if not entries:
                raise
            entry = entries[job["idx"]] if job["idx"] < len(entries) else entries[0]
            job["entry"] = entry
            shutil.rmtree(job["dir"], ignore_errors=True)
            os.makedirs(job["dir"], exist_ok=True)
            try:
                return await loop.run_in_executor(None, self._download_sync, job, entry)
            except yt_dlp.utils.DownloadCancelled:
                raise asyncio.CancelledError()

    async def _cobalt_download(self, job):
        api = (self.config["COBALT_API"] or "").strip().rstrip("/")
        if not api:
            raise RuntimeError(self.strings["cobalt_fail"].format(code="not configured"))
        audio = job["mode"] == "ca"
        body = {
            "url": job["url"],
            "videoQuality": "1080",
            "downloadMode": "audio" if audio else "auto",
            "youtubeVideoCodec": "h264",
            "audioFormat": "mp3",
            "audioBitrate": str(self.config["MP3_BITRATE"]),
            "filenameStyle": "basic",
        }
        headers = {"Accept": "application/json", "Content-Type": "application/json", "User-Agent": UA}
        key = (self.config["COBALT_KEY"] or "").strip()
        if key:
            headers["Authorization"] = f"Api-Key {key}"
        job["st"]["stage"] = "extract"
        timeout = aiohttp.ClientTimeout(total=None, sock_connect=20, sock_read=60)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(f"{api}/", json=body, headers=headers) as r:
                data = await r.json(content_type=None)
            status = data.get("status")
            if status == "error":
                code = (data.get("error") or {}).get("code") or "error"
                raise RuntimeError(self.strings["cobalt_fail"].format(code=code))
            if status in ("tunnel", "redirect"):
                link, filename = data.get("url"), data.get("filename")
            elif status == "picker":
                items = [i for i in data.get("picker") or [] if i.get("type") in ("video", "gif")]
                if audio and data.get("audio"):
                    link, filename = data["audio"], data.get("audioFilename")
                elif items:
                    link, filename = items[0].get("url"), None
                else:
                    raise RuntimeError(self.strings["no_video"])
            else:
                raise RuntimeError(self.strings["cobalt_fail"].format(code=status or "unknown"))
            if not link:
                raise RuntimeError(self.strings["cobalt_fail"].format(code="no url"))
            if filename:
                job["title"] = os.path.splitext(filename)[0]
            ext = os.path.splitext(filename or "")[1] or (".mp3" if audio else ".mp4")
            path = os.path.join(job["dir"], f"media{ext}")
            s = job["st"]
            s["stage"] = "download"
            async with session.get(link, headers={"User-Agent": UA}) as r:
                if r.status != 200:
                    raise RuntimeError(self.strings["cobalt_fail"].format(code=f"HTTP {r.status}"))
                s["dl_total"] = int(
                    r.headers.get("Content-Length") or r.headers.get("Estimated-Content-Length") or 0
                )
                t0 = time.time()
                with open(path, "wb") as f:
                    async for chunk in r.content.iter_chunked(256 * 1024):
                        if job["cancel"]:
                            raise asyncio.CancelledError()
                        f.write(chunk)
                        s["dl_cur"] += len(chunk)
                        s["dl_speed"] = s["dl_cur"] / max(time.time() - t0, 0.001)
        if not os.path.getsize(path):
            raise RuntimeError(self.strings["no_video"])
        return path

    async def _run_ff(self, args, duration=0, job=None):
        proc = await asyncio.create_subprocess_exec(
            self._ffmpeg_bin(), "-hide_banner", "-nostdin", "-y", "-loglevel", "error", *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        err_task = asyncio.ensure_future(proc.stderr.read())
        try:
            while True:
                line = await proc.stdout.readline()
                if not line:
                    break
                if job is None or not duration:
                    continue
                key, _, value = line.decode(errors="ignore").strip().partition("=")
                if key in ("out_time_us", "out_time_ms") and value.isdigit():
                    job["st"]["ff_pct"] = min(int(value) / 1e6 / duration * 100, 100.0)
            await proc.wait()
        except asyncio.CancelledError:
            try:
                proc.kill()
            except ProcessLookupError:
                pass
            err_task.cancel()
            raise
        err = (await err_task).decode(errors="ignore").strip()
        if proc.returncode != 0:
            raise RuntimeError(f"ffmpeg: {err[-500:] or proc.returncode}")
        return err

    async def _probe(self, path):
        proc = await asyncio.create_subprocess_exec(
            self._ffmpeg_bin(), "-hide_banner", "-nostdin", "-i", path,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        _, err = await proc.communicate()
        text = err.decode(errors="ignore")
        info = {"vcodec": None, "pix_fmt": None, "width": 0, "height": 0, "duration": 0.0, "acodec": None}
        m = re.search(r"Duration:\s*(\d+):(\d+):(\d+(?:\.\d+)?)", text)
        if m:
            info["duration"] = int(m.group(1)) * 3600 + int(m.group(2)) * 60 + float(m.group(3))
        video = re.search(r"Stream #\d+:\d+[^\n]*?: Video: (\w+)([^\n]*)", text)
        if video:
            info["vcodec"] = video.group(1)
            rest = video.group(2)
            pix = re.search(r"\),\s*(\w+)|^[^,]*,\s*(\w+)", rest)
            if pix:
                info["pix_fmt"] = pix.group(1) or pix.group(2)
            dims = re.search(r",\s*(\d{2,5})x(\d{2,5})", rest)
            if dims:
                info["width"], info["height"] = int(dims.group(1)), int(dims.group(2))
            tail = text[video.end():]
            next_stream = tail.find("Stream #")
            block = tail if next_stream < 0 else tail[:next_stream]
            rot = re.search(r"rotation of (-?\d+(?:\.\d+)?) degrees|rotate\s*:\s*(-?\d+)", block)
            if rot:
                angle = abs(int(float(rot.group(1) or rot.group(2)))) % 180
                if angle == 90:
                    info["width"], info["height"] = info["height"], info["width"]
        audio = re.search(r"Stream #\d+:\d+[^\n]*?: Audio: (\w+)", text)
        if audio:
            info["acodec"] = audio.group(1)
        return info

    async def _normalize(self, job, src):
        s = job["st"]
        probe = await self._probe(src)
        if not probe["vcodec"]:
            raise RuntimeError(self.strings["no_video"])
        copy_video = probe["vcodec"] == "h264" and probe["pix_fmt"] in ("yuv420p", "yuvj420p")
        copy_audio = probe["acodec"] in (None, "aac")
        out = os.path.join(job["dir"], "out.mp4")
        args = ["-progress", "pipe:1", "-nostats", "-i", src, "-map", "0:v:0", "-map", "0:a:0?", "-sn", "-dn"]
        if copy_video:
            args += ["-c:v", "copy"]
        else:
            args += [
                "-c:v", "libx264", "-preset", "veryfast", "-crf", "21",
                "-profile:v", "high", "-pix_fmt", "yuv420p",
                "-vf", "scale=trunc(iw/2)*2:trunc(ih/2)*2",
            ]
        args += ["-c:a", "copy"] if copy_audio else ["-c:a", "aac", "-b:a", "192k", "-ac", "2"]
        args += ["-movflags", "+faststart", "-f", "mp4", out]
        s["stage"] = "ffmpeg"
        s["ff"] = "run"
        s["ff_kind"] = self.strings["ff_copy"] if copy_video else self.strings["ff_x264"]
        s["ff_pct"] = 0.0
        await self._run_ff(args, probe["duration"], job)
        s["ff"] = "done"
        s["ff_size"] = os.path.getsize(out)
        return out

    async def _make_thumb(self, src, dst, at=None):
        args = []
        if at:
            args += ["-ss", f"{at:.2f}"]
        args += [
            "-i", src, "-frames:v", "1",
            "-vf", "scale='if(gt(iw,ih),320,-2)':'if(gt(iw,ih),-2,320)'",
            "-q:v", "3", dst,
        ]
        try:
            await self._run_ff(args)
        except asyncio.CancelledError:
            raise
        except Exception:
            return None
        return dst if os.path.isfile(dst) and os.path.getsize(dst) else None

    async def _cover(self, job, entry):
        url = _pick_thumb(entry or {}) or (entry or {}).get("thumbnail")
        if not url:
            return None
        data = await download_image(url)
        if not data:
            return None
        src = os.path.join(job["dir"], "cover.src")
        with open(src, "wb") as f:
            f.write(data)
        return await self._make_thumb(src, os.path.join(job["dir"], "thumb.jpg"))

    def _upload_cb(self, job):
        s = job["st"]

        def callback(current, total):
            if not s["up_t0"]:
                s["up_t0"] = time.time()
            s["up_cur"] = current
            s["up_total"] = total

        return callback

    def _check_size(self, size):
        if size <= int(self.config["DIRECT_LIMIT_MB"]) * MB:
            return None
        if size > USER_LIMIT:
            raise RuntimeError(self.strings["too_large"].format(size=size / MB))
        if size > BOT_LIMIT and not self._premium():
            raise RuntimeError(self.strings["need_premium"].format(size=size / MB))
        channel = self._asset_channel()
        if not channel:
            raise RuntimeError(self.strings["no_channel"].format(limit=self.config["DIRECT_LIMIT_MB"]))
        return channel

    async def _deliver(self, job, path, attributes, thumb, mime, force_document, caption):
        size = os.path.getsize(path)
        s = job["st"]
        s["stage"] = "upload"
        s["up_total"] = size
        s["up_t0"] = time.time()
        bot = self._bot()
        callback = self._upload_cb(job)

        if size <= int(self.config["DIRECT_LIMIT_MB"]) * MB:
            uploaded = await bot.upload_file(path, part_size_kb=512, progress_callback=callback)
            thumb_file = await bot.upload_file(thumb) if thumb else None
            media = InputMediaUploadedDocument(
                file=uploaded,
                mime_type=mime,
                attributes=attributes,
                thumb=thumb_file,
                force_file=force_document or None,
            )
            peers = [InputPeerSelf()]
            try:
                peers.append(await bot.get_input_entity(job["user"]))
            except Exception:
                pass
            channel = self._asset_channel()
            if channel:
                peers.append(channel)
            last = None
            for peer in peers:
                try:
                    result = await bot(functions.messages.UploadMediaRequest(peer=peer, media=media))
                    s["up_done"] = True
                    return tl_utils.get_input_media(result)
                except Exception as e:
                    last = e
            raise last or RuntimeError(self.strings["no_media"])

        channel = self._check_size(size)

        topic_id = await self._ensure_topic(fresh=True)
        kwargs = dict(
            caption=caption,
            parse_mode="html",
            attributes=attributes,
            thumb=thumb,
            force_document=force_document,
            supports_streaming=not force_document,
            mime_type=mime,
            reply_to=topic_id,
            part_size_kb=512,
            progress_callback=callback,
        )
        if size <= BOT_LIMIT:
            msg = await bot.send_file(channel, path, **kwargs)
        else:
            sent = await self._client.send_file(channel, path, **kwargs)
            msg = await bot.get_messages(channel, ids=sent.id)
        if not msg or not getattr(msg, "media", None):
            raise RuntimeError(self.strings["no_media"])
        s["up_done"] = True
        return tl_utils.get_input_media(msg.media)

    async def _process(self, job):
        os.makedirs(job["dir"], exist_ok=True)
        mode = job["mode"]
        s = job["st"]
        entry = await self._resolve_entry(job)
        title = job["title"]
        base = _sanitize_fn(title)
        is_photo = mode == "photo" or (entry and not entry.get("formats") and mode not in ("audio", "ca"))
        if is_photo:
            s["stage"] = "download"
            img_url = _pick_thumb(entry or {}) or (entry or {}).get("thumbnail") or job["url"]
            data = await download_image(img_url)
            if not data:
                raise RuntimeError(self.strings["no_video"])
            path = os.path.join(job["dir"], f"{base}.jpg")
            with open(path, "wb") as f:
                f.write(data)
            s["dl_done"] = True
            s["dl_cur"] = len(data)
            s["dl_total"] = len(data)
            s["ff"] = "skip"
            attributes = [DocumentAttributeFilename(f"{base}.jpg")]
            mime = "image/jpeg"
            force_document = (mode == "raw")
            thumb = None
            info = [f"{len(data) / MB:.1f} MB", "JPG"]
            caption = self.strings["caption"].format(
                url=utils.escape_html(job["url"]),
                title=utils.escape_html(title[:200]),
                info=" | ".join(info + [_fmt_elapsed(time.time() - job["t0"])]),
            )
            media = await self._deliver(job, path, attributes, thumb, mime, force_document, caption)
            s["stage"] = "finish"
            return caption, media, self._after_markup(job)

        if entry is None:
            path = await self._cobalt_download(job)
        else:
            path = await self._ytdl_download(job, entry)
        s["dl_done"] = True
        s["dl_cur"] = max(s["dl_cur"], os.path.getsize(path))
        base = _sanitize_fn(title)
        is_audio = mode in ("audio", "ca") or os.path.splitext(path)[1].lower() in (".mp3", ".m4a", ".opus", ".ogg")
        thumb = None

        if is_audio:
            s["ff"] = "done"
            s["ff_size"] = os.path.getsize(path)
            probe = await self._probe(path)
            ext = os.path.splitext(path)[1].lower() or ".mp3"
            performer = (entry or {}).get("artist") or (entry or {}).get("uploader") or ""
            attributes = [
                DocumentAttributeAudio(
                    duration=int(probe["duration"]),
                    title=title[:200],
                    performer=performer[:200] or None,
                ),
                DocumentAttributeFilename(f"{base}{ext}"),
            ]
            mime = mimetypes.guess_type(f"x{ext}")[0] or "audio/mpeg"
            force_document = False
            thumb = await self._cover(job, entry)
            info = [f"{os.path.getsize(path) / MB:.1f} MB", _fmt_dur(probe["duration"])]
        elif mode == "raw":
            s["ff"] = "skip"
            probe = await self._probe(path)
            ext = os.path.splitext(path)[1].lower() or ".mkv"
            attributes = [DocumentAttributeFilename(f"{base}{ext}")]
            mime = mimetypes.guess_type(f"x{ext}")[0] or (
                "video/x-matroska" if ext == ".mkv" else "application/octet-stream"
            )
            force_document = True
            if probe["vcodec"]:
                thumb = await self._make_thumb(
                    path, os.path.join(job["dir"], "thumb.jpg"), min(1.0, probe["duration"] / 2)
                )
            codecs = " + ".join(c for c in (_codec(probe["vcodec"]), _codec(probe["acodec"])) if c)
            info = [f"{os.path.getsize(path) / MB:.1f} MB"]
            if probe["width"]:
                info.append(f"{probe['width']}x{probe['height']}")
            if codecs:
                info.append(codecs)
            info.append(_fmt_dur(probe["duration"]))
        else:
            path = await self._normalize(job, path)
            probe = await self._probe(path)
            attributes = [
                DocumentAttributeVideo(
                    duration=probe["duration"],
                    w=probe["width"],
                    h=probe["height"],
                    supports_streaming=True,
                ),
                DocumentAttributeFilename(f"{base}.mp4"),
            ]
            mime = "video/mp4"
            force_document = False
            thumb = await self._make_thumb(
                path, os.path.join(job["dir"], "thumb.jpg"), min(1.0, probe["duration"] / 2)
            )
            info = [
                f"{os.path.getsize(path) / MB:.1f} MB",
                f"{probe['width']}x{probe['height']}",
                _fmt_dur(probe["duration"]),
            ]

        caption = self.strings["caption"].format(
            url=utils.escape_html(job["url"]),
            title=utils.escape_html(title[:200]),
            info=" | ".join(info + [_fmt_elapsed(time.time() - job["t0"])]),
        )
        media = await self._deliver(job, path, attributes, thumb, mime, force_document, caption)
        s["stage"] = "finish"
        caption = self.strings["caption"].format(
            url=utils.escape_html(job["url"]),
            title=utils.escape_html(title[:200]),
            info=" | ".join(info + [_fmt_elapsed(time.time() - job["t0"])]),
        )
        return caption, media, self._after_markup(job)

    @loader.command(
        ru_doc="Обновить yt-dlp до последней версии",
        en_doc="Update yt-dlp to the latest version",
    )
    async def dlupd(self, message):
        """Update yt-dlp to the latest version"""
        message = await utils.answer(message, self.strings["upd_running"])
        old = yt_dlp.version.__version__
        proc = await asyncio.create_subprocess_exec(
            sys.executable, "-m", "pip", "install", "-U", "-q", "yt-dlp",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        _, err = await proc.communicate()
        if proc.returncode != 0:
            await utils.answer(
                message,
                self.strings["upd_fail"].format(error=utils.escape_html(err.decode(errors="ignore")[-800:])),
            )
            return
        check = await asyncio.create_subprocess_exec(
            sys.executable, "-c", "import yt_dlp.version as v; print(v.__version__)",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        out, _ = await check.communicate()
        new = out.decode().strip() or old
        if new == old:
            await utils.answer(message, self.strings["upd_same"].format(ver=old))
        else:
            await utils.answer(message, self.strings["upd_done"].format(old=old, new=new))