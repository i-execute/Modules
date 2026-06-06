__version__ = (4, 1, 0)
# meta developer: I_execute.t.me
# meta banner: https://raw.githubusercontent.com/i-execute/Modules/main/Storage/Grabber/MetaBanner.jpeg

import os
import io
import re
import time
import json
import asyncio
import logging
import shutil
import tempfile
import sys
import concurrent.futures
import threading

from telethon import TelegramClient, events, Button
from telethon.tl import functions, types
from telethon.tl.types import (
    DocumentAttributeVideo,
    DocumentAttributeAudio,
    DocumentAttributeFilename,
    ReactionEmoji,
)
from telethon.tl.functions.messages import SendReactionRequest
from telethon.errors import (
    MessageNotModifiedError,
    FloodWaitError,
)
from .. import loader, utils

try:
    import TgCrypto
except ImportError:
    pass

try:
    from PIL import Image
except ImportError:
    Image = None

try:
    import aiohttp
except ImportError:
    aiohttp = None

logger = logging.getLogger(__name__)

_URL_PATTERN = re.compile(r'https?://[^\s<>"\']+', re.IGNORECASE)
_BOT_TOKEN_RE = re.compile(r'\b\d{8,10}:[A-Za-z0-9_-]{35}\b')

OG_IMAGE_RE = re.compile(
    r'<meta\s+(?:property|name)=["\']og:image["\']\s+content=["\']([^"\']+)["\']',
    re.IGNORECASE,
)
OG_IMAGE_RE2 = re.compile(
    r'<meta\s+content=["\']([^"\']+)["\']\s+(?:property|name)=["\']og:image["\']',
    re.IGNORECASE,
)

GRABBER_TOPIC_ICON = 5269364267290765511


def _escape_html(t):
    if not t:
        return ""
    return str(t).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _sanitize_fn(n):
    return re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", n).strip(". ")[:180] or "track"


def _fmt_dur(s):
    if not s or s <= 0:
        return "0:00"
    s = int(s)
    if s < 3600:
        m, sec = divmod(s, 60)
        return f"{m}:{sec:02d}"
    h, rem = divmod(s, 3600)
    m, sec = divmod(rem, 60)
    return f"{h}:{m:02d}:{sec:02d}"


def _fmt_speed(b):
    if not b:
        return "..."
    if b < 1024:
        return f"{b:.0f} B/s"
    if b < 1024 * 1024:
        return f"{b / 1024:.1f} KB/s"
    return f"{b / 1024 / 1024:.1f} MB/s"


def _fmt_elapsed(s):
    if not s or s <= 0:
        return "0s"
    s = int(s)
    if s < 60:
        return f"{s}s"
    if s < 3600:
        m, sec = divmod(s, 60)
        return f"{m}m {sec}s" if sec else f"{m}m"
    h, rem = divmod(s, 3600)
    m, sec = divmod(rem, 60)
    parts = [f"{h}h"]
    if m:
        parts.append(f"{m}m")
    if sec:
        parts.append(f"{sec}s")
    return " ".join(parts)



def normalize_cover(raw, max_size=None, force_jpeg=False):
    if not raw or len(raw) < 100:
        return None
    if not Image:
        return raw
    try:
        img = Image.open(io.BytesIO(raw))
        w, h = img.size
        needs_resize = max_size and (w > max_size or h > max_size)
        if force_jpeg:
            img = img.convert("RGB")
            if needs_resize:
                ratio = min(max_size / w, max_size / h)
                img = img.resize((int(w * ratio), int(h * ratio)), Image.LANCZOS)
            buf = io.BytesIO()
            img.save(buf, format="JPEG", quality=95)
            result = buf.getvalue()
            return result if len(result) >= 100 else None
        is_png = raw[:8] == b'\x89PNG\r\n\x1a\n'
        if is_png and not needs_resize:
            return raw
        img = img.convert("RGB")
        if needs_resize:
            ratio = min(max_size / w, max_size / h)
            img = img.resize((int(w * ratio), int(h * ratio)), Image.LANCZOS)
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=95)
        result = buf.getvalue()
        return result if len(result) >= 100 else None
    except Exception:
        return raw


async def _download_url(url, timeout=20):
    if not url or not aiohttp:
        return None
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as r:
                if r.status != 200:
                    return None
                data = await r.read()
                return data if data and len(data) >= 100 else None
    except Exception:
        return None


async def _upload_to_x0(data, filename, content_type="application/octet-stream"):
    if not aiohttp:
        return ""
    try:
        form = aiohttp.FormData()
        form.add_field("file", data, filename=filename, content_type=content_type)
        async with aiohttp.ClientSession() as s:
            async with s.post(
                "https://x0.at",
                data=form,
                timeout=aiohttp.ClientTimeout(total=60),
            ) as r:
                text = (await r.text()).strip()
                return text if text.startswith("http") else ""
    except Exception:
        return ""


async def _fetch_og_image(url):
    if not aiohttp:
        return None
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                url,
                headers={"User-Agent": "facebookexternalhit/1.1"},
                timeout=aiohttp.ClientTimeout(total=15),
                allow_redirects=True,
            ) as r:
                if r.status != 200:
                    return None
                html = await r.text(errors="replace")
        for pat in [OG_IMAGE_RE, OG_IMAGE_RE2]:
            m = pat.search(html)
            if m:
                return m.group(1).replace("&amp;", "&")
    except Exception:
        pass
    return None


def _get_best_thumb_url(info):
    if not info:
        return None
    thumbs = info.get("thumbnails")
    if thumbs and isinstance(thumbs, list):
        best_url = None
        best_pref = -9999
        best_res = 0
        for t in thumbs:
            url = t.get("url")
            if not url:
                continue
            w = t.get("width") or 0
            h = t.get("height") or 0
            pref = t.get("preference") or 0
            res = w * h
            if pref > best_pref or (pref == best_pref and res > best_res):
                best_pref = pref
                best_res = res
                best_url = url
        if best_url:
            return best_url
    return info.get("thumbnail")


class SafeList:
    def __init__(self):
        self._list = []
        self._lock = threading.Lock()

    def append(self, item):
        with self._lock:
            self._list.append(item)

    def pop_first(self):
        with self._lock:
            return self._list.pop(0) if self._list else None

    def clear(self):
        with self._lock:
            self._list.clear()

    def __len__(self):
        with self._lock:
            return len(self._list)


class SafeSet:
    def __init__(self):
        self._set = set()
        self._lock = threading.Lock()

    def add(self, item):
        with self._lock:
            if item in self._set:
                return False
            self._set.add(item)
            return True

    def remove(self, item):
        with self._lock:
            self._set.discard(item)

    def clear(self):
        with self._lock:
            self._set.clear()


async def _embed_cover_ffmpeg(audio_path, cover_path, output_path):
    cmd = [
        "ffmpeg", "-y",
        "-i", audio_path,
        "-i", cover_path,
        "-map", "0:a",
        "-map", "1:0",
        "-c", "copy",
        "-id3v2_version", "3",
        "-metadata:s:v", "title=Album cover",
        "-metadata:s:v", "comment=Cover (front)",
        output_path,
    ]
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        await proc.communicate()
        return proc.returncode == 0
    except Exception:
        return False


DEPS = ["yt-dlp", "Pillow", "aiohttp", "mutagen", "TgCrypto-pyrofork"]


def _install_deps():
    import importlib, subprocess
    pip = __import__('os').path.join(__import__('os').path.dirname(sys.executable), "pip")
    if not __import__('os').path.exists(pip):
        pip = "pip"
    in_venv = sys.prefix != sys.base_prefix
    imp_map = {
        "yt-dlp": "yt_dlp", "Pillow": "PIL",
        "aiohttp": "aiohttp", "mutagen": "mutagen", "TgCrypto-pyrofork": "TgCrypto",
    }
    ver_attr = {
        "yt_dlp": "version.__version__",
        "mutagen": "version.version_string",
    }
    lines = [f"venv: {'yes' if in_venv else 'no'} ({sys.prefix})"]
    for pkg in DEPS:
        try:
            subprocess.run([pip, "install", "-U", pkg, "--break-system-packages", "-q"],
                capture_output=True, text=True, timeout=120)
            try:
                imp_name = imp_map.get(pkg, pkg)
                importlib.invalidate_caches()
                mod = importlib.import_module(imp_name)
                if imp_name in ver_attr:
                    parts = ver_attr[imp_name].split(".")
                    obj = mod
                    for part in parts:
                        obj = getattr(obj, part, None)
                        if obj is None:
                            break
                    ver = str(obj) if obj else getattr(mod, "__version__", "?")
                else:
                    ver = getattr(mod, "__version__", "?")
                lines.append(f"{pkg}: OK ({ver})")
            except ImportError:
                lines.append(f"{pkg}: FAIL (import error)")
        except Exception as e:
            lines.append(f"{pkg}: FAIL ({e})")
    return lines



@loader.tds
class Grabber(loader.Module):
    """Universal media downloader"""

    strings = {
        "name": "Grabber",
        "btn_video": "Video",
        "btn_audio": "Audio (MP3)",
        "btn_cancel": "Cancel",
        "btn_back": "Back",
        "btn_orig_thumb": "With cover",
        "btn_orig_clean": "Without cover",
        "btn_custom": "Editor",
        "btn_no_cover": "No cover",
        "btn_confirm": "Download",
        "btn_retry": "Retry",
        "no_token": (
            "<b>Bot token not set!</b>\n"
            "Use: <code>{prefix}grab token [token]</code>"
        ),
        "token_stored": (
            "<b>Token saved!</b>\n"
            "<blockquote expandable>Startup log:\n{log}</blockquote>"
        ),
        "token_started": (
            "<b>Grabber started!</b>\n"
            "<blockquote expandable>Startup log:\n{log}</blockquote>"
        ),
        "need_token": "<b>Specify token or reply to message with it!</b>",
        "already_running": "<b>Already running!</b>",
        "not_running": "<b>Bot not running!</b>",
        "start_failed": "<b>Start error:</b>\n<code>{error}</code>\nUse <code>{prefix}unextarnal Grabber</code> to reload.",
        "reboot_start": "<b>REBOOT...</b>",
        "reboot_done": "<b>Reboot complete!</b>",
        "reboot_no_token": "<b>No token for restart!</b>",
        "clear_done": "<b>Factory reset complete!</b>",
        "cookie_saved": "<b>Cookies saved!</b>",
        "cookie_cleared": "<b>Cookies deleted!</b>",
        "cookie_empty": "<b>Cookies not set.</b>",
        "cookie_ok": "<b>Cookies installed</b>",
        "no_reply_file": "<b>Reply to .txt file with cookies!</b>",
        "invalid_ext": "<b>File must have .txt extension!</b>",
        "cookie_err": "<b>Cookie save error:</b>\n<code>{}</code>",
        "cookie_invalid_format": "<b>Invalid cookie format! Expected Netscape format.</b>",
        "status_template": (
            "<b>Grabber Status</b>\n\n"
            "Status: {status}\n"
            "Cookies: {cookies}\n"
            "In queue: {pending}\n"
            "Active tasks: {active}\n"
            "Completed: {completed}\n"
            "Errors: {errors}"
        ),
        "status_running": "Running",
        "status_stopped": "Stopped",
        "no_active_process": "<b>No active process</b>",
        "analyzing": "<b>Analyzing...</b>",
        "found_media": "<b>{title}</b>\n\nDuration: {duration}\nSelect format:",
        "found_media_group": "<b>{title}</b>\n\nDuration: {duration}",
        "grab_failed": "<b>ATTENTION:</b>\n<code>{error}</code>",
        "queue_pos": "<b>Queue: #{pos}</b>",
        "starting": "<b>Starting download!</b>",
        "too_large_bot": "<b>File too large for bot API ({size_mb:.1f} MB). Uploading via userbot...</b>",
        "too_large_no_premium": "<b>File too large ({size_mb:.1f} MB). Telegram Premium required on userbot account.</b>",
        "too_large_over_limit": "<b>File too large ({size_mb:.1f} MB). Max is 4096 MB.</b>",
        "cancelled": "<b>Cancelled</b>",
        "already_processing": "Already processing!",
        "hello": "<b>Hello, {user_link}!</b>\n\nSend a video link!",
        "hello_fallback": "<b>Hello!</b>\n\nSend a video link!",
        "hello_group": "<b>Bot activated in this group!</b>\n\nSend video links.",
        "hello_topic": "<b>Bot activated in this topic!</b>\n\nSend video links.",
        "hello_group_off": "<b>Bot deactivated in this group.</b>",
        "hello_topic_off": "<b>Bot deactivated in this topic.</b>",
        "file_caption": (
            "<blockquote><b>{title}</b></blockquote>\n"
            "<pre><code class=\"language-grabber\">{size_mb:.1f} MB | {width}x{height} | {elapsed}</code></pre>"
        ),
        "audio_caption": (
            "<blockquote><b>{title}</b></blockquote>\n"
            "<pre><code class=\"language-grabber\">{size_mb:.1f} MB</code></pre>"
        ),
        "progress_title": "<b>{title}</b>",
        "dl_waiting": "Downloading: waiting...",
        "dl_progress": "Downloading: {pct:.1f}% ({size:.1f}/{total:.1f} MB)",
        "dl_done": "Downloading: done {size:.1f} MB",
        "merge_waiting": "Merge: waiting...",
        "merge_done": "Merge: done {size:.1f} MB",
        "merge_active": "Merge: FFmpeg working...",
        "convert_waiting": "Convert: waiting...",
        "convert_done": "Convert: done {size:.1f} MB",
        "convert_active": "Convert: FFmpeg...",
        "upload_waiting": "Telegram: waiting...",
        "upload_progress": "Telegram: {pct:.1f}% ({cur:.1f}/{total:.1f} MB)",
        "upload_success": "Telegram: done {elapsed:.1f}s",
        "stage_init": "Initializing...",
        "stage_video": "Downloading video...",
        "stage_audio": "Downloading audio...",
        "stage_ffmpeg": "FFmpeg processing...",
        "stage_probe": "Analyzing file...",
        "stage_upload": "Uploading to Telegram...",
        "stage_done_success": "Successfully uploaded!",
        "time_stage": "Time: {elapsed:.1f}s | {stage_text}",
        "audio_menu": "<b>Select option:</b>",
        "editor_mode": "<b>Editor mode!</b>\n\nSend TRACK NAME:",
        "edit_title_done": "Title: <b>{}</b>\n\nNow enter ARTIST name:",
        "edit_artist_done": "Artist: <b>{}</b>\n\nNow send COVER (PNG/JPEG as FILE!):",
        "only_image": "<b>Only PNG or JPEG as FILE!</b>",
        "downloading_image": "<b>Downloading image...</b>",
        "text_needed_image": "<b>Need PNG/JPEG file, not text!</b>",
        "confirm_menu": (
            "<b>Check data:</b>\n\n"
            "Track: {title}\n"
            "Artist: {artist}\n"
            "Cover: {thumb}\n\n"
            "All correct?"
        ),
        "thumb_yes": "Yes",
        "thumb_no": "No",
        "op_cancelled": "<b>Operation cancelled.</b>",
        "not_your_editor": "<b>This is not your editor!</b>",
        "session_expired": "Session expired",
        "quality_menu": "<b>Select quality:</b>",
        "react_ok": "\U0001F44C",
        "react_fail": "\U0001F971",
        "usage": (
            "<b>Grabber - Universal media downloader</b>\n\n"
            "<code>{prefix}grab token [token]</code> - Set bot token\n"
            "<code>{prefix}grab reboot</code> - Clear cache and restart\n"
            "<code>{prefix}grab clear</code> - Factory reset\n"
            "<code>{prefix}grab cookies add</code> - Add cookies (reply to .txt)\n"
            "<code>{prefix}grab cookies remove</code> - Remove cookies\n"
            "<code>{prefix}grab status</code> - Status\n"
        ),
        "log_processing": "<b>Processing:</b> {title}\nFrom: {user}\nURL: <code>{url}</code>",
        "log_done": "<b>Done:</b> {title} | {size_mb:.1f} MB",
        "log_error": "<b>Error:</b> {title}\n<code>{error}</code>",
        "log_large": "<b>Large file ({size_mb:.1f} MB), uploading via userbot...</b>",
        "main_menu": (
            "<b>Grabber</b>\n\n"
            "Status: {status}\n"
            "Token: {token}\n"
            "Cookies: {cookies}\n"
            "Queue: {pending} | Active: {active}\n"
            "Done: {completed} | Errors: {errors}"
        ),
        "token_menu": "<b>Token</b>\n\nSet bot token below:",
        "cookies_menu": "<b>Cookies</b>\n\nStatus: {status}",
        "clear_confirm": "<b>Factory reset?</b>\n\nAll data will be deleted.",
        "btn_status": "Refresh",
        "btn_token": "Token",
        "btn_cookies": "Cookies",
        "btn_reboot": "Reboot",
        "btn_clear": "Reset",
        "btn_close": "Close",
        "btn_start": "Start",
        "btn_stop": "Stop",
        "btn_set_token": "Set token",
        "btn_remove_cookies": "Remove cookies",
        "btn_confirm_reboot": "Confirm",
        "btn_confirm_clear": "Confirm reset",
        "input_token": "Paste bot token:",
        "installing": "Installing...",
        "cookie_cleared_short": "Cookies removed!",
    }

    strings_ru = {
        "name": "Grabber",
        "btn_video": "Видео",
        "btn_audio": "Аудио (MP3)",
        "btn_cancel": "Отмена",
        "btn_back": "Назад",
        "btn_orig_thumb": "С обложкой",
        "btn_orig_clean": "Без обложки",
        "btn_custom": "Редактор",
        "btn_no_cover": "Без обложки",
        "btn_confirm": "Скачать",
        "btn_retry": "Заново",
        "no_token": (
            "<b>Токен бота не задан!</b>\n"
            "Используй: <code>{prefix}grab token [токен]</code>"
        ),
        "token_stored": (
            "<b>Токен сохранён!</b>\n"
            "<blockquote expandable>Лог запуска:\n{log}</blockquote>"
        ),
        "token_started": (
            "<b>Grabber запущен!</b>\n"
            "<blockquote expandable>Лог запуска:\n{log}</blockquote>"
        ),
        "need_token": "<b>Укажи токен или ответь на сообщение с ним!</b>",
        "already_running": "<b>Уже запущен!</b>",
        "not_running": "<b>Бот не запущен!</b>",
        "start_failed": "<b>Ошибка запуска:</b>\n<code>{error}</code>\nИспользуй <code>{prefix}unextarnal Grabber</code>.",
        "reboot_start": "<b>ПЕРЕЗАГРУЗКА...</b>",
        "reboot_done": "<b>Перезагрузка завершена!</b>",
        "reboot_no_token": "<b>Нет токена для перезапуска!</b>",
        "clear_done": "<b>Сброс к заводским настройкам!</b>",
        "cookie_saved": "<b>Куки сохранены!</b>",
        "cookie_cleared": "<b>Куки удалены!</b>",
        "cookie_empty": "<b>Куки не установлены.</b>",
        "cookie_ok": "<b>Куки установлены</b>",
        "no_reply_file": "<b>Ответь на .txt файл с куки!</b>",
        "invalid_ext": "<b>Файл должен иметь расширение .txt!</b>",
        "cookie_err": "<b>Ошибка сохранения куки:</b>\n<code>{}</code>",
        "cookie_invalid_format": "<b>Неверный формат куки! Ожидается формат Netscape.</b>",
        "status_template": (
            "<b>Статус Grabber</b>\n\n"
            "Статус: {status}\n"
            "Куки: {cookies}\n"
            "В очереди: {pending}\n"
            "Активно задач: {active}\n"
            "Завершено: {completed}\n"
            "Ошибки: {errors}"
        ),
        "status_running": "Работает",
        "status_stopped": "Остановлен",
        "no_active_process": "<b>Нет активного процесса</b>",
        "analyzing": "<b>Анализ...</b>",
        "found_media": "<blockquote><b>{title}</b></blockquote>\nДлительность: {duration}\nВыберите формат:",
        "found_media_group": "<blockquote><b>{title}</b></blockquote>\nДлительность: {duration}",
        "grab_failed": "<b>ATTENTION:</b>\n<code>{error}</code>",
        "queue_pos": "<b>Очередь: #{pos}</b>",
        "starting": "<b>Начинаю загрузку!</b>",
        "too_large_bot": "<b>Файл слишком большой для bot API ({size_mb:.1f} МБ). Загружаю через юзербот...</b>",
        "too_large_no_premium": "<b>Файл слишком большой ({size_mb:.1f} МБ). Нужен Telegram Premium на аккаунте юзербота.</b>",
        "too_large_over_limit": "<b>Файл слишком большой ({size_mb:.1f} МБ). Максимум 4096 МБ.</b>",
        "cancelled": "<b>Отменено</b>",
        "already_processing": "Уже обрабатывается!",
        "hello": "<b>Привет, {user_link}!</b>\n\nОтправь ссылку на видео!",
        "hello_fallback": "<b>Привет!</b>\n\nОтправь ссылку на видео!",
        "hello_group": "<b>Бот активирован в этой группе!</b>\n\nОтправляйте ссылки на видео.",
        "hello_topic": "<b>Бот активирован в этом топике!</b>\n\nОтправляйте ссылки на видео.",
        "hello_group_off": "<b>Бот деактивирован в этой группе.</b>",
        "hello_topic_off": "<b>Бот деактивирован в этом топике.</b>",
        "file_caption": (
            "<blockquote><b>{title}</b></blockquote>\n"
            "<pre><code class=\"language-grabber\">{size_mb:.1f} MB | {width}x{height} | {elapsed}</code></pre>"
        ),
        "audio_caption": (
            "<blockquote><b>{title}</b></blockquote>\n"
            "<pre><code class=\"language-grabber\">{size_mb:.1f} MB</code></pre>"
        ),
        "progress_title": "<b>{title}</b>",
        "video_waiting": "Видео: ожидание...",
        "video_done": "Видео: готово {size:.1f} MB",
        "video_progress": "Видео: {pct:.1f}% ({size:.1f}/{total:.1f} MB) | {speed}",
        "audio_waiting": "Аудио: ожидание...",
        "audio_done": "Аудио: готово {size:.1f} MB",
        "audio_progress": "Аудио: {pct:.1f}% ({size:.1f}/{total:.1f} MB) | {speed}",
        "merge_waiting": "Слияние: ожидание...",
        "merge_done": "Слияние: готово {size:.1f} MB",
        "merge_active": "Слияние: FFmpeg работает...",
        "merge_starting": "Слияние: запуск...",
        "convert_waiting": "Конвертация: ожидание...",
        "convert_done": "Конвертация: готово {size:.1f} MB",
        "convert_active": "Конвертация: FFmpeg...",
        "upload_waiting": "Telegram: ожидание...",
        "upload_progress": "Telegram: {pct:.1f}% ({cur:.1f}/{total:.1f} MB)",
        "upload_success": "Telegram: готово {elapsed:.1f}с",
        "stage_init": "Инициализация...",
        "stage_video": "Загрузка видео...",
        "stage_audio": "Загрузка аудио...",
        "stage_ffmpeg": "Обработка FFmpeg...",
        "stage_probe": "Анализ файла...",
        "stage_upload": "Загрузка в Telegram...",
        "stage_done_success": "Успешно загружено!",
        "time_stage": "Время: {elapsed:.1f}с | {stage_text}",
        "audio_menu": "<b>Выберите вариант:</b>",
        "editor_mode": "<b>Режим редактора!</b>\n\nОтправьте НАЗВАНИЕ ТРЕКА:",
        "edit_title_done": "Название: <b>{}</b>\n\nТеперь введите ИМЯ АРТИСТА:",
        "edit_artist_done": "Артист: <b>{}</b>\n\nТеперь отправьте ОБЛОЖКУ (PNG/JPEG как ФАЙЛ!):",
        "only_image": "<b>Только PNG или JPEG как ФАЙЛ!</b>",
        "downloading_image": "<b>Загрузка изображения...</b>",
        "text_needed_image": "<b>Нужен PNG/JPEG файл, не текст!</b>",
        "confirm_menu": (
            "<b>Проверьте данные:</b>\n\n"
            "Трек: {title}\n"
            "Артист: {artist}\n"
            "Обложка: {thumb}\n\n"
            "Всё верно?"
        ),
        "thumb_yes": "Да",
        "thumb_no": "Нет",
        "op_cancelled": "<b>Операция отменена.</b>",
        "not_your_editor": "<b>Это не ваш редактор!</b>",
        "session_expired": "Сессия истекла",
        "quality_menu": "<b>Выберите качество:</b>",
        "react_ok": "\U0001F44C",
        "react_fail": "\U0001F971",
        "usage": (
            "<b>Grabber - Универсальный загрузчик медиа</b>\n\n"
            "<code>{prefix}grab token [токен]</code> - Установить токен бота\n"
            "<code>{prefix}grab reboot</code> - Очистить кеш и перезапустить\n"
            "<code>{prefix}grab clear</code> - Сброс к заводским\n"
            "<code>{prefix}grab cookies add</code> - Добавить куки (реплай на .txt)\n"
            "<code>{prefix}grab cookies remove</code> - Удалить куки\n"
            "<code>{prefix}grab status</code> - Статус\n"
        ),
        "log_processing": "<b>Обработка:</b> {title}\nОт: {user}\nURL: <code>{url}</code>",
        "log_done": "<b>Готово:</b> {title} | {size_mb:.1f} MB",
        "log_error": "<b>Ошибка:</b> {title}\n<code>{error}</code>",
        "log_large": "<b>Большой файл ({size_mb:.1f} МБ), загружаю через юзербот...</b>",
        "main_menu": (
            "<b>Grabber</b>\n\n"
            "Статус: {status}\n"
            "Токен: {token}\n"
            "Куки: {cookies}\n"
            "Очередь: {pending} | Активно: {active}\n"
            "Готово: {completed} | Ошибки: {errors}"
        ),
        "token_menu": "<b>Токен</b>\n\nВведи токен бота:",
        "cookies_menu": "<b>Куки</b>\n\nСтатус: {status}",
        "clear_confirm": "<b>Сброс к заводским?</b>\n\nВсе данные будут удалены.",
        "btn_status": "Обновить",
        "btn_token": "Токен",
        "btn_cookies": "Куки",
        "btn_reboot": "Перезагрузка",
        "btn_clear": "Сброс",
        "btn_close": "Закрыть",
        "btn_start": "Старт",
        "btn_stop": "Стоп",
        "btn_set_token": "Установить токен",
        "btn_remove_cookies": "Удалить куки",
        "btn_confirm_reboot": "Подтвердить",
        "btn_confirm_clear": "Подтвердить сброс",
        "input_token": "Вставьте токен бота:",
        "installing": "Установка...",
        "cookie_cleared_short": "Куки удалены!",
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "BOT_TOKEN",
                "",
                "Bot token for Grabber",
                validator=loader.validators.Hidden(),
            ),
            loader.ConfigValue(
                "AUTORUNNER",
                False,
                "Auto-start bot on module load",
                validator=loader.validators.Boolean(),
            ),
            loader.ConfigValue(
                "MAX_FILE_MB",
                2000,
                "Max file size in MB (>2000 sent via userbot if premium)",
                validator=loader.validators.Integer(minimum=50, maximum=4096),
            ),
            loader.ConfigValue(
                "MAX_WORKERS",
                1,
                "Number of parallel download workers",
                validator=loader.validators.Integer(minimum=1, maximum=10),
            ),
            loader.ConfigValue(
                "ACTION_DELAY",
                3,
                "Progress message update interval in seconds",
                validator=loader.validators.Integer(minimum=1, maximum=30),
            ),
            loader.ConfigValue(
                "ACTIVE_GROUPS",
                [],
                "Groups where bot is active (managed automatically)",
                validator=loader.validators.Series(loader.validators.Integer()),
            ),
            loader.ConfigValue(
                "ACTIVE_TOPICS",
                {},
                "Topics where bot is active (managed automatically)",
            ),
        )

        self._bot = None
        self._running = False
        self._download_queue = asyncio.Queue()
        self._queue_items = SafeList()
        self._processed_buttons = SafeSet()
        self._worker_tasks = []
        self._stats = {"completed": 0, "errors": 0}

        self._root = None
        self._cache_dir = None
        self._session_dir = None
        self._cookie_dir = None
        self._cookie_path = None

        self._lock = asyncio.Lock()
        self._edit_lock = asyncio.Lock()
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)
        self._active_processes = []

        self._edit_state = {}
        self._active_groups = set()
        self._active_topics = {}

        self._current_downloads = {}
        self._og_cache = {}
        self._grabber_topic_id = None

    def _empty_progress(self):
        return {
            "active": False, "title": "", "start_time": 0, "is_audio_only": False,
            "dl_percent": 0.0, "dl_size": 0.0, "dl_total": 0.0, "dl_speed_bytes": 0.0, "dl_done": False,
            "ffmpeg_active": False, "ffmpeg_done": False,
            "upload_started": False, "upload_elapsed": 0.0,
            "upload_current": 0.0, "upload_total": 0.0, "upload_speed": "N/A",
            "final_size": 0.0, "stage": "init", "og_url": None,
        }

    def _get_elapsed(self, task_id):
        start = self._current_downloads.get(task_id, {}).get("start_time", 0)
        return time.time() - start if start > 0 else 0.0

    def _build_status_message(self, task_id):
        d = self._current_downloads.get(task_id, self._empty_progress())
        stage = d.get("stage", "init")
        is_audio = d.get("is_audio_only", False)
        title = _escape_html(d.get("title", "Unknown")[:80])
        elapsed = self._get_elapsed(task_id)

        inner = ["----------------"]

        if d.get("dl_done"):
            dl_display = d.get("final_size") or d.get("dl_total") or d.get("dl_size", 0)
            inner.append(self.strings["dl_done"].format(size=dl_display))
        elif d.get("dl_percent", 0) > 0 or d.get("dl_size", 0) > 0:
            pct = d.get("dl_percent", 0)
            sz = d.get("dl_size", 0)
            total = d.get("dl_total", 0)
            inner.append(self.strings["dl_progress"].format(pct=pct, size=sz, total=total if total > 0 else sz))
        else:
            inner.append(self.strings["dl_waiting"])

        if not is_audio:
            if d.get("ffmpeg_done"):
                inner.append(self.strings["merge_done"].format(size=d.get("final_size", 0)))
            elif d.get("ffmpeg_active") or stage == "ffmpeg":
                inner.append(self.strings["merge_active"])
            else:
                inner.append(self.strings["merge_waiting"])
        else:
            if d.get("ffmpeg_done"):
                inner.append(self.strings["convert_done"].format(size=d.get("final_size", 0)))
            elif d.get("ffmpeg_active") or stage == "ffmpeg":
                inner.append(self.strings["convert_active"])
            else:
                inner.append(self.strings["convert_waiting"])

        if stage == "done":
            inner.append(self.strings["upload_success"].format(elapsed=d.get("upload_elapsed", 0)))
        elif stage == "upload" or d.get("upload_started"):
            cur = d.get("upload_current", 0.0)
            total = d.get("upload_total", 0.0)
            pct = (cur / total * 100) if total > 0 else 0.0
            inner.append(self.strings["upload_progress"].format(
                pct=pct, cur=cur, total=total if total > 0 else cur))
        else:
            inner.append(self.strings["upload_waiting"])

        inner.append("----------------")

        if stage in ("ffmpeg", "probe"):
            speed_str = "N/A"
        elif stage == "upload" or d.get("upload_started"):
            speed_str = d.get("upload_speed", "N/A")
        else:
            speed_str = _fmt_speed(d.get("dl_speed_bytes", 0))

        inner.append(self.strings["time_stage"].format(elapsed=elapsed, stage_text=f"Speed: {speed_str}"))

        title_block = f"<b>{title}</b>"
        debug_block = f'<pre><code class="language-grabber">{chr(10).join(inner)}</code></pre>'
        return f"{title_block}\n{debug_block}"
    def _make_progress_hook(self, task_id):
        def _hook(d):
            dl = self._current_downloads.get(task_id)
            if not dl:
                return
            status = d.get("status")
            if status == "downloading":
                total = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
                downloaded = d.get("downloaded_bytes", 0)
                speed = d.get("speed") or 0
                dl["dl_percent"] = (downloaded / total * 100) if total > 0 else 0
                dl["dl_size"] = downloaded / 1024 / 1024
                dl["dl_total"] = total / 1024 / 1024 if total > 0 else dl["dl_size"]
                dl["dl_speed_bytes"] = speed
                dl["stage"] = "dl"
                dl["active"] = True
            elif status == "finished":
                total = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
                size_mb = total / 1024 / 1024
                dl["dl_done"] = True
                dl["dl_percent"] = 100.0
                if size_mb > 0:
                    dl["dl_total"] = dl.get("dl_total", 0) + size_mb
                    dl["dl_size"] = dl["dl_total"]
        return _hook
    def _make_pp_hook(self, task_id):
        def _hook(d):
            dl = self._current_downloads.get(task_id)
            if not dl:
                return
            if d.get("status") == "started":
                dl["ffmpeg_active"] = True
                dl["stage"] = "ffmpeg"
            elif d.get("status") == "finished":
                dl["ffmpeg_done"] = True
                dl["ffmpeg_active"] = False
        return _hook

    def _extract_url(self, text):
        if not text:
            return None
        m = _URL_PATTERN.search(text)
        return m.group(0) if m else None

    def _extract_bot_token(self, text):
        if not text:
            return None
        m = _BOT_TOKEN_RE.search(text)
        return m.group(0) if m else None

    def _get_topic_id(self, event):
        reply_to = getattr(event, "reply_to", None)
        if reply_to:
            return getattr(reply_to, "reply_to_top_id", None) or getattr(reply_to, "reply_to_msg_id", None)
        return None

    def _is_forum(self, chat):
        return getattr(chat, "forum", False)

    def _make_workdir(self):
        name = f"job_{int(time.time() * 1000)}_{os.getpid()}"
        path = os.path.join(self._cache_dir, name)
        os.makedirs(path, exist_ok=True)
        return path

    def _clean_workdir(self, path):
        if path and os.path.exists(path):
            try:
                shutil.rmtree(path)
            except Exception:
                pass

    def _clean_cache(self):
        if self._cache_dir and os.path.exists(self._cache_dir):
            try:
                shutil.rmtree(self._cache_dir)
            except Exception:
                pass

    def _clean_session(self):
        if self._session_dir and os.path.exists(self._session_dir):
            try:
                shutil.rmtree(self._session_dir)
            except Exception:
                pass

    def _clean_cookies(self):
        if self._cookie_dir and os.path.exists(self._cookie_dir):
            try:
                shutil.rmtree(self._cookie_dir)
            except Exception:
                pass
        self._db.set("Grabber", "cookies_content", None)

    def _clean_all(self):
        self._clean_cache()
        self._clean_session()
        self._clean_cookies()
        self.config["BOT_TOKEN"] = ""
        self.config["AUTORUNNER"] = False
        self.config["ACTIVE_GROUPS"] = []
        self.config["ACTIVE_TOPICS"] = {}
        self._active_groups.clear()
        self._active_topics.clear()

    def _save_active_groups(self):
        self.config["ACTIVE_GROUPS"] = list(self._active_groups)

    def _save_active_topics(self):
        self.config["ACTIVE_TOPICS"] = {str(k): list(v) for k, v in self._active_topics.items()}

    def _load_active_groups(self):
        saved = self.config.get("ACTIVE_GROUPS") or []
        self._active_groups = set(saved)

    def _load_active_topics(self):
        saved = self.config.get("ACTIVE_TOPICS") or {}
        if isinstance(saved, dict):
            self._active_topics = {int(k): set(v) for k, v in saved.items()}
        else:
            self._active_topics = {}

    def _kill_active_processes(self):
        for proc in self._active_processes:
            try:
                if proc and proc.poll() is None:
                    proc.terminate()
                    proc.wait(timeout=5)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass
        self._active_processes.clear()

    def _validate_cookies(self, content):
        if not content:
            return False
        lower = content.lower()
        return "# netscape" in lower or "youtube.com" in lower or "TRUE" in content

    def _format_duration(self, seconds):
        if not seconds:
            return "N/A"
        return _fmt_dur(seconds)

    async def _get_grabber_topic_id(self):
        try:
            forums_cache = self._db.get("heroku.forums", "forums_cache", {})
            tid = forums_cache.get("heroku-userbot", {}).get("Grabber")
            if tid:
                return tid
            asset_channel = self._db.get("heroku.forums", "channel_id", None)
            if not asset_channel:
                return None
            topic = await utils.asset_forum_topic(
                self._client, self._db, asset_channel,
                "Grabber", description="Grabber downloads log.",
                icon_emoji_id=GRABBER_TOPIC_ICON,
            )
            return topic.id if topic else None
        except Exception as e:
            logger.error(f"[GRABBER] topic init failed: {e}")
            return None

    async def _log_to_topic(self, text):
        if not self._grabber_topic_id:
            return
        asset_channel = self._db.get("heroku.forums", "channel_id", None)
        if not asset_channel:
            return
        try:
            await self.inline.bot.send_message(
                int(f"-100{asset_channel}"),
                text,
                parse_mode="HTML",
                message_thread_id=self._grabber_topic_id,
                disable_web_page_preview=True,
            )
        except Exception:
            pass

    async def client_ready(self, client, db):
        self._client = client
        self._db = db

        me = await client.get_me()
        tg_id = me.id
        self._root = os.path.join(tempfile.gettempdir(), f"grabber_{tg_id}")
        self._cache_dir = os.path.join(self._root, "cache")
        self._session_dir = os.path.join(self._root, "session")
        self._cookie_dir = os.path.join(self._root, "cookies")
        self._cookie_path = os.path.join(self._cookie_dir, "cookies.txt")

        self._clean_cache()
        os.makedirs(self._cache_dir, exist_ok=True)
        os.makedirs(self._session_dir, exist_ok=True)
        os.makedirs(self._cookie_dir, exist_ok=True)

        self._load_active_groups()
        self._load_active_topics()

        saved_cookies = self._db.get("Grabber", "cookies_content")
        if saved_cookies:
            try:
                with open(self._cookie_path, "w", encoding="utf-8") as f:
                    f.write(saved_cookies)
            except Exception:
                pass

        self._grabber_topic_id = await self._get_grabber_topic_id()

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(self._executor, _install_deps)
        if self.config["AUTORUNNER"] and self.config["BOT_TOKEN"]:
            try:
                await self._launch(self.config["BOT_TOKEN"])
            except Exception as e:
                logger.error(f"[GRABBER] Autorunner failed: {e}")

    async def _check_deps(self):
        deps = {"yt-dlp": "yt_dlp", "TgCrypto-pyrofork": "TgCrypto",
                "Pillow": "PIL", "aiohttp": "aiohttp", "mutagen": "mutagen"}
        lines = []
        for pkg, imp in deps.items():
            try:
                __import__(imp)
                lines.append(f"{pkg}: OK")
            except ImportError:
                lines.append(f"{pkg}: FAIL")
        for name in ["ffmpeg", "ffprobe"]:
            try:
                proc = await asyncio.create_subprocess_shell(
                    f"{name} -version",
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                await proc.communicate()
                lines.append(f"{name}: {'OK' if proc.returncode == 0 else 'FAIL'}")
            except Exception:
                lines.append(f"{name}: FAIL")
        return "\n".join(lines)

    async def _install_and_check(self):
        loop = asyncio.get_event_loop()
        dep_lines = await loop.run_in_executor(self._executor, _install_deps)
        for name in ["ffmpeg", "ffprobe"]:
            try:
                proc = await asyncio.create_subprocess_shell(
                    f"{name} -version", stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                await proc.communicate()
                dep_lines.append(f"{name}: {'OK' if proc.returncode == 0 else 'FAIL'}")
            except Exception:
                dep_lines.append(f"{name}: FAIL")
        dep_lines.reverse()
        return "\n".join(dep_lines)


    async def _create_backup(self):
        data = {
            "version": 4,
            "created": time.strftime("%Y-%m-%d %H:%M:%S"),
            "bot": {"BOT_TOKEN": self.config["BOT_TOKEN"]},
            "settings": {
                "AUTORUNNER": self.config["AUTORUNNER"],
                "MAX_FILE_MB": self.config["MAX_FILE_MB"],
                "MAX_WORKERS": self.config["MAX_WORKERS"],
                "ACTION_DELAY": self.config["ACTION_DELAY"],
            },
            "groups": {
                "active_groups": list(self._active_groups),
                "active_topics": {str(k): list(v) for k, v in self._active_topics.items()},
            },
        }
        backup_path = os.path.join(self._cache_dir, "Grabber_backup.json")
        with open(backup_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return backup_path

    async def _restore_from_backup(self, raw_content):
        try:
            data = json.loads(raw_content)
            bot_data = data.get("bot", {})
            settings = data.get("settings", {})
            groups = data.get("groups", {})
            if bot_data.get("BOT_TOKEN"):
                self.config["BOT_TOKEN"] = bot_data["BOT_TOKEN"]
            for k, t in [("AUTORUNNER", bool), ("MAX_FILE_MB", int),
                          ("MAX_WORKERS", int), ("ACTION_DELAY", int)]:
                if k in settings:
                    self.config[k] = t(settings[k])
            if "active_groups" in groups:
                self._active_groups = set(groups["active_groups"])
                self._save_active_groups()
            if "active_topics" in groups:
                self._active_topics = {int(k): set(v) for k, v in groups["active_topics"].items()}
                self._save_active_topics()
            return True
        except Exception as e:
            return False, str(e)

    def _fmt_main_menu(self):
        st = self.strings["status_running"] if self._running else self.strings["status_stopped"]
        cookies_st = (
            self.strings["cookie_ok"]
            if self._cookie_path and os.path.exists(self._cookie_path)
            else self.strings["cookie_empty"]
        )
        has_token = bool(self.config["BOT_TOKEN"])
        return self.strings["main_menu"].format(
            status=st,
            token="+" if has_token else "-",
            cookies=cookies_st,
            pending=len(self._queue_items),
            active=len(self._current_downloads),
            completed=self._stats["completed"],
            errors=self._stats["errors"],
        )

    def _get_main_markup(self):
        if self._running:
            start_stop = {"text": self.strings["btn_stop"], "callback": self._cb_stop, "style": "danger"}
        else:
            start_stop = {"text": self.strings["btn_start"], "callback": self._cb_start, "style": "success"}
        return [
            [
                {"text": self.strings["btn_status"], "callback": self._cb_status, "style": "primary"},
                {"text": self.strings["btn_token"], "callback": self._cb_token_menu, "style": "primary"},
            ],
            [
                {"text": self.strings["btn_cookies"], "callback": self._cb_cookies_menu, "style": "primary"},
                {"text": self.strings["btn_reboot"], "callback": self._cb_reboot, "style": "primary"},
            ],
            [
                start_stop,
                {"text": self.strings["btn_clear"], "callback": self._cb_clear_confirm, "style": "danger"},
            ],
            [
                {"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"},
            ],
        ]

    @loader.command(ru_doc="Управление Grabber", en_doc="Grabber management")
    async def grab(self, message):
        """Grabber management"""
        reply = await message.get_reply_message()
        token = None
        if reply and reply.text:
            token = self._extract_bot_token(reply.text)
        if token:
            await self._do_set_token_msg(message, token)
            return
        await self.inline.form(
            text=self._fmt_main_menu(),
            message=message,
            reply_markup=self._get_main_markup(),
            silent=True,
        )

    @loader.command(ru_doc="Добавить куки (реплай на .txt)", en_doc="Add cookies (reply to .txt file)")
    async def cookie(self, message):
        """Add cookies from .txt file reply"""
        reply = await message.get_reply_message()
        if not reply or not reply.media:
            await utils.answer(message, self.strings["no_reply_file"])
            return
        fname = getattr(reply.file, "name", "") or ""
        if not fname.endswith(".txt"):
            await utils.answer(message, self.strings["invalid_ext"])
            return
        try:
            temp_path = os.path.join(self._root, "temp_cookies.txt")
            dl = await reply.download_media(file=temp_path)
            with open(dl, "r", encoding="utf-8") as f:
                cookie_content = f.read()
            if not self._validate_cookies(cookie_content):
                os.remove(dl)
                await utils.answer(message, self.strings["cookie_invalid_format"])
                return
            self._db.set("Grabber", "cookies_content", cookie_content)
            os.makedirs(self._cookie_dir, exist_ok=True)
            with open(self._cookie_path, "w", encoding="utf-8") as f:
                f.write(cookie_content)
            os.remove(dl)
            await utils.answer(message, self.strings["cookie_saved"])
            await self._create_backup()
        except Exception as e:
            await utils.answer(message, self.strings["cookie_err"].format(str(e)))

    async def _do_set_token_msg(self, message, token: str):
        msg = await utils.answer(message, self.strings["installing"])
        if isinstance(msg, list):
            msg = msg[0]
        dep_log = await self._install_and_check()
        self.config["BOT_TOKEN"] = token
        prefix = self.get_prefix()
        try:
            await self._launch(token)
            bot_me = await self._bot.get_me()
            connect_line = f"bot: @{bot_me.username} ({bot_me.id}): OK"
            self.config["AUTORUNNER"] = True
            await self._create_backup()
            full_log = connect_line + "\n" + dep_log
            await self._safe_edit(msg, self.strings["token_started"].format(
                log=_escape_html(full_log[-3700:])), parse_mode="html")
        except Exception as e:
            await self._safe_edit(msg, self.strings["start_failed"].format(
                error=str(e)[:200], prefix=prefix))

    async def _cb_status(self, call):
        await call.edit(
            text=self._fmt_main_menu(),
            reply_markup=self._get_main_markup(),
        )

    async def _cb_token_menu(self, call):
        await call.edit(
            text=self.strings["token_menu"],
            reply_markup=[
                [
                    {"text": self.strings["btn_set_token"], "input": self.strings["input_token"], "handler": self._cb_set_token, "style": "success"},
                ],
                [
                    {"text": self.strings["btn_back"], "callback": self._cb_back_main, "style": "danger"},
                ],
            ],
        )

    async def _cb_set_token(self, call, token_input: str):
        token = self._extract_bot_token(token_input.strip())
        if not token:
            await call.answer(self.strings["need_token"], show_alert=True)
            return
        await call.edit(text=self.strings["installing"])
        dep_log = await self._install_and_check()
        self.config["BOT_TOKEN"] = token
        prefix = self.get_prefix()
        try:
            await self._launch(token)
            bot_me = await self._bot.get_me()
            connect_line = f"bot: @{bot_me.username} ({bot_me.id}): OK"
            self.config["AUTORUNNER"] = True
            await self._create_backup()
            full_log = connect_line + "\n" + dep_log
            await call.edit(
                text=self.strings["token_started"].format(log=_escape_html(full_log[-3700:])),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_back_main, "style": "primary"}]],
            )
        except Exception as e:
            await call.edit(
                text=self.strings["start_failed"].format(error=str(e)[:200], prefix=prefix),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_back_main, "style": "danger"}]],
            )

    async def _cb_cookies_menu(self, call):
        has_cookies = bool(self._cookie_path and os.path.exists(self._cookie_path))
        markup = []
        if has_cookies:
            markup.append([
                {"text": self.strings["btn_remove_cookies"], "callback": self._cb_remove_cookies, "style": "danger"},
            ])
        markup.append([
            {"text": self.strings["btn_back"], "callback": self._cb_back_main, "style": "danger"},
        ])
        await call.edit(
            text=self.strings["cookies_menu"].format(
                status=self.strings["cookie_ok"] if has_cookies else self.strings["cookie_empty"]
            ),
            reply_markup=markup,
        )

    async def _cb_remove_cookies(self, call):
        self._clean_cookies()
        os.makedirs(self._cookie_dir, exist_ok=True)
        await call.answer(self.strings["cookie_cleared_short"], show_alert=True)
        await self._cb_cookies_menu(call)

    async def _cb_reboot(self, call):
        await call.edit(
            text=self.strings["reboot_start"],
            reply_markup=[
                [
                    {"text": self.strings["btn_confirm_reboot"], "callback": self._cb_do_reboot, "style": "danger"},
                    {"text": self.strings["btn_back"], "callback": self._cb_back_main, "style": "primary"},
                ],
            ],
        )

    async def _cb_do_reboot(self, call):
        await call.edit(text=self.strings["reboot_start"])
        await self._shutdown()
        self._kill_active_processes()
        self._clean_cache()
        self._clean_session()
        os.makedirs(self._cache_dir, exist_ok=True)
        os.makedirs(self._session_dir, exist_ok=True)
        self._stats = {"completed": 0, "errors": 0}
        self._queue_items.clear()
        self._processed_buttons.clear()
        self._edit_state.clear()
        self._download_queue = asyncio.Queue()
        self._current_downloads.clear()
        self._og_cache.clear()
        await self._install_and_check()
        tkn = self.config["BOT_TOKEN"]
        prefix = self.get_prefix()
        if tkn:
            try:
                await self._launch(tkn)
                self.config["AUTORUNNER"] = True
                await call.edit(
                    text=self.strings["reboot_done"],
                    reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_back_main, "style": "success"}]],
                )
            except Exception as e:
                await call.edit(
                    text=self.strings["start_failed"].format(error=str(e)[:200], prefix=prefix),
                    reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_back_main, "style": "danger"}]],
                )
        else:
            await call.edit(
                text=self.strings["reboot_no_token"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_back_main, "style": "danger"}]],
            )

    async def _cb_clear_confirm(self, call):
        await call.edit(
            text=self.strings["clear_confirm"],
            reply_markup=[
                [
                    {"text": self.strings["btn_confirm_clear"], "callback": self._cb_do_clear, "style": "danger"},
                    {"text": self.strings["btn_back"], "callback": self._cb_back_main, "style": "primary"},
                ],
            ],
        )

    async def _cb_do_clear(self, call):
        await self._shutdown()
        self._clean_all()
        os.makedirs(self._cache_dir, exist_ok=True)
        os.makedirs(self._session_dir, exist_ok=True)
        os.makedirs(self._cookie_dir, exist_ok=True)
        self._stats = {"completed": 0, "errors": 0}
        self._queue_items.clear()
        self._processed_buttons.clear()
        self._edit_state.clear()
        self._download_queue = asyncio.Queue()
        self._current_downloads.clear()
        await call.edit(
            text=self.strings["clear_done"],
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_back_main, "style": "success"}]],
        )

    async def _cb_start(self, call):
        tkn = self.config["BOT_TOKEN"]
        if not tkn:
            await call.answer(self.strings["need_token"], show_alert=True)
            return
        await call.edit(text=self.strings["installing"])
        prefix = self.get_prefix()
        try:
            await self._launch(tkn)
            self.config["AUTORUNNER"] = True
        except Exception as e:
            await call.edit(
                text=self.strings["start_failed"].format(error=str(e)[:200], prefix=prefix),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_back_main, "style": "danger"}]],
            )
            return
        await call.edit(text=self._fmt_main_menu(), reply_markup=self._get_main_markup())

    async def _cb_stop(self, call):
        await self._shutdown()
        await call.edit(text=self._fmt_main_menu(), reply_markup=self._get_main_markup())

    async def _cb_back_main(self, call):
        await call.edit(
            text=self._fmt_main_menu(),
            reply_markup=self._get_main_markup(),
        )

    async def _cb_close(self, call):
        await call.delete()

    async def _launch(self, tkn):
        await self._shutdown()
        os.makedirs(self._session_dir, exist_ok=True)
        sess_file = os.path.join(self._session_dir, "grabber_bot")
        for ext in ["", ".session", ".session-journal"]:
            path = sess_file + ext
            if os.path.exists(path):
                try:
                    os.remove(path)
                except Exception:
                    pass
        await asyncio.sleep(0.5)
        self._bot = TelegramClient(
            sess_file,
            api_id=self._client.api_id,
            api_hash=self._client.api_hash,
            connection_retries=3,
            retry_delay=1,
            request_retries=3,
        )
        await self._bot.start(bot_token=tkn)
        self._running = True

        asset_channel = self._db.get("heroku.forums", "channel_id", None)
        if asset_channel:
            try:
                me = await self._client.get_me()
                if getattr(me, "premium", False):
                    from telethon.tl.functions.channels import InviteToChannelRequest, EditAdminRequest
                    from telethon.tl.types import ChatAdminRights
                    from telethon.errors import UserAlreadyParticipantError
                    log_chat = await self._client.get_entity(int(f"-100{asset_channel}"))
                    bot_me = await self._bot.get_me()
                    bot_username = f"@{bot_me.username}"
                    try:
                        await self._client(InviteToChannelRequest(log_chat, [bot_username]))
                    except UserAlreadyParticipantError:
                        pass
                    except Exception:
                        pass
                    try:
                        await self._client(EditAdminRequest(
                            log_chat, bot_username,
                            ChatAdminRights(post_messages=True, edit_messages=True, delete_messages=True),
                            rank="Grabber",
                        ))
                    except Exception:
                        pass
            except Exception as e:
                logger.debug(f"[GRABBER] Bot invite to log group failed: {e}")

        self._bot.add_event_handler(self._h_start, events.NewMessage(pattern="/start"))
        self._bot.add_event_handler(self._h_msg, events.NewMessage())
        self._bot.add_event_handler(self._h_btn, events.CallbackQuery())
        num_workers = max(1, self.config["MAX_WORKERS"])
        self._worker_tasks = [asyncio.create_task(self._queue_worker()) for _ in range(num_workers)]
        bot_me = await self._bot.get_me()
        logger.info(f"[GRABBER] Started as @{bot_me.username}, workers={num_workers}")

    async def _shutdown(self):
        self._running = False
        self._kill_active_processes()
        for t in self._worker_tasks:
            t.cancel()
            try:
                await t
            except Exception:
                pass
        self._worker_tasks.clear()
        if self._bot:
            try:
                self._bot.remove_event_handler(self._h_start)
                self._bot.remove_event_handler(self._h_msg)
                self._bot.remove_event_handler(self._h_btn)
            except Exception:
                pass
            try:
                await self._bot.disconnect()
            except Exception:
                pass
            await asyncio.sleep(1.0)
            self._bot = None
        self._download_queue = asyncio.Queue()
        self._queue_items.clear()
        self._processed_buttons.clear()
        self._edit_state.clear()
        self._current_downloads.clear()

    async def _queue_worker(self):
        while self._running:
            try:
                try:
                    task = await asyncio.wait_for(self._download_queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue

                (task_id, chat_id, msg_id, url, mode, workdir,
                 info, meta_dict, reply_to_topic, orig_msg_id) = task
                self._queue_items.pop_first()
                title = (info.get("title") or "Unknown")[:100]

                try:
                    await self._process_download(
                        task_id, chat_id, msg_id, url, mode,
                        workdir, info, meta_dict, reply_to_topic, orig_msg_id,
                    )
                    self._stats["completed"] += 1
                except Exception as e:
                    self._stats["errors"] += 1
                    err_text = str(e)[:150]
                    try:
                        await self._bot.edit_message(
                            chat_id, msg_id,
                            self.strings["grab_failed"].format(error=err_text[:1024]),
                            parse_mode="html",
                        )
                    except Exception:
                        pass
                    await self._log_to_topic(self.strings["log_error"].format(
                        title=_escape_html(title[:60]), error=_escape_html(err_text)))
                finally:
                    self._current_downloads.pop(task_id, None)
                    self._clean_workdir(workdir)
                    self._download_queue.task_done()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[GRABBER] Worker error: {e}")
                await asyncio.sleep(1)

    async def _edit_progress(self, task_id, chat_id, msg_id):
        dl = self._current_downloads.get(task_id)
        if not dl:
            return
        og_url = dl.get("og_url")
        text = self._build_status_message(task_id)
        try:
            if og_url:
                from telethon.tl.functions.messages import EditMessageRequest
                from telethon.tl.types import InputMediaWebPage
                msg_text, entities = await self._bot._parse_message_text(text, "html")
                peer = await self._bot.get_input_entity(chat_id)
                current_msg = await self._bot.get_messages(chat_id, ids=msg_id)
                reply_markup = current_msg.reply_markup if current_msg else None
                await self._bot(EditMessageRequest(
                    peer=peer,
                    id=msg_id,
                    message=msg_text,
                    media=InputMediaWebPage(url=og_url, optional=True, force_large_media=True),
                    invert_media=True,
                    reply_markup=reply_markup,
                    entities=entities,
                    no_webpage=False,
                ))
            else:
                await self._bot.edit_message(chat_id, msg_id, text, parse_mode="html")
        except Exception:
            pass

    async def _safe_edit(self, target, text, **kwargs):
        for attempt in range(3):
            try:
                if hasattr(target, "edit"):
                    await target.edit(text, **kwargs)
                elif self._bot and isinstance(target, tuple):
                    chat_id, msg_id = target
                    await self._bot.edit_message(chat_id, msg_id, text, **kwargs)
                return True
            except MessageNotModifiedError:
                return True
            except FloodWaitError as e:
                await asyncio.sleep(min(e.seconds, 5))
            except Exception:
                if attempt < 2:
                    await asyncio.sleep(0.5)
        return False

    async def _safe_delete(self, chat_id, msg_id):
        try:
            if self._bot:
                await self._bot.delete_messages(chat_id, msg_id)
        except Exception:
            pass

    async def _try_delete_orig(self, chat_id, orig_msg_id):
        if not orig_msg_id:
            return
        try:
            if self._bot:
                await self._bot.delete_messages(chat_id, orig_msg_id)
        except Exception:
            pass

    async def _set_reaction(self, event, emoticon):
        try:
            peer = await event.get_input_chat()
            await self._bot(SendReactionRequest(
                peer=peer, msg_id=event.id,
                reaction=[ReactionEmoji(emoticon=emoticon)],
            ))
        except Exception:
            pass

    async def _h_start(self, ev):
        if not self._running:
            return
        if ev.is_private:
            try:
                user = await self._bot.get_entity(ev.sender_id)
                name = (
                    f"{getattr(user, 'first_name', '') or ''} "
                    f"{getattr(user, 'last_name', '') or ''}"
                ).strip() or "friend"
                user_link = f'<a href="tg://user?id={ev.sender_id}">{_escape_html(name)}</a>'
                await ev.reply(self.strings["hello"].format(user_link=user_link), parse_mode="html")
            except Exception:
                await ev.reply(self.strings["hello_fallback"], parse_mode="html")
        else:
            chat_id = ev.chat_id
            topic_id = self._get_topic_id(ev)
            try:
                chat = await self._bot.get_entity(chat_id)
                is_forum = self._is_forum(chat)
            except Exception:
                is_forum = False

            if is_forum and topic_id:
                if chat_id in self._active_topics and topic_id in self._active_topics.get(chat_id, set()):
                    self._active_topics[chat_id].discard(topic_id)
                    if not self._active_topics[chat_id]:
                        del self._active_topics[chat_id]
                    self._save_active_topics()
                    await ev.reply(self.strings["hello_topic_off"], parse_mode="html")
                else:
                    if chat_id not in self._active_topics:
                        self._active_topics[chat_id] = set()
                    self._active_topics[chat_id].add(topic_id)
                    self._save_active_topics()
                    await ev.reply(self.strings["hello_topic"], parse_mode="html")
            else:
                if chat_id in self._active_groups:
                    self._active_groups.discard(chat_id)
                    self._save_active_groups()
                    await ev.reply(self.strings["hello_group_off"], parse_mode="html")
                else:
                    self._active_groups.add(chat_id)
                    self._save_active_groups()
                    await ev.reply(self.strings["hello_group"], parse_mode="html")

    async def _h_msg(self, ev):
        if not self._running or not self._bot:
            return
        if ev.text and ev.text.startswith("/"):
            return

        user_id = ev.sender_id
        is_private = ev.is_private

        if is_private and user_id in self._edit_state:
            await self._handle_editor_input(ev)
            return

        text = ev.text or ""

        if is_private:
            if not text:
                return
            link = self._extract_url(text)
            if not link:
                await self._set_reaction(ev, self.strings["react_fail"])
                return
        else:
            chat_id = ev.chat_id
            topic_id = self._get_topic_id(ev)
            try:
                chat = await self._bot.get_entity(chat_id)
                is_forum = self._is_forum(chat)
            except Exception:
                is_forum = False
            if is_forum:
                if chat_id not in self._active_topics:
                    return
                if topic_id not in self._active_topics.get(chat_id, set()):
                    return
            else:
                if chat_id not in self._active_groups:
                    return
            if not text:
                return
            link = self._extract_url(text)
            if not link:
                return

        await self._set_reaction(ev, self.strings["react_ok"])
        topic_id = self._get_topic_id(ev) if not is_private else None
        msg = await ev.reply(self.strings["analyzing"], parse_mode="html")

        try:
            info = await self._get_info(link)
            title = (info.get("title") or "Media")[:120]
            duration = info.get("duration", 0) or 0
            duration_str = self._format_duration(duration)

            if is_private:
                buttons = [
                    [
                        Button.inline(self.strings["btn_video"], data=f"menu:video:{ev.id}"),
                        Button.inline(self.strings["btn_audio"], data=f"menu:audio:{ev.id}"),
                    ],
                    [Button.inline(self.strings["btn_cancel"], data=f"g:cancel:{ev.id}")],
                ]
                t = self.strings["found_media"].format(title=title, duration=duration_str)
            else:
                buttons = [
                    [Button.inline(self.strings["btn_video"], data=f"menu:video:{ev.id}:{topic_id or 0}")],
                    [Button.inline(self.strings["btn_cancel"], data=f"g:cancel:{ev.id}")],
                ]
                t = self.strings["found_media_group"].format(title=title, duration=duration_str)

            await msg.edit(t, buttons=buttons, parse_mode="html")

            asyncio.ensure_future(self._apply_og_preview(msg, link))
        except Exception as e:
            try:
                await msg.edit(self.strings["grab_failed"].format(error=str(e)[:1024]), parse_mode="html")
            except Exception:
                pass

    async def _apply_og_preview(self, msg, url):
        try:
            if url in self._og_cache:
                x0_url = self._og_cache[url]
            else:
                og_url = await _fetch_og_image(url)
                if not og_url:
                    return None
                img_data = await _download_url(og_url, timeout=15)
                if not img_data:
                    return None
                normalized = normalize_cover(img_data, force_jpeg=True)
                if not normalized:
                    return None
                filename = f"preview_{int(time.time())}.jpg"
                x0_url = await _upload_to_x0(normalized, filename, "image/jpeg")
                if not x0_url:
                    return None
                self._og_cache[url] = x0_url

            try:
                await self._bot(functions.messages.GetWebPageRequest(url=x0_url, hash=0))
            except Exception:
                pass
            await asyncio.sleep(1)

            try:
                from telethon.tl.functions.messages import EditMessageRequest
                from telethon.tl.types import InputMediaWebPage
                current_msg = await self._bot.get_messages(msg.chat_id, ids=msg.id)
                if not current_msg:
                    return None
                current_text = current_msg.message or self.strings["analyzing"]
                current_entities = current_msg.entities or []
                current_buttons = current_msg.reply_markup
                peer = await self._bot.get_input_entity(msg.chat_id)
                await self._bot(EditMessageRequest(
                    peer=peer,
                    id=msg.id,
                    message=current_text,
                    media=InputMediaWebPage(url=x0_url, optional=True, force_large_media=True),
                    invert_media=True,
                    reply_markup=current_buttons,
                    entities=current_entities,
                    no_webpage=False,
                ))
                return x0_url
            except Exception as e:
                logger.debug(f"[GRABBER] og preview apply failed: {e}")
        except Exception as e:
            logger.debug(f"[GRABBER] og preview pipeline failed: {e}")
        return None

    async def _h_btn(self, ev):
        if not self._running or not self._bot:
            return
        data = ev.data.decode()
        parts = data.split(":")
        if len(parts) < 2:
            return

        prefix = parts[0]
        action = parts[1]
        is_private = ev.is_private
        current_topic_id = self._get_topic_id(ev) if not is_private else None

        if prefix == "g" and action == "cancel":
            self._processed_buttons.add((ev.chat_id, ev.message_id))
            try:
                await ev.edit(self.strings["cancelled"], parse_mode="html")
            except Exception:
                pass
            return

        if prefix == "menu":
            orig_id = int(parts[2]) if len(parts) > 2 else 0
            stored_topic_id = int(parts[3]) if len(parts) > 3 and parts[3] != "0" else None
            topic_id = stored_topic_id or current_topic_id
            try:
                orig = await self._bot.get_messages(ev.chat_id, ids=orig_id)
                if not orig or not orig.text:
                    return
                link = self._extract_url(orig.text)
                if not link:
                    return
            except Exception:
                return

            if action == "video":
                try:
                    info = await self._get_info(link)
                    available = self._get_available_formats(info)
                except Exception:
                    available = [720, 480, 360]
                buttons = [
                    [Button.inline(f"{h}p", data=f"dl:video:{h}:{orig_id}:{topic_id or 0}")]
                    for h in available
                ]
                buttons.append([Button.inline(self.strings["btn_back"], data=f"back:main:{orig_id}:{topic_id or 0}")])
                try:
                    await ev.edit(self.strings["quality_menu"], buttons=buttons, parse_mode="html")
                except Exception:
                    pass

            elif action == "audio":
                if not is_private:
                    return
                buttons = [
                    [Button.inline(self.strings["btn_orig_thumb"], data=f"dl:audio:thumb:{orig_id}")],
                    [Button.inline(self.strings["btn_orig_clean"], data=f"dl:audio:clean:{orig_id}")],
                    [Button.inline(self.strings["btn_custom"], data=f"edit:start:{orig_id}:{ev.sender_id}")],
                    [Button.inline(self.strings["btn_back"], data=f"back:main:{orig_id}:0")],
                ]
                try:
                    await ev.edit(self.strings["audio_menu"], buttons=buttons, parse_mode="html")
                except Exception:
                    pass
            return

        if prefix == "back":
            orig_id = int(parts[2]) if len(parts) > 2 else 0
            stored_topic_id = int(parts[3]) if len(parts) > 3 and parts[3] != "0" else None
            topic_id = stored_topic_id or current_topic_id
            try:
                orig = await self._bot.get_messages(ev.chat_id, ids=orig_id)
                if not orig or not orig.text:
                    return
                link = self._extract_url(orig.text)
                if not link:
                    return
                info = await self._get_info(link)
            except Exception:
                return
            title = (info.get("title") or "Media")[:120]
            duration_str = self._format_duration(info.get("duration", 0) or 0)
            if is_private:
                buttons = [
                    [
                        Button.inline(self.strings["btn_video"], data=f"menu:video:{orig_id}"),
                        Button.inline(self.strings["btn_audio"], data=f"menu:audio:{orig_id}"),
                    ],
                    [Button.inline(self.strings["btn_cancel"], data=f"g:cancel:{orig_id}")],
                ]
                t = self.strings["found_media"].format(title=title, duration=duration_str)
            else:
                buttons = [
                    [Button.inline(self.strings["btn_video"], data=f"menu:video:{orig_id}:{topic_id or 0}")],
                    [Button.inline(self.strings["btn_cancel"], data=f"g:cancel:{orig_id}")],
                ]
                t = self.strings["found_media_group"].format(title=title, duration=duration_str)
            try:
                await ev.edit(t, buttons=buttons, parse_mode="html")
            except Exception:
                pass
            return

        if prefix == "edit":
            if not is_private:
                return
            if action == "start":
                orig_id = int(parts[2]) if len(parts) > 2 else 0
                owner_id = int(parts[3]) if len(parts) > 3 else ev.sender_id
                try:
                    orig = await self._bot.get_messages(ev.chat_id, ids=orig_id)
                    if not orig or not orig.text:
                        return
                    link = self._extract_url(orig.text)
                    if not link:
                        return
                except Exception:
                    return
                async with self._edit_lock:
                    self._edit_state[ev.sender_id] = {
                        "step": "waiting_title", "url": link,
                        "msg_event": ev, "orig_id": orig_id, "owner_id": owner_id,
                        "custom_title": None, "custom_artist": None, "custom_thumb": None,
                    }
                try:
                    await ev.edit(self.strings["editor_mode"],
                                  buttons=[[Button.inline(self.strings["btn_cancel"], data=f"edit:cancel:{ev.sender_id}")]],
                                  parse_mode="html")
                except Exception:
                    pass

            elif action == "cancel":
                target_id = int(parts[2]) if len(parts) > 2 else ev.sender_id
                if ev.sender_id != target_id:
                    await ev.answer(self.strings["not_your_editor"], alert=True)
                    return
                async with self._edit_lock:
                    if target_id in self._edit_state:
                        state = self._edit_state[target_id]
                        if state.get("custom_thumb"):
                            self._clean_workdir(os.path.dirname(state["custom_thumb"]))
                        del self._edit_state[target_id]
                try:
                    await ev.edit(self.strings["op_cancelled"], buttons=None, parse_mode="html")
                except Exception:
                    pass

            elif action == "skipthumb":
                target_id = int(parts[2]) if len(parts) > 2 else ev.sender_id
                if ev.sender_id != target_id:
                    await ev.answer(self.strings["not_your_editor"], alert=True)
                    return
                async with self._edit_lock:
                    if target_id in self._edit_state:
                        self._edit_state[target_id]["step"] = "confirm"
                        state = self._edit_state[target_id]
                        await self._show_confirmation(ev, state, target_id)

            elif action == "confirm":
                target_id = int(parts[2]) if len(parts) > 2 else ev.sender_id
                if ev.sender_id != target_id:
                    await ev.answer(self.strings["not_your_editor"], alert=True)
                    return
                async with self._edit_lock:
                    if target_id not in self._edit_state:
                        await ev.answer(self.strings["session_expired"], alert=True)
                        return
                    state = self._edit_state[target_id]
                    url = state["url"]
                    meta_dict = {
                        "title": state.get("custom_title"),
                        "artist": state.get("custom_artist"),
                        "thumb_path": state.get("custom_thumb"),
                    }
                    del self._edit_state[target_id]
                await self._queue_download(ev, url, "mp3", meta_dict, None)

            elif action == "retry":
                target_id = int(parts[2]) if len(parts) > 2 else ev.sender_id
                if ev.sender_id != target_id:
                    await ev.answer(self.strings["not_your_editor"], alert=True)
                    return
                async with self._edit_lock:
                    if target_id in self._edit_state:
                        state = self._edit_state[target_id]
                        if state.get("custom_thumb"):
                            self._clean_workdir(os.path.dirname(state["custom_thumb"]))
                        state.update({"step": "waiting_title", "custom_title": None,
                                      "custom_artist": None, "custom_thumb": None})
                try:
                    await ev.edit(self.strings["editor_mode"],
                                  buttons=[[Button.inline(self.strings["btn_cancel"], data=f"edit:cancel:{target_id}")]],
                                  parse_mode="html")
                except Exception:
                    pass
            return

        if prefix == "dl":
            button_key = (ev.chat_id, ev.message_id)
            if not self._processed_buttons.add(button_key):
                await ev.answer(self.strings["already_processing"], alert=True)
                return
            media_type = parts[1]
            param = parts[2] if len(parts) > 2 else None
            orig_id = int(parts[3]) if len(parts) > 3 else 0
            stored_topic_id = int(parts[4]) if len(parts) > 4 and parts[4] != "0" else None
            topic_id = stored_topic_id or current_topic_id
            try:
                orig = await self._bot.get_messages(ev.chat_id, ids=orig_id)
                if not orig or not orig.text:
                    return
                link = self._extract_url(orig.text)
                if not link:
                    return
            except Exception:
                return
            meta_dict = None
            if media_type == "video":
                mode = param
            elif media_type == "audio":
                if not is_private:
                    return
                mode = "mp3"
                if param == "thumb":
                    meta_dict = {"use_yt_thumb": True}
            else:
                return
            await self._queue_download(ev, link, mode, meta_dict, topic_id, orig_id=orig_id)

    async def _handle_editor_input(self, ev):
        user_id = ev.sender_id
        state = self._edit_state.get(user_id)
        if not state:
            return
        msg = state.get("msg_event")
        step = state.get("step")

        if step == "waiting_thumb":
            if ev.document:
                mime = getattr(ev.document, "mime_type", "") or ""
                if mime in ("image/png", "image/jpeg"):
                    try:
                        await ev.delete()
                    except Exception:
                        pass
                    dl_msg = await ev.respond(self.strings["downloading_image"], parse_mode="html")
                    temp_dir = os.path.join(self._cache_dir, f"thumb_{user_id}_{int(time.time())}")
                    os.makedirs(temp_dir, exist_ok=True)
                    try:
                        downloaded = await ev.download_media(file=temp_dir)
                        if downloaded and os.path.exists(downloaded):
                            async with self._edit_lock:
                                state["custom_thumb"] = downloaded
                                state["step"] = "confirm"
                            await dl_msg.delete()
                            await self._show_confirmation(msg, state, user_id)
                    except Exception:
                        try:
                            await dl_msg.delete()
                        except Exception:
                            pass
                        self._clean_workdir(temp_dir)
                    return
                else:
                    await ev.respond(self.strings["only_image"], parse_mode="html")
                    return
            elif ev.photo:
                await ev.respond(self.strings["only_image"], parse_mode="html")
                return
            elif ev.text and not ev.text.startswith("/"):
                await ev.respond(self.strings["text_needed_image"], parse_mode="html")
                return
            return

        if not ev.text:
            return
        try:
            await ev.delete()
        except Exception:
            pass

        if step == "waiting_title":
            async with self._edit_lock:
                state["custom_title"] = ev.text
                state["step"] = "waiting_artist"
            await msg.edit(
                self.strings["edit_title_done"].format(_escape_html(ev.text)),
                parse_mode="html",
                buttons=[[Button.inline(self.strings["btn_cancel"], data=f"edit:cancel:{user_id}")]],
            )
        elif step == "waiting_artist":
            async with self._edit_lock:
                state["custom_artist"] = ev.text
                state["step"] = "waiting_thumb"
            await msg.edit(
                self.strings["edit_artist_done"].format(_escape_html(ev.text)),
                parse_mode="html",
                buttons=[
                    [Button.inline(self.strings["btn_no_cover"], data=f"edit:skipthumb:{user_id}")],
                    [Button.inline(self.strings["btn_cancel"], data=f"edit:cancel:{user_id}")],
                ],
            )

    async def _show_confirmation(self, msg, state, user_id):
        title = _escape_html(state.get("custom_title", "Unknown"))
        artist = _escape_html(state.get("custom_artist", "Unknown"))
        has_thumb = self.strings["thumb_yes"] if state.get("custom_thumb") else self.strings["thumb_no"]
        await msg.edit(
            self.strings["confirm_menu"].format(title=title, artist=artist, thumb=has_thumb),
            parse_mode="html",
            buttons=[
                [Button.inline(self.strings["btn_confirm"], data=f"edit:confirm:{user_id}")],
                [Button.inline(self.strings["btn_retry"], data=f"edit:retry:{user_id}")],
                [Button.inline(self.strings["btn_cancel"], data=f"edit:cancel:{user_id}")],
            ],
        )

    async def _queue_download(self, ev, url, mode, meta_dict=None, topic_id=None, orig_id=None):
        try:
            info = await self._get_info(url)
        except Exception as e:
            try:
                await ev.edit(self.strings["grab_failed"].format(error=str(e)[:1024]), parse_mode="html")
            except Exception:
                pass
            return

        workdir = self._make_workdir()
        title = (info.get("title") or "Media")[:50]
        task_id = f"{ev.chat_id}_{ev.message_id}_{int(time.time() * 1000)}"
        self._queue_items.append({"title": title, "url": url})

        is_active = bool(self._current_downloads)
        pending = len(self._queue_items)

        if is_active or pending > 1:
            try:
                await ev.edit(self.strings["queue_pos"].format(pos=pending), parse_mode="html")
            except Exception:
                pass
        else:
            dl = self._empty_progress()
            dl.update({
                "active": True, "title": title,
                "start_time": time.time(), "stage": "init",
                "is_audio_only": mode == "mp3",
                "og_url": self._og_cache.get(url),
            })
            self._current_downloads[task_id] = dl
            try:
                await ev.edit(self._build_status_message(task_id), parse_mode="html")
            except Exception:
                pass

        await self._download_queue.put(
            (task_id, ev.chat_id, ev.message_id, url, mode, workdir, info, meta_dict, topic_id, orig_id)
        )

    def _yt_extract_info(self, url):
        try:
            import yt_dlp
            opts = {
                "quiet": True,
                "no_warnings": True,
                "socket_timeout": 30,
                "skip_download": True,
            }
            if os.path.exists(self._cookie_path):
                opts["cookiefile"] = self._cookie_path
            with yt_dlp.YoutubeDL(opts) as ydl:
                return ydl.extract_info(url, download=False)
        except Exception as e:
            return None, str(e)

    async def _get_info(self, url):
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(self._executor, self._yt_extract_info, url)
        if isinstance(result, tuple):
            _, err = result
            raise Exception(err[:1024])
        if result is None:
            raise Exception("Could not extract media info")
        return result

    def _get_available_formats(self, info):
        formats = info.get("formats", [])
        heights = set()
        for fmt in formats:
            h = fmt.get("height")
            vcodec = fmt.get("vcodec", "none")
            if h and vcodec != "none":
                heights.add(h)
        standard = [2160, 1440, 1080, 720, 480, 360, 240, 144]
        available = sorted([h for h in standard if h in heights], reverse=True)
        return available or [720, 480, 360]

    def _is_combined_format(self, info, height):
        for fmt in info.get("formats", []):
            h = fmt.get("height") or 0
            vcodec = fmt.get("vcodec", "none") or "none"
            acodec = fmt.get("acodec", "none") or "none"
            if h <= height and vcodec != "none" and acodec != "none":
                return True
        return False

    def _build_format_string(self, height):
        return (
            f"bestvideo[height<={height}][vcodec^=avc1]+bestaudio[ext=m4a]"
            f"/bestvideo[height<={height}]+bestaudio"
            f"/best[height<={height}][ext=mp4]/best[height<={height}]"
        )

    def _build_video_only_format(self, height):
        return (
            f"bestvideo[height<={height}][vcodec^=avc1]"
            f"/bestvideo[height<={height}]"
            f"/best[height<={height}]"
        )

    async def _progress_loop(self, task_id, chat_id, msg_id, done_event):
        delay = max(1, self.config["ACTION_DELAY"])
        while not done_event.is_set():
            try:
                await asyncio.sleep(delay)
                if not done_event.is_set():
                    await self._edit_progress(task_id, chat_id, msg_id)
            except asyncio.CancelledError:
                break
            except Exception:
                pass

    async def _upload_progress_loop(self, task_id, chat_id, msg_id, start_time):
        delay = max(1, self.config["ACTION_DELAY"])
        while True:
            try:
                await asyncio.sleep(delay)
                dl = self._current_downloads.get(task_id)
                if dl:
                    dl["upload_elapsed"] = time.time() - start_time
                await self._edit_progress(task_id, chat_id, msg_id)
            except asyncio.CancelledError:
                break
            except Exception:
                pass

    def _make_upload_callback(self, task_id, start_time):
        def _cb(current, total):
            dl = self._current_downloads.get(task_id)
            if not dl:
                return
            cur_mb = current / 1024 / 1024
            total_mb = total / 1024 / 1024 if total else cur_mb
            elapsed = time.time() - start_time
            speed_bytes = current / elapsed if elapsed > 0 else 0
            dl["upload_current"] = cur_mb
            dl["upload_total"] = total_mb
            dl["upload_speed"] = _fmt_speed(speed_bytes)
            dl["upload_elapsed"] = elapsed
        return _cb

    async def _send_or_forward(
        self, task_id, chat_id, filepath, caption, attrs,
        filesize, reply_to_topic, is_audio, send_thumb=None,
    ):
        BOT_LIMIT = 2000.0
        USER_LIMIT = 4096.0

        upload_start = time.time()
        cb = self._make_upload_callback(task_id, upload_start)

        if filesize <= BOT_LIMIT:
            await self._bot.send_file(
                chat_id, filepath,
                caption=caption, parse_mode="html",
                force_document=False, supports_streaming=(not is_audio),
                attributes=attrs, thumb=send_thumb, reply_to=reply_to_topic,
                progress_callback=cb,
            )
            return

        if filesize > USER_LIMIT:
            await self._bot.send_message(
                chat_id, self.strings["too_large_over_limit"].format(size_mb=filesize),
                parse_mode="html",
            )
            return

        me = await self._client.get_me()
        if not getattr(me, "premium", False):
            await self._bot.send_message(
                chat_id, self.strings["too_large_no_premium"].format(size_mb=filesize),
                parse_mode="html",
            )
            return

        asset_channel = self._db.get("heroku.forums", "channel_id", None)
        if not asset_channel or not self._grabber_topic_id:
            await self._bot.send_message(
                chat_id, self.strings["too_large_no_premium"].format(size_mb=filesize),
                parse_mode="html",
            )
            return

        await self._log_to_topic(self.strings["log_large"].format(size_mb=filesize))
        log_chat_id = int(f"-100{asset_channel}")

        sent = await self._client.send_file(
            log_chat_id, filepath,
            caption=caption, parse_mode="html",
            force_document=False, supports_streaming=(not is_audio),
            attributes=attrs, reply_to=self._grabber_topic_id,
            progress_callback=cb,
        )

        from telethon.tl.types import InputDocument
        bot_msg = await self._bot.get_messages(log_chat_id, ids=sent.id)
        doc = bot_msg.media.document
        input_doc = InputDocument(
            id=doc.id,
            access_hash=doc.access_hash,
            file_reference=doc.file_reference,
        )
        await self._bot.send_file(
            chat_id, input_doc,
            caption=caption, parse_mode="html",
            force_document=False, supports_streaming=(not is_audio),
            attributes=attrs,
        )

    async def _process_download(
        self,
        task_id,
        chat_id,
        msg_id,
        url,
        mode,
        workdir,
        info,
        meta_dict=None,
        reply_to_topic=None,
        orig_msg_id=None,
    ):
        try:
            import yt_dlp
        except ImportError:
            raise Exception("yt-dlp not installed")

        if not self._bot or not self._running:
            return

        title = (info.get("title") or "media")[:100]
        orig_width = info.get("width", 0) or 0
        orig_height = info.get("height", 0) or 0
        orig_duration = info.get("duration", 0) or 0
        is_audio = mode == "mp3"

        task_start = time.time()
        if task_id not in self._current_downloads:
            dl = self._empty_progress()
            self._current_downloads[task_id] = dl
        else:
            dl = self._current_downloads[task_id]
        dl.update({
            "active": True, "title": title,
            "start_time": task_start, "stage": "init",
            "is_audio_only": is_audio,
        })
        if not dl.get("og_url"):
            dl["og_url"] = self._og_cache.get(url)

        await self._log_to_topic(self.strings["log_processing"].format(
            title=_escape_html(title[:60]),
            user=str(chat_id),
            url=url[:120],
        ))
        await self._edit_progress(task_id, chat_id, msg_id)

        safe_title = re.sub(r'[\\/*?:"<>|]', "", title).replace(" ", "_")[:50]
        loop = asyncio.get_event_loop()
        base_opts = {"quiet": True, "no_warnings": True, "restrictfilenames": True}
        if os.path.exists(self._cookie_path):
            base_opts["cookiefile"] = self._cookie_path

        if is_audio:
            out_tmpl = os.path.join(workdir, f"{safe_title}.%(ext)s")
            dl_opts = {
                **base_opts,
                "outtmpl": out_tmpl,
                "format": "bestaudio/best",
                "progress_hooks": [self._make_progress_hook(task_id)],
                "postprocessor_hooks": [self._make_pp_hook(task_id)],
                "postprocessors": [{"key": "FFmpegExtractAudio",
                                    "preferredcodec": "mp3", "preferredquality": "320"}],
            }
            done_event = asyncio.Event()
            dl_error = [None]

            def do_audio():
                try:
                    with yt_dlp.YoutubeDL(dl_opts) as ydl:
                        ydl.download([url])
                except Exception as e:
                    dl_error[0] = e
                finally:
                    loop.call_soon_threadsafe(done_event.set)

            fut = loop.run_in_executor(self._executor, do_audio)
            up_task = asyncio.create_task(self._progress_loop(task_id, chat_id, msg_id, done_event))
            await done_event.wait()
            await fut
            up_task.cancel()
            try:
                await up_task
            except Exception:
                pass
            if dl_error[0]:
                raise dl_error[0]

        else:
            height = int(mode)
            is_combined = self._is_combined_format(info, height)
            dl["is_combined"] = is_combined

            if is_combined:
                dl["audio_done"] = True
                dl["audio_percent"] = 100.0
                out_tmpl = os.path.join(workdir, f"{safe_title}.%(ext)s")
                dl_opts = {
                    **base_opts,
                    "outtmpl": out_tmpl,
                    "format": self._build_format_string(height),
                    "progress_hooks": [self._make_progress_hook(task_id)],
                    "postprocessor_hooks": [self._make_pp_hook(task_id)],
                    "postprocessors": [{"key": "FFmpegVideoConvertor", "preferedformat": "mp4"}],
                }
                done_event = asyncio.Event()
                dl_error = [None]

                def do_combined():
                    try:
                        with yt_dlp.YoutubeDL(dl_opts) as ydl:
                            ydl.download([url])
                    except Exception as e:
                        dl_error[0] = e
                    finally:
                        loop.call_soon_threadsafe(done_event.set)

                fut = loop.run_in_executor(self._executor, do_combined)
                up_task = asyncio.create_task(self._progress_loop(task_id, chat_id, msg_id, done_event))
                await done_event.wait()
                await fut
                up_task.cancel()
                try:
                    await up_task
                except Exception:
                    pass
                if dl_error[0]:
                    raise dl_error[0]

            else:
                video_out = os.path.join(workdir, f"{safe_title}_video.%(ext)s")
                video_opts = {
                    **base_opts,
                    "outtmpl": video_out,
                    "format": self._build_video_only_format(height),
                    "progress_hooks": [self._make_progress_hook(task_id)],
                }
                done_v = asyncio.Event()
                dl_error = [None]

                def do_video():
                    try:
                        with yt_dlp.YoutubeDL(video_opts) as ydl:
                            ydl.download([url])
                    except Exception as e:
                        dl_error[0] = e
                    finally:
                        loop.call_soon_threadsafe(done_v.set)

                fut_v = loop.run_in_executor(self._executor, do_video)
                up_task_v = asyncio.create_task(self._progress_loop(task_id, chat_id, msg_id, done_v))
                await done_v.wait()
                await fut_v
                up_task_v.cancel()
                try:
                    await up_task_v
                except Exception:
                    pass
                if dl_error[0]:
                    raise dl_error[0]

                dl["video_done"] = True
                dl["video_percent"] = 100.0

                audio_out = os.path.join(workdir, f"{safe_title}_audio.%(ext)s")
                audio_opts = {
                    **base_opts,
                    "outtmpl": audio_out,
                    "format": "bestaudio[ext=m4a]/bestaudio",
                    "progress_hooks": [self._make_progress_hook(task_id)],
                }
                done_a = asyncio.Event()
                dl_error_a = [None]

                def do_audio_sep():
                    try:
                        with yt_dlp.YoutubeDL(audio_opts) as ydl:
                            ydl.download([url])
                    except Exception as e:
                        dl_error_a[0] = e
                    finally:
                        loop.call_soon_threadsafe(done_a.set)

                fut_a = loop.run_in_executor(self._executor, do_audio_sep)
                up_task_a = asyncio.create_task(self._progress_loop(task_id, chat_id, msg_id, done_a))
                await done_a.wait()
                await fut_a
                up_task_a.cancel()
                try:
                    await up_task_a
                except Exception:
                    pass

                dl["audio_done"] = True
                dl["audio_percent"] = 100.0

                video_files = [
                    f for f in os.listdir(workdir)
                    if f.startswith(f"{safe_title}_video") and os.path.isfile(os.path.join(workdir, f))
                ]
                audio_files = [
                    f for f in os.listdir(workdir)
                    if f.startswith(f"{safe_title}_audio") and os.path.isfile(os.path.join(workdir, f))
                ]
                if not video_files:
                    raise Exception("Video file not found after download")

                vf = os.path.join(workdir, video_files[0])

                if audio_files and not dl_error_a[0]:
                    af = os.path.join(workdir, audio_files[0])
                    merged = os.path.join(workdir, f"{safe_title}_merged.mp4")
                    dl["stage"] = "ffmpeg"
                    dl["ffmpeg_active"] = True
                    await self._edit_progress(task_id, chat_id, msg_id)
                    merge_cmd = [
                        "ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
                        "-i", vf, "-i", af, "-c:v", "copy", "-c:a", "aac", merged,
                    ]
                    proc = await asyncio.create_subprocess_exec(
                        *merge_cmd,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE,
                    )
                    await proc.communicate()
                    dl["ffmpeg_active"] = False
                    dl["ffmpeg_done"] = True
                    if proc.returncode == 0 and os.path.exists(merged) and os.path.getsize(merged) > 0:
                        try:
                            os.remove(vf)
                            os.remove(af)
                        except Exception:
                            pass
                        final_path = merged
                    else:
                        final_path = vf
                else:
                    dl["ffmpeg_done"] = True
                    final_path = vf

                filesize = os.path.getsize(final_path) / (1024 * 1024)
                dl["final_size"] = filesize
                dl["stage"] = "probe"
                await self._edit_progress(task_id, chat_id, msg_id)

                probe = self._get_video_info_ffprobe(final_path)
                real_width = probe["width"] if probe else orig_width
                real_height = probe["height"] if probe else orig_height
                real_duration = probe["duration"] if probe and probe["duration"] > 0 else orig_duration

                dl["stage"] = "upload"
                dl["upload_started"] = True
                upload_start = time.time()
                await self._edit_progress(task_id, chat_id, msg_id)

                caption = self.strings["file_caption"].format(
                    title=_escape_html(title[:120]),
                    size_mb=filesize, width=real_width, height=real_height, elapsed=_fmt_elapsed(time.time()-task_start),
                )
                fname = os.path.basename(final_path)
                attrs = [
                    DocumentAttributeFilename(file_name=fname),
                    DocumentAttributeVideo(
                        duration=int(real_duration), w=int(real_width), h=int(real_height),
                        supports_streaming=True,
                    ),
                ]

                up_loop = asyncio.create_task(self._upload_progress_loop(task_id, chat_id, msg_id, upload_start))
                next_task = asyncio.ensure_future(self._prefetch_next())
                try:
                    await self._send_or_forward(
                        task_id, chat_id, final_path, caption, attrs,
                        filesize, reply_to_topic, False,
                    )
                finally:
                    up_loop.cancel()
                    try:
                        await up_loop
                    except Exception:
                        pass

                await self._log_to_topic(self.strings["log_done"].format(
                    title=_escape_html(title[:60]), size_mb=filesize))
                upload_elapsed = time.time() - upload_start
                dl["upload_elapsed"] = upload_elapsed
                dl["stage"] = "done"
                await self._edit_progress(task_id, chat_id, msg_id)
                await asyncio.sleep(3)
                await self._safe_delete(chat_id, msg_id)
                await self._try_delete_orig(chat_id, orig_msg_id)
                return

        extensions = (
            (".mp3", ".m4a", ".opus", ".ogg", ".wav")
            if is_audio
            else (".mp4", ".mkv", ".webm", ".mov", ".avi")
        )
        all_files = os.listdir(workdir)
        files = [f for f in all_files if f.endswith(extensions) and os.path.isfile(os.path.join(workdir, f))]
        if not files:
            raise Exception(f"File not found after download. Files: {all_files}")

        filepath = os.path.join(workdir, files[0])
        filesize = os.path.getsize(filepath) / (1024 * 1024)
        dl["final_size"] = filesize
        dl["ffmpeg_done"] = True
        dl["video_done"] = True
        dl["audio_done"] = True

        send_thumb = None
        final_title = title
        final_artist = info.get("uploader", "Unknown") or "Unknown"

        if is_audio and meta_dict:
            if meta_dict.get("title"):
                final_title = meta_dict["title"]
            if meta_dict.get("artist"):
                final_artist = meta_dict["artist"]
            if meta_dict.get("thumb_path") and os.path.exists(meta_dict["thumb_path"]):
                thumb_path = meta_dict["thumb_path"]
                try:
                    with open(thumb_path, "rb") as f:
                        raw_thumb = f.read()
                    norm = normalize_cover(raw_thumb, force_jpeg=True)
                    if norm:
                        norm_path = os.path.join(workdir, "norm_cover.jpg")
                        with open(norm_path, "wb") as f:
                            f.write(norm)
                        thumb_path = norm_path
                except Exception:
                    pass
                temp_out = os.path.join(workdir, "with_cover.mp3")
                ok = await _embed_cover_ffmpeg(filepath, thumb_path, temp_out)
                if ok and os.path.exists(temp_out):
                    os.remove(filepath)
                    os.rename(temp_out, filepath)
                    filesize = os.path.getsize(filepath) / (1024 * 1024)
                send_thumb = thumb_path
            elif meta_dict.get("use_yt_thumb"):
                thumb_url = _get_best_thumb_url(info)
                if thumb_url:
                    raw = await _download_url(thumb_url, timeout=15)
                    if raw:
                        cover_data = normalize_cover(raw, force_jpeg=True)
                        if cover_data:
                            thumb_path = os.path.join(workdir, "yt_cover.jpg")
                            with open(thumb_path, "wb") as f:
                                f.write(cover_data)
                            temp_out = os.path.join(workdir, "with_yt_cover.mp3")
                            ok = await _embed_cover_ffmpeg(filepath, thumb_path, temp_out)
                            if ok and os.path.exists(temp_out):
                                os.remove(filepath)
                                os.rename(temp_out, filepath)
                                filesize = os.path.getsize(filepath) / (1024 * 1024)
                            send_thumb = thumb_path

        dl["stage"] = "probe"
        await self._edit_progress(task_id, chat_id, msg_id)

        real_width, real_height, real_duration = orig_width, orig_height, orig_duration
        if not is_audio:
            probe = self._get_video_info_ffprobe(filepath)
            if probe:
                real_width = probe["width"]
                real_height = probe["height"]
                if probe["duration"] > 0:
                    real_duration = probe["duration"]
        else:
            probe = self._get_audio_info_ffprobe(filepath)
            if probe and probe["duration"] > 0:
                real_duration = probe["duration"]

        dl["stage"] = "upload"
        dl["upload_started"] = True
        upload_start = time.time()
        await self._edit_progress(task_id, chat_id, msg_id)

        fname = os.path.basename(filepath)
        attrs = [DocumentAttributeFilename(file_name=fname)]
        if not is_audio:
            attrs.append(DocumentAttributeVideo(
                duration=int(real_duration), w=int(real_width), h=int(real_height),
                supports_streaming=True,
            ))
            caption = self.strings["file_caption"].format(
                title=_escape_html(final_title[:120]),
                size_mb=filesize, width=real_width, height=real_height, elapsed=_fmt_elapsed(time.time()-task_start),
            )
        else:
            attrs.append(DocumentAttributeAudio(
                duration=int(real_duration),
                title=final_title[:64],
                performer=final_artist[:64],
            ))
            caption = self.strings["audio_caption"].format(
                title=_escape_html(final_title[:120]),
                size_mb=filesize,
            )

        up_loop = asyncio.create_task(self._upload_progress_loop(task_id, chat_id, msg_id, upload_start))
        asyncio.ensure_future(self._prefetch_next())
        try:
            await self._send_or_forward(
                task_id, chat_id, filepath, caption, attrs,
                filesize, reply_to_topic, is_audio, send_thumb=send_thumb,
            )
        finally:
            up_loop.cancel()
            try:
                await up_loop
            except Exception:
                pass

        await self._log_to_topic(self.strings["log_done"].format(
            title=_escape_html(title[:60]), size_mb=filesize, elapsed=_fmt_elapsed(time.time()-task_start)))
        upload_elapsed = time.time() - upload_start
        dl["upload_elapsed"] = upload_elapsed
        dl["stage"] = "done"
        await self._edit_progress(task_id, chat_id, msg_id)
        await asyncio.sleep(3)
        await self._safe_delete(chat_id, msg_id)
        await self._try_delete_orig(chat_id, orig_msg_id)

    async def _prefetch_next(self):
        pass

    def _get_video_info_ffprobe(self, filepath):
        try:
            import json as _json
            import subprocess as _sp
            cmd = [
                "ffprobe", "-v", "error", "-select_streams", "v:0",
                "-show_entries", "stream=width,height,duration,codec_name",
                "-of", "json", filepath,
            ]
            result = _sp.run(cmd, capture_output=True, text=True, timeout=30)
            if result.returncode == 0:
                streams = _json.loads(result.stdout).get("streams", [])
                if streams:
                    s = streams[0]
                    return {
                        "width": int(s.get("width", 0)),
                        "height": int(s.get("height", 0)),
                        "duration": float(s.get("duration", 0)) if s.get("duration") else 0,
                        "codec": s.get("codec_name", "unknown"),
                    }
        except Exception:
            pass
        return None

    def _get_audio_info_ffprobe(self, filepath):
        try:
            import json as _json
            import subprocess as _sp
            cmd = [
                "ffprobe", "-v", "error", "-select_streams", "a:0",
                "-show_entries", "stream=duration,codec_name",
                "-of", "json", filepath,
            ]
            result = _sp.run(cmd, capture_output=True, text=True, timeout=30)
            if result.returncode == 0:
                streams = _json.loads(result.stdout).get("streams", [])
                if streams:
                    s = streams[0]
                    return {
                        "duration": float(s.get("duration", 0)) if s.get("duration") else 0,
                        "codec": s.get("codec_name", "unknown"),
                    }
        except Exception:
            pass
        return None

    async def on_unload(self):
        logger.info("[GRABBER] Unloading...")
        await self._shutdown()
        await asyncio.sleep(0.5)
        self._clean_cache()
        try:
            self._executor.shutdown(wait=False)
        except Exception:
            pass
        logger.info("[GRABBER] Unloaded")