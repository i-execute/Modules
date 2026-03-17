__version__ = (2, 0, 0)
# meta developer: FireJester.t.me 

import os
import io
import asyncio
import logging
import shutil
import time
import re
import threading
import subprocess
import concurrent.futures
from telethon import TelegramClient, events, Button
from telethon.tl.types import DocumentAttributeVideo, DocumentAttributeAudio, DocumentAttributeFilename
from telethon.tl.functions.messages import SendReactionRequest
from telethon.tl.types import ReactionEmoji
from telethon.errors import MessageNotModifiedError, FloodWaitError
from .. import loader, utils

try:
    import cryptg
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
_BOT_TOKEN_PATTERN = re.compile(r'\b\d{8,10}:[A-Za-z0-9_-]{35}\b')


def normalize_cover(raw_data, max_size=None, force_jpeg=False):

    if not raw_data or len(raw_data) < 100:
        return None
    if not Image:
        return raw_data
    try:
        img = Image.open(io.BytesIO(raw_data))
        w, h = img.size
        needs_resize = max_size is not None and (w > max_size or h > max_size)
        if force_jpeg:
            img = img.convert("RGB")
            if needs_resize:
                ratio = min(max_size / w, max_size / h)
                img = img.resize((int(w * ratio), int(h * ratio)), Image.LANCZOS)
            buf = io.BytesIO()
            img.save(buf, format="JPEG", quality=95)
            result = buf.getvalue()
            return result if len(result) >= 100 else None
        is_png = raw_data[:8] == b'\x89PNG\r\n\x1a\n'
        if is_png and not needs_resize:
            return raw_data
        img = img.convert("RGB")
        if needs_resize:
            ratio = min(max_size / w, max_size / h)
            img = img.resize((int(w * ratio), int(h * ratio)), Image.LANCZOS)
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=95)
        result = buf.getvalue()
        return result if len(result) >= 100 else None
    except Exception:
        return raw_data


async def download_thumbnail_hq(url):

    if not url or not aiohttp:
        return None
    try:
        async with aiohttp.ClientSession() as sess:
            async with sess.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    return None
                data = await resp.read()
                return data if len(data) >= 1000 else None
    except Exception:
        return None


def get_best_thumbnail_url(info):

    if not info:
        return None

    thumbnails = info.get('thumbnails')
    if thumbnails and isinstance(thumbnails, list):
        best_url = None
        best_preference = -9999
        best_resolution = 0
        for t in thumbnails:
            url = t.get('url')
            if not url:
                continue
            w = t.get('width') or 0
            h = t.get('height') or 0
            pref = t.get('preference') or 0
            resolution = w * h
            if pref > best_preference or (pref == best_preference and resolution > best_resolution):
                best_preference = pref
                best_resolution = resolution
                best_url = url
        if best_url:
            return best_url

    return info.get('thumbnail') or None


class SafeList:
    def __init__(self):
        self._list = []
        self._lock = threading.Lock()
    
    def append(self, item):
        with self._lock:
            self._list.append(item)
    
    def pop_first(self):
        with self._lock:
            if self._list:
                return self._list.pop(0)
            return None
    
    def clear(self):
        with self._lock:
            self._list.clear()
    
    def __len__(self):
        with self._lock:
            return len(self._list)
    
    def to_list(self):
        with self._lock:
            return self._list.copy()


class SafeSet:
    def __init__(self):
        self._set = set()
        self._lock = threading.Lock()
    
    def add(self, item) -> bool:
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


@loader.tds
class Grabber(loader.Module):
    """Universal media downloader via bot"""

    strings = {
        "name": "Grabber",
    }

    strings_en = {
        "line": "----------------",
        
        "btn_video": "Video",
        "btn_audio": "Audio (MP3)",
        "btn_cancel": "Cancel",
        "btn_back": "Back",
        
        "btn_orig_thumb": "With preview",
        "btn_orig_clean": "Without preview",
        "btn_custom": "Editor",
        "btn_no_cover": "No cover",
        "btn_confirm": "Download",
        "btn_retry": "Retry",
        
        "no_token": "<b>Token not specified!</b>\nUse: <code>{prefix}grabber token [token]</code>",
        "token_stored": "<b>Token saved!</b>\nStart: <code>{prefix}grabber on</code>",
        "need_token": "<b>Specify token or reply to message with it!</b>",
        "already_running": "<b>Already running!</b>",
        "not_running": "<b>Bot not running!</b>",
        "started": "<b>Grabber started!</b>",
        "stopped": "<b>Grabber stopped!</b>",
        "start_failed": "<b>Start error:</b>\n<code>{error}</code>",
        
        "reboot_start": "<b>REBOOT...</b>",
        "reboot_done": "<b>Reboot complete, bot restarted!</b>",
        "reboot_no_token": "<b>No token for restart!</b>",
        "clear_done": "<b>Factory reset complete!</b>\nToken, cookies, cache - all deleted.",
        
        "cookie_saved": "<b>Cookies saved!</b>",
        "cookie_cleared": "<b>Cookies deleted!</b>",
        "cookie_empty": "<b>Cookies not set.</b>",
        "cookie_ok": "<b>Cookies installed</b>",
        "no_reply_file": "<b>Reply to .txt file with cookies!</b>",
        "invalid_ext": "<b>File must have .txt extension!</b>",
        "cookie_err": "<b>Cookie save error:</b>\n<code>{}</code>",
        "cookie_invalid_format": "<b>Invalid cookie format! Expected Netscape format.</b>",
        
        "topic_enabled": "<b>Bot enabled in this topic!</b>",
        "topic_disabled": "<b>Bot disabled in this topic!</b>",
        "topic_not_active": "<b>This topic is not active! First send /start in this topic.</b>",
        "topic_not_in_group": "<b>This command only works in groups!</b>",
        "topic_not_forum": "<b>This group doesn't have topics enabled!</b>",
        "topic_usage": "<b>Usage:</b> <code>{prefix}grabber topic on/off</code>",
        
        "deps_processing": "<b>Installing dependencies...</b>",
        "deps_result": "<b>Dependencies check:</b>\n\n{results}",
        
        "logs_empty": "<b>No download logs yet.</b>",
        "logs_generated": "<b>Last {count} downloads log.</b>",
        
        "usage": (
            "<b>Grabber - Universal media downloader</b>\n\n"
            "<code>{prefix}grabber on</code> - Start\n"
            "<code>{prefix}grabber off</code> - Stop\n"
            "<code>{prefix}grabber token [token]</code> - Set bot token\n"
            "<code>{prefix}grabber start</code> - Install dependencies\n"
            "<code>{prefix}grabber reboot</code> - Clear cache and restart\n"
            "<code>{prefix}grabber clear</code> - Factory reset\n"
            "<code>{prefix}grabber cookies add</code> - Add cookies (reply to .txt)\n"
            "<code>{prefix}grabber cookies remove</code> - Remove cookies\n"
            "<code>{prefix}grabber topic on/off</code> - Enable/disable bot in topic\n"
            "<code>{prefix}grabber status</code> - Status\n"
            "<code>{prefix}grabber process</code> - Current process\n"
            "<code>{prefix}grabber logs</code> - Download logs\n"
        ),
        
        "status_template": (
            "<b>Grabber Status</b>\n\n"
            "Status: {status}\n"
            "Cookies: {cookies}\n"
            "In queue: {pending}\n"
            "Active: {active}\n"
            "Completed: {completed}\n"
            "Errors: {errors}"
        ),
        "status_running": "Running",
        "status_stopped": "Stopped",
        
        "no_active_process": "<b>No active process</b>",
        "analyzing": "<b>Analyzing...</b>",
        "found_media": "<b>{title}</b>\n\nDuration: {duration}\nSelect format:",
        "found_media_group": "<b>{title}</b>\n\nDuration: {duration}",
        "grab_failed": "<b>Error:</b>\n<code>{error}</code>",
        "no_link": "<b>No link found!</b>",
        "queue_pos": "<b>Queue: #{pos}</b>",
        "starting": "<b>Starting download!</b>",
        "too_large": "<b>File too large!</b>\nMax: {max_mb}MB, File: {size_mb:.1f}MB",
        "cancelled": "<b>Cancelled</b>",
        "already_processing": "Already processing!",
        
        "hello": "<b>Hello, {user_link}!</b>\n\nSend video link!",
        "hello_fallback": "<b>Hello!</b>\n\nSend video link!",
        "hello_group": "<b>Bot activated in this group!</b>\n\nSend video links - I will offer to download.",
        "hello_topic": "<b>Bot activated in this topic!</b>\n\nSend video links - I will offer to download.",
        
        "file_caption": "<b>{title}</b>\n{size_mb:.1f} MB | {width}x{height}",
        "audio_caption": "<b>{title}</b>\n{size_mb:.1f} MB",
        
        "progress_header": "<b>Downloading...</b>\n",
        "progress_title": "<code>{title}</code>",
        
        "video_waiting": "Video: waiting...",
        "video_done": "Video: done {size:.1f} MB",
        "video_progress": "Video: {pct:.1f}% ({size:.1f}/{total:.1f} MB) | {speed}",
        
        "audio_waiting": "Audio: waiting...",
        "audio_done": "Audio: done {size:.1f} MB",
        "audio_progress": "Audio: {pct:.1f}% ({size:.1f}/{total:.1f} MB) | {speed}",
        
        "merge_waiting": "Merge: waiting...",
        "merge_done": "Merge: done {size:.1f} MB",
        "merge_active": "Merge: FFmpeg working...",
        "merge_starting": "Merge: starting...",
        
        "convert_waiting": "Convert: waiting...",
        "convert_done": "Convert: done {size:.1f} MB",
        "convert_active": "Convert: FFmpeg...",
        
        "upload_waiting": "Telegram: waiting...",
        "upload_done": "Telegram: sent!",
        "upload_progress": "Telegram: {elapsed:.1f}s",
        "upload_success": "Telegram: {elapsed:.1f}s",
        
        "stage_init": "Initializing...",
        "stage_video": "Downloading video...",
        "stage_audio": "Downloading audio...",
        "stage_ffmpeg": "FFmpeg processing...",
        "stage_probe": "Analyzing file...",
        "stage_upload": "Uploading to Telegram...",
        "stage_done": "Done!",
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
        
        "group_added": "<b>Group added to monitoring.</b>",
        
        "react_ok": "\U0001F44C",
        "react_fail": "\U0001F971",
        
        "quality_menu": "<b>Select quality:</b>",
    }

    strings_ru = {
        "line": "----------------",
        
        "btn_video": "Видео",
        "btn_audio": "Аудио (MP3)",
        "btn_cancel": "Отмена",
        "btn_back": "Назад",
        
        "btn_orig_thumb": "С превью",
        "btn_orig_clean": "Без превью",
        "btn_custom": "Редактор",
        "btn_no_cover": "Без обложки",
        "btn_confirm": "Скачать",
        "btn_retry": "Заново",
        
        "no_token": "<b>Токен не указан!</b>\nИспользуй: <code>{prefix}grabber token [токен]</code>",
        "token_stored": "<b>Токен сохранён!</b>\nЗапуск: <code>{prefix}grabber on</code>",
        "need_token": "<b>Укажи токен или ответь на сообщение с ним!</b>",
        "already_running": "<b>Уже запущен!</b>",
        "not_running": "<b>Бот не запущен!</b>",
        "started": "<b>Grabber запущен!</b>",
        "stopped": "<b>Grabber остановлен!</b>",
        "start_failed": "<b>Ошибка запуска:</b>\n<code>{error}</code>",
        
        "reboot_start": "<b>ПЕРЕЗАГРУЗКА...</b>",
        "reboot_done": "<b>Перезагрузка завершена, бот перезапущен!</b>",
        "reboot_no_token": "<b>Нет токена для перезапуска!</b>",
        "clear_done": "<b>Сброс к заводским настройкам!</b>\nТокен, куки, кэш - всё удалено.",
        
        "cookie_saved": "<b>Куки сохранены!</b>",
        "cookie_cleared": "<b>Куки удалены!</b>",
        "cookie_empty": "<b>Куки не установлены.</b>",
        "cookie_ok": "<b>Куки установлены</b>",
        "no_reply_file": "<b>Ответь на .txt файл с куки!</b>",
        "invalid_ext": "<b>Файл должен иметь расширение .txt!</b>",
        "cookie_err": "<b>Ошибка сохранения куки:</b>\n<code>{}</code>",
        "cookie_invalid_format": "<b>Неверный формат куки! Ожидается формат Netscape.</b>",
        
        "topic_enabled": "<b>Бот включён в этом топике!</b>",
        "topic_disabled": "<b>Бот отключён в этом топике!</b>",
        "topic_not_active": "<b>Этот топик не активен! Сначала отправьте /start в этом топике.</b>",
        "topic_not_in_group": "<b>Эта команда работает только в группах!</b>",
        "topic_not_forum": "<b>В этой группе не включены топики!</b>",
        "topic_usage": "<b>Использование:</b> <code>{prefix}grabber topic on/off</code>",
        
        "deps_processing": "<b>Установка зависимостей...</b>",
        "deps_result": "<b>Проверка зависимостей:</b>\n\n{results}",
        
        "logs_empty": "<b>Логов скачиваний пока нет.</b>",
        "logs_generated": "<b>Лог последних {count} скачиваний.</b>",
        
        "usage": (
            "<b>Grabber - Универсальный загрузчик медиа</b>\n\n"
            "<code>{prefix}grabber on</code> - Запуск\n"
            "<code>{prefix}grabber off</code> - Остановка\n"
            "<code>{prefix}grabber token [токен]</code> - Установить токен бота\n"
            "<code>{prefix}grabber start</code> - Установить зависимости\n"
            "<code>{prefix}grabber reboot</code> - Очистить кэш и перезапустить\n"
            "<code>{prefix}grabber clear</code> - Сброс к заводским\n"
            "<code>{prefix}grabber cookies add</code> - Добавить куки (реплай на .txt)\n"
            "<code>{prefix}grabber cookies remove</code> - Удалить куки\n"
            "<code>{prefix}grabber topic on/off</code> - Вкл/выкл бота в топике\n"
            "<code>{prefix}grabber status</code> - Статус\n"
            "<code>{prefix}grabber process</code> - Текущий процесс\n"
            "<code>{prefix}grabber logs</code> - Логи скачиваний\n"
        ),
        
        "status_template": (
            "<b>Статус Grabber</b>\n\n"
            "Статус: {status}\n"
            "Куки: {cookies}\n"
            "В очереди: {pending}\n"
            "Активно: {active}\n"
            "Завершено: {completed}\n"
            "Ошибки: {errors}"
        ),
        "status_running": "Работает",
        "status_stopped": "Остановлен",
        
        "no_active_process": "<b>Нет активного процесса</b>",
        "analyzing": "<b>Анализ...</b>",
        "found_media": "<b>{title}</b>\n\nДлительность: {duration}\nВыберите формат:",
        "found_media_group": "<b>{title}</b>\n\nДлительность: {duration}",
        "grab_failed": "<b>Ошибка:</b>\n<code>{error}</code>",
        "no_link": "<b>Ссылка не найдена!</b>",
        "queue_pos": "<b>Очередь: #{pos}</b>",
        "starting": "<b>Начинаю загрузку!</b>",
        "too_large": "<b>Файл слишком большой!</b>\nМакс: {max_mb}MB, Файл: {size_mb:.1f}MB",
        "cancelled": "<b>Отменено</b>",
        "already_processing": "Уже обрабатывается!",
        
        "hello": "<b>Привет, {user_link}!</b>\n\nОтправь ссылку на видео!",
        "hello_fallback": "<b>Привет!</b>\n\nОтправь ссылку на видео!",
        "hello_group": "<b>Бот активирован в этой группе!</b>\n\nОтправляйте ссылки на видео - я предложу скачать.",
        "hello_topic": "<b>Бот активирован в этом топике!</b>\n\nОтправляйте ссылки на видео - я предложу скачать.",
        
        "file_caption": "<b>{title}</b>\n{size_mb:.1f} MB | {width}x{height}",
        "audio_caption": "<b>{title}</b>\n{size_mb:.1f} MB",
        
        "progress_header": "<b>Загрузка...</b>\n",
        "progress_title": "<code>{title}</code>",
        
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
        "upload_done": "Telegram: отправлено!",
        "upload_progress": "Telegram: {elapsed:.1f}с",
        "upload_success": "Telegram: {elapsed:.1f}с",
        
        "stage_init": "Инициализация...",
        "stage_video": "Загрузка видео...",
        "stage_audio": "Загрузка аудио...",
        "stage_ffmpeg": "Обработка FFmpeg...",
        "stage_probe": "Анализ файла...",
        "stage_upload": "Загрузка в Telegram...",
        "stage_done": "Готово!",
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
        
        "group_added": "<b>Группа добавлена в мониторинг.</b>",
        
        "react_ok": "\U0001F44C",
        "react_fail": "\U0001F971",
        
        "quality_menu": "<b>Выберите качество:</b>",
    }

    def __init__(self):
        self._bot = None
        self._running = False
        self._download_queue = asyncio.Queue()
        self._queue_items = SafeList()
        self._processed_buttons = SafeSet()
        self._worker_task = None
        self._stats = {"completed": 0, "errors": 0}
        
        self._root = None
        self._cache_dir = None
        self._session_dir = None
        self._cookie_dir = None
        self._cookie_path = None
        
        self._lock = asyncio.Lock()
        self._edit_lock = asyncio.Lock()
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)
        self._active_processes = []
        
        self._edit_state = {}
        self._active_groups = set()
        self._active_topics = {}
        
        self._current_download = self._empty_progress()
        self._download_logs = []

    def _empty_progress(self):
        return {
            'active': False,
            'title': '',
            'start_time': 0,
            'is_audio_only': False,
            'video_percent': 0.0,
            'video_size': 0.0,
            'video_total': 0.0,
            'video_speed': '',
            'video_done': False,
            'audio_percent': 0.0,
            'audio_size': 0.0,
            'audio_total': 0.0,
            'audio_speed': '',
            'audio_done': False,
            'audio_started': False,
            'ffmpeg_active': False,
            'ffmpeg_done': False,
            'upload_started': False,
            'upload_elapsed': 0.0,
            'final_size': 0.0,
            'stage': 'init',
        }

    def _add_download_log(self, title: str, url: str, mode: str, success: bool, error: str = None):
        log_entry = {
            'timestamp': time.strftime("%Y-%m-%d %H:%M:%S"),
            'title': title[:100],
            'url': url[:200],
            'mode': mode,
            'success': success,
            'error': error[:200] if error else None
        }
        self._download_logs.append(log_entry)
        if len(self._download_logs) > 100:
            self._download_logs = self._download_logs[-100:]

    def _build_status_message(self):
        d = self._current_download
        elapsed = self._get_elapsed()
        stage = d.get('stage', 'init')
        is_audio_only = d.get('is_audio_only', False)
        
        title = d.get('title', 'Unknown')[:60]
        
        lines = [
            self.strings["progress_header"],
            self.strings["progress_title"].format(title=title),
            self.strings["line"],
        ]
        
        if not is_audio_only:
            if stage == 'init':
                lines.append(self.strings["video_waiting"])
            elif d.get('video_done'):
                lines.append(self.strings["video_done"].format(size=d.get('video_total', 0)))
            elif stage == 'video':
                lines.append(self.strings["video_progress"].format(
                    pct=d.get('video_percent', 0),
                    size=d.get('video_size', 0),
                    total=d.get('video_total', 0),
                    speed=d.get('video_speed', 'N/A')
                ))
            else:
                lines.append(self.strings["video_waiting"])
        
        if d.get('audio_done'):
            lines.append(self.strings["audio_done"].format(size=d.get('audio_total', 0)))
        elif stage == 'audio' or d.get('audio_started'):
            lines.append(self.strings["audio_progress"].format(
                pct=d.get('audio_percent', 0),
                size=d.get('audio_size', 0),
                total=d.get('audio_total', 0),
                speed=d.get('audio_speed', 'N/A')
            ))
        else:
            lines.append(self.strings["audio_waiting"])
        
        if not is_audio_only:
            if d.get('ffmpeg_done'):
                lines.append(self.strings["merge_done"].format(size=d.get('final_size', 0)))
            elif d.get('ffmpeg_active') or stage == 'ffmpeg':
                lines.append(self.strings["merge_active"])
            elif d.get('video_done') and d.get('audio_done'):
                lines.append(self.strings["merge_starting"])
            else:
                lines.append(self.strings["merge_waiting"])
        else:
            if d.get('ffmpeg_done'):
                lines.append(self.strings["convert_done"].format(size=d.get('final_size', 0)))
            elif d.get('ffmpeg_active') or stage == 'ffmpeg':
                lines.append(self.strings["convert_active"])
            else:
                lines.append(self.strings["convert_waiting"])
        
        if stage == 'done':
            lines.append(self.strings["upload_success"].format(elapsed=d.get('upload_elapsed', 0)))
        elif stage == 'upload' or d.get('upload_started'):
            lines.append(self.strings["upload_progress"].format(elapsed=d.get('upload_elapsed', 0)))
        else:
            lines.append(self.strings["upload_waiting"])
        
        lines.append(self.strings["line"])
        
        stage_map = {
            'init': self.strings["stage_init"],
            'video': self.strings["stage_video"],
            'audio': self.strings["stage_audio"],
            'ffmpeg': self.strings["stage_ffmpeg"],
            'probe': self.strings["stage_probe"],
            'upload': self.strings["stage_upload"],
            'done': self.strings["stage_done_success"],
        }
        stage_text = stage_map.get(stage, self.strings["stage_init"])
        
        lines.append(self.strings["time_stage"].format(elapsed=elapsed, stage_text=stage_text))
        
        return "\n".join(lines)

    def _get_elapsed(self):
        start = self._current_download.get('start_time', 0)
        if start > 0:
            return time.time() - start
        return 0.0

    def _format_speed(self, speed_bytes):
        if not speed_bytes or speed_bytes == 0:
            return "..."
        if speed_bytes < 1024:
            return f"{speed_bytes:.0f} B/s"
        elif speed_bytes < 1024 * 1024:
            return f"{speed_bytes / 1024:.1f} KB/s"
        else:
            return f"{speed_bytes / 1024 / 1024:.1f} MB/s"

    def _format_duration(self, seconds):
        if not seconds:
            return "N/A"
        seconds = int(seconds)
        if seconds < 60:
            return f"{seconds}s"
        elif seconds < 3600:
            m, s = divmod(seconds, 60)
            return f"{m}m {s}s"
        else:
            h, rem = divmod(seconds, 3600)
            m, s = divmod(rem, 60)
            return f"{h}h {m}m"

    def _escape_html(self, text: str) -> str:
        if not text:
            return ""
        return text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')

    def _validate_cookies_content(self, content):
        if not content:
            return False
        lower_content = content.lower()
        if "# netscape" in lower_content or "youtube.com" in lower_content or "TRUE" in content:
            return True
        return False

    def _get_topic_id(self, event):
        reply_to = getattr(event, 'reply_to', None)
        if reply_to:
            return getattr(reply_to, 'reply_to_top_id', None) or getattr(reply_to, 'reply_to_msg_id', None)
        return None

    def _is_forum(self, chat):
        return getattr(chat, 'forum', False)

    def _save_active_topics(self):
        save_data = {str(k): list(v) for k, v in self._active_topics.items()}
        self._db.set("Grabber", "active_topics", save_data)

    def _load_active_topics(self):
        saved = self._db.get("Grabber", "active_topics", {})
        self._active_topics = {int(k): set(v) for k, v in saved.items()}

    async def _install_dependencies(self):
        deps = {
            'yt-dlp': 'yt_dlp',
            'cryptg': 'cryptg',
            'Pillow': 'PIL',
            'hachoir': 'hachoir',
            'aiohttp': 'aiohttp',
        }
        
        results = {}
        
        for pkg, import_name in deps.items():
            try:
                proc = await asyncio.create_subprocess_shell(
                    f"pip install -U {pkg}",
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                stdout, stderr = await proc.communicate()
                
                if stdout:
                    logger.warning(f"[GRABBER] pip {pkg}: {stdout.decode()}")
                if stderr:
                    logger.warning(f"[GRABBER] pip {pkg} stderr: {stderr.decode()}")
                
            except Exception as e:
                logger.warning(f"[GRABBER] Failed to install {pkg}: {e}")
        
        for pkg, import_name in deps.items():
            try:
                __import__(import_name)
                results[pkg] = "OK"
            except ImportError:
                results[pkg] = "FAIL"
        
        try:
            proc = await asyncio.create_subprocess_shell(
                "ffmpeg -version",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await proc.communicate()
            if proc.returncode == 0:
                results['ffmpeg'] = "OK"
                if stdout:
                    logger.warning(f"[GRABBER] ffmpeg: {stdout.decode()[:100]}")
            else:
                results['ffmpeg'] = "FAIL"
        except Exception:
            results['ffmpeg'] = "FAIL"
        
        try:
            proc = await asyncio.create_subprocess_shell(
                "ffprobe -version",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            await proc.communicate()
            results['ffprobe'] = "OK" if proc.returncode == 0 else "FAIL"
        except Exception:
            results['ffprobe'] = "FAIL"
        
        return results

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        
        self._root = "/tmp/grabber_data"
        self._cache_dir = os.path.join(self._root, "cache")
        self._session_dir = os.path.join(self._root, "session")
        self._cookie_dir = os.path.join(self._root, "cookies")
        self._cookie_path = os.path.join(self._cookie_dir, "cookies.txt")
        
        self._clean_cache()
        
        os.makedirs(self._cache_dir, exist_ok=True)
        os.makedirs(self._session_dir, exist_ok=True)
        os.makedirs(self._cookie_dir, exist_ok=True)
        
        self._active_groups = set(self._db.get("Grabber", "active_groups", []))
        self._load_active_topics()
        
        saved_cookies = self._db.get("Grabber", "cookies_content")
        if saved_cookies:
            try:
                with open(self._cookie_path, "w", encoding="utf-8") as f:
                    f.write(saved_cookies)
                logger.info(f"[GRABBER] Cookies restored to {self._cookie_path}")
            except Exception as e:
                logger.error(f"[GRABBER] Failed to restore cookies: {e}")
        
        await self._ensure_ytdlp()
        
        if self._db.get("Grabber", "autorun", False):
            tkn = self._db.get("Grabber", "tkn")
            if tkn:
                try:
                    await self._launch(tkn)
                    logger.info("[GRABBER] Autorun successful")
                except Exception as e:
                    logger.error(f"[GRABBER] Autorun failed: {e}")

    async def _ensure_ytdlp(self):
        proc = await asyncio.create_subprocess_shell(
            "pip install -U yt-dlp 2>/dev/null || pip install yt-dlp",
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL
        )
        await proc.wait()

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

    def _clean_workdir(self, path):
        if path and os.path.exists(path):
            try:
                shutil.rmtree(path)
            except Exception:
                pass

    def _clean_all(self):
        self._clean_cache()
        self._clean_session()
        self._clean_cookies()
        self._db.set("Grabber", "tkn", None)
        self._db.set("Grabber", "autorun", False)
        self._db.set("Grabber", "active_groups", [])
        self._db.set("Grabber", "active_topics", {})
        self._active_groups.clear()
        self._active_topics.clear()

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

    def _extract_url(self, text):
        if not text:
            return None
        match = _URL_PATTERN.search(text)
        return match.group(0) if match else None

    def _extract_token(self, text):
        if not text:
            return None
        match = _BOT_TOKEN_PATTERN.search(text)
        return match.group(0) if match else None

    def _make_workdir(self):
        name = f"job_{int(time.time() * 1000)}_{os.getpid()}"
        path = os.path.join(self._cache_dir, name)
        os.makedirs(path, exist_ok=True)
        return path

    def _save_active_groups(self):
        self._db.set("Grabber", "active_groups", list(self._active_groups))

    def _get_video_info_ffprobe(self, filepath):
        try:
            cmd = [
                'ffprobe', '-v', 'error',
                '-select_streams', 'v:0',
                '-show_entries', 'stream=width,height,duration,codec_name',
                '-of', 'json',
                filepath
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                import json
                data = json.loads(result.stdout)
                streams = data.get('streams', [])
                if streams:
                    stream = streams[0]
                    return {
                        'width': int(stream.get('width', 0)),
                        'height': int(stream.get('height', 0)),
                        'duration': float(stream.get('duration', 0)) if stream.get('duration') else 0,
                        'codec': stream.get('codec_name', 'unknown'),
                    }
        except Exception as e:
            logger.error(f"[GRABBER] ffprobe error: {e}")
        return None

    def _get_audio_info_ffprobe(self, filepath):
        try:
            cmd = [
                'ffprobe', '-v', 'error',
                '-select_streams', 'a:0',
                '-show_entries', 'stream=duration,codec_name',
                '-of', 'json',
                filepath
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                import json
                data = json.loads(result.stdout)
                streams = data.get('streams', [])
                if streams:
                    stream = streams[0]
                    return {
                        'duration': float(stream.get('duration', 0)) if stream.get('duration') else 0,
                        'codec': stream.get('codec_name', 'unknown'),
                    }
        except Exception as e:
            logger.error(f"[GRABBER] ffprobe audio error: {e}")
        return None

    def _get_available_formats(self, info):
        formats = info.get('formats', [])
        heights = set()
        
        for fmt in formats:
            h = fmt.get('height')
            vcodec = fmt.get('vcodec', 'none')
            if h and vcodec != 'none':
                heights.add(h)
        
        standard = [2160, 1440, 1080, 720, 480, 360, 240, 144]
        available = sorted([h for h in standard if h in heights], reverse=True)
        
        if not available:
            available = [720, 480, 360]
        
        return available

    def _build_format_string(self, height):
        return f'bestvideo[height<={height}][vcodec^=avc1]+bestaudio[ext=m4a]/bestvideo[height<={height}]+bestaudio/best[height<={height}][ext=mp4]/best[height<={height}]'

    def _detect_stream_type(self, d):
        info = d.get('info_dict', {})
        vcodec = info.get('vcodec', 'none') or 'none'
        acodec = info.get('acodec', 'none') or 'none'
        
        is_video_only = vcodec != 'none' and acodec == 'none'
        is_audio_only = vcodec == 'none' and acodec != 'none'
        is_combined = vcodec != 'none' and acodec != 'none'
        
        return is_video_only, is_audio_only, is_combined

    def _progress_hook(self, d):
        status = d.get('status')
        is_audio_mode = self._current_download.get('is_audio_only', False)
        
        is_video_stream, is_audio_stream, is_combined = self._detect_stream_type(d)
        
        if status == 'downloading':
            total = d.get('total_bytes') or d.get('total_bytes_estimate') or 0
            downloaded = d.get('downloaded_bytes', 0)
            speed = d.get('speed') or 0
            
            percent = (downloaded / total * 100) if total > 0 else 0
            total_mb = total / 1024 / 1024 if total > 0 else 0
            size_mb = downloaded / 1024 / 1024
            speed_str = self._format_speed(speed)
            
            self._current_download['active'] = True
            
            if is_audio_mode:
                self._current_download.update({
                    'stage': 'audio',
                    'audio_started': True,
                    'audio_percent': percent,
                    'audio_size': size_mb,
                    'audio_total': total_mb,
                    'audio_speed': speed_str,
                })
            elif is_audio_stream:
                self._current_download.update({
                    'stage': 'audio',
                    'audio_started': True,
                    'audio_percent': percent,
                    'audio_size': size_mb,
                    'audio_total': total_mb,
                    'audio_speed': speed_str,
                })
            elif is_video_stream:
                self._current_download.update({
                    'stage': 'video',
                    'video_percent': percent,
                    'video_size': size_mb,
                    'video_total': total_mb,
                    'video_speed': speed_str,
                })
            elif is_combined:
                self._current_download.update({
                    'stage': 'video',
                    'video_percent': percent,
                    'video_size': size_mb,
                    'video_total': total_mb,
                    'video_speed': speed_str,
                })
            else:
                if self._current_download.get('video_done') and not self._current_download.get('audio_done'):
                    self._current_download.update({
                        'stage': 'audio',
                        'audio_started': True,
                        'audio_percent': percent,
                        'audio_size': size_mb,
                        'audio_total': total_mb,
                        'audio_speed': speed_str,
                    })
                elif not self._current_download.get('video_done'):
                    self._current_download.update({
                        'stage': 'video',
                        'video_percent': percent,
                        'video_size': size_mb,
                        'video_total': total_mb,
                        'video_speed': speed_str,
                    })
                else:
                    self._current_download.update({
                        'stage': 'audio',
                        'audio_started': True,
                        'audio_percent': percent,
                        'audio_size': size_mb,
                        'audio_total': total_mb,
                        'audio_speed': speed_str,
                    })
            
        elif status == 'finished':
            if is_audio_mode:
                self._current_download['audio_done'] = True
                self._current_download['audio_percent'] = 100.0
            elif is_audio_stream:
                self._current_download['audio_done'] = True
                self._current_download['audio_percent'] = 100.0
            elif is_video_stream:
                self._current_download['video_done'] = True
                self._current_download['video_percent'] = 100.0
            elif is_combined:
                self._current_download['video_done'] = True
                self._current_download['video_percent'] = 100.0
                self._current_download['audio_done'] = True
                self._current_download['audio_percent'] = 100.0
            else:
                if not self._current_download.get('video_done'):
                    self._current_download['video_done'] = True
                    self._current_download['video_percent'] = 100.0
                elif not self._current_download.get('audio_done'):
                    self._current_download['audio_done'] = True
                    self._current_download['audio_percent'] = 100.0

    def _postprocessor_hook(self, d):
        status = d.get('status')
        
        if status == 'started':
            self._current_download['ffmpeg_active'] = True
            self._current_download['stage'] = 'ffmpeg'
        elif status == 'finished':
            self._current_download['ffmpeg_done'] = True
            self._current_download['ffmpeg_active'] = False

    async def _edit_message(self, chat_id: int, msg_id: int, text: str, **kwargs):
        for attempt in range(3):
            try:
                if self._bot:
                    await self._bot.edit_message(chat_id, msg_id, text, **kwargs)
                return True
            except MessageNotModifiedError:
                return True
            except FloodWaitError as e:
                await asyncio.sleep(min(e.seconds, 5))
            except Exception as e:
                if attempt < 2:
                    await asyncio.sleep(0.5)
        return False

    async def _safe_edit(self, target, text, **kwargs):
        for attempt in range(3):
            try:
                if hasattr(target, 'edit'):
                    await target.edit(text, **kwargs)
                elif self._bot and isinstance(target, tuple):
                    chat_id, msg_id = target
                    await self._bot.edit_message(chat_id, msg_id, text, **kwargs)
                return True
            except MessageNotModifiedError:
                return True
            except FloodWaitError as e:
                await asyncio.sleep(min(e.seconds, 5))
            except Exception as e:
                if attempt < 2:
                    await asyncio.sleep(0.5)
        return False

    async def _safe_delete(self, chat_id, msg_id):
        try:
            if self._bot:
                await self._bot.delete_messages(chat_id, msg_id)
        except Exception:
            pass

    async def _set_reaction(self, event, emoticon):
        try:
            peer = await event.get_input_chat()
            await self._bot(SendReactionRequest(
                peer=peer,
                msg_id=event.id,
                reaction=[ReactionEmoji(emoticon=emoticon)]
            ))
        except Exception as e:
            logger.debug(f"[GRABBER] Reaction failed: {e}")

    async def _cancel_task_safe(self, task):
        if task is None:
            return
        task.cancel()
        try:
            await task
        except BaseException:
            pass

    async def _embed_cover_ffmpeg(self, audio_path, thumb_path, output_path):
        cmd = [
            'ffmpeg', '-y',
            '-i', audio_path,
            '-i', thumb_path,
            '-map', '0:a',
            '-map', '1:0',
            '-c', 'copy',
            '-id3v2_version', '3',
            '-metadata:s:v', 'title=Album cover',
            '-metadata:s:v', 'comment=Cover (front)',
            output_path
        ]

        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            await proc.communicate()
            return proc.returncode == 0
        except Exception as e:
            logger.error(f"[GRABBER] FFmpeg cover error: {e}")
            return False

    async def _download_and_prepare_yt_cover(self, info, workdir):
        
        thumbnail_url = get_best_thumbnail_url(info)
        if not thumbnail_url:
            return None, None
        
        raw_data = await download_thumbnail_hq(thumbnail_url)
        if not raw_data:
            return None, None
        
        cover_data = normalize_cover(raw_data, force_jpeg=True)
        if not cover_data:
            return None, None
        
        thumb_path = os.path.join(workdir, "yt_cover_hq.jpg")
        try:
            with open(thumb_path, "wb") as f:
                f.write(cover_data)
        except Exception:
            return None, None
        
        if not os.path.exists(thumb_path) or os.path.getsize(thumb_path) < 100:
            return None, None
        
        return thumb_path, cover_data

    @loader.command(
        ru_doc="Управление медиа-загрузчиком",
        en_doc="Media downloader management",
    )
    async def grabber(self, message):
        """Media downloader management"""
        args = utils.get_args(message)
        cmd = args[0].lower() if args else None
        prefix = self.get_prefix()

        if not cmd:
            await utils.answer(
                message,
                self.strings["usage"].format(prefix=prefix),
            )
            return

        if cmd == "status":
            st = self.strings["status_running"] if self._running else self.strings["status_stopped"]
            cookies_st = self.strings["cookie_ok"] if os.path.exists(self._cookie_path) else self.strings["cookie_empty"]
            active = "Yes" if self._current_download.get('active') else "No"
            
            await utils.answer(message, self.strings["status_template"].format(
                status=st,
                cookies=cookies_st,
                pending=len(self._queue_items),
                active=active,
                completed=self._stats["completed"],
                errors=self._stats["errors"]
            ))

        elif cmd == "process":
            if not self._current_download.get('active'):
                await utils.answer(message, self.strings["no_active_process"])
                return
            await utils.answer(message, self._build_status_message(), parse_mode='html')

        elif cmd == "token":
            token = None
            if len(args) >= 2:
                token = self._extract_token(args[1])
            if not token:
                reply = await message.get_reply_message()
                if reply and reply.text:
                    token = self._extract_token(reply.text)
            if not token:
                await utils.answer(message, self.strings["need_token"])
                return
            self._db.set("Grabber", "tkn", token)
            await utils.answer(
                message,
                self.strings["token_stored"].format(prefix=prefix),
            )

        elif cmd == "start":
            msg = await utils.answer(message, self.strings["deps_processing"])
            
            results = await self._install_dependencies()
            
            results_text = "\n".join([f"<code>{pkg}</code>: <b>{status}</b>" for pkg, status in results.items()])
            
            if isinstance(msg, list):
                msg = msg[0]
            await self._safe_edit(msg, self.strings["deps_result"].format(results=results_text), parse_mode='html')

        elif cmd == "logs":
            if not self._download_logs:
                await utils.answer(message, self.strings["logs_empty"])
                return
            
            logs_to_show = self._download_logs[-30:]
            
            content_lines = ["Grabber Download Logs", "=" * 50, ""]
            
            for i, log in enumerate(logs_to_show, 1):
                status = "SUCCESS" if log['success'] else "FAILED"
                content_lines.append(f"#{i} [{log['timestamp']}]")
                content_lines.append(f"Title: {log['title']}")
                content_lines.append(f"URL: {log['url']}")
                content_lines.append(f"Mode: {log['mode']}")
                content_lines.append(f"Status: {status}")
                if log.get('error'):
                    content_lines.append(f"Error: {log['error']}")
                content_lines.append("-" * 30)
                content_lines.append("")
            
            content = "\n".join(content_lines)
            
            file_path = os.path.join(self._cache_dir, "Grabber_logs.txt")
            try:
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(content)
                
                await self._client.send_file(
                    message.chat_id,
                    file_path,
                    caption=self.strings["logs_generated"].format(count=len(logs_to_show)),
                    parse_mode='html'
                )
            finally:
                if os.path.exists(file_path):
                    os.remove(file_path)

        elif cmd == "on":
            if self._running:
                await utils.answer(message, self.strings["already_running"])
                return
            tkn = self._db.get("Grabber", "tkn")
            if not tkn:
                await utils.answer(
                    message,
                    self.strings["no_token"].format(prefix=prefix),
                )
                return
            try:
                await self._launch(tkn)
                self._db.set("Grabber", "autorun", True)
                await utils.answer(message, self.strings["started"])
            except Exception as e:
                logger.exception("[GRABBER] Launch failed")
                await utils.answer(message, self.strings["start_failed"].format(error=str(e)[:200]))

        elif cmd == "off":
            if not self._running:
                await utils.answer(message, self.strings["not_running"])
                return
            await self._shutdown()
            self._db.set("Grabber", "autorun", False)
            await utils.answer(message, self.strings["stopped"])

        elif cmd == "reboot":
            msg = await utils.answer(message, self.strings["reboot_start"])
            
            await self._shutdown()
            self._clean_cache()
            self._clean_session()
            
            os.makedirs(self._cache_dir, exist_ok=True)
            os.makedirs(self._session_dir, exist_ok=True)
            
            self._stats = {"completed": 0, "errors": 0}
            self._queue_items.clear()
            self._processed_buttons.clear()
            self._edit_state.clear()
            self._download_queue = asyncio.Queue()
            
            tkn = self._db.get("Grabber", "tkn")
            if tkn:
                try:
                    await self._launch(tkn)
                    self._db.set("Grabber", "autorun", True)
                    if isinstance(msg, list):
                        msg = msg[0]
                    await self._safe_edit(msg, self.strings["reboot_done"])
                except Exception as e:
                    if isinstance(msg, list):
                        msg = msg[0]
                    await self._safe_edit(msg, self.strings["start_failed"].format(error=str(e)[:200]))
            else:
                if isinstance(msg, list):
                    msg = msg[0]
                await self._safe_edit(msg, self.strings["reboot_no_token"])

        elif cmd == "clear":
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
            self._download_logs.clear()
            
            await utils.answer(message, self.strings["clear_done"])

        elif cmd == "topic":
            if len(args) < 2:
                await utils.answer(
                    message,
                    self.strings["topic_usage"].format(prefix=prefix),
                )
                return
            
            action = args[1].lower()
            if action not in ("on", "off"):
                await utils.answer(
                    message,
                    self.strings["topic_usage"].format(prefix=prefix),
                )
                return
            
            if message.is_private:
                await utils.answer(message, self.strings["topic_not_in_group"])
                return
            
            chat = await message.get_chat()
            if not self._is_forum(chat):
                await utils.answer(message, self.strings["topic_not_forum"])
                return
            
            topic_id = self._get_topic_id(message)
            if topic_id is None:
                topic_id = message.id
            
            chat_id = message.chat_id
            
            if chat_id not in self._active_topics:
                self._active_topics[chat_id] = set()
            
            if action == "on":
                if topic_id not in self._active_topics[chat_id]:
                    await utils.answer(message, self.strings["topic_not_active"])
                    return
                self._save_active_topics()
                await utils.answer(message, self.strings["topic_enabled"])
            else:
                if topic_id in self._active_topics[chat_id]:
                    self._active_topics[chat_id].discard(topic_id)
                    self._save_active_topics()
                await utils.answer(message, self.strings["topic_disabled"])

        elif cmd == "cookies":
            if len(args) >= 2:
                action = args[1].lower()
                
                if action == "add":
                    reply = await message.get_reply_message()
                    if not reply or not reply.media:
                        await utils.answer(message, self.strings["no_reply_file"])
                        return
                    
                    if not reply.file.name.endswith(".txt"):
                        await utils.answer(message, self.strings["invalid_ext"])
                        return
                    
                    try:
                        temp_path = os.path.join(self._root, "temp_cookies.txt")
                        dl = await reply.download_media(file=temp_path)
                        
                        with open(dl, "r", encoding="utf-8") as f:
                            content = f.read()
                        
                        if not self._validate_cookies_content(content):
                            os.remove(dl)
                            await utils.answer(message, self.strings["cookie_invalid_format"])
                            return
                        
                        self._db.set("Grabber", "cookies_content", content)
                        
                        os.makedirs(self._cookie_dir, exist_ok=True)
                        with open(self._cookie_path, "w", encoding="utf-8") as f:
                            f.write(content)
                        
                        os.remove(dl)
                        await utils.answer(message, self.strings["cookie_saved"])
                    except Exception as e:
                        logger.error(f"[GRABBER] Cookie save error: {e}")
                        await utils.answer(message, self.strings["cookie_err"].format(str(e)))
                
                elif action == "remove":
                    self._clean_cookies()
                    os.makedirs(self._cookie_dir, exist_ok=True)
                    await utils.answer(message, self.strings["cookie_cleared"])
                
                else:
                    await utils.answer(
                        message,
                        self.strings["usage"].format(prefix=prefix),
                    )
            else:
                await utils.answer(
                    message,
                    self.strings["usage"].format(prefix=prefix),
                )

        else:
            await utils.answer(
                message,
                self.strings["usage"].format(prefix=prefix),
            )

    async def _launch(self, tkn):
        await self._shutdown()
        os.makedirs(self._session_dir, exist_ok=True)
        
        sess_file = os.path.join(self._session_dir, "grabber_bot")
        for ext in ['', '.session', '.session-journal']:
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
        
        self._bot.add_event_handler(self._h_start, events.NewMessage(pattern='/start'))
        self._bot.add_event_handler(self._h_msg, events.NewMessage())
        self._bot.add_event_handler(self._h_btn, events.CallbackQuery())
        
        self._worker_task = asyncio.create_task(self._queue_worker())
        logger.info("[GRABBER] Bot started, worker task created")

    async def _shutdown(self):
        logger.info("[GRABBER] Shutting down...")
        self._running = False
        self._kill_active_processes()
        
        if self._worker_task:
            await self._cancel_task_safe(self._worker_task)
            self._worker_task = None
        
        if self._bot:
            try:
                await self._bot.disconnect()
            except Exception:
                pass
            await asyncio.sleep(0.3)
            self._bot = None
        
        self._download_queue = asyncio.Queue()
        self._queue_items.clear()
        self._processed_buttons.clear()
        self._edit_state.clear()
        self._current_download = self._empty_progress()
        logger.info("[GRABBER] Shutdown complete")

    async def _queue_worker(self):
        logger.info("[GRABBER] Queue worker started...")
        
        while self._running:
            try:
                try:
                    task = await asyncio.wait_for(self._download_queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                
                chat_id, msg_id, url, mode, workdir, info, meta_dict, reply_to_topic = task
                self._queue_items.pop_first()
                
                title = (info.get('title') or 'Unknown')[:100]
                logger.info(f"[GRABBER] Worker got task: {url[:60]}...")
                
                try:
                    await self._process_download(chat_id, msg_id, url, mode, workdir, info, meta_dict, reply_to_topic)
                    self._stats["completed"] += 1
                    self._add_download_log(title, url, mode, True)
                except Exception as e:
                    logger.exception("[GRABBER] Task failed")
                    self._stats["errors"] += 1
                    self._add_download_log(title, url, mode, False, str(e))
                    try:
                        await self._edit_message(
                            chat_id, msg_id,
                            self.strings["grab_failed"].format(error=str(e)[:150]),
                            parse_mode='html'
                        )
                    except Exception:
                        pass
                finally:
                    self._current_download = self._empty_progress()
                    self._clean_workdir(workdir)
                    self._download_queue.task_done()
                    
            except asyncio.CancelledError:
                logger.info("[GRABBER] Queue worker cancelled")
                break
            except Exception as e:
                logger.exception(f"[GRABBER] Worker error: {e}")
                await asyncio.sleep(1)
        
        logger.info("[GRABBER] Queue worker stopped")

    async def _process_download(self, chat_id: int, msg_id: int, url: str, mode: str, workdir: str, info: dict, meta_dict: dict = None, reply_to_topic: int = None):
        import yt_dlp
        
        if not self._bot or not self._running:
            return
        
        title = (info.get('title') or 'media')[:100]
        orig_width = info.get('width', 0) or 0
        orig_height = info.get('height', 0) or 0
        orig_duration = info.get('duration', 0) or 0
        
        is_audio = mode == 'mp3'
        
        logger.info(f"[GRABBER] Processing: '{title[:50]}', mode={mode}, is_audio={is_audio}")
        
        self._current_download = self._empty_progress()
        self._current_download.update({
            'active': True,
            'title': title,
            'start_time': time.time(),
            'stage': 'init',
            'is_audio_only': is_audio,
        })
        
        await self._edit_message(chat_id, msg_id, self._build_status_message(), parse_mode='html')
        
        safe_title = re.sub(r'[\\/*?:"<>|]', '', title).replace(" ", "_")[:50]
        out_tmpl = os.path.join(workdir, f"{safe_title}.%(ext)s")
        
        final_opts = {
            'outtmpl': out_tmpl,
            'quiet': True,
            'no_warnings': True,
            'restrictfilenames': True,
            'progress_hooks': [self._progress_hook],
            'postprocessor_hooks': [self._postprocessor_hook],
        }
        
        if os.path.exists(self._cookie_path):
            final_opts['cookiefile'] = self._cookie_path
            logger.info(f"[GRABBER] Using cookies: {self._cookie_path}")
        
        if is_audio:
            final_opts['format'] = 'bestaudio/best'
            final_opts['postprocessors'] = [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3',
                'preferredquality': '320',
            }]
        else:
            height = int(mode)
            final_opts['format'] = self._build_format_string(height)
            final_opts['postprocessors'] = [{
                'key': 'FFmpegVideoConvertor',
                'preferedformat': 'mp4',
            }]
        
        loop = asyncio.get_event_loop()
        download_done = asyncio.Event()
        download_error = [None]
        
        def do_download():
            try:
                logger.info("[GRABBER] yt-dlp starting...")
                with yt_dlp.YoutubeDL(final_opts) as ydl:
                    ydl.download([url])
                logger.info("[GRABBER] yt-dlp completed!")
            except Exception as e:
                logger.error(f"[GRABBER] yt-dlp error: {e}")
                download_error[0] = e
            finally:
                loop.call_soon_threadsafe(download_done.set)
        
        future = loop.run_in_executor(self._executor, do_download)
        update_task = asyncio.create_task(self._update_progress_loop(chat_id, msg_id, download_done))
        
        await download_done.wait()
        await future
        await self._cancel_task_safe(update_task)
        
        if download_error[0]:
            raise download_error[0]
        
        if is_audio:
            extensions = ('.mp3', '.m4a', '.opus', '.ogg', '.wav')
        else:
            extensions = ('.mp4', '.mkv', '.webm', '.mov', '.avi')
        
        all_files = os.listdir(workdir)
        files = [f for f in all_files if f.endswith(extensions) and os.path.isfile(os.path.join(workdir, f))]
        
        if not files:
            raise Exception(f"File not found. Files: {all_files}")
        
        filepath = os.path.join(workdir, files[0])
        filesize = os.path.getsize(filepath) / (1024 * 1024)
        
        self._current_download['final_size'] = filesize
        self._current_download['ffmpeg_done'] = True
        self._current_download['video_done'] = True
        self._current_download['audio_done'] = True
        
        logger.info(f"[GRABBER] Output: {files[0]}, size: {filesize:.1f} MB")
        
        send_thumb = None
        final_title = title
        final_artist = info.get('uploader', 'Unknown')
        
        if is_audio and meta_dict:
            if meta_dict.get('title'):
                final_title = meta_dict['title']
            if meta_dict.get('artist'):
                final_artist = meta_dict['artist']
            
            if meta_dict.get('thumb_path') and os.path.exists(meta_dict['thumb_path']):
                thumb_path = meta_dict['thumb_path']
                
                try:
                    with open(thumb_path, "rb") as f:
                        raw_thumb_data = f.read()
                    normalized = normalize_cover(raw_thumb_data, force_jpeg=True)
                    if normalized:
                        norm_path = os.path.join(workdir, "normalized_custom_cover.jpg")
                        with open(norm_path, "wb") as f:
                            f.write(normalized)
                        thumb_path = norm_path
                except Exception as e:
                    logger.error(f"[GRABBER] Custom thumb normalize error: {e}")
                
                temp_output = os.path.join(workdir, "temp_with_cover.mp3")
                success = await self._embed_cover_ffmpeg(filepath, thumb_path, temp_output)
                
                if success and os.path.exists(temp_output):
                    os.remove(filepath)
                    os.rename(temp_output, filepath)
                    filesize = os.path.getsize(filepath) / (1024 * 1024)
                
                send_thumb = thumb_path
            
            elif meta_dict.get('use_yt_thumb'):
                thumb_path, cover_data = await self._download_and_prepare_yt_cover(info, workdir)
                
                if thumb_path and os.path.exists(thumb_path):
                    temp_output = os.path.join(workdir, "temp_with_yt_cover.mp3")
                    success = await self._embed_cover_ffmpeg(filepath, thumb_path, temp_output)
                    
                    if success and os.path.exists(temp_output):
                        os.remove(filepath)
                        os.rename(temp_output, filepath)
                        filesize = os.path.getsize(filepath) / (1024 * 1024)
                    
                    send_thumb = thumb_path
        
        self._current_download['stage'] = 'probe'
        await self._edit_message(chat_id, msg_id, self._build_status_message(), parse_mode='html')
        
        real_width, real_height, real_duration = orig_width, orig_height, orig_duration
        
        if not is_audio:
            probe = self._get_video_info_ffprobe(filepath)
            if probe:
                real_width = probe['width']
                real_height = probe['height']
                if probe['duration'] > 0:
                    real_duration = probe['duration']
        else:
            probe = self._get_audio_info_ffprobe(filepath)
            if probe and probe['duration'] > 0:
                real_duration = probe['duration']
        
        self._current_download['stage'] = 'upload'
        self._current_download['upload_started'] = True
        upload_start = time.time()
        
        upload_done = asyncio.Event()
        upload_update_task = asyncio.create_task(self._update_upload_loop(chat_id, msg_id, upload_start, upload_done))
        
        fname = os.path.basename(filepath)
        attrs = [DocumentAttributeFilename(file_name=fname)]
        
        if not is_audio:
            attrs.append(DocumentAttributeVideo(
                duration=int(real_duration),
                w=int(real_width),
                h=int(real_height),
                supports_streaming=True
            ))
            caption = self.strings["file_caption"].format(
                title=final_title[:80],
                size_mb=filesize,
                width=real_width,
                height=real_height
            )
        else:
            attrs.append(DocumentAttributeAudio(
                duration=int(real_duration),
                title=final_title[:64],
                performer=final_artist[:64]
            ))
            caption = self.strings["audio_caption"].format(
                title=final_title[:80],
                size_mb=filesize
            )
        
        try:
            await self._bot.send_file(
                chat_id,
                filepath,
                caption=caption,
                parse_mode='html',
                force_document=False,
                supports_streaming=(not is_audio),
                attributes=attrs,
                thumb=send_thumb,
                reply_to=reply_to_topic
            )
        finally:
            upload_done.set()
            await self._cancel_task_safe(upload_update_task)
        
        upload_elapsed = time.time() - upload_start
        self._current_download['upload_elapsed'] = upload_elapsed
        
        self._current_download['stage'] = 'done'
        await self._edit_message(chat_id, msg_id, self._build_status_message(), parse_mode='html')
        
        await asyncio.sleep(3)
        await self._safe_delete(chat_id, msg_id)

    async def _update_progress_loop(self, chat_id: int, msg_id: int, done_event: asyncio.Event):
        while not done_event.is_set():
            try:
                await asyncio.sleep(1.5)
                if not done_event.is_set():
                    await self._edit_message(chat_id, msg_id, self._build_status_message(), parse_mode='html')
            except asyncio.CancelledError:
                break
            except Exception:
                pass

    async def _update_upload_loop(self, chat_id: int, msg_id: int, start_time: float, done_event: asyncio.Event):
        while not done_event.is_set():
            try:
                await asyncio.sleep(1.5)
                if not done_event.is_set():
                    self._current_download['upload_elapsed'] = time.time() - start_time
                    await self._edit_message(chat_id, msg_id, self._build_status_message(), parse_mode='html')
            except asyncio.CancelledError:
                break
            except Exception:
                pass

    async def _get_info(self, url):
        import yt_dlp
        ydl_opts = {
            'quiet': True,
            'no_warnings': True,
            'socket_timeout': 30
        }
        if os.path.exists(self._cookie_path):
            ydl_opts['cookiefile'] = self._cookie_path
        
        loop = asyncio.get_event_loop()
        def extract():
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                return ydl.extract_info(url, download=False)
        return await loop.run_in_executor(self._executor, extract)

    async def _h_start(self, ev):
        if not self._running:
            return
        
        if ev.is_private:
            try:
                user = await self._bot.get_entity(ev.sender_id)
                name = f"{getattr(user, 'first_name', '') or ''} {getattr(user, 'last_name', '') or ''}".strip() or "friend"
                name = self._escape_html(name)
                user_link = f'<a href="tg://user?id={ev.sender_id}">{name}</a>'
                greeting = self.strings["hello"].format(user_link=user_link)
            except Exception:
                greeting = self.strings["hello_fallback"]
            await ev.reply(greeting, parse_mode='html')
        else:
            chat_id = ev.chat_id
            topic_id = self._get_topic_id(ev)
            
            try:
                chat = await self._bot.get_entity(chat_id)
                is_forum = self._is_forum(chat)
            except Exception:
                is_forum = False
            
            if is_forum and topic_id:
                if chat_id not in self._active_topics:
                    self._active_topics[chat_id] = set()
                self._active_topics[chat_id].add(topic_id)
                self._save_active_topics()
                await ev.reply(self.strings["hello_topic"], parse_mode='html')
            else:
                if chat_id not in self._active_groups:
                    self._active_groups.add(chat_id)
                    self._save_active_groups()
                await ev.reply(self.strings["hello_group"], parse_mode='html')

    async def _h_msg(self, ev):
        if not self._running or not self._bot:
            return
        
        if ev.text and ev.text.startswith('/'):
            return
        
        user_id = ev.sender_id
        is_private = ev.is_private
        
        if is_private and user_id in self._edit_state:
            await self._handle_editor_input(ev)
            return
        
        if is_private:
            if not ev.text:
                return
            
            link = self._extract_url(ev.text)
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
                if topic_id not in self._active_topics[chat_id]:
                    return
            else:
                if chat_id not in self._active_groups:
                    return
            
            if not ev.text:
                return
            
            link = self._extract_url(ev.text)
            if not link:
                return
        
        logger.info(f"[GRABBER] New URL: {link[:50]}...")
        
        await self._set_reaction(ev, self.strings["react_ok"])
        
        topic_id = self._get_topic_id(ev) if not is_private else None
        msg = await ev.reply(self.strings["analyzing"], parse_mode='html')
        
        try:
            info = await self._get_info(link)
            
            title = (info.get('title') or 'Media')[:80]
            duration = info.get('duration', 0) or 0
            duration_str = self._format_duration(duration)
            
            if is_private:
                buttons = [
                    [
                        Button.inline(self.strings["btn_video"], data=f"menu:video:{ev.id}"),
                        Button.inline(self.strings["btn_audio"], data=f"menu:audio:{ev.id}")
                    ],
                    [Button.inline(self.strings["btn_cancel"], data=f"g:cancel:{ev.id}")]
                ]
                text = self.strings["found_media"].format(title=title, duration=duration_str)
            else:
                buttons = [
                    [Button.inline(self.strings["btn_video"], data=f"menu:video:{ev.id}:{topic_id or 0}")],
                    [Button.inline(self.strings["btn_cancel"], data=f"g:cancel:{ev.id}")]
                ]
                text = self.strings["found_media_group"].format(title=title, duration=duration_str)
            
            await msg.edit(text, buttons=buttons, parse_mode='html')
        except Exception as e:
            logger.error(f"[GRABBER] Get info failed: {e}")
            await self._safe_edit(msg, self.strings["grab_failed"].format(error=str(e)[:150]), parse_mode='html')

    async def _handle_editor_input(self, ev):
        user_id = ev.sender_id
        state = self._edit_state.get(user_id)
        
        if not state:
            return
        
        msg = state.get('msg_event')
        step = state.get('step')
        
        if step == 'waiting_thumb':
            if ev.document:
                mime = getattr(ev.document, 'mime_type', '') or ''
                if mime in ('image/png', 'image/jpeg'):
                    try:
                        await ev.delete()
                    except Exception:
                        pass
                    
                    dl_msg = await ev.respond(self.strings["downloading_image"], parse_mode='html')
                    
                    temp_thumb_dir = os.path.join(self._cache_dir, f"thumb_{user_id}_{int(time.time())}")
                    os.makedirs(temp_thumb_dir, exist_ok=True)
                    
                    try:
                        downloaded = await ev.download_media(file=temp_thumb_dir)
                        logger.info(f"[GRABBER] Downloaded to: {downloaded}")
                        
                        if not downloaded or not os.path.exists(downloaded):
                            logger.error("[GRABBER] Download failed - file not exists")
                            await dl_msg.delete()
                            return
                        
                        async with self._edit_lock:
                            state['custom_thumb'] = downloaded
                            state['step'] = 'confirm'
                        
                        await dl_msg.delete()
                        await self._show_confirmation(msg, state, user_id)
                        
                    except Exception as e:
                        logger.error(f"[GRABBER] Thumb processing error: {e}")
                        try:
                            await dl_msg.delete()
                        except Exception:
                            pass
                        self._clean_workdir(temp_thumb_dir)
                    return
                else:
                    await ev.respond(self.strings["only_image"], parse_mode='html')
                    return
            
            elif ev.photo:
                await ev.respond(self.strings["only_image"], parse_mode='html')
                return
            
            elif ev.text and not ev.text.startswith("/"):
                await ev.respond(self.strings["text_needed_image"], parse_mode='html')
                return
            
            return
        
        if not ev.text:
            return
        
        try:
            await ev.delete()
        except Exception:
            pass
        
        if step == 'waiting_title':
            async with self._edit_lock:
                state['custom_title'] = ev.text
                state['step'] = 'waiting_artist'
            
            await msg.edit(
                self.strings["edit_title_done"].format(self._escape_html(ev.text)),
                parse_mode='html',
                buttons=[[Button.inline(self.strings["btn_cancel"], data=f"edit:cancel:{user_id}")]]
            )
        
        elif step == 'waiting_artist':
            async with self._edit_lock:
                state['custom_artist'] = ev.text
                state['step'] = 'waiting_thumb'
            
            await msg.edit(
                self.strings["edit_artist_done"].format(self._escape_html(ev.text)),
                parse_mode='html',
                buttons=[
                    [Button.inline(self.strings["btn_no_cover"], data=f"edit:skipthumb:{user_id}")],
                    [Button.inline(self.strings["btn_cancel"], data=f"edit:cancel:{user_id}")]
                ]
            )

    async def _show_confirmation(self, msg, state, user_id):
        title = self._escape_html(state.get('custom_title', 'Unknown'))
        artist = self._escape_html(state.get('custom_artist', 'Unknown'))
        has_thumb = self.strings["thumb_yes"] if state.get('custom_thumb') else self.strings["thumb_no"]
        
        await msg.edit(
            self.strings["confirm_menu"].format(title=title, artist=artist, thumb=has_thumb),
            parse_mode='html',
            buttons=[
                [Button.inline(self.strings["btn_confirm"], data=f"edit:confirm:{user_id}")],
                [Button.inline(self.strings["btn_retry"], data=f"edit:retry:{user_id}")],
                [Button.inline(self.strings["btn_cancel"], data=f"edit:cancel:{user_id}")]
            ]
        )

    async def _h_btn(self, ev):
        if not self._running or not self._bot:
            return
        
        data = ev.data.decode()
        parts = data.split(":")
        
        if len(parts) < 2:
            return
        
        prefix = parts[0]
        action = parts[1]
        
        button_key = (ev.chat_id, ev.message_id)
        is_private = ev.is_private
        
        current_topic_id = self._get_topic_id(ev) if not is_private else None
        
        if prefix == "g" and action == "cancel":
            self._processed_buttons.add(button_key)
            await self._safe_edit(ev, self.strings["cancelled"], parse_mode='html')
            return
        
        if prefix == "menu":
            orig_id = int(parts[2]) if len(parts) > 2 else 0
            stored_topic_id = int(parts[3]) if len(parts) > 3 and parts[3] != '0' else None
            
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
                
                buttons = []
                for h in available:
                    buttons.append([Button.inline(f"{h}p", data=f"dl:video:{h}:{orig_id}:{topic_id or 0}")])
                buttons.append([Button.inline(self.strings["btn_back"], data=f"back:main:{orig_id}:{topic_id or 0}")])
                
                await ev.edit(
                    self.strings["quality_menu"],
                    buttons=buttons,
                    parse_mode='html'
                )
            
            elif action == "audio":
                if not is_private:
                    return
                
                buttons = [
                    [Button.inline(self.strings["btn_orig_thumb"], data=f"dl:audio:thumb:{orig_id}")],
                    [Button.inline(self.strings["btn_orig_clean"], data=f"dl:audio:clean:{orig_id}")],
                    [Button.inline(self.strings["btn_custom"], data=f"edit:start:{orig_id}:{ev.sender_id}")],
                    [Button.inline(self.strings["btn_back"], data=f"back:main:{orig_id}:0")]
                ]
                
                await ev.edit(
                    self.strings["audio_menu"],
                    buttons=buttons,
                    parse_mode='html'
                )
            return
        
        if prefix == "back":
            orig_id = int(parts[2]) if len(parts) > 2 else 0
            stored_topic_id = int(parts[3]) if len(parts) > 3 and parts[3] != '0' else None
            
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
            
            title = (info.get('title') or 'Media')[:80]
            duration = info.get('duration', 0) or 0
            duration_str = self._format_duration(duration)
            
            if is_private:
                buttons = [
                    [
                        Button.inline(self.strings["btn_video"], data=f"menu:video:{orig_id}"),
                        Button.inline(self.strings["btn_audio"], data=f"menu:audio:{orig_id}")
                    ],
                    [Button.inline(self.strings["btn_cancel"], data=f"g:cancel:{orig_id}")]
                ]
                text = self.strings["found_media"].format(title=title, duration=duration_str)
            else:
                buttons = [
                    [Button.inline(self.strings["btn_video"], data=f"menu:video:{orig_id}:{topic_id or 0}")],
                    [Button.inline(self.strings["btn_cancel"], data=f"g:cancel:{orig_id}")]
                ]
                text = self.strings["found_media_group"].format(title=title, duration=duration_str)
            
            await ev.edit(text, buttons=buttons, parse_mode='html')
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
                        'step': 'waiting_title',
                        'url': link,
                        'msg_event': ev,
                        'orig_id': orig_id,
                        'owner_id': owner_id,
                        'custom_title': None,
                        'custom_artist': None,
                        'custom_thumb': None,
                    }
                
                await ev.edit(
                    self.strings["editor_mode"],
                    buttons=[[Button.inline(self.strings["btn_cancel"], data=f"edit:cancel:{ev.sender_id}")]],
                    parse_mode='html'
                )
            
            elif action == "cancel":
                target_id = int(parts[2]) if len(parts) > 2 else ev.sender_id
                
                if ev.sender_id != target_id:
                    await ev.answer(self.strings["not_your_editor"], alert=True)
                    return
                
                async with self._edit_lock:
                    if target_id in self._edit_state:
                        state = self._edit_state[target_id]
                        if state.get('custom_thumb'):
                            thumb_dir = os.path.dirname(state['custom_thumb'])
                            self._clean_workdir(thumb_dir)
                        del self._edit_state[target_id]
                
                await ev.edit(self.strings["op_cancelled"], buttons=None, parse_mode='html')
            
            elif action == "skipthumb":
                target_id = int(parts[2]) if len(parts) > 2 else ev.sender_id
                
                if ev.sender_id != target_id:
                    await ev.answer(self.strings["not_your_editor"], alert=True)
                    return
                
                async with self._edit_lock:
                    if target_id in self._edit_state:
                        self._edit_state[target_id]['step'] = 'confirm'
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
                    url = state['url']
                    meta_dict = {
                        'title': state.get('custom_title'),
                        'artist': state.get('custom_artist'),
                        'thumb_path': state.get('custom_thumb'),
                    }
                    del self._edit_state[target_id]
                
                await self._queue_download(ev, url, 'mp3', meta_dict, None)
            
            elif action == "retry":
                target_id = int(parts[2]) if len(parts) > 2 else ev.sender_id
                
                if ev.sender_id != target_id:
                    await ev.answer(self.strings["not_your_editor"], alert=True)
                    return
                
                async with self._edit_lock:
                    if target_id in self._edit_state:
                        state = self._edit_state[target_id]
                        if state.get('custom_thumb'):
                            thumb_dir = os.path.dirname(state['custom_thumb'])
                            self._clean_workdir(thumb_dir)
                        state['step'] = 'waiting_title'
                        state['custom_title'] = None
                        state['custom_artist'] = None
                        state['custom_thumb'] = None
                
                await ev.edit(
                    self.strings["editor_mode"],
                    buttons=[[Button.inline(self.strings["btn_cancel"], data=f"edit:cancel:{target_id}")]],
                    parse_mode='html'
                )
            return
        
        if prefix == "dl":
            if not self._processed_buttons.add(button_key):
                await ev.answer(self.strings["already_processing"], alert=True)
                return
            
            media_type = parts[1]
            param = parts[2] if len(parts) > 2 else None
            orig_id = int(parts[3]) if len(parts) > 3 else 0
            stored_topic_id = int(parts[4]) if len(parts) > 4 and parts[4] != '0' else None
            
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
                    meta_dict = {'use_yt_thumb': True}
            else:
                return
            
            await self._queue_download(ev, link, mode, meta_dict, topic_id)

    async def _queue_download(self, ev, url, mode, meta_dict=None, topic_id=None):
        try:
            info = await self._get_info(url)
        except Exception as e:
            logger.error(f"[GRABBER] Get info failed: {e}")
            await self._safe_edit(ev, self.strings["grab_failed"].format(error=str(e)[:150]), parse_mode='html')
            return
        
        workdir = self._make_workdir()
        title = (info.get('title') or 'Media')[:50]
        
        self._queue_items.append({'title': title, 'url': url})
        
        is_active = self._current_download.get('active', False)
        pending = len(self._queue_items)
        
        if is_active or pending > 1:
            txt = self.strings["queue_pos"].format(pos=pending)
        else:
            txt = self.strings["starting"]
        
        await self._safe_edit(ev, txt, parse_mode='html')
        
        await self._download_queue.put((
            ev.chat_id,
            ev.message_id,
            url,
            mode,
            workdir,
            info,
            meta_dict,
            topic_id
        ))

    async def on_unload(self):
        logger.info("[GRABBER] Module unloading...")
        await self._shutdown()
        await asyncio.sleep(0.5)
        self._clean_cache()
        try:
            self._executor.shutdown(wait=False)
        except Exception:
            pass
        logger.info("[GRABBER] Module unloaded")