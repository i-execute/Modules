# CopyLeft 2026 github.com/i-execute
# Author: I_execute.t.me
# Licensed under AGPLv3.

__version__ = (1, 1, 0)
# meta developer: Execute_forge.t.me

import asyncio
import base64
import hashlib
import io
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import uuid
from collections import OrderedDict


def _ensure_deps():
    for mod, pkg in (("mutagen", "mutagen"), ("PIL", "Pillow")):
        try:
            __import__(mod)
        except ImportError:
            subprocess.check_call(
                [sys.executable, "-m", "pip", "install", pkg, "-q"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )


_ensure_deps()

import mutagen
from mutagen.flac import FLAC, Picture
from mutagen.id3 import APIC, ID3, ID3NoHeaderError, TIT2, TPE1
from mutagen.mp3 import MP3
from mutagen.mp4 import MP4, MP4Cover
from mutagen.oggopus import OggOpus
from mutagen.oggvorbis import OggVorbis
from PIL import Image, ImageOps

from telethon.errors import MessageEditTimeExpiredError, MessageNotModifiedError
from telethon.tl.types import (
    DocumentAttributeAudio,
    DocumentAttributeFilename,
    DocumentAttributeSticker,
    Message,
    MessageMediaDocument,
    MessageMediaPhoto,
)

from .. import loader, utils
from ..inline.types import InlineCall

logger = logging.getLogger(__name__)


THUMB_SIZE = 320
MAX_TAG = 64
CACHE_PAGE = 6
HEIF_BRANDS = (b"heic", b"heix", b"hevc", b"hevx", b"mif1", b"msf1", b"heim", b"heis", b"avif")
IMAGE_EXTS = (".jpg", ".jpeg", ".png", ".heic", ".heif", ".webp", ".bmp", ".tif", ".tiff", ".avif")
AUDIO_EXTS = {
    "audio/mpeg": ".mp3",
    "audio/mp3": ".mp3",
    "audio/mp4": ".m4a",
    "audio/x-m4a": ".m4a",
    "audio/m4a": ".m4a",
    "audio/aac": ".m4a",
    "audio/flac": ".flac",
    "audio/x-flac": ".flac",
    "audio/ogg": ".ogg",
    "audio/opus": ".opus",
    "audio/vorbis": ".ogg",
    "audio/wav": ".wav",
    "audio/x-wav": ".wav",
}


def _esc(text):
    return utils.escape_html(str(text if text is not None else ""))


def _fmt_size(size):
    size = float(size or 0)
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024 or unit == "GB":
            return f"{size:.0f} {unit}" if unit == "B" else f"{size:.2f} {unit}"
        size /= 1024


def _fmt_dur(sec):
    sec = int(sec or 0)
    h, rem = divmod(sec, 3600)
    m, s = divmod(rem, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"


def _sanitize(name):
    return re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", name).strip(". ")[:180] or "track"


def _is_heif(raw):
    return raw[4:8] == b"ftyp" and raw[8:12] in HEIF_BRANDS


def _load_heif_support():
    try:
        import pillow_heif
    except ImportError:
        subprocess.run(
            [sys.executable, "-m", "pip", "install", "pillow-heif", "-q"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=300,
        )
        import pillow_heif
    pillow_heif.register_heif_opener()


def _normalize_cover(raw, max_size):
    if not raw or len(raw) < 100:
        return None
    try:
        if _is_heif(raw):
            _load_heif_support()
        img = Image.open(io.BytesIO(raw))
        img = ImageOps.exif_transpose(img)
        if img.mode in ("RGBA", "LA", "PA") or (img.mode == "P" and "transparency" in img.info):
            img = img.convert("RGBA")
            bg = Image.new("RGB", img.size, (255, 255, 255))
            bg.paste(img, mask=img.split()[-1])
            img = bg
        else:
            img = img.convert("RGB")
        w, h = img.size
        if max_size and (w > max_size or h > max_size):
            ratio = min(max_size / w, max_size / h)
            img = img.resize((max(1, int(w * ratio)), max(1, int(h * ratio))), Image.LANCZOS)
        quality = 95
        while True:
            buf = io.BytesIO()
            img.save(buf, format="JPEG", quality=quality, optimize=True)
            data = buf.getvalue()
            if max_size != THUMB_SIZE or len(data) <= 190 * 1024 or quality <= 60:
                break
            quality -= 10
        return (data, img.size) if len(data) >= 100 else None
    except Exception as e:
        logger.warning("[CoverEditor] cover normalize failed: %s", e)
        return None


def _audio_kind(path):
    try:
        f = mutagen.File(path)
    except Exception:
        return None, None
    if f is None:
        return None, None
    if isinstance(f, MP3):
        kind = "mp3"
    elif isinstance(f, MP4):
        kind = "mp4"
    elif isinstance(f, FLAC):
        kind = "flac"
    elif isinstance(f, (OggOpus, OggVorbis)):
        kind = "ogg"
    else:
        kind = "other"
    length = getattr(getattr(f, "info", None), "length", None)
    return kind, length


def _extract_cover(path):
    try:
        f = mutagen.File(path)
        if f is None:
            return None
        if isinstance(f, MP4):
            covr = (f.tags or {}).get("covr")
            return bytes(covr[0]) if covr else None
        if isinstance(f, FLAC):
            pics = sorted(f.pictures, key=lambda p: p.type != 3)
            return pics[0].data if pics else None
        if isinstance(f, (OggOpus, OggVorbis)):
            for b64 in (f.tags or {}).get("metadata_block_picture", []):
                try:
                    return Picture(base64.b64decode(b64)).data
                except Exception:
                    continue
            return None
        tags = getattr(f, "tags", None)
        if tags is not None and hasattr(tags, "getall"):
            pics = sorted(tags.getall("APIC"), key=lambda p: p.type != 3)
            return pics[0].data if pics else None
    except Exception as e:
        logger.debug("[CoverEditor] cover extract failed: %s", e)
    return None


def _flac_picture(data, size):
    pic = Picture()
    pic.type = 3
    pic.mime = "image/jpeg"
    pic.desc = "Cover (front)"
    pic.width, pic.height = size
    pic.depth = 24
    pic.data = data
    return pic


def _mutagen_retag(path, kind, title, artist, cover_mode, cover):
    if kind == "mp4":
        f = MP4(path)
        if f.tags is None:
            f.add_tags()
        f.tags["\xa9nam"] = [title]
        f.tags["\xa9ART"] = [artist]
        if cover_mode != "orig":
            f.tags.pop("covr", None)
        if cover:
            f.tags["covr"] = [MP4Cover(cover[0], imageformat=MP4Cover.FORMAT_JPEG)]
        f.save()
        return True
    if kind == "flac":
        f = FLAC(path)
        f["title"] = title
        f["artist"] = artist
        if cover_mode != "orig":
            f.clear_pictures()
        if cover:
            f.add_picture(_flac_picture(*cover))
        f.save()
        return True
    if kind == "ogg":
        f = mutagen.File(path)
        f["title"] = title
        f["artist"] = artist
        if cover_mode != "orig":
            for key in ("metadata_block_picture", "coverart", "coverartmime"):
                f.pop(key, None)
        if cover:
            f["metadata_block_picture"] = [base64.b64encode(_flac_picture(*cover).write()).decode("ascii")]
        f.save()
        return True
    if kind in ("mp3", "other"):
        f = mutagen.File(path)
        if kind == "other" and not hasattr(f, "add_tags"):
            return False
        if kind == "mp3":
            try:
                tags = ID3(path)
            except ID3NoHeaderError:
                tags = ID3()
        else:
            if f.tags is None:
                f.add_tags()
            tags = f.tags
            if not hasattr(tags, "setall"):
                return False
        tags.setall("TIT2", [TIT2(encoding=3, text=[title])])
        tags.setall("TPE1", [TPE1(encoding=3, text=[artist])])
        if cover_mode != "orig":
            tags.delall("APIC")
        if cover:
            tags.add(APIC(encoding=3, mime="image/jpeg", type=3, desc="Cover (front)", data=cover[0]))
        if kind == "mp3":
            tags.save(path, v2_version=3)
        else:
            f.save()
        return True
    return False


@loader.tds
class CoverEditor(loader.Module):
    """Edit track title, artist and cover art with an inline editor and a cover cache"""

    strings = {
        "name": "CoverEditor",
        "_cls_doc": "Edit track title, artist and cover art with an inline editor and a cover cache",
        "_cfg_cover": "Max side of the embedded cover in pixels. Bigger covers are downscaled, never upscaled",
        "_cfg_cache": "How many covers to keep in the cache (oldest are removed)",
        "no_track": (
            "<b>No track</b>\n"
            "<blockquote>Reply to an audio track with <code>{prefix}ce</code></blockquote>"
        ),
        "no_session": (
            "<b>Nothing is being edited</b>\n"
            "<blockquote>Start editing a track first: reply to it with <code>{prefix}ce</code>, "
            "then use <code>{prefix}ce add</code> on a picture</blockquote>"
        ),
        "need_image": (
            "<b>No picture</b>\n"
            "<blockquote>Reply to a photo or an image file (JPEG, PNG, HEIF, WEBP)</blockquote>"
        ),
        "bad_image": (
            "<b>Cannot read the picture</b>\n"
            "<blockquote>The file is damaged or the format is not supported</blockquote>"
        ),
        "cover_added": (
            "<b>Cover added to cache and selected</b>\n"
            "<blockquote>Size: <code>{w}x{h}</code>, <code>{size}</code>\n"
            "Track: <code>{track}</code></blockquote>"
        ),
        "editor": (
            "<b>CoverEditor</b>\n"
            "<blockquote><b>Artist:</b> <code>{artist}</code>{artist_old}\n"
            "<b>Title:</b> <code>{title}</code>{title_old}\n"
            "<b>Cover:</b> {cover}\n"
            "<b>Mode:</b> {mode}</blockquote>\n"
            "<blockquote expandable><b>File:</b> <code>{file}</code>\n"
            "<b>Size:</b> <code>{size}</code>\n"
            "<b>Duration:</b> <code>{duration}</code>\n"
            "<b>Cache:</b> <code>{cached}</code> covers\n"
            "Reply to a picture with <code>{prefix}ce add</code> to add it to the cache and select it</blockquote>"
        ),
        "was": " <i>(was: {old})</i>",
        "cover_orig": "original",
        "cover_none": "removed",
        "cover_cache": "from cache <code>{w}x{h}</code>, <code>{size}</code>",
        "cover_missing": "from cache (file is gone, original will be kept)",
        "mode_replace": "replace in the original message",
        "mode_new": "send as a new message",
        "cache_title": (
            "<b>Cover cache</b>\n"
            "<blockquote>Selected: {cover}\n"
            "Covers in cache: <code>{count}</code>\n"
            "Add new ones with <code>{prefix}ce add</code> (reply to a picture)</blockquote>"
        ),
        "cache_empty": "Cache is empty",
        "input_artist": "New artist",
        "input_title": "New title",
        "btn_artist": "Artist",
        "btn_title": "Title",
        "btn_covers": "Covers ({count})",
        "btn_swap": "Swap",
        "btn_reset": "Reset",
        "btn_mode_replace": "Mode: replace",
        "btn_mode_new": "Mode: new message",
        "btn_apply": "Apply",
        "btn_cancel": "Cancel",
        "btn_close": "Close",
        "btn_back": "Back",
        "btn_orig": "Original",
        "btn_none": "No cover",
        "btn_clear": "Clear cache",
        "btn_prev": "Prev",
        "btn_next": "Next",
        "busy": "Already processing",
        "expired": "Session expired, run .ce again",
        "empty_input": "Empty value ignored",
        "st_download": "<b>Downloading track...</b>\n<blockquote>{file}, {size}</blockquote>",
        "st_process": "<b>Writing tags and cover...</b>\n<blockquote>Engine: {engine}</blockquote>",
        "st_upload": "<b>Uploading...</b>\n<blockquote>{mode}</blockquote>",
        "done": (
            "<b>Done</b>\n"
            "<blockquote><b>Artist:</b> <code>{artist}</code>\n"
            "<b>Title:</b> <code>{title}</code>\n"
            "<b>Cover:</b> {cover}\n"
            "<b>Size:</b> <code>{size}</code>\n"
            "<b>Engine:</b> <code>{engine}</code>\n"
            "<b>Result:</b> {mode}</blockquote>"
        ),
        "done_fallback": "\n<blockquote>Editing the original was not possible ({error}), sent as a new message</blockquote>",
        "tags_skipped": "Telegram attributes only (format does not support embedded tags)",
        "failed": "<b>Error</b>\n<blockquote>{error}</blockquote>",
    }

    strings_ru = {
        "_cls_doc": "Редактор названия, исполнителя и обложки трека с инлайн формой и кешем обложек",
        "_cfg_cover": "Максимальная сторона встраиваемой обложки в пикселях. Большие уменьшаются, маленькие не растягиваются",
        "_cfg_cache": "Сколько обложек держать в кеше (старые удаляются)",
        "no_track": (
            "<b>Нет трека</b>\n"
            "<blockquote>Ответьте на аудиотрек командой <code>{prefix}ce</code></blockquote>"
        ),
        "no_session": (
            "<b>Сейчас ничего не редактируется</b>\n"
            "<blockquote>Сначала начните редактирование трека: реплай <code>{prefix}ce</code> на трек, "
            "потом <code>{prefix}ce add</code> реплаем на картинку</blockquote>"
        ),
        "need_image": (
            "<b>Нет картинки</b>\n"
            "<blockquote>Ответьте на фото или файл-картинку (JPEG, PNG, HEIF, WEBP)</blockquote>"
        ),
        "bad_image": (
            "<b>Не удалось прочитать картинку</b>\n"
            "<blockquote>Файл поврежден или формат не поддерживается</blockquote>"
        ),
        "cover_added": (
            "<b>Обложка добавлена в кеш и выбрана</b>\n"
            "<blockquote>Размер: <code>{w}x{h}</code>, <code>{size}</code>\n"
            "Трек: <code>{track}</code></blockquote>"
        ),
        "editor": (
            "<b>CoverEditor</b>\n"
            "<blockquote><b>Исполнитель:</b> <code>{artist}</code>{artist_old}\n"
            "<b>Название:</b> <code>{title}</code>{title_old}\n"
            "<b>Обложка:</b> {cover}\n"
            "<b>Режим:</b> {mode}</blockquote>\n"
            "<blockquote expandable><b>Файл:</b> <code>{file}</code>\n"
            "<b>Размер:</b> <code>{size}</code>\n"
            "<b>Длительность:</b> <code>{duration}</code>\n"
            "<b>Кеш:</b> <code>{cached}</code> обложек\n"
            "Реплай <code>{prefix}ce add</code> на картинку добавит ее в кеш и выберет</blockquote>"
        ),
        "was": " <i>(было: {old})</i>",
        "cover_orig": "оригинальная",
        "cover_none": "удалена",
        "cover_cache": "из кеша <code>{w}x{h}</code>, <code>{size}</code>",
        "cover_missing": "из кеша (файл пропал, останется оригинальная)",
        "mode_replace": "заменить в исходном сообщении",
        "mode_new": "отправить новым сообщением",
        "cache_title": (
            "<b>Кеш обложек</b>\n"
            "<blockquote>Выбрана: {cover}\n"
            "Обложек в кеше: <code>{count}</code>\n"
            "Новые добавляются через <code>{prefix}ce add</code> (реплай на картинку)</blockquote>"
        ),
        "cache_empty": "Кеш пуст",
        "input_artist": "Новый исполнитель",
        "input_title": "Новое название",
        "btn_artist": "Исполнитель",
        "btn_title": "Название",
        "btn_covers": "Обложки ({count})",
        "btn_swap": "Поменять местами",
        "btn_reset": "Сбросить",
        "btn_mode_replace": "Режим: заменить",
        "btn_mode_new": "Режим: новым",
        "btn_apply": "Применить",
        "btn_cancel": "Отмена",
        "btn_close": "Закрыть",
        "btn_back": "Назад",
        "btn_orig": "Оригинальная",
        "btn_none": "Без обложки",
        "btn_clear": "Очистить кеш",
        "btn_prev": "Назад",
        "btn_next": "Вперед",
        "busy": "Уже обрабатывается",
        "expired": "Сессия устарела, запустите .ce заново",
        "empty_input": "Пустое значение проигнорировано",
        "st_download": "<b>Скачиваю трек...</b>\n<blockquote>{file}, {size}</blockquote>",
        "st_process": "<b>Записываю теги и обложку...</b>\n<blockquote>Движок: {engine}</blockquote>",
        "st_upload": "<b>Загружаю...</b>\n<blockquote>{mode}</blockquote>",
        "done": (
            "<b>Готово</b>\n"
            "<blockquote><b>Исполнитель:</b> <code>{artist}</code>\n"
            "<b>Название:</b> <code>{title}</code>\n"
            "<b>Обложка:</b> {cover}\n"
            "<b>Размер:</b> <code>{size}</code>\n"
            "<b>Движок:</b> <code>{engine}</code>\n"
            "<b>Результат:</b> {mode}</blockquote>"
        ),
        "done_fallback": "\n<blockquote>Отредактировать исходное сообщение не вышло ({error}), отправлено новым</blockquote>",
        "tags_skipped": "только атрибуты Telegram (формат не поддерживает встроенные теги)",
        "failed": "<b>Ошибка</b>\n<blockquote>{error}</blockquote>",
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "COVER_MAX_SIZE",
                1400,
                lambda: self.strings["_cfg_cover"],
                validator=loader.validators.Integer(minimum=300, maximum=3000),
            ),
            loader.ConfigValue(
                "CACHE_LIMIT",
                30,
                lambda: self.strings["_cfg_cache"],
                validator=loader.validators.Integer(minimum=1, maximum=500),
            ),
        )
        self._sessions = OrderedDict()
        self._ffmpeg = None

    def _data_dir(self):
        uid = getattr(self, "_tg_id", None) or (getattr(self, "_owner", None).id if getattr(self, "_owner", None) else "common")
        p = os.path.join(tempfile.gettempdir(), "CoverEditor", str(uid))
        os.makedirs(p, exist_ok=True)
        return p

    def _covers_dir(self):
        p = os.path.join(self._data_dir(), "covers")
        os.makedirs(p, exist_ok=True)
        return p

    async def client_ready(self, client, db):
        self._client = client
        self._owner = await client.get_me()
        self._tg_id = self._owner.id if self._owner else getattr(client, "tg_id", None)
        os.makedirs(self._covers_dir(), exist_ok=True)
        try:
            for name in os.listdir(self._data_dir()):
                if name.startswith("work_"):
                    shutil.rmtree(os.path.join(self._data_dir(), name), ignore_errors=True)
        except Exception:
            pass

    async def on_unload(self):
        self._sessions.clear()

    def _s(self, key, **kwargs):
        try:
            return self.strings[key].format(prefix=self.get_prefix(), **kwargs)
        except (KeyError, IndexError):
            return self.strings[key]

    def _ffmpeg_bin(self):
        if self._ffmpeg is None:
            path = shutil.which("ffmpeg")
            if not path:
                try:
                    import imageio_ffmpeg
                    path = imageio_ffmpeg.get_ffmpeg_exe()
                except Exception:
                    path = ""
            self._ffmpeg = path
        return self._ffmpeg

    def _cache_list(self):
        os.makedirs(self._covers_dir(), exist_ok=True)
        items = []
        for name in os.listdir(self._covers_dir()):
            m = re.match(r"^(\d+)_(\d+)x(\d+)_([0-9a-f]{12})\.jpg$", name)
            if not m:
                continue
            path = os.path.join(self._covers_dir(), name)
            try:
                size = os.path.getsize(path)
            except OSError:
                continue
            items.append({
                "ts": int(m.group(1)),
                "w": int(m.group(2)),
                "h": int(m.group(3)),
                "key": m.group(4),
                "path": path,
                "size": size,
            })
        items.sort(key=lambda x: x["ts"], reverse=True)
        return items

    def _cache_get(self, key):
        for item in self._cache_list():
            if item["key"] == key:
                return item
        return None

    def _cache_put(self, data, size):
        key = hashlib.sha1(data).hexdigest()[:12]
        old = self._cache_get(key)
        if old:
            os.remove(old["path"])
        path = os.path.join(self._covers_dir(), f"{int(time.time() * 1000)}_{size[0]}x{size[1]}_{key}.jpg")
        with open(path, "wb") as fh:
            fh.write(data)
        for item in self._cache_list()[int(self.config["CACHE_LIMIT"]):]:
            try:
                os.remove(item["path"])
            except OSError:
                pass
        return self._cache_get(key)

    def _cover_text(self, sess):
        mode = sess["cover"]
        if mode == "orig":
            return self.strings["cover_orig"]
        if mode == "none":
            return self.strings["cover_none"]
        item = self._cache_get(mode.split(":", 1)[1])
        if not item:
            return self.strings["cover_missing"]
        return self._s("cover_cache", w=item["w"], h=item["h"], size=_fmt_size(item["size"]))

    def _mode_text(self, sess):
        return self.strings["mode_replace"] if sess["mode"] == "replace" else self.strings["mode_new"]

    def _render(self, sess):
        sid = sess["sid"]
        title_old = self._s("was", old=_esc(sess["orig_title"] or "-")) if sess["title"] != sess["orig_title"] else ""
        artist_old = self._s("was", old=_esc(sess["orig_artist"] or "-")) if sess["artist"] != sess["orig_artist"] else ""
        text = self._s(
            "editor",
            artist=_esc(sess["artist"] or "-"),
            artist_old=artist_old,
            title=_esc(sess["title"] or "-"),
            title_old=title_old,
            cover=self._cover_text(sess),
            mode=self._mode_text(sess),
            file=_esc(sess["filename"]),
            size=_fmt_size(sess["size"]),
            duration=_fmt_dur(sess["duration"]),
            cached=len(self._cache_list()),
        )
        markup = [
            [
                {"text": self.strings["btn_artist"], "input": self.strings["input_artist"], "handler": self._in_artist, "args": (sid,), "style": "primary"},
                {"text": self.strings["btn_title"], "input": self.strings["input_title"], "handler": self._in_title, "args": (sid,), "style": "primary"},
            ],
            [
                {"text": self._s("btn_covers", count=len(self._cache_list())), "callback": self._cb_covers, "args": (sid, 0), "style": "primary"},
                {"text": self.strings["btn_swap"], "callback": self._cb_swap, "args": (sid,), "style": "primary"},
            ],
        ]
        row = []
        if sess["can_edit"]:
            key = "btn_mode_replace" if sess["mode"] == "replace" else "btn_mode_new"
            row.append({"text": self.strings[key], "callback": self._cb_mode, "args": (sid,), "style": "primary"})
        row.append({"text": self.strings["btn_reset"], "callback": self._cb_reset, "args": (sid,), "style": "primary"})
        markup.append(row)
        markup.append([
            {"text": self.strings["btn_apply"], "callback": self._cb_apply, "args": (sid,), "style": "success"},
            {"text": self.strings["btn_cancel"], "callback": self._cb_cancel, "args": (sid,), "style": "danger"},
        ])
        return text, markup

    def _render_covers(self, sess, page):
        sid = sess["sid"]
        items = self._cache_list()
        pages = max(1, (len(items) + CACHE_PAGE - 1) // CACHE_PAGE)
        page = max(0, min(page, pages - 1))
        text = self._s("cache_title", cover=self._cover_text(sess), count=len(items))
        markup = []
        chunk = items[page * CACHE_PAGE:(page + 1) * CACHE_PAGE]
        for i in range(0, len(chunk), 2):
            row = []
            for n, item in enumerate(chunk[i:i + 2], start=page * CACHE_PAGE + i + 1):
                active = sess["cover"] == f"cache:{item['key']}"
                row.append({
                    "text": f"#{n} {item['w']}x{item['h']} {time.strftime('%d.%m %H:%M', time.localtime(item['ts'] / 1000))}",
                    "callback": self._cb_pick,
                    "args": (sid, f"cache:{item['key']}", page),
                    "style": "success" if active else "primary",
                })
            markup.append(row)
        if not items:
            markup.append([{"text": self.strings["cache_empty"], "callback": self._cb_covers, "args": (sid, 0), "style": "primary"}])
        if pages > 1:
            markup.append([
                {"text": self.strings["btn_prev"], "callback": self._cb_covers, "args": (sid, (page - 1) % pages), "style": "primary"},
                {"text": f"{page + 1}/{pages}", "callback": self._cb_covers, "args": (sid, page), "style": "primary"},
                {"text": self.strings["btn_next"], "callback": self._cb_covers, "args": (sid, (page + 1) % pages), "style": "primary"},
            ])
        markup.append([
            {"text": self.strings["btn_orig"], "callback": self._cb_pick, "args": (sid, "orig", page), "style": "success" if sess["cover"] == "orig" else "primary"},
            {"text": self.strings["btn_none"], "callback": self._cb_pick, "args": (sid, "none", page), "style": "success" if sess["cover"] == "none" else "primary"},
        ])
        row = []
        if items:
            row.append({"text": self.strings["btn_clear"], "callback": self._cb_clear, "args": (sid,), "style": "danger"})
        row.append({"text": self.strings["btn_back"], "callback": self._cb_back, "args": (sid,), "style": "primary"})
        markup.append(row)
        return text, markup

    async def _show(self, sess, call=None):
        text, markup = self._render(sess)
        target = call or sess["form"]
        try:
            await target.edit(text, reply_markup=markup)
        except Exception as e:
            logger.debug("[CoverEditor] edit failed: %s", e)

    async def _get(self, call, sid, check_busy=True):
        sess = self._sessions.get(sid)
        if not sess:
            await call.answer(self.strings["expired"], show_alert=True)
            return None
        if check_busy and sess["busy"]:
            await call.answer(self.strings["busy"])
            return None
        self._sessions.move_to_end(sid)
        return sess

    async def _in_artist(self, call, text, sid):
        sess = await self._get(call, sid)
        if not sess:
            return
        value = (text or "").strip()[:MAX_TAG]
        if not value:
            await call.answer(self.strings["empty_input"])
            return
        sess["artist"] = value
        await self._show(sess, call)

    async def _in_title(self, call, text, sid):
        sess = await self._get(call, sid)
        if not sess:
            return
        value = (text or "").strip()[:MAX_TAG]
        if not value:
            await call.answer(self.strings["empty_input"])
            return
        sess["title"] = value
        await self._show(sess, call)

    async def _cb_swap(self, call: InlineCall, sid):
        sess = await self._get(call, sid)
        if sess:
            sess["artist"], sess["title"] = sess["title"], sess["artist"]
            await self._show(sess, call)

    async def _cb_reset(self, call: InlineCall, sid):
        sess = await self._get(call, sid)
        if sess:
            sess["artist"], sess["title"] = sess["orig_artist"], sess["orig_title"]
            sess["cover"] = "orig"
            sess["mode"] = "replace" if sess["can_edit"] else "new"
            await self._show(sess, call)

    async def _cb_mode(self, call: InlineCall, sid):
        sess = await self._get(call, sid)
        if sess:
            sess["mode"] = "new" if sess["mode"] == "replace" else "replace"
            await self._show(sess, call)

    async def _cb_covers(self, call: InlineCall, sid, page):
        sess = await self._get(call, sid)
        if sess:
            text, markup = self._render_covers(sess, page)
            await call.edit(text, reply_markup=markup)

    async def _cb_pick(self, call: InlineCall, sid, cover, page):
        sess = await self._get(call, sid)
        if sess:
            sess["cover"] = cover
            text, markup = self._render_covers(sess, page)
            await call.edit(text, reply_markup=markup)

    async def _cb_clear(self, call: InlineCall, sid):
        sess = await self._get(call, sid)
        if not sess:
            return
        for item in self._cache_list():
            try:
                os.remove(item["path"])
            except OSError:
                pass
        for other in self._sessions.values():
            if other["cover"].startswith("cache:"):
                other["cover"] = "orig"
        text, markup = self._render_covers(sess, 0)
        await call.edit(text, reply_markup=markup)

    async def _cb_back(self, call: InlineCall, sid):
        sess = await self._get(call, sid)
        if sess:
            await self._show(sess, call)

    async def _cb_cancel(self, call: InlineCall, sid):
        sess = self._sessions.get(sid)
        if sess and sess["busy"]:
            await call.answer(self.strings["busy"])
            return
        self._sessions.pop(sid, None)
        await call.delete()

    async def _cb_close(self, call: InlineCall):
        await call.delete()

    async def _cb_apply(self, call: InlineCall, sid):
        sess = await self._get(call, sid)
        if not sess:
            return
        sess["busy"] = True
        close = [[{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}]]
        try:
            text = await self._process(sess, call)
            self._sessions.pop(sid, None)
            await call.edit(text, reply_markup=close)
        except Exception as e:
            logger.exception("[CoverEditor] apply failed")
            sess["busy"] = False
            text, markup = self._render(sess)
            await call.edit(self._s("failed", error=_esc(str(e)[:300])) + "\n\n" + text, reply_markup=markup)

    async def _status(self, call, text):
        try:
            await call.edit(text, reply_markup=[])
        except Exception as e:
            logger.debug("[CoverEditor] status edit failed: %s", e)

    async def _run_ffmpeg(self, args, timeout=180):
        proc = await asyncio.create_subprocess_exec(
            self._ffmpeg_bin(), "-hide_banner", "-loglevel", "error", "-y", *args,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            _, err = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        except asyncio.TimeoutError:
            proc.kill()
            return False, "timeout"
        return proc.returncode == 0, err.decode("utf-8", "replace")[-300:]

    async def _ffmpeg_retag(self, src, out, kind, title, artist, cover_mode, cover_path):
        if kind not in ("mp3", "mp4", "flac") or not self._ffmpeg_bin():
            return False
        args = ["-i", src]
        if cover_path:
            args += ["-i", cover_path, "-map", "0:a", "-map", "1:0"]
        elif cover_mode == "none":
            args += ["-map", "0:a"]
        else:
            args += ["-map", "0"]
        args += ["-c", "copy", "-map_metadata", "0"]
        if kind == "mp3":
            args += ["-id3v2_version", "3", "-write_id3v1", "0"]
        args += ["-metadata", f"title={title}", "-metadata", f"artist={artist}"]
        if cover_path:
            args += [
                "-metadata:s:v", "title=Album cover",
                "-metadata:s:v", "comment=Cover (front)",
                "-disposition:v", "attached_pic",
            ]
        fmt = {"mp3": "mp3", "flac": "flac", "mp4": "ipod" if out.lower().endswith((".m4a", ".m4b")) else "mp4"}[kind]
        args += ["-f", fmt, out]
        ok, err = await self._run_ffmpeg(args)
        if not ok:
            logger.warning("[CoverEditor] ffmpeg failed: %s", err)
            return False
        check, _ = _audio_kind(out)
        return check == kind and os.path.getsize(out) > 0

    def _ext_for(self, sess):
        ext = os.path.splitext(sess["filename"])[1].lower()
        if re.match(r"^\.[a-z0-9]{1,5}$", ext):
            return ext
        return AUDIO_EXTS.get((sess["mime"] or "").split(";")[0], ".mp3")

    async def _process(self, sess, call):
        os.makedirs(self._data_dir(), exist_ok=True)
        workdir = tempfile.mkdtemp(prefix="work_", dir=self._data_dir())
        try:
            await self._status(call, self._s("st_download", file=_esc(sess["filename"]), size=_fmt_size(sess["size"])))
            ext = self._ext_for(sess)
            src = os.path.join(workdir, "src" + ext)
            with open(src, "wb"):
                pass
            result = await sess["msg"].download_media(file=src)
            if isinstance(result, str) and os.path.isfile(result):
                src = result
            kind, length = await asyncio.to_thread(_audio_kind, src)

            cover_mode = sess["cover"]
            cover = None
            cover_path = None
            thumb = None
            if cover_mode.startswith("cache:"):
                item = self._cache_get(cover_mode.split(":", 1)[1])
                if item:
                    with open(item["path"], "rb") as fh:
                        cover = (fh.read(), (item["w"], item["h"]))
                    cover_path = os.path.join(workdir, "cover.jpg")
                    shutil.copy(item["path"], cover_path)
                else:
                    cover_mode = "orig"
            if cover:
                thumb = await asyncio.to_thread(_normalize_cover, cover[0], THUMB_SIZE)
            elif cover_mode == "orig":
                embedded = await asyncio.to_thread(_extract_cover, src)
                if not embedded:
                    try:
                        embedded = await sess["msg"].download_media(file=bytes, thumb=-1)
                    except Exception:
                        embedded = None
                if embedded:
                    thumb = await asyncio.to_thread(_normalize_cover, embedded, THUMB_SIZE)
            thumb_path = None
            if thumb:
                thumb_path = os.path.join(workdir, "thumb.jpg")
                with open(thumb_path, "wb") as fh:
                    fh.write(thumb[0])

            title, artist = sess["title"] or "", sess["artist"] or ""
            out = os.path.join(workdir, "out" + os.path.splitext(src)[1])
            engine = "ffmpeg" if kind in ("mp3", "mp4", "flac") and self._ffmpeg_bin() else "mutagen"
            await self._status(call, self._s("st_process", engine=engine))
            ok = await self._ffmpeg_retag(src, out, kind, title, artist, cover_mode, cover_path)
            if not ok:
                engine = "mutagen"
                shutil.copy(src, out)
                try:
                    ok = await asyncio.to_thread(_mutagen_retag, out, kind, title, artist, cover_mode, cover)
                except Exception as e:
                    logger.warning("[CoverEditor] mutagen failed: %s", e)
                    ok = False
                if not ok:
                    shutil.copy(src, out)
                    engine = self.strings["tags_skipped"]
            new_kind, new_length = await asyncio.to_thread(_audio_kind, out)
            duration = int(round(new_length or length or sess["duration"] or 0))
            size = os.path.getsize(out)

            if title != sess["orig_title"] or artist != sess["orig_artist"] or not sess["filename"]:
                fname = _sanitize(f"{artist} - {title}" if artist and title else (title or artist or "track")) + os.path.splitext(out)[1]
            else:
                fname = sess["filename"]
            attrs = [
                DocumentAttributeAudio(duration=duration, voice=False, title=title[:MAX_TAG] or None, performer=artist[:MAX_TAG] or None),
                DocumentAttributeFilename(fname),
            ]
            await self._status(call, self._s("st_upload", mode=self._mode_text(sess)))
            fallback = ""
            mode_text = self._mode_text(sess)
            sent = False
            if sess["mode"] == "replace" and sess["can_edit"]:
                try:
                    await self._client.edit_message(
                        sess["chat_id"],
                        sess["msg_id"],
                        text=sess["caption"] or "",
                        formatting_entities=sess["entities"],
                        file=out,
                        attributes=attrs,
                        thumb=thumb_path,
                        force_document=False,
                    )
                    sent = True
                except (MessageEditTimeExpiredError, MessageNotModifiedError) as e:
                    fallback = self._s("done_fallback", error=_esc(type(e).__name__))
                except Exception as e:
                    fallback = self._s("done_fallback", error=_esc(str(e)[:120]))
            if not sent:
                await self._client.send_file(
                    sess["chat_id"],
                    out,
                    caption=sess["caption"] or None,
                    formatting_entities=sess["entities"],
                    attributes=attrs,
                    thumb=thumb_path,
                    reply_to=sess["msg_id"],
                    voice=False,
                    force_document=False,
                )
                mode_text = self.strings["mode_new"]
            return self._s(
                "done",
                artist=_esc(artist or "-"),
                title=_esc(title or "-"),
                cover=self._cover_text({"cover": cover_mode}),
                size=_fmt_size(size),
                engine=_esc(engine),
                mode=mode_text,
            ) + fallback
        finally:
            shutil.rmtree(workdir, ignore_errors=True)

    def _track_info(self, msg):
        media = getattr(msg, "media", None)
        doc = getattr(media, "document", None) if isinstance(media, MessageMediaDocument) else None
        if not doc:
            return None
        audio = None
        filename = ""
        for a in doc.attributes or []:
            if isinstance(a, DocumentAttributeAudio):
                audio = a
            elif isinstance(a, DocumentAttributeFilename):
                filename = a.file_name or ""
        mime = (doc.mime_type or "").lower()
        if audio and audio.voice:
            return None
        if not audio and not mime.startswith("audio/"):
            return None
        return {
            "doc": doc,
            "title": (audio.title if audio else None) or "",
            "artist": (audio.performer if audio else None) or "",
            "duration": (audio.duration if audio else 0) or 0,
            "filename": filename or f"track{AUDIO_EXTS.get(mime.split(';')[0], '.mp3')}",
            "mime": mime,
            "size": doc.size,
        }

    def _is_image(self, msg):
        media = getattr(msg, "media", None)
        if isinstance(media, MessageMediaPhoto) and media.photo:
            return True
        doc = getattr(media, "document", None) if isinstance(media, MessageMediaDocument) else None
        if not doc:
            return False
        mime = (doc.mime_type or "").lower()
        name = ""
        for a in doc.attributes or []:
            if isinstance(a, DocumentAttributeSticker) and mime != "image/webp":
                return False
            if isinstance(a, DocumentAttributeFilename):
                name = (a.file_name or "").lower()
        return mime.startswith("image/") or name.endswith(IMAGE_EXTS)

    async def _start(self, message, reply):
        info = self._track_info(reply)
        if not info:
            await utils.answer(message, self._s("no_track"))
            return
        sid = uuid.uuid4().hex[:10]
        can_edit = bool(reply.out) and not reply.fwd_from
        sess = {
            "sid": sid,
            "chat_id": message.chat_id,
            "msg_id": reply.id,
            "msg": reply,
            "can_edit": can_edit,
            "mode": "replace" if can_edit else "new",
            "orig_title": info["title"],
            "orig_artist": info["artist"],
            "title": info["title"],
            "artist": info["artist"],
            "duration": info["duration"],
            "filename": info["filename"],
            "mime": info["mime"],
            "size": info["size"],
            "caption": reply.message or "",
            "entities": reply.entities,
            "cover": "orig",
            "busy": False,
            "form": None,
        }
        text, markup = self._render(sess)
        form = await self.inline.form(text=text, message=message, reply_markup=markup, silent=True)
        if not form:
            return
        sess["form"] = form
        self._sessions[sid] = sess
        while len(self._sessions) > 20:
            self._sessions.popitem(last=False)

    def _target_session(self, chat_id):
        for sess in reversed(self._sessions.values()):
            if sess["chat_id"] == chat_id and not sess["busy"]:
                return sess
        for sess in reversed(self._sessions.values()):
            if not sess["busy"]:
                return sess
        return None

    async def _add(self, message, reply):
        sess = self._target_session(message.chat_id)
        if not sess:
            await utils.answer(message, self._s("no_session"))
            return
        if not reply or not self._is_image(reply):
            await utils.answer(message, self._s("need_image"))
            return
        raw = await reply.download_media(file=bytes)
        norm = await asyncio.to_thread(_normalize_cover, raw, int(self.config["COVER_MAX_SIZE"]))
        if not norm:
            await utils.answer(message, self._s("bad_image"))
            return
        item = await asyncio.to_thread(self._cache_put, norm[0], norm[1])
        sess["cover"] = f"cache:{item['key']}"
        self._sessions.move_to_end(sess["sid"])
        await self._show(sess)
        track = f"{sess['artist']} - {sess['title']}".strip(" -") or sess["filename"]
        await utils.answer(
            message,
            self._s("cover_added", w=item["w"], h=item["h"], size=_fmt_size(item["size"]), track=_esc(track)),
        )

    @loader.command(
        ru_doc="Реплай на трек - редактор названия, исполнителя и обложки. ce add реплаем на картинку - добавить обложку в кеш (только во время редактирования)",
        en_doc="Reply to a track - title, artist and cover editor. ce add as reply to a picture - add a cover to the cache (only while editing)",
    )
    async def cecmd(self, message: Message):
        """Reply to a track - title, artist and cover editor. ce add as reply to a picture - add a cover to the cache (only while editing)"""
        args = (utils.get_args_raw(message) or "").strip().lower()
        reply = await message.get_reply_message()
        if args == "add":
            await self._add(message, reply)
            return
        if not reply:
            await utils.answer(message, self._s("no_track"))
            return
        await self._start(message, reply)