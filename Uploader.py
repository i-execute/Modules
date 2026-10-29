# CopyLeft 2026 github.com/i-execute
# Author: I_execute.t.me
# Licensed under AGPLv3.

__version__ = (1, 4, 1)
# meta developer: I_execute.t.me

import os
import json
import asyncio
import logging
import time
import tempfile
import tarfile
import shutil
import re
import random
import string

from telethon.tl.types import MessageMediaWebPage

from .. import loader, utils
from ..inline.types import InlineCall

logger = logging.getLogger(__name__)

MAX_FILE_SIZE = 512 * 1024 * 1024

UA = "Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/115.0"

PROVIDERS = {
    "tempsh": {"label": "temp.sh", "btn": "btn_tempsh"},
    "uguu": {"label": "uguu.se", "btn": "btn_uguu"},
    "tmpfiles": {"label": "tmpfiles.org", "btn": "btn_tmpfiles"},
    "x0.at": {"label": "x0.at", "btn": "btn_x0"},
    "litterbox": {"label": "litterbox.catbox.moe", "btn": "btn_litter"},
}


def _parse_url(host, text):
    text = (text or "").strip()
    if not text:
        return None
    if host == "tmpfiles":
        try:
            url = json.loads(text)["data"]["url"]
        except Exception:
            return None
        url = url.replace("http://", "https://", 1)
        if "tmpfiles.org/dl/" not in url:
            url = url.replace("tmpfiles.org/", "tmpfiles.org/dl/", 1)
        return url
    first = text.splitlines()[0].strip()
    if first.startswith("http") and " " not in first:
        return first
    return None


def _remove_file(path):
    if not path:
        return
    try:
        os.remove(path)
    except Exception:
        pass
    parent = os.path.dirname(path)
    if os.path.basename(parent).startswith("upl_"):
        try:
            os.rmdir(parent)
        except Exception:
            pass


def _hashed_name(filename):
    filename = os.path.basename(filename or "") or "file"
    tag = "".join(random.choices(string.ascii_lowercase + string.digits, k=5))
    if filename.lower().endswith(".tar.gz"):
        stem, ext = filename[:-7], filename[-7:]
    else:
        stem, ext = os.path.splitext(filename)
    return f"{stem or 'file'}_{tag}{ext}"


def _safe_name(filename):
    safe = re.sub(r"[^\w.\-]", "_", filename, flags=re.UNICODE)
    return safe[-120:] or "file"


def _escape(text):
    if not text:
        return ""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _format_bytes(b):
    if b < 1024:
        return f"{b} B"
    if b < 1024 * 1024:
        return f"{b / 1024:.1f} KB"
    if b < 1024 * 1024 * 1024:
        return f"{b / (1024 * 1024):.1f} MB"
    return f"{b / (1024 * 1024 * 1024):.2f} GB"


def _format_time(seconds):
    m, s = divmod(int(seconds), 60)
    ms = int((seconds - int(seconds)) * 100)
    if m > 0:
        return f"{m}:{s:02d}.{ms:02d}"
    return f"{s}.{ms:02d}s"


def _detect_type(name):
    if not name:
        return "unknown"
    ext = name.rsplit(".", 1)[-1].lower() if "." in name else ""
    types = {
        "png": "image/png",
        "jpg": "image/jpeg",
        "jpeg": "image/jpeg",
        "heif": "image/heif",
        "heic": "image/heic",
        "gif": "image/gif",
        "webp": "image/webp",
        "bmp": "image/bmp",
        "svg": "image/svg+xml",
        "mp4": "video/mp4",
        "mov": "video/quicktime",
        "avi": "video/x-msvideo",
        "mkv": "video/x-matroska",
        "webm": "video/webm",
        "mp3": "audio/mpeg",
        "wav": "audio/wav",
        "ogg": "audio/ogg",
        "flac": "audio/flac",
        "aac": "audio/aac",
        "py": "text/x-python",
        "sh": "text/x-shellscript",
        "go": "text/x-go",
        "swift": "text/x-swift",
        "ts": "text/typescript",
        "tsx": "text/typescript",
        "jsx": "text/javascript",
        "java": "text/x-java",
        "kt": "text/x-kotlin",
        "c": "text/x-c",
        "h": "text/x-c",
        "cpp": "text/x-c++",
        "hpp": "text/x-c++",
        "cs": "text/x-csharp",
        "rb": "text/x-ruby",
        "php": "text/x-php",
        "rs": "text/x-rust",
        "lua": "text/x-lua",
        "dart": "text/x-dart",
        "sql": "text/x-sql",
        "r": "text/x-r",
        "scala": "text/x-scala",
        "pl": "text/x-perl",
        "txt": "text/plain",
        "json": "application/json",
        "xml": "text/xml",
        "html": "text/html",
        "css": "text/css",
        "js": "text/javascript",
        "md": "text/markdown",
        "csv": "text/csv",
        "log": "text/plain",
        "cfg": "text/plain",
        "ini": "text/plain",
        "yaml": "text/yaml",
        "yml": "text/yaml",
        "toml": "application/toml",
        "zip": "application/zip",
        "skill": "application/zip",
        "rar": "application/x-rar",
        "7z": "application/x-7z-compressed",
        "tar": "application/x-tar",
        "gz": "application/gzip",
        "apk": "application/vnd.android.package-archive",
        "ipa": "application/octet-stream",
        "pdf": "application/pdf",
        "doc": "application/msword",
        "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "xls": "application/vnd.ms-excel",
        "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        "tgs": "application/x-tgsticker",
        "gia": "application/octet-stream",
        "gil": "application/octet-stream",
    }
    return types.get(ext, f"file/{ext}" if ext else "unknown")


ALLOWED_EXTENSIONS = {
    "png", "jpg", "jpeg", "heif", "heic", "gif", "webp", "bmp", "svg",
    "mp4", "mov", "avi", "mkv", "webm",
    "mp3", "wav", "ogg", "flac", "aac",
    "py", "sh", "go", "swift", "ts", "tsx", "jsx", "java", "kt",
    "c", "h", "cpp", "hpp", "cs", "rb", "php", "rs", "lua", "dart",
    "sql", "r", "scala", "pl",
    "txt", "json", "xml", "html", "css", "js", "md", "csv",
    "log", "cfg", "ini", "yaml", "yml", "toml",
    "zip", "rar", "7z", "tar", "gz", "skill",
    "apk", "ipa", "pdf", "doc", "docx", "xls", "xlsx", "pptx",
    "tgs", "gia", "gil",
}


@loader.tds
class Uploader(loader.Module):
    """Upload files to temp.sh, uguu.se, tmpfiles.org, x0.at or litterbox"""

    strings = {
        "name": "Uploader",
        "help": (
            "<b>Uploader</b>\n"
            "<blockquote>"
            "<code>{prefix}upl</code> - reply to photo/video/file/album to upload"
            "</blockquote>\n"
            "<b>Supported formats:</b>\n"
            "<blockquote>"
            "Images: png, jpeg, jpg, heif, heic, gif, webp, bmp, svg\n"
            "Video: mp4, mov, avi, mkv, webm\n"
            "Audio: mp3, wav, ogg, flac, aac\n"
            "Text: py, sh, go, swift, ts, tsx, jsx, java, kt, c, h, cpp, hpp, cs, rb, php, rs, lua, dart, sql, r, scala, pl, txt, json, xml, html, css, js, md, csv, log, cfg, ini, yaml, yml, toml\n"
            "Archives: zip, rar, 7z, tar, gz, skill\n"
            "Docs: pdf, doc, docx, xls, xlsx, pptx\n"
            "Other: apk, ipa, tgs, gia, gil"
            "</blockquote>"
        ),
        "no_reply": (
            "<b>No reply</b>\n"
            "<blockquote>Reply to a photo, video or file</blockquote>"
        ),
        "unsupported": (
            "<b>Unsupported format</b>\n"
            "<blockquote>"
            "Use <code>{prefix}upl</code> without args to see supported formats"
            "</blockquote>"
        ),
        "too_large": (
            "<b>File too large</b>\n"
            "<blockquote>"
            "Max allowed size: 512 MB\n"
            "File size: <code>{size}</code>"
            "</blockquote>"
        ),
        "downloading": (
            "<b>Downloading</b>\n"
            "<blockquote><code>{time}</code></blockquote>"
        ),
        "choose_host": (
            "<b>Downloaded</b>\n"
            "<blockquote>"
            "Name: <code>{name}</code>\n"
            "Size: <code>{size}</code>"
            "</blockquote>\n"
            "<b>Choose upload destination:</b>"
        ),
        "uploading": (
            "<b>Uploading to {host}</b>\n"
            "<blockquote><code>{time}</code></blockquote>"
        ),
        "done": (
            "<b>Uploaded</b>\n"
            "<b>Link:</b>\n"
            "<blockquote><a href=\"{url}\">{url}</a></blockquote>\n"
            "<b>File:</b>\n"
            "<blockquote>"
            "Name: <code>{name}</code>\n"
            "Type: <code>{type}</code>\n"
            "Size: <code>{size}</code>"
            "</blockquote>\n"
            "<b>Time:</b>\n"
            "<blockquote>"
            "Download: <code>{dl_time}</code>\n"
            "Upload: <code>{ul_time}</code>\n"
            "Total: <code>{total_time}</code>"
            "</blockquote>"
        ),
        "upload_fail": (
            "<b>Upload failed</b>\n"
            "<blockquote><code>{error}</code></blockquote>"
        ),
        "download_fail": (
            "<b>Download failed</b>\n"
            "<blockquote><code>{error}</code></blockquote>"
        ),
        "killed": "<b>Upload cancelled</b>",
        "kill_stale": "Download is already finished or cancelled",
        "album_choose": (
            "<b>Album detected</b>\n"
            "<blockquote>Files in album: <code>{count}</code></blockquote>\n"
            "<b>What to upload?</b>"
        ),
        "btn_album_all": "Whole album (.tar.gz)",
        "btn_album_one": "Only replied",
        "btn_close": "Close",
        "btn_back": "Back",
        "btn_tempsh": "temp.sh (3d)",
        "btn_uguu": "uguu.se",
        "btn_tmpfiles": "tmpfiles.org",
        "btn_x0": "x0.at",
        "btn_litter": "litterbox (72h)",
        "btn_kill": "Kill",
    }

    strings_ru = {
        "help": (
            "<b>Uploader</b>\n"
            "<blockquote>"
            "<code>{prefix}upl</code> - реплай на фото/видео/файл/альбом для загрузки"
            "</blockquote>\n"
            "<b>Поддерживаемые форматы:</b>\n"
            "<blockquote>"
            "Изображения: png, jpeg, jpg, heif, heic, gif, webp, bmp, svg\n"
            "Видео: mp4, mov, avi, mkv, webm\n"
            "Аудио: mp3, wav, ogg, flac, aac\n"
            "Текст: py, sh, go, swift, ts, tsx, jsx, java, kt, c, h, cpp, hpp, cs, rb, php, rs, lua, dart, sql, r, scala, pl, txt, json, xml, html, css, js, md, csv, log, cfg, ini, yaml, yml, toml\n"
            "Архивы: zip, rar, 7z, tar, gz, skill\n"
            "Документы: pdf, doc, docx, xls, xlsx, pptx\n"
            "Другое: apk, ipa, tgs, gia, gil"
            "</blockquote>"
        ),
        "no_reply": (
            "<b>Нет реплая</b>\n"
            "<blockquote>Ответьте на фото, видео или файл</blockquote>"
        ),
        "unsupported": (
            "<b>Неподдерживаемый формат</b>\n"
            "<blockquote>"
            "Используйте <code>{prefix}upl</code> без аргументов для списка форматов"
            "</blockquote>"
        ),
        "too_large": (
            "<b>Файл слишком большой</b>\n"
            "<blockquote>"
            "Максимальный размер: 512 МБ\n"
            "Размер файла: <code>{size}</code>"
            "</blockquote>"
        ),
        "downloading": (
            "<b>Скачивание</b>\n"
            "<blockquote><code>{time}</code></blockquote>"
        ),
        "choose_host": (
            "<b>Скачано</b>\n"
            "<blockquote>"
            "Имя: <code>{name}</code>\n"
            "Размер: <code>{size}</code>"
            "</blockquote>\n"
            "<b>Выберите куда загрузить:</b>"
        ),
        "uploading": (
            "<b>Загрузка на {host}</b>\n"
            "<blockquote><code>{time}</code></blockquote>"
        ),
        "done": (
            "<b>Загружено</b>\n"
            "<b>Ссылка:</b>\n"
            "<blockquote><a href=\"{url}\">{url}</a></blockquote>\n"
            "<b>Файл:</b>\n"
            "<blockquote>"
            "Имя: <code>{name}</code>\n"
            "Тип: <code>{type}</code>\n"
            "Размер: <code>{size}</code>"
            "</blockquote>\n"
            "<b>Время:</b>\n"
            "<blockquote>"
            "Скачивание: <code>{dl_time}</code>\n"
            "Загрузка: <code>{ul_time}</code>\n"
            "Всего: <code>{total_time}</code>"
            "</blockquote>"
        ),
        "upload_fail": (
            "<b>Ошибка загрузки</b>\n"
            "<blockquote><code>{error}</code></blockquote>"
        ),
        "download_fail": (
            "<b>Ошибка скачивания</b>\n"
            "<blockquote><code>{error}</code></blockquote>"
        ),
        "killed": "<b>Загрузка отменена</b>",
        "kill_stale": "Загрузка уже завершена или отменена",
        "album_choose": (
            "<b>Обнаружен альбом</b>\n"
            "<blockquote>Файлов в альбоме: <code>{count}</code></blockquote>\n"
            "<b>Что загрузить?</b>"
        ),
        "btn_album_all": "Весь альбом (.tar.gz)",
        "btn_album_one": "Только реплай",
        "btn_close": "Закрыть",
        "btn_back": "Назад",
        "btn_tempsh": "temp.sh (3d)",
        "btn_uguu": "uguu.se",
        "btn_tmpfiles": "tmpfiles.org",
        "btn_x0": "x0.at",
        "btn_litter": "litterbox (72h)",
        "btn_kill": "Kill",
    }

    def __init__(self):
        self._upload_procs = {}
        self._pending_files = {}
        self._albums = {}
        self._dl_tasks = {}

    def _s(self, key, **kwargs):
        prefix = self.get_prefix()
        text = self.strings.get(key, "")
        try:
            return text.format(prefix=prefix, **kwargs)
        except (KeyError, IndexError):
            return text

    def _get_filename(self, media):
        attrs = getattr(media, "attributes", []) or []
        for attr in attrs:
            name = getattr(attr, "file_name", None)
            if name:
                return name
        if hasattr(media, "mime_type"):
            mime = media.mime_type or ""
            ext_map = {
                "image/png": "file.png",
                "image/jpeg": "file.jpg",
                "image/heif": "file.heif",
                "image/heic": "file.heic",
                "image/gif": "file.gif",
                "image/webp": "file.webp",
                "image/bmp": "file.bmp",
                "image/svg+xml": "file.svg",
                "video/mp4": "file.mp4",
                "video/quicktime": "file.mov",
                "video/x-msvideo": "file.avi",
                "video/x-matroska": "file.mkv",
                "video/webm": "file.webm",
                "audio/mpeg": "file.mp3",
                "audio/wav": "file.wav",
                "audio/x-wav": "file.wav",
                "audio/ogg": "file.ogg",
                "audio/flac": "file.flac",
                "audio/aac": "file.aac",
                "text/x-python": "file.py",
                "text/x-shellscript": "file.sh",
                "text/plain": "file.txt",
                "application/json": "file.json",
                "text/xml": "file.xml",
                "text/html": "file.html",
                "text/css": "file.css",
                "text/javascript": "file.js",
                "text/markdown": "file.md",
                "text/csv": "file.csv",
                "application/zip": "file.zip",
                "application/x-rar": "file.rar",
                "application/x-7z-compressed": "file.7z",
                "application/x-tar": "file.tar",
                "application/gzip": "file.gz",
                "application/pdf": "file.pdf",
                "application/vnd.android.package-archive": "file.apk",
                "application/x-tgsticker": "file.tgs",
            }
            if mime in ext_map:
                return ext_map[mime]
        return "file"

    def _get_extension(self, filename):
        if "." in filename:
            return filename.rsplit(".", 1)[-1].lower()
        return ""

    def _get_file_size(self, media):
        doc = getattr(media, "document", None)
        if doc and hasattr(doc, "size"):
            return doc.size
        if hasattr(media, "size") and isinstance(getattr(media, "size"), int):
            return media.size
        photo = getattr(media, "photo", None)
        if photo:
            sizes = getattr(photo, "sizes", []) or []
            for s in reversed(sizes):
                sz = getattr(s, "size", None)
                if isinstance(sz, int) and sz > 0:
                    return sz
        return None

    def _cleanup(self, uid, tmp_path):
        self._upload_procs.pop(uid, None)
        self._pending_files.pop(uid, None)
        _remove_file(tmp_path)

    def _make_curl_args(self, host, tmp_path):
        ua = UA
        if host == "tempsh":
            return [
                "curl", "-sL", "--max-time", "120",
                "-A", ua,
                "-F", f"file=@{tmp_path}",
                "https://temp.sh/upload",
            ]
        if host == "uguu":
            return [
                "curl", "-sL", "--max-time", "120",
                "-A", ua,
                "-F", f"files[]=@{tmp_path}",
                "https://uguu.se/upload?output=text",
            ]
        if host == "tmpfiles":
            return [
                "curl", "-sL", "--max-time", "120",
                "-A", ua,
                "-F", f"file=@{tmp_path}",
                "https://tmpfiles.org/api/v1/upload",
            ]
        if host == "x0.at":
            return [
                "curl", "-sL", "--max-time", "120",
                "-A", ua,
                "-F", f"file=@{tmp_path}",
                "https://x0.at",
            ]
        if host == "litterbox":
            return [
                "curl", "-sL", "--max-time", "120",
                "-A", ua,
                "-F", "reqtype=fileupload",
                "-F", "time=72h",
                "-F", f"fileToUpload=@{tmp_path}",
                "https://litterbox.catbox.moe/resources/internals/api.php",
            ]
        return []

    async def _cb_close(self, call: InlineCall, uid=None):
        if uid is not None:
            self._albums.pop(uid, None)
            data = self._pending_files.pop(uid, None)
            self._upload_procs.pop(uid, None)
            if data:
                _remove_file(data[0])
        await call.delete()

    def _dl_kill_rows(self, uid):
        return [[{
            "text": self.strings["btn_kill"],
            "callback": self._cb_dl_kill,
            "args": (uid,),
            "style": "danger",
        }]]

    async def _cb_dl_kill(self, call: InlineCall, uid):
        rec = self._dl_tasks.get(uid)
        if not rec:
            try:
                await call.answer(self._s("kill_stale"))
            except Exception:
                pass
            return
        rec["killed"] = True
        rec["stop"].set()
        task = rec.get("task")
        if task is not None and not task.done():
            task.cancel()
        if rec.get("path"):
            _remove_file(rec["path"])
        await call.edit(
            self._s("killed"),
            reply_markup=[[{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}]],
        )

    async def _cb_kill(self, call: InlineCall, uid):
        proc = self._upload_procs.get(uid)
        if proc:
            try:
                proc.kill()
            except Exception:
                pass
        data = self._pending_files.pop(uid, None)
        self._upload_procs.pop(uid, None)
        if data:
            _remove_file(data[0])
        await call.edit(
            self._s("killed"),
            reply_markup=[[{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}]],
        )

    def _host_rows(self, uid):
        buttons = [
            {
                "text": self.strings[prov["btn"]],
                "callback": self._cb_upload,
                "args": (uid, key),
                "style": "primary",
            }
            for key, prov in PROVIDERS.items()
        ]
        rows = [buttons[i:i + 2] for i in range(0, len(buttons), 2)]
        rows.append([{"text": self.strings["btn_close"], "callback": self._cb_close, "args": (uid,), "style": "danger"}])
        return rows

    def _fail_rows(self, uid):
        return [
            [{"text": self.strings["btn_back"], "callback": self._cb_back, "args": (uid,), "style": "primary"}],
            [{"text": self.strings["btn_close"], "callback": self._cb_close, "args": (uid,), "style": "danger"}],
        ]

    async def _fail(self, form, uid, error):
        self._upload_procs.pop(uid, None)
        await form.edit(
            self._s("upload_fail", error=error),
            reply_markup=self._fail_rows(uid),
        )

    async def _cb_back(self, call: InlineCall, uid):
        data = self._pending_files.get(uid)
        if not data or not os.path.exists(data[0]):
            self._pending_files.pop(uid, None)
            await call.edit(
                self._s("upload_fail", error="temp file not found"),
                reply_markup=[[{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}]],
            )
            return
        tmp_path, filename, file_type, file_size, dl_elapsed = data
        await call.edit(
            self._s("choose_host", name=_escape(filename), size=_format_bytes(file_size)),
            reply_markup=self._host_rows(uid),
        )

    async def _cb_upload(self, call: InlineCall, uid, host):
        data = self._pending_files.get(uid)
        if not data or host not in PROVIDERS:
            return
        tmp_path, filename, file_type, file_size, dl_elapsed = data
        await self._do_upload(call, uid, tmp_path, filename, file_type, file_size, dl_elapsed, host)

    async def _do_upload(self, form, uid, tmp_path, filename, file_type, file_size, dl_elapsed, host):
        if not os.path.exists(tmp_path):
            await form.edit(
                self._s("upload_fail", error="temp file not found"),
                reply_markup=[[{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}]],
            )
            return

        stop_event = asyncio.Event()
        ul_start = time.time()
        host_label = PROVIDERS.get(host, {}).get("label", host)

        async def _timer():
            while not stop_event.is_set():
                elapsed = time.time() - ul_start
                try:
                    await form.edit(
                        self._s("uploading", host=host_label, time=_format_time(elapsed)),
                        reply_markup=[[
                            {
                                "text": self.strings["btn_kill"],
                                "callback": self._cb_kill,
                                "args": (uid,),
                                "style": "danger",
                            }
                        ]],
                    )
                except Exception:
                    pass
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=1.7)
                    break
                except asyncio.TimeoutError:
                    pass

        timer_task = asyncio.ensure_future(_timer())

        try:
            args = self._make_curl_args(host, tmp_path)

            proc = await asyncio.create_subprocess_exec(
                *args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            self._upload_procs[uid] = proc

            try:
                out, err = await asyncio.wait_for(proc.communicate(), timeout=130)
            except asyncio.TimeoutError:
                stop_event.set()
                await timer_task
                await self._fail(form, uid, "timeout")
                return

            ul_elapsed = time.time() - ul_start
            stop_event.set()
            await timer_task

            stdout_text = out.decode("utf-8", errors="replace").strip()
            stderr_text = err.decode("utf-8", errors="replace").strip()

            if uid not in self._pending_files:
                return

            if proc.returncode != 0:
                error = stderr_text or stdout_text or f"exit code {proc.returncode}"
                await self._fail(form, uid, _escape(error[:300]))
                return

            if not stdout_text:
                error = stderr_text or "empty response"
                await self._fail(form, uid, _escape(error[:300]))
                return

            url = _parse_url(host, stdout_text)
            if not url:
                await self._fail(form, uid, _escape(stdout_text[:300]))
                return

            self._cleanup(uid, tmp_path)
            total_elapsed = dl_elapsed + ul_elapsed

            await form.edit(
                self._s(
                    "done",
                    url=_escape(url),
                    name=_escape(filename),
                    type=_escape(file_type),
                    size=_format_bytes(file_size),
                    dl_time=_format_time(dl_elapsed),
                    ul_time=_format_time(ul_elapsed),
                    total_time=_format_time(total_elapsed),
                ),
                reply_markup=[[{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}]],
            )

        except FileNotFoundError:
            stop_event.set()
            await timer_task
            self._cleanup(uid, tmp_path)
            await form.edit(
                self._s("upload_fail", error="curl not found"),
                reply_markup=[[{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}]],
            )
        except Exception as e:
            stop_event.set()
            try:
                await timer_task
            except Exception:
                pass
            await self._fail(form, uid, _escape(str(e)[:300]))

    async def _get_album_messages(self, chat_id, message_id, grouped_id):
        album_messages = []
        async for msg in self._client.iter_messages(chat_id, max_id=message_id + 15, limit=30):
            if msg.grouped_id == grouped_id and msg.media and not isinstance(msg.media, MessageMediaWebPage):
                album_messages.append(msg)
        return sorted(album_messages, key=lambda m: m.id)

    def _close_rows(self):
        return [[{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}]]

    async def _reject(self, text, message=None, call=None):
        if call is not None:
            await call.edit(text, reply_markup=self._close_rows())
            return
        await self.inline.form(
            text=text,
            message=message,
            reply_markup=self._close_rows(),
            silent=True,
        )

    async def _build_archive(self, album, tmp_path):
        work = tempfile.mkdtemp()
        try:
            names = []
            for i, m in enumerate(album, 1):
                if m.photo:
                    name = "photo.jpg"
                else:
                    media = m.media
                    name = self._get_filename(getattr(media, "document", None) or media)
                name = f"{i:02d}_{os.path.basename(name)}"
                await m.download_media(file=os.path.join(work, name))
                names.append(name)

            def _pack():
                with tarfile.open(tmp_path, "w:gz") as tar:
                    for n in names:
                        tar.add(os.path.join(work, n), arcname=n)

            await asyncio.get_event_loop().run_in_executor(None, _pack)
        finally:
            shutil.rmtree(work, ignore_errors=True)

    async def _download_flow(self, form, filename, file_type, fetch, uid):
        filename = _hashed_name(filename)
        stop_event = asyncio.Event()
        dl_start = time.time()
        self._dl_tasks[uid] = {"task": None, "path": None, "stop": stop_event, "killed": False}
        kill_rows = self._dl_kill_rows(uid)

        async def _dl_timer():
            while not stop_event.is_set():
                elapsed = time.time() - dl_start
                try:
                    await form.edit(self._s("downloading", time=_format_time(elapsed)), reply_markup=kill_rows)
                except Exception:
                    pass
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=1.7)
                    break
                except asyncio.TimeoutError:
                    pass

        timer_task = asyncio.ensure_future(_dl_timer())
        tmp_path = None

        try:
            tmp_path = os.path.join(tempfile.mkdtemp(prefix="upl_"), _safe_name(filename))
            self._dl_tasks[uid]["path"] = tmp_path

            try:
                task = asyncio.ensure_future(fetch(tmp_path))
                self._dl_tasks[uid]["task"] = task
                await task
            except asyncio.CancelledError:
                stop_event.set()
                try:
                    await timer_task
                except Exception:
                    pass
                _remove_file(tmp_path)
                self._dl_tasks.pop(uid, None)
                return
            self._dl_tasks.pop(uid, None)
            dl_elapsed = time.time() - dl_start
            stop_event.set()
            await timer_task

            file_size = os.path.getsize(tmp_path)
            if file_size > MAX_FILE_SIZE:
                _remove_file(tmp_path)
                await form.edit(
                    self._s("too_large", size=_format_bytes(file_size)),
                    reply_markup=self._close_rows(),
                )
                return

            self._pending_files[uid] = (tmp_path, filename, file_type, file_size, dl_elapsed)

            await form.edit(
                self._s("choose_host", name=_escape(filename), size=_format_bytes(file_size)),
                reply_markup=self._host_rows(uid),
            )

        except Exception as e:
            stop_event.set()
            try:
                await timer_task
            except Exception:
                pass
            await form.edit(
                self._s("download_fail", error=_escape(str(e)[:300])),
                reply_markup=self._close_rows(),
            )
            _remove_file(tmp_path)

    async def _process_single(self, reply, message=None, call=None):
        media = reply.media
        doc = getattr(media, "document", None) or media
        filename = self._get_filename(doc)
        ext = self._get_extension(filename)

        if hasattr(media, "photo") or type(media).__name__ == "MessageMediaPhoto":
            ext = "jpg"
            filename = "photo.jpg"

        if ext not in ALLOWED_EXTENSIONS:
            await self._reject(self._s("unsupported"), message, call)
            return

        file_size_meta = self._get_file_size(media)
        if file_size_meta is not None and file_size_meta > MAX_FILE_SIZE:
            await self._reject(self._s("too_large", size=_format_bytes(file_size_meta)), message, call)
            return

        file_type = _detect_type(filename)

        if call is not None:
            form = call
            uid = str(id(form))
            await form.edit(
                self._s("downloading", time="0.00s"),
                reply_markup=self._dl_kill_rows(uid),
            )
        else:
            form = await self.inline.form(
                text=self._s("downloading", time="0.00s"),
                message=message,
                reply_markup=self._close_rows(),
                silent=True,
            )
            uid = str(id(form))

        async def _fetch(path):
            await reply.download_media(file=path)

        await self._download_flow(form, filename, file_type, _fetch, uid)

    async def _process_album(self, album, call):
        total = 0
        for m in album:
            size = self._get_file_size(m.media)
            if size:
                total += size
        if total > MAX_FILE_SIZE:
            await self._reject(self._s("too_large", size=_format_bytes(total)), call=call)
            return

        uid = str(id(call))
        await call.edit(
            self._s("downloading", time="0.00s"),
            reply_markup=self._dl_kill_rows(uid),
        )

        async def _fetch(path):
            await self._build_archive(album, path)

        await self._download_flow(call, "album.tar.gz", _detect_type("album.tar.gz"), _fetch, uid)

    async def _cb_album_all(self, call: InlineCall, uid):
        data = self._albums.pop(uid, None)
        if not data:
            await self._reject(self._s("upload_fail", error="album not found"), call=call)
            return
        await self._process_album(data[1], call)

    async def _cb_album_one(self, call: InlineCall, uid):
        data = self._albums.pop(uid, None)
        if not data:
            await self._reject(self._s("upload_fail", error="album not found"), call=call)
            return
        await self._process_single(data[0], call=call)

    @loader.command(
        ru_doc="Реплай на фото/видео/файл/альбом - загрузить на temp.sh, uguu, tmpfiles, x0.at или litterbox",
        en_doc="Reply to photo/video/file/album - upload to temp.sh, uguu, tmpfiles, x0.at or litterbox",
    )
    async def upl(self, message):
        """Reply to photo/video/file/album - upload to temp.sh, uguu, tmpfiles, x0.at or litterbox"""
        reply = await message.get_reply_message()

        if not reply or not reply.media:
            await self._reject(self._s("no_reply"), message)
            return

        if reply.grouped_id:
            try:
                album = await self._get_album_messages(reply.chat_id, reply.id, reply.grouped_id)
            except Exception:
                album = []
            if len(album) > 1:
                uid = f"{reply.chat_id}_{reply.id}_{message.id}"
                self._albums[uid] = (reply, album)
                await self.inline.form(
                    text=self._s("album_choose", count=len(album)),
                    message=message,
                    reply_markup=[
                        [
                            {
                                "text": self.strings["btn_album_all"],
                                "callback": self._cb_album_all,
                                "args": (uid,),
                                "style": "primary",
                            },
                            {
                                "text": self.strings["btn_album_one"],
                                "callback": self._cb_album_one,
                                "args": (uid,),
                                "style": "primary",
                            },
                        ],
                        [{"text": self.strings["btn_close"], "callback": self._cb_close, "args": (uid,), "style": "danger"}],
                    ],
                    silent=True,
                )
                return

        await self._process_single(reply, message=message)