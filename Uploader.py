__version__ = (1, 2, 0)
# meta developer: I_execute.t.me

import os
import asyncio
import logging
import time
import tempfile

from .. import loader, utils
from ..inline.types import InlineCall

logger = logging.getLogger(__name__)

MAX_FILE_SIZE = 512 * 1024 * 1024


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
        "rar": "application/x-rar",
        "7z": "application/x-7z-compressed",
        "tar": "application/x-tar",
        "gz": "application/gzip",
        "apk": "application/vnd.android.package-archive",
        "pdf": "application/pdf",
        "doc": "application/msword",
        "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "xls": "application/vnd.ms-excel",
        "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        "tgs": "application/x-tgsticker",
    }
    return types.get(ext, f"file/{ext}" if ext else "unknown")


ALLOWED_EXTENSIONS = {
    "png", "jpg", "jpeg", "heif", "heic", "gif", "webp", "bmp", "svg",
    "mp4", "mov", "avi", "mkv", "webm",
    "mp3", "wav", "ogg", "flac", "aac",
    "py", "sh", "txt", "json", "xml", "html", "css", "js", "md", "csv",
    "log", "cfg", "ini", "yaml", "yml", "toml",
    "zip", "rar", "7z", "tar", "gz",
    "apk", "pdf", "doc", "docx", "xls", "xlsx", "pptx",
    "tgs",
}


@loader.tds
class Uploader(loader.Module):
    """Upload files to x0.at or catbox.moe"""

    strings = {
        "name": "Uploader",
        "help": (
            "<b>Uploader</b>\n"
            "<blockquote>"
            "<code>{prefix}upl</code> - reply to photo/video/file to upload"
            "</blockquote>\n"
            "<b>Supported formats:</b>\n"
            "<blockquote>"
            "Images: png, jpeg, jpg, heif, heic, gif, webp, bmp, svg\n"
            "Video: mp4, mov, avi, mkv, webm\n"
            "Audio: mp3, wav, ogg, flac, aac\n"
            "Text: py, sh, txt, json, xml, html, css, js, md, csv, log, cfg, ini, yaml, yml, toml\n"
            "Archives: zip, rar, 7z, tar, gz\n"
            "Docs: pdf, doc, docx, xls, xlsx, pptx\n"
            "Other: apk, tgs"
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
        "killed": (
            "<b>Upload cancelled</b>"
        ),
        "btn_close": "Close",
        "btn_x0": "x0.at",
        "btn_catbox": "catbox.moe",
        "btn_kill": "Kill",
    }

    strings_ru = {
        "help": (
            "<b>Uploader</b>\n"
            "<blockquote>"
            "<code>{prefix}upl</code> - реплай на фото/видео/файл для загрузки"
            "</blockquote>\n"
            "<b>Поддерживаемые форматы:</b>\n"
            "<blockquote>"
            "Изображения: png, jpeg, jpg, heif, heic, gif, webp, bmp, svg\n"
            "Видео: mp4, mov, avi, mkv, webm\n"
            "Аудио: mp3, wav, ogg, flac, aac\n"
            "Текст: py, sh, txt, json, xml, html, css, js, md, csv, log, cfg, ini, yaml, yml, toml\n"
            "Архивы: zip, rar, 7z, tar, gz\n"
            "Документы: pdf, doc, docx, xls, xlsx, pptx\n"
            "Другое: apk, tgs"
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
        "killed": (
            "<b>Загрузка отменена</b>"
        ),
        "btn_close": "Закрыть",
        "btn_x0": "x0.at",
        "btn_catbox": "catbox.moe",
        "btn_kill": "Kill",
    }

    def __init__(self):
        self._upload_procs = {}

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

    async def _cb_close(self, call: InlineCall):
        await call.delete()

    async def _cb_kill(self, call: InlineCall, uid):
        proc = self._upload_procs.get(uid)
        if proc:
            try:
                proc.kill()
            except Exception:
                pass
        await call.edit(
            self._s("killed"),
            reply_markup=[[{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}]],
        )

    async def _cb_upload_x0(self, call: InlineCall, uid, tmp_path, filename, file_type, file_size, dl_elapsed):
        await self._do_upload(call, uid, tmp_path, filename, file_type, file_size, dl_elapsed, "x0.at")

    async def _cb_upload_catbox(self, call: InlineCall, uid, tmp_path, filename, file_type, file_size, dl_elapsed):
        await self._do_upload(call, uid, tmp_path, filename, file_type, file_size, dl_elapsed, "catbox.moe")

    async def _timer_loop(self, call: InlineCall, key, start_time, stop_event, extra=None):
        while not stop_event.is_set():
            elapsed = time.time() - start_time
            try:
                kwargs = {"time": _format_time(elapsed)}
                if extra:
                    kwargs.update(extra)
                await call.edit(self._s(key, **kwargs))
            except Exception:
                pass
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=1.7)
                break
            except asyncio.TimeoutError:
                pass

    async def _do_upload(self, form, uid, tmp_path, filename, file_type, file_size, dl_elapsed, host):
        stop_event = asyncio.Event()
        ul_start = time.time()

        async def _timer_with_kill():
            while not stop_event.is_set():
                elapsed = time.time() - ul_start
                try:
                    await form.edit(
                        self._s("uploading", host=host, time=_format_time(elapsed)),
                        reply_markup=[[
                            {"text": self.strings["btn_kill"], "callback": self._cb_kill, "args": (uid,), "style": "danger"}
                        ]],
                    )
                except Exception:
                    pass
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=1.7)
                    break
                except asyncio.TimeoutError:
                    pass

        timer_task = asyncio.ensure_future(_timer_with_kill())

        try:
            if host == "x0.at":
                cmd = [
                    "curl", "-sL", "--max-time", "120",
                    "-F", f"file=@{tmp_path}",
                    "https://x0.at",
                ]
            else:
                cmd = [
                    "curl", "-sL", "--max-time", "120",
                    "-F", "reqtype=fileupload",
                    "-F", f"fileToUpload=@{tmp_path}",
                    "https://catbox.moe/user/api.php",
                ]

            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            self._upload_procs[uid] = proc

            try:
                out, err = await asyncio.wait_for(proc.communicate(), timeout=130)
            except asyncio.TimeoutError:
                stop_event.set()
                await timer_task
                self._upload_procs.pop(uid, None)
                await form.edit(
                    self._s("upload_fail", error="timeout"),
                    reply_markup=[[{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}]],
                )
                return

            ul_elapsed = time.time() - ul_start
            stop_event.set()
            await timer_task
            self._upload_procs.pop(uid, None)

            if proc.returncode != 0 or not out:
                error = (err or b"").decode().strip() or f"exit code {proc.returncode}"
                await form.edit(
                    self._s("upload_fail", error=_escape(error[:300])),
                    reply_markup=[[{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}]],
                )
                return

            url = out.decode().strip()
            if not url.startswith("http"):
                await form.edit(
                    self._s("upload_fail", error=_escape(url[:300])),
                    reply_markup=[[{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}]],
                )
                return

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
            self._upload_procs.pop(uid, None)
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
            self._upload_procs.pop(uid, None)
            await form.edit(
                self._s("upload_fail", error=_escape(str(e)[:300])),
                reply_markup=[[{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}]],
            )

    @loader.command(
        ru_doc="Реплай на фото/видео/файл - загрузить на x0.at или catbox.moe",
        en_doc="Reply to photo/video/file - upload to x0.at or catbox.moe",
    )
    async def upl(self, message):
        """Reply to photo/video/file - upload to x0.at or catbox.moe"""
        reply = await message.get_reply_message()

        if not reply or not reply.media:
            await self.inline.form(
                text=self._s("no_reply"),
                message=message,
                reply_markup=[[{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}]],
                silent=True,
            )
            return

        media = reply.media
        doc = getattr(media, "document", None) or media
        filename = self._get_filename(doc)
        ext = self._get_extension(filename)

        if hasattr(media, "photo") or type(media).__name__ == "MessageMediaPhoto":
            ext = "jpg"
            filename = "photo.jpg"

        if ext not in ALLOWED_EXTENSIONS:
            await self.inline.form(
                text=self._s("unsupported"),
                message=message,
                reply_markup=[[{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}]],
                silent=True,
            )
            return

        file_size_meta = self._get_file_size(media)
        if file_size_meta is not None and file_size_meta > MAX_FILE_SIZE:
            await self.inline.form(
                text=self._s("too_large", size=_format_bytes(file_size_meta)),
                message=message,
                reply_markup=[[{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}]],
                silent=True,
            )
            return

        file_type = _detect_type(filename)

        form = await self.inline.form(
            text=self._s("downloading", time="0.00s"),
            message=message,
            reply_markup=[[{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}]],
            silent=True,
        )

        stop_event = asyncio.Event()
        dl_start = time.time()
        timer_task = asyncio.ensure_future(
            self._timer_loop(form, "downloading", dl_start, stop_event)
        )

        tmp_path = None
        try:
            tmp_fd = tempfile.NamedTemporaryFile(suffix=f".{ext}", delete=False)
            tmp_path = tmp_fd.name
            tmp_fd.close()

            await reply.download_media(file=tmp_path)
            dl_elapsed = time.time() - dl_start
            stop_event.set()
            await timer_task

            file_size = os.path.getsize(tmp_path)
            uid = str(id(form))

            await form.edit(
                self._s("choose_host", name=_escape(filename), size=_format_bytes(file_size)),
                reply_markup=[
                    [
                        {
                            "text": self.strings["btn_x0"],
                            "callback": self._cb_upload_x0,
                            "args": (uid, tmp_path, filename, file_type, file_size, dl_elapsed),
                            "style": "primary",
                        },
                        {
                            "text": self.strings["btn_catbox"],
                            "callback": self._cb_upload_catbox,
                            "args": (uid, tmp_path, filename, file_type, file_size, dl_elapsed),
                            "style": "primary",
                        },
                    ],
                    [{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}],
                ],
            )

        except Exception as e:
            stop_event.set()
            try:
                await timer_task
            except Exception:
                pass
            await form.edit(
                self._s("download_fail", error=_escape(str(e)[:300])),
                reply_markup=[[{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}]],
            )
            if tmp_path:
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass