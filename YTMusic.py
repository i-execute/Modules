__version__ = (2, 0, 1)
# meta developer: I_execute.t.me
# meta banner: https://raw.githubusercontent.com/i-execute/Modules/main/Storage/YTMusic/MetaBanner.jpeg

import asyncio
import io
import logging
import os
import re
import shutil
import sys
import tempfile
import time
import typing

from telethon.tl.types import Message, DocumentAttributeAudio
from .. import loader, utils
from ..inline.types import InlineCall

logger = logging.getLogger(__name__)

DEPS = ["yt-dlp", "aiohttp", "Pillow", "mutagen"]


def _install_deps():
    import importlib
    import subprocess
    pip = os.path.join(os.path.dirname(sys.executable), "pip")
    if not os.path.exists(pip):
        pip = "pip"
    imp_map = {"yt-dlp": "yt_dlp", "Pillow": "PIL", "aiohttp": "aiohttp", "mutagen": "mutagen"}
    lines = []
    for pkg in DEPS:
        try:
            subprocess.run(
                [pip, "install", "-U", pkg, "--break-system-packages", "-q"],
                capture_output=True, text=True, timeout=120,
            )
            mod = importlib.import_module(imp_map.get(pkg, pkg))
            ver = getattr(mod, "__version__", "?")
            lines.append(f"{pkg}: OK ({ver})")
        except Exception as e:
            lines.append(f"{pkg}: FAIL ({e})")
    return lines


_dep_log = _install_deps()

try:
    import yt_dlp
    YT_DLP_OK = True
except ImportError:
    yt_dlp = None
    YT_DLP_OK = False

try:
    import aiohttp
    AIOHTTP_OK = True
except ImportError:
    aiohttp = None
    AIOHTTP_OK = False

try:
    from PIL import Image
    PIL_OK = True
except ImportError:
    Image = None
    PIL_OK = False

try:
    from mutagen.id3 import ID3, TIT2, TPE1, APIC, ID3NoHeaderError
    MUTAGEN_OK = True
except ImportError:
    ID3 = TIT2 = TPE1 = APIC = ID3NoHeaderError = None
    MUTAGEN_OK = False

MAX_FILE_SIZE = 50 * 1024 * 1024
REQUEST_OK = 200

YT_URL_RE = re.compile(
    r"(?:https?://)?(?:www\.|m\.)?"
    r"(?:"
    r"youtu\.be/([a-zA-Z0-9_-]{11})"
    r"|youtube\.com/watch\?(?:[^&]*&)*v=([a-zA-Z0-9_-]{11})"
    r"|youtube\.com/shorts/([a-zA-Z0-9_-]{11})"
    r"|youtube\.com/live/([a-zA-Z0-9_-]{11})"
    r"|music\.youtube\.com/watch\?(?:[^&]*&)*v=([a-zA-Z0-9_-]{11})"
    r")",
    re.IGNORECASE,
)

INNERTUBE_API_URL = "https://www.youtube.com/youtubei/v1/search"
INNERTUBE_KEY = "AIzaSyAO_FJ2SlqU8Q4STEHLGCilw_Y9_11qcW8"
INNERTUBE_CONTEXT = {
    "client": {
        "clientName": "WEB",
        "clientVersion": "2.20240101",
        "hl": "ru",
        "gl": "RU",
    }
}
HEADERS_WEB = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
}
THUMB_KEYS = ["maxresdefault", "sddefault", "hqdefault", "mqdefault", "default"]

LOG_ENTRIES = []
MAX_LOG = 300


def _log(tag: str, msg: str):
    ts = time.strftime("%H:%M:%S")
    entry = f"[{ts}][YTMusic][{tag}] {msg}"
    LOG_ENTRIES.append(entry)
    if len(LOG_ENTRIES) > MAX_LOG:
        LOG_ENTRIES.pop(0)
    logger.info(entry)


def escape_html(t: str) -> str:
    return (t or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def sanitize_fn(n: str) -> str:
    return re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", n).strip(". ")[:180] or "track"


def normalize_cover(raw: bytes, max_size: typing.Optional[int] = None, force_jpeg: bool = False) -> typing.Optional[bytes]:
    if not PIL_OK or not raw or len(raw) < 100:
        return None
    try:
        img = Image.open(io.BytesIO(raw))
        w, h = img.size
        needs_resize = max_size is not None and (w > max_size or h > max_size)
        img = img.convert("RGB")
        if needs_resize:
            ratio = min(max_size / w, max_size / h)
            img = img.resize((int(w * ratio), int(h * ratio)), Image.LANCZOS)
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=95)
        result = buf.getvalue()
        return result if len(result) >= 100 else None
    except Exception:
        return None


async def _download_image(url: str) -> typing.Optional[bytes]:
    if not AIOHTTP_OK or not url:
        return None
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=15), allow_redirects=True) as r:
                if r.status != REQUEST_OK:
                    return None
                data = await r.read()
                return data if len(data) >= 500 else None
    except Exception as e:
        _log("DL_IMG", f"Error {url[:80]}: {e}")
        return None


async def _check_thumb(session, url: str) -> bool:
    if not AIOHTTP_OK:
        return False
    try:
        async with session.head(url, timeout=aiohttp.ClientTimeout(total=5), allow_redirects=True) as r:
            if r.status == REQUEST_OK:
                cl = r.headers.get("Content-Length", "")
                if cl and int(cl) < 1000:
                    return False
                return True
    except Exception:
        pass
    return False


async def _best_thumb_url(video_id: str) -> typing.Optional[str]:
    if not AIOHTTP_OK:
        return f"https://img.youtube.com/vi/{video_id}/hqdefault.jpg"
    candidates = {k: f"https://img.youtube.com/vi/{video_id}/{k}.jpg" for k in THUMB_KEYS}
    async with aiohttp.ClientSession(headers=HEADERS_WEB) as s:
        results = await asyncio.gather(
            *[_check_thumb(s, url) for url in candidates.values()],
            return_exceptions=True,
        )
    for key, ok in zip(THUMB_KEYS, results):
        if ok is True:
            return candidates[key]
    return f"https://img.youtube.com/vi/{video_id}/hqdefault.jpg"


def _extract_video_id(text: str) -> typing.Optional[str]:
    m = YT_URL_RE.search(text)
    if not m:
        return None
    for group in m.groups():
        if group:
            return group
    return None


async def _search_innertube(query: str, limit: int = 5) -> list:
    if not AIOHTTP_OK:
        return []
    payload = {
        "query": query,
        "context": INNERTUBE_CONTEXT,
        "params": "EgIQAQ%3D%3D",
    }
    try:
        async with aiohttp.ClientSession(headers=HEADERS_WEB) as s:
            async with s.post(
                f"{INNERTUBE_API_URL}?key={INNERTUBE_KEY}",
                json=payload,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as r:
                if r.status != REQUEST_OK:
                    return []
                data = await r.json()
    except Exception as e:
        _log("INNERTUBE", f"Request failed: {e}")
        return []
    results = []
    try:
        contents = (
            data
            .get("contents", {})
            .get("twoColumnSearchResultsRenderer", {})
            .get("primaryContents", {})
            .get("sectionListRenderer", {})
            .get("contents", [])
        )
        for section in contents:
            items = section.get("itemSectionRenderer", {}).get("contents", [])
            for item in items:
                vr = item.get("videoRenderer")
                if not vr:
                    continue
                video_id = vr.get("videoId")
                if not video_id:
                    continue
                title = vr.get("title", {}).get("runs", [{}])[0].get("text", "Unknown")
                channel = vr.get("ownerText", {}).get("runs", [{}])[0].get("text", "")
                dur_str = vr.get("lengthText", {}).get("simpleText", "?:??")
                results.append({
                    "video_id": video_id,
                    "title": title,
                    "channel": channel,
                    "dur_str": dur_str,
                })
                if len(results) >= limit:
                    break
            if len(results) >= limit:
                break
    except Exception as e:
        _log("INNERTUBE", f"Parse failed: {e}")
    return results


async def _search_ytdlp(query: str, limit: int = 5) -> list:
    if not YT_DLP_OK:
        return []
    try:
        loop = asyncio.get_event_loop()
        opts = {"quiet": True, "no_warnings": True, "extract_flat": True, "skip_download": True}

        def _run():
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(f"ytsearch{limit}:{query}", download=False)
                return info.get("entries", []) if info else []

        entries = await loop.run_in_executor(None, _run)
        results = []
        for e in entries[:limit]:
            vid = e.get("id") or ""
            if not vid:
                continue
            dur_s = int(e.get("duration") or 0)
            dur_str = f"{dur_s // 60}:{dur_s % 60:02d}" if dur_s else "?:??"
            results.append({
                "video_id": vid,
                "title": e.get("title", "Unknown"),
                "channel": e.get("channel") or e.get("uploader", ""),
                "dur_str": dur_str,
            })
        return results
    except Exception as e:
        _log("YTDLP_SEARCH", f"Failed: {e}")
        return []


def _embed_id3(filepath: str, title: str, artist: str, cover_data: typing.Optional[bytes]):
    if not MUTAGEN_OK:
        return
    try:
        try:
            tags = ID3(filepath)
        except ID3NoHeaderError:
            tags = ID3()
        tags.add(TIT2(encoding=3, text=[title or "Unknown"]))
        tags.add(TPE1(encoding=3, text=[artist or "Unknown"]))
        if cover_data and len(cover_data) > 500:
            tags.add(APIC(encoding=3, mime="image/jpeg", type=3, desc="Cover", data=cover_data))
        tags.save(filepath)
    except Exception:
        pass


async def _upload_to_x0(data: bytes, filename: str, content_type: str = "audio/mpeg") -> str:
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
                if text.startswith("http"):
                    return text
    except Exception:
        pass
    return ""


@loader.tds
class YTMusic(loader.Module):
    """YouTube Music - search and download audio from YouTube"""

    strings = {
        "name": "YTMusic",
        "no_results": "<b>Nothing found</b>",
        "provide_query": "<b>Provide a search query</b>",
        "searching": "<b>Searching</b> <code>{query}</code>",
        "uploading": "<b>Uploading...</b>",
        "download_fail": "<b>Download failed.</b> Try again or use cookies.",
        "error": "<b>Error:</b> {msg}",
        "downloading_track": "<b>Downloading</b> <code>{title}</code>",
        "btn_download": "⬇️ Download",
        "btn_cancel": "✖️ Close",
        "btn_new_search": "🔍 New search",
        "btn_left": "⬅️",
        "btn_right": "➡️",
        "menu_title": "<b>YouTube Music</b>\n<blockquote>Search a track</blockquote>",
        "via_link": "Via link",
        "via_query": "Via query",
        "enter_link": "Enter YouTube link:",
        "enter_query": "Enter search query:",
        "link_not_found": "<b>Video not found by link</b>",
        "cookies_menu_title": (
            "<b>YTMusic - Cookies</b>\n"
            "<blockquote>Cookies allow downloading age-restricted or region-locked videos.\n"
            "Status: {status}</blockquote>"
        ),
        "cookies_active": "Active",
        "cookies_inactive": "Not set",
        "cookies_set_btn": "Set cookies",
        "cookies_clear_btn": "Clear cookies",
        "cookies_toggle_btn": "Cookies: {val}",
        "cookies_saved": "<b>Cookies saved!</b>",
        "cookies_cleared": "<b>Cookies cleared.</b>",
        "cookies_invalid": "<b>Invalid cookies format!</b>",
        "applying_cookies": "<b>Saving cookies...</b>",
        "clearing_cookies": "<b>Clearing cookies...</b>",
        "no_reply_file": "<b>Reply to a .txt file with cookies</b>",
        "invalid_ext": "<b>File must be .txt</b>",
        "cookie_err": "<b>Error:</b> {0}",
    }

    strings_ru = {
        "no_results": "<b>Ничего не найдено</b>",
        "provide_query": "<b>Укажите поисковый запрос</b>",
        "searching": "<b>Поиск</b> <code>{query}</code>",
        "uploading": "<b>Загрузка...</b>",
        "download_fail": "<b>Ошибка скачивания.</b> Попробуйте снова или используйте куки.",
        "error": "<b>Ошибка:</b> {msg}",
        "downloading_track": "<b>Загружаю</b> <code>{title}</code>",
        "btn_download": "⬇️ Скачать",
        "btn_cancel": "✖️ Закрыть",
        "btn_new_search": "🔍 Новый поиск",
        "btn_left": "⬅️",
        "btn_right": "➡️",
        "menu_title": "<b>YouTube Music</b>\n<blockquote>Поиск трека</blockquote>",
        "via_link": "По ссылке",
        "via_query": "По запросу",
        "enter_link": "Введите ссылку на YouTube:",
        "enter_query": "Введите поисковый запрос:",
        "link_not_found": "<b>Видео по ссылке не найдено</b>",
        "cookies_menu_title": (
            "<b>YTMusic - Куки</b>\n"
            "<blockquote>Куки позволяют скачивать видео с возрастными ограничениями или региональной блокировкой.\n"
            "Статус: {status}</blockquote>"
        ),
        "cookies_active": "Активны",
        "cookies_inactive": "Не установлены",
        "cookies_set_btn": "Установить куки",
        "cookies_clear_btn": "Удалить куки",
        "cookies_toggle_btn": "Куки: {val}",
        "cookies_saved": "<b>Куки сохранены!</b>",
        "cookies_cleared": "<b>Куки удалены.</b>",
        "cookies_invalid": "<b>Неверный формат куки!</b>",
        "applying_cookies": "<b>Сохраняю куки...</b>",
        "clearing_cookies": "<b>Удаляю куки...</b>",
        "no_reply_file": "<b>Ответь на .txt файл с куками</b>",
        "invalid_ext": "<b>Файл должен быть .txt</b>",
        "cookie_err": "<b>Ошибка:</b> {0}",
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "SEARCH_LIMIT", 5,
                "Search results limit (1-10)",
                validator=loader.validators.Integer(minimum=1, maximum=10),
            ),
            loader.ConfigValue(
                "YT_COOKIES", "",
                "Cookies for yt-dlp (contents of .txt file)",
                validator=loader.validators.Hidden(),
            ),
            loader.ConfigValue(
                "COOKIES_ENABLED", True,
                "Use cookies if available",
                validator=loader.validators.Boolean(),
            ),
        )
        self._tmp = None
        self._me_id = None
        self._upload_lock = None
        self._yts_sessions = {}
        self._yts_locks = {}
        self._cover_cache = {}

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        self._upload_lock = asyncio.Lock()
        me = await client.get_me()
        self._me_id = me.id
        self._tmp = os.path.join(tempfile.gettempdir(), f"YTMusic_{me.id}")
        if os.path.exists(self._tmp):
            shutil.rmtree(self._tmp, ignore_errors=True)
        os.makedirs(self._tmp, exist_ok=True)

    async def on_unload(self):
        self._yts_sessions.clear()
        self._yts_locks.clear()
        self._cover_cache.clear()
        if self._tmp and os.path.exists(self._tmp):
            shutil.rmtree(self._tmp, ignore_errors=True)

    def _get_limit(self):
        try:
            return max(1, min(10, int(self.config["SEARCH_LIMIT"])))
        except Exception:
            return 5

    def _cookies_file(self) -> typing.Optional[str]:
        cookies_txt = self.config["YT_COOKIES"]
        if not cookies_txt or not self.config["COOKIES_ENABLED"]:
            return None
        path = os.path.join(self._tmp, "cookies.txt")
        with open(path, "w", encoding="utf-8") as f:
            f.write(cookies_txt)
        return path

    def _has_cookies(self) -> bool:
        return bool(self.config["YT_COOKIES"].strip())

    def _validate_cookies(self, content: str) -> bool:
        if not content:
            return False
        lower = content.lower()
        return "# netscape" in lower or "youtube.com" in lower or "TRUE" in content

    async def _is_forum_chat(self, message):
        try:
            chat = await message.get_chat()
            return getattr(chat, "forum", False)
        except Exception:
            return False

    def _get_topic_id(self, message):
        try:
            if message.reply_to and hasattr(message.reply_to, "reply_to_top_id"):
                return message.reply_to.reply_to_top_id or message.reply_to.reply_to_msg_id
        except Exception:
            pass
        return None

    @loader.command(
        ru_doc="Управление куками для yt-dlp. Реплай на .txt для установки",
        en_doc="Manage yt-dlp cookies. Reply to .txt file to set",
    )
    async def ytcookies(self, message: Message):
        """Manage cookies (reply to .txt file to set)"""
        reply = await message.get_reply_message()
        if reply and reply.file:
            fname = getattr(reply.file, "name", "") or ""
            if not fname.lower().endswith(".txt"):
                await utils.answer(message, self.strings["invalid_ext"])
                return
            try:
                data = await reply.download_media(bytes)
                if not data:
                    await utils.answer(message, self.strings["invalid_ext"])
                    return
                cookie_content = data.decode("utf-8", errors="replace")
                if not self._validate_cookies(cookie_content):
                    await utils.answer(message, self.strings["cookies_invalid"])
                    return
                self.config["YT_COOKIES"] = cookie_content
                self.config["COOKIES_ENABLED"] = True
                await utils.answer(message, self.strings["cookies_saved"])
            except Exception as e:
                await utils.answer(message, self.strings["cookie_err"].format(str(e)))
            return

        has = self._has_cookies()
        enabled = self.config["COOKIES_ENABLED"] if has else False
        status = self.strings["cookies_active"] if (has and enabled) else self.strings["cookies_inactive"]
        
        markup = []
        if has:
            val_str = "On" if enabled else "Off"
            markup.append([{
                "text": self.strings["cookies_toggle_btn"].format(val=val_str),
                "callback": self._cb_toggle_cookies,
                "style": "success" if enabled else "danger",
            }])
            markup.append([{
                "text": self.strings["cookies_clear_btn"],
                "callback": self._cb_clear_cookies,
                "style": "danger",
            }])
        
        await self.inline.form(
            text=self.strings["cookies_menu_title"].format(status=status),
            message=message,
            reply_markup=markup,
            silent=True,
        )

    async def _cb_toggle_cookies(self, call: InlineCall):
        current = self.config["COOKIES_ENABLED"]
        self.config["COOKIES_ENABLED"] = not current
        has = self._has_cookies()
        enabled = self.config["COOKIES_ENABLED"]
        status = self.strings["cookies_active"] if (has and enabled) else self.strings["cookies_inactive"]
        val_str = "On" if enabled else "Off"
        await call.edit(
            text=self.strings["cookies_menu_title"].format(status=status),
            reply_markup=[
                [{
                    "text": self.strings["cookies_toggle_btn"].format(val=val_str),
                    "callback": self._cb_toggle_cookies,
                    "style": "success" if enabled else "danger",
                }],
                [{
                    "text": self.strings["cookies_clear_btn"],
                    "callback": self._cb_clear_cookies,
                    "style": "danger",
                }],
            ],
        )

    async def _cb_clear_cookies(self, call: InlineCall):
        await call.edit(self.strings["clearing_cookies"])
        await asyncio.sleep(0.8)
        self.config["YT_COOKIES"] = ""
        self.config["COOKIES_ENABLED"] = False
        status = self.strings["cookies_inactive"]
        await call.edit(
            text=self.strings["cookies_menu_title"].format(status=status),
            reply_markup=[],
        )

    async def _prepare_track(self, video_id: str, ddir: str) -> typing.Tuple[typing.Optional[dict], typing.Optional[str]]:
        if not YT_DLP_OK:
            return None, "yt-dlp not installed"

        yt_url = f"https://youtu.be/{video_id}"
        cookies = self._cookies_file()
        loop = asyncio.get_event_loop()
        info_holder = {}

        def _extract():
            opts = {"quiet": True, "no_warnings": True, "socket_timeout": 30}
            if cookies:
                opts["cookiefile"] = cookies
            with yt_dlp.YoutubeDL(opts) as ydl:
                info_holder["info"] = ydl.extract_info(yt_url, download=False)

        try:
            await loop.run_in_executor(None, _extract)
        except Exception as e:
            return None, str(e)[:120]

        info = info_holder.get("info") or {}
        title = info.get("title", "Unknown") or "Unknown"
        artist = info.get("uploader") or info.get("channel") or "YouTube"
        dur_s = int(info.get("duration") or 0)
        thumbnail_url = info.get("thumbnail", "")

        safe_name = sanitize_fn(f"{artist} - {title}")
        out_tmpl = os.path.join(ddir, f"{safe_name}.%(ext)s")

        def _download():
            opts = {
                "outtmpl": out_tmpl,
                "quiet": True,
                "no_warnings": True,
                "format": "bestaudio/best",
                "postprocessors": [{
                    "key": "FFmpegExtractAudio",
                    "preferredcodec": "mp3",
                    "preferredquality": "320",
                }],
                "noplaylist": True,
            }
            if cookies:
                opts["cookiefile"] = cookies
            with yt_dlp.YoutubeDL(opts) as ydl:
                ydl.download([yt_url])

        try:
            await loop.run_in_executor(None, _download)
        except Exception as e:
            return None, str(e)[:120]

        audio_exts = (".mp3", ".m4a", ".opus", ".ogg", ".wav", ".webm")
        found = None
        for fn in os.listdir(ddir):
            if any(fn.endswith(ext) for ext in audio_exts):
                found = os.path.join(ddir, fn)
                break

        if not found or not os.path.exists(found):
            return None, "audio file not found after download"

        final_mp3 = os.path.join(ddir, f"{safe_name}.mp3")
        if not found.endswith(".mp3"):
            proc = await asyncio.create_subprocess_exec(
                "ffmpeg", "-hide_banner", "-loglevel", "error",
                "-y", "-i", found, "-vn", "-acodec", "libmp3lame", "-ab", "320k", final_mp3,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
            )
            await asyncio.wait_for(proc.communicate(), timeout=120)
            if proc.returncode == 0 and os.path.exists(final_mp3) and os.path.getsize(final_mp3) > 0:
                try:
                    os.remove(found)
                except Exception:
                    pass
            else:
                final_mp3 = found
        else:
            if found != final_mp3:
                try:
                    os.rename(found, final_mp3)
                except Exception:
                    final_mp3 = found

        if not os.path.exists(final_mp3) or os.path.getsize(final_mp3) == 0:
            return None, "empty file"

        if os.path.getsize(final_mp3) > MAX_FILE_SIZE:
            return None, "too_large"

        raw_cover = await _download_image(thumbnail_url) if thumbnail_url else None
        cover_data = normalize_cover(raw_cover) if raw_cover else None
        thumb_data = normalize_cover(raw_cover, max_size=320) if raw_cover else None

        if cover_data and final_mp3.endswith(".mp3"):
            cover_path = os.path.join(ddir, "cover.jpg")
            covered_mp3 = os.path.join(ddir, f"{safe_name}_cover.mp3")
            with open(cover_path, "wb") as cf:
                cf.write(cover_data)
            proc = await asyncio.create_subprocess_exec(
                "ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
                "-i", final_mp3, "-i", cover_path,
                "-map", "0:a", "-map", "1:0",
                "-c:a", "copy", "-c:v", "copy",
                "-id3v2_version", "3",
                "-metadata:s:v", "title=Cover",
                "-metadata:s:v", "comment=Cover (front)",
                "-disposition:v", "attached_pic", covered_mp3,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
            )
            await asyncio.wait_for(proc.communicate(), timeout=60)
            if proc.returncode == 0 and os.path.exists(covered_mp3) and os.path.getsize(covered_mp3) > 0:
                try:
                    os.remove(final_mp3)
                except Exception:
                    pass
                final_mp3 = covered_mp3
            else:
                _embed_id3(final_mp3, title, artist, cover_data)
        elif final_mp3.endswith(".mp3"):
            _embed_id3(final_mp3, title, artist, None)

        return {
            "path": final_mp3,
            "title": title,
            "artist": artist,
            "dur_s": dur_s,
            "thumb_data": thumb_data,
        }, None

    async def _send_audio(self, chat_id, info: dict, reply_to=None, retries: int = 3) -> bool:
        with open(info["path"], "rb") as f:
            mp3_bytes = f.read()
        thumb_bytes = info.get("thumb_data")
        last_err = None
        for attempt in range(retries):
            try:
                audio_buf = io.BytesIO(mp3_bytes)
                audio_buf.name = os.path.basename(info["path"])
                thumb_buf = None
                if thumb_bytes:
                    thumb_buf = io.BytesIO(thumb_bytes)
                    thumb_buf.name = "cover.jpg"
                await self._client.send_file(
                    chat_id,
                    file=audio_buf,
                    caption=None,
                    attributes=[
                        DocumentAttributeAudio(
                            duration=info["dur_s"],
                            title=info["title"],
                            performer=info["artist"],
                        )
                    ],
                    thumb=thumb_buf,
                    voice=False,
                    reply_to=reply_to,
                )
                return True
            except Exception as e:
                last_err = e
                _log("SEND_AUDIO", f"attempt {attempt + 1}/{retries} failed: {e}")
                if attempt != retries - 1:
                    await asyncio.sleep(2 * (attempt + 1))
        _log("SEND_AUDIO", f"gave up: {last_err}")
        return False

    @loader.command(
        ru_doc="Поиск трека на YouTube. Без аргументов открывает форму выбора",
        en_doc="Search track on YouTube. Without args opens selection form",
    )
    async def yts(self, message: Message):
        """Search YouTube track."""
        query = utils.get_args_raw(message).strip()
        is_forum = await self._is_forum_chat(message)
        topic_id = self._get_topic_id(message) if is_forum else None

        if not query:
            session_id = str(id(message))
            self._yts_sessions[session_id] = {
                "chat_id": message.chat_id,
                "is_forum": is_forum,
                "topic_id": topic_id,
            }
            form_kwargs = dict(
                text=self.strings["menu_title"],
                message=message,
                reply_markup=[[
                    {
                        "text": self.strings["via_link"],
                        "input": self.strings["enter_link"],
                        "handler": self._yts_link_input,
                        "args": (session_id,),
                        "style": "primary",
                    },
                    {
                        "text": self.strings["via_query"],
                        "input": self.strings["enter_query"],
                        "handler": self._yts_query_input,
                        "args": (session_id,),
                        "style": "primary",
                    },
                ]],
                silent=True,
            )
            if is_forum and topic_id:
                form_kwargs["reply_to"] = topic_id
            await self.inline.form(**form_kwargs)
            return

        vid = _extract_video_id(query)
        if vid:
            await self._yts_download_by_id(message, vid, is_forum, topic_id)
            return

        await self._yts_run_search(message, query, is_forum, topic_id)

    async def _yts_link_input(self, call: InlineCall, text: str, session_id: str):
        url = text.strip()
        sess = self._yts_sessions.get(session_id, {})
        vid = _extract_video_id(url)
        if not vid:
            await call.edit(self.strings["link_not_found"])
            return
        await call.edit(self.strings["uploading"])
        chat_id = sess.get("chat_id")
        is_forum = sess.get("is_forum", False)
        topic_id = sess.get("topic_id")
        self._yts_sessions.pop(session_id, None)
        ddir = tempfile.mkdtemp(dir=self._tmp)
        try:
            info, err = await self._prepare_track(vid, ddir)
            if err:
                await call.edit(self.strings["download_fail"])
                return
            await self._send_audio(chat_id, info, reply_to=topic_id if is_forum and topic_id else None)
            await call.delete()
        finally:
            shutil.rmtree(ddir, ignore_errors=True)

    async def _yts_query_input(self, call: InlineCall, text: str, session_id: str):
        query = text.strip()
        sess = self._yts_sessions.get(session_id, {})
        if not query:
            await call.edit(self.strings["no_results"])
            return
        await call.edit(self.strings["searching"].format(query=escape_html(query)))
        limit = self._get_limit()
        tracks = await _search_innertube(query, limit=limit)
        if not tracks:
            tracks = await _search_ytdlp(query, limit=limit)
        if not tracks:
            await call.edit(self.strings["no_results"])
            return
        chat_id = sess.get("chat_id")
        is_forum = sess.get("is_forum", False)
        topic_id = sess.get("topic_id")
        new_sid = session_id + "_s"
        self._yts_sessions[new_sid] = {
            "tracks": tracks,
            "index": 0,
            "chat_id": chat_id,
            "is_forum": is_forum,
            "topic_id": topic_id,
            "cover_cache": {},
        }
        self._yts_locks[new_sid] = asyncio.Lock()
        cover_url = await self._yts_get_cover(new_sid, 0)
        markup = self._yts_markup(new_sid, len(tracks))
        track = tracks[0]
        edit_kwargs = dict(
            text=f"<b>{escape_html(track['title'])}</b>\n<b>{escape_html(track['channel'])}</b>\n<blockquote>{track['dur_str']}</blockquote>",
            reply_markup=markup,
        )
        if cover_url:
            edit_kwargs["photo"] = cover_url
        await call.edit(**edit_kwargs)
        asyncio.ensure_future(self._yts_prefetch_covers(new_sid))

    async def _yts_download_by_id(self, message, vid: str, is_forum: bool, topic_id):
        msg = await utils.answer(message, self.strings["uploading"])
        ddir = tempfile.mkdtemp(dir=self._tmp)
        try:
            info, err = await self._prepare_track(vid, ddir)
            if err:
                await utils.answer(msg, self.strings["download_fail"])
                return
            try:
                await msg.delete()
            except Exception:
                pass
            await self._send_audio(message.chat_id, info, reply_to=topic_id if is_forum and topic_id else None)
        finally:
            shutil.rmtree(ddir, ignore_errors=True)

    async def _yts_run_search(self, message, query: str, is_forum: bool, topic_id):
        msg = await utils.answer(message, self.strings["searching"].format(query=escape_html(query)))
        limit = self._get_limit()
        tracks = await _search_innertube(query, limit=limit)
        if not tracks:
            tracks = await _search_ytdlp(query, limit=limit)
        if not tracks:
            await utils.answer(msg, self.strings["no_results"])
            return
        session_id = str(id(message))
        self._yts_sessions[session_id] = {
            "tracks": tracks,
            "index": 0,
            "chat_id": message.chat_id,
            "is_forum": is_forum,
            "topic_id": topic_id,
            "cover_cache": {},
        }
        self._yts_locks[session_id] = asyncio.Lock()
        cover_url = await self._yts_get_cover(session_id, 0)
        markup = self._yts_markup(session_id, len(tracks))
        track = tracks[0]
        try:
            await msg.delete()
        except Exception:
            pass
        form_kwargs = dict(
            text=f"<b>{escape_html(track['title'])}</b>\n<b>{escape_html(track['channel'])}</b>\n<blockquote>{track['dur_str']}</blockquote>",
            message=message,
            reply_markup=markup,
            silent=True,
        )
        if cover_url:
            form_kwargs["photo"] = cover_url
        if is_forum and topic_id:
            form_kwargs["reply_to"] = topic_id
        await self.inline.form(**form_kwargs)
        asyncio.ensure_future(self._yts_prefetch_covers(session_id))

    async def _yts_get_cover(self, session_id: str, idx: int) -> typing.Optional[str]:
        sess = self._yts_sessions.get(session_id)
        if not sess:
            return None
        cache = sess["cover_cache"]
        if idx in cache:
            return cache[idx]
        track = sess["tracks"][idx]
        vid = track["video_id"]
        if vid in self._cover_cache:
            cache[idx] = self._cover_cache[vid]
            return cache[idx]
        thumb_url = await _best_thumb_url(vid)
        raw = await _download_image(thumb_url) if thumb_url else None
        x0_url = None
        if raw:
            norm = normalize_cover(raw, force_jpeg=True) or raw
            x0_url = await _upload_to_x0(
                norm,
                sanitize_fn(f"{track['channel']} - {track['title']}") + ".jpg",
                "image/jpeg",
            )
        self._cover_cache[vid] = x0_url
        cache[idx] = x0_url
        return x0_url

    async def _yts_prefetch_covers(self, session_id: str):
        sess = self._yts_sessions.get(session_id)
        if not sess:
            return
        for i in range(1, len(sess["tracks"])):
            if session_id not in self._yts_sessions:
                return
            if i not in sess["cover_cache"]:
                await self._yts_get_cover(session_id, i)
            await asyncio.sleep(0.3)

    def _yts_markup(self, session_id: str, total: int):
        sess = self._yts_sessions.get(session_id, {})
        idx = sess.get("index", 0)
        left_btn = {"text": self.strings["btn_left"], "callback": self._yts_left, "args": (session_id,)}
        right_btn = {"text": self.strings["btn_right"], "callback": self._yts_right, "args": (session_id,)}
        if idx > 0:
            left_btn["style"] = "primary"
        if idx < total - 1:
            right_btn["style"] = "primary"
        return [
            [{"text": self.strings["btn_download"], "callback": self._yts_download, "args": (session_id,), "style": "success"}],
            [left_btn, right_btn],
            [{"text": self.strings["btn_cancel"], "callback": self._yts_cancel, "args": (session_id,), "style": "danger"}],
        ]

    def _yts_done_markup(self, session_id: str):
        return [
            [{
                "text": self.strings["btn_new_search"],
                "input": self.strings["enter_query"],
                "handler": self._yts_new_search_input,
                "args": (session_id,),
                "style": "primary",
            }],
            [{"text": self.strings["btn_cancel"], "callback": self._yts_cancel, "args": (session_id,), "style": "danger"}],
        ]

    async def _yts_left(self, call: InlineCall, session_id: str):
        sess = self._yts_sessions.get(session_id)
        if not sess or sess["index"] <= 0:
            await call.answer()
            return
        lock = self._yts_locks.get(session_id)
        if lock and lock.locked():
            await call.answer()
            return
        async with lock:
            sess["index"] -= 1
            await self._yts_update(call, session_id)

    async def _yts_right(self, call: InlineCall, session_id: str):
        sess = self._yts_sessions.get(session_id)
        if not sess or sess["index"] >= len(sess["tracks"]) - 1:
            await call.answer()
            return
        lock = self._yts_locks.get(session_id)
        if lock and lock.locked():
            await call.answer()
            return
        async with lock:
            sess["index"] += 1
            await self._yts_update(call, session_id)

    async def _yts_update(self, call: InlineCall, session_id: str):
        sess = self._yts_sessions[session_id]
        idx = sess["index"]
        track = sess["tracks"][idx]
        cover_url = await self._yts_get_cover(session_id, idx)
        markup = self._yts_markup(session_id, len(sess["tracks"]))
        edit_kwargs = dict(
            text=f"<b>{escape_html(track['title'])}</b>\n<b>{escape_html(track['channel'])}</b>\n<blockquote>{track['dur_str']}</blockquote>",
            reply_markup=markup,
        )
        if cover_url:
            edit_kwargs["photo"] = cover_url
        await call.edit(**edit_kwargs)

    async def _yts_download(self, call: InlineCall, session_id: str):
        sess = self._yts_sessions.get(session_id)
        if not sess:
            await call.answer()
            return
        lock = self._yts_locks.get(session_id)
        if lock and lock.locked():
            await call.answer()
            return
        async with lock:
            track = sess["tracks"][sess["index"]]
            vid = track["video_id"]
            title = track["title"]
            channel = track["channel"]
            chat_id = sess["chat_id"]
            is_forum = sess.get("is_forum", False)
            topic_id = sess.get("topic_id")
            try:
                await call.edit(
                    self.strings["downloading_track"].format(title=escape_html(f"{channel} - {title}")),
                    reply_markup=[],
                )
            except Exception:
                pass
            ddir = tempfile.mkdtemp(dir=self._tmp)
            try:
                info, err = await self._prepare_track(vid, ddir)
                if err:
                    try:
                        await call.edit(
                            self.strings["download_fail"],
                            reply_markup=self._yts_done_markup(session_id),
                        )
                    except Exception:
                        pass
                    return
                await self._send_audio(chat_id, info, reply_to=topic_id if is_forum and topic_id else None)
            finally:
                shutil.rmtree(ddir, ignore_errors=True)
            try:
                await call.edit(
                    f"<b>{escape_html(channel)} — {escape_html(title)}</b>",
                    reply_markup=self._yts_done_markup(session_id),
                )
            except Exception:
                pass

    async def _yts_new_search_input(self, call: InlineCall, text: str, session_id: str):
        query = text.strip()
        if not query:
            await call.delete()
            return
        sess = self._yts_sessions.get(session_id, {})
        await call.edit(self.strings["searching"].format(query=escape_html(query)))
        limit = self._get_limit()
        tracks = await _search_innertube(query, limit=limit)
        if not tracks:
            tracks = await _search_ytdlp(query, limit=limit)
        if not tracks:
            await call.edit(self.strings["no_results"], reply_markup=self._yts_done_markup(session_id))
            return
        self._yts_sessions[session_id] = {
            "tracks": tracks,
            "index": 0,
            "chat_id": sess.get("chat_id"),
            "is_forum": sess.get("is_forum", False),
            "topic_id": sess.get("topic_id"),
            "cover_cache": {},
        }
        self._yts_locks[session_id] = asyncio.Lock()
        cover_url = await self._yts_get_cover(session_id, 0)
        markup = self._yts_markup(session_id, len(tracks))
        track = tracks[0]
        edit_kwargs = dict(
            text=f"<b>{escape_html(track['title'])}</b>\n<b>{escape_html(track['channel'])}</b>\n<blockquote>{track['dur_str']}</blockquote>",
            reply_markup=markup,
        )
        if cover_url:
            edit_kwargs["photo"] = cover_url
        await call.edit(**edit_kwargs)
        asyncio.ensure_future(self._yts_prefetch_covers(session_id))

    async def _yts_cancel(self, call: InlineCall, session_id: str):
        self._yts_sessions.pop(session_id, None)
        self._yts_locks.pop(session_id, None)
        await call.delete()