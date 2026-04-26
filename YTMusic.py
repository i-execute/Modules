__version__ = (1, 1, 0)
# meta developer: I_execute.t.me
# requires: aiohttp, yt-dlp, mutagen, Pillow

import os
import io
import re
import time
import logging
import tempfile
import shutil
import asyncio
import traceback

import aiohttp
from PIL import Image

from aiogram.types import (
    InlineQuery,
    InlineQueryResultCachedAudio,
    InlineQueryResultArticle,
    InputTextMessageContent,
    BufferedInputFile,
    InputMediaAudio,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ChosenInlineResult,
    Update,
)

from telethon.tl.types import Message

from .. import loader, utils

logger = logging.getLogger(__name__)

try:
    import yt_dlp
    YT_DLP_OK = True
except ImportError:
    YT_DLP_OK = False

try:
    from mutagen.id3 import ID3, TIT2, TPE1, APIC, ID3NoHeaderError
    MUTAGEN_OK = True
except ImportError:
    MUTAGEN_OK = False

INNERTUBE_API_URL = "https://www.youtube.com/youtubei/v1/search"
INNERTUBE_KEY     = "AIzaSyAO_FJ2SlqU8Q4STEHLGCilw_Y9_11qcW8"
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

THUMB_KEYS    = ["maxresdefault", "sddefault", "hqdefault", "mqdefault", "default"]
MAX_FILE_SIZE = 50 * 1024 * 1024
REQUEST_OK    = 200

INLINE_QUERY_BANNER = "https://raw.githubusercontent.com/i-execute/Modules/main/Assets/YTMusic/Inline_query.png"
DOWNLOADING_STUB    = "https://raw.githubusercontent.com/i-execute/Modules/main/Assets/YTMusic/Downloading.mp3"

YT_URL_RE = re.compile(
    r"(?:https?://)?(?:www\.|m\.)??"
    r"(?:"
    r"youtu\.be/([a-zA-Z0-9_-]{11})"
    r"|youtube\.com/watch\?(?:[^&]*&)*?v=([a-zA-Z0-9_-]{11})"
    r"|youtube\.com/shorts/([a-zA-Z0-9_-]{11})"
    r"|youtube\.com/live/([a-zA-Z0-9_-]{11})"
    r"|youtube\.com/embed/([a-zA-Z0-9_-]{11})"
    r"|youtube\.com/v/([a-zA-Z0-9_-]{11})"
    r"|youtube\.com/e/([a-zA-Z0-9_-]{11})"
    r"|music\.youtube\.com/watch\?(?:[^&]*&)*?v=([a-zA-Z0-9_-]{11})"
    r"|youtube\.com/watch/([a-zA-Z0-9_-]{11})"
    r")",
    re.IGNORECASE,
)

LOG_ENTRIES = []
MAX_LOG     = 300


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


def normalize_cover(raw: bytes, max_size: int | None = None) -> bytes | None:
    if not raw or len(raw) < 100:
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


async def _download_image(url: str) -> bytes | None:
    if not url:
        return None
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=15), allow_redirects=True) as r:
                if r.status != REQUEST_OK:
                    return None
                data = await r.read()
                return data if len(data) >= 1000 else None
    except Exception as e:
        _log("DL_IMG", f"Error {url[:80]}: {e}")
        return None


async def _check_thumb(session: aiohttp.ClientSession, url: str) -> bool:
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


async def _best_thumb_for_inline(video_id: str) -> str:
    candidates = {k: f"https://img.youtube.com/vi/{video_id}/{k}.jpg" for k in THUMB_KEYS}
    async with aiohttp.ClientSession(headers=HEADERS_WEB) as s:
        results = await asyncio.gather(
            *[_check_thumb(s, url) for url in candidates.values()],
            return_exceptions=True,
        )
    for key, ok in zip(THUMB_KEYS, results):
        if ok is True:
            _log("THUMB", f"{video_id} best={key}")
            return candidates[key]
    fallback = f"https://img.youtube.com/vi/{video_id}/hqdefault.jpg"
    _log("THUMB", f"{video_id} fallback")
    return fallback


def _extract_video_id(text: str) -> str | None:
    m = YT_URL_RE.search(text)
    if not m:
        return None
    for group in m.groups():
        if group:
            return group
    return None


async def _search_innertube(query: str, limit: int = 5) -> list[dict]:
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
                    _log("INNERTUBE", f"HTTP {r.status}")
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
                title   = vr.get("title", {}).get("runs", [{}])[0].get("text", "Unknown")
                channel = vr.get("ownerText", {}).get("runs", [{}])[0].get("text", "")
                dur_str = vr.get("lengthText", {}).get("simpleText", "?:??")
                results.append({
                    "video_id": video_id,
                    "title":    title,
                    "channel":  channel,
                    "dur_str":  dur_str,
                    "yt_url":   f"https://youtu.be/{video_id}",
                })
                if len(results) >= limit:
                    break
            if len(results) >= limit:
                break
    except Exception as e:
        _log("INNERTUBE", f"Parse failed: {e}")

    _log("INNERTUBE", f"Found {len(results)} for {query!r}")
    return results


async def _search_ytdlp(query: str, limit: int = 5) -> list[dict]:
    if not YT_DLP_OK:
        return []
    try:
        loop = asyncio.get_event_loop()
        opts = {
            "quiet": True,
            "no_warnings": True,
            "extract_flat": True,
            "skip_download": True,
        }
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
            dur_s   = int(e.get("duration") or 0)
            dur_str = f"{dur_s // 60}:{dur_s % 60:02d}" if dur_s else "?:??"
            results.append({
                "video_id": vid,
                "title":    e.get("title", "Unknown"),
                "channel":  e.get("channel") or e.get("uploader", ""),
                "dur_str":  dur_str,
                "yt_url":   f"https://youtu.be/{vid}",
            })
        _log("YTDLP_SEARCH", f"Found {len(results)} for {query!r}")
        return results
    except Exception as e:
        _log("YTDLP_SEARCH", f"Failed: {e}")
        return []


async def _get_video_info_ytdlp(video_id: str, cookies: str | None = None) -> dict | None:
    if not YT_DLP_OK:
        return None
    try:
        loop = asyncio.get_event_loop()
        opts = {
            "quiet": True,
            "no_warnings": True,
            "extract_flat": False,
            "skip_download": True,
            "socket_timeout": 30,
        }
        if cookies:
            opts["cookiefile"] = cookies

        def _run():
            with yt_dlp.YoutubeDL(opts) as ydl:
                return ydl.extract_info(f"https://youtu.be/{video_id}", download=False)

        info = await loop.run_in_executor(None, _run)
        if not info:
            return None
        dur_s   = int(info.get("duration") or 0)
        dur_str = f"{dur_s // 60}:{dur_s % 60:02d}" if dur_s else "?:??"
        return {
            "video_id": video_id,
            "title":    info.get("title", "Unknown") or "Unknown",
            "channel":  info.get("uploader") or info.get("channel") or "YouTube",
            "dur_str":  dur_str,
            "yt_url":   f"https://youtu.be/{video_id}",
        }
    except Exception as e:
        _log("INFO_YTDLP", f"Failed for {video_id}: {e}")
        return None


def _embed_id3(filepath: str, title: str, artist: str, cover_data: bytes | None):
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


@loader.tds
class YTMusic(loader.Module):
    """YouTube Music - search and download audio from YouTube"""

    strings = {
        "name": "YTMusic",
        "cookies_saved":    "<b>Cookies saved!</b>",
        "cookies_cleared":  "<b>Cookies cleared</b>",
        "cookies_no_reply": "<b>Reply to a .txt file with cookies</b>",
        "cookies_bad_file": "<b>File must be .txt</b>",
        "not_found":        "Nothing found",
        "not_found_desc":   "Try a different query",
        "hint_title":       "YTMusic",
        "hint_desc":        "Type a track name or paste a YouTube link",
        "downloading":      "Downloading...",
        "link_not_found":   "Video not found",
        "link_not_found_desc": "Could not get video info by link",
    }

    strings_ru = {
        "cookies_saved":    "<b>Куки сохранены!</b>",
        "cookies_cleared":  "<b>Куки удалены</b>",
        "cookies_no_reply": "<b>Ответь на .txt файл с куками</b>",
        "cookies_bad_file": "<b>Файл должен быть .txt</b>",
        "not_found":        "Ничего не найдено",
        "not_found_desc":   "Попробуй другой запрос",
        "hint_title":       "YTMusic",
        "hint_desc":        "Введи название трека или вставь ссылку на YouTube",
        "downloading":      "Загрузка...",
        "link_not_found":   "Видео не найдено",
        "link_not_found_desc": "Не удалось получить информацию о видео по ссылке",
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
        )
        self._tmp:          str | None       = None
        self._me_id:        int | None       = None
        self._patched:      bool             = False
        self._upload_lock:  asyncio.Lock | None = None
        self._real_cache:   dict[str, tuple] = {}
        self._stub_cache:   dict[str, str]   = {}
        self._search_cache: dict[str, list]  = {}
        self._thumb_cache:  dict[str, str]   = {}

    async def client_ready(self, client, db):
        self._client = client
        self._db     = db
        self._upload_lock = asyncio.Lock()
        me = await client.get_me()
        self._me_id = me.id
        self._tmp   = os.path.join(tempfile.gettempdir(), f"YTMusic_{me.id}")
        if os.path.exists(self._tmp):
            shutil.rmtree(self._tmp, ignore_errors=True)
        os.makedirs(self._tmp, exist_ok=True)
        self.inline_bot = self.inline.bot
        await self._unpatch()
        self._patch()

    def _patch(self):
        if self._patched:
            return
        try:
            dp = self.inline._dp
            if getattr(dp.feed_update, "_ytmusic_patched", False):
                self._patched = True
                return
            orig = dp.feed_update
            dp._ytmusic_orig = orig
            mod = self

            async def patched(bot_inst, update: Update, **kw):
                if (
                    hasattr(update, "chosen_inline_result")
                    and update.chosen_inline_result is not None
                ):
                    chosen = update.chosen_inline_result
                    _log("CHOSEN_RAW", f"result_id={chosen.result_id!r} imid={chosen.inline_message_id!r}")
                    asyncio.ensure_future(mod._on_chosen(chosen))
                return await orig(bot_inst, update, **kw)

            patched._ytmusic_patched = True
            dp.feed_update = patched
            self._patched  = True
            _log("PATCH", "OK")
        except Exception as e:
            _log("PATCH", f"Failed: {e}\n{traceback.format_exc()}")

    async def _unpatch(self):
        if not self._patched:
            return
        try:
            dp = self.inline._dp
            if hasattr(dp, "_ytmusic_orig"):
                dp.feed_update = dp._ytmusic_orig
                del dp._ytmusic_orig
            self._patched = False
            _log("PATCH", "Unpatched")
        except Exception as e:
            _log("PATCH", f"Unpatch failed: {e}")

    async def _on_chosen(self, chosen: ChosenInlineResult):
        rid  = chosen.result_id
        imid = chosen.inline_message_id
        _log("CHOSEN", f"rid={rid!r} imid={imid!r}")
        if not rid.startswith("yt_") or not imid:
            return
        video_id = rid[3:]
        if video_id in self._real_cache:
            await self._do_replace(imid, self._real_cache[video_id])
            return
        asyncio.ensure_future(self._bg_dl_replace(video_id, imid))

    async def _bg_dl_replace(self, video_id: str, imid: str):
        _log("BG", f"Start dl video_id={video_id}")
        try:
            result = await self._dl_and_upload(video_id)
            if "error" in result:
                _log("BG", f"Error: {result['error']}")
                return
            data = (
                result["file_id"],
                result["title"],
                result["artist"],
                result["duration"],
            )
            self._real_cache[video_id] = data
            await self._do_replace(imid, data)
        except Exception as e:
            _log("BG", f"Exception: {e}\n{traceback.format_exc()}")

    async def _do_replace(self, imid: str, data: tuple):
        file_id, title, artist, duration = data
        _log("REPLACE", f"imid={imid!r} file_id={file_id!r}")
        for attempt, kwargs in enumerate([
            dict(
                media=InputMediaAudio(media=file_id, title=title, performer=artist, duration=duration),
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[]),
            ),
            dict(media=InputMediaAudio(media=file_id, title=title, performer=artist, duration=duration)),
            dict(media=InputMediaAudio(media=file_id)),
        ]):
            try:
                await self.inline_bot.edit_message_media(inline_message_id=imid, **kwargs)
                _log("REPLACE", f"OK attempt {attempt + 1}")
                return
            except Exception as e:
                _log("REPLACE", f"Attempt {attempt + 1} failed: {e}")

    def _cookies_file(self) -> str | None:
        cookies_txt = self.config["YT_COOKIES"]
        if not cookies_txt:
            return None
        path = os.path.join(self._tmp, "cookies.txt")
        with open(path, "w", encoding="utf-8") as f:
            f.write(cookies_txt)
        return path

    async def _dl_and_upload(self, video_id: str) -> dict:
        if not YT_DLP_OK:
            return {"error": "yt-dlp not installed"}

        ddir = tempfile.mkdtemp(dir=self._tmp)
        try:
            yt_url   = f"https://youtu.be/{video_id}"
            cookies  = self._cookies_file()
            loop     = asyncio.get_event_loop()
            info_holder = {}

            def _extract():
                opts = {"quiet": True, "no_warnings": True, "socket_timeout": 30}
                if cookies:
                    opts["cookiefile"] = cookies
                with yt_dlp.YoutubeDL(opts) as ydl:
                    info_holder["info"] = ydl.extract_info(yt_url, download=False)

            await loop.run_in_executor(None, _extract)
            info = info_holder.get("info") or {}

            title         = info.get("title", "Unknown") or "Unknown"
            artist        = info.get("uploader") or info.get("channel") or "YouTube"
            dur_s         = int(info.get("duration") or 0)
            thumbnail_url = info.get("thumbnail", "")

            _log("DL", f"title={title!r} artist={artist!r} thumb={thumbnail_url[:80]}")

            safe_name = sanitize_fn(f"{artist} - {title}")
            out_tmpl  = os.path.join(ddir, f"{safe_name}.%(ext)s")

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

            await loop.run_in_executor(None, _download)

            audio_exts = (".mp3", ".m4a", ".opus", ".ogg", ".wav", ".webm")
            found = None
            for fn in os.listdir(ddir):
                if any(fn.endswith(ext) for ext in audio_exts):
                    found = os.path.join(ddir, fn)
                    break

            if not found or not os.path.exists(found):
                return {"error": "audio file not found after download"}

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
                return {"error": "empty file"}

            size = os.path.getsize(final_mp3)
            if size > MAX_FILE_SIZE:
                _log("DL", f"Too large: {size // (1024 * 1024)} MB")
                return {"error": "too_large"}

            raw_cover  = await _download_image(thumbnail_url) if thumbnail_url else None
            cover_data = normalize_cover(raw_cover) if raw_cover else None
            thumb_data = normalize_cover(raw_cover, max_size=320) if raw_cover else None

            if cover_data and final_mp3.endswith(".mp3"):
                cover_path  = os.path.join(ddir, "cover.jpg")
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

            if not thumb_data and cover_data:
                thumb_data = cover_data

            with open(final_mp3, "rb") as f:
                audio_bytes = f.read()

            file_id = await self._upload_to_tg(
                audio_bytes,
                os.path.basename(final_mp3),
                title,
                artist,
                dur_s,
                thumb_data,
            )
            if file_id:
                return {"file_id": file_id, "title": title, "artist": artist, "duration": dur_s}
            return {"error": "Telegram upload failed"}

        except Exception as e:
            _log("DL", f"Exception: {str(e)[:120]}")
            return {"error": str(e)[:120]}
        finally:
            shutil.rmtree(ddir, ignore_errors=True)

    async def _upload_to_tg(
        self,
        file_bytes: bytes,
        filename:   str,
        title:      str,
        artist:     str,
        dur_s:      int,
        thumb_data: bytes | None,
    ) -> str | None:
        async with self._upload_lock:
            audio_inp = BufferedInputFile(file_bytes, filename=filename)
            thumb_inp = (
                BufferedInputFile(thumb_data, filename="cover.jpg")
                if thumb_data else None
            )
            try:
                sent = await self.inline_bot.send_audio(
                    chat_id=self._me_id,
                    audio=audio_inp,
                    title=title,
                    performer=artist,
                    duration=dur_s,
                    thumbnail=thumb_inp,
                )
            except Exception as e:
                _log("UPLOAD", f"send_audio failed: {e}")
                return None
            if sent and sent.audio:
                fid    = sent.audio.file_id
                msg_id = sent.message_id
                await asyncio.sleep(0.5)
                for attempt in range(5):
                    try:
                        await self.inline_bot.delete_message(chat_id=self._me_id, message_id=msg_id)
                        break
                    except Exception:
                        await asyncio.sleep(1.0 * (attempt + 1))
                return fid
            return None

    async def _get_stub(self, video_id: str, title: str, artist: str) -> str | None:
        if video_id in self._stub_cache:
            return self._stub_cache[video_id]

        _log("STUB", f"Creating stub for {video_id} ({artist} - {title})")

        stub_bytes = b""
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(DOWNLOADING_STUB, timeout=aiohttp.ClientTimeout(total=20)) as r:
                    if r.status == REQUEST_OK:
                        stub_bytes = await r.read()
        except Exception as e:
            _log("STUB", f"Stub audio dl failed: {e}")

        if not stub_bytes:
            return None

        hq_url     = f"https://img.youtube.com/vi/{video_id}/hqdefault.jpg"
        raw        = await _download_image(hq_url)
        thumb_data = normalize_cover(raw, max_size=320) if raw else None

        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text=self.strings["downloading"],
                callback_data=f"ytm_dl_{video_id[:32]}",
            )
        ]])

        try:
            sent = await self.inline_bot.send_audio(
                chat_id=self._me_id,
                audio=BufferedInputFile(stub_bytes, filename="Downloading.mp3"),
                title=title,
                performer=artist,
                thumbnail=(
                    BufferedInputFile(thumb_data, filename="cover.jpg") if thumb_data else None
                ),
                reply_markup=kb,
            )
            if sent and sent.audio:
                fid = sent.audio.file_id
                self._stub_cache[video_id] = fid
                _log("STUB", f"Created: file_id={fid!r}")
                try:
                    await self.inline_bot.delete_message(
                        chat_id=self._me_id, message_id=sent.message_id
                    )
                except Exception:
                    pass
                return fid
        except Exception as e:
            _log("STUB", f"send_audio failed: {e}\n{traceback.format_exc()}")
        return None

    @loader.command(ru_doc="Добавить куки для yt-dlp (реплай на .txt файл)")
    async def ytcookies(self, message: Message):
        """Reply to a .txt file with YouTube cookies"""
        reply = await message.get_reply_message()

        if not reply or not reply.file:
            if utils.get_args_raw(message).strip().lower() in ("clear", "del", "remove"):
                self.config["YT_COOKIES"] = ""
                await utils.answer(message, self.strings["cookies_cleared"])
                return
            await utils.answer(message, self.strings["cookies_no_reply"])
            return

        fn = reply.file.name or ""
        if not fn.lower().endswith(".txt"):
            await utils.answer(message, self.strings["cookies_bad_file"])
            return

        data = await reply.download_media(bytes)
        if not data:
            await utils.answer(message, self.strings["cookies_bad_file"])
            return

        self.config["YT_COOKIES"] = data.decode("utf-8", errors="replace")
        await utils.answer(message, self.strings["cookies_saved"])

    @loader.inline_handler(ru_doc="YouTube поиск", en_doc="YouTube search")
    async def yt_inline_handler(self, query: InlineQuery):
        """YouTube search"""
        raw    = query.query.strip()
        prefix = "yt"
        text   = raw[len(prefix):].strip() if raw.lower().startswith(prefix) else raw.strip()

        if not text:
            await self._hint(query)
            return

        _log("INLINE", f"query={text!r} from={query.from_user.id}")

        video_id = _extract_video_id(text)
        if video_id:
            await self._handle_link_inline(query, video_id)
        else:
            await self._handle_search_inline(query, text)

    async def _handle_link_inline(self, query: InlineQuery, video_id: str):
        _log("LINK", f"video_id={video_id}")

        if video_id in self._real_cache:
            fid = self._real_cache[video_id][0]
            try:
                await self.inline_bot.answer_inline_query(
                    inline_query_id=query.id,
                    results=[InlineQueryResultCachedAudio(
                        id=f"yt_{video_id}",
                        audio_file_id=fid,
                    )],
                    cache_time=0,
                    is_personal=True,
                )
            except Exception:
                pass
            return

        cookies = self._cookies_file()
        track   = await _get_video_info_ytdlp(video_id, cookies)

        if not track:
            await self._inline_msg(
                query,
                self.strings["link_not_found"],
                self.strings["link_not_found_desc"],
            )
            return

        title   = track["title"]
        channel = track["channel"]

        thumb = self._thumb_cache.get(video_id)
        if not thumb:
            thumb = await _best_thumb_for_inline(video_id)
            self._thumb_cache[video_id] = thumb

        stub_fid = await self._get_stub(video_id, title, channel)

        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text=self.strings["downloading"],
                callback_data=f"ytm_dl_{video_id[:32]}",
            )
        ]])

        if stub_fid:
            results = [InlineQueryResultCachedAudio(
                id=f"yt_{video_id}",
                audio_file_id=stub_fid,
                reply_markup=kb,
            )]
        else:
            kwargs = dict(
                id=f"yt_{video_id}",
                title=title,
                description=channel,
                input_message_content=InputTextMessageContent(
                    message_text=f"<b>YTMusic:</b> {escape_html(title)}",
                    parse_mode="HTML",
                ),
                reply_markup=kb,
            )
            if thumb:
                kwargs["thumbnail_url"]    = thumb
                kwargs["thumbnail_width"]  = 480
                kwargs["thumbnail_height"] = 360
            results = [InlineQueryResultArticle(**kwargs)]

        try:
            await self.inline_bot.answer_inline_query(
                inline_query_id=query.id,
                results=results,
                cache_time=0,
                is_personal=True,
            )
            _log("LINK", f"Answered OK video_id={video_id}")
        except Exception as e:
            _log("LINK", f"answer_inline_query FAILED: {e}")

    async def _handle_search_inline(self, query: InlineQuery, text: str):
        limit     = max(1, min(10, int(self.config["SEARCH_LIMIT"])))
        cache_key = text.lower()[:80]

        if cache_key in self._search_cache:
            tracks = self._search_cache[cache_key]
            _log("CACHE", f"Hit: {len(tracks)} tracks")
        else:
            tracks = await _search_innertube(text, limit=limit)
            if not tracks:
                tracks = await _search_ytdlp(text, limit=limit)
            self._search_cache[cache_key] = tracks

        if not tracks:
            await self._inline_msg(query, self.strings["not_found"], self.strings["not_found_desc"])
            return

        thumb_tasks   = [_best_thumb_for_inline(t["video_id"]) for t in tracks]
        thumb_results = await asyncio.gather(*thumb_tasks, return_exceptions=True)

        for i, t in enumerate(tracks):
            if not isinstance(thumb_results[i], Exception):
                self._thumb_cache[t["video_id"]] = thumb_results[i]

        stub_tasks   = [self._get_stub(t["video_id"], t["title"], t["channel"]) for t in tracks]
        stub_results = await asyncio.gather(*stub_tasks, return_exceptions=True)

        inline_results = []
        for i, t in enumerate(tracks):
            vid     = t["video_id"]
            title   = t["title"]
            channel = t["channel"]
            dur_str = t["dur_str"]

            thumb = (
                thumb_results[i]
                if not isinstance(thumb_results[i], Exception)
                else f"https://img.youtube.com/vi/{vid}/hqdefault.jpg"
            )
            stub_fid = (
                stub_results[i]
                if not isinstance(stub_results[i], Exception)
                else None
            )

            _log("RESULT", f"[{i}] {title!r} stub_fid={stub_fid!r} thumb={str(thumb)[:60]}")

            kb = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(
                    text=self.strings["downloading"],
                    callback_data=f"ytm_dl_{vid[:32]}",
                )
            ]])

            if vid in self._real_cache:
                inline_results.append(InlineQueryResultCachedAudio(
                    id=f"yt_{vid}",
                    audio_file_id=self._real_cache[vid][0],
                ))
            elif stub_fid:
                inline_results.append(InlineQueryResultCachedAudio(
                    id=f"yt_{vid}",
                    audio_file_id=stub_fid,
                    reply_markup=kb,
                ))
            else:
                kwargs = dict(
                    id=f"yt_{vid}",
                    title=title,
                    description=f"{channel} | {dur_str}",
                    input_message_content=InputTextMessageContent(
                        message_text=f"<b>YTMusic:</b> {escape_html(title)}",
                        parse_mode="HTML",
                    ),
                    reply_markup=kb,
                )
                if thumb:
                    kwargs["thumbnail_url"]    = thumb
                    kwargs["thumbnail_width"]  = 480
                    kwargs["thumbnail_height"] = 360
                inline_results.append(InlineQueryResultArticle(**kwargs))

        try:
            await self.inline_bot.answer_inline_query(
                inline_query_id=query.id,
                results=inline_results,
                cache_time=0,
                is_personal=True,
            )
            _log("INLINE", f"Answered {len(inline_results)} results OK")
        except Exception as e:
            _log("INLINE", f"answer_inline_query FAILED: {e}\n{traceback.format_exc()}")

    async def _hint(self, query: InlineQuery):
        try:
            await self.inline_bot.answer_inline_query(
                inline_query_id=query.id,
                results=[InlineQueryResultArticle(
                    id=f"hint_{int(time.time())}",
                    title=self.strings["hint_title"],
                    description=self.strings["hint_desc"],
                    input_message_content=InputTextMessageContent(
                        message_text=f"<b>YTMusic:</b> {self.strings['hint_desc']}",
                        parse_mode="HTML",
                    ),
                    thumbnail_url=INLINE_QUERY_BANNER,
                    thumbnail_width=640,
                    thumbnail_height=360,
                )],
                cache_time=0,
                is_personal=True,
            )
        except Exception:
            pass

    async def _inline_msg(self, query: InlineQuery, title: str, desc: str):
        try:
            await self.inline_bot.answer_inline_query(
                inline_query_id=query.id,
                results=[InlineQueryResultArticle(
                    id=f"msg_{int(time.time())}",
                    title=title,
                    description=desc,
                    input_message_content=InputTextMessageContent(
                        message_text=f"<b>YTMusic:</b> {escape_html(desc)}",
                        parse_mode="HTML",
                    ),
                )],
                cache_time=0,
                is_personal=True,
            )
        except Exception:
            pass

    async def on_unload(self):
        await self._unpatch()
        self._real_cache.clear()
        self._stub_cache.clear()
        self._search_cache.clear()
        self._thumb_cache.clear()
        if self._tmp and os.path.exists(self._tmp):
            shutil.rmtree(self._tmp, ignore_errors=True)
