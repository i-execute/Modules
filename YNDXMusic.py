__version__ = (2, 0, 0)
# meta developer: I_execute.t.me forked from @codrago_m
# meta banner: https://github.com/i-execute/Modules/raw/main/Storage/YNDXMusic/MetaBannerNew.jpeg

import os
import io
import re
import time
import json
import random
import string
import hashlib
import logging
import tempfile
import shutil
import asyncio
import subprocess
import sys
import traceback
import uuid

from telethon.tl.types import Message, DocumentAttributeAudio
from telethon import functions
from telethon.tl.types import InputMediaWebPage

from .. import loader, utils

logger = logging.getLogger(__name__)

FAVORITE_COVER_URL = "https://github.com/i-execute/Modules/raw/main/Storage/YNDXMusic/Favorite.jpeg"

YM_CLIENT_ID = "23cabbbdc6cd418abb4b39c32c41195d"
YM_TOKEN_PATTERN = re.compile(r"access_token=([^&]+)")
YM_ALBUM_TRACK_RE = re.compile(
    r"https?://music\.yandex\.(?:ru|com|by|kz|uz)/album/\d+/track/(\d+)"
)
YM_DIRECT_TRACK_RE = re.compile(
    r"https?://music\.yandex\.(?:ru|com|by|kz|uz)/track/(\d+)"
)
YM_PLAYLIST_RE = re.compile(
    r"https?://music\.yandex\.(?:ru|com|by|kz|uz)/users/([^/]+)/playlists/(\d+)"
)
YM_PLAYLIST_UUID_RE = re.compile(
    r"https?://music\.yandex\.(?:ru|com|by|kz|uz)/playlists/([a-f0-9\-]{36})"
)
YM_PLAYLIST_LK_RE = re.compile(
    r"https?://music\.yandex\.(?:ru|com|by|kz|uz)/playlists/lk\.([a-f0-9\-]{36})"
)
YM_ALBUM_RE = re.compile(
    r"https?://music\.yandex\.(?:ru|com|by|kz|uz)/album/(\d+)(?:[/?]|$)"
)

REQUEST_OK = 200
CACHE_TTL = 600
COVER_SIZES = [1000, 900, 800, 700, 600, 400, 300, 200]

LOG_ENTRIES = []
MAX_LOG = 300


def _log(tag: str, msg: str):
    ts = time.strftime("%H:%M:%S")
    entry = f"[{ts}] [{tag}] {msg}"
    LOG_ENTRIES.append(entry)
    if len(LOG_ENTRIES) > MAX_LOG:
        LOG_ENTRIES.pop(0)
    logger.info(entry)


def _ensure_all_deps():
    for mod, pip in {
        "aiohttp": "aiohttp",
        "mutagen": "mutagen",
        "yandex_music": "yandex-music",
        "PIL": "Pillow",
    }.items():
        try:
            __import__(mod)
        except ImportError:
            subprocess.check_call(
                [sys.executable, "-m", "pip", "install", pip, "-q"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )


_ensure_all_deps()

import aiohttp
from yandex_music import ClientAsync
from yandex_music.exceptions import TimedOutError as YMTimedOutError
from yandex_music.utils.request_async import Request as _YMRequest
from yandex_music.exceptions import TimedOutError as _YMTimedOutError, NetworkError as _YMNetworkError

if not getattr(_YMRequest, "_yndx_curl_patched", False):
    from curl_cffi.requests import AsyncSession as _CurlSession
    import asyncio as _asyncio

    _YNDX_IMPERSONATE = "chrome116"
    _YNDX_TIMEOUT = 0.7
    _YNDX_ATTEMPTS = 30

    async def _yndx_curl_request_wrapper(self, *args, **kwargs):
        method = args[0] if args else kwargs.pop("method", "GET")
        url = args[1] if len(args) > 1 else kwargs.pop("url")
        kwargs.pop("timeout", None)
        headers = kwargs.pop("headers", self.headers)
        params = kwargs.pop("params", None)
        data = kwargs.pop("data", None)
        json = kwargs.pop("json", None)
        proxy = kwargs.pop("proxy", self.proxy_url)

        last_exc = None
        async with _CurlSession(impersonate=_YNDX_IMPERSONATE) as session:
            for _ in range(_YNDX_ATTEMPTS):
                try:
                    r = await session.request(
                        method, url,
                        headers=headers,
                        params=params,
                        data=data,
                        json=json,
                        proxy=proxy,
                        timeout=_YNDX_TIMEOUT,
                    )
                    content = r.content
                    if 200 <= r.status_code <= 299:
                        return content
                    self._handle_error_response(r.status_code, content)
                    return None
                except Exception as e:
                    last_exc = e
                    continue
        raise _YMTimedOutError from last_exc

    _YMRequest._request_wrapper = _yndx_curl_request_wrapper
    _YMRequest._yndx_curl_patched = True
from PIL import Image

try:
    from mutagen.id3 import ID3, TIT2, TPE1, TALB, APIC, ID3NoHeaderError
except ImportError:
    ID3 = None


def escape_html(t):
    return (t or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def sanitize_fn(n):
    return re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", n).strip(". ")[:180] or "track"


def parse_ym_track_id(text):
    if not text:
        return None
    for pat in [YM_ALBUM_TRACK_RE, YM_DIRECT_TRACK_RE]:
        m = pat.search(text)
        if m:
            return m.group(1)
    return None


def extract_ym_token(text):
    if not text:
        return None
    m = YM_TOKEN_PATTERN.search(text)
    return m.group(1) if m else None


def _build_ym_auth_url():
    return (
        f"https://oauth.yandex.ru/authorize?"
        f"response_type=token&client_id={YM_CLIENT_ID}"
    )


def _is_ym_track_link(text):
    if not text:
        return False
    return bool(YM_ALBUM_TRACK_RE.search(text) or YM_DIRECT_TRACK_RE.search(text))


def _is_ym_album_link(text):
    if not text:
        return False
    m = YM_ALBUM_RE.search(text)
    if not m:
        return False
    if YM_ALBUM_TRACK_RE.search(text):
        return False
    return True


def normalize_cover(raw_data, max_size=None, force_jpeg=False):
    if not raw_data or len(raw_data) < 100:
        return None
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
        img.save(buf, format="PNG")
        result = buf.getvalue()
        return result if len(result) >= 100 else None
    except Exception:
        return None


async def _get_best_cover_url(cover_uri):
    if not cover_uri:
        return None
    try:
        async with aiohttp.ClientSession() as sess:
            for sz in COVER_SIZES:
                url = f"https://{cover_uri.replace('%%', f'{sz}x{sz}')}"
                try:
                    async with sess.head(url, timeout=aiohttp.ClientTimeout(total=5)) as r:
                        if r.status == 200:
                            return url
                except Exception:
                    continue
    except Exception:
        pass
    return None


async def _download_cover_best(cover_uri, covers_dir=None):
    if not cover_uri:
        return None
    cache_key = hashlib.md5(cover_uri.encode()).hexdigest()
    if covers_dir:
        cache_path = os.path.join(covers_dir, f"{cache_key}.jpg")
        if os.path.exists(cache_path):
            try:
                with open(cache_path, "rb") as f:
                    return f.read()
            except Exception:
                pass
    url = await _get_best_cover_url(cover_uri)
    if not url:
        return None
    try:
        async with aiohttp.ClientSession() as sess:
            async with sess.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
                if r.status != 200:
                    return None
                data = await r.read()
                if not data or len(data) < 500:
                    return None
                if covers_dir:
                    try:
                        with open(cache_path, "wb") as f:
                            f.write(data)
                    except Exception:
                        pass
                return data
    except Exception:
        return None


async def _convert_to_mp3(inp, out):
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-hide_banner", "-loglevel", "error",
            "-y", "-i", inp, "-vn", "-acodec", "libmp3lame", "-ab", "320k", out,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        await asyncio.wait_for(proc.communicate(), timeout=60)
        return proc.returncode == 0 and os.path.exists(out) and os.path.getsize(out) > 0
    except Exception:
        return False


async def _embed_cover(mp3_path, cover_path, out_path):
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
            "-i", mp3_path, "-i", cover_path,
            "-map", "0:a", "-map", "1:0",
            "-c:a", "copy", "-c:v", "copy",
            "-id3v2_version", "3",
            "-metadata:s:v", "title=Cover",
            "-metadata:s:v", "comment=Cover (front)",
            "-disposition:v", "attached_pic", out_path,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        await asyncio.wait_for(proc.communicate(), timeout=30)
        return proc.returncode == 0 and os.path.exists(out_path) and os.path.getsize(out_path) > 0
    except Exception:
        return False


def _write_id3_tags(filepath, title, artist, album_title=None, cover_data=None):
    if not ID3:
        return
    try:
        try:
            tags = ID3(filepath)
        except ID3NoHeaderError:
            tags = ID3()
        tags.add(TIT2(encoding=3, text=[title]))
        tags.add(TPE1(encoding=3, text=[artist]))
        if album_title:
            tags.add(TALB(encoding=3, text=[album_title]))
        if cover_data and len(cover_data) > 100:
            is_png = cover_data[:8] == b'\x89PNG\r\n\x1a\n'
            mime = "image/png" if is_png else "image/jpeg"
            tags.add(APIC(encoding=3, mime=mime, type=3, desc="Cover", data=cover_data))
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


class YMApiClient:
    def __init__(self):
        self._token = None
        self._client = None
        self._ok = False
        self._uid = None
        self._login = None
        self._has_plus = False

    @property
    def ok(self):
        return self._ok and self._token is not None

    @property
    def has_plus(self):
        return self._has_plus

    async def auth(self, token):
        if not token:
            self.reset()
            return False
        self._token = token
        try:
            self._client = ClientAsync(token)
            await self._client.init()
            me = self._client.me
            self._uid = me.account.uid
            self._login = me.account.login
            plus = getattr(me, 'plus', None)
            self._has_plus = bool(getattr(plus, 'has_plus', False)) if plus else False
            self._ok = True
            return True
        except Exception as e:
            _log("AUTH", f"Auth failed: {e}")
            self.reset()
            return False

    def reset(self):
        self._token = None
        self._client = None
        self._ok = False
        self._uid = None
        self._login = None
        self._has_plus = False

    async def fetch_track(self, track_id):
        if not self._client:
            return None
        try:
            tracks = await self._client.tracks(track_id, with_positions=False)
            return tracks[0] if tracks else None
        except Exception as e:
            _log("FETCH", f"fetch_track error: {e}")
            return None

    async def fetch_playlist(self, user_login, playlist_id):
        if not self._client:
            return None
        try:
            return await self._client.users_playlists(playlist_id, user_login)
        except Exception as e:
            _log("FETCH", f"fetch_playlist error: {e}")
            return None

    async def fetch_playlist_by_uuid(self, uuid):
        if not self._client:
            return None
        try:
            all_pl = await self._client.users_playlists_list(self._uid)
            if not all_pl:
                return None
            for pl in all_pl:
                if getattr(pl, "playlist_uuid", None) == uuid:
                    owner = getattr(pl, "owner", None)
                    login = getattr(owner, "login", None) if owner else None
                    kind = getattr(pl, "kind", None)
                    if login and kind is not None:
                        return await self._client.users_playlists(kind, login)
            return None
        except Exception as e:
            _log("FETCH", f"fetch_playlist_by_uuid error: {e}")
            return None

    async def fetch_all_playlists_meta(self):
        if not self._client:
            return []
        try:
            return await self._client.users_playlists_list(self._uid) or []
        except Exception as e:
            _log("FETCH", f"fetch_all_playlists_meta error: {e}")
            return []

    async def fetch_liked_tracks(self):
        if not self._client:
            return []
        try:
            result = await self._client.users_likes_tracks(self._uid)
            if not result:
                return []
            tracks_short = getattr(result, "tracks", None) or []
            if not tracks_short:
                return []
            ids = []
            for t in tracks_short:
                tid = getattr(t, "id", None)
                if tid is not None:
                    ids.append(str(tid))
                else:
                    raw = getattr(t, "track_id", None)
                    if raw:
                        ids.append(str(raw).split(":")[0])
            if not ids:
                return []
            fetched = []
            for i in range(0, len(ids), 50):
                chunk = ids[i:i + 50]
                try:
                    res = await self._client.tracks(chunk)
                    if res:
                        fetched.extend(res)
                except Exception as e:
                    _log("LIKED", f"batch {i} error: {e}")
            return fetched
        except Exception as e:
            _log("LIKED", f"fetch_liked_tracks error: {e}")
            return []

    async def fetch_liked_albums_books(self):
        if not self._client:
            return []
        try:
            liked = await self._client.users_likes_albums(self._uid)
            if not liked:
                return []
            result = []
            for item in liked:
                al = getattr(item, 'album', item)
                if getattr(al, 'type', None) == 'audiobook':
                    result.append(al)
            return result
        except Exception as e:
            _log("BOOKS", f"fetch_liked_albums_books error: {e}")
            return []

    async def fetch_album_with_tracks(self, album_id):
        if not self._client:
            return None
        try:
            return await self._client.albums_with_tracks(album_id)
        except Exception as e:
            _log("FETCH", f"fetch_album_with_tracks error: {e}")
            return None

    async def fetch_playlist_cover_url(self, pl):
        og_image = getattr(pl, "og_image", None)
        if og_image:
            url = await _get_best_cover_url(og_image)
            if url:
                return url
        cover = getattr(pl, "cover", None)
        if cover:
            uri = getattr(cover, "uri", None)
            if uri:
                url = await _get_best_cover_url(uri)
                if url:
                    return url
        return None

    async def download_track_file(self, track, filepath):
        try:
            if os.path.exists(filepath):
                try:
                    os.remove(filepath)
                except Exception:
                    pass
            await track.download_async(filepath)
            return os.path.exists(filepath) and os.path.getsize(filepath) > 0
        except Exception as e:
            _log("DOWNLOAD", f"download_track_file failed: {e}")
            return False

    async def download_book_part_file(self, track, filepath, retries=10, timeout_pause=60):
        last_err = None
        for attempt in range(retries):
            try:
                if os.path.exists(filepath):
                    try:
                        os.remove(filepath)
                    except Exception:
                        pass
                await track.download_async(filepath)
                if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
                    return True
                last_err = "empty file"
            except (YMTimedOutError, asyncio.TimeoutError) as e:
                last_err = e
                _log("DOWNLOAD", f"download_book_part_file attempt {attempt + 1}/{retries} timed out: {e}")
                if attempt != retries - 1:
                    _log("DOWNLOAD", f"Pausing {timeout_pause}s before retry due to timeout")
                    await asyncio.sleep(timeout_pause)
                continue
            except Exception as e:
                last_err = e
                _log("DOWNLOAD", f"download_book_part_file attempt {attempt + 1}/{retries} failed: {e}")
                if attempt != retries - 1:
                    await asyncio.sleep(2 * (attempt + 1))
        _log("DOWNLOAD", f"download_book_part_file gave up after {retries} attempts: {last_err}")
        return False

    async def search_track(self, query, count=5):
        if not self._client:
            return []
        try:
            result = await self._client.search(query, type_="track")
            if not result or not result.tracks or not result.tracks.results:
                return []
            return result.tracks.results[:count]
        except Exception as e:
            _log("SEARCH", f"search failed: {e}")
            try:
                self._client = None
                self._ok = False
                await self.auth(self._token)
                result = await self._client.search(query, type_="track")
                if not result or not result.tracks or not result.tracks.results:
                    return []
                return result.tracks.results[:count]
            except Exception as e2:
                _log("SEARCH", f"retry failed: {e2}")
                return []

    @staticmethod
    def track_artist(track):
        if track.artists:
            return ", ".join(a.name for a in track.artists if a.name) or "Unknown"
        return "Unknown"

    @staticmethod
    def track_title(track):
        return track.title or "Unknown"

    @staticmethod
    def cover_url(cover_uri, size="200x200"):
        if not cover_uri:
            return None
        return f"https://{cover_uri.replace('%%', size)}"


@loader.tds
class YNDXMusic(loader.Module):
    """Yandex Music audio downloader and search"""

    strings = {
        "name": "YNDXMusic",
        "ymauth_menu_title": (
            "<b>YNDXMusic - Authorization</b>\n"
            "<blockquote>"
            "1. Open the link below\n"
            "2. Sign in and grant permissions\n"
            "3. Copy the full URL from the address bar\n"
            "4. Tap the button below and paste the URL"
            "</blockquote>\n"
            '<a href="{ym_url}">Authorize via Yandex</a>'
        ),
        "ymauth_paste_btn": "Paste URL",
        "ymauth_enter_url": "Paste full URL from address bar:",
        "ymauth_cancel": "Cancel",
        "token_ok": (
            "<b>Yandex Music authorized!</b>\n"
            "<blockquote>UID: <code>{uid}</code>\n"
            "Login: <code>{login}</code>\n"
            "Plus: {plus}</blockquote>"
        ),
        "token_fail": "<b>Token is invalid!</b>",
        "token_bad_format": "<b>Wrong format!</b> Provide the full URL or token.",
        "no_token": "<b>Not authorized.</b> Use <code>{prefix}ymauth</code>",
        "no_plus": "<b>Yandex Plus not active.</b> Download may fail.",
        "no_playing": "<b>Nothing is playing right now</b>",
        "fetching": "<b>Fetching current track...</b>",
        "uploading": "<b>Uploading...</b>",
        "error": "<b>Error:</b> {msg}",
        "download_fail": "<b>Download failed.</b> Check your Yandex Plus subscription or try again.",
        "track_text": (
            "<blockquote>"
            "<b>Listening on:</b> {device}\n"
            "<b>Playing from:</b> <tg-emoji emoji-id=5256072030704410885>📂</tg-emoji>{source}\n"
            "<b>Played till:</b> <tg-emoji emoji-id=5256060906739113185>🎶</tg-emoji> {played_till}\n"
            "<b>Track link:</b> <tg-emoji emoji-id=5258508389557771865>🔗</tg-emoji>{link}"
            "</blockquote>"
        ),
        "yms_searching": "<b>Searching</b> <code>{query}</code>",
        "yms_download": "⬇️ Download",
        "yms_cancel": "✖️ Close",
        "yms_new_search": "🔍 New search",
        "yms_downloading": "Downloading...",
        "yms_edit_dl": "<b>Downloading</b> <code>{title}</code>",
        "yms_no_results": "<b>Nothing found</b>",
        "yms_provide_query": "<b>Provide a search query</b>",
        "yms_menu_title": "<b>Yandex Music</b>\n<blockquote>Search a track</blockquote>",
        "yms_via_link": "Via link",
        "yms_via_query": "Via query",
        "yms_enter_link": "Enter track link:",
        "yms_enter_query": "Enter search query:",
        "ymp_title": "<b>{name}</b>\n<blockquote>{count} tracks</blockquote>",
        "ymp_progress": "<b>{name}</b>\n<blockquote>{done}/{total} downloaded</blockquote>",
        "ymp_no_url": "<b>Provide a playlist URL</b>",
        "ymp_not_found": "<b>Playlist not found</b>",
        "ymp_download": "Download",
        "ymp_cancel": "Cancel",
        "ymp_kill": "Kill",
        "ymp_menu_my": "My playlists",
        "ymp_menu_link": "Enter link",
        "ymp_enter_link": "Enter playlist link:",
        "ymp_fetching_pl": "<b>Fetching playlist...</b>",
        "ymp_favorites": "Favorites",
        "ymp_menu_title": "<b>Yandex Music</b>\n<blockquote>Select playlist source</blockquote>",
        "ymp_playlist_title": "<b>{title}</b>\n<blockquote>{count} tracks</blockquote>",
        "ymb_menu_title": "<b>Yandex Music</b>\n<blockquote>Select audiobook source</blockquote>",
        "ymb_menu_my": "My books",
        "ymb_menu_link": "Enter link",
        "ymb_enter_link": "Enter audiobook link:",
        "ymb_fetching": "<b>Fetching audiobook...</b>",
        "ymb_not_found": "<b>Audiobook not found</b>",
        "ymb_not_book": "<b>This is not an audiobook link</b>",
        "ymb_title": "<b>{name}</b>\n<blockquote>{count} parts</blockquote>",
        "ymb_progress": "<b>{name}</b>\n<blockquote>{done}/{total} parts downloaded</blockquote>",
        "ymb_download": "Download",
        "ymb_cancel": "Cancel",
        "ymb_kill": "Kill",
        "ymb_no_books": "<b>No audiobooks in library</b>",
        "ymb_stopped": "<b>Download stopped: {done}/{total} parts sent.</b>\n<blockquote>Failed on part {idx}: {name}\nReason: {error}</blockquote>",
        "btn_left": "⬅️",
        "btn_right": "➡️",
        "unknown_device": "Unknown",
        "source_liked": "Liked",
        "link_text": "Song",
    }

    strings_ru = {
        "ymauth_menu_title": (
            "<b>YNDXMusic - Авторизация</b>\n"
            "<blockquote>"
            "1. Откройте ссылку ниже\n"
            "2. Войдите в аккаунт и дайте разрешения\n"
            "3. Скопируйте полный URL из адресной строки\n"
            "4. Нажмите кнопку ниже и вставьте URL"
            "</blockquote>\n"
            '<a href="{ym_url}">Авторизация через Яндекс</a>'
        ),
        "ymauth_paste_btn": "Вставить URL",
        "ymauth_enter_url": "Вставьте полный URL из адресной строки:",
        "ymauth_cancel": "Отмена",
        "token_ok": (
            "<b>Yandex Music авторизован!</b>\n"
            "<blockquote>UID: <code>{uid}</code>\n"
            "Логин: <code>{login}</code>\n"
            "Плюс: {plus}</blockquote>"
        ),
        "token_fail": "<b>Токен недействителен!</b>",
        "token_bad_format": "<b>Неверный формат!</b> Укажите полный URL или токен.",
        "no_token": "<b>Не авторизован.</b> Используйте <code>{prefix}ymauth</code>",
        "no_plus": "<b>Яндекс Плюс не активен.</b> Скачивание может не работать.",
        "no_playing": "<b>Сейчас ничего не играет</b>",
        "fetching": "<b>Получение текущего трека...</b>",
        "uploading": "<b>Загрузка...</b>",
        "error": "<b>Ошибка:</b> {msg}",
        "download_fail": "<b>Ошибка скачивания.</b> Проверьте подписку Яндекс Плюс или попробуйте позже.",
        "track_text": (
            "<blockquote>"
            "<b>Слушаю на:</b> {device}\n"
            "<b>Играет из:</b> <tg-emoji emoji-id=5256072030704410885>📂</tg-emoji>{source}\n"
            "<b>Прослушано до:</b> <tg-emoji emoji-id=5256060906739113185>🎶</tg-emoji> {played_till}\n"
            "<b>Ссылка на трек:</b> <tg-emoji emoji-id=5258508389557771865>🔗</tg-emoji>{link}"
            "</blockquote>"
        ),
        "yms_searching": "<b>Поиск</b> <code>{query}</code>",
        "yms_download": "⬇️ Скачать",
        "yms_cancel": "✖️ Закрыть",
        "yms_new_search": "🔍 Новый поиск",
        "yms_downloading": "Загрузка...",
        "yms_edit_dl": "<b>Загружаю</b> <code>{title}</code>",
        "yms_no_results": "<b>Ничего не найдено</b>",
        "yms_provide_query": "<b>Укажите поисковый запрос</b>",
        "yms_menu_title": "<b>Yandex Music</b>\n<blockquote>Поиск трека</blockquote>",
        "yms_via_link": "По ссылке",
        "yms_via_query": "По запросу",
        "yms_enter_link": "Введите ссылку на трек:",
        "yms_enter_query": "Введите поисковый запрос:",
        "ymp_title": "<b>{name}</b>\n<blockquote>{count} треков</blockquote>",
        "ymp_progress": "<b>{name}</b>\n<blockquote>{done}/{total} загружено</blockquote>",
        "ymp_no_url": "<b>Укажите URL плейлиста</b>",
        "ymp_not_found": "<b>Плейлист не найден</b>",
        "ymp_download": "Скачать",
        "ymp_cancel": "Отмена",
        "ymp_kill": "Остановить",
        "ymp_menu_my": "Мои плейлисты",
        "ymp_menu_link": "Ввести ссылку",
        "ymp_enter_link": "Введите ссылку на плейлист:",
        "ymp_fetching_pl": "<b>Получаю плейлист...</b>",
        "ymp_favorites": "Избранное",
        "ymp_menu_title": "<b>Yandex Music</b>\n<blockquote>Выберите источник плейлиста</blockquote>",
        "ymp_playlist_title": "<b>{title}</b>\n<blockquote>{count} треков</blockquote>",
        "ymb_menu_title": "<b>Yandex Music</b>\n<blockquote>Выберите источник аудиокниги</blockquote>",
        "ymb_menu_my": "Мои книги",
        "ymb_menu_link": "Ввести ссылку",
        "ymb_enter_link": "Введите ссылку на аудиокнигу:",
        "ymb_fetching": "<b>Получаю аудиокнигу...</b>",
        "ymb_not_found": "<b>Аудиокнига не найдена</b>",
        "ymb_not_book": "<b>Это не ссылка на аудиокнигу</b>",
        "ymb_title": "<b>{name}</b>\n<blockquote>{count} частей</blockquote>",
        "ymb_progress": "<b>{name}</b>\n<blockquote>{done}/{total} частей загружено</blockquote>",
        "ymb_download": "Скачать",
        "ymb_cancel": "Отмена",
        "ymb_kill": "Остановить",
        "ymb_no_books": "<b>Нет аудиокниг в библиотеке</b>",
        "ymb_stopped": "<b>Загрузка остановлена: {done}/{total} частей отправлено.</b>\n<blockquote>Ошибка на части {idx}: {name}\nПричина: {error}</blockquote>",
        "btn_left": "⬅️",
        "btn_right": "➡️",
        "unknown_device": "Неизвестно",
        "source_liked": "Мне нравится",
        "link_text": "Песня",
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "YM_TOKEN", "",
                "Yandex Music access token",
                validator=loader.validators.Hidden(),
            ),
            loader.ConfigValue(
                "SEARCH_LIMIT", 5,
                "Search results limit (1-10)",
                validator=loader.validators.Integer(minimum=1, maximum=10),
            ),
        )
        self._tmp = None
        self._covers_dir = None
        self._ym = None
        self._upload_lock = None
        self._now_track_id = None
        self._now_mp3_url = None
        self._ymauth_sessions = {}
        self._yms_sessions = {}
        self._yms_locks = {}
        self._ymp_sessions = {}
        self._ymp_my_sessions = {}
        self._ymb_sessions = {}
        self._ymb_my_sessions = {}

    def _init_dirs(self):
        if self._tmp and os.path.exists(self._tmp):
            shutil.rmtree(self._tmp, ignore_errors=True)
        os.makedirs(self._tmp, exist_ok=True)
        self._covers_dir = os.path.join(self._tmp, "covers")
        os.makedirs(self._covers_dir, exist_ok=True)

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        self._upload_lock = asyncio.Lock()
        me = await client.get_me()
        self._me_id = me.id
        self._tmp = os.path.join(tempfile.gettempdir(), f"YNDXMusic_{me.id}")
        self._init_dirs()
        self._ym = YMApiClient()
        await self._ensure_ym()

    async def _ensure_ym(self):
        token = self.config["YM_TOKEN"]
        if not token:
            self._ym.reset()
            return False
        if self._ym.ok and self._ym._token == token:
            return True
        return await self._ym.auth(token)

    def _get_limit(self):
        try:
            return max(1, min(10, int(self.config["SEARCH_LIMIT"])))
        except Exception:
            return 5

    async def _get_ynison(self):
        token = self._ym._token
        device_id = "".join(random.choices(string.ascii_lowercase, k=16))
        ws_proto = {
            "Ynison-Device-Id": device_id,
            "Ynison-Device-Info": json.dumps({"app_name": "Chrome", "type": 1}),
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(
                    "wss://ynison.music.yandex.ru/redirector.YnisonRedirectService/GetRedirectToYnison",
                    headers={
                        "Sec-WebSocket-Protocol": f"Bearer, v2, {json.dumps(ws_proto)}",
                        "Origin": "http://music.yandex.ru",
                        "Authorization": f"OAuth {token}",
                    },
                    timeout=10,
                ) as ws:
                    response = await asyncio.wait_for(ws.receive(), timeout=10)
                    if response.type != aiohttp.WSMsgType.TEXT:
                        _log("YNISON", f"Unexpected redirect frame type: {response.type}")
                        return None
                    redirect_data = json.loads(response.data)
                host = redirect_data.get("host")
                ticket = redirect_data.get("redirect_ticket")
                if not host or not ticket:
                    return None
                ws_proto2 = {
                    "Ynison-Device-Id": device_id,
                    "Ynison-Redirect-Ticket": ticket,
                    "Ynison-Device-Info": json.dumps({"app_name": "Chrome", "type": 1}),
                }
                async with session.ws_connect(
                    f"wss://{host}/ynison_state.YnisonStateService/PutYnisonState",
                    headers={
                        "Sec-WebSocket-Protocol": f"Bearer, v2, {json.dumps(ws_proto2)}",
                        "Origin": "http://music.yandex.ru",
                        "Authorization": f"OAuth {token}",
                    },
                    timeout=10,
                ) as ws2:
                    await ws2.send_str(json.dumps({
                        "update_full_state": {
                            "player_state": {
                                "player_queue": {
                                    "current_playable_index": -1,
                                    "entity_id": "",
                                    "entity_type": "VARIOUS",
                                    "playable_list": [],
                                    "options": {"repeat_mode": "NONE"},
                                    "entity_context": "BASED_ON_ENTITY_BY_DEFAULT",
                                    "version": {
                                        "device_id": device_id,
                                        "version": random.randint(1, 9999999999999999),
                                        "timestamp_ms": 0,
                                    },
                                    "from_optional": "",
                                },
                                "status": {
                                    "duration_ms": 0,
                                    "paused": True,
                                    "playback_speed": 1,
                                    "progress_ms": 0,
                                    "version": {
                                        "device_id": device_id,
                                        "version": random.randint(1, 9999999999999999),
                                        "timestamp_ms": 0,
                                    },
                                },
                            },
                            "device": {
                                "capabilities": {
                                    "can_be_player": True,
                                    "can_be_remote_controller": False,
                                    "volume_granularity": 16,
                                },
                                "info": {
                                    "device_id": device_id,
                                    "type": "WEB",
                                    "title": "Chrome Browser",
                                    "app_name": "Chrome",
                                },
                                "volume_info": {"volume": 0},
                                "is_shadow": True,
                            },
                            "is_currently_active": False,
                        },
                        "rid": str(uuid.uuid4()),
                        "player_action_timestamp_ms": 0,
                        "activity_interception_type": "DO_NOT_INTERCEPT_BY_DEFAULT",
                    }))
                    response2 = await asyncio.wait_for(ws2.receive(), timeout=10)
                    if response2.type != aiohttp.WSMsgType.TEXT:
                        _log("YNISON", f"Unexpected state frame type: {response2.type}")
                        return None
                    return json.loads(response2.data)
        except asyncio.TimeoutError:
            _log("YNISON", "Timed out waiting for Ynison response")
            return None
        except Exception as e:
            _log("YNISON", f"Failed to get now playing: {e}")
            return None

    async def _get_now_playing_track(self):
        try:
            ynison = await asyncio.wait_for(self._get_ynison(), timeout=25)
            if not ynison:
                return None
            player_state = ynison.get("player_state", {})
            queue = player_state.get("player_queue", {})
            playable_list = queue.get("playable_list", [])
            idx = queue.get("current_playable_index", -1)
            if not playable_list or idx < 0 or idx >= len(playable_list):
                return None
            raw_track = playable_list[idx]
            if raw_track.get("playable_type") == "LOCAL_TRACK":
                return None
            playable_id = raw_track.get("playable_id")
            if not playable_id:
                return None
            track_obj = (await self._ym._client.tracks(playable_id))[0]
            status = player_state.get("status", {})
            device_title = None
            volume = None
            try:
                active_id = ynison.get("active_device_id_optional", "")
                for dev in ynison.get("devices", []):
                    if dev.get("info", {}).get("device_id") == active_id:
                        device_title = dev.get("info", {}).get("title")
                        vol_raw = dev.get("volume_info", {}).get("volume")
                        if vol_raw is not None:
                            volume = round(vol_raw * 100)
                        break
            except Exception:
                pass
            return {
                "track": track_obj,
                "progress_ms": int(status.get("progress_ms", 0)),
                "duration_ms": int(status.get("duration_ms", 0)),
                "paused": status.get("paused", True),
                "playable_id": playable_id,
                "device_title": device_title,
                "volume": volume,
                "entity_type": queue.get("entity_type", "VARIOUS"),
                "entity_id": queue.get("entity_id", ""),
                "from_optional": queue.get("from_optional", ""),
            }
        except asyncio.TimeoutError:
            _log("NOW_PLAYING", "Timed out fetching now playing")
            return None
        except Exception as e:
            _log("NOW_PLAYING", f"Error: {e}")
            return None

    def _device_str(self, now):
        device_title = now.get("device_title")
        volume = now.get("volume")
        if not device_title:
            return ""
        if volume is not None:
            return f"{device_title} (<tg-emoji emoji-id=5255970510562432622>🔊</tg-emoji>{volume}%)"
        return device_title

    async def _get_source(self, now):
        entity_type = now.get("entity_type", "VARIOUS")
        entity_id = now.get("entity_id", "")
        from_optional = now.get("from_optional", "")
        try:
            match entity_type:
                case "PLAYLIST":
                    playlist = (await self._ym._client.playlists_list(entity_id))[0]
                    return escape_html(playlist.title)
                case "ALBUM":
                    album = (await self._ym._client.albums(entity_id))[0]
                    url = f"https://music.yandex.ru/album/{album.id}"
                    return f'<a href="{url}">{escape_html(album.title)}</a>'
                case "ARTIST":
                    artist = (await self._ym._client.artists(entity_id))[0]
                    url = f"https://music.yandex.ru/artist/{artist.id}"
                    return f'<a href="{url}">{escape_html(artist.name)}</a>'
                case _:
                    if from_optional:
                        fo = from_optional.lower()
                        if "liked" in fo or "my_vibe" in fo or "myvibe" in fo or "favorite" in fo:
                            return self.strings["source_liked"]
                    return "—"
        except Exception:
            return "—"

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

    async def _prepare_track_file(self, track, ddir, with_cover=False):
        artist = YMApiClient.track_artist(track)
        title = YMApiClient.track_title(track)
        album_title = track.albums[0].title if track.albums else ""
        dur_s = (track.duration_ms or 0) // 1000
        raw_path = os.path.join(ddir, "raw_track")
        
        dl_ok = await self._ym.download_track_file(track, raw_path)
        if not dl_ok:
            return None, "download_fail"
            
        if not os.path.exists(raw_path) or os.path.getsize(raw_path) == 0:
            return None, "download_fail"
        
        cover_data = None
        thumb_data = None
        if with_cover and track.cover_uri:
            raw_cover = await _download_cover_best(track.cover_uri, self._covers_dir)
            if raw_cover:
                cover_data = normalize_cover(raw_cover)
                thumb_data = normalize_cover(raw_cover, max_size=320, force_jpeg=True)
        
        clean_name = sanitize_fn(f"{artist} - {title}")
        final_mp3 = os.path.join(ddir, f"{clean_name}.mp3")
        try:
            with open(raw_path, "rb") as rf:
                header = rf.read(4)
            is_mp3 = header[:3] == b"ID3" or header[:2] in (b"\xff\xfb", b"\xff\xf3")
        except Exception:
            is_mp3 = True
        
        if is_mp3:
            try:
                os.rename(raw_path, final_mp3)
            except Exception:
                final_mp3 = raw_path
        else:
            ok = await _convert_to_mp3(raw_path, final_mp3)
            if ok:
                try:
                    os.remove(raw_path)
                except Exception:
                    pass
            else:
                final_mp3 = raw_path
        
        if with_cover and cover_data and final_mp3.endswith(".mp3"):
            cover_path = os.path.join(ddir, "cover.jpg")
            with open(cover_path, "wb") as cf:
                cf.write(normalize_cover(cover_data, force_jpeg=True) or cover_data)
            covered_mp3 = os.path.join(ddir, f"{clean_name}_cover.mp3")
            embed_ok = await _embed_cover(final_mp3, cover_path, covered_mp3)
            if embed_ok:
                try:
                    os.remove(final_mp3)
                except Exception:
                    pass
                final_mp3 = covered_mp3
            else:
                _write_id3_tags(final_mp3, title, artist, album_title or None, cover_data)
        elif final_mp3.endswith(".mp3"):
            _write_id3_tags(final_mp3, title, artist, album_title or None, None)
        
        return {
            "path": final_mp3,
            "title": title,
            "artist": artist,
            "album_title": album_title,
            "dur_s": dur_s,
            "cover_data": cover_data,
            "thumb_data": thumb_data,
        }, None

    async def _prepare_book_part(self, track, ddir, cover_data, album_title):
        title = YMApiClient.track_title(track)
        artist = YMApiClient.track_artist(track)
        dur_s = (track.duration_ms or 0) // 1000
        raw_path = os.path.join(ddir, "raw_track")
        
        dl_ok = await self._ym.download_book_part_file(track, raw_path)
        if not dl_ok:
            return None, "download_fail"
            
        if not os.path.exists(raw_path) or os.path.getsize(raw_path) == 0:
            return None, "download_fail"
        
        clean_name = sanitize_fn(f"{title}")
        final_mp3 = os.path.join(ddir, f"{clean_name}.mp3")
        try:
            with open(raw_path, "rb") as rf:
                header = rf.read(4)
            is_mp3 = header[:3] == b"ID3" or header[:2] in (b"\xff\xfb", b"\xff\xf3")
        except Exception:
            is_mp3 = True
        
        if is_mp3:
            try:
                os.rename(raw_path, final_mp3)
            except Exception:
                final_mp3 = raw_path
        else:
            ok = await _convert_to_mp3(raw_path, final_mp3)
            if ok:
                try:
                    os.remove(raw_path)
                except Exception:
                    pass
            else:
                final_mp3 = raw_path
        
        thumb_data = None
        if cover_data and final_mp3.endswith(".mp3"):
            cover_path = os.path.join(ddir, "cover.jpg")
            norm = normalize_cover(cover_data, force_jpeg=True) or cover_data
            with open(cover_path, "wb") as cf:
                cf.write(norm)
            covered_mp3 = os.path.join(ddir, f"{clean_name}_cover.mp3")
            embed_ok = await _embed_cover(final_mp3, cover_path, covered_mp3)
            if embed_ok:
                try:
                    os.remove(final_mp3)
                except Exception:
                    pass
                final_mp3 = covered_mp3
            else:
                _write_id3_tags(final_mp3, title, artist, album_title, cover_data)
            thumb_data = normalize_cover(cover_data, max_size=320, force_jpeg=True)
        
        return {
            "path": final_mp3,
            "title": title,
            "artist": artist,
            "album_title": album_title,
            "dur_s": dur_s,
            "thumb_data": thumb_data,
        }, None

    async def _send_audio(self, chat_id, info, reply_to=None, retries=3):
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
        _log("SEND_AUDIO", f"gave up after {retries} attempts: {last_err}")
        return False

    @loader.command(
        ru_doc="Авторизация Yandex Music",
        en_doc="Yandex Music authorization",
    )
    async def ymauth(self, message: Message):
        """Yandex Music authorization"""
        args = utils.get_args_raw(message).strip()
        if args:
            token = extract_ym_token(args)
            if not token:
                if not args.startswith("http") and len(args) > 10:
                    token = args
                else:
                    await utils.answer(message, self.strings["token_bad_format"])
                    return
            try:
                await message.delete()
            except Exception:
                pass
            ok = await self._ym.auth(token)
            if ok:
                self.config["YM_TOKEN"] = token
                await self._client.send_message(
                    message.chat_id,
                    self.strings["token_ok"].format(
                        uid=self._ym._uid or "?",
                        login=self._ym._login or "?",
                        plus="OK" if self._ym.has_plus else "ERR",
                    ),
                    parse_mode="html",
                )
            else:
                await self._client.send_message(
                    message.chat_id,
                    self.strings["token_fail"],
                    parse_mode="html",
                )
            return
        session_id = str(id(message))
        self._ymauth_sessions[session_id] = {"chat_id": message.chat_id}
        await self.inline.form(
            text=self.strings["ymauth_menu_title"].format(ym_url=_build_ym_auth_url()),
            message=message,
            reply_markup=[[
                {
                    "text": self.strings["ymauth_paste_btn"],
                    "input": self.strings["ymauth_enter_url"],
                    "handler": self._ymauth_input,
                    "args": (session_id,),
                    "style": "primary",
                },
                {
                    "text": self.strings["ymauth_cancel"],
                    "callback": self._ymauth_cancel,
                    "args": (session_id,),
                    "style": "danger",
                },
            ]],
            silent=True,
        )

    async def _ymauth_input(self, call, text: str, session_id: str):
        self._ymauth_sessions.pop(session_id, None)
        url_or_token = text.strip()
        token = extract_ym_token(url_or_token)
        if not token:
            if not url_or_token.startswith("http") and len(url_or_token) > 10:
                token = url_or_token
            else:
                await call.edit(self.strings["token_bad_format"])
                return
        ok = await self._ym.auth(token)
        if ok:
            self.config["YM_TOKEN"] = token
            await call.edit(
                self.strings["token_ok"].format(
                    uid=self._ym._uid or "?",
                    login=self._ym._login or "?",
                    plus="OK" if self._ym.has_plus else "ERR",
                )
            )
        else:
            await call.edit(self.strings["token_fail"])

    async def _ymauth_cancel(self, call, session_id: str):
        self._ymauth_sessions.pop(session_id, None)
        await call.delete()

    @loader.command(
        ru_doc="Текущий трек. Флаг -f отправить файл",
        en_doc="Current track. Flag -f to send file",
    )
    async def ymt(self, message: Message):
        """Current track. Use -f to send audio file."""
        prefix = self.get_prefix()
        args = utils.get_args_raw(message).strip()
        send_file_only = "-f" in args.split()
        if not await self._ensure_ym():
            await utils.answer(message, self.strings["no_token"].format(prefix=prefix))
            return
        msg = await utils.answer(message, self.strings["fetching"])
        now = await self._get_now_playing_track()
        if not now:
            await utils.answer(msg, self.strings["no_playing"])
            return
        track = now["track"]
        artist = YMApiClient.track_artist(track)
        title = YMApiClient.track_title(track)
        await utils.answer(msg, self.strings["uploading"])
        if self._now_track_id != now["playable_id"]:
            self._now_track_id = now["playable_id"]
            self._now_mp3_url = None
        if send_file_only:
            ddir = tempfile.mkdtemp(dir=self._tmp)
            try:
                info, err = await self._prepare_track_file(track, ddir, with_cover=True)
                if err:
                    await utils.answer(msg, self.strings["download_fail"] if err == "download_fail" else self.strings["error"].format(msg=err))
                    return
                try:
                    await msg.delete()
                except Exception:
                    pass
                is_forum = await self._is_forum_chat(message)
                topic_id = self._get_topic_id(message) if is_forum else None
                await self._send_audio(message.chat_id, info, reply_to=topic_id if is_forum and topic_id else None)
            finally:
                if os.path.exists(ddir):
                    shutil.rmtree(ddir, ignore_errors=True)
            return
        if self._now_mp3_url:
            mp3_url = self._now_mp3_url
        else:
            ddir = tempfile.mkdtemp(dir=self._tmp)
            try:
                info, err = await self._prepare_track_file(track, ddir, with_cover=False)
                if err:
                    await utils.answer(msg, self.strings["download_fail"] if err == "download_fail" else self.strings["error"].format(msg=err))
                    return
                with open(info["path"], "rb") as f:
                    mp3_bytes = f.read()
                filename = sanitize_fn(f"{artist} - {title}") + ".mp3"
                mp3_url = await _upload_to_x0(mp3_bytes, filename, "audio/mpeg")
                if not mp3_url:
                    await utils.answer(msg, self.strings["error"].format(msg="Upload to x0.at failed"))
                    return
                self._now_mp3_url = mp3_url
            finally:
                if os.path.exists(ddir):
                    shutil.rmtree(ddir, ignore_errors=True)
        try:
            await self._client(functions.messages.GetWebPageRequest(url=mp3_url, hash=0))
        except Exception:
            pass
        await asyncio.sleep(1)
        source = await self._get_source(now)
        track_id = now["playable_id"]
        album_id = None
        try:
            albums = now["track"].albums
            if albums:
                album_id = albums[0].id
        except Exception:
            pass
        if album_id:
            link = f"https://music.yandex.ru/album/{album_id}/track/{track_id}"
        else:
            link = f"https://music.yandex.ru/track/{track_id}"
        volume = now.get("volume")
        device_title = now.get("device_title") or self.strings["unknown_device"]
        if volume is not None:
            device_str = f"{escape_html(device_title)} (<tg-emoji emoji-id=5255970510562432622>🔊</tg-emoji>{volume}%)"
        else:
            device_str = escape_html(device_title)
        progress_ms = now.get("progress_ms", 0)
        progress_s = progress_ms // 1000
        played_till = f"{progress_s // 60}:{progress_s % 60:02d}"
        text = self.strings["track_text"].format(
            device=device_str,
            source=source,
            played_till=played_till,
            link=f'<a href="{link}">{self.strings["link_text"]}</a>',
        )
        try:
            await msg.edit(
                text,
                file=InputMediaWebPage(mp3_url, optional=True),
                parse_mode="html",
                link_preview=True,
                invert_media=True,
            )
        except Exception:
            await utils.answer(msg, text)

    @loader.command(
        ru_doc="Поиск трека. Без аргументов открывает форму выбора",
        en_doc="Search track. Without args opens selection form",
    )
    async def yms(self, message: Message):
        """Search track."""
        prefix = self.get_prefix()
        if not await self._ensure_ym():
            await utils.answer(message, self.strings["no_token"].format(prefix=prefix))
            return
        query = utils.get_args_raw(message).strip()
        is_forum = await self._is_forum_chat(message)
        topic_id = self._get_topic_id(message) if is_forum else None
        if not query:
            session_id = str(id(message))
            self._yms_sessions[session_id] = {
                "chat_id": message.chat_id,
                "is_forum": is_forum,
                "topic_id": topic_id,
            }
            form_kwargs = dict(
                text=self.strings["yms_menu_title"],
                message=message,
                reply_markup=[[
                    {
                        "text": self.strings["yms_via_link"],
                        "input": self.strings["yms_enter_link"],
                        "handler": self._yms_link_input,
                        "args": (session_id,),
                        "style": "primary",
                    },
                    {
                        "text": self.strings["yms_via_query"],
                        "input": self.strings["yms_enter_query"],
                        "handler": self._yms_query_input,
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
        if _is_ym_track_link(query):
            await self._yms_download_by_link(message, query, is_forum, topic_id)
            return
        await self._yms_run_search(message, query, is_forum, topic_id)

    async def _yms_link_input(self, call, query: str, session_id: str):
        url = query.strip()
        sess = self._yms_sessions.get(session_id, {})
        if not _is_ym_track_link(url):
            await call.edit(self.strings["yms_no_results"])
            return
        track_id = parse_ym_track_id(url)
        if not track_id:
            await call.edit(self.strings["yms_no_results"])
            return
        await call.edit(self.strings["yms_edit_dl"].format(title=url[:60]))
        track = await self._ym.fetch_track(track_id)
        if not track:
            await call.edit(self.strings["yms_no_results"])
            return
        chat_id = sess.get("chat_id", call._call.from_user.id if hasattr(call, '_call') else 0)
        is_forum = sess.get("is_forum", False)
        topic_id = sess.get("topic_id")
        self._yms_sessions.pop(session_id, None)
        ddir = tempfile.mkdtemp(dir=self._tmp)
        try:
            info, err = await self._prepare_track_file(track, ddir, with_cover=True)
            if err:
                await call.edit(self.strings["download_fail"] if err == "download_fail" else self.strings["error"].format(msg=err))
                return
            await self._send_audio(chat_id, info, reply_to=topic_id if is_forum and topic_id else None)
            await call.delete()
        finally:
            if os.path.exists(ddir):
                shutil.rmtree(ddir, ignore_errors=True)

    async def _yms_query_input(self, call, query: str, session_id: str):
        query = query.strip()
        sess = self._yms_sessions.get(session_id, {})
        if not query:
            await call.edit(self.strings["yms_no_results"])
            return
        await call.edit(self.strings["yms_searching"].format(query=escape_html(query)))
        limit = self._get_limit()
        tracks = await self._ym.search_track(query, count=limit)
        if not tracks:
            await call.edit(self.strings["yms_no_results"])
            return
        chat_id = sess.get("chat_id")
        is_forum = sess.get("is_forum", False)
        topic_id = sess.get("topic_id")
        new_sid = session_id + "_s"
        self._yms_sessions[new_sid] = {
            "tracks": tracks,
            "index": 0,
            "chat_id": chat_id,
            "is_forum": is_forum,
            "topic_id": topic_id,
            "cover_cache": {},
        }
        self._yms_locks[new_sid] = asyncio.Lock()
        track = tracks[0]
        title = YMApiClient.track_title(track)
        artist = YMApiClient.track_artist(track)
        cover_url = await self._yms_get_cover(new_sid, 0)
        markup = self._yms_markup(new_sid, len(tracks))
        edit_kwargs = dict(
            text=f"<b>{escape_html(title)}</b>\n<b>{escape_html(artist)}</b>",
            reply_markup=markup,
        )
        if cover_url:
            edit_kwargs["photo"] = cover_url
        await call.edit(**edit_kwargs)
        asyncio.ensure_future(self._yms_prefetch_covers(new_sid))

    async def _yms_download_by_link(self, message, url, is_forum, topic_id):
        track_id = parse_ym_track_id(url)
        msg = await utils.answer(message, self.strings["uploading"])
        track = await self._ym.fetch_track(track_id)
        if not track:
            await utils.answer(msg, self.strings["yms_no_results"])
            return
        ddir = tempfile.mkdtemp(dir=self._tmp)
        try:
            info, err = await self._prepare_track_file(track, ddir, with_cover=True)
            if err:
                await utils.answer(msg, self.strings["download_fail"] if err == "download_fail" else self.strings["error"].format(msg=err))
                return
            try:
                await msg.delete()
            except Exception:
                pass
            await self._send_audio(message.chat_id, info, reply_to=topic_id if is_forum and topic_id else None)
        finally:
            if os.path.exists(ddir):
                shutil.rmtree(ddir, ignore_errors=True)

    async def _yms_run_search(self, message, query, is_forum, topic_id):
        msg = await utils.answer(
            message,
            self.strings["yms_searching"].format(query=escape_html(query)),
        )
        limit = self._get_limit()
        tracks = await self._ym.search_track(query, count=limit)
        if not tracks:
            await utils.answer(msg, self.strings["yms_no_results"])
            return
        session_id = str(id(message))
        self._yms_sessions[session_id] = {
            "tracks": tracks,
            "index": 0,
            "chat_id": message.chat_id,
            "is_forum": is_forum,
            "topic_id": topic_id,
            "cover_cache": {},
        }
        self._yms_locks[session_id] = asyncio.Lock()
        track = tracks[0]
        title = YMApiClient.track_title(track)
        artist = YMApiClient.track_artist(track)
        cover_url = await self._yms_get_cover(session_id, 0)
        markup = self._yms_markup(session_id, len(tracks))
        try:
            await msg.delete()
        except Exception:
            pass
        form_kwargs = dict(
            text=f"<b>{escape_html(title)}</b>\n<b>{escape_html(artist)}</b>",
            message=message,
            reply_markup=markup,
            silent=True,
        )
        if cover_url:
            form_kwargs["photo"] = cover_url
        if is_forum and topic_id:
            form_kwargs["reply_to"] = topic_id
        await self.inline.form(**form_kwargs)
        asyncio.ensure_future(self._yms_prefetch_covers(session_id))

    async def _yms_get_cover(self, session_id: str, idx: int):
        sess = self._yms_sessions.get(session_id)
        if not sess:
            return None
        if idx in sess["cover_cache"]:
            return sess["cover_cache"][idx]
        track = sess["tracks"][idx]
        if not track.cover_uri:
            sess["cover_cache"][idx] = None
            return None
        title = YMApiClient.track_title(track)
        artist = YMApiClient.track_artist(track)
        raw_cover = await _download_cover_best(track.cover_uri, self._covers_dir)
        cover_url = None
        if raw_cover:
            filename = sanitize_fn(f"{artist} - {title}") + ".jpg"
            cover_url = await _upload_to_x0(
                normalize_cover(raw_cover, force_jpeg=True) or raw_cover,
                filename,
                "image/jpeg",
            )
        sess["cover_cache"][idx] = cover_url
        return cover_url

    async def _yms_prefetch_covers(self, session_id: str):
        sess = self._yms_sessions.get(session_id)
        if not sess:
            return
        tracks = sess["tracks"]
        for i in range(1, len(tracks)):
            if session_id not in self._yms_sessions:
                return
            if i not in sess["cover_cache"]:
                await self._yms_get_cover(session_id, i)
            await asyncio.sleep(0.3)

    def _yms_markup(self, session_id, total):
        sess = self._yms_sessions.get(session_id, {})
        idx = sess.get("index", 0)
        left_btn = {"text": self.strings["btn_left"], "callback": self._yms_left, "args": (session_id,)}
        right_btn = {"text": self.strings["btn_right"], "callback": self._yms_right, "args": (session_id,)}
        if idx > 0:
            left_btn["style"] = "primary"
        if idx < total - 1:
            right_btn["style"] = "primary"
        return [
            [{"text": self.strings["yms_download"], "callback": self._yms_download, "args": (session_id,), "style": "success"}],
            [left_btn, right_btn],
            [{"text": self.strings["yms_cancel"], "callback": self._yms_cancel, "args": (session_id,), "style": "danger"}],
        ]

    def _yms_done_markup(self, session_id):
        return [
            [{
                "text": self.strings["yms_new_search"],
                "input": self.strings["yms_enter_query"],
                "handler": self._yms_new_search_input,
                "args": (session_id,),
                "style": "primary",
            }],
            [{"text": self.strings["yms_cancel"], "callback": self._yms_cancel, "args": (session_id,), "style": "danger"}],
        ]

    async def _yms_new_search_input(self, call, query: str, session_id: str):
        query = query.strip()
        if not query:
            await call.delete()
            return
        sess = self._yms_sessions.get(session_id, {})
        await call.edit(self.strings["yms_searching"].format(query=escape_html(query)))
        limit = self._get_limit()
        tracks = await self._ym.search_track(query, count=limit)
        if not tracks:
            await call.edit(self.strings["yms_no_results"], reply_markup=self._yms_done_markup(session_id))
            return
        chat_id = sess.get("chat_id")
        is_forum = sess.get("is_forum", False)
        topic_id = sess.get("topic_id")
        self._yms_sessions[session_id] = {
            "tracks": tracks,
            "index": 0,
            "chat_id": chat_id,
            "is_forum": is_forum,
            "topic_id": topic_id,
            "cover_cache": {},
        }
        self._yms_locks[session_id] = asyncio.Lock()
        track = tracks[0]
        title = YMApiClient.track_title(track)
        artist = YMApiClient.track_artist(track)
        cover_url = await self._yms_get_cover(session_id, 0)
        markup = self._yms_markup(session_id, len(tracks))
        edit_kwargs = dict(
            text=f"<b>{escape_html(title)}</b>\n<b>{escape_html(artist)}</b>",
            reply_markup=markup,
        )
        if cover_url:
            edit_kwargs["photo"] = cover_url
        await call.edit(**edit_kwargs)
        asyncio.ensure_future(self._yms_prefetch_covers(session_id))

    async def _yms_left(self, call, session_id: str):
        sess = self._yms_sessions.get(session_id)
        if not sess or sess["index"] <= 0:
            await call.answer()
            return
        lock = self._yms_locks.get(session_id)
        if lock and lock.locked():
            await call.answer()
            return
        async with lock:
            sess["index"] -= 1
            await self._yms_update(call, session_id)

    async def _yms_right(self, call, session_id: str):
        sess = self._yms_sessions.get(session_id)
        if not sess or sess["index"] >= len(sess["tracks"]) - 1:
            await call.answer()
            return
        lock = self._yms_locks.get(session_id)
        if lock and lock.locked():
            await call.answer()
            return
        async with lock:
            sess["index"] += 1
            await self._yms_update(call, session_id)

    async def _yms_update(self, call, session_id: str):
        sess = self._yms_sessions[session_id]
        idx = sess["index"]
        track = sess["tracks"][idx]
        title = YMApiClient.track_title(track)
        artist = YMApiClient.track_artist(track)
        cover_url = await self._yms_get_cover(session_id, idx)
        markup = self._yms_markup(session_id, len(sess["tracks"]))
        edit_kwargs = dict(
            text=f"<b>{escape_html(title)}</b>\n<b>{escape_html(artist)}</b>",
            reply_markup=markup,
        )
        if cover_url:
            edit_kwargs["photo"] = cover_url
        await call.edit(**edit_kwargs)

    async def _yms_download(self, call, session_id: str):
        sess = self._yms_sessions.get(session_id)
        if not sess:
            await call.answer()
            return
        lock = self._yms_locks.get(session_id)
        if lock and lock.locked():
            await call.answer()
            return
        async with lock:
            track = sess["tracks"][sess["index"]]
            title = YMApiClient.track_title(track)
            artist = YMApiClient.track_artist(track)
            chat_id = sess["chat_id"]
            is_forum = sess.get("is_forum", False)
            topic_id = sess.get("topic_id")
            await call.answer(self.strings["yms_downloading"])
            try:
                await call.edit(
                    self.strings["yms_edit_dl"].format(title=escape_html(f"{artist} - {title}")),
                    reply_markup=[],
                )
            except Exception:
                pass
            ddir = tempfile.mkdtemp(dir=self._tmp)
            try:
                info, err = await self._prepare_track_file(track, ddir, with_cover=True)
                if err:
                    try:
                        await call.edit(self.strings["download_fail"] if err == "download_fail" else self.strings["error"].format(msg=err), reply_markup=self._yms_done_markup(session_id))
                    except Exception:
                        pass
                    return
                await self._send_audio(chat_id, info, reply_to=topic_id if is_forum and topic_id else None)
            finally:
                if os.path.exists(ddir):
                    shutil.rmtree(ddir, ignore_errors=True)
            try:
                await call.edit(
                    f"<b>{escape_html(artist)} - {escape_html(title)}</b>",
                    reply_markup=self._yms_done_markup(session_id),
                )
            except Exception:
                pass

    async def _yms_cancel(self, call, session_id: str):
        self._yms_sessions.pop(session_id, None)
        self._yms_locks.pop(session_id, None)
        await call.delete()

    @loader.command(
        ru_doc="Скачать плейлист. Можно передать ссылку аргументом",
        en_doc="Download playlist. Can pass link as argument",
    )
    async def ymp(self, message: Message):
        """Download playlist."""
        prefix = self.get_prefix()
        if not await self._ensure_ym():
            await utils.answer(message, self.strings["no_token"].format(prefix=prefix))
            return
        args = utils.get_args_raw(message).strip()
        is_forum = await self._is_forum_chat(message)
        topic_id = self._get_topic_id(message) if is_forum else None
        session_id = str(id(message))
        self._ymp_sessions[session_id] = {
            "chat_id": message.chat_id,
            "is_forum": is_forum,
            "topic_id": topic_id,
            "kill": False,
        }
        if args:
            msg = await utils.answer(message, self.strings["ymp_fetching_pl"])
            playlist, is_liked = await self._resolve_playlist_url(args)
            if is_liked:
                tracks = await self._ym.fetch_liked_tracks()
                if not tracks:
                    await utils.answer(msg, self.strings["ymp_not_found"])
                    return
                pl_title = self.strings["ymp_favorites"]
                self._ymp_sessions[session_id].update({
                    "tracks_list": tracks,
                    "tracks_short": None,
                    "pl_title": pl_title,
                    "count": len(tracks),
                })
                try:
                    await msg.delete()
                except Exception:
                    pass
                cover_url = await self._get_x0_cover_url(FAVORITE_COVER_URL, "favorites")
                await self._ymp_show_confirm_form(message, session_id, pl_title, len(tracks), cover_url, is_forum, topic_id)
                return
            if not playlist:
                await utils.answer(msg, self.strings["ymp_not_found"])
                return
            tracks_short = getattr(playlist, "tracks", None) or []
            if not tracks_short:
                await utils.answer(msg, self.strings["ymp_not_found"])
                return
            pl_title = getattr(playlist, "title", None) or "Playlist"
            self._ymp_sessions[session_id].update({
                "playlist": playlist,
                "tracks_short": tracks_short,
                "tracks_list": None,
                "pl_title": pl_title,
                "count": len(tracks_short),
            })
            raw_cover = await self._ym.fetch_playlist_cover_url(playlist)
            cover_url = None
            if raw_cover:
                cover_url = await self._get_x0_cover_from_url(raw_cover, sanitize_fn(pl_title))
            try:
                await msg.delete()
            except Exception:
                pass
            await self._ymp_show_confirm_form(message, session_id, pl_title, len(tracks_short), cover_url, is_forum, topic_id)
            return
        form_kwargs = dict(
            text=self.strings["ymp_menu_title"],
            message=message,
            reply_markup=[[
                {"text": self.strings["ymp_menu_my"], "callback": self._ymp_open_my, "args": (session_id,), "style": "primary"},
                {"text": self.strings["ymp_menu_link"], "input": self.strings["ymp_enter_link"], "handler": self._ymp_link_input, "args": (session_id,), "style": "primary"},
            ]],
            silent=True,
        )
        if is_forum and topic_id:
            form_kwargs["reply_to"] = topic_id
        await self.inline.form(**form_kwargs)

    async def _ymp_show_confirm_form(self, message, session_id, pl_title, count, cover_url, is_forum, topic_id):
        form_kwargs = dict(
            text=self.strings["ymp_title"].format(name=escape_html(pl_title), count=count),
            message=message,
            reply_markup=[[
                {"text": self.strings["ymp_download"], "callback": self._ymp_download, "args": (session_id,), "style": "success"},
                {"text": self.strings["ymp_cancel"], "callback": self._ymp_cancel, "args": (session_id,), "style": "danger"},
            ]],
            silent=True,
        )
        if cover_url:
            form_kwargs["photo"] = cover_url
        if is_forum and topic_id:
            form_kwargs["reply_to"] = topic_id
        await self.inline.form(**form_kwargs)

    async def _get_x0_cover_url(self, raw_url, name):
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(raw_url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                    if r.status == 200:
                        data = await r.read()
                        return await _upload_to_x0(
                            normalize_cover(data, force_jpeg=True) or data,
                            sanitize_fn(name) + ".jpg",
                            "image/jpeg",
                        )
        except Exception:
            pass
        return None

    async def _get_x0_cover_from_url(self, raw_url, name):
        return await self._get_x0_cover_url(raw_url, name)

    async def _resolve_playlist_url(self, url):
        if YM_PLAYLIST_LK_RE.search(url):
            return None, True
        if YM_PLAYLIST_RE.search(url):
            m = YM_PLAYLIST_RE.search(url)
            pl = await self._ym.fetch_playlist(m.group(1), m.group(2))
            return pl, False
        if YM_PLAYLIST_UUID_RE.search(url):
            mu = YM_PLAYLIST_UUID_RE.search(url)
            pl = await self._ym.fetch_playlist_by_uuid(mu.group(1))
            return pl, False
        return None, False

    async def _ymp_link_input(self, call, query: str, session_id: str):
        url = query.strip()
        sess = self._ymp_sessions.get(session_id)
        if not sess:
            await call.answer("Session expired", show_alert=True)
            return
        await call.edit(self.strings["ymp_fetching_pl"])
        playlist, is_liked = await self._resolve_playlist_url(url)
        if is_liked:
            await self._ymp_show_liked(call, session_id)
            return
        if not playlist:
            await call.edit(self.strings["ymp_not_found"], reply_markup=[[{"text": self.strings["ymp_cancel"], "callback": self._ymp_cancel, "args": (session_id,), "style": "danger"}]])
            return
        await self._ymp_show_playlist(call, session_id, playlist)

    async def _ymp_show_liked(self, call, session_id: str):
        sess = self._ymp_sessions.get(session_id)
        if not sess:
            await call.answer()
            return
        tracks = await self._ym.fetch_liked_tracks()
        if not tracks:
            await call.edit(self.strings["ymp_not_found"])
            return
        pl_title = self.strings["ymp_favorites"]
        count = len(tracks)
        sess.update({"tracks_list": tracks, "tracks_short": None, "pl_title": pl_title, "count": count})
        x0_url = await self._get_x0_cover_url(FAVORITE_COVER_URL, "favorites")
        markup = [[
            {"text": self.strings["ymp_download"], "callback": self._ymp_download, "args": (session_id,), "style": "success"},
            {"text": self.strings["ymp_cancel"], "callback": self._ymp_cancel, "args": (session_id,), "style": "danger"},
        ]]
        edit_kwargs = dict(text=self.strings["ymp_title"].format(name=escape_html(pl_title), count=count), reply_markup=markup)
        if x0_url:
            edit_kwargs["photo"] = x0_url
        await call.edit(**edit_kwargs)

    async def _ymp_open_my(self, call, session_id: str):
        sess = self._ymp_sessions.get(session_id)
        if not sess:
            await call.answer()
            return
        await call.edit(self.strings["ymp_fetching_pl"])
        all_meta = await self._ym.fetch_all_playlists_meta()
        liked_tracks = await self._ym.fetch_liked_tracks()
        liked_count = len(liked_tracks)
        favorites_entry = {"is_favorites": True, "title": self.strings["ymp_favorites"], "tracks_count": liked_count, "tracks_list": liked_tracks}
        all_items = [favorites_entry] + list(all_meta)
        cover_cache = {}
        for i, item in enumerate(all_items):
            if isinstance(item, dict):
                cover_cache[i] = FAVORITE_COVER_URL
            else:
                og = getattr(item, "og_image", None)
                if og:
                    best = await _get_best_cover_url(og)
                    if best:
                        cover_cache[i] = best
        my_sid = session_id + "_my"
        self._ymp_my_sessions[my_sid] = {"items": all_items, "index": 0, "cover_cache": cover_cache, "x0_cache": {}, "parent_sid": session_id}
        await self._ymp_my_render(call, my_sid)

    async def _ymp_my_render(self, call, my_sid: str):
        sess = self._ymp_my_sessions.get(my_sid)
        if not sess:
            return
        idx = sess["index"]
        item = sess["items"][idx]
        if isinstance(item, dict):
            title = item["title"]
            count = item["tracks_count"]
        else:
            title = getattr(item, "title", None) or "Playlist"
            count = getattr(item, "track_count", None) or getattr(item, "tracks_count", None) or len(getattr(item, "tracks", None) or [])
        raw_cover_url = sess["cover_cache"].get(idx)
        x0_url = sess["x0_cache"].get(idx)
        if raw_cover_url and not x0_url:
            x0_url = await self._get_x0_cover_url(raw_cover_url, title)
            sess["x0_cache"][idx] = x0_url
        markup = self._ymp_my_markup(my_sid, len(sess["items"]))
        edit_kwargs = dict(text=self.strings["ymp_playlist_title"].format(title=escape_html(title), count=count), reply_markup=markup)
        if x0_url:
            edit_kwargs["photo"] = x0_url
        await call.edit(**edit_kwargs)

    def _ymp_my_markup(self, my_sid: str, total: int):
        sess = self._ymp_my_sessions.get(my_sid, {})
        idx = sess.get("index", 0)
        parent_sid = sess.get("parent_sid", my_sid.replace("_my", ""))
        left = {"text": self.strings["btn_left"], "callback": self._ymp_my_left, "args": (my_sid,)}
        right = {"text": self.strings["btn_right"], "callback": self._ymp_my_right, "args": (my_sid,)}
        if idx > 0:
            left["style"] = "primary"
        if idx < total - 1:
            right["style"] = "primary"
        return [
            [{"text": self.strings["ymp_download"], "callback": self._ymp_my_select, "args": (my_sid,), "style": "success"}],
            [left, right],
            [{"text": self.strings["ymp_cancel"], "callback": self._ymp_cancel, "args": (parent_sid,), "style": "danger"}],
        ]

    async def _ymp_my_left(self, call, my_sid: str):
        sess = self._ymp_my_sessions.get(my_sid)
        if not sess or sess["index"] <= 0:
            await call.answer()
            return
        sess["index"] -= 1
        await self._ymp_my_render(call, my_sid)

    async def _ymp_my_right(self, call, my_sid: str):
        sess = self._ymp_my_sessions.get(my_sid)
        if not sess or sess["index"] >= len(sess["items"]) - 1:
            await call.answer()
            return
        sess["index"] += 1
        await self._ymp_my_render(call, my_sid)

    async def _ymp_my_select(self, call, my_sid: str):
        sess = self._ymp_my_sessions.get(my_sid)
        if not sess:
            await call.answer()
            return
        idx = sess["index"]
        item = sess["items"][idx]
        parent_sid = sess.get("parent_sid", my_sid.replace("_my", ""))
        self._ymp_my_sessions.pop(my_sid, None)
        parent_sess = self._ymp_sessions.get(parent_sid)
        if not parent_sess:
            await call.answer()
            return
        await call.edit(self.strings["ymp_fetching_pl"])
        if isinstance(item, dict) and item.get("is_favorites"):
            tracks = item["tracks_list"]
            pl_title = item["title"]
            parent_sess.update({"tracks_list": tracks, "tracks_short": None, "pl_title": pl_title, "count": len(tracks)})
            await self._ymp_download(call, parent_sid)
            return
        owner = getattr(item, "owner", None)
        login = getattr(owner, "login", None) if owner else None
        kind = getattr(item, "kind", None)
        if not login or kind is None:
            await call.answer("Error", show_alert=True)
            return
        playlist = await self._ym.fetch_playlist(login, kind)
        if not playlist:
            await call.edit(self.strings["ymp_not_found"])
            return
        tracks_short = getattr(playlist, "tracks", None) or []
        if not tracks_short:
            await call.edit(self.strings["ymp_not_found"])
            return
        pl_title = getattr(playlist, "title", None) or "Playlist"
        parent_sess.update({"playlist": playlist, "tracks_short": tracks_short, "tracks_list": None, "pl_title": pl_title, "count": len(tracks_short)})
        await self._ymp_download(call, parent_sid)

    async def _ymp_show_playlist(self, call, session_id: str, playlist):
        pl_title = getattr(playlist, "title", None) or "Playlist"
        tracks_short = getattr(playlist, "tracks", None) or []
        count = len(tracks_short)
        if not tracks_short:
            await call.edit(self.strings["ymp_not_found"])
            return
        sess = self._ymp_sessions.get(session_id)
        if sess:
            sess.update({"playlist": playlist, "tracks_short": tracks_short, "tracks_list": None, "pl_title": pl_title, "count": count})
        raw_cover_url = await self._ym.fetch_playlist_cover_url(playlist)
        x0_url = None
        if raw_cover_url:
            x0_url = await self._get_x0_cover_url(raw_cover_url, pl_title)
        markup = [[
            {"text": self.strings["ymp_download"], "callback": self._ymp_download, "args": (session_id,), "style": "success"},
            {"text": self.strings["ymp_cancel"], "callback": self._ymp_cancel, "args": (session_id,), "style": "danger"},
        ]]
        edit_kwargs = dict(text=self.strings["ymp_title"].format(name=escape_html(pl_title), count=count), reply_markup=markup)
        if x0_url:
            edit_kwargs["photo"] = x0_url
        await call.edit(**edit_kwargs)

    async def _ymp_download(self, call, session_id: str):
        sess = self._ymp_sessions.get(session_id)
        if not sess:
            await call.answer()
            return
        pl_title = sess["pl_title"]
        count = sess["count"]
        await call.edit(
            self.strings["ymp_progress"].format(name=escape_html(pl_title), done=0, total=count),
            reply_markup=[[{"text": self.strings["ymp_kill"], "callback": self._ymp_kill, "args": (session_id,), "style": "danger"}]],
        )
        chat_id = sess["chat_id"]
        is_forum = sess.get("is_forum", False)
        topic_id = sess.get("topic_id")
        reply_to = topic_id if is_forum and topic_id else None
        tracks_list = sess.get("tracks_list")
        tracks_short = sess.get("tracks_short")
        items_iter = tracks_list if tracks_list is not None else (tracks_short or [])
        use_direct = tracks_list is not None
        done = 0
        for ts in items_iter:
            if sess.get("kill"):
                break
            try:
                if use_direct:
                    track = ts
                else:
                    track = getattr(ts, "track", None)
                    if track is None:
                        tid = str(getattr(ts, "track_id", None) or getattr(ts, "id", None) or "").split(":")[0]
                        if not tid:
                            continue
                        track = await self._ym.fetch_track(tid)
                if not track:
                    continue
                ddir = tempfile.mkdtemp(dir=self._tmp)
                try:
                    info, err = await self._prepare_track_file(track, ddir, with_cover=True)
                    if err:
                        continue
                    await self._send_audio(chat_id, info, reply_to=reply_to)
                    done += 1
                finally:
                    if os.path.exists(ddir):
                        shutil.rmtree(ddir, ignore_errors=True)
                if not sess.get("kill"):
                    try:
                        await call.edit(
                            self.strings["ymp_progress"].format(name=escape_html(pl_title), done=done, total=count),
                            reply_markup=[[{"text": self.strings["ymp_kill"], "callback": self._ymp_kill, "args": (session_id,), "style": "danger"}]],
                        )
                    except Exception:
                        pass
            except Exception as e:
                _log("YMP", f"Track error: {e}")
                continue
        self._ymp_sessions.pop(session_id, None)
        try:
            await call.delete()
        except Exception:
            pass

    async def _ymp_kill(self, call, session_id: str):
        sess = self._ymp_sessions.get(session_id)
        if sess:
            sess["kill"] = True
        await call.answer()

    async def _ymp_cancel(self, call, session_id: str):
        self._ymp_sessions.pop(session_id, None)
        self._ymp_my_sessions.pop(session_id + "_my", None)
        await call.delete()

    @loader.command(
        ru_doc="Скачать аудиокнигу. Без аргументов открывает форму выбора",
        en_doc="Download audiobook. Without args opens selection form",
    )
    async def ymb(self, message: Message):
        """Download audiobook."""
        prefix = self.get_prefix()
        if not await self._ensure_ym():
            await utils.answer(message, self.strings["no_token"].format(prefix=prefix))
            return
        args = utils.get_args_raw(message).strip()
        is_forum = await self._is_forum_chat(message)
        topic_id = self._get_topic_id(message) if is_forum else None
        session_id = str(id(message))
        self._ymb_sessions[session_id] = {
            "chat_id": message.chat_id,
            "is_forum": is_forum,
            "topic_id": topic_id,
            "kill": False,
        }
        if args:
            if not _is_ym_album_link(args):
                await utils.answer(message, self.strings["ymb_not_book"])
                return
            m = YM_ALBUM_RE.search(args)
            if not m:
                await utils.answer(message, self.strings["ymb_not_found"])
                return
            album_id = int(m.group(1))
            msg = await utils.answer(message, self.strings["ymb_fetching"])
            al = await self._ym.fetch_album_with_tracks(album_id)
            if not al or getattr(al, "type", None) != "audiobook":
                await utils.answer(msg, self.strings["ymb_not_book"])
                return
            try:
                await msg.delete()
            except Exception:
                pass
            await self._ymb_show_book_form(message, session_id, al, is_forum, topic_id)
            return
        form_kwargs = dict(
            text=self.strings["ymb_menu_title"],
            message=message,
            reply_markup=[[
                {"text": self.strings["ymb_menu_my"], "callback": self._ymb_open_my, "args": (session_id,), "style": "primary"},
                {"text": self.strings["ymb_menu_link"], "input": self.strings["ymb_enter_link"], "handler": self._ymb_link_input, "args": (session_id,), "style": "primary"},
            ]],
            silent=True,
        )
        if is_forum and topic_id:
            form_kwargs["reply_to"] = topic_id
        await self.inline.form(**form_kwargs)

    async def _ymb_show_book_form(self, message, session_id, al, is_forum, topic_id):
        title = getattr(al, "title", None) or "Audiobook"
        tracks = al.volumes[0] if al.volumes else []
        count = len(tracks)
        artist_names = ", ".join(a.name for a in (al.artists or []) if a.name) or "Unknown"
        self._ymb_sessions[session_id].update({
            "album": al,
            "tracks": tracks,
            "title": title,
            "artist": artist_names,
            "count": count,
        })
        cover_uri = getattr(al, "cover_uri", None) or getattr(al, "og_image", None)
        x0_url = None
        if cover_uri:
            raw = await _download_cover_best(cover_uri, self._covers_dir)
            if raw:
                x0_url = await _upload_to_x0(normalize_cover(raw, force_jpeg=True) or raw, sanitize_fn(title) + ".jpg", "image/jpeg")
        form_kwargs = dict(
            text=self.strings["ymb_title"].format(name=escape_html(title), count=count),
            message=message,
            reply_markup=[[
                {"text": self.strings["ymb_download"], "callback": self._ymb_download, "args": (session_id,), "style": "success"},
                {"text": self.strings["ymb_cancel"], "callback": self._ymb_cancel, "args": (session_id,), "style": "danger"},
            ]],
            silent=True,
        )
        if x0_url:
            form_kwargs["photo"] = x0_url
        if is_forum and topic_id:
            form_kwargs["reply_to"] = topic_id
        await self.inline.form(**form_kwargs)

    async def _ymb_link_input(self, call, query: str, session_id: str):
        url = query.strip()
        sess = self._ymb_sessions.get(session_id)
        if not sess:
            await call.answer("Session expired", show_alert=True)
            return
        if not _is_ym_album_link(url):
            await call.edit(self.strings["ymb_not_book"])
            return
        m = YM_ALBUM_RE.search(url)
        if not m:
            await call.edit(self.strings["ymb_not_found"])
            return
        album_id = int(m.group(1))
        await call.edit(self.strings["ymb_fetching"])
        al = await self._ym.fetch_album_with_tracks(album_id)
        if not al or getattr(al, "type", None) != "audiobook":
            await call.edit(self.strings["ymb_not_book"])
            return
        title = getattr(al, "title", None) or "Audiobook"
        tracks = al.volumes[0] if al.volumes else []
        count = len(tracks)
        artist_names = ", ".join(a.name for a in (al.artists or []) if a.name) or "Unknown"
        sess.update({"album": al, "tracks": tracks, "title": title, "artist": artist_names, "count": count})
        cover_uri = getattr(al, "cover_uri", None) or getattr(al, "og_image", None)
        x0_url = None
        if cover_uri:
            raw = await _download_cover_best(cover_uri, self._covers_dir)
            if raw:
                x0_url = await _upload_to_x0(normalize_cover(raw, force_jpeg=True) or raw, sanitize_fn(title) + ".jpg", "image/jpeg")
        markup = [[
            {"text": self.strings["ymb_download"], "callback": self._ymb_download, "args": (session_id,), "style": "success"},
            {"text": self.strings["ymb_cancel"], "callback": self._ymb_cancel, "args": (session_id,), "style": "danger"},
        ]]
        edit_kwargs = dict(text=self.strings["ymb_title"].format(name=escape_html(title), count=count), reply_markup=markup)
        if x0_url:
            edit_kwargs["photo"] = x0_url
        await call.edit(**edit_kwargs)

    async def _ymb_open_my(self, call, session_id: str):
        sess = self._ymb_sessions.get(session_id)
        if not sess:
            await call.answer()
            return
        await call.edit(self.strings["ymb_fetching"])
        books = await self._ym.fetch_liked_albums_books()
        if not books:
            await call.edit(self.strings["ymb_no_books"], reply_markup=[[{"text": self.strings["ymb_cancel"], "callback": self._ymb_cancel, "args": (session_id,), "style": "danger"}]])
            return
        cover_cache = {}
        for i, al in enumerate(books):
            uri = getattr(al, "cover_uri", None) or getattr(al, "og_image", None)
            if uri:
                best = await _get_best_cover_url(uri)
                if best:
                    cover_cache[i] = best
        my_sid = session_id + "_my"
        self._ymb_my_sessions[my_sid] = {"books": books, "index": 0, "cover_cache": cover_cache, "x0_cache": {}, "parent_sid": session_id}
        await self._ymb_my_render(call, my_sid)

    async def _ymb_my_render(self, call, my_sid: str):
        sess = self._ymb_my_sessions.get(my_sid)
        if not sess:
            return
        idx = sess["index"]
        al = sess["books"][idx]
        title = getattr(al, "title", None) or "Audiobook"
        track_count = getattr(al, "track_count", None) or 0
        raw_cover = sess["cover_cache"].get(idx)
        x0_url = sess["x0_cache"].get(idx)
        if raw_cover and not x0_url:
            x0_url = await self._get_x0_cover_url(raw_cover, title)
            sess["x0_cache"][idx] = x0_url
        markup = self._ymb_my_markup(my_sid, len(sess["books"]))
        edit_kwargs = dict(text=self.strings["ymb_title"].format(name=escape_html(title), count=track_count), reply_markup=markup)
        if x0_url:
            edit_kwargs["photo"] = x0_url
        await call.edit(**edit_kwargs)

    def _ymb_my_markup(self, my_sid: str, total: int):
        sess = self._ymb_my_sessions.get(my_sid, {})
        idx = sess.get("index", 0)
        parent_sid = sess.get("parent_sid", my_sid.replace("_my", ""))
        left = {"text": self.strings["btn_left"], "callback": self._ymb_my_left, "args": (my_sid,)}
        right = {"text": self.strings["btn_right"], "callback": self._ymb_my_right, "args": (my_sid,)}
        if idx > 0:
            left["style"] = "primary"
        if idx < total - 1:
            right["style"] = "primary"
        return [
            [{"text": self.strings["ymb_download"], "callback": self._ymb_my_select, "args": (my_sid,), "style": "success"}],
            [left, right],
            [{"text": self.strings["ymb_cancel"], "callback": self._ymb_cancel, "args": (parent_sid,), "style": "danger"}],
        ]

    async def _ymb_my_left(self, call, my_sid: str):
        sess = self._ymb_my_sessions.get(my_sid)
        if not sess or sess["index"] <= 0:
            await call.answer()
            return
        sess["index"] -= 1
        await self._ymb_my_render(call, my_sid)

    async def _ymb_my_right(self, call, my_sid: str):
        sess = self._ymb_my_sessions.get(my_sid)
        if not sess or sess["index"] >= len(sess["books"]) - 1:
            await call.answer()
            return
        sess["index"] += 1
        await self._ymb_my_render(call, my_sid)

    async def _ymb_my_select(self, call, my_sid: str):
        sess = self._ymb_my_sessions.get(my_sid)
        if not sess:
            await call.answer()
            return
        idx = sess["index"]
        al_meta = sess["books"][idx]
        parent_sid = sess.get("parent_sid", my_sid.replace("_my", ""))
        self._ymb_my_sessions.pop(my_sid, None)
        parent_sess = self._ymb_sessions.get(parent_sid)
        if not parent_sess:
            await call.answer()
            return
        await call.edit(self.strings["ymb_fetching"])
        al = await self._ym.fetch_album_with_tracks(al_meta.id)
        if not al:
            await call.edit(self.strings["ymb_not_found"])
            return
        title = getattr(al, "title", None) or "Audiobook"
        tracks = al.volumes[0] if al.volumes else []
        artist_names = ", ".join(a.name for a in (al.artists or []) if a.name) or "Unknown"
        parent_sess.update({"album": al, "tracks": tracks, "title": title, "artist": artist_names, "count": len(tracks)})
        await self._ymb_download(call, parent_sid)

    async def _ymb_download(self, call, session_id: str):
        sess = self._ymb_sessions.get(session_id)
        if not sess:
            await call.answer()
            return
        book_title = sess["title"]
        artist = sess["artist"]
        tracks = sess["tracks"]
        count = sess["count"]
        chat_id = sess["chat_id"]
        is_forum = sess.get("is_forum", False)
        topic_id = sess.get("topic_id")
        reply_to = topic_id if is_forum and topic_id else None
        al = sess.get("album")
        cover_data = None
        if al:
            cover_uri = getattr(al, "cover_uri", None) or getattr(al, "og_image", None)
            if cover_uri:
                cover_data = await _download_cover_best(cover_uri, self._covers_dir)
        await call.edit(
            self.strings["ymb_progress"].format(name=escape_html(book_title), done=0, total=count),
            reply_markup=[[{"text": self.strings["ymb_kill"], "callback": self._ymb_kill, "args": (session_id,), "style": "danger"}]],
        )
        done = 0
        stopped_at = None
        for idx, track in enumerate(tracks, start=1):
            if sess.get("kill"):
                break
            part_title = YMApiClient.track_title(track)
            try:
                ddir = tempfile.mkdtemp(dir=self._tmp)
                try:
                    info, err = await self._prepare_book_part(track, ddir, cover_data, book_title)
                    if err:
                        _log("BOOK", f"Stopped at part {idx} ({part_title}): {err}")
                        stopped_at = (idx, part_title, err)
                        break
                    info["artist"] = artist
                    sent_ok = await self._send_audio(chat_id, info, reply_to=reply_to)
                    if not sent_ok:
                        _log("BOOK", f"Stopped at part {idx} ({part_title}): send_fail")
                        stopped_at = (idx, part_title, "send_fail")
                        break
                    done += 1
                finally:
                    if os.path.exists(ddir):
                        shutil.rmtree(ddir, ignore_errors=True)
                if not sess.get("kill"):
                    try:
                        await call.edit(
                            self.strings["ymb_progress"].format(name=escape_html(book_title), done=done, total=count),
                            reply_markup=[[{"text": self.strings["ymb_kill"], "callback": self._ymb_kill, "args": (session_id,), "style": "danger"}]],
                        )
                    except Exception:
                        pass
            except Exception as e:
                _log("YMB", f"Stopped at part {idx} ({part_title}) error: {e}")
                stopped_at = (idx, part_title, str(e))
                break
        self._ymb_sessions.pop(session_id, None)
        try:
            await call.delete()
        except Exception:
            pass
        if stopped_at:
            idx, part_title, err = stopped_at
            try:
                await self._client.send_message(
                    chat_id,
                    self.strings["ymb_stopped"].format(
                        done=done, total=count, idx=idx, name=escape_html(part_title), error=escape_html(str(err))
                    ),
                    parse_mode="html",
                    reply_to=reply_to,
                )
            except Exception:
                pass

    async def _ymb_kill(self, call, session_id: str):
        sess = self._ymb_sessions.get(session_id)
        if sess:
            sess["kill"] = True
        await call.answer()

    async def _ymb_cancel(self, call, session_id: str):
        self._ymb_sessions.pop(session_id, None)
        self._ymb_my_sessions.pop(session_id + "_my", None)
        await call.delete()

    async def on_unload(self):
        self._ymauth_sessions.clear()
        self._yms_sessions.clear()
        self._yms_locks.clear()
        self._ymp_sessions.clear()
        self._ymp_my_sessions.clear()
        self._ymb_sessions.clear()
        self._ymb_my_sessions.clear()
        if self._tmp and os.path.exists(self._tmp):
            shutil.rmtree(self._tmp, ignore_errors=True)