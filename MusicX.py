__version__ = (2, 2, 3)
# meta developer: I_execute.t.me

import os
import io
import re
import time
import logging
import tempfile
import shutil
import asyncio
import subprocess
import sys

from aiogram.types import (
    InlineQuery,
    InlineQueryResultCachedAudio,
    InlineQueryResultArticle,
    InputTextMessageContent,
    BufferedInputFile,
)

from telethon.tl.types import Message

from .. import loader, utils

logger = logging.getLogger(__name__)

INLINE_QUERY_BANNER = "https://github.com/FireJester/Media/raw/main/Banner_for_inline_query_in_MusicX_new.jpeg"


def _ensure_all_deps():
    for mod, pip in {
        "aiohttp": "aiohttp",
        "mutagen": "mutagen",
        "Crypto": "pycryptodome",
        "m3u8": "m3u8",
        "yandex_music": "yandex-music",
        "yt_dlp": "yt-dlp",
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
from PIL import Image

try:
    import m3u8 as m3u8_lib
    from Crypto.Cipher import AES
    from Crypto.Util.Padding import unpad
except ImportError:
    m3u8_lib = None

try:
    from mutagen.id3 import ID3, TIT2, TPE1, TALB, APIC, ID3NoHeaderError
except ImportError:
    ID3 = None

try:
    import yt_dlp as yt_dlp_lib
except ImportError:
    yt_dlp_lib = None

REQUEST_OK = 200
MAX_FILE_SIZE = 50 * 1024 * 1024
MAX_CONCURRENT = 15
SEG_TIMEOUT = 30
CACHE_TTL = 600

VK_AUDIO_RE = re.compile(
    r"https?://(?:www\.)?(?:vk\.com|vk\.ru)/audio(-?\d+)_(\d+)(?:_([a-f0-9]+))?"
)
VK_TOKEN_RE = re.compile(r"access_token=([A-Za-z0-9._-]+)")
YM_ALBUM_TRACK_RE = re.compile(
    r"https?://music\.yandex\.(?:ru|com|by|kz|uz)/album/\d+/track/(\d+)"
)
YM_DIRECT_TRACK_RE = re.compile(
    r"https?://music\.yandex\.(?:ru|com|by|kz|uz)/track/(\d+)"
)
YM_TOKEN_PATTERN = re.compile(r"access_token=([^&]+)")
YT_URL_RE = re.compile(
    r"https?://(?:www\.)?(?:youtube\.com/watch\?v=|youtu\.be/|youtube\.com/shorts/|music\.youtube\.com/watch\?v=)([A-Za-z0-9_-]{11})"
)
VK_REDIRECT_RE = re.compile(r"oauth\.vk\.com/blank\.html")
YM_REDIRECT_RE = re.compile(r"(?:oauth\.yandex\.\w+|music\.yandex\.\w+)")
OG_IMAGE_RE = re.compile(
    r'<meta\s+(?:property|name)=["\']og:image["\']\s+content=["\']([^"\']+)["\']',
    re.IGNORECASE,
)
OG_IMAGE_RE2 = re.compile(
    r'<meta\s+content=["\']([^"\']+)["\']\s+(?:property|name)=["\']og:image["\']',
    re.IGNORECASE,
)

VK_KATE_APP_ID = 2685278
VK_REDIRECT = "https://oauth.vk.com/blank.html"
VK_API_BASE = "https://api.vk.com/method"
VK_API_VERSION = "5.131"
YM_CLIENT_ID = "23cabbbdc6cd418abb4b39c32c41195d"

VK_USER_AGENTS = {
    "kate": "KateMobileAndroid/100.1 lite-530 (Android 13; SDK 33; arm64-v8a; Xiaomi; Mi 9T Pro; cepheus; ru; 320)",
    "vk_android": "VKAndroidApp/8.31-17556 (Android 13; SDK 33; arm64-v8a; Xiaomi; Mi 9T Pro; cepheus; ru; 320)",
    "vk_iphone": "com.vk.vkclient/1032 (iPhone, iOS 16.0, iPhone14,5, Scale/3.0)",
    "chrome": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "facebook": "facebookexternalhit/1.1",
}

STUB_URLS = ["audio_api_unavailable.mp3", "audio_api_unavailable"]
STUB_TITLES = [
    "\u0410\u0443\u0434\u0438\u043e \u0434\u043e\u0441\u0442\u0443\u043f\u043d\u043e \u043d\u0430 vk.com",
    "Audio is available on vk.com",
]

SOURCE_VK = "vk"
SOURCE_YM = "ym"
SOURCE_YT = "yt"


def escape_html(t):
    return (t or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def sanitize_fn(n):
    return re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", n).strip(". ")[:180] or "track"


def fmt_dur(s):
    return f"{s // 60}:{s % 60:02d}" if s and s > 0 else "0:00"


def fmt_dur_ms(ms):
    s = (ms or 0) // 1000
    return f"{s // 60}:{s % 60:02d}" if s > 0 else "0:00"


def detect_source(text):
    if not text:
        return None
    if VK_AUDIO_RE.search(text):
        return SOURCE_VK
    if YM_ALBUM_TRACK_RE.search(text) or YM_DIRECT_TRACK_RE.search(text):
        return SOURCE_YM
    if YT_URL_RE.search(text):
        return SOURCE_YT
    return None


def parse_vk_link(text):
    m = VK_AUDIO_RE.search(text or "")
    if m:
        return int(m.group(1)), int(m.group(2)), m.group(3)
    return None, None, None


def parse_ym_track_id(text):
    if not text:
        return None
    for pat in [YM_ALBUM_TRACK_RE, YM_DIRECT_TRACK_RE]:
        m = pat.search(text)
        if m:
            return m.group(1)
    return None


def parse_yt_url(text):
    if not text:
        return None
    m = YT_URL_RE.search(text)
    return m.group(0) if m else None


def parse_yt_video_id(text):
    if not text:
        return None
    m = YT_URL_RE.search(text)
    return m.group(1) if m else None


def extract_vk_token(text):
    if not text:
        return None
    m = VK_TOKEN_RE.search(text)
    return m.group(1) if m else None


def extract_ym_token(text):
    if not text:
        return None
    m = YM_TOKEN_PATTERN.search(text)
    return m.group(1) if m else None


def _is_stub_url(url):
    if not url:
        return True
    return any(s in url for s in STUB_URLS)


def _is_stub_title(title):
    if not title:
        return False
    return any(s.lower() in title.lower() for s in STUB_TITLES)


def _build_vk_auth_url():
    return (
        f"https://oauth.vk.com/authorize?client_id={VK_KATE_APP_ID}"
        f"&display=page&redirect_uri={VK_REDIRECT}"
        f"&scope=audio,offline&response_type=token&v={VK_API_VERSION}"
    )


def _build_ym_auth_url():
    return (
        f"https://oauth.yandex.ru/authorize?"
        f"response_type=token&client_id={YM_CLIENT_ID}"
    )


def _detect_token_source(text):
    if not text:
        return None
    if VK_REDIRECT_RE.search(text):
        return SOURCE_VK
    if YM_REDIRECT_RE.search(text):
        return SOURCE_YM
    token_match = re.search(r"access_token=([^&]+)", text)
    if token_match:
        token_val = token_match.group(1)
        if token_val.startswith(("y0_", "y1_")):
            return SOURCE_YM
        if re.match(r"^[A-Za-z0-9._-]+$", token_val) and not token_val.startswith("y"):
            return SOURCE_VK
    return None


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


class CoverFetcher:
    @staticmethod
    async def download_image(url):
        if not url:
            return None
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                    if r.status != REQUEST_OK:
                        return None
                    data = await r.read()
                    return data if len(data) > 1000 else None
        except Exception:
            return None

    @staticmethod
    async def fetch_vk_og_image(audio_id_str):
        url = f"https://vk.com/audio{audio_id_str}"
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(
                    url,
                    headers={
                        "User-Agent": VK_USER_AGENTS["facebook"],
                        "Accept-Language": "ru-RU,ru;q=0.9",
                    },
                    timeout=aiohttp.ClientTimeout(total=10),
                    allow_redirects=True,
                ) as r:
                    if r.status != REQUEST_OK:
                        return ""
                    html = await r.text(errors="replace")
            for pat in [OG_IMAGE_RE, OG_IMAGE_RE2]:
                m = pat.search(html)
                if m:
                    u = m.group(1).replace("&amp;", "&")
                    if u and "userapi.com" in u:
                        return u
        except Exception:
            pass
        return ""

    @staticmethod
    async def fetch_vk_og_image_legacy(owner_id, audio_id):
        url = f"https://vk.com/audio{owner_id}_{audio_id}"
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(
                    url,
                    headers={
                        "User-Agent": VK_USER_AGENTS["facebook"],
                        "Accept-Language": "ru-RU,ru;q=0.9",
                    },
                    timeout=aiohttp.ClientTimeout(total=10),
                    allow_redirects=True,
                ) as r:
                    if r.status != REQUEST_OK:
                        return ""
                    html = await r.text(errors="replace")
            for pat in [OG_IMAGE_RE, OG_IMAGE_RE2]:
                m = pat.search(html)
                if m:
                    u = m.group(1).replace("&amp;", "&")
                    if u and "userapi.com" in u:
                        return u
        except Exception:
            pass
        return ""

    @staticmethod
    async def try_bigger_vk(url):
        if not url:
            return url
        if not re.search(r'size=\d+x\d+', url):
            return url
        try:
            async with aiohttp.ClientSession() as s:
                for sz in [1200, 1000, 800, 600]:
                    candidate = re.sub(r'size=\d+x\d+', f'size={sz}x{sz}', url)
                    if candidate == url:
                        return url
                    try:
                        async with s.head(candidate, timeout=aiohttp.ClientTimeout(total=5)) as r:
                            if r.status == REQUEST_OK:
                                return candidate
                    except Exception:
                        continue
        except Exception:
            pass
        return url

    @staticmethod
    async def download_ym_cover(cover_uri, size="1000x1000"):
        if not cover_uri:
            return None
        url = f"https://{cover_uri.replace('%%', size)}"
        try:
            async with aiohttp.ClientSession() as sess:
                async with sess.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                    if resp.status != REQUEST_OK:
                        return None
                    data = await resp.read()
                    return data if len(data) > 500 else None
        except Exception:
            return None

    @staticmethod
    def ym_cover_url(cover_uri, size="200x200"):
        if not cover_uri:
            return None
        return f"https://{cover_uri.replace('%%', size)}"


class TagHelper:
    @staticmethod
    def write(filepath, title, artist, album=None, cover_data=None):
        if not ID3:
            return False
        try:
            try:
                tags = ID3(filepath)
            except ID3NoHeaderError:
                tags = ID3()
            tags.add(TIT2(encoding=3, text=[title or "Unknown"]))
            tags.add(TPE1(encoding=3, text=[artist or "Unknown"]))
            if album:
                tags.add(TALB(encoding=3, text=[album]))
            if cover_data and len(cover_data) > 500:
                is_png = cover_data[:8] == b'\x89PNG\r\n\x1a\n'
                mime = "image/png" if is_png else "image/jpeg"
                tags.add(APIC(encoding=3, mime=mime, type=3, desc="Cover", data=cover_data))
            tags.save(filepath)
            return True
        except Exception:
            return False


class Converter:
    @staticmethod
    async def to_mp3(inp, out):
        try:
            proc = await asyncio.create_subprocess_exec(
                "ffmpeg", "-hide_banner", "-loglevel", "error",
                "-y", "-i", inp, "-vn", "-acodec", "libmp3lame", "-ab", "320k", out,
                stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE,
            )
            await asyncio.wait_for(proc.communicate(), timeout=60)
            if proc.returncode == 0 and os.path.exists(out) and os.path.getsize(out) > 0:
                return True
            proc2 = await asyncio.create_subprocess_exec(
                "ffmpeg", "-hide_banner", "-loglevel", "error",
                "-y", "-i", inp, "-vn", "-acodec", "copy", out,
                stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE,
            )
            await asyncio.wait_for(proc2.communicate(), timeout=60)
            return proc2.returncode == 0
        except FileNotFoundError:
            try:
                shutil.copy2(inp, out)
                return True
            except Exception:
                return False
        except Exception:
            return False

    @staticmethod
    async def embed_cover(mp3_path, cover_path, out_path):
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
                stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE,
            )
            await asyncio.wait_for(proc.communicate(), timeout=30)
            return proc.returncode == 0 and os.path.exists(out_path) and os.path.getsize(out_path) > 0
        except Exception:
            return False


class VKAPIClient:
    def __init__(self):
        self._token = None
        self._ok = False
        self._session = None
        self._user_id = None

    @property
    def ok(self):
        return self._ok and self._token is not None

    def reset(self):
        self._ok = False
        self._token = None
        self._user_id = None

    async def _get_session(self):
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30))
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    async def _api(self, method, app="kate", **params):
        params["access_token"] = self._token
        params["v"] = VK_API_VERSION
        s = await self._get_session()
        ua = VK_USER_AGENTS.get(app, VK_USER_AGENTS["kate"])
        async with s.post(f"{VK_API_BASE}/{method}", data=params, headers={"User-Agent": ua}) as r:
            if r.status != REQUEST_OK:
                return None
            data = await r.json()
        if "error" in data:
            return None
        return data.get("response")

    async def auth(self, token):
        if not token:
            self.reset()
            return False
        self._token = token
        try:
            r = await self._api("users.get")
            if r and isinstance(r, list) and r:
                self._user_id = r[0].get("id")
                self._ok = True
                return True
        except Exception:
            pass
        self.reset()
        return False

    async def get_audio(self, oid, aid):
        for app in ["kate", "vk_android", "vk_iphone"]:
            try:
                r = await self._api("audio.getById", app=app, audios=f"{oid}_{aid}")
                if r and isinstance(r, list) and r:
                    p = self._parse(r[0])
                    if p and not _is_stub_url(p["url"]) and not _is_stub_title(p["title"]):
                        return p
            except Exception:
                pass
        for code in [
            'return API.audio.getById({{audios:"{a}"}});',
            'var a=API.audio.getById({{audios:"{a}"}});if(a.length>0){{return a[0];}}return null;',
        ]:
            try:
                r = await self._api("execute", app="kate", code=code.format(a=f"{oid}_{aid}"))
                if r:
                    items = r if isinstance(r, list) else [r]
                    if items and isinstance(items[0], dict):
                        p = self._parse(items[0])
                        if p and not _is_stub_url(p["url"]) and not _is_stub_title(p["title"]):
                            return p
            except Exception:
                pass
        return None

    async def search_audio(self, query, count=5):
        for app in ["kate", "vk_android"]:
            try:
                r = await self._api("audio.search", app=app, q=query, count=count, sort=2)
                if r:
                    items = r.get("items", []) if isinstance(r, dict) else []
                    results = []
                    for a in items:
                        p = self._parse(a)
                        if p and not _is_stub_url(p["url"]) and not _is_stub_title(p["title"]):
                            results.append(p)
                    if results:
                        return results[:count]
            except Exception:
                pass
        return []

    async def get_release_audio_id(self, oid, aid):
        try:
            r = await self._api("audio.getById", app="kate", audios=f"{oid}_{aid}")
            if r and isinstance(r, list) and r:
                item = r[0]
                release_id = item.get("release_audio_id")
                if release_id:
                    return release_id
        except Exception:
            pass
        return None

    def _parse(self, a):
        if not a:
            return None
        url = a.get("url", "")
        title = a.get("title", "Unknown") or "Unknown"
        artist = a.get("artist", "Unknown") or "Unknown"
        if not url:
            return None
        thumb = ""
        album = a.get("album", {})
        if album:
            th = album.get("thumb", {})
            if th:
                for k in ["photo_1200", "photo_600", "photo_300", "photo_270"]:
                    if th.get(k):
                        thumb = th[k]
                        break
        release_audio_id = a.get("release_audio_id", "")
        return {
            "id": a.get("id"), "owner_id": a.get("owner_id"), "url": url,
            "artist": artist, "title": title,
            "duration": int(a.get("duration", 0) or 0), "thumbnail": thumb,
            "release_audio_id": release_audio_id,
        }


class YMApiClient:
    def __init__(self):
        self._token = None
        self._client = None
        self._ok = False
        self._uid = None
        self._login = None

    @property
    def ok(self):
        return self._ok and self._token is not None

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
            self._ok = True
            return True
        except Exception:
            self.reset()
            return False

    def reset(self):
        self._token = None
        self._client = None
        self._ok = False
        self._uid = None
        self._login = None

    async def fetch_track(self, track_id):
        if not self._client:
            return None
        try:
            tracks = await self._client.tracks(track_id, with_positions=False)
            return tracks[0] if tracks else None
        except Exception:
            return None

    async def download_track_file(self, track, filepath):
        try:
            await track.download_async(filepath)
            return os.path.exists(filepath) and os.path.getsize(filepath) > 0
        except Exception:
            return False

    async def download_track_bytes(self, track):
        try:
            info = await self._client.tracks_download_info(track.track_id, get_direct_links=True)
            if not info:
                return None
            best = max(info, key=lambda x: x.bitrate_in_kbps or 0)
            data = await best.download_bytes_async()
            return data if data and len(data) > 1000 else None
        except Exception:
            return None

    async def download_cover_file(self, track, filepath):
        try:
            if not track.cover_uri:
                return False
            await track.download_cover_async(filepath, size="1000x1000")
            return os.path.exists(filepath) and os.path.getsize(filepath) > 500
        except Exception:
            return False

    async def search_track(self, query, count=5):
        if not self._client:
            return []
        try:
            result = await self._client.search(query, type_="track")
            if not result or not result.tracks or not result.tracks.results:
                return []
            return result.tracks.results[:count]
        except Exception:
            self._client = None
            self._ok = False
            try:
                await self.auth(self._token)
                result = await self._client.search(query, type_="track")
                if not result or not result.tracks or not result.tracks.results:
                    return []
                return result.tracks.results[:count]
            except Exception:
                return []

    @staticmethod
    def track_artist(track):
        if track.artists:
            return ", ".join(a.name for a in track.artists if a.name) or "Unknown"
        return "Unknown"

    @staticmethod
    def track_title(track):
        return track.title or "Unknown"


class VKDownloader:
    async def dl(self, url, out):
        return await self._m3u8(url, out) if ".m3u8" in url else await self._direct(url, out)

    async def _direct(self, url, out):
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=120)) as s:
                async with s.get(url) as r:
                    if r.status != REQUEST_OK:
                        return False
                    tot = 0
                    with open(out, "wb") as f:
                        async for ch in r.content.iter_chunked(65536):
                            tot += len(ch)
                            if tot > MAX_FILE_SIZE:
                                return False
                            f.write(ch)
            return os.path.exists(out) and os.path.getsize(out) > 0
        except Exception:
            return False

    async def _m3u8(self, url, out):
        if not m3u8_lib:
            return False
        try:
            conn = aiohttp.TCPConnector(limit=MAX_CONCURRENT, ssl=False)
            async with aiohttp.ClientSession(connector=conn, timeout=aiohttp.ClientTimeout(total=120, connect=15)) as s:
                async with s.get(url) as r:
                    if r.status != REQUEST_OK:
                        return False
                    txt = await r.text()
                pl = m3u8_lib.loads(txt)
                if pl.playlists:
                    best = max(pl.playlists, key=lambda p: (p.stream_info.bandwidth if p.stream_info else 0))
                    su = best.uri
                    if not su.startswith("http"):
                        su = f"{url.rsplit('/', 1)[0]}/{su}"
                    async with s.get(su) as r2:
                        if r2.status != REQUEST_OK:
                            return False
                        txt = await r2.text()
                    pl = m3u8_lib.loads(txt)
                    url = su
                segs = pl.segments
                if not segs:
                    return False
                base = url.rsplit("/", 1)[0]
                sem = asyncio.Semaphore(MAX_CONCURRENT)
                chunks = [None] * len(segs)
                keys = {}

                async def gk(ku):
                    if ku in keys:
                        return keys[ku]
                    async with s.get(ku, timeout=aiohttp.ClientTimeout(total=15)) as kr:
                        if kr.status == REQUEST_OK:
                            kd = await kr.read()
                            keys[ku] = kd
                            return kd
                    return None

                async def ds(i, seg):
                    async with sem:
                        uri = seg.uri
                        if not uri.startswith("http"):
                            uri = f"{base}/{uri}"
                        try:
                            async with s.get(uri, timeout=aiohttp.ClientTimeout(total=SEG_TIMEOUT)) as rr:
                                if rr.status != REQUEST_OK:
                                    chunks[i] = b""
                                    return
                                data = await rr.read()
                        except Exception:
                            chunks[i] = b""
                            return
                        if seg.key and seg.key.method == "AES-128" and seg.key.uri:
                            ku = seg.key.uri
                            if not ku.startswith("http"):
                                ku = f"{base}/{ku}"
                            key = await gk(ku)
                            if key:
                                data = self._aes(data, key)
                        chunks[i] = data

                await asyncio.gather(*[ds(i, seg) for i, seg in enumerate(segs)])
                result = b"".join(c for c in chunks if c)
                if not result:
                    return False
                with open(out, "wb") as f:
                    f.write(result)
                return True
        except Exception:
            return False

    @staticmethod
    def _aes(data, key):
        try:
            if len(data) < 16:
                return data
            iv, ct = data[:16], data[16:]
            if not ct:
                return data
            dec = AES.new(key, AES.MODE_CBC, iv=iv).decrypt(ct)
            try:
                dec = unpad(dec, AES.block_size)
            except ValueError:
                pass
            return dec
        except Exception:
            return data

    async def to_mp3(self, inp, out):
        try:
            p = await asyncio.create_subprocess_exec(
                "ffmpeg", "-hide_banner", "-loglevel", "error",
                "-y", "-i", inp, "-vn", "-acodec", "libmp3lame", "-ab", "320k", out,
                stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE,
            )
            await asyncio.wait_for(p.communicate(), timeout=60)
            if p.returncode != 0:
                p2 = await asyncio.create_subprocess_exec(
                    "ffmpeg", "-hide_banner", "-loglevel", "error",
                    "-y", "-i", inp, "-vn", "-acodec", "copy", out,
                    stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE,
                )
                await asyncio.wait_for(p2.communicate(), timeout=60)
                return p2.returncode == 0
            return True
        except FileNotFoundError:
            try:
                shutil.copy2(inp, out)
                return True
            except Exception:
                return False
        except Exception:
            return False


class YTDownloader:
    def __init__(self, tmp):
        self.tmp = tmp

    async def download_audio(self, url, ddir):
        if not yt_dlp_lib:
            return None
        loop = asyncio.get_event_loop()
        info = await loop.run_in_executor(None, self._extract_info, url)
        if not info:
            return None
        title = info.get("title", "Unknown") or "Unknown"
        uploader = info.get("uploader", "Unknown") or "Unknown"
        duration = int(info.get("duration", 0) or 0)
        thumbnail_url = info.get("thumbnail", "")
        safe_name = sanitize_fn(f"{uploader} - {title}")
        out_template = os.path.join(ddir, f"{safe_name}.%(ext)s")
        dl_result = await loop.run_in_executor(None, self._download_audio_sync, url, out_template)
        if not dl_result:
            return None
        audio_ext = (".mp3", ".m4a", ".opus", ".ogg", ".wav", ".webm")
        found_file = None
        for f in os.listdir(ddir):
            if f.endswith(audio_ext) and os.path.isfile(os.path.join(ddir, f)):
                found_file = os.path.join(ddir, f)
                break
        if not found_file:
            return None
        final_mp3 = os.path.join(ddir, f"{safe_name}.mp3")
        if not found_file.endswith(".mp3"):
            conv_ok = await Converter.to_mp3(found_file, final_mp3)
            if conv_ok and os.path.exists(final_mp3) and os.path.getsize(final_mp3) > 0:
                try:
                    os.remove(found_file)
                except Exception:
                    pass
            else:
                final_mp3 = found_file
        else:
            if found_file != final_mp3:
                try:
                    os.rename(found_file, final_mp3)
                except Exception:
                    final_mp3 = found_file
        if not os.path.exists(final_mp3) or os.path.getsize(final_mp3) == 0:
            return None
        if os.path.getsize(final_mp3) > MAX_FILE_SIZE:
            return {"error": "too_big"}
        raw_cover = None
        if thumbnail_url:
            raw_cover = await self._download_thumbnail(thumbnail_url)
        cover_data = normalize_cover(raw_cover) if raw_cover else None
        thumb_data = normalize_cover(raw_cover, max_size=320) if raw_cover else None
        if cover_data and final_mp3.endswith(".mp3"):
            cover_path = os.path.join(ddir, "cover.png")
            with open(cover_path, "wb") as cf:
                cf.write(cover_data)
            covered_mp3 = os.path.join(ddir, f"{safe_name}_cover.mp3")
            if await Converter.embed_cover(final_mp3, cover_path, covered_mp3):
                try:
                    os.remove(final_mp3)
                except Exception:
                    pass
                final_mp3 = covered_mp3
            else:
                TagHelper.write(final_mp3, title, uploader, cover_data=cover_data)
        elif final_mp3.endswith(".mp3"):
            TagHelper.write(final_mp3, title, uploader)
        if not thumb_data and cover_data:
            thumb_data = cover_data
        return {
            "file": final_mp3,
            "track": {
                "title": title, "artist": uploader, "duration": duration,
                "duration_str": fmt_dur(duration), "thumbnail": thumbnail_url, "thumb_data": thumb_data,
            },
        }

    def _extract_info(self, url):
        try:
            opts = {"quiet": True, "no_warnings": True, "socket_timeout": 30}
            with yt_dlp_lib.YoutubeDL(opts) as ydl:
                return ydl.extract_info(url, download=False)
        except Exception:
            return None

    def _download_audio_sync(self, url, out_template):
        try:
            opts = {
                "outtmpl": out_template, "quiet": True, "no_warnings": True,
                "restrictfilenames": True, "format": "bestaudio/best",
                "postprocessors": [{"key": "FFmpegExtractAudio", "preferredcodec": "mp3", "preferredquality": "320"}],
            }
            with yt_dlp_lib.YoutubeDL(opts) as ydl:
                ydl.download([url])
            return True
        except Exception:
            return False

    @staticmethod
    async def _download_thumbnail(url):
        if not url:
            return None
        try:
            async with aiohttp.ClientSession() as sess:
                async with sess.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                    if resp.status != REQUEST_OK:
                        return None
                    data = await resp.read()
                    return data if len(data) >= 1000 else None
        except Exception:
            return None


@loader.tds
class MusicX(loader.Module):
    """VK + Yandex Music + YouTube audio downloader and search"""

    strings = {
        "name": "MusicX",
        "line": "-----------------------",
        "help": (
            "<b>MusicX - Audio Downloader & Search</b>\n"
            "{line}\n\n"
            "<b>Download by link:</b>\n"
            "<code>@{bot} MusicX LINK</code>\n\n"
            "<b>Search by name:</b>\n"
            "<code>@{bot} MusicX song name</code>\n\n"
            "<b>Supported:</b>\n"
            "VK Audio | Yandex Music | YouTube\n\n"
            "<b>Commands:</b>\n"
            "<code>{prefix}musicx auth</code> - get auth links\n"
            "<code>{prefix}musicx token URL</code> - submit token\n"
            "<code>{prefix}musicx status</code> - check auth\n"
            "<code>{prefix}musicx logout vk/ym</code> - log out\n"
            "\n{line}"
        ),
        "auth_links": (
            "<b>MusicX Authorization</b>\n"
            "{line}\n\n"
            "<b>1.</b> Open the link for the service you need\n"
            "<b>2.</b> Log in and grant permissions\n"
            "<b>3.</b> Copy the <b>full URL</b> from the address bar\n"
            "<b>4.</b> <code>{prefix}musicx token URL</code>\n\n"
            '<b>VK:</b> <a href="{vk_url}">Authorize via Kate Mobile</a>\n'
            '<b>YM:</b> <a href="{ym_url}">Authorize via Yandex</a>\n'
            "\n{line}"
        ),
        "token_vk_ok": "<b>VK authorized!</b>\n{line}\nID: <code>{user_id}</code>\n{line}",
        "token_ym_ok": "<b>YM authorized!</b>\n{line}\nUID: <code>{uid}</code> | Login: <code>{login}</code>\n{line}",
        "token_vk_fail": "<b>VK token is invalid!</b>",
        "token_ym_fail": "<b>YM token is invalid!</b>",
        "token_no_url": "<b>Error:</b> Provide a URL with token (argument or reply)",
        "token_unknown": "<b>Error:</b> Cannot detect service from URL.",
        "status": "<b>MusicX Status</b>\n{line}\nVK: {vk_status}\nYM: {ym_status}\n{line}",
        "status_vk_ok": "Authorized | ID <code>{user_id}</code>",
        "status_vk_no": "Not authorized",
        "status_ym_ok": "Authorized | UID <code>{uid}</code> | <code>{login}</code>",
        "status_ym_no": "Not authorized",
        "logout_vk": "<b>VK logged out.</b>",
        "logout_ym": "<b>YM logged out.</b>",
        "logout_usage": "<b>Error:</b> <code>{prefix}musicx logout vk</code> or <code>{prefix}musicx logout ym</code>",
        "logout_unknown": "<b>Error:</b> Unknown service. Use <code>vk</code> or <code>ym</code>",
    }

    strings_ru = {
        "line": "-----------------------",
        "help": (
            "<b>MusicX - Загрузчик и поиск аудио</b>\n"
            "{line}\n\n"
            "<b>Скачать по ссылке:</b>\n"
            "<code>@{bot} MusicX ССЫЛКА</code>\n\n"
            "<b>Поиск по названию:</b>\n"
            "<code>@{bot} MusicX название песни</code>\n\n"
            "<b>Поддерживается:</b>\n"
            "VK Audio | Yandex Music | YouTube\n\n"
            "<b>Команды:</b>\n"
            "<code>{prefix}musicx auth</code> - ссылки для авторизации\n"
            "<code>{prefix}musicx token URL</code> - отправить токен\n"
            "<code>{prefix}musicx status</code> - проверить авторизацию\n"
            "<code>{prefix}musicx logout vk/ym</code> - выйти\n"
            "\n{line}"
        ),
        "auth_links": (
            "<b>Авторизация MusicX</b>\n"
            "{line}\n\n"
            "<b>1.</b> Откройте ссылку нужного сервиса\n"
            "<b>2.</b> Войдите и дайте разрешения\n"
            "<b>3.</b> Скопируйте <b>полный URL</b> из адресной строки\n"
            "<b>4.</b> <code>{prefix}musicx token URL</code>\n\n"
            '<b>VK:</b> <a href="{vk_url}">Авторизация через Kate Mobile</a>\n'
            '<b>YM:</b> <a href="{ym_url}">Авторизация через Yandex</a>\n'
            "\n{line}"
        ),
        "token_vk_ok": "<b>VK авторизован!</b>\n{line}\nID: <code>{user_id}</code>\n{line}",
        "token_ym_ok": "<b>YM авторизован!</b>\n{line}\nUID: <code>{uid}</code> | Логин: <code>{login}</code>\n{line}",
        "token_vk_fail": "<b>VK токен недействителен!</b>",
        "token_ym_fail": "<b>YM токен недействителен!</b>",
        "token_no_url": "<b>Ошибка:</b> Укажите URL с токеном (аргументом или реплаем)",
        "token_unknown": "<b>Ошибка:</b> Не удалось определить сервис по URL.",
        "status": "<b>Статус MusicX</b>\n{line}\nVK: {vk_status}\nYM: {ym_status}\n{line}",
        "status_vk_ok": "Авторизован | ID <code>{user_id}</code>",
        "status_vk_no": "Не авторизован",
        "status_ym_ok": "Авторизован | UID <code>{uid}</code> | <code>{login}</code>",
        "status_ym_no": "Не авторизован",
        "logout_vk": "<b>VK разлогинен.</b>",
        "logout_ym": "<b>YM разлогинен.</b>",
        "logout_usage": "<b>Ошибка:</b> <code>{prefix}musicx logout vk</code> или <code>{prefix}musicx logout ym</code>",
        "logout_unknown": "<b>Ошибка:</b> Неизвестный сервис. Используйте <code>vk</code> или <code>ym</code>",
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue("VK_TOKEN", "", "VK access token", validator=loader.validators.Hidden()),
            loader.ConfigValue("YM_TOKEN", "", "Yandex Music access token", validator=loader.validators.Hidden()),
            loader.ConfigValue("SEARCH_LIMIT", 3, "Results per platform (1-10)", validator=loader.validators.Integer(minimum=1, maximum=10)),
            loader.ConfigValue("SEQUENTIAL_DOWNLOAD", True, "Download one by one", validator=loader.validators.Boolean()),
        )
        self.inline_bot = None
        self.inline_bot_username = None
        self._tmp = None
        self._vk = None
        self._ym = None
        self._vk_dl = None
        self._yt_dl = None
        self._pending_futures = {}
        self._search_futures = {}
        self._search_results = {}
        self._search_tracks_info = {}
        self._upload_lock = None
        self._data_lock = None

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        self._upload_lock = asyncio.Lock()
        self._data_lock = asyncio.Lock()

        me = await client.get_me()
        tg_user_id = me.id
        self._tmp = os.path.join(tempfile.gettempdir(), f"MusicX_{tg_user_id}")

        if os.path.exists(self._tmp):
            shutil.rmtree(self._tmp, ignore_errors=True)
        os.makedirs(self._tmp, exist_ok=True)
        self._vk = VKAPIClient()
        self._ym = YMApiClient()
        self._vk_dl = VKDownloader()
        self._yt_dl = YTDownloader(self._tmp)
        if hasattr(self, "inline") and hasattr(self.inline, "bot"):
            self.inline_bot = self.inline.bot
            try:
                bi = await self.inline_bot.get_me()
                self.inline_bot_username = bi.username
            except Exception:
                pass
        self._cleanup_cache_db()

    async def _ensure_vk(self):
        token = self.config["VK_TOKEN"]
        if not token:
            self._vk.reset()
            return False
        if self._vk.ok and self._vk._token == token:
            return True
        return await self._vk.auth(token)

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
            return 3

    def _get_cache_db(self):
        return self._db.get("MusicX", "inline_cache", {})

    def _save_cache_db(self, cache):
        self._db.set("MusicX", "inline_cache", cache)

    def _cache_get(self, key):
        cache = self._get_cache_db()
        entry = cache.get(key)
        if not entry:
            return None
        if time.time() - entry.get("ts", 0) > CACHE_TTL:
            cache.pop(key, None)
            self._save_cache_db(cache)
            return None
        entry["ts"] = time.time()
        self._save_cache_db(cache)
        return entry.get("data")

    def _cache_set(self, key, data):
        cache = self._get_cache_db()
        cache[key] = {"data": data, "ts": time.time()}
        self._save_cache_db(cache)

    def _cleanup_cache_db(self):
        cache = self._get_cache_db()
        if not cache:
            return
        now = time.time()
        dead = [k for k, v in cache.items() if now - v.get("ts", 0) > CACHE_TTL]
        if dead:
            for k in dead:
                cache.pop(k, None)
            self._save_cache_db(cache)

    def _make_link_cache_key(self, text):
        source = detect_source(text)
        if source == SOURCE_VK:
            owner, aid, _ = parse_vk_link(text)
            return f"vk_{owner}_{aid}" if owner is not None else None
        elif source == SOURCE_YM:
            tid = parse_ym_track_id(text)
            return f"ym_{tid}" if tid else None
        elif source == SOURCE_YT:
            vid = parse_yt_video_id(text)
            return f"yt_{vid}" if vid else None
        return None

    @loader.command(
        ru_doc="Управление MusicX: auth, token, status, logout",
        en_doc="MusicX management: auth, token, status, logout",
    )
    async def musicx(self, message: Message):
        """MusicX management: auth, token, status, logout"""
        args = utils.get_args_raw(message)
        args_list = args.split() if args else []
        if not args_list:
            await self._cmd_help(message)
            return
        cmd = args_list[0].lower()
        if cmd == "auth":
            await self._cmd_auth(message)
        elif cmd == "token":
            await self._cmd_token(message, args)
        elif cmd == "status":
            await self._cmd_status(message)
        elif cmd == "logout":
            await self._cmd_logout(message, args_list)
        else:
            await self._cmd_help(message)

    async def _cmd_help(self, message):
        prefix = self.get_prefix()
        await utils.answer(message, self.strings["help"].format(
            line=self.strings["line"], bot=self.inline_bot_username or "bot",
            prefix=prefix,
        ))

    async def _cmd_auth(self, message):
        prefix = self.get_prefix()
        await utils.answer(message, self.strings["auth_links"].format(
            line=self.strings["line"], vk_url=_build_vk_auth_url(), ym_url=_build_ym_auth_url(),
            prefix=prefix,
        ))

    async def _cmd_token(self, message, raw_args):
        url_text = raw_args[6:].strip() if len(raw_args) > 6 else ""
        if not url_text:
            reply = await message.get_reply_message()
            if reply and reply.text:
                url_text = reply.text.strip()
        if not url_text:
            await utils.answer(message, self.strings["token_no_url"])
            return
        source = _detect_token_source(url_text)
        if source == SOURCE_VK:
            token = extract_vk_token(url_text)
            if not token:
                await utils.answer(message, self.strings["token_vk_fail"])
                return
            try:
                await message.delete()
            except Exception:
                pass
            ok = await self._vk.auth(token)
            if ok:
                self.config["VK_TOKEN"] = token
                await self._client.send_message(message.chat_id, self.strings["token_vk_ok"].format(
                    line=self.strings["line"], user_id=self._vk._user_id or "?",
                ), parse_mode="html")
            else:
                await self._client.send_message(message.chat_id, self.strings["token_vk_fail"], parse_mode="html")
        elif source == SOURCE_YM:
            token = extract_ym_token(url_text)
            if not token:
                await utils.answer(message, self.strings["token_ym_fail"])
                return
            try:
                await message.delete()
            except Exception:
                pass
            ok = await self._ym.auth(token)
            if ok:
                self.config["YM_TOKEN"] = token
                await self._client.send_message(message.chat_id, self.strings["token_ym_ok"].format(
                    line=self.strings["line"], uid=self._ym._uid or "?", login=self._ym._login or "?",
                ), parse_mode="html")
            else:
                await self._client.send_message(message.chat_id, self.strings["token_ym_fail"], parse_mode="html")
        else:
            await utils.answer(message, self.strings["token_unknown"])

    async def _cmd_status(self, message):
        vk_alive = await self._ensure_vk()
        ym_alive = await self._ensure_ym()
        vk_str = self.strings["status_vk_ok"].format(user_id=self._vk._user_id or "?") if vk_alive else self.strings["status_vk_no"]
        ym_str = self.strings["status_ym_ok"].format(uid=self._ym._uid or "?", login=self._ym._login or "?") if ym_alive else self.strings["status_ym_no"]
        await utils.answer(message, self.strings["status"].format(
            line=self.strings["line"], vk_status=vk_str, ym_status=ym_str,
        ))

    async def _cmd_logout(self, message, args_list):
        if len(args_list) < 2:
            prefix = self.get_prefix()
            await utils.answer(message, self.strings["logout_usage"].format(prefix=prefix))
            return
        service = args_list[1].lower()
        if service == "vk":
            self.config["VK_TOKEN"] = ""
            await self._vk.close()
            self._vk = VKAPIClient()
            await utils.answer(message, self.strings["logout_vk"])
        elif service == "ym":
            self.config["YM_TOKEN"] = ""
            self._ym.reset()
            self._ym = YMApiClient()
            await utils.answer(message, self.strings["logout_ym"])
        else:
            await utils.answer(message, self.strings["logout_unknown"])

    async def _upload_to_tg(self, file_bytes, filename, title, artist, dur_s, thumb_data, user_id):
        async with self._upload_lock:
            audio_inp = BufferedInputFile(file_bytes, filename=filename)
            thumb_inp = None
            if thumb_data:
                is_jpeg = thumb_data[:3] == b'\xff\xd8\xff'
                thumb_ext = "cover.jpg" if is_jpeg else "cover.png"
                thumb_inp = BufferedInputFile(thumb_data, filename=thumb_ext)
            try:
                sent = await self.inline_bot.send_audio(
                    chat_id=user_id, audio=audio_inp, title=title,
                    performer=artist, duration=dur_s, thumbnail=thumb_inp,
                )
            except Exception:
                return None
            if sent and sent.audio:
                file_id = sent.audio.file_id
                msg_id = sent.message_id
                await asyncio.sleep(0.5)
                for attempt in range(5):
                    try:
                        await self.inline_bot.delete_message(chat_id=user_id, message_id=msg_id)
                        break
                    except Exception:
                        await asyncio.sleep(1.0 * (attempt + 1))
                await asyncio.sleep(0.3)
                return file_id
            return None

    async def _resolve_vk_cover_by_release_id(self, track_info):
        release_id = track_info.get("release_audio_id", "")
        owner_id = track_info.get("owner_id")
        audio_id = track_info.get("id")

        if release_id:
            og_url = await CoverFetcher.fetch_vk_og_image(release_id)
            if og_url:
                og_url = await CoverFetcher.try_bigger_vk(og_url)
                raw = await CoverFetcher.download_image(og_url)
                if raw:
                    return og_url, raw

        if not release_id and owner_id is not None and audio_id is not None:
            release_id = await self._vk.get_release_audio_id(owner_id, audio_id)
            if release_id:
                og_url = await CoverFetcher.fetch_vk_og_image(release_id)
                if og_url:
                    og_url = await CoverFetcher.try_bigger_vk(og_url)
                    raw = await CoverFetcher.download_image(og_url)
                    if raw:
                        return og_url, raw

        if owner_id is not None and audio_id is not None:
            og_url = await CoverFetcher.fetch_vk_og_image_legacy(owner_id, audio_id)
            if og_url:
                og_url = await CoverFetcher.try_bigger_vk(og_url)
                raw = await CoverFetcher.download_image(og_url)
                if raw:
                    return og_url, raw

        return "", None

    async def _vk_link_dl(self, owner, aid):
        ddir = tempfile.mkdtemp(dir=self._tmp)
        try:
            if not await self._ensure_vk():
                return {"error": "vk_not_auth", "dir": ddir}
            info = await self._vk.get_audio(owner, aid)
            if not info:
                return {"error": "vk_stub", "dir": ddir}
            url = info.get("url", "")
            if not url or _is_stub_url(url) or _is_stub_title(info.get("title", "")):
                return {"error": "vk_stub", "dir": ddir}
            artist = info.get("artist", "Unknown") or "Unknown"
            title = info.get("title", "Unknown") or "Unknown"
            dur = int(info.get("duration", 0) or 0)
            thumb_url = info.get("thumbnail", "")

            raw_cover = None
            if thumb_url:
                thumb_url = await CoverFetcher.try_bigger_vk(thumb_url)
                raw_cover = await CoverFetcher.download_image(thumb_url)

            if not raw_cover:
                _, raw_cover = await self._resolve_vk_cover_by_release_id(info)

            cover_data = normalize_cover(raw_cover, force_jpeg=True) if raw_cover else None
            thumb_data = normalize_cover(raw_cover, max_size=320, force_jpeg=True) if raw_cover else None

            ext = "ts" if ".m3u8" in url else "mp3"
            raw = os.path.join(ddir, f"raw.{ext}")
            if not await self._vk_dl.dl(url, raw):
                return {"error": "dl_fail", "dir": ddir}
            if os.path.getsize(raw) == 0:
                return {"error": "empty", "dir": ddir}
            if os.path.getsize(raw) > MAX_FILE_SIZE:
                return {"error": "too_big", "dir": ddir}
            name = sanitize_fn(f"{artist} - {title}")
            mp3 = os.path.join(ddir, f"{name}.mp3")
            if ext != "mp3":
                if await self._vk_dl.to_mp3(raw, mp3) and os.path.exists(mp3):
                    try:
                        os.remove(raw)
                    except Exception:
                        pass
                else:
                    mp3 = raw
            else:
                try:
                    os.rename(raw, mp3)
                except Exception:
                    mp3 = raw
            if os.path.getsize(mp3) > MAX_FILE_SIZE:
                return {"error": "too_big", "dir": ddir}
            if cover_data and mp3.endswith(".mp3"):
                cover_path = os.path.join(ddir, "cover.jpg")
                with open(cover_path, "wb") as cf:
                    cf.write(cover_data)
                covered_mp3 = os.path.join(ddir, f"{name}_cover.mp3")
                if await Converter.embed_cover(mp3, cover_path, covered_mp3):
                    try:
                        os.remove(mp3)
                    except Exception:
                        pass
                    mp3 = covered_mp3
                else:
                    TagHelper.write(mp3, title, artist, cover_data=cover_data)
            elif mp3.endswith(".mp3"):
                TagHelper.write(mp3, title, artist)
            return {"file": mp3, "dir": ddir, "track": {"title": title, "artist": artist, "duration": dur, "thumb_data": thumb_data}}
        except Exception as e:
            return {"error": str(e), "dir": ddir}

    async def _ym_link_dl(self, track_id_str):
        ddir = tempfile.mkdtemp(dir=self._tmp)
        try:
            if not await self._ensure_ym():
                return {"error": "ym_not_auth", "dir": ddir}
            track = await self._ym.fetch_track(track_id_str)
            if not track:
                return {"error": "no_track", "dir": ddir}
            artist = YMApiClient.track_artist(track)
            title = YMApiClient.track_title(track)
            album_title = track.albums[0].title if track.albums else ""
            dur = int((track.duration_ms or 0) / 1000)
            raw_cover = await CoverFetcher.download_ym_cover(track.cover_uri) if track.cover_uri else None
            cover_data = normalize_cover(raw_cover) if raw_cover else None
            thumb_data = normalize_cover(raw_cover, max_size=320) if raw_cover else None
            raw_path = os.path.join(ddir, "raw_track")
            dl_ok = await self._ym.download_track_file(track, raw_path)
            if not dl_ok:
                return {"error": "dl_fail", "dir": ddir}
            if os.path.getsize(raw_path) == 0:
                return {"error": "empty", "dir": ddir}
            if os.path.getsize(raw_path) > MAX_FILE_SIZE:
                return {"error": "too_big", "dir": ddir}
            clean_name = sanitize_fn(f"{artist} - {title}")
            final_mp3 = os.path.join(ddir, f"{clean_name}.mp3")
            try:
                with open(raw_path, "rb") as rf:
                    header = rf.read(4)
                is_mp3 = header[:3] == b"ID3" or header[:2] in (b"\xff\xfb", b"\xff\xf3")
            except Exception:
                is_mp3 = True
            if not is_mp3:
                conv_ok = await Converter.to_mp3(raw_path, final_mp3)
                if conv_ok and os.path.exists(final_mp3) and os.path.getsize(final_mp3) > 0:
                    try:
                        os.remove(raw_path)
                    except Exception:
                        pass
                else:
                    final_mp3 = raw_path
            else:
                try:
                    os.rename(raw_path, final_mp3)
                except Exception:
                    final_mp3 = raw_path
            if os.path.getsize(final_mp3) > MAX_FILE_SIZE:
                return {"error": "too_big", "dir": ddir}
            if cover_data and final_mp3.endswith(".mp3"):
                cover_path = os.path.join(ddir, "cover.png")
                with open(cover_path, "wb") as cf:
                    cf.write(cover_data)
                covered_mp3 = os.path.join(ddir, f"{clean_name}_cover.mp3")
                if await Converter.embed_cover(final_mp3, cover_path, covered_mp3):
                    try:
                        os.remove(final_mp3)
                    except Exception:
                        pass
                    final_mp3 = covered_mp3
                else:
                    TagHelper.write(final_mp3, title, artist, album_title, cover_data)
            elif final_mp3.endswith(".mp3"):
                TagHelper.write(final_mp3, title, artist, album_title)
            return {"file": final_mp3, "dir": ddir, "track": {"title": title, "artist": artist, "duration": dur, "thumb_data": thumb_data}}
        except Exception as e:
            return {"error": str(e), "dir": ddir}

    async def _yt_link_dl(self, url):
        ddir = tempfile.mkdtemp(dir=self._tmp)
        try:
            if not yt_dlp_lib:
                return {"error": "yt_no_ytdlp", "dir": ddir}
            result = await self._yt_dl.download_audio(url, ddir)
            if result is None:
                return {"error": "dl_fail", "dir": ddir}
            if isinstance(result, dict) and "error" in result:
                return {"error": result["error"], "dir": ddir}
            return {"file": result["file"], "track": result["track"], "dir": ddir}
        except Exception as e:
            return {"error": str(e), "dir": ddir}

    async def _link_dl_and_upload(self, text, user_id, cache_key):
        ddir = None
        try:
            source = detect_source(text)
            if source == SOURCE_VK:
                owner, aid, _ = parse_vk_link(text)
                res = await self._vk_link_dl(owner, aid)
            elif source == SOURCE_YM:
                res = await self._ym_link_dl(parse_ym_track_id(text))
            elif source == SOURCE_YT:
                res = await self._yt_link_dl(parse_yt_url(text))
            else:
                res = {"error": "bad_link"}
            ddir = res.get("dir")
            if res.get("error"):
                err_map = {
                    "too_big": "File > 50 MB", "no_track": "Track not found",
                    "vk_not_auth": "VK not authorized", "ym_not_auth": "YM not authorized",
                    "empty": "Empty audio", "dl_fail": "Download error",
                    "vk_stub": "VK blocks this audio", "bad_link": "Invalid link",
                    "yt_no_ytdlp": "yt-dlp not available",
                }
                result = {"error": err_map.get(res["error"], str(res["error"])[:80])}
                self._cache_set(cache_key, result)
                return result
            fp = res["file"]
            t = res["track"]
            with open(fp, "rb") as f:
                audio_bytes = f.read()
            file_id = await self._upload_to_tg(
                audio_bytes, os.path.basename(fp), t["title"], t["artist"],
                t["duration"], t.get("thumb_data"), user_id,
            )
            if file_id:
                result = {"file_id": file_id, "title": t["title"], "artist": t["artist"], "duration": t["duration"]}
                self._cache_set(cache_key, result)
                return result
            result = {"error": "Telegram upload failed"}
            self._cache_set(cache_key, result)
            return result
        except Exception as e:
            result = {"error": str(e)[:80]}
            self._cache_set(cache_key, result)
            return result
        finally:
            if ddir and os.path.exists(ddir):
                shutil.rmtree(ddir, ignore_errors=True)

    async def _dl_vk_search_track(self, track_info, user_id):
        ddir = tempfile.mkdtemp(dir=self._tmp)
        artist = track_info["artist"]
        title = track_info["title"]
        url = track_info["url"]
        dur = track_info["duration"]
        try:
            cover_url, raw_cover = await self._resolve_vk_cover(track_info)
            cover_data = normalize_cover(raw_cover, force_jpeg=True) if raw_cover else None
            thumb_data = normalize_cover(raw_cover, max_size=320, force_jpeg=True) if raw_cover else None
            ext = "ts" if ".m3u8" in url else "mp3"
            raw = os.path.join(ddir, f"raw.{ext}")
            if not await self._vk_dl.dl(url, raw):
                return {"error": "Download failed"}
            fsize = os.path.getsize(raw)
            if fsize == 0:
                return {"error": "Empty file"}
            if fsize > MAX_FILE_SIZE:
                return {"error": "File > 50 MB"}
            clean_name = sanitize_fn(f"{artist} - {title}")
            final_mp3 = os.path.join(ddir, f"{clean_name}.mp3")
            if ext != "mp3":
                conv_ok = await self._vk_dl.to_mp3(raw, final_mp3)
                if conv_ok and os.path.exists(final_mp3) and os.path.getsize(final_mp3) > 0:
                    try:
                        os.remove(raw)
                    except Exception:
                        pass
                else:
                    final_mp3 = raw
            else:
                try:
                    os.rename(raw, final_mp3)
                except Exception:
                    final_mp3 = raw
            if os.path.getsize(final_mp3) > MAX_FILE_SIZE:
                return {"error": "File > 50 MB"}
            if cover_data and final_mp3.endswith(".mp3"):
                cover_path = os.path.join(ddir, "cover.jpg")
                with open(cover_path, "wb") as cf:
                    cf.write(cover_data)
                covered_mp3 = os.path.join(ddir, f"{clean_name}_cover.mp3")
                if await Converter.embed_cover(final_mp3, cover_path, covered_mp3):
                    try:
                        os.remove(final_mp3)
                    except Exception:
                        pass
                    final_mp3 = covered_mp3
                else:
                    TagHelper.write(final_mp3, title, artist, cover_data=cover_data)
            elif final_mp3.endswith(".mp3"):
                TagHelper.write(final_mp3, title, artist)
            with open(final_mp3, "rb") as f:
                file_bytes = f.read()
            file_id = await self._upload_to_tg(file_bytes, os.path.basename(final_mp3), title, artist, dur, thumb_data, user_id)
            if file_id:
                return {"file_id": file_id, "title": title, "artist": artist, "duration": dur}
            return {"error": "Upload failed"}
        except Exception as e:
            return {"error": str(e)[:80]}
        finally:
            if os.path.exists(ddir):
                shutil.rmtree(ddir, ignore_errors=True)

    async def _dl_ym_search_track(self, track, user_id):
        ddir = tempfile.mkdtemp(dir=self._tmp)
        artist = YMApiClient.track_artist(track)
        title = YMApiClient.track_title(track)
        try:
            album_title = track.albums[0].title if track.albums else ""
            dur_s = (track.duration_ms or 0) // 1000
            audio_data = await self._ym.download_track_bytes(track)
            if not audio_data:
                return {"error": "Download failed"}
            if len(audio_data) > MAX_FILE_SIZE:
                return {"error": "File > 50 MB"}
            raw_cover = await CoverFetcher.download_ym_cover(track.cover_uri) if track.cover_uri else None
            cover_data = normalize_cover(raw_cover) if raw_cover else None
            thumb_data = normalize_cover(raw_cover, max_size=320) if raw_cover else None
            clean_name = sanitize_fn(f"{artist} - {title}")
            raw_path = os.path.join(ddir, f"{clean_name}_raw")
            with open(raw_path, "wb") as f:
                f.write(audio_data)
            try:
                with open(raw_path, "rb") as rf:
                    header = rf.read(4)
                is_mp3 = header[:3] == b"ID3" or header[:2] in (b"\xff\xfb", b"\xff\xf3")
            except Exception:
                is_mp3 = True
            final_mp3 = os.path.join(ddir, f"{clean_name}.mp3")
            if is_mp3:
                os.rename(raw_path, final_mp3)
            else:
                ok = await Converter.to_mp3(raw_path, final_mp3)
                if ok and os.path.exists(final_mp3) and os.path.getsize(final_mp3) > 0:
                    try:
                        os.remove(raw_path)
                    except Exception:
                        pass
                else:
                    final_mp3 = raw_path
            if cover_data and final_mp3.endswith(".mp3"):
                cover_path = os.path.join(ddir, "cover.png")
                with open(cover_path, "wb") as cf:
                    cf.write(cover_data)
                covered_mp3 = os.path.join(ddir, f"{clean_name}_cover.mp3")
                if await Converter.embed_cover(final_mp3, cover_path, covered_mp3):
                    try:
                        os.remove(final_mp3)
                    except Exception:
                        pass
                    final_mp3 = covered_mp3
                else:
                    TagHelper.write(final_mp3, title, artist, album_title, cover_data)
            elif final_mp3.endswith(".mp3"):
                TagHelper.write(final_mp3, title, artist, album_title)
            if not os.path.exists(final_mp3) or os.path.getsize(final_mp3) == 0:
                return {"error": "Empty file"}
            with open(final_mp3, "rb") as f:
                file_bytes = f.read()
            file_id = await self._upload_to_tg(file_bytes, os.path.basename(final_mp3), title, artist, dur_s, thumb_data, user_id)
            if file_id:
                return {"file_id": file_id, "title": title, "artist": artist, "duration": dur_s}
            return {"error": "Upload failed"}
        except Exception as e:
            return {"error": str(e)[:80]}
        finally:
            if os.path.exists(ddir):
                shutil.rmtree(ddir, ignore_errors=True)

    async def _resolve_vk_preview_url(self, track_info):
        thumb_url = track_info.get("thumbnail", "")
        if thumb_url:
            return thumb_url
        release_id = track_info.get("release_audio_id", "")
        owner_id = track_info.get("owner_id")
        audio_id = track_info.get("id")
        if release_id:
            og_url = await CoverFetcher.fetch_vk_og_image(release_id)
            if og_url:
                return og_url
        if not release_id and owner_id is not None and audio_id is not None:
            release_id = await self._vk.get_release_audio_id(owner_id, audio_id)
            if release_id:
                og_url = await CoverFetcher.fetch_vk_og_image(release_id)
                if og_url:
                    return og_url
        if owner_id is not None and audio_id is not None:
            og_url = await CoverFetcher.fetch_vk_og_image_legacy(owner_id, audio_id)
            if og_url:
                return og_url
        return None

    async def _resolve_vk_cover(self, track_info):
        thumb_url = track_info.get("thumbnail", "")
        owner_id = track_info.get("owner_id")
        audio_id = track_info.get("id")

        if thumb_url:
            thumb_url_big = await CoverFetcher.try_bigger_vk(thumb_url)
            raw = await CoverFetcher.download_image(thumb_url_big)
            if raw:
                return thumb_url_big, raw

        cover_url, raw_cover = await self._resolve_vk_cover_by_release_id(track_info)
        if raw_cover:
            return cover_url, raw_cover

        return "", None

    async def _resolve_all_vk_previews(self, tracks):
        tasks = [self._resolve_vk_preview_url(t) for t in tracks]
        urls = await asyncio.gather(*tasks, return_exceptions=True)
        return [(u if not isinstance(u, Exception) else None) or None for u in urls]

    async def _start_search_downloads(self, cache_key, ym_tracks, vk_tracks, user_id):
        async with self._data_lock:
            if cache_key in self._search_futures:
                return

        vk_preview_urls = await self._resolve_all_vk_previews(vk_tracks) if vk_tracks else []

        ym_infos = []
        for t in ym_tracks:
            ym_infos.append({
                "tid": f"ym_{t.track_id}", "source": SOURCE_YM,
                "artist": YMApiClient.track_artist(t), "title": YMApiClient.track_title(t),
                "duration": (t.duration_ms or 0) // 1000,
                "thumb_preview": CoverFetcher.ym_cover_url(t.cover_uri),
                "track_obj": t,
            })

        vk_infos = []
        for i, t in enumerate(vk_tracks):
            vk_infos.append({
                "tid": f"vk_{t['owner_id']}_{t['id']}", "source": SOURCE_VK,
                "artist": t["artist"], "title": t["title"], "duration": t["duration"],
                "thumb_preview": vk_preview_urls[i] if i < len(vk_preview_urls) else None,
                "track_obj": t,
            })

        interleaved = []
        max_len = max(len(ym_infos), len(vk_infos))
        for i in range(max_len):
            if i < len(ym_infos):
                interleaved.append(ym_infos[i])
            if i < len(vk_infos):
                interleaved.append(vk_infos[i])

        async with self._data_lock:
            if cache_key in self._search_futures:
                return
            self._search_tracks_info[cache_key] = interleaved

        if self.config["SEQUENTIAL_DOWNLOAD"]:
            asyncio.ensure_future(self._search_dl_sequential(cache_key, interleaved, user_id))
        else:
            await self._search_dl_parallel(cache_key, interleaved, user_id)

    async def _search_dl_parallel(self, cache_key, interleaved, user_id):
        futures = {}
        for info in interleaved:
            tid = info["tid"]
            if info["source"] == SOURCE_YM:
                futures[tid] = asyncio.ensure_future(self._dl_ym_search_track(info["track_obj"], user_id))
            else:
                futures[tid] = asyncio.ensure_future(self._dl_vk_search_track(info["track_obj"], user_id))
        async with self._data_lock:
            self._search_futures[cache_key] = futures
            self._search_results[cache_key] = {}

    async def _search_dl_sequential(self, cache_key, interleaved, user_id):
        placeholder_futures = {}
        for info in interleaved:
            fut = asyncio.get_event_loop().create_future()
            placeholder_futures[info["tid"]] = fut
        async with self._data_lock:
            self._search_futures[cache_key] = placeholder_futures
            self._search_results[cache_key] = {}
        for info in interleaved:
            tid = info["tid"]
            try:
                if info["source"] == SOURCE_YM:
                    result = await self._dl_ym_search_track(info["track_obj"], user_id)
                else:
                    result = await self._dl_vk_search_track(info["track_obj"], user_id)
            except Exception as e:
                result = {"error": str(e)[:80]}
            async with self._data_lock:
                self._search_results[cache_key][tid] = result
                if not placeholder_futures[tid].done():
                    placeholder_futures[tid].set_result(result)

    async def _collect_search_results(self, cache_key):
        async with self._data_lock:
            futures = self._search_futures.get(cache_key, {})
            results = self._search_results.get(cache_key, {})
            for tid, fut in futures.items():
                if tid not in results and fut.done():
                    try:
                        results[tid] = fut.result()
                    except Exception:
                        results[tid] = {"error": "Internal error"}
            self._search_results[cache_key] = results
            return dict(results)

    async def _get_search_tracks_info(self, cache_key):
        async with self._data_lock:
            return list(self._search_tracks_info.get(cache_key, []))

    async def _has_search_cache(self, cache_key):
        async with self._data_lock:
            return cache_key in self._search_futures

    def _build_search_inline_results(self, tracks_info, results_map):
        inline_results = []
        for info in tracks_info:
            tid = info["tid"]
            artist = info["artist"]
            title = info["title"]
            source = info["source"]
            tag = "[YNDX]" if source == SOURCE_YM else "[VK]"
            thumb_preview = info.get("thumb_preview") or None
            res = results_map.get(tid)
            if res and "file_id" in res:
                inline_results.append(InlineQueryResultCachedAudio(
                    id=f"audio_{tid}_{int(time.time())}", audio_file_id=res["file_id"],
                ))
            elif res and "error" in res:
                kwargs = {
                    "id": f"err_{tid}_{int(time.time())}",
                    "title": f"{tag} {artist} - {title}",
                    "description": f"Error: {res['error']}",
                    "input_message_content": InputTextMessageContent(
                        message_text=f"<b>MusicX:</b> Error: <b>{escape_html(artist)} - {escape_html(title)}</b>: {escape_html(res['error'])}",
                        parse_mode="HTML",
                    ),
                }
                if thumb_preview:
                    kwargs["thumbnail_url"] = thumb_preview
                    kwargs["thumbnail_width"] = 200
                    kwargs["thumbnail_height"] = 200
                inline_results.append(InlineQueryResultArticle(**kwargs))
            else:
                kwargs = {
                    "id": f"dl_{tid}_{int(time.time())}",
                    "title": f"{tag} {artist} - {title}",
                    "description": "Downloading... Repeat the query in ~10 sec",
                    "input_message_content": InputTextMessageContent(
                        message_text=f"<b>MusicX:</b> Downloading <b>{escape_html(artist)} - {escape_html(title)}</b>...",
                        parse_mode="HTML",
                    ),
                }
                if thumb_preview:
                    kwargs["thumbnail_url"] = thumb_preview
                    kwargs["thumbnail_width"] = 200
                    kwargs["thumbnail_height"] = 200
                inline_results.append(InlineQueryResultArticle(**kwargs))
        return inline_results

    @loader.inline_handler(
        ru_doc="VK / Yandex Music / YouTube",
        en_doc="VK / Yandex Music / YouTube"
    )
    async def musicx_inline_handler(self, query: InlineQuery):
        raw = query.query.strip()
        prefix = "musicx"
        if raw.lower().startswith(prefix):
            text = raw[len(prefix):].strip()
        else:
            text = raw.strip()
        if not text:
            await self._inline_hint(query)
            return
        source = detect_source(text)
        if source:
            await self._handle_link_inline(query, text, source)
        else:
            await self._handle_search_inline(query, text)

    async def _handle_link_inline(self, query, text, source):
        cache_key = self._make_link_cache_key(text)
        if not cache_key:
            await self._inline_hint(query)
            return
        if source == SOURCE_VK and not await self._ensure_vk():
            await self._inline_msg(query, "VK not authorized", "Use .musicx auth")
            return
        if source == SOURCE_YM and not await self._ensure_ym():
            await self._inline_msg(query, "YM not authorized", "Use .musicx auth")
            return
        self._cleanup_cache_db()
        cached = self._cache_get(cache_key)
        if cached:
            if "error" in cached:
                await self._inline_msg(query, "Error", cached["error"])
                return
            if "file_id" in cached:
                try:
                    await self.inline_bot.answer_inline_query(
                        inline_query_id=query.id,
                        results=[InlineQueryResultCachedAudio(id=f"{cache_key}_{int(time.time())}", audio_file_id=cached["file_id"])],
                        cache_time=0, is_personal=True,
                    )
                except Exception:
                    pass
                return
        if source == SOURCE_YT:
            await self._handle_yt_link_inline(query, text, cache_key)
        else:
            await self._handle_vk_ym_link_inline(query, text, cache_key)

    async def _handle_vk_ym_link_inline(self, query, text, cache_key):
        if cache_key not in self._pending_futures:
            self._pending_futures[cache_key] = asyncio.ensure_future(
                self._link_dl_and_upload(text, query.from_user.id, cache_key)
            )
        fut = self._pending_futures[cache_key]
        try:
            result = await asyncio.wait_for(asyncio.shield(fut), timeout=25)
        except asyncio.TimeoutError:
            await self._inline_hint(query)
            return
        self._pending_futures.pop(cache_key, None)
        if "error" in result:
            await self._inline_msg(query, "Error", result["error"])
            return
        try:
            await self.inline_bot.answer_inline_query(
                inline_query_id=query.id,
                results=[InlineQueryResultCachedAudio(id=f"{cache_key}_{int(time.time())}", audio_file_id=result["file_id"])],
                cache_time=0, is_personal=True,
            )
        except Exception:
            pass

    async def _handle_yt_link_inline(self, query, text, cache_key):
        if cache_key in self._pending_futures:
            fut = self._pending_futures[cache_key]
            if fut.done():
                self._pending_futures.pop(cache_key, None)
                try:
                    result = fut.result()
                except Exception:
                    result = {"error": "Internal error"}
                if "error" in result:
                    await self._inline_msg(query, "Error", result["error"])
                elif "file_id" in result:
                    try:
                        await self.inline_bot.answer_inline_query(
                            inline_query_id=query.id,
                            results=[InlineQueryResultCachedAudio(id=f"{cache_key}_{int(time.time())}", audio_file_id=result["file_id"])],
                            cache_time=0, is_personal=True,
                        )
                    except Exception:
                        pass
                return
            await self._inline_yt_wait(query, text)
            return
        self._pending_futures[cache_key] = asyncio.ensure_future(
            self._link_dl_and_upload(text, query.from_user.id, cache_key)
        )
        await self._inline_yt_wait(query, text)

    async def _inline_yt_wait(self, query, text):
        vid = parse_yt_video_id(text)
        thumb = f"https://img.youtube.com/vi/{vid}/hqdefault.jpg" if vid else None
        kwargs = {
            "id": f"ytwait_{int(time.time())}",
            "title": "YouTube: downloading track...",
            "description": "Please wait ~15 sec and repeat the query",
            "input_message_content": InputTextMessageContent(
                message_text="<b>MusicX:</b> YouTube track is being downloaded. Please wait and try again.",
                parse_mode="HTML",
            ),
        }
        if thumb:
            kwargs["thumbnail_url"] = thumb
            kwargs["thumbnail_width"] = 320
            kwargs["thumbnail_height"] = 180
        try:
            await self.inline_bot.answer_inline_query(
                inline_query_id=query.id, results=[InlineQueryResultArticle(**kwargs)],
                cache_time=0, is_personal=True,
            )
        except Exception:
            pass

    async def _handle_search_inline(self, query, text):
        has_vk = await self._ensure_vk()
        has_ym = await self._ensure_ym()
        if not has_vk and not has_ym:
            await self._inline_msg(query, "Not authorized", "Use .musicx auth first")
            return
        limit = self._get_limit()
        cache_key = f"search_{text.lower().replace(' ', '_')[:60]}"
        has_cache = await self._has_search_cache(cache_key)
        if has_cache:
            results_map = await self._collect_search_results(cache_key)
            tracks_info = await self._get_search_tracks_info(cache_key)
            inline_results = self._build_search_inline_results(tracks_info, results_map)
            if inline_results:
                try:
                    await self.inline_bot.answer_inline_query(
                        inline_query_id=query.id, results=inline_results,
                        cache_time=0, is_personal=True,
                    )
                except Exception:
                    pass
            else:
                await self._inline_hint(query)
            return
        ym_coro = self._ym.search_track(text, count=limit) if has_ym else asyncio.sleep(0, result=[])
        vk_coro = self._vk.search_audio(text, count=limit) if has_vk else asyncio.sleep(0, result=[])
        ym_raw, vk_raw = await asyncio.gather(ym_coro, vk_coro, return_exceptions=True)
        ym_tracks = ym_raw if isinstance(ym_raw, list) else []
        vk_tracks = vk_raw if isinstance(vk_raw, list) else []
        if not ym_tracks and not vk_tracks:
            await self._inline_msg(query, "Not found", f"No results for: {text}")
            return
        for t in vk_tracks:
            t["tid"] = f"vk_{t['owner_id']}_{t['id']}"
        await self._start_search_downloads(cache_key, ym_tracks, vk_tracks, query.from_user.id)
        tracks_info = await self._get_search_tracks_info(cache_key)
        results_map = await self._collect_search_results(cache_key)
        inline_results = self._build_search_inline_results(tracks_info, results_map)
        try:
            await self.inline_bot.answer_inline_query(
                inline_query_id=query.id, results=inline_results,
                cache_time=0, is_personal=True,
            )
        except Exception:
            pass

    async def _inline_hint(self, query):
        try:
            await self.inline_bot.answer_inline_query(
                inline_query_id=query.id,
                results=[InlineQueryResultArticle(
                    id=f"hint_{int(time.time())}", title="MusicX",
                    description="Paste a link or type a song name to search",
                    input_message_content=InputTextMessageContent(
                        message_text="<b>MusicX:</b> Paste a link (VK/YM/YT) or type a song name",
                        parse_mode="HTML",
                    ),
                    thumbnail_url=INLINE_QUERY_BANNER, thumbnail_width=640, thumbnail_height=360,
                )],
                cache_time=0, is_personal=True,
            )
        except Exception:
            pass

    async def _inline_msg(self, query, title, desc):
        try:
            await self.inline_bot.answer_inline_query(
                inline_query_id=query.id,
                results=[InlineQueryResultArticle(
                    id=f"msg_{int(time.time())}", title=title, description=desc,
                    input_message_content=InputTextMessageContent(
                        message_text=f"<b>MusicX:</b> {escape_html(desc)}", parse_mode="HTML",
                    ),
                )],
                cache_time=0, is_personal=True,
            )
        except Exception:
            pass

    async def on_unload(self):
        for fut in self._pending_futures.values():
            fut.cancel()
        self._pending_futures.clear()
        for futs in self._search_futures.values():
            for fut in futs.values():
                fut.cancel()
        self._search_futures.clear()
        self._search_results.clear()
        self._search_tracks_info.clear()
        if self._vk:
            await self._vk.close()
        if self._tmp and os.path.exists(self._tmp):
            shutil.rmtree(self._tmp, ignore_errors=True)