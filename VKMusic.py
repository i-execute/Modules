__version__ = (2, 0, 0)
# meta developer: I_execute.t.me 
# meta banner: https://raw.githubusercontent.com/i-execute/Modules/main/Storage/VKMusic/MetaBanner.jpeg

import os
import io
import re
import sys
import time
import logging
import tempfile
import shutil
import asyncio
import traceback
import typing

from telethon.tl.types import (
    DocumentAttributeAudio,
    InputDocument,
    InputMediaDocument,
)
from telethon.tl.functions.messages import EditInlineBotMessageRequest

from .. import loader, utils

logger = logging.getLogger(__name__)

DEPS = ["aiohttp", "Pillow", "mutagen", "pycryptodome", "m3u8"]

def _install_deps():
    import importlib
    import subprocess
    
    pip = os.path.join(os.path.dirname(sys.executable), "pip")
    if not os.path.exists(pip):
        pip = "pip"
    
    in_venv = sys.prefix != sys.base_prefix
    
    imp_map = {
        "Pillow": "PIL",
        "pycryptodome": "Crypto",
        "m3u8": "m3u8",
        "aiohttp": "aiohttp",
        "mutagen": "mutagen",
    }
    
    ver_attr = {
        "mutagen": "version.version_string",
    }
    
    lines = [f"venv: {'yes' if in_venv else 'no'} ({sys.prefix})"]
    
    for pkg in DEPS:
        try:
            subprocess.run(
                [pip, "install", "-U", pkg, "--break-system-packages", "-q"],
                capture_output=True,
                text=True,
                timeout=120,
            )
            try:
                imp_name = imp_map.get(pkg, pkg)
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

_dep_log = _install_deps()

try:
    import aiohttp
    from PIL import Image
    AIOHTTP_OK = True
    PIL_OK = True
except ImportError:
    aiohttp = None
    Image = None
    AIOHTTP_OK = False
    PIL_OK = False

try:
    import m3u8 as m3u8_lib
    from Crypto.Cipher import AES
    from Crypto.Util.Padding import unpad
    M3U8_OK = True
except ImportError:
    m3u8_lib = None
    AES = None
    unpad = None
    M3U8_OK = False

try:
    from mutagen.id3 import ID3, TIT2, TPE1, APIC, ID3NoHeaderError
    MUTAGEN_OK = True
except ImportError:
    ID3 = None
    TIT2 = None
    TPE1 = None
    APIC = None
    ID3NoHeaderError = None
    MUTAGEN_OK = False

INLINE_QUERY_BANNER = "https://raw.githubusercontent.com/i-execute/Modules/main/Storage/VKMusic/Inline_query.png"
DOWNLOADING_STUB = "https://raw.githubusercontent.com/i-execute/Modules/main/Storage/VKMusic/Downloading.mp3"

VK_KATE_APP_ID = 2685278
VK_REDIRECT = "https://oauth.vk.com/blank.html"
VK_API_BASE = "https://api.vk.com/method"
VK_API_VERSION = "5.131"

VK_KATE_UA = "KateMobileAndroid/100.1 lite-530 (Android 13; SDK 33; arm64-v8a; Xiaomi; Mi 9T Pro; cepheus; ru; 320)"
VK_FACEBOOK_UA = "facebookexternalhit/1.1"

VK_TOKEN_RE = re.compile(r"access_token=([A-Za-z0-9._-]+)")

VK_AUDIO_LINK_RE = re.compile(
    r"https?://vk\.(?:ru|com)/audio(-?\d+)_(\d+)"
)

OG_IMAGE_SIGNED_RE = re.compile(
    r'<meta\s+(?:property|name)=["\']og:image["\']\s+content=["\']'
    r'(https?://[^\s"\'<>]+userapi\.com[^\s"\'<>]+sign=[^\s"\'<>]+)["\']',
    re.IGNORECASE,
)
OG_IMAGE_SIGNED_RE2 = re.compile(
    r'<meta\s+content=["\']'
    r'(https?://[^\s"\'<>]+userapi\.com[^\s"\'<>]+sign=[^\s"\'<>]+)'
    r'["\']\s+(?:property|name)=["\']og:image["\']',
    re.IGNORECASE,
)
OG_IMAGE_RE = re.compile(
    r'<meta\s+(?:property|name)=["\']og:image["\']\s+content=["\']([^"\']+)["\']',
    re.IGNORECASE,
)
OG_IMAGE_RE2 = re.compile(
    r'<meta\s+content=["\']([^"\']+)["\']\s+(?:property|name)=["\']og:image["\']',
    re.IGNORECASE,
)

REQUEST_OK = 200
MAX_FILE_SIZE = 50 * 1024 * 1024
MAX_CONCURRENT = 15
SEG_TIMEOUT = 30

STUB_URLS = ["audio_api_unavailable.mp3", "audio_api_unavailable"]
STUB_TITLES = [
    "Аудио доступно на vk.com",
    "Audio is available on vk.com",
]

LOG_ENTRIES = []
MAX_LOG = 300


def _log(tag: str, msg: str):
    ts = time.strftime("%H:%M:%S")
    entry = f"[{ts}][VKMusic][{tag}] {msg}"
    LOG_ENTRIES.append(entry)
    if len(LOG_ENTRIES) > MAX_LOG:
        LOG_ENTRIES.pop(0)
    logger.info(entry)


def escape_html(t: str) -> str:
    return (t or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def sanitize_fn(n: str) -> str:
    return re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", n).strip(". ")[:180] or "track"


def _is_stub_url(url: str) -> bool:
    if not url:
        return True
    return any(s in url for s in STUB_URLS)


def _is_stub_title(title: str) -> bool:
    if not title:
        return False
    return any(s.lower() in title.lower() for s in STUB_TITLES)


def _build_vk_auth_url() -> str:
    return (
        f"https://oauth.vk.com/authorize?client_id={VK_KATE_APP_ID}"
        f"&display=page&redirect_uri={VK_REDIRECT}"
        f"&scope=audio,offline&response_type=token&v={VK_API_VERSION}"
    )


def extract_vk_token(text: str) -> typing.Optional[str]:
    if not text:
        return None
    m = VK_TOKEN_RE.search(text)
    return m.group(1) if m else None


def normalize_cover(
    raw_data: bytes,
    max_size: typing.Optional[int] = None,
    force_jpeg: bool = False,
) -> typing.Optional[bytes]:
    if not PIL_OK or not raw_data or len(raw_data) < 100:
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
            async with s.get(
                url,
                timeout=aiohttp.ClientTimeout(total=15),
                allow_redirects=True,
            ) as r:
                if r.status != REQUEST_OK:
                    return None
                data = await r.read()
                return data if len(data) > 500 else None
    except Exception as e:
        _log("DL_IMG", f"Error {url[:80]}: {e}")
        return None


async def _try_bigger_signed(signed_url: str) -> str:
    if not AIOHTTP_OK or not signed_url or "size=" not in signed_url:
        return signed_url

    sizes = [f"{s}x{s}" for s in range(1200, 200, -100)]

    async def _check(session, sz):
        candidate = re.sub(r"size=\d+x\d+", f"size={sz}", signed_url)
        try:
            async with session.head(
                candidate,
                timeout=aiohttp.ClientTimeout(total=6),
                allow_redirects=True,
            ) as r:
                if r.status == REQUEST_OK:
                    return sz, candidate
        except Exception:
            pass
        return sz, None

    async with aiohttp.ClientSession() as s:
        results = await asyncio.gather(
            *[_check(s, sz) for sz in sizes],
            return_exceptions=True,
        )

    for sz, url in results:
        if isinstance(url, str) and url:
            _log("BIGGER", f"size={sz}: {url[:80]}")
            return url

    _log("BIGGER", f"All failed, original: {signed_url[:80]}")
    return signed_url


def _extract_signed_og_image(html: str) -> str:
    for pat in [OG_IMAGE_SIGNED_RE, OG_IMAGE_SIGNED_RE2]:
        m = pat.search(html)
        if m:
            url = m.group(1).replace("&amp;", "&").strip()
            _log("EXTRACT", f"signed og:image: {url[:100]}")
            return url
    for pat in [OG_IMAGE_RE, OG_IMAGE_RE2]:
        m = pat.search(html)
        if m:
            url = m.group(1).replace("&amp;", "&").strip()
            if "userapi.com" in url:
                _log("EXTRACT", f"fallback og:image: {url[:100]}")
                return url
    return ""


async def _get_release_id_via_execute(
    token: str,
    owner_id: int,
    audio_id: int,
) -> str:
    if not AIOHTTP_OK:
        return ""
    
    code = (
        f'var a=API.audio.getById({{audios:"{owner_id}_{audio_id}"}});'
        f'if(a.length>0){{return a[0].release_audio_id;}}return null;'
    )
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(
                f"{VK_API_BASE}/execute",
                data={
                    "access_token": token,
                    "v": VK_API_VERSION,
                    "code": code,
                },
                headers={"User-Agent": VK_KATE_UA},
                timeout=aiohttp.ClientTimeout(total=12),
            ) as r:
                if r.status != REQUEST_OK:
                    return ""
                data = await r.json()
                resp = data.get("response")
                if isinstance(resp, str) and resp:
                    _log("EXECUTE", f"release_id={resp!r}")
                    return resp
    except Exception as e:
        _log("EXECUTE", f"error: {e}")
    return ""


async def _fetch_signed_cover_from_release(release_id: str) -> str:
    if not AIOHTTP_OK:
        return ""
    
    url = f"https://vk.com/audio{release_id}"
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                url,
                headers={
                    "User-Agent": VK_FACEBOOK_UA,
                    "Accept-Language": "ru-RU,ru;q=0.9",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                },
                timeout=aiohttp.ClientTimeout(total=12),
                allow_redirects=True,
            ) as r:
                if r.status != REQUEST_OK:
                    _log("FETCH_REL", f"HTTP {r.status}")
                    return ""
                html = await r.text(errors="replace")
        result = _extract_signed_og_image(html)
        _log("FETCH_REL", f"release={release_id} -> {result[:80] if result else 'NONE'}")
        return result
    except Exception as e:
        _log("FETCH_REL", f"exception: {e}")
        return ""


async def resolve_vk_cover(
    token: str,
    owner_id: int,
    audio_id: int,
) -> tuple[str, str]:
    release_id = await _get_release_id_via_execute(token, owner_id, audio_id)
    if not release_id:
        _log("COVER", "No release_id")
        return "", ""

    signed_url = await _fetch_signed_cover_from_release(release_id)
    if not signed_url:
        _log("COVER", "No signed og:image")
        return "", ""

    stub_url = re.sub(r"size=\d+x\d+", "size=360x360", signed_url)
    
    if AIOHTTP_OK:
        async with aiohttp.ClientSession() as s:
            try:
                async with s.head(
                    stub_url,
                    timeout=aiohttp.ClientTimeout(total=6),
                    allow_redirects=True,
                ) as r:
                    if r.status != REQUEST_OK:
                        stub_url = signed_url
            except Exception:
                stub_url = signed_url

    big_url = await _try_bigger_signed(signed_url)
    _log("COVER", f"stub={stub_url[:80]} big={big_url[:80]}")
    return stub_url, big_url


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
            is_png = cover_data[:8] == b'\x89PNG\r\n\x1a\n'
            mime = "image/png" if is_png else "image/jpeg"
            tags.add(APIC(encoding=3, mime=mime, type=3, desc="Cover", data=cover_data))
        tags.save(filepath)
    except Exception:
        pass


async def _embed_cover_ffmpeg(mp3_path: str, cover_path: str, out_path: str) -> bool:
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
        await asyncio.wait_for(proc.communicate(), timeout=60)
        return (
            proc.returncode == 0
            and os.path.exists(out_path)
            and os.path.getsize(out_path) > 0
        )
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
        if not AIOHTTP_OK:
            return None
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30)
            )
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    async def _api(self, method: str, **params):
        if not AIOHTTP_OK:
            return None
        
        params["access_token"] = self._token
        params["v"] = VK_API_VERSION
        s = await self._get_session()
        if not s:
            return None
        
        async with s.post(
            f"{VK_API_BASE}/{method}",
            data=params,
            headers={"User-Agent": VK_KATE_UA},
        ) as r:
            if r.status != REQUEST_OK:
                return None
            data = await r.json()
        if "error" in data:
            return None
        return data.get("response")

    async def auth(self, token: str) -> bool:
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

    async def search_audio(self, query: str, count: int = 5) -> list:
        try:
            r = await self._api("audio.search", q=query, count=count, sort=2)
            if r:
                items = r.get("items", []) if isinstance(r, dict) else []
                results = []
                for a in items:
                    p = self._parse(a)
                    if (
                        p
                        and not _is_stub_url(p["url"])
                        and not _is_stub_title(p["title"])
                    ):
                        results.append(p)
                if results:
                    return results[:count]
        except Exception:
            pass
        return []

    async def get_audio_by_id(self, owner_id: int, audio_id: int) -> typing.Optional[dict]:
        try:
            r = await self._api("audio.getById", audios=f"{owner_id}_{audio_id}")
            if r and isinstance(r, list) and r:
                return self._parse(r[0])
        except Exception:
            pass
        return None

    def _parse(self, a: dict) -> typing.Optional[dict]:
        if not a:
            return None
        url = a.get("url", "")
        title = a.get("title", "Unknown") or "Unknown"
        artist = a.get("artist", "Unknown") or "Unknown"
        if not url:
            return None
        return {
            "id": a.get("id"),
            "owner_id": a.get("owner_id"),
            "url": url,
            "artist": artist,
            "title": title,
            "duration": int(a.get("duration", 0) or 0),
        }


class VKDownloader:
    async def dl(self, url: str, out: str) -> bool:
        return (
            await self._m3u8(url, out)
            if ".m3u8" in url
            else await self._direct(url, out)
        )

    async def _direct(self, url: str, out: str) -> bool:
        if not AIOHTTP_OK:
            return False
        
        try:
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=120)
            ) as s:
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

    async def _m3u8(self, url: str, out: str) -> bool:
        if not M3U8_OK or not AIOHTTP_OK:
            return False
        
        try:
            conn = aiohttp.TCPConnector(limit=MAX_CONCURRENT, ssl=False)
            async with aiohttp.ClientSession(
                connector=conn,
                timeout=aiohttp.ClientTimeout(total=120, connect=15),
            ) as s:
                async with s.get(url) as r:
                    if r.status != REQUEST_OK:
                        return False
                    txt = await r.text()
                pl = m3u8_lib.loads(txt)
                if pl.playlists:
                    best = max(
                        pl.playlists,
                        key=lambda p: (p.stream_info.bandwidth if p.stream_info else 0),
                    )
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
                            async with s.get(
                                uri,
                                timeout=aiohttp.ClientTimeout(total=SEG_TIMEOUT),
                            ) as rr:
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
    def _aes(data: bytes, key: bytes) -> bytes:
        if not M3U8_OK:
            return data
        
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

    async def to_mp3(self, inp: str, out: str) -> bool:
        try:
            p = await asyncio.create_subprocess_exec(
                "ffmpeg", "-hide_banner", "-loglevel", "error",
                "-y", "-i", inp,
                "-vn", "-acodec", "libmp3lame", "-ab", "320k", out,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
            )
            await asyncio.wait_for(p.communicate(), timeout=60)
            if p.returncode != 0:
                p2 = await asyncio.create_subprocess_exec(
                    "ffmpeg", "-hide_banner", "-loglevel", "error",
                    "-y", "-i", inp, "-vn", "-acodec", "copy", out,
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.PIPE,
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


@loader.tds
class VKMusic(loader.Module):
    """VK Music - search and download audio from VKontakte"""

    strings = {
        "name": "VKMusic",
        "deps_installing": "<b>Installing dependencies...</b>\n\n<code>{}</code>",
        "deps_installed": "<b>Dependencies installed!</b>\n\n<code>{}</code>",
        "auth_instruction": (
            "<b>VKMusic - Authorization</b>\n\n"
            "<blockquote>"
            "1. Open the link below\n"
            "2. Sign in and grant permissions\n"
            "3. Copy the full URL from the address bar\n"
            "4. Paste it: <code>{prefix}vkauth URL</code>"
            "</blockquote>\n\n"
            '<a href="{vk_url}">Authorize via Kate Mobile</a>'
        ),
        "token_ok": (
            "<b>VK authorized!</b>\n\n"
            "<blockquote>ID: <code>{user_id}</code></blockquote>"
        ),
        "token_fail": "<b>Token is invalid!</b>",
        "token_bad_format": "<b>Wrong format!</b> Provide the full URL or token.",
        "not_authorized": "Not authorized",
        "not_authorized_desc": "Use .vkauth to authorize",
        "link_not_found": "Track not found",
        "link_not_found_desc": "Could not get track info by link",
        "link_stub": "Track unavailable",
        "link_stub_desc": "This track is a VK stub",
        "not_found": "Nothing found",
        "not_found_desc": "Try a different query",
        "hint_title": "VKMusic",
        "hint_desc": "Type a track name or paste a VK audio link",
        "downloading": "Downloading...",
    }

    strings_ru = {
        "deps_installing": "<b>Установка зависимостей...</b>\n\n<code>{}</code>",
        "deps_installed": "<b>Зависимости установлены!</b>\n\n<code>{}</code>",
        "auth_instruction": (
            "<b>VKMusic - Авторизация</b>\n\n"
            "<blockquote>"
            "1. Откройте ссылку ниже\n"
            "2. Войдите в аккаунт и дайте разрешения\n"
            "3. Скопируйте полный URL из адресной строки\n"
            "4. Вставьте его: <code>{prefix}vkauth URL</code>"
            "</blockquote>\n\n"
            '<a href="{vk_url}">Авторизация через Kate Mobile</a>'
        ),
        "token_ok": (
            "<b>VK авторизован!</b>\n\n"
            "<blockquote>ID: <code>{user_id}</code></blockquote>"
        ),
        "token_fail": "<b>Токен недействителен!</b>",
        "token_bad_format": "<b>Неверный формат!</b> Укажите полный URL или токен.",
        "not_authorized": "Не авторизован",
        "not_authorized_desc": "Используйте .vkauth",
        "link_not_found": "Трек не найден",
        "link_not_found_desc": "Не удалось получить информацию о треке по ссылке",
        "link_stub": "Трек недоступен",
        "link_stub_desc": "Это заглушка VK",
        "not_found": "Ничего не найдено",
        "not_found_desc": "Попробуйте другой запрос",
        "hint_title": "VKMusic",
        "hint_desc": "Введите название трека или вставьте ссылку VK",
        "downloading": "Загрузка...",
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "VK_TOKEN", "",
                "VK access token",
                validator=loader.validators.Hidden(),
            ),
            loader.ConfigValue(
                "SEARCH_LIMIT", 5,
                "Search results limit (1-10)",
                validator=loader.validators.Integer(minimum=1, maximum=10),
            ),
        )
        self._tmp = None
        self._me_id = None
        self._vk = None
        self._vk_dl = None
        self._upload_lock = None
        self._real_cache = {}
        self._stub_cache = {}
        self._search_cache = {}
        self._cover_cache = {}
        self._link_cache = {}

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        self._upload_lock = asyncio.Lock()
        me = await client.get_me()
        self._me_id = me.id
        self._tmp = os.path.join(tempfile.gettempdir(), f"VKMusic_{me.id}")
        if os.path.exists(self._tmp):
            shutil.rmtree(self._tmp, ignore_errors=True)
        os.makedirs(self._tmp, exist_ok=True)
        self._vk = VKAPIClient()
        self._vk_dl = VKDownloader()
        await self._ensure_vk()

    @loader.command(ru_doc="Показать статус зависимостей")
    async def vkdeps(self, message):
        """Show dependencies status"""
        status = "\n".join(_dep_log)
        await utils.answer(
            message,
            self.strings["deps_installed"].format(status),
        )

    @loader.need_update("chosen_inline_result")
    async def _on_chosen(self, update):
        rid = update.id
        msg_id = update.msg_id
        _log("CHOSEN", f"rid={rid!r} msg_id={msg_id!r}")
        if not rid or not rid.startswith("vk_") or not msg_id:
            return
        track_key = rid[3:]
        if track_key in self._real_cache:
            await self._do_replace(msg_id, self._real_cache[track_key])
            return
        asyncio.ensure_future(self._bg_dl_replace(track_key, msg_id))

    async def _bg_dl_replace(self, track_key: str, msg_id):
        _log("BG", f"Start dl track_key={track_key}")
        try:
            result = await self._dl_and_upload(track_key)
            if "error" in result:
                _log("BG", f"Error: {result['error']}")
                return
            data = (
                result["file_id"],
                result["title"],
                result["artist"],
                result["duration"],
            )
            self._real_cache[track_key] = data
            await self._do_replace(msg_id, data)
        except Exception as e:
            _log("BG", f"Exception: {e}\n{traceback.format_exc()}")

    async def _do_replace(self, msg_id, data: tuple):
        file_id, title, artist, duration = data
        _log("REPLACE", f"msg_id={msg_id!r} file_id={file_id!r}")
        for attempt in range(3):
            try:
                await self.inline.bot.client(
                    EditInlineBotMessageRequest(
                        id=msg_id,
                        media=InputMediaDocument(id=file_id),
                    )
                )
                _log("REPLACE", f"OK attempt {attempt + 1}")
                return
            except Exception as e:
                _log("REPLACE", f"Attempt {attempt + 1} failed: {e}")
                await asyncio.sleep(1)

    async def _ensure_vk(self) -> bool:
        token = self.config["VK_TOKEN"]
        if not token:
            self._vk.reset()
            return False
        if self._vk.ok and self._vk._token == token:
            return True
        return await self._vk.auth(token)

    async def _get_covers_for_track(self, track: dict) -> tuple[str, str]:
        track_key = f"{track['owner_id']}_{track['id']}"
        if track_key in self._cover_cache:
            return self._cover_cache[track_key]
        token = self.config["VK_TOKEN"]
        stub_url, big_url = await resolve_vk_cover(
            token=token,
            owner_id=track["owner_id"],
            audio_id=track["id"],
        )
        self._cover_cache[track_key] = (stub_url, big_url)
        return stub_url, big_url

    async def _get_stub(
        self,
        track_key: str,
        title: str,
        artist: str,
        stub_cover_url: str,
    ) -> typing.Optional[InputDocument]:
        if track_key in self._stub_cache:
            return self._stub_cache[track_key]

        _log("STUB", f"Creating stub for {track_key} ({artist} - {title})")

        stub_bytes = b""
        if AIOHTTP_OK:
            try:
                async with aiohttp.ClientSession() as s:
                    async with s.get(
                        DOWNLOADING_STUB,
                        timeout=aiohttp.ClientTimeout(total=20),
                    ) as r:
                        if r.status == REQUEST_OK:
                            stub_bytes = await r.read()
            except Exception as e:
                _log("STUB", f"Stub audio dl failed: {e}")

        if not stub_bytes:
            return None

        thumb_data = None
        if stub_cover_url:
            raw = await _download_image(stub_cover_url)
            if raw:
                thumb_data = normalize_cover(raw, max_size=320, force_jpeg=True)

        stub_buf = io.BytesIO(stub_bytes)
        stub_buf.name = "Downloading.mp3"
        thumb_buf = None
        if thumb_data:
            thumb_buf = io.BytesIO(thumb_data)
            thumb_buf.name = "cover.jpg"

        try:
            sent = await self.inline.bot.send_audio(
                self._me_id,
                stub_buf,
                title=title,
                performer=artist,
                thumbnail=thumb_buf,
            )
            if sent and sent.media:
                doc = getattr(sent.media, "document", None)
                if doc:
                    fid = InputDocument(
                        id=doc.id,
                        access_hash=doc.access_hash,
                        file_reference=doc.file_reference,
                    )
                    self._stub_cache[track_key] = fid
                    _log("STUB", f"Created: doc_id={doc.id}")
                    try:
                        await self.inline.bot.delete_message(self._me_id, sent.id)
                    except Exception:
                        pass
                    return fid
        except Exception as e:
            _log("STUB", f"send_audio failed: {e}\n{traceback.format_exc()}")
        return None

    async def _dl_and_upload(self, track_key: str) -> dict:
        track_info = None

        for cache_val in self._search_cache.values():
            for t in cache_val:
                if f"{t['owner_id']}_{t['id']}" == track_key:
                    track_info = t
                    break
            if track_info:
                break

        if not track_info and track_key in self._link_cache:
            track_info = self._link_cache[track_key]

        if not track_info:
            return {"error": "Track info not found in cache"}

        ddir = tempfile.mkdtemp(dir=self._tmp)
        try:
            url = track_info.get("url", "")
            artist = track_info.get("artist", "Unknown")
            title = track_info.get("title", "Unknown")
            dur = int(track_info.get("duration", 0) or 0)

            if not url or _is_stub_url(url) or _is_stub_title(title):
                return {"error": "vk_stub"}

            _, big_cover_url = await self._get_covers_for_track(track_info)

            cover_data = None
            thumb_data = None
            if big_cover_url:
                raw_big = await _download_image(big_cover_url)
                if raw_big:
                    cover_data = normalize_cover(raw_big, force_jpeg=True)
                    thumb_data = normalize_cover(raw_big, max_size=320, force_jpeg=True)

            ext = "ts" if ".m3u8" in url else "mp3"
            raw = os.path.join(ddir, f"raw.{ext}")
            if not await self._vk_dl.dl(url, raw):
                return {"error": "Download failed"}
            if os.path.getsize(raw) == 0:
                return {"error": "Empty file"}
            if os.path.getsize(raw) > MAX_FILE_SIZE:
                return {"error": "File > 50 MB"}

            safe_name = sanitize_fn(f"{artist} - {title}")
            mp3 = os.path.join(ddir, f"{safe_name}.mp3")
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
                return {"error": "File > 50 MB"}

            if cover_data and mp3.endswith(".mp3"):
                cover_path = os.path.join(ddir, "cover.jpg")
                with open(cover_path, "wb") as cf:
                    cf.write(cover_data)
                covered_mp3 = os.path.join(ddir, f"{safe_name}_cover.mp3")
                if await _embed_cover_ffmpeg(mp3, cover_path, covered_mp3):
                    try:
                        os.remove(mp3)
                    except Exception:
                        pass
                    mp3 = covered_mp3
                else:
                    _embed_id3(mp3, title, artist, cover_data)
            elif mp3.endswith(".mp3"):
                _embed_id3(mp3, title, artist, None)

            with open(mp3, "rb") as f:
                audio_bytes = f.read()

            file_id = await self._upload_to_tg(
                audio_bytes,
                os.path.basename(mp3),
                title,
                artist,
                dur,
                thumb_data,
            )
            if file_id:
                return {"file_id": file_id, "title": title, "artist": artist, "duration": dur}
            return {"error": "Telegram upload failed"}
        except Exception as e:
            return {"error": str(e)[:120]}
        finally:
            shutil.rmtree(ddir, ignore_errors=True)

    async def _upload_to_tg(
        self,
        file_bytes: bytes,
        filename: str,
        title: str,
        artist: str,
        dur_s: int,
        thumb_data: typing.Optional[bytes],
    ) -> typing.Optional[InputDocument]:
        async with self._upload_lock:
            audio_buf = io.BytesIO(file_bytes)
            audio_buf.name = filename
            thumb_buf = None
            if thumb_data:
                thumb_buf = io.BytesIO(thumb_data)
                thumb_buf.name = "cover.jpg"
            try:
                sent = await self.inline.bot.send_audio(
                    self._me_id,
                    audio_buf,
                    title=title,
                    performer=artist,
                    duration=dur_s,
                    thumbnail=thumb_buf,
                )
            except Exception as e:
                _log("UPLOAD", f"send_audio failed: {e}")
                return None
            if sent and sent.media:
                doc = getattr(sent.media, "document", None)
                if doc:
                    fid = InputDocument(
                        id=doc.id,
                        access_hash=doc.access_hash,
                        file_reference=doc.file_reference,
                    )
                    msg_id = sent.id
                    await asyncio.sleep(0.5)
                    for attempt in range(5):
                        try:
                            await self.inline.bot.delete_message(self._me_id, msg_id)
                            break
                        except Exception:
                            await asyncio.sleep(1.0 * (attempt + 1))
                    return fid
            return None

    @loader.command(ru_doc="Авторизация VK Music")
    async def vkauth(self, message):
        """VK Music authorization"""
        prefix = self.get_prefix()
        args = utils.get_args_raw(message).strip()
        if not args:
            await utils.answer(
                message,
                self.strings["auth_instruction"].format(
                    prefix=prefix,
                    vk_url=_build_vk_auth_url(),
                ),
            )
            return
        token = extract_vk_token(args)
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
        ok = await self._vk.auth(token)
        if ok:
            self.config["VK_TOKEN"] = token
            await self._client.send_message(
                message.chat_id,
                self.strings["token_ok"].format(user_id=self._vk._user_id or "?"),
                parse_mode="html",
            )
        else:
            await self._client.send_message(
                message.chat_id,
                self.strings["token_fail"],
                parse_mode="html",
            )

    @loader.inline_handler(ru_doc="VK Music поиск", en_doc="VK Music search")
    async def vk_inline_handler(self, query):
        """VK Music search"""
        raw = query.query.strip()
        prefix = "vk"
        text = raw[len(prefix):].strip() if raw.lower().startswith(prefix) else raw.strip()

        if not text:
            await self._hint(query)
            return

        _log("INLINE", f"query={text!r} from={query.from_user.id}")

        if not await self._ensure_vk():
            await query.answer(
                [
                    await query.builder.article(
                        title=self.strings["not_authorized"],
                        description=self.strings["not_authorized_desc"],
                        text=f"<b>VKMusic:</b> {self.strings['not_authorized_desc']}",
                        parse_mode="HTML",
                        link_preview=False,
                        id=f"noauth_{int(time.time())}",
                    )
                ],
                cache_time=0,
                private=True,
            )
            return

        link_match = VK_AUDIO_LINK_RE.search(text)
        if link_match:
            await self._handle_link_inline(query, link_match)
        else:
            await self._handle_search_inline(query, text)

    async def _handle_link_inline(self, query, match: re.Match):
        owner_id = int(match.group(1))
        audio_id = int(match.group(2))
        track_key = f"{owner_id}_{audio_id}"

        _log("LINK", f"owner_id={owner_id} audio_id={audio_id}")

        if track_key in self._real_cache:
            stub_fid = self._real_cache[track_key][0]
            try:
                await query.answer(
                    [
                        await query.builder.document(
                            stub_fid,
                            title=self._real_cache[track_key][1],
                            description=self._real_cache[track_key][2],
                            mime_type="audio/mpeg",
                            id=f"vk_{track_key}",
                        )
                    ],
                    cache_time=0,
                    private=True,
                )
            except Exception as e:
                _log("LINK", f"answer failed (cached): {e}")
            return

        track = await self._vk.get_audio_by_id(owner_id, audio_id)

        if not track:
            await query.answer(
                [
                    await query.builder.article(
                        title=self.strings["link_not_found"],
                        description=self.strings["link_not_found_desc"],
                        text=f"<b>VKMusic:</b> {self.strings['link_not_found_desc']}",
                        parse_mode="HTML",
                        link_preview=False,
                        id=f"notfound_{int(time.time())}",
                    )
                ],
                cache_time=0,
                private=True,
            )
            return

        if _is_stub_url(track.get("url", "")) or _is_stub_title(track.get("title", "")):
            await query.answer(
                [
                    await query.builder.article(
                        title=self.strings["link_stub"],
                        description=self.strings["link_stub_desc"],
                        text=f"<b>VKMusic:</b> {self.strings['link_stub_desc']}",
                        parse_mode="HTML",
                        link_preview=False,
                        id=f"stub_{int(time.time())}",
                    )
                ],
                cache_time=0,
                private=True,
            )
            return

        self._link_cache[track_key] = track

        title = track["title"]
        artist = track["artist"]
        cov = await self._get_covers_for_track(track)
        stub_url = cov[0] if cov else ""

        stub_fid = await self._get_stub(track_key, title, artist, stub_url)

        if stub_fid:
            try:
                await query.answer(
                    [
                        await query.builder.document(
                            stub_fid,
                            title=title,
                            description=artist,
                            mime_type="audio/mpeg",
                            id=f"vk_{track_key}",
                            buttons=self.inline.generate_markup(
                                {"text": self.strings["downloading"], "data": f"vkm_{track_key[:32]}"}
                            ),
                        )
                    ],
                    cache_time=0,
                    private=True,
                )
                _log("LINK", f"Answered with stub track_key={track_key}")
                return
            except Exception as e:
                _log("LINK", f"answer with stub failed: {e}")

        thumb = stub_url if stub_url else None
        await query.answer(
            [
                await query.builder.article(
                    title=title,
                    description=artist,
                    text=f"<b>VKMusic:</b> {escape_html(title)}",
                    parse_mode="HTML",
                    link_preview=False,
                    thumb=self.inline._web_document(thumb, width=200, height=200) if thumb else None,
                    id=f"vk_{track_key}",
                )
            ],
            cache_time=0,
            private=True,
        )

    async def _handle_search_inline(self, query, text: str):
        limit = max(1, min(10, int(self.config["SEARCH_LIMIT"])))
        cache_key = text.lower()[:80]

        if cache_key in self._search_cache:
            tracks = self._search_cache[cache_key]
        else:
            tracks = await self._vk.search_audio(text, count=limit)
            if not tracks:
                await query.answer(
                    [
                        await query.builder.article(
                            title=self.strings["not_found"],
                            description=self.strings["not_found_desc"],
                            text=f"<b>VKMusic:</b> {self.strings['not_found_desc']}",
                            parse_mode="HTML",
                            link_preview=False,
                            id=f"notfound_{int(time.time())}",
                        )
                    ],
                    cache_time=0,
                    private=True,
                )
                return
            self._search_cache[cache_key] = tracks

        cover_tasks = [self._get_covers_for_track(t) for t in tracks]
        cover_results = await asyncio.gather(*cover_tasks, return_exceptions=True)

        stub_tasks = []
        for i, t in enumerate(tracks):
            track_key = f"{t['owner_id']}_{t['id']}"
            cov = cover_results[i]
            stub_url = cov[0] if not isinstance(cov, Exception) and cov else ""
            stub_tasks.append(self._get_stub(track_key, t["title"], t["artist"], stub_url))
        stub_results = await asyncio.gather(*stub_tasks, return_exceptions=True)

        inline_results = []
        for i, t in enumerate(tracks):
            vid = f"{t['owner_id']}_{t['id']}"
            title = t["title"]
            artist = t["artist"]

            cov = cover_results[i]
            thumb = cov[0] if not isinstance(cov, Exception) and cov else ""
            stub_fid = stub_results[i] if not isinstance(stub_results[i], Exception) else None

            if vid in self._real_cache:
                inline_results.append(
                    await query.builder.document(
                        self._real_cache[vid][0],
                        title=title,
                        description=artist,
                        mime_type="audio/mpeg",
                        id=f"vk_{vid}",
                    )
                )
            elif stub_fid:
                inline_results.append(
                    await query.builder.document(
                        stub_fid,
                        title=title,
                        description=artist,
                        mime_type="audio/mpeg",
                        id=f"vk_{vid}",
                        buttons=self.inline.generate_markup(
                            {"text": self.strings["downloading"], "data": f"vkm_{vid[:32]}"}
                        ),
                    )
                )
            else:
                inline_results.append(
                    await query.builder.article(
                        title=title,
                        description=artist,
                        text=f"<b>VKMusic:</b> {escape_html(title)}",
                        parse_mode="HTML",
                        link_preview=False,
                        thumb=self.inline._web_document(thumb, width=200, height=200) if thumb else None,
                        id=f"vk_{vid}",
                    )
                )

        try:
            await query.answer(inline_results, cache_time=0, private=True)
            _log("INLINE", f"Answered {len(inline_results)} results OK")
        except Exception as e:
            _log("INLINE", f"answer failed: {e}\n{traceback.format_exc()}")

    async def _hint(self, query):
        try:
            await query.answer(
                [
                    await query.builder.article(
                        title=self.strings["hint_title"],
                        description=self.strings["hint_desc"],
                        text=f"<b>VKMusic:</b> {self.strings['hint_desc']}",
                        parse_mode="HTML",
                        link_preview=False,
                        thumb=self.inline._web_document(INLINE_QUERY_BANNER, width=640, height=360),
                        id=f"hint_{int(time.time())}",
                    )
                ],
                cache_time=0,
                private=True,
            )
        except Exception:
            pass

    async def on_unload(self):
        self._real_cache.clear()
        self._stub_cache.clear()
        self._search_cache.clear()
        self._cover_cache.clear()
        self._link_cache.clear()
        if self._vk:
            await self._vk.close()
        if self._tmp and os.path.exists(self._tmp):
            shutil.rmtree(self._tmp, ignore_errors=True)