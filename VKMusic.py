__version__ = (1, 1, 0)
# meta developer: I_execute.t.me
# requires: aiohttp, mutagen, pycryptodome, m3u8, Pillow

import os
import io
import re
import time
import logging
import tempfile
import shutil
import asyncio
import traceback

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

INLINE_QUERY_BANNER = "https://raw.githubusercontent.com/i-execute/Modules/main/Assets/VKMusic/Inline_query.png"
DOWNLOADING_STUB = "https://raw.githubusercontent.com/i-execute/Modules/main/Assets/VKMusic/Downloading.mp3"

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
CACHE_TTL = 600

STUB_URLS = ["audio_api_unavailable.mp3", "audio_api_unavailable"]
STUB_TITLES = [
    "Аудио доступно на vk.com",
    "Audio is available on vk.com",
]

LOG_ENTRIES = []
MAX_LOG = 300

def _log(tag: str, msg: str):
    ts = time.strftime("%H:%M:%S")
    entry = f"[{ts}] [{tag}] {msg}"
    LOG_ENTRIES.append(entry)
    if len(LOG_ENTRIES) > MAX_LOG:
        LOG_ENTRIES.pop(0)
    logger.info(entry)

import aiohttp
from PIL import Image

try:
    import m3u8 as m3u8_lib
    from Crypto.Cipher import AES
    from Crypto.Util.Padding import unpad
except ImportError:
    m3u8_lib = None

try:
    from mutagen.id3 import ID3, TIT2, TPE1, APIC, ID3NoHeaderError
except ImportError:
    ID3 = None

def escape_html(t):
    return (t or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def sanitize_fn(n):
    return re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", n).strip(". ")[:180] or "track"

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

def extract_vk_token(text):
    if not text:
        return None
    m = VK_TOKEN_RE.search(text)
    return m.group(1) if m else None

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

async def _download_image(url: str) -> bytes | None:
    if not url:
        return None
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                url,
                timeout=aiohttp.ClientTimeout(total=15),
                allow_redirects=True,
            ) as r:
                if r.status != REQUEST_OK:
                    _log("DL_IMG", f"HTTP {r.status} for {url[:80]}")
                    return None
                data = await r.read()
                return data if len(data) > 500 else None
    except Exception as e:
        _log("DL_IMG", f"Error {url[:80]}: {e}")
        return None

async def _try_bigger_signed(signed_url: str) -> str:
    if not signed_url or "size=" not in signed_url:
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
        tasks = [_check(s, sz) for sz in sizes]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    for sz, url in results:
        if isinstance(url, str) and url:
            _log("TRY_BIGGER", f"Best size={sz}: {url[:80]}")
            return url

    _log("TRY_BIGGER", f"All sizes failed, using original: {signed_url[:80]}")
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
                    _log("FETCH_RELEASE", f"HTTP {r.status} for {url}")
                    return ""
                html = await r.text(errors="replace")
        result = _extract_signed_og_image(html)
        _log("FETCH_RELEASE", f"release={release_id} -> {result[:80] if result else 'NONE'}")
        return result
    except Exception as e:
        _log("FETCH_RELEASE", f"exception: {e}")
        return ""


async def resolve_vk_cover(
    token: str,
    owner_id: int,
    audio_id: int,
) -> tuple[str, str]:
    release_id = await _get_release_id_via_execute(token, owner_id, audio_id)
    if not release_id:
        _log("COVER", "No release_id, cannot get cover")
        return "", ""

    signed_url = await _fetch_signed_cover_from_release(release_id)
    if not signed_url:
        _log("COVER", "No signed og:image found")
        return "", ""

    stub_url = re.sub(r"size=\d+x\d+", "size=360x360", signed_url)
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
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30)
            )
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    async def _api(self, method, **params):
        params["access_token"] = self._token
        params["v"] = VK_API_VERSION
        s = await self._get_session()
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

    async def search_audio(self, query, count=5):
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

    async def get_audio_by_id(self, owner_id: int, audio_id: int):
        """Получить один трек по owner_id_audio_id."""
        try:
            r = await self._api(
                "audio.getById",
                audios=f"{owner_id}_{audio_id}",
            )
            if r and isinstance(r, list) and r:
                return self._parse(r[0])
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
        return {
            "id": a.get("id"),
            "owner_id": a.get("owner_id"),
            "url": url,
            "artist": artist,
            "title": title,
            "duration": int(a.get("duration", 0) or 0),
        }


class VKDownloader:
    async def dl(self, url, out):
        return (
            await self._m3u8(url, out)
            if ".m3u8" in url
            else await self._direct(url, out)
        )

    async def _direct(self, url, out):
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

    async def _m3u8(self, url, out):
        if not m3u8_lib:
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
                        key=lambda p: (
                            p.stream_info.bandwidth if p.stream_info else 0
                        ),
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
                    async with s.get(
                        ku, timeout=aiohttp.ClientTimeout(total=15)
                    ) as kr:
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
                        if (
                            seg.key
                            and seg.key.method == "AES-128"
                            and seg.key.uri
                        ):
                            ku = seg.key.uri
                            if not ku.startswith("http"):
                                ku = f"{base}/{ku}"
                            key = await gk(ku)
                            if key:
                                data = self._aes(data, key)
                        chunks[i] = data

                await asyncio.gather(
                    *[ds(i, seg) for i, seg in enumerate(segs)]
                )
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


def _embed_cover_id3(filepath, title, artist, cover_data):
    if not ID3:
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
            tags.add(
                APIC(
                    encoding=3, mime=mime, type=3,
                    desc="Cover", data=cover_data,
                )
            )
        tags.save(filepath)
    except Exception:
        pass


async def _embed_cover_ffmpeg(mp3_path, cover_path, out_path):
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
        return (
            proc.returncode == 0
            and os.path.exists(out_path)
            and os.path.getsize(out_path) > 0
        )
    except Exception:
        return False


@loader.tds
class VKMusic(loader.Module):
    """VK Music — поиск и скачивание аудио из ВКонтакте"""

    strings = {
        "name": "VKMusic",
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
        "not_authorized_inline": "Not authorized",
        "not_authorized_inline_desc": "Use .vkauth to authorize",
        "link_not_found": "Track not found by link",
        "link_stub": "Track unavailable (VK stub)",
    }

    strings_ru = {
        "name": "VKMusic",
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
        "not_authorized_inline": "Не авторизован",
        "not_authorized_inline_desc": "Используйте .vkauth",
        "link_not_found": "Трек по ссылке не найден",
        "link_stub": "Трек недоступен (VK заглушка)",
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
        self.inline_bot = None
        self.inline_bot_username = None
        self._tmp = None
        self._vk = None
        self._vk_dl = None
        self._upload_lock = None
        self._patched = False
        self._real_cache = {}
        self._stub_cache = {}
        self._search_cache = {}
        self._cover_cache = {}

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
        if hasattr(self, "inline") and hasattr(self.inline, "bot"):
            self.inline_bot = self.inline.bot
            try:
                bi = await self.inline_bot.get_me()
                self.inline_bot_username = bi.username
            except Exception:
                pass
        await self._ensure_vk()
        await self._unpatch_feed_update()
        self._patch_feed_update()

    def _patch_feed_update(self):
        if self._patched:
            return
        try:
            dp = self.inline._dp
            if hasattr(dp.feed_update, "_is_patched_vkmusic"):
                self._patched = True
                return
            original_feed = dp.feed_update
            dp._vkmusic_original_feed_update = original_feed
            module_self = self

            async def patched_feed(bot_inst, update: Update, **kw):
                if (
                    hasattr(update, "chosen_inline_result")
                    and update.chosen_inline_result is not None
                ):
                    chosen = update.chosen_inline_result
                    _log(
                        "CHOSEN_RAW",
                        f"result_id={chosen.result_id!r} "
                        f"imid={chosen.inline_message_id!r} "
                        f"user={chosen.from_user.id}",
                    )
                    asyncio.ensure_future(
                        module_self._on_chosen_inline_result(chosen)
                    )
                return await original_feed(bot_inst, update, **kw)

            patched_feed._is_patched_vkmusic = True
            dp.feed_update = patched_feed
            self._patched = True
            _log("PATCH", "feed_update patched OK")
        except Exception as e:
            _log("PATCH", f"Patch failed: {e}\n{traceback.format_exc()}")

    async def _unpatch_feed_update(self):
        if not self._patched:
            return
        try:
            dp = self.inline._dp
            if hasattr(dp, "_vkmusic_original_feed_update"):
                dp.feed_update = dp._vkmusic_original_feed_update
                del dp._vkmusic_original_feed_update
            self._patched = False
            _log("PATCH", "feed_update unpatched OK")
        except Exception as e:
            _log("PATCH", f"Unpatch failed: {e}")

    async def _on_chosen_inline_result(self, chosen: ChosenInlineResult):
        rid = chosen.result_id
        imid = chosen.inline_message_id
        user_id = chosen.from_user.id
        _log("CHOSEN", f"result_id={rid!r} imid={imid!r} user={user_id}")
        if not rid.startswith("vk_"):
            return
        if not imid:
            _log("CHOSEN", "inline_message_id is None")
            return
        track_key = rid[3:]
        if track_key in self._real_cache:
            _log("CHOSEN", "Cache hit, replacing immediately")
            await self._do_replace(imid, self._real_cache[track_key])
            return
        asyncio.ensure_future(
            self._bg_download_and_replace(track_key, user_id, imid)
        )

    async def _bg_download_and_replace(self, track_key, user_id, imid):
        _log("BG", f"Start download track_key={track_key}")
        try:
            result = await self._vk_dl_and_upload(track_key, user_id)
            if "error" in result or "file_id" not in result:
                _log("BG", f"Download failed: {result.get('error')}")
                return
            data = (
                result["file_id"],
                result["title"],
                result["artist"],
                result["duration"],
            )
            self._real_cache[track_key] = data
            _log("BG", f"Done: file_id={data[0]!r}")
            await self._do_replace(imid, data)
        except Exception as e:
            _log("BG", f"Exception: {e}\n{traceback.format_exc()}")

    async def _do_replace(self, imid, data):
        file_id, title, artist, duration = data
        _log("REPLACE", f"imid={imid!r} file_id={file_id!r}")
        for attempt, kwargs in enumerate([
            dict(
                media=InputMediaAudio(
                    media=file_id,
                    title=title,
                    performer=artist,
                    duration=duration,
                ),
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[]),
            ),
            dict(
                media=InputMediaAudio(
                    media=file_id,
                    title=title,
                    performer=artist,
                    duration=duration,
                ),
            ),
            dict(media=InputMediaAudio(media=file_id)),
        ]):
            try:
                await self.inline_bot.edit_message_media(
                    inline_message_id=imid, **kwargs
                )
                _log("REPLACE", f"SUCCESS attempt {attempt + 1}")
                return
            except Exception as e:
                _log("REPLACE", f"Attempt {attempt + 1} failed: {e}")

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

    async def _get_stub_file_id(
        self,
        track_key: str,
        title: str,
        artist: str,
        stub_cover_url: str,
    ) -> str | None:
        if track_key in self._stub_cache:
            return self._stub_cache[track_key]

        _log("STUB", f"Creating stub for {track_key} ({artist} - {title})")

        stub_bytes = b""
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(
                    DOWNLOADING_STUB,
                    timeout=aiohttp.ClientTimeout(total=20),
                ) as r:
                    if r.status == REQUEST_OK:
                        stub_bytes = await r.read()
        except Exception as e:
            _log("STUB", f"Stub audio download failed: {e}")

        if not stub_bytes:
            return None

        thumb_data = None
        if stub_cover_url:
            raw = await _download_image(stub_cover_url)
            if raw:
                thumb_data = normalize_cover(raw, max_size=320, force_jpeg=True)

        try:
            kb = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(
                    text="Downloading...",
                    callback_data=f"vkm_dl_{track_key[:32]}",
                )
            ]])
            sent = await self.inline_bot.send_audio(
                chat_id=self._me_id,
                audio=BufferedInputFile(stub_bytes, filename="Downloading.mp3"),
                title=title,
                performer=artist,
                thumbnail=(
                    BufferedInputFile(thumb_data, filename="cover.jpg")
                    if thumb_data else None
                ),
                reply_markup=kb,
            )
            if sent and sent.audio:
                fid = sent.audio.file_id
                self._stub_cache[track_key] = fid
                _log("STUB", f"Stub created: file_id={fid!r}")
                try:
                    await self.inline_bot.delete_message(
                        chat_id=self._me_id,
                        message_id=sent.message_id,
                    )
                except Exception:
                    pass
                return fid
        except Exception as e:
            _log("STUB", f"send_audio failed: {e}\n{traceback.format_exc()}")
        return None

    async def _vk_dl_and_upload(self, track_key: str, user_id: int) -> dict:
        """Скачать трек VK и загрузить в Telegram. Ищет в _search_cache и _link_cache."""
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
                    thumb_data = normalize_cover(
                        raw_big, max_size=320, force_jpeg=True
                    )

            ext = "ts" if ".m3u8" in url else "mp3"
            raw = os.path.join(ddir, f"raw.{ext}")
            if not await self._vk_dl.dl(url, raw):
                return {"error": "Download failed"}
            if os.path.getsize(raw) == 0:
                return {"error": "Empty file"}
            if os.path.getsize(raw) > MAX_FILE_SIZE:
                return {"error": "File > 50 MB"}

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
                return {"error": "File > 50 MB"}

            if cover_data and mp3.endswith(".mp3"):
                cover_path = os.path.join(ddir, "cover.jpg")
                with open(cover_path, "wb") as cf:
                    cf.write(cover_data)
                covered_mp3 = os.path.join(ddir, f"{name}_cover.mp3")
                if await _embed_cover_ffmpeg(mp3, cover_path, covered_mp3):
                    try:
                        os.remove(mp3)
                    except Exception:
                        pass
                    mp3 = covered_mp3
                else:
                    _embed_cover_id3(mp3, title, artist, cover_data)
            elif mp3.endswith(".mp3"):
                _embed_cover_id3(mp3, title, artist, None)

            with open(mp3, "rb") as f:
                audio_bytes = f.read()

            file_id = await self._upload_audio_to_tg(
                audio_bytes,
                os.path.basename(mp3),
                title,
                artist,
                dur,
                thumb_data,
                user_id,
            )
            if file_id:
                return {
                    "file_id": file_id,
                    "title": title,
                    "artist": artist,
                    "duration": dur,
                }
            return {"error": "Telegram upload failed"}
        except Exception as e:
            return {"error": str(e)[:80]}
        finally:
            if os.path.exists(ddir):
                shutil.rmtree(ddir, ignore_errors=True)

    async def _upload_audio_to_tg(
        self,
        file_bytes: bytes,
        filename: str,
        title: str,
        artist: str,
        dur_s: int,
        thumb_data: bytes | None,
        user_id: int,
    ) -> str | None:
        async with self._upload_lock:
            audio_inp = BufferedInputFile(file_bytes, filename=filename)
            thumb_inp = None
            if thumb_data:
                is_jpeg = thumb_data[:3] == b'\xff\xd8\xff'
                thumb_ext = "cover.jpg" if is_jpeg else "cover.png"
                thumb_inp = BufferedInputFile(thumb_data, filename=thumb_ext)
            try:
                sent = await self.inline_bot.send_audio(
                    chat_id=user_id,
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
                file_id = sent.audio.file_id
                msg_id = sent.message_id
                await asyncio.sleep(0.5)
                for attempt in range(5):
                    try:
                        await self.inline_bot.delete_message(
                            chat_id=user_id, message_id=msg_id
                        )
                        break
                    except Exception:
                        await asyncio.sleep(1.0 * (attempt + 1))
                return file_id
            return None

    async def _ensure_vk(self):
        token = self.config["VK_TOKEN"]
        if not token:
            self._vk.reset()
            return False
        if self._vk.ok and self._vk._token == token:
            return True
        return await self._vk.auth(token)

    def _get_limit(self):
        try:
            return max(1, min(10, int(self.config["SEARCH_LIMIT"])))
        except Exception:
            return 5

    @loader.command(
        ru_doc="Авторизация VK Music",
        en_doc="VK Music authorization",
    )
    async def vkauth(self, message: Message):
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
                self.strings["token_ok"].format(
                    user_id=self._vk._user_id or "?"
                ),
                parse_mode="html",
            )
        else:
            await self._client.send_message(
                message.chat_id,
                self.strings["token_fail"],
                parse_mode="html",
            )

    @loader.inline_handler(
        ru_doc="VK Music - поиск",
        en_doc="VK Music - search",
    )
    async def vk_inline_handler(self, query: InlineQuery):
        """VK Music - search"""
        raw = query.query.strip()
        text = raw[2:].strip() if raw.lower().startswith("vk") else raw.strip()
        _log("INLINE", f"query={text!r} from={query.from_user.id}")

        if not text:
            await self._inline_hint(query)
            return

        if not await self._ensure_vk():
            try:
                await self.inline_bot.answer_inline_query(
                    inline_query_id=query.id,
                    results=[InlineQueryResultArticle(
                        id="noauth",
                        title=self.strings["not_authorized_inline"],
                        description=self.strings["not_authorized_inline_desc"],
                        input_message_content=InputTextMessageContent(
                            message_text=self.strings["not_authorized_inline"],
                        ),
                    )],
                    cache_time=0,
                    is_personal=True,
                )
            except Exception:
                pass
            return
        link_match = VK_AUDIO_LINK_RE.search(text)
        if link_match:
            await self._handle_link_inline(query, link_match)
        else:
            await self._handle_search_inline(query, text)

    async def _handle_link_inline(self, query: InlineQuery, match: re.Match):
        owner_id = int(match.group(1))
        audio_id = int(match.group(2))
        track_key = f"{owner_id}_{audio_id}"

        _log("LINK", f"owner_id={owner_id} audio_id={audio_id}")

        if track_key in self._real_cache:
            fid, title, artist, _ = self._real_cache[track_key]
            try:
                await self.inline_bot.answer_inline_query(
                    inline_query_id=query.id,
                    results=[InlineQueryResultCachedAudio(
                        id=f"vk_{track_key}",
                        audio_file_id=fid,
                    )],
                    cache_time=0,
                    is_personal=True,
                )
            except Exception:
                pass
            return

        track = await self._vk.get_audio_by_id(owner_id, audio_id)
        if not track:
            await self._inline_msg(
                query,
                self.strings["link_not_found"],
                self.strings["link_not_found"],
            )
            return

        if _is_stub_url(track.get("url", "")) or _is_stub_title(track.get("title", "")):
            await self._inline_msg(
                query,
                self.strings["link_stub"],
                self.strings["link_stub"],
            )
            return

        if not hasattr(self, "_link_cache"):
            self._link_cache = {}
        self._link_cache[track_key] = track

        title = track["title"]
        artist = track["artist"]
        cov = await self._get_covers_for_track(track)
        stub_url = cov[0] if cov else ""

        stub_fid = await self._get_stub_file_id(track_key, title, artist, stub_url)

        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="Downloading...",
                callback_data=f"vkm_dl_{track_key[:32]}",
            )
        ]])

        results = []
        if stub_fid:
            results.append(InlineQueryResultCachedAudio(
                id=f"vk_{track_key}",
                audio_file_id=stub_fid,
                reply_markup=kb,
            ))
        else:
            kw = dict(
                id=f"vk_{track_key}",
                title=f"{artist} - {title}",
                description="Tap to download",
                input_message_content=InputTextMessageContent(
                    message_text=(
                        f"<b>VKMusic:</b> Downloading "
                        f"<b>{escape_html(artist)} - {escape_html(title)}</b>..."
                    ),
                    parse_mode="HTML",
                ),
                reply_markup=kb,
            )
            if stub_url:
                kw["thumbnail_url"] = stub_url
                kw["thumbnail_width"] = 200
                kw["thumbnail_height"] = 200
            results.append(InlineQueryResultArticle(**kw))

        try:
            await self.inline_bot.answer_inline_query(
                inline_query_id=query.id,
                results=results,
                cache_time=0,
                is_personal=True,
            )
        except Exception as e:
            _log("LINK_INLINE", f"answer_inline_query failed: {e}")

    async def _handle_search_inline(self, query: InlineQuery, text: str):
        limit = self._get_limit()
        cache_key = f"search_{text.lower().replace(' ', '_')[:60]}"

        if cache_key not in self._search_cache:
            tracks = await self._vk.search_audio(text, count=limit)
            if not tracks:
                await self._inline_msg(
                    query, "Not found", f"No results for: {text}"
                )
                return
            self._search_cache[cache_key] = tracks

        tracks = self._search_cache[cache_key]

        cover_tasks = [
            asyncio.ensure_future(self._get_covers_for_track(t))
            for t in tracks
        ]
        cover_results = await asyncio.gather(*cover_tasks, return_exceptions=True)

        stub_tasks = []
        for i, track in enumerate(tracks):
            track_key = f"{track['owner_id']}_{track['id']}"
            cov = cover_results[i]
            stub_url = (
                cov[0]
                if not isinstance(cov, Exception) and cov
                else ""
            )
            stub_tasks.append(
                asyncio.ensure_future(
                    self._get_stub_file_id(
                        track_key,
                        track["title"],
                        track["artist"],
                        stub_url,
                    )
                )
            )
        stub_results = await asyncio.gather(*stub_tasks, return_exceptions=True)

        results = []
        for i, track in enumerate(tracks):
            track_key = f"{track['owner_id']}_{track['id']}"
            title = track["title"]
            artist = track["artist"]
            cov = cover_results[i]
            stub_url = (
                cov[0]
                if not isinstance(cov, Exception) and cov
                else ""
            )
            stub_fid = (
                stub_results[i]
                if not isinstance(stub_results[i], Exception)
                else None
            )
            _log(
                "INLINE_SEARCH",
                f"Track {i}: key={track_key} stub_fid={stub_fid!r}",
            )
            kb = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(
                    text="Downloading...",
                    callback_data=f"vkm_dl_{track_key[:32]}",
                )
            ]])

            if track_key in self._real_cache:
                fid = self._real_cache[track_key][0]
                results.append(InlineQueryResultCachedAudio(
                    id=f"vk_{track_key}",
                    audio_file_id=fid,
                ))
            elif stub_fid:
                results.append(InlineQueryResultCachedAudio(
                    id=f"vk_{track_key}",
                    audio_file_id=stub_fid,
                    reply_markup=kb,
                ))
            else:
                kw = dict(
                    id=f"vk_{track_key}",
                    title=f"{artist} - {title}",
                    description="Tap to download",
                    input_message_content=InputTextMessageContent(
                        message_text=(
                            f"<b>VKMusic:</b> Downloading "
                            f"<b>{escape_html(artist)} - {escape_html(title)}</b>..."
                        ),
                        parse_mode="HTML",
                    ),
                    reply_markup=kb,
                )
                if stub_url:
                    kw["thumbnail_url"] = stub_url
                    kw["thumbnail_width"] = 200
                    kw["thumbnail_height"] = 200
                results.append(InlineQueryResultArticle(**kw))

        if not results:
            await self._inline_hint(query)
            return

        try:
            await self.inline_bot.answer_inline_query(
                inline_query_id=query.id,
                results=results,
                cache_time=0,
                is_personal=True,
            )
            _log("INLINE_SEARCH", f"Answered {len(results)} results OK")
        except Exception as e:
            _log(
                "INLINE_SEARCH",
                f"answer_inline_query FAILED: {e}\n{traceback.format_exc()}",
            )

    async def _inline_hint(self, query: InlineQuery):
        try:
            await self.inline_bot.answer_inline_query(
                inline_query_id=query.id,
                results=[InlineQueryResultArticle(
                    id="hint",
                    title="VKMusic",
                    description="Type a song name or paste a VK audio link",
                    input_message_content=InputTextMessageContent(
                        message_text=(
                            "<b>VKMusic:</b> Type a song name or paste a VK audio link"
                        ),
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
                    id="msg",
                    title=title,
                    description=desc,
                    input_message_content=InputTextMessageContent(
                        message_text=f"<b>VKMusic:</b> {escape_html(desc)}",
                        parse_mode="HTML",
                    ),
                )],
                cache_time=0,
                is_personal=True,
            )
        except Exception:
            pass

    async def on_unload(self):
        await self._unpatch_feed_update()
        self._real_cache.clear()
        self._stub_cache.clear()
        self._search_cache.clear()
        self._cover_cache.clear()
        if hasattr(self, "_link_cache"):
            self._link_cache.clear()
        if self._vk:
            await self._vk.close()
        if self._tmp and os.path.exists(self._tmp):
            shutil.rmtree(self._tmp, ignore_errors=True)