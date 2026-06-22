__version__ = (2, 0, 1)
# meta developer: I_execute.t.me

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

DEPS = ["curl_cffi", "Pillow", "mutagen", "aiohttp"]


def _install_deps():
    import importlib
    import subprocess
    
    pip = os.path.join(os.path.dirname(sys.executable), "pip")
    if not os.path.exists(pip):
        pip = "pip"
    
    imp_map = {
        "curl_cffi": "curl_cffi",
        "Pillow": "PIL",
        "mutagen": "mutagen",
        "aiohttp": "aiohttp",
    }
    
    lines = []
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
                ver = getattr(mod, "__version__", "?")
                lines.append(f"{pkg}: OK ({ver})")
            except ImportError:
                lines.append(f"{pkg}: FAIL (import error)")
        except Exception as e:
            lines.append(f"{pkg}: FAIL ({e})")
    
    return lines


_dep_log = _install_deps()

try:
    from curl_cffi import requests as cffi_requests
    CFFI_OK = True
except ImportError:
    cffi_requests = None
    CFFI_OK = False

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

SC_SEARCH_URL = "https://api-v2.soundcloud.com/search/tracks"
SC_RESOLVE_URL = "https://api-v2.soundcloud.com/resolve"

MAX_FILE_SIZE = 50 * 1024 * 1024
REQUEST_OK = 200

SC_URL_RE = re.compile(
    r"https?://(?:(?:www\.|m\.)?soundcloud\.com|on\.soundcloud\.com)/[^\s]+",
    re.IGNORECASE,
)

LOG_ENTRIES = []
MAX_LOG = 300


def _log(tag: str, msg: str):
    ts = time.strftime("%H:%M:%S")
    entry = f"[{ts}][SCMusic][{tag}] {msg}"
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
    if not AIOHTTP_OK:
        return ""
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


async def _download_image_aiohttp(url: str) -> typing.Optional[bytes]:
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


@loader.tds
class SCMusic(loader.Module):
    """SoundCloud Music - search and download audio from SoundCloud"""

    strings = {
        "name": "SCMusic",
        "deps_installed": "<b>Dependencies installed!</b>\n\n<code>{}</code>",
        "no_results": "<b>Nothing found</b>",
        "provide_query": "<b>Provide a search query</b>",
        "searching": "<b>Searching</b> <code>{query}</code>",
        "uploading": "<b>Uploading...</b>",
        "download_fail": "<b>Download failed.</b> Try again.",
        "error": "<b>Error:</b> {msg}",
        "downloading_track": "<b>Downloading</b> <code>{title}</code>",
        "btn_download": "⬇️ Download",
        "btn_cancel": "✖️ Close",
        "btn_new_search": "🔍 New search",
        "btn_left": "⬅️",
        "btn_right": "➡️",
        "menu_title": "<b>SoundCloud Music</b>\n<blockquote>Search a track</blockquote>",
        "via_link": "Via link",
        "via_query": "Via query",
        "enter_link": "Enter SoundCloud link:",
        "enter_query": "Enter search query:",
        "link_not_found": "<b>Track not found by link</b>",
        "no_cffi": "<b>curl_cffi not installed</b>",
    }

    strings_ru = {
        "deps_installed": "<b>Зависимости установлены!</b>\n\n<code>{}</code>",
        "no_results": "<b>Ничего не найдено</b>",
        "provide_query": "<b>Укажите поисковый запрос</b>",
        "searching": "<b>Поиск</b> <code>{query}</code>",
        "uploading": "<b>Загрузка...</b>",
        "download_fail": "<b>Ошибка скачивания.</b> Попробуйте снова.",
        "error": "<b>Ошибка:</b> {msg}",
        "downloading_track": "<b>Загружаю</b> <code>{title}</code>",
        "btn_download": "⬇️ Скачать",
        "btn_cancel": "✖️ Закрыть",
        "btn_new_search": "🔍 Новый поиск",
        "btn_left": "⬅️",
        "btn_right": "➡️",
        "menu_title": "<b>SoundCloud Music</b>\n<blockquote>Поиск трека</blockquote>",
        "via_link": "По ссылке",
        "via_query": "По запросу",
        "enter_link": "Введите ссылку на SoundCloud:",
        "enter_query": "Введите поисковый запрос:",
        "link_not_found": "<b>Трек по ссылке не найден</b>",
        "no_cffi": "<b>curl_cffi не установлен</b>",
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "SEARCH_LIMIT", 5,
                "Search results limit (1-10)",
                validator=loader.validators.Integer(minimum=1, maximum=10),
            ),
        )
        self._client_id = None
        self._tmp = None
        self._me_id = None
        self._upload_lock = None
        self._scs_sessions = {}
        self._scs_locks = {}
        self._cover_cache = {}

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        self._upload_lock = asyncio.Lock()
        me = await client.get_me()
        self._me_id = me.id
        self._tmp = os.path.join(tempfile.gettempdir(), f"SCMusic_{me.id}")
        if os.path.exists(self._tmp):
            shutil.rmtree(self._tmp, ignore_errors=True)
        os.makedirs(self._tmp, exist_ok=True)

    async def on_unload(self):
        self._scs_sessions.clear()
        self._scs_locks.clear()
        self._cover_cache.clear()
        if self._tmp and os.path.exists(self._tmp):
            shutil.rmtree(self._tmp, ignore_errors=True)

    def _get_limit(self):
        try:
            return max(1, min(10, int(self.config["SEARCH_LIMIT"])))
        except Exception:
            return 5

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

    async def _get_client_id(self, ses) -> typing.Optional[str]:
        if self._client_id:
            return self._client_id
        try:
            h_resp = await ses.get("https://soundcloud.com")
            if h_resp.status_code != REQUEST_OK:
                return None
            html = h_resp.text
            for scr in reversed(re.findall(r'src="(https://a-v2\.sndcdn\.com/assets/[^"]+\.js)"', html)):
                m = re.search(r'client_id:"([a-zA-Z0-9]{32})"', (await ses.get(scr)).text)
                if m:
                    self._client_id = m.group(1)
                    return self._client_id
        except Exception as e:
            _log("CLIENT_ID", f"Failed: {e}")
        return None

    async def _resolve_url(self, url: str) -> typing.Optional[dict]:
        if not CFFI_OK:
            return None
        
        try:
            async with cffi_requests.AsyncSession(impersonate="chrome120") as ses:
                if "on.soundcloud.com" in url:
                    _log("RESOLVE", f"Short link detected: {url}")
                    r = await ses.get(url, allow_redirects=True)
                    if r.status_code == REQUEST_OK:
                        url = str(r.url)
                        _log("RESOLVE", f"Redirected to: {url}")
                
                c_id = await self._get_client_id(ses)
                if not c_id:
                    return None

                r = await ses.get(SC_RESOLVE_URL, params={"url": url, "client_id": c_id})
                if r.status_code != REQUEST_OK:
                    return None
                item = r.json()
                if item.get("kind") != "track":
                    return None

                dur_s = item.get("duration", 0) // 1000
                art = item.get("artwork_url") or item.get("user", {}).get("avatar_url") or ""
                if art:
                    art = art.replace("-large.jpg", "-t500x500.jpg")
                return {
                    "id": item.get("id"),
                    "permalink_url": item.get("permalink_url", ""),
                    "title": item.get("title", "Unknown"),
                    "username": item.get("user", {}).get("username", "Unknown"),
                    "dur_str": f"{dur_s // 60}:{dur_s % 60:02d}" if dur_s else "?:??",
                    "duration": dur_s,
                    "artwork_url": art,
                    "track_data": item,
                }
        except Exception as e:
            _log("RESOLVE", f"Failed for {url}: {e}")
        return None

    async def _search_sc(self, query: str, limit: int = 5) -> list:
        if not CFFI_OK:
            return []
        try:
            async with cffi_requests.AsyncSession(impersonate="chrome120") as ses:
                c_id = await self._get_client_id(ses)
                if not c_id:
                    return []
                r = await ses.get(
                    SC_SEARCH_URL,
                    params={"q": query, "client_id": c_id, "limit": limit, "offset": 0},
                )
                if r.status_code != REQUEST_OK:
                    return []
                data = r.json()
        except Exception as e:
            _log("SEARCH", f"Failed: {e}")
            return []

        results = []
        for item in data.get("collection", [])[:limit]:
            if item.get("kind") != "track":
                continue
            dur_s = item.get("duration", 0) // 1000
            dur_str = f"{dur_s // 60}:{dur_s % 60:02d}" if dur_s else "?:??"
            art = item.get("artwork_url") or item.get("user", {}).get("avatar_url") or ""
            if art:
                art = art.replace("-large.jpg", "-t500x500.jpg")
            results.append({
                "id": item.get("id"),
                "permalink_url": item.get("permalink_url", ""),
                "title": item.get("title", "Unknown"),
                "username": item.get("user", {}).get("username", "Unknown"),
                "dur_str": dur_str,
                "duration": dur_s,
                "artwork_url": art,
                "track_data": item,
            })
        return results

    async def _prepare_track(self, track_data: dict, ddir: str) -> typing.Tuple[typing.Optional[dict], typing.Optional[str]]:
        if not CFFI_OK:
            return None, "curl_cffi not installed"

        try:
            async with cffi_requests.AsyncSession(impersonate="chrome120") as ses:
                c_id = await self._get_client_id(ses)
                if not c_id:
                    return None, "no client_id"

                title = track_data.get("title", "Unknown")
                artist = track_data.get("user", {}).get("username", "Unknown")
                dur_s = track_data.get("duration", 0) // 1000

                art = track_data.get("artwork_url") or track_data.get("user", {}).get("avatar_url") or ""
                if art:
                    art = art.replace("-large.jpg", "-t500x500.jpg")

                tr = track_data.get("media", {}).get("transcodings", [])
                if not tr:
                    return None, "no transcodings"

                s_info = next(
                    (t for t in tr if t.get("format", {}).get("protocol") == "progressive"),
                    tr[0],
                )
                s_url = s_info.get("url") + f"?client_id={c_id}"
                if track_data.get("track_authorization"):
                    s_url += f"&track_authorization={track_data['track_authorization']}"

                s_resp = await ses.get(s_url)
                if s_resp.status_code != REQUEST_OK or not s_resp.json().get("url"):
                    return None, "stream url failed"

                stream_url = s_resp.json().get("url")
                a_buf = io.BytesIO()

                if s_info.get("format", {}).get("protocol") == "progressive":
                    m_resp = await ses.get(stream_url)
                    if m_resp.status_code != REQUEST_OK:
                        return None, "progressive dl failed"
                    a_buf.write(m_resp.content)
                else:
                    m3_resp = await ses.get(stream_url)
                    if m3_resp.status_code != REQUEST_OK:
                        return None, "m3u8 fetch failed"
                    chk = [l for l in m3_resp.text.splitlines() if l and not l.startswith("#")]
                    if not chk:
                        return None, "empty m3u8"
                    for c_u in chk:
                        c_r = await ses.get(c_u)
                        if c_r.status_code != REQUEST_OK:
                            return None, "chunk dl failed"
                        a_buf.write(c_r.content)

                if a_buf.tell() == 0:
                    return None, "empty audio"
                if a_buf.tell() > MAX_FILE_SIZE:
                    return None, "too_large"

                raw_cover = None
                if art:
                    raw_cover = await _download_image_aiohttp(art)

                cover_data = normalize_cover(raw_cover) if raw_cover else None
                thumb_data = normalize_cover(raw_cover, max_size=320) if raw_cover else None

                safe_name = sanitize_fn(f"{artist} - {title}")
                mp3_path = os.path.join(ddir, f"{safe_name}.mp3")
                with open(mp3_path, "wb") as f:
                    a_buf.seek(0)
                    f.write(a_buf.read())

                if cover_data:
                    cover_path = os.path.join(ddir, "cover.jpg")
                    covered_mp3 = os.path.join(ddir, f"{safe_name}_cover.mp3")
                    with open(cover_path, "wb") as cf:
                        cf.write(cover_data)
                    proc = await asyncio.create_subprocess_exec(
                        "ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
                        "-i", mp3_path, "-i", cover_path,
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
                            os.remove(mp3_path)
                        except Exception:
                            pass
                        mp3_path = covered_mp3
                    else:
                        _embed_id3(mp3_path, title, artist, cover_data)
                else:
                    _embed_id3(mp3_path, title, artist, None)

                return {
                    "path": mp3_path,
                    "title": title,
                    "artist": artist,
                    "dur_s": dur_s,
                    "thumb_data": thumb_data,
                }, None

        except Exception as e:
            return None, str(e)[:120]

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

    @loader.command(ru_doc="Показать статус зависимостей")
    async def scdeps(self, message):
        """Show dependencies status"""
        status = "\n".join(_dep_log)
        await utils.answer(
            message,
            self.strings["deps_installed"].format(status),
        )

    @loader.command(
        ru_doc="Поиск трека на SoundCloud. Без аргументов открывает форму выбора",
        en_doc="Search track on SoundCloud. Without args opens selection form",
    )
    async def scs(self, message: Message):
        """Search SoundCloud track."""
        if not CFFI_OK:
            await utils.answer(message, self.strings["no_cffi"])
            return

        query = utils.get_args_raw(message).strip()
        is_forum = await self._is_forum_chat(message)
        topic_id = self._get_topic_id(message) if is_forum else None

        if not query:
            session_id = str(id(message))
            self._scs_sessions[session_id] = {
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
                        "handler": self._scs_link_input,
                        "args": (session_id,),
                        "style": "primary",
                    },
                    {
                        "text": self.strings["via_query"],
                        "input": self.strings["enter_query"],
                        "handler": self._scs_query_input,
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

        m = SC_URL_RE.search(query)
        if m:
            await self._scs_download_by_link(message, m.group(0), is_forum, topic_id)
            return

        await self._scs_run_search(message, query, is_forum, topic_id)

    async def _scs_link_input(self, call, text: str, session_id: str):
        url = text.strip()
        sess = self._scs_sessions.get(session_id, {})
        m = SC_URL_RE.search(url)
        if not m:
            await call.edit(self.strings["link_not_found"])
            return
        await call.edit(self.strings["uploading"])
        
        track = await self._resolve_url(m.group(0))
        if not track:
            await call.edit(self.strings["link_not_found"])
            return

        chat_id = sess.get("chat_id")
        is_forum = sess.get("is_forum", False)
        topic_id = sess.get("topic_id")
        self._scs_sessions.pop(session_id, None)
        
        ddir = tempfile.mkdtemp(dir=self._tmp)
        try:
            info, err = await self._prepare_track(track["track_data"], ddir)
            if err:
                await call.edit(self.strings["download_fail"])
                return
            await self._send_audio(chat_id, info, reply_to=topic_id if is_forum and topic_id else None)
            await call.delete()
        finally:
            shutil.rmtree(ddir, ignore_errors=True)

    async def _scs_query_input(self, call, text: str, session_id: str):
        query = text.strip()
        sess = self._scs_sessions.get(session_id, {})
        if not query:
            await call.edit(self.strings["no_results"])
            return
        await call.edit(self.strings["searching"].format(query=escape_html(query)))
        limit = self._get_limit()
        tracks = await self._search_sc(query, limit=limit)
        if not tracks:
            await call.edit(self.strings["no_results"])
            return
        
        chat_id = sess.get("chat_id")
        is_forum = sess.get("is_forum", False)
        topic_id = sess.get("topic_id")
        new_sid = session_id + "_s"
        self._scs_sessions[new_sid] = {
            "tracks": tracks,
            "index": 0,
            "chat_id": chat_id,
            "is_forum": is_forum,
            "topic_id": topic_id,
            "cover_cache": {},
        }
        self._scs_locks[new_sid] = asyncio.Lock()
        cover_url = await self._scs_get_cover(new_sid, 0)
        markup = self._scs_markup(new_sid, len(tracks))
        track = tracks[0]
        edit_kwargs = dict(
            text=f"<b>{escape_html(track['title'])}</b>\n<b>{escape_html(track['username'])}</b>\n<blockquote>{track['dur_str']}</blockquote>",
            reply_markup=markup,
        )
        if cover_url:
            edit_kwargs["photo"] = cover_url
        await call.edit(**edit_kwargs)
        asyncio.ensure_future(self._scs_prefetch_covers(new_sid))

    async def _scs_download_by_link(self, message, url: str, is_forum: bool, topic_id):
        msg = await utils.answer(message, self.strings["uploading"])
        track = await self._resolve_url(url)
        if not track:
            await utils.answer(msg, self.strings["link_not_found"])
            return
        
        ddir = tempfile.mkdtemp(dir=self._tmp)
        try:
            info, err = await self._prepare_track(track["track_data"], ddir)
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

    async def _scs_run_search(self, message, query: str, is_forum: bool, topic_id):
        msg = await utils.answer(message, self.strings["searching"].format(query=escape_html(query)))
        limit = self._get_limit()
        tracks = await self._search_sc(query, limit=limit)
        if not tracks:
            await utils.answer(msg, self.strings["no_results"])
            return
        
        session_id = str(id(message))
        self._scs_sessions[session_id] = {
            "tracks": tracks,
            "index": 0,
            "chat_id": message.chat_id,
            "is_forum": is_forum,
            "topic_id": topic_id,
            "cover_cache": {},
        }
        self._scs_locks[session_id] = asyncio.Lock()
        cover_url = await self._scs_get_cover(session_id, 0)
        markup = self._scs_markup(session_id, len(tracks))
        track = tracks[0]
        try:
            await msg.delete()
        except Exception:
            pass
        form_kwargs = dict(
            text=f"<b>{escape_html(track['title'])}</b>\n<b>{escape_html(track['username'])}</b>\n<blockquote>{track['dur_str']}</blockquote>",
            message=message,
            reply_markup=markup,
            silent=True,
        )
        if cover_url:
            form_kwargs["photo"] = cover_url
        if is_forum and topic_id:
            form_kwargs["reply_to"] = topic_id
        await self.inline.form(**form_kwargs)
        asyncio.ensure_future(self._scs_prefetch_covers(session_id))

    async def _scs_get_cover(self, session_id: str, idx: int) -> typing.Optional[str]:
        sess = self._scs_sessions.get(session_id)
        if not sess:
            return None
        cache = sess["cover_cache"]
        if idx in cache:
            return cache[idx]
        track = sess["tracks"][idx]
        art_url = track.get("artwork_url", "")
        if not art_url:
            cache[idx] = None
            return None
        
        track_id = track["id"]
        if track_id in self._cover_cache:
            cache[idx] = self._cover_cache[track_id]
            return cache[idx]
        
        raw = await _download_image_aiohttp(art_url)
        if not raw:
            cache[idx] = None
            return None
        
        norm = normalize_cover(raw, force_jpeg=True) or raw
        x0_url = await _upload_to_x0(
            norm,
            sanitize_fn(f"{track['username']} - {track['title']}") + ".jpg",
            "image/jpeg",
        )
        self._cover_cache[track_id] = x0_url
        cache[idx] = x0_url
        return x0_url

    async def _scs_prefetch_covers(self, session_id: str):
        sess = self._scs_sessions.get(session_id)
        if not sess:
            return
        for i in range(1, len(sess["tracks"])):
            if session_id not in self._scs_sessions:
                return
            if i not in sess["cover_cache"]:
                await self._scs_get_cover(session_id, i)
            await asyncio.sleep(0.3)

    def _scs_markup(self, session_id: str, total: int):
        sess = self._scs_sessions.get(session_id, {})
        idx = sess.get("index", 0)
        left_btn = {"text": self.strings["btn_left"], "callback": self._scs_left, "args": (session_id,)}
        right_btn = {"text": self.strings["btn_right"], "callback": self._scs_right, "args": (session_id,)}
        if idx > 0:
            left_btn["style"] = "primary"
        if idx < total - 1:
            right_btn["style"] = "primary"
        return [
            [{"text": self.strings["btn_download"], "callback": self._scs_download, "args": (session_id,), "style": "success"}],
            [left_btn, right_btn],
            [{"text": self.strings["btn_cancel"], "callback": self._scs_cancel, "args": (session_id,), "style": "danger"}],
        ]

    def _scs_done_markup(self, session_id: str):
        return [
            [{
                "text": self.strings["btn_new_search"],
                "input": self.strings["enter_query"],
                "handler": self._scs_new_search_input,
                "args": (session_id,),
                "style": "primary",
            }],
            [{"text": self.strings["btn_cancel"], "callback": self._scs_cancel, "args": (session_id,), "style": "danger"}],
        ]

    async def _scs_left(self, call, session_id: str):
        sess = self._scs_sessions.get(session_id)
        if not sess or sess["index"] <= 0:
            await call.answer()
            return
        lock = self._scs_locks.get(session_id)
        if lock and lock.locked():
            await call.answer()
            return
        async with lock:
            sess["index"] -= 1
            await self._scs_update(call, session_id)

    async def _scs_right(self, call, session_id: str):
        sess = self._scs_sessions.get(session_id)
        if not sess or sess["index"] >= len(sess["tracks"]) - 1:
            await call.answer()
            return
        lock = self._scs_locks.get(session_id)
        if lock and lock.locked():
            await call.answer()
            return
        async with lock:
            sess["index"] += 1
            await self._scs_update(call, session_id)

    async def _scs_update(self, call, session_id: str):
        sess = self._scs_sessions[session_id]
        idx = sess["index"]
        track = sess["tracks"][idx]
        cover_url = await self._scs_get_cover(session_id, idx)
        markup = self._scs_markup(session_id, len(sess["tracks"]))
        edit_kwargs = dict(
            text=f"<b>{escape_html(track['title'])}</b>\n<b>{escape_html(track['username'])}</b>\n<blockquote>{track['dur_str']}</blockquote>",
            reply_markup=markup,
        )
        if cover_url:
            edit_kwargs["photo"] = cover_url
        await call.edit(**edit_kwargs)

    async def _scs_download(self, call, session_id: str):
        sess = self._scs_sessions.get(session_id)
        if not sess:
            await call.answer()
            return
        lock = self._scs_locks.get(session_id)
        if lock and lock.locked():
            await call.answer()
            return
        async with lock:
            track = sess["tracks"][sess["index"]]
            title = track["title"]
            username = track["username"]
            chat_id = sess["chat_id"]
            is_forum = sess.get("is_forum", False)
            topic_id = sess.get("topic_id")
            try:
                await call.edit(
                    self.strings["downloading_track"].format(title=escape_html(f"{username} - {title}")),
                    reply_markup=[],
                )
            except Exception:
                pass
            ddir = tempfile.mkdtemp(dir=self._tmp)
            try:
                info, err = await self._prepare_track(track["track_data"], ddir)
                if err:
                    try:
                        await call.edit(
                            self.strings["download_fail"],
                            reply_markup=self._scs_done_markup(session_id),
                        )
                    except Exception:
                        pass
                    return
                await self._send_audio(chat_id, info, reply_to=topic_id if is_forum and topic_id else None)
            finally:
                shutil.rmtree(ddir, ignore_errors=True)
            try:
                await call.edit(
                    f"<b>{escape_html(username)} — {escape_html(title)}</b>",
                    reply_markup=self._scs_done_markup(session_id),
                )
            except Exception:
                pass

    async def _scs_new_search_input(self, call, text: str, session_id: str):
        query = text.strip()
        if not query:
            await call.delete()
            return
        sess = self._scs_sessions.get(session_id, {})
        await call.edit(self.strings["searching"].format(query=escape_html(query)))
        limit = self._get_limit()
        tracks = await self._search_sc(query, limit=limit)
        if not tracks:
            await call.edit(self.strings["no_results"], reply_markup=self._scs_done_markup(session_id))
            return
        self._scs_sessions[session_id] = {
            "tracks": tracks,
            "index": 0,
            "chat_id": sess.get("chat_id"),
            "is_forum": sess.get("is_forum", False),
            "topic_id": sess.get("topic_id"),
            "cover_cache": {},
        }
        self._scs_locks[session_id] = asyncio.Lock()
        cover_url = await self._scs_get_cover(session_id, 0)
        markup = self._scs_markup(session_id, len(tracks))
        track = tracks[0]
        edit_kwargs = dict(
            text=f"<b>{escape_html(track['title'])}</b>\n<b>{escape_html(track['username'])}</b>\n<blockquote>{track['dur_str']}</blockquote>",
            reply_markup=markup,
        )
        if cover_url:
            edit_kwargs["photo"] = cover_url
        await call.edit(**edit_kwargs)
        asyncio.ensure_future(self._scs_prefetch_covers(session_id))

    async def _scs_cancel(self, call, session_id: str):
        self._scs_sessions.pop(session_id, None)
        self._scs_locks.pop(session_id, None)
        await call.delete()