__version__ = (1, 0, 0)
# meta developer: I_execute.t.me

import asyncio
import io
import logging
import os
import re
import shutil
import tempfile
import time
import traceback
import typing

from telethon.tl.types import (
    InputDocument,
    InputMediaDocument,
)
from telethon.tl.functions.messages import EditInlineBotMessageRequest

from .. import loader, utils

logger = logging.getLogger(__name__)

try:
    from curl_cffi import requests as cffi_requests
    CFFI_OK = True
except ImportError:
    cffi_requests = None
    CFFI_OK = False

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

INLINE_QUERY_BANNER = "https://raw.githubusercontent.com/i-execute/Modules/main/Storage/SCMusic/Inline_query.png"
DOWNLOADING_STUB = "https://raw.githubusercontent.com/i-execute/Modules/main/Storage/SCMusic/Downloading.mp3"

MAX_FILE_SIZE = 50 * 1024 * 1024
REQUEST_OK = 200

SC_URL_RE = re.compile(
    r"https?://(?:[a-zA-Z0-9-]+\.)?soundcloud\.com/[^\s]+",
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


def normalize_cover(raw: bytes, max_size: typing.Optional[int] = None) -> typing.Optional[bytes]:
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


@loader.tds
class SCMusic(loader.Module):
    """SoundCloud Music - search and download audio from SoundCloud"""

    strings = {
        "name": "SCMusic",
        "not_found": "Nothing found",
        "not_found_desc": "Try a different query",
        "hint_title": "SCMusic",
        "hint_desc": "Type a track name or paste a SoundCloud link",
        "downloading": "Downloading...",
        "link_not_found": "Track not found",
        "link_not_found_desc": "Could not get track info by link",
    }

    strings_ru = {
        "not_found": "Ничего не найдено",
        "not_found_desc": "Попробуй другой запрос",
        "hint_title": "SCMusic",
        "hint_desc": "Введи название трека или вставь ссылку на SoundCloud",
        "downloading": "Загрузка...",
        "link_not_found": "Трек не найден",
        "link_not_found_desc": "Не удалось получить информацию о треке по ссылке",
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
        self._real_cache = {}
        self._stub_cache = {}
        self._search_cache = {}

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
                "duration": item.get("duration", 0) // 1000,
                "artwork_url": art,
                "track_data": item,
            })
        return results

    async def _resolve_url(self, url: str) -> typing.Optional[dict]:
        if not CFFI_OK:
            return None
        try:
            async with cffi_requests.AsyncSession(impersonate="chrome120") as ses:
                c_id = await self._get_client_id(ses)
                if not c_id:
                    h_resp = await ses.get(url)
                    if h_resp.status_code != REQUEST_OK:
                        return None
                    html = h_resp.text
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

    async def _dl_track(self, track_data: dict) -> dict:
        if not CFFI_OK:
            return {"error": "curl_cffi not installed"}

        ddir = tempfile.mkdtemp(dir=self._tmp)
        try:
            async with cffi_requests.AsyncSession(impersonate="chrome120") as ses:
                c_id = await self._get_client_id(ses)
                if not c_id:
                    return {"error": "no client_id"}

                title = track_data.get("title", "Unknown")
                artist = track_data.get("user", {}).get("username", "Unknown")
                dur_s = track_data.get("duration", 0) // 1000

                art = track_data.get("artwork_url") or track_data.get("user", {}).get("avatar_url") or ""
                if art:
                    art = art.replace("-large.jpg", "-t500x500.jpg")

                tr = track_data.get("media", {}).get("transcodings", [])
                if not tr:
                    return {"error": "no transcodings"}

                s_info = next(
                    (t for t in tr if t.get("format", {}).get("protocol") == "progressive"),
                    tr[0],
                )
                s_url = s_info.get("url") + f"?client_id={c_id}"
                if track_data.get("track_authorization"):
                    s_url += f"&track_authorization={track_data['track_authorization']}"

                s_resp = await ses.get(s_url)
                if s_resp.status_code != REQUEST_OK or not s_resp.json().get("url"):
                    return {"error": "stream url failed"}

                stream_url = s_resp.json().get("url")
                a_buf = io.BytesIO()
                a_buf.name = "track.mp3"

                if s_info.get("format", {}).get("protocol") == "progressive":
                    m_resp = await ses.get(stream_url)
                    if m_resp.status_code != REQUEST_OK:
                        return {"error": "progressive dl failed"}
                    a_buf.write(m_resp.content)
                else:
                    m3_resp = await ses.get(stream_url)
                    if m3_resp.status_code != REQUEST_OK:
                        return {"error": "m3u8 fetch failed"}
                    chk = [l for l in m3_resp.text.splitlines() if l and not l.startswith("#")]
                    if not chk:
                        return {"error": "empty m3u8"}
                    for c_u in chk:
                        c_r = await ses.get(c_u)
                        if c_r.status_code != REQUEST_OK:
                            return {"error": "chunk dl failed"}
                        a_buf.write(c_r.content)

                if a_buf.tell() == 0:
                    return {"error": "empty audio"}
                if a_buf.tell() > MAX_FILE_SIZE:
                    return {"error": "too_large"}

                raw_cover = None
                if art:
                    try:
                        a_r = await ses.get(art)
                        if a_r.status_code == REQUEST_OK:
                            raw_cover = a_r.content
                    except Exception:
                        pass

                cover_data = normalize_cover(raw_cover) if raw_cover else None
                thumb_data = normalize_cover(raw_cover, max_size=320) if raw_cover else None

                mp3_path = os.path.join(ddir, "track.mp3")
                with open(mp3_path, "wb") as f:
                    a_buf.seek(0)
                    f.write(a_buf.read())

                if cover_data:
                    cover_path = os.path.join(ddir, "cover.jpg")
                    covered_mp3 = os.path.join(ddir, "track_cover.mp3")
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

                if not thumb_data and cover_data:
                    thumb_data = cover_data

                with open(mp3_path, "rb") as f:
                    audio_bytes = f.read()

                file_id = await self._upload_to_tg(
                    audio_bytes,
                    "track.mp3",
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

    async def _get_stub(self, track_id: int, title: str, artist: str, artwork_url: str = "") -> typing.Optional[InputDocument]:
        cache_key = str(track_id)
        if cache_key in self._stub_cache:
            return self._stub_cache[cache_key]

        _log("STUB", f"Creating stub for {track_id} ({artist} - {title})")

        stub_bytes = b""
        if CFFI_OK:
            try:
                async with cffi_requests.AsyncSession(impersonate="chrome120") as ses:
                    r = await ses.get(DOWNLOADING_STUB)
                    if r.status_code == REQUEST_OK:
                        stub_bytes = r.content
            except Exception as e:
                _log("STUB", f"Stub audio dl failed: {e}")

        if not stub_bytes:
            return None

        raw_cover = None
        if artwork_url and CFFI_OK:
            try:
                async with cffi_requests.AsyncSession(impersonate="chrome120") as ses:
                    r = await ses.get(artwork_url)
                    if r.status_code == REQUEST_OK:
                        raw_cover = r.content
            except Exception:
                pass

        thumb_data = normalize_cover(raw_cover, max_size=320) if raw_cover else None

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
                    self._stub_cache[cache_key] = fid
                    _log("STUB", f"Created: doc_id={doc.id}")
                    try:
                        await self.inline.bot.delete_message(self._me_id, sent.id)
                    except Exception:
                        pass
                    return fid
        except Exception as e:
            _log("STUB", f"send_audio failed: {e}\n{traceback.format_exc()}")
        return None

    @loader.need_update("chosen_inline_result")
    async def _on_chosen(self, update):
        rid = update.id
        msg_id = update.msg_id
        _log("CHOSEN", f"rid={rid!r} msg_id={msg_id!r}")
        if not rid or not rid.startswith("sc_") or not msg_id:
            return
        cache_key = rid[3:]
        if cache_key in self._real_cache:
            await self._do_replace(msg_id, self._real_cache[cache_key])
            return
        asyncio.ensure_future(self._bg_dl_replace(cache_key, msg_id))

    async def _bg_dl_replace(self, cache_key: str, msg_id):
        _log("BG", f"Start dl cache_key={cache_key}")
        try:
            track_data = self._search_cache.get(f"__td_{cache_key}")
            if not track_data:
                _log("BG", "No track_data in cache")
                return
            result = await self._dl_track(track_data)
            if "error" in result:
                _log("BG", f"Error: {result['error']}")
                return
            data = (
                result["file_id"],
                result["title"],
                result["artist"],
                result["duration"],
            )
            self._real_cache[cache_key] = data
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

    @loader.inline_handler(ru_doc="SoundCloud поиск", en_doc="SoundCloud search")
    async def sc_inline_handler(self, query):
        """SoundCloud search"""
        raw = query.query.strip()
        prefix = "sc"
        text = raw[len(prefix):].strip() if raw.lower().startswith(prefix) else raw.strip()

        if not text:
            await self._hint(query)
            return

        _log("INLINE", f"query={text!r} from={query.from_user.id}")

        m = SC_URL_RE.search(text)
        if m:
            await self._handle_link_inline(query, m.group(0))
        else:
            await self._handle_search_inline(query, text)

    async def _handle_link_inline(self, query, url: str):
        _log("LINK", f"url={url}")

        cache_key = re.sub(r"https?://(?:www\.)?soundcloud\.com/", "", url).strip("/").replace("/", "_")

        if cache_key in self._real_cache:
            data = self._real_cache[cache_key]
            try:
                await query.answer(
                    [
                        await query.builder.document(
                            data[0],
                            title=data[1],
                            description=data[2],
                            mime_type="audio/mpeg",
                            id=f"sc_{cache_key}",
                        )
                    ],
                    cache_time=0,
                    private=True,
                )
            except Exception as e:
                _log("LINK", f"answer failed (cached): {e}")
            return

        track = await self._resolve_url(url)
        if not track:
            await query.answer(
                [
                    await query.builder.article(
                        title=self.strings["link_not_found"],
                        description=self.strings["link_not_found_desc"],
                        text=f"<b>SCMusic:</b> {self.strings['link_not_found_desc']}",
                        parse_mode="HTML",
                        link_preview=False,
                        id=f"notfound_{int(time.time())}",
                    )
                ],
                cache_time=0,
                private=True,
            )
            return

        self._search_cache[f"__td_{cache_key}"] = track["track_data"]

        stub_fid = await self._get_stub(
            track["id"],
            track["title"],
            track["username"],
            track["artwork_url"],
        )

        if stub_fid:
            try:
                await query.answer(
                    [
                        await query.builder.document(
                            stub_fid,
                            title=track["title"],
                            description=track["username"],
                            mime_type="audio/mpeg",
                            id=f"sc_{cache_key}",
                            buttons=self.inline.generate_markup(
                                {"text": self.strings["downloading"], "data": f"scm_{cache_key[:32]}"}
                            ),
                        )
                    ],
                    cache_time=0,
                    private=True,
                )
                _log("LINK", f"Answered with stub cache_key={cache_key}")
                return
            except Exception as e:
                _log("LINK", f"answer with stub failed: {e}")

        await query.answer(
            [
                await query.builder.article(
                    title=track["title"],
                    description=track["username"],
                    text=f"<b>SCMusic:</b> {escape_html(track['title'])}",
                    parse_mode="HTML",
                    link_preview=False,
                    id=f"sc_{cache_key}",
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
            tracks = await self._search_sc(text, limit=limit)
            self._search_cache[cache_key] = tracks

        if not tracks:
            await query.answer(
                [
                    await query.builder.article(
                        title=self.strings["not_found"],
                        description=self.strings["not_found_desc"],
                        text=f"<b>SCMusic:</b> {self.strings['not_found_desc']}",
                        parse_mode="HTML",
                        link_preview=False,
                        id=f"notfound_{int(time.time())}",
                    )
                ],
                cache_time=0,
                private=True,
            )
            return

        for t in tracks:
            td_key = f"__td_{str(t['id'])}"
            if td_key not in self._search_cache:
                self._search_cache[td_key] = t["track_data"]

        stub_tasks = [
            self._get_stub(t["id"], t["title"], t["username"], t["artwork_url"])
            for t in tracks
        ]
        stub_results = await asyncio.gather(*stub_tasks, return_exceptions=True)

        inline_results = []
        for i, t in enumerate(tracks):
            track_id = t["id"]
            cache_key_t = str(track_id)
            title = t["title"]
            username = t["username"]

            stub_fid = (
                stub_results[i]
                if not isinstance(stub_results[i], Exception)
                else None
            )

            if cache_key_t in self._real_cache:
                inline_results.append(
                    await query.builder.document(
                        self._real_cache[cache_key_t][0],
                        title=title,
                        description=username,
                        mime_type="audio/mpeg",
                        id=f"sc_{cache_key_t}",
                    )
                )
            elif stub_fid:
                inline_results.append(
                    await query.builder.document(
                        stub_fid,
                        title=title,
                        description=username,
                        mime_type="audio/mpeg",
                        id=f"sc_{cache_key_t}",
                        buttons=self.inline.generate_markup(
                            {"text": self.strings["downloading"], "data": f"scm_{cache_key_t[:32]}"}
                        ),
                    )
                )
            else:
                inline_results.append(
                    await query.builder.article(
                        title=title,
                        description=username,
                        text=f"<b>SCMusic:</b> {escape_html(title)}",
                        parse_mode="HTML",
                        link_preview=False,
                        id=f"sc_{cache_key_t}",
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
                        text=f"<b>SCMusic:</b> {self.strings['hint_desc']}",
                        parse_mode="HTML",
                        link_preview=False,
                        thumb=self.inline._web_document(INLINE_QUERY_BANNER, width=640, height=640),
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
        if self._tmp and os.path.exists(self._tmp):
            shutil.rmtree(self._tmp, ignore_errors=True)
