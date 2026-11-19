# CopyLeft 2026 github.com/i-execute
# Author: I_execute.t.me
# Licensed under AGPLv3.

import os
import re
import json
import time
import random
import shutil
import tempfile
import threading

__version__ = (1, 0, 0)

_KINO_URL_RE = re.compile(
    r"https?://(?:www\.|hd\.)?kinopoisk\.ru/(?:film|series|movie)/(\d+)", re.IGNORECASE
)

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
WV = "4d8dbfa5cd49094e5cf3a6b75f003280425dd2392667f53bd65435cc8c7e4e2f"
KINOPOISK_API_KEY = "c20595a1-3d8c-4cbd-92eb-fd7b0fa75c67"
LINKPP_API = "https://p.linkpp.ink/api/players"

_HEADERS = {
    "User-Agent": UA,
    "Accept": "*/*",
    "Accept-Language": "ru,en-US;q=0.9,en;q=0.8",
}


def is_kinopoisk_url(url):
    return bool(_KINO_URL_RE.search(url or ""))


def get_kinopoisk_id(url):
    m = _KINO_URL_RE.search(url or "")
    return m.group(1) if m else None


def _http():
    import requests

    s = requests.Session()
    s.headers.update(_HEADERS)
    return s


def _get_meta(kp_id):
    try:
        s = _http()
        r = s.get(
            f"https://kinopoiskapiunofficial.tech/api/v2.2/films/{kp_id}",
            headers={"X-API-Key": KINOPOISK_API_KEY, "User-Agent": UA},
            timeout=10,
        )
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return {}


def _get_players(kp_id, retries=3, delay=2.0):
    s = _http()
    last = None
    for i in range(retries):
        try:
            r = s.get(f"{LINKPP_API}?kinopoisk={kp_id}", timeout=15)
            r.raise_for_status()
            return r.json().get("data", [])
        except Exception as e:
            last = e
            time.sleep(delay)
    raise RuntimeError(f"linkpp API failed: {last}")


def _pick_player(players, order=("Alloha", "Collaps")):
    for name in order:
        for p in players:
            if p.get("type") == name:
                return p
    return players[0] if players else None


def _borth(wl):
    return f"{WV}|{_a5(_a6(_a7(wl)))}"


def _a5(s):
    ah = len(s)
    if ah <= 0:
        return s
    ax = _npa(ah)
    ac = []
    seen = [False] * ah
    pos = 0
    while len(ac) < ah:
        pos = (pos + 2) % ax
        if pos < ah and not seen[pos]:
            ac.append(pos)
            seen[pos] = True
    r = [""] * ah
    for i in range(ah):
        r[ac[i]] = s[i]
    return "".join(r)


def _a6(s):
    ah = len(s)
    if ah <= 0:
        return s
    aU = _l2c(ah)
    def ax(i):
        if i == 0:
            return aU
        c = 0
        while not i & 1:
            c += 1
            i >>= 1
        return c
    bs = [0] * (aU + 1)
    for i in range(ah):
        bs[ax(i)] += 1
    pieces = []
    pos = 0
    for b in range(aU + 1):
        pieces.append(s[pos:pos + bs[b]])
        pos += bs[b]
    ptrs = [0] * (aU + 1)
    r = [""] * ah
    for i in range(ah):
        b = ax(i)
        r[i] = pieces[b][ptrs[b]]
        ptrs[b] += 1
    return "".join(r)


def _a7(s):
    ah = len(s)
    if ah <= 0:
        return s
    aU = _l2c(ah)
    def ax(i):
        if i == 0:
            return 0
        return _bl(i)
    bs = [0] * (aU + 1)
    for i in range(ah):
        bs[ax(i)] += 1
    pieces = [None] * (aU + 1)
    pos = 0
    for b in range(aU, -1, -1):
        pieces[b] = s[pos:pos + bs[b]]
        pos += bs[b]
    ptrs = [0] * (aU + 1)
    r = [""] * ah
    for i in range(ah):
        b = ax(i)
        r[i] = pieces[b][ptrs[b]]
        ptrs[b] += 1
    return "".join(r)


def _isp(n):
    if n < 2:
        return False
    if n % 2 == 0:
        return n == 2
    c = 3
    while c * c <= n:
        if n % c == 0:
            return False
        c += 2
    return True


def _npa(n):
    c = max(2, n + 1)
    while not _isp(c):
        c += 1
    return c


def _bl(i):
    n = 0
    while i > 0:
        n += 1
        i >>= 1
    return n


def _l2c(n):
    a = 0
    while (1 << a) < n:
        a += 1
    return a


class KinopoiskInfo:
    def __init__(self, kp_id, player, meta, iframe_url, file_list, wl, token):
        self.kp_id = kp_id
        self.player = player
        self.meta = meta
        self.iframe_url = iframe_url
        self.file_list = file_list
        self.wl = wl
        self.token = token
        self._entries = []
        self._collect_entries(self.file_list.get("all", {}))
        self.is_series = self.file_list.get("type", "movie") == "serial"

    def _collect_entries(self, data):
        if isinstance(data, dict):
            if "id" in data and "id_translation" in data:
                self._entries.append(data)
            else:
                for v in data.values():
                    self._collect_entries(v)
        elif isinstance(data, list):
            for item in data:
                self._collect_entries(item)

    @property
    def title(self):
        return self.meta.get("nameRu") or self.meta.get("nameOriginal") or self.meta.get("nameEn") or f"Kinopoisk_{self.kp_id}"

    @property
    def year(self):
        return self.meta.get("year") or ""

    @property
    def duration(self):
        return self.meta.get("filmLength") or 0

    @property
    def poster(self):
        return self.meta.get("posterUrl") or self.meta.get("posterUrlPreview")

    def translations(self):
        seen = {}
        for e in self._entries:
            tid = e.get("id_translation")
            if tid is not None and tid not in seen:
                seen[tid] = e.get("translation", "?")
        return [{"id": t, "name": n} for t, n in sorted(seen.items())]

    def seasons(self):
        if not self.is_series:
            return []
        return sorted(int(s) for s in self.file_list.get("all", {}) if str(s).isdigit())

    def episodes(self, season):
        sd = self.file_list.get("all", {}).get(str(season), {})
        return sorted(int(e) for e in sd if str(e).isdigit())

    def entry_for(self, season=None, episode=None, translation_id=None):
        if self.is_series and season and episode:
            sd = self.file_list["all"][str(season)]
            ed = sd.get(str(episode), {})
            if not ed:
                raise ValueError(f"Episode S{season:02d}E{episode:02d} not found")
            if translation_id is not None:
                e = ed.get(f"t{translation_id}")
                if not e:
                    raise ValueError(f"Translation {translation_id} not found for this episode")
                return e
            return next(iter(ed.values()))
        if translation_id is not None:
            for e in self._entries:
                if e.get("id_translation") == translation_id:
                    return e
            raise ValueError(f"Translation {translation_id} not found")
        active = self.file_list.get("active")
        if active and "id" in active:
            return active
        return self._entries[0] if self._entries else None


def get_info(url):
    kp_id = get_kinopoisk_id(url)
    if not kp_id:
        raise ValueError("Not a Kinopoisk URL")
    players = _get_players(kp_id)
    player = _pick_player(players)
    if not player:
        raise RuntimeError("No players available")
    return _load_player(kp_id, player)


def _load_player(kp_id, player, target_iframe=None):
    s = _http()
    iframe = target_iframe or player.get("iframeUrl") or ""
    if player.get("type") == "Vibix":
        m = re.search(r'data-publisher-id="(\d+)"\s+data-type="(\w+)"\s+data-id="(\d+)"', iframe)
        if m:
            pub, _, vid = m.groups()
            iframe = f"https://vibix.to/publisher/{pub}/embed/{vid}"
    html = s.get(iframe, headers={"Referer": "https://linkpp.ink/"}, timeout=20).text
    meta = _get_meta(kp_id)
    m_fl = re.search(r"const fileList\s*=\s*JSON\.parse\('((?:[^'\\]|\\.)*)'\)", html)
    if not m_fl:
        raise RuntimeError("fileList not found in player HTML")
    file_list = json.loads(m_fl.group(1).replace("\\'", "'"))
    m_wl = re.search(r'<meta\s+name=["\']viewporti["\']\s+content=["\']([^"\']+)["\']', html)
    wl = m_wl.group(1) if m_wl else ""
    m_tk = re.search(r"token=([a-f0-9]+)", iframe)
    token = m_tk.group(1) if m_tk else ""
    return KinopoiskInfo(kp_id, player, meta, iframe, file_list, wl, token)


def get_streams(info, season=None, episode=None, translation_id=None):
    entry = info.entry_for(season, episode, translation_id)
    if not entry:
        raise RuntimeError("No playable entry")
    s = _http()
    headers = {
        "User-Agent": UA,
        "accept": "*/*",
        "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
        "x-requested-with": "XMLHttpRequest",
        "borth": _borth(info.wl),
        "Origin": "https://theatre.stravers.live",
        "Referer": info.iframe_url,
    }
    data = f"token={info.token}&av1=true&autoplay=0&audio=&subtitle="
    r = s.post(
        f"https://theatre.stravers.live/bnsi/movies/{entry['id']}",
        headers=headers, data=data, timeout=20,
    )
    if r.status_code != 200:
        raise RuntimeError(f"player API HTTP {r.status_code}")
    d = r.json()
    hls = d.get("hlsSource") or []
    if not hls:
        raise RuntimeError("no HLS sources in response")
    quality = hls[0].get("quality", {})
    guard = d.get("guard") or d.get("token") or ""
    return {"quality": quality, "guard": guard, "entry": entry}


class KinopoiskDownloader:
    def __init__(self, url):
        self.url = url
        self.info = None
        self.progress_cb = None

    def load(self):
        self.info = get_info(self.url)
        return self.info

    def list_translations(self):
        return self.translations_list() if self.info is None else self.info.translations()

    def translations_list(self):
        if self.info is None:
            self.load()
        return self.info.translations()

    def list_seasons(self):
        if self.info is None:
            self.load()
        return self.info.seasons()

    def list_episodes(self, season):
        if self.info is None:
            self.load()
        return self.info.episodes(season)

    def qualities(self, season=None, episode=None, translation_id=None):
        if self.info is None:
            self.load()
        streams = get_streams(self.info, season, episode, translation_id)
        return sorted(int(q) for q in streams["quality"]), streams

    def download(self, output_dir=None, quality=None, season=None, episode=None, translation_id=None):
        if self.info is None:
            self.load()
        if quality is None:
            quality = "1080"
        qualities, streams = self.qualities(season, episode, translation_id)
        q = str(quality) if str(quality) in streams["quality"] else str(qualities[0]) if qualities else "720"
        m3u8 = streams["quality"][q]
        guard = streams["guard"]
        output_dir = output_dir or tempfile.mkdtemp(prefix="kinodl_")
        title = re.sub(r'[\\/*?:"<>|]', "_", self.info.title).strip()[:80]
        if self.info.is_series and season and episode:
            title = f"{title}_S{int(season):02d}E{int(episode):02d}"
        elif self.info.year:
            title = f"{title}_{self.info.year}"
        output_path = os.path.join(output_dir, f"{title}.mp4")
        hdr = (
            f"Accept: */*\r\n"
            f"Accept-Language: ru,en-US;q=0.9,en;q=0.8\r\n"
            f"Accepts-Controls: {WV}\r\n"
            f"Authorizations: Bearer {guard}\r\n"
            f"Origin: https://theatre.stravers.live\r\n"
            f"Referer: {self.info.iframe_url}\r\n"
            f"User-Agent: {UA}\r\n"
        )
        return _ffmpeg_download(m3u8, output_path, hdr, self.progress_cb), q

    def download_audio(self, output_dir=None, quality=None, season=None, episode=None, translation_id=None, bitrate="192k"):
        if self.info is None:
            self.load()
        if quality is None:
            quality = "1080"
        qualities, streams = self.qualities(season, episode, translation_id)
        q = str(quality) if str(quality) in streams["quality"] else str(qualities[0]) if qualities else "720"
        m3u8 = streams["quality"][q]
        guard = streams["guard"]
        output_dir = output_dir or tempfile.mkdtemp(prefix="kinodl_")
        title = re.sub(r'[\\/*?:"<>|]', "_", self.info.title).strip()[:80]
        if self.info.is_series and season and episode:
            title = f"{title}_S{int(season):02d}E{int(episode):02d}"
        output_path = os.path.join(output_dir, f"{title}.mp3")
        hdr = (
            f"Accept: */*\r\n"
            f"Accept-Language: ru,en-US;q=0.9,en;q=0.8\r\n"
            f"Accepts-Controls: {WV}\r\n"
            f"Authorizations: Bearer {guard}\r\n"
            f"Origin: https://theatre.stravers.live\r\n"
            f"Referer: {self.info.iframe_url}\r\n"
            f"User-Agent: {UA}\r\n"
        )
        return _ffmpeg_download(m3u8, output_path, hdr, self.progress_cb, audio=True, bitrate=bitrate), q


def _ffmpeg_download(m3u8, output_path, headers, progress_cb=None, audio=False, bitrate="192k"):
    import subprocess

    cmd = ["ffmpeg", "-y", "-loglevel", "error", "-headers", headers, "-i", m3u8]
    if audio:
        cmd += ["-vn", "-c:a", "libmp3lame", "-b:a", bitrate]
    else:
        cmd += ["-c", "copy"]
    if progress_cb:
        cmd += ["-progress", "pipe:1"]
    cmd.append(output_path)
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if progress_cb:
        for line in proc.stdout:
            line = line.strip()
            if line.startswith("out_time_ms="):
                try:
                    progress_cb(int(line.split("=")[1]) / 1_000_000)
                except Exception:
                    pass
            elif line.startswith("progress=end"):
                break
    proc.wait()
    if proc.returncode != 0:
        raise RuntimeError(f"ffmpeg failed: {proc.stderr.read()[-300:] if proc.stderr else ''}")
    return output_path


def download(url, output_dir=None, quality=None, season=None, episode=None, voice=None):
    d = KinopoiskDownloader(url)
    info = d.load()
    translation_id = None
    if voice is not None:
        for t in info.translations():
            if voice.lower() in t["name"].lower():
                translation_id = t["id"]
                break
        if translation_id is None:
            raise ValueError(f"Voice '{voice}' not found")
    return d.download(output_dir, quality, season, episode, translation_id)
