__version__ = (2, 0, 0)
# meta developer: I_execute.t.me

import time
import logging
import asyncio
import ipaddress
import socket
import html
from typing import Optional, Dict

import aiohttp

from telethon.tl.types import (
    InputBotInlineResult,
    InputBotInlineMessageText,
    InputWebDocument,
)
from telethon.utils import html as tl_html

from .. import loader, utils

logger = logging.getLogger(__name__)

BANNER = "https://raw.githubusercontent.com/i-execute/Modules/main/Storage/IPScanner/InlineQuery.png"

CACHE_TTL = 120


def escape_html(t):
    return html.escape(t or "")


async def _fetch_json(url: str, timeout: int = 10):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
                if resp.status != 200:
                    return None
                return await resp.json(content_type=None)
    except Exception:
        return None


async def _resolve_hostname(hostname: str) -> Optional[str]:
    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, socket.gethostbyname, hostname)
        return result
    except Exception:
        return None


async def scan_ip(target: str) -> Dict:
    try:
        ipaddress.ip_address(target)
        ip_to_scan = target
    except ValueError:
        resolved = await _resolve_hostname(target)
        if not resolved:
            return {"error": "resolve"}
        ip_to_scan = resolved

    data = await _fetch_json(
        f"http://ip-api.com/json/{ip_to_scan}"
        "?fields=status,message,country,countryCode,region,regionName,"
        "city,zip,lat,lon,timezone,isp,org,as,asname,reverse,mobile,proxy,hosting,query"
    )

    if not data:
        return {"error": "no_data"}

    if data.get("status") == "fail":
        return {"error": data.get("message", "unknown")}

    return {
        "ip": data.get("query", ip_to_scan),
        "country": data.get("country", "N/A"),
        "country_code": data.get("countryCode", "N/A"),
        "region": data.get("regionName", "N/A"),
        "region_code": data.get("region", "N/A"),
        "city": data.get("city", "N/A"),
        "zip": data.get("zip", "N/A"),
        "lat": data.get("lat", "N/A"),
        "lon": data.get("lon", "N/A"),
        "timezone": data.get("timezone", "N/A"),
        "isp": data.get("isp", "N/A"),
        "org": data.get("org", "N/A"),
        "as": data.get("as", "N/A"),
        "asname": data.get("asname", "N/A"),
        "reverse": data.get("reverse", "N/A"),
        "mobile": data.get("mobile", False),
        "proxy": data.get("proxy", False),
        "hosting": data.get("hosting", False),
    }


def build_message(d: Dict) -> str:
    lines = []
    lines.append('<b>IPScanner</b>')

    info_lines = []
    info_lines.append(f"<b>IP:</b> <code>{escape_html(d['ip'])}</code>")
    info_lines.append(f"<b>Country:</b> {escape_html(d['country'])} ({escape_html(d['country_code'])})")
    info_lines.append(f"<b>Region:</b> {escape_html(d['region'])} ({escape_html(d['region_code'])})")
    info_lines.append(f"<b>City:</b> {escape_html(d['city'])}")
    info_lines.append(f"<b>ZIP:</b> {escape_html(str(d['zip']))}")
    lines.append("<blockquote>" + "\n".join(info_lines) + "</blockquote>")

    net_lines = []
    net_lines.append(f"<b>Coordinates:</b> {escape_html(str(d['lat']))}, {escape_html(str(d['lon']))}")
    net_lines.append(f"<b>Timezone:</b> {escape_html(d['timezone'])}")
    net_lines.append(f"<b>ISP:</b> {escape_html(d['isp'])}")
    net_lines.append(f"<b>Organization:</b> {escape_html(d['org'])}")
    net_lines.append(f"<b>AS:</b> {escape_html(d['as'])}")
    net_lines.append(f"<b>AS Name:</b> {escape_html(d['asname'])}")
    net_lines.append(f"<b>Reverse DNS:</b> {escape_html(d['reverse'])}")
    lines.append("<blockquote>" + "\n".join(net_lines) + "</blockquote>")

    flag_lines = []
    flag_lines.append(f"<b>Mobile:</b> {'Yes' if d['mobile'] else 'No'}")
    flag_lines.append(f"<b>Proxy:</b> {'Yes' if d['proxy'] else 'No'}")
    flag_lines.append(f"<b>Hosting:</b> {'Yes' if d['hosting'] else 'No'}")
    lines.append("<blockquote>" + "\n".join(flag_lines) + "</blockquote>")

    return "\n".join(lines)


@loader.tds
class IPScanner(loader.Module):
    """IP address scanner via inline query"""

    strings = {
        "name": "IPScanner",
        "hint_title": "IPScanner",
        "hint_desc": "Paste IP address or hostname",
        "hint_msg": "<b>IPScanner:</b> Paste an IP address or hostname",
        "loading_title": "Scanning...",
        "loading_desc": "Fetching IP data, wait a few seconds",
        "loading_msg": "<b>IPScanner:</b> Scanning IP... Try again in a few seconds.",
        "err_title": "Error",
        "err_resolve": "Failed to resolve hostname",
        "err_no_data": "No data received from API",
        "err_unknown": "Scan failed",
    }

    strings_ru = {
        "hint_title": "IPScanner",
        "hint_desc": "Вставьте IP адрес или хостнейм",
        "hint_msg": "<b>IPScanner:</b> Вставьте IP адрес или хостнейм",
        "loading_title": "Сканирую...",
        "loading_desc": "Получаю данные, подождите несколько секунд",
        "loading_msg": "<b>IPScanner:</b> Сканирую IP... Повторите запрос через несколько секунд.",
        "err_title": "Ошибка",
        "err_resolve": "Не удалось разрешить хостнейм",
        "err_no_data": "Нет данных от API",
        "err_unknown": "Сканирование не удалось",
    }

    def __init__(self):
        self._pending = {}
        self._cache = {}

    async def client_ready(self, client, db):
        self._client = client
        self._db = db

    def _cache_get(self, key):
        entry = self._cache.get(key)
        if not entry:
            return None
        if time.time() - entry.get("ts", 0) > CACHE_TTL:
            self._cache.pop(key, None)
            return None
        return entry.get("data")

    def _cache_set(self, key, data):
        self._cache[key] = {"data": data, "ts": time.time()}

    async def _scan_task(self, target: str, cache_key: str):
        try:
            result = await scan_ip(target)
            if "error" in result:
                err = result["error"]
                if err == "resolve":
                    msg = self.strings["err_resolve"]
                elif err == "no_data":
                    msg = self.strings["err_no_data"]
                else:
                    msg = escape_html(err)
                data = {"error": msg}
            else:
                data = {"message": build_message(result), "ip": result["ip"]}
            self._cache_set(cache_key, data)
            return data
        except Exception as e:
            data = {"error": str(e)[:80]}
            self._cache_set(cache_key, data)
            return data

    def _make_web_document(self, url, mime_type="image/png"):
        return InputWebDocument(
            url=url,
            size=0,
            mime_type=mime_type,
            attributes=[],
        )

    def _make_article(self, uid, title, description, message_text):
        plain, entities = tl_html.parse(message_text)
        return InputBotInlineResult(
            id=uid,
            type="article",
            title=title,
            description=description,
            thumb=self._make_web_document(BANNER),
            send_message=InputBotInlineMessageText(
                message=plain,
                no_webpage=True,
                entities=entities or None,
            ),
        )

    @loader.inline_handler(
        ru_doc="Сканировать IP адрес или хостнейм",
        en_doc="Scan IP address or hostname",
    )
    async def ip_inline_handler(self, query):
        """Scan IP address or hostname"""
        text = query.query.strip()
        if text.lower().startswith("ip"):
            text = text[2:].strip()

        if not text:
            await query.answer(
                results=[self._make_article(
                    f"h_{int(time.time())}",
                    self.strings["hint_title"],
                    self.strings["hint_desc"],
                    self.strings["hint_msg"],
                )],
                cache_time=0,
                private=True,
            )
            return

        target = text.strip()
        cache_key = f"ip_{target}"

        cached = self._cache_get(cache_key)
        if cached:
            if "error" in cached:
                await query.answer(
                    results=[self._make_article(
                        f"e_{int(time.time())}",
                        self.strings["err_title"],
                        str(cached["error"])[:100],
                        f"<b>IPScanner:</b> {escape_html(str(cached['error']))}",
                    )],
                    cache_time=0,
                    private=True,
                )
                return
            if "message" in cached:
                await query.answer(
                    results=[self._make_article(
                        f"r_{int(time.time())}",
                        "IPScanner",
                        f"IP: {cached.get('ip', '?')}",
                        cached["message"],
                    )],
                    cache_time=0,
                    private=True,
                )
                return

        if cache_key in self._pending:
            fut = self._pending[cache_key]
            if fut.done():
                self._pending.pop(cache_key, None)
                try:
                    res = fut.result()
                except Exception:
                    res = {"error": self.strings["err_unknown"]}
                if "error" in res:
                    await query.answer(
                        results=[self._make_article(
                            f"e_{int(time.time())}",
                            self.strings["err_title"],
                            str(res["error"])[:100],
                            f"<b>IPScanner:</b> {escape_html(str(res['error']))}",
                        )],
                        cache_time=0,
                        private=True,
                    )
                elif "message" in res:
                    await query.answer(
                        results=[self._make_article(
                            f"r_{int(time.time())}",
                            "IPScanner",
                            f"IP: {res.get('ip', '?')}",
                            res["message"],
                        )],
                        cache_time=0,
                        private=True,
                    )
                else:
                    await query.answer(
                        results=[self._make_article(
                            f"e_{int(time.time())}",
                            self.strings["err_title"],
                            self.strings["err_unknown"],
                            f"<b>IPScanner:</b> {self.strings['err_unknown']}",
                        )],
                        cache_time=0,
                        private=True,
                    )
                return
            await query.answer(
                results=[self._make_article(
                    f"ld_{int(time.time())}",
                    self.strings["loading_title"],
                    self.strings["loading_desc"],
                    self.strings["loading_msg"],
                )],
                cache_time=0,
                private=True,
            )
            return

        self._pending[cache_key] = asyncio.ensure_future(
            self._scan_task(target, cache_key)
        )
        await query.answer(
            results=[self._make_article(
                f"ld_{int(time.time())}",
                self.strings["loading_title"],
                self.strings["loading_desc"],
                self.strings["loading_msg"],
            )],
            cache_time=0,
            private=True,
        )

    async def on_unload(self):
        for fut in self._pending.values():
            fut.cancel()
        self._pending.clear()
        self._cache.clear()