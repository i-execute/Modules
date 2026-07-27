__version__ = (1, 0, 0)
# meta developer: @I_execute

import logging
import asyncio
import ipaddress
import socket
from typing import Optional, Dict

from telethon.tl.types import Message
from .. import loader, utils

logger = logging.getLogger(__name__)

try:
    import aiohttp
    AIOHTTP_OK = True
except ImportError:
    AIOHTTP_OK = False


def _escape(text):
    if not text:
        return ""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


async def _fetch_json(url: str, timeout: int = 10):
    if not AIOHTTP_OK:
        return None
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
                if resp.status != 200:
                    return None
                return await resp.json()
    except Exception as e:
        logger.debug("_fetch_json error %s: %s", url, e)
        return None


async def _resolve_hostname(hostname: str) -> Optional[str]:
    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, socket.gethostbyname, hostname)
        return result
    except Exception:
        return None


async def _scan_ip(ip: str) -> Dict:
    data = await _fetch_json(f"http://ip-api.com/json/{ip}?fields=status,message,country,countryCode,region,regionName,city,zip,lat,lon,timezone,isp,org,as,asname,reverse,mobile,proxy,hosting,query")
    
    if not data:
        return {}
    
    if data.get("status") == "fail":
        return {"error": data.get("message", "Unknown error")}
    
    return {
        "ip": data.get("query", ip),
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


@loader.tds
class IPScanner(loader.Module):
    """IP address information scanner"""

    strings = {
        "name": "IPScanner",

        "loading": "<b>Scanning...</b>",

        "result": (
            "<b>IP Information</b>\n"
            "<blockquote>"
            "IP: <code>{ip}</code>\n"
            "Country: {country} ({country_code})\n"
            "Region: {region} ({region_code})\n"
            "City: {city}\n"
            "ZIP: {zip}\n"
            "Coordinates: {lat}, {lon}\n"
            "Timezone: {timezone}\n"
            "ISP: {isp}\n"
            "Organization: {org}\n"
            "AS: {as_info}\n"
            "AS Name: {asname}\n"
            "Reverse DNS: {reverse}\n"
            "Mobile: {mobile}\n"
            "Proxy: {proxy}\n"
            "Hosting: {hosting}"
            "</blockquote>"
        ),

        "err_no_arg": (
            "<b>Error</b>\n"
            "<blockquote>Provide an IP address or hostname</blockquote>"
        ),

        "err_invalid_ip": (
            "<b>Error</b>\n"
            "<blockquote>Invalid IP address</blockquote>"
        ),

        "err_resolve": (
            "<b>Error</b>\n"
            "<blockquote>Failed to resolve hostname</blockquote>"
        ),

        "err_scan": (
            "<b>Error</b>\n"
            "<blockquote>Scan failed: {error}</blockquote>"
        ),

        "err_no_data": (
            "<b>Error</b>\n"
            "<blockquote>No data received from API</blockquote>"
        ),
    }

    strings_ru = {
        "loading": "<b>Сканирование...</b>",

        "result": (
            "<b>Информация об IP</b>\n"
            "<blockquote>"
            "IP: <code>{ip}</code>\n"
            "Страна: {country} ({country_code})\n"
            "Регион: {region} ({region_code})\n"
            "Город: {city}\n"
            "Индекс: {zip}\n"
            "Координаты: {lat}, {lon}\n"
            "Часовой пояс: {timezone}\n"
            "Провайдер: {isp}\n"
            "Организация: {org}\n"
            "AS: {as_info}\n"
            "AS имя: {asname}\n"
            "Reverse DNS: {reverse}\n"
            "Мобильный: {mobile}\n"
            "Прокси: {proxy}\n"
            "Хостинг: {hosting}"
            "</blockquote>"
        ),

        "err_no_arg": (
            "<b>Ошибка</b>\n"
            "<blockquote>Укажите IP адрес или хостнейм</blockquote>"
        ),

        "err_invalid_ip": (
            "<b>Ошибка</b>\n"
            "<blockquote>Неверный IP адрес</blockquote>"
        ),

        "err_resolve": (
            "<b>Ошибка</b>\n"
            "<blockquote>Не удалось разрешить хостнейм</blockquote>"
        ),

        "err_scan": (
            "<b>Ошибка</b>\n"
            "<blockquote>Сканирование не удалось: {error}</blockquote>"
        ),

        "err_no_data": (
            "<b>Ошибка</b>\n"
            "<blockquote>Нет данных от API</blockquote>"
        ),
    }

    @loader.command(
        ru_doc="[IP/hostname] - сканировать IP адрес",
        en_doc="[IP/hostname] - scan IP address",
    )
    async def ip(self, message: Message):
        """[IP/hostname] - scan IP address"""
        args = utils.get_args_raw(message).strip()

        if not args:
            await utils.answer(message, self.strings["err_no_arg"])
            return

        status = await utils.answer(message, self.strings["loading"])

        target = args
        try:
            ipaddress.ip_address(target)
            ip_to_scan = target
        except ValueError:
            resolved = await _resolve_hostname(target)
            if not resolved:
                await utils.answer(status, self.strings["err_resolve"])
                return
            ip_to_scan = resolved

        result = await _scan_ip(ip_to_scan)

        if not result:
            await utils.answer(status, self.strings["err_no_data"])
            return

        if "error" in result:
            await utils.answer(
                status,
                self.strings["err_scan"].format(error=_escape(result["error"]))
            )
            return

        await utils.answer(
            status,
            self.strings["result"].format(
                ip=_escape(result.get("ip", "N/A")),
                country=_escape(result.get("country", "N/A")),
                country_code=_escape(result.get("country_code", "N/A")),
                region=_escape(result.get("region", "N/A")),
                region_code=_escape(result.get("region_code", "N/A")),
                city=_escape(result.get("city", "N/A")),
                zip=_escape(str(result.get("zip", "N/A"))),
                lat=_escape(str(result.get("lat", "N/A"))),
                lon=_escape(str(result.get("lon", "N/A"))),
                timezone=_escape(result.get("timezone", "N/A")),
                isp=_escape(result.get("isp", "N/A")),
                org=_escape(result.get("org", "N/A")),
                as_info=_escape(result.get("as", "N/A")),
                asname=_escape(result.get("asname", "N/A")),
                reverse=_escape(result.get("reverse", "N/A")),
                mobile="Yes" if result.get("mobile") else "No",
                proxy="Yes" if result.get("proxy") else "No",
                hosting="Yes" if result.get("hosting") else "No",
            )
        )