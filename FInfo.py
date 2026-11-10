# CopyLeft 2026 github.com/i-execute // i_execute.t.me
# Licensed under AGPLv3.

# meta developer: @i_execute
# scope: inline
# scope: telegram_stars

import asyncio
import contextlib
import getpass
import logging
import os
import platform
import re
import socket
import sys
import time
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import aiohttp
import psutil
import telethon
from telethon.tl.functions.payments import GetStarsStatusRequest
from telethon.tl.functions.users import GetFullUserRequest
from telethon.tl.types import Message
from telethon.utils import get_display_name

from .. import loader, utils, version
from ..assets import FEROKU_IMAGE_URL

logger = logging.getLogger(__name__)

STAR_USD = 0.022


class AsyncTTLCache:
    """Thread-safe and async-friendly TTL cache with max entries limit."""

    def __init__(self, max_size: int = 128, default_ttl: int = 300):
        self._cache: OrderedDict[str, tuple[float, object]] = OrderedDict()
        self._max_size = max_size
        self._default_ttl = default_ttl
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> object | None:
        async with self._lock:
            if key not in self._cache:
                return None
            expires_at, value = self._cache[key]
            if time.monotonic() > expires_at:
                self._cache.pop(key, None)
                return None
            self._cache.move_to_end(key)
            return value

    async def set(self, key: str, value: object, ttl: int | None = None) -> None:
        async with self._lock:
            if len(self._cache) >= self._max_size:
                self._cache.popitem(last=False)
            self._cache[key] = (
                time.monotonic() + (ttl if ttl is not None else self._default_ttl),
                value,
            )


@loader.tds
class FInfo(loader.Module):
    """Feroku Info module with placeholder rendering."""

    strings = {
        "name": "FInfo",
        "default_status_bar": (
            "<b>Status bar</b>\n"
            "<blockquote>"
            "<b>[ SYSTEM INFO ]</b>\n"
            "<b>HOST ::</b> <code>{host}</code>\n"
            "<b>USER ::</b> <code>{user}</code>\n"
            "<b>KERN ::</b> <code>{kernel}</code>\n"
            "<b> OS  ::</b> <code>{os}</code>\n"
            "<b>CPU  ::</b> <code>{cpu_usage}%</code>\n"
            "<b>RAM  ::</b> <code>{ram_usage}</code>\n"
            "<b>[ SOFTWARE &amp; GIT ]</b>\n"
            "<b>PY   ::</b> <code>{py_ver} | HTL {htl_ver}</code>\n"
            "<b>VER  ::</b> <code>{ver}</code>\n"
            "<b>GIT  ::</b> <code>{git_branch} -&gt; {git_status}</code>\n"
            "<b>PFX  ::</b> <code>{pfx}</code>\n"
            "<b>Upd:</b> <code>{upd}</code>"
            "</blockquote>"
        ),
        "rich_status_bar": (
            "{img}<h1>Feroku Status</h1>"
            "<details><summary><b>System Info</b></summary>"
            "<p><b>Host:</b> <code>{host}</code></p>"
            "<p><b>User:</b> <code>{user}</code></p>"
            "<p><b>Kernel:</b> <code>{kernel}</code></p>"
            "<p><b>OS:</b> <code>{os}</code></p>"
            "<p><b>CPU:</b> <code>{cpu_usage}%</code></p>"
            "<p><b>RAM:</b> <code>{ram_usage}</code></p>"
            "<p><b>Uptime:</b> <code>{os_uptime}</code></p>"
            "</details>"
            "<details><summary><b>Software &amp; Git</b></summary>"
            "<p><b>Python:</b> <code>{py_ver}</code></p>"
            "<p><b>Telethon:</b> <code>{htl_ver} (layer {layer})</code></p>"
            "<p><b>Version:</b> <code>{ver}</code></p>"
            "<p><b>Branch:</b> <code>{git_branch}</code> (<code>{git_status}</code>)</p>"
            "<p><b>Prefix:</b> <code>{pfx}</code></p>"
            "<p><b>Status:</b> <code>{upd}</code></p>"
            "</details>"
        ),
        "custom_message_doc": "Custom Info/Status bar template. Leave empty to use the default.",
        "banner_url_doc": "Banner image used by Info and Rich views.",
        "quote_media_doc": "Switch Info preview media to quote.",
        "invert_media_doc": "Invert preview media position.",
        "rich_mode_doc": "Render Info as a Rich message.",
        "city_doc": "City for weather placeholder (e.g. 'Berlin', 'Moscow').",
        "github_user_doc": "Default GitHub username for repos and followers placeholders.",
        "lastfm_user_doc": "Default Last.FM username for music placeholders.",
        "no_val": "N/A",
        "no_bio": "No bio",
        "no_premium": "No Premium",
        "up_to_date": "Up-to-date",
        "clean": "clean",
    }

    strings_ru = {
        "_cls_doc": "Модуль информации и статус-бара Feroku с поддержкой плейсхолдеров.",
        "_cmd_doc_info": "Показать статус-бар или системную информацию Feroku.",
        "custom_message_doc": "Пользовательский шаблон. Оставьте пустым для стандартного.",
        "banner_url_doc": "Ссылка на баннер для шапки.",
        "city_doc": "Город для погоды.",
        "github_user_doc": "Имя пользователя GitHub.",
        "lastfm_user_doc": "Имя пользователя Last.FM.",
        "no_val": "Н/Д",
        "no_bio": "Нет описания",
        "no_premium": "Нет Premium",
        "up_to_date": "Актуально",
        "clean": "чисто",
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "custom_message",
                None,
                lambda: self.strings["custom_message_doc"],
            ),
            loader.ConfigValue(
                "banner_url",
                FEROKU_IMAGE_URL,
                lambda: self.strings["banner_url_doc"],
                validator=loader.validators.RandomLink(),
            ),
            loader.ConfigValue(
                "city",
                "",
                lambda: self.strings["city_doc"],
                validator=loader.validators.String(),
            ),
            loader.ConfigValue(
                "github_user",
                "",
                lambda: self.strings["github_user_doc"],
                validator=loader.validators.String(),
            ),
            loader.ConfigValue(
                "lastfm_user",
                "",
                lambda: self.strings["lastfm_user_doc"],
                validator=loader.validators.String(),
            ),
            loader.ConfigValue(
                "my_crypto_address",
                "",
                lambda: "Custom crypto address for placeholder",
                validator=loader.validators.String(),
            ),
            loader.ConfigValue(
                "my_card_number",
                "",
                lambda: "Custom bank card number for placeholder",
                validator=loader.validators.String(),
            ),
            loader.ConfigValue(
                "my_donate_site",
                "",
                lambda: "Custom donate link for placeholder",
                validator=loader.validators.String(),
            ),
            loader.ConfigValue(
                "my_channel",
                "",
                lambda: "Custom Telegram channel for placeholder",
                validator=loader.validators.String(),
            ),
            loader.ConfigValue(
                "my_social_network",
                "",
                lambda: "Custom social link for placeholder",
                validator=loader.validators.String(),
            ),
            loader.ConfigValue(
                "quote_media",
                False,
                lambda: self.strings["quote_media_doc"],
                validator=loader.validators.Boolean(),
            ),
            loader.ConfigValue(
                "invert_media",
                False,
                lambda: self.strings["invert_media_doc"],
                validator=loader.validators.Boolean(),
            ),
            loader.ConfigValue(
                "rich_mode",
                False,
                lambda: self.strings["rich_mode_doc"],
                validator=loader.validators.Boolean(),
            ),
        )
        self._cache = AsyncTTLCache(max_size=256, default_ttl=300)

    @staticmethod
    def _os_pretty_name() -> str:
        try:
            with open("/etc/os-release", encoding="utf-8") as f:
                for line in f:
                    if line.startswith("PRETTY_NAME="):
                        return line.split("=", 1)[1].strip().strip('"')
        except OSError:
            pass
        return platform.system() + " " + platform.release()

    @staticmethod
    def _format_uptime(seconds: float) -> str:
        s = int(seconds)
        days, s = divmod(s, 86400)
        hours, s = divmod(s, 3600)
        minutes, s = divmod(s, 60)
        if days > 0:
            return f"{days}d {hours}h {minutes}m"
        if hours > 0:
            return f"{hours}h {minutes}m"
        return f"{minutes}m"

    async def _fetch_json(self, url: str, timeout: int = 4) -> dict | list | None:
        cached = await self._cache.get(url)
        if cached is not None:
            return cached
        try:
            to = aiohttp.ClientTimeout(total=timeout)
            async with aiohttp.ClientSession(timeout=to) as session:
                async with session.get(url, headers={"User-Agent": "Feroku/3.0"}) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        await self._cache.set(url, data)
                        return data
        except Exception:
            logger.debug("Failed to fetch JSON from %s", url, exc_info=True)
        return None

    async def _fetch_text(self, url: str, timeout: int = 4) -> str | None:
        cached = await self._cache.get(url)
        if cached is not None:
            return str(cached)
        try:
            to = aiohttp.ClientTimeout(total=timeout)
            async with aiohttp.ClientSession(timeout=to) as session:
                async with session.get(url, headers={"User-Agent": "Feroku/3.0"}) as resp:
                    if resp.status == 200:
                        text = (await resp.text()).strip()
                        await self._cache.set(url, text)
                        return text
        except Exception:
            logger.debug("Failed to fetch text from %s", url, exc_info=True)
        return None

    async def _get_rates(self) -> dict[str, float]:
        cached = await self._cache.get("rates_dict")
        if isinstance(cached, dict):
            return cached

        rates: dict[str, float] = {
            "USD_RUB": 95.0,
            "EUR_RUB": 102.0,
            "CNY_RUB": 13.0,
            "KZT_RUB": 0.19,
            "USDT_RUB": 95.5,
            "TON_RUB": 520.0,
            "BTC_RUB": 6500000.0,
            "ETH_RUB": 250000.0,
        }

        data = await self._fetch_json("https://open.er-api.com/v6/latest/USD", timeout=4)
        if isinstance(data, dict) and "rates" in data:
            er = data["rates"]
            usd_rub = float(er.get("RUB", 95.0))
            rates["USD_RUB"] = usd_rub
            if er.get("EUR"):
                rates["EUR_RUB"] = usd_rub / float(er["EUR"])
            if er.get("CNY"):
                rates["CNY_RUB"] = usd_rub / float(er["CNY"])
            if er.get("KZT"):
                rates["KZT_RUB"] = usd_rub / float(er["KZT"])

        crypto = await self._fetch_json(
            "https://api.coingecko.com/api/v3/simple/price?ids=tether,the-open-network,bitcoin,ethereum&vs_currencies=usd,rub",
            timeout=4,
        )
        if isinstance(crypto, dict):
            if "tether" in crypto:
                rates["USDT_RUB"] = float(crypto["tether"].get("rub", rates["USD_RUB"]))
            if "the-open-network" in crypto:
                rates["TON_RUB"] = float(crypto["the-open-network"].get("rub", 500.0))
            if "bitcoin" in crypto:
                rates["BTC_RUB"] = float(crypto["bitcoin"].get("rub", 6000000.0))
            if "ethereum" in crypto:
                rates["ETH_RUB"] = float(crypto["ethereum"].get("rub", 240000.0))

        await self._cache.set("rates_dict", rates, ttl=600)
        return rates

    @staticmethod
    def _calc_moon_phase() -> tuple[str, str]:
        now = datetime.now(timezone.utc)
        diff = now - datetime(2001, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        days = diff.total_seconds() / 86400.0
        cycle = 29.53058867
        phase = (days % cycle) / cycle
        if phase < 0.06 or phase > 0.94:
            return "New Moon", "🌑"
        if phase < 0.24:
            return "Waxing Crescent", "🌒"
        if phase < 0.26:
            return "First Quarter", "🌓"
        if phase < 0.44:
            return "Waxing Gibbous", "🌔"
        if phase < 0.56:
            return "Full Moon", "🌕"
        if phase < 0.74:
            return "Waning Gibbous", "🌖"
        if phase < 0.76:
            return "Last Quarter", "🌗"
        return "Waning Crescent", "🌘"

    @staticmethod
    def _calc_zodiac() -> str:
        now = datetime.now()
        day, month = now.day, now.month
        zodiacs = [
            (1, 20, "Capricorn ♑"),
            (2, 19, "Aquarius ♒"),
            (3, 20, "Pisces ♓"),
            (4, 20, "Aries ♈"),
            (5, 21, "Taurus ♉"),
            (6, 21, "Gemini ♊"),
            (7, 22, "Cancer ♋"),
            (8, 23, "Leo ♌"),
            (9, 23, "Virgo ♍"),
            (10, 23, "Libra ♎"),
            (11, 22, "Scorpio ♏"),
            (12, 21, "Sagittarius ♐"),
            (12, 31, "Capricorn ♑"),
        ]
        for m, d, name in zodiacs:
            if month == m and day <= d:
                return name
        return "Capricorn ♑"

    async def _build_placeholders(self, start_time: float) -> dict[str, str]:
        me = getattr(self._client, "feroku_me", None) or await self._client.get_me()
        user_full = None
        stars_balance = 0
        with contextlib.suppress(Exception):
            user_full = await self._client(GetFullUserRequest(me.id))
        with contextlib.suppress(Exception):
            stars_res = await self._client(GetStarsStatusRequest(peer=me))
            stars_balance = getattr(stars_res, "balance", 0)

        rates = await self._get_rates()
        usd_rub = rates.get("USD_RUB", 95.0)
        eur_rub = rates.get("EUR_RUB", 102.0)
        cny_rub = rates.get("CNY_RUB", 13.0)
        kzt_rub = rates.get("KZT_RUB", 0.19)
        usdt_rub = rates.get("USDT_RUB", usd_rub)
        ton_rub = rates.get("TON_RUB", 520.0)
        btc_rub = rates.get("BTC_RUB", 6500000.0)
        eth_rub = rates.get("ETH_RUB", 250000.0)
        star_rub = STAR_USD * usd_rub
        star_ton = (STAR_USD * usd_rub) / ton_rub if ton_rub else 0.0
        star_usdt = STAR_USD

        # System stats
        cpu_pct = psutil.cpu_percent(interval=None)
        mem = psutil.virtual_memory()
        disk = psutil.disk_usage("/")
        boot_time = psutil.boot_time()
        os_uptime = self._format_uptime(time.time() - boot_time)

        net_io = psutil.net_io_counters()
        sent_gb = round(net_io.bytes_sent / (1024**3), 2)
        recv_gb = round(net_io.bytes_recv / (1024**3), 2)

        local_ip = "127.0.0.1"
        with contextlib.suppress(Exception):
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            local_ip = s.getsockname()[0]
            s.close()

        battery = self.strings["no_val"]
        with contextlib.suppress(Exception):
            if ps_bat := psutil.sensors_battery():
                plugged = " (AC)" if ps_bat.power_plugged else ""
                battery = f"{round(ps_bat.percent)}%{plugged}"

        # Git status
        git_branch = getattr(version, "branch", "main")
        git_status = self.strings["clean"]
        with contextlib.suppress(Exception):
            import git
            with git.Repo() as repo:
                git_branch = repo.active_branch.name
                changed = len(repo.index.diff(None)) + len(repo.untracked_files)
                if changed > 0:
                    git_status = f"{changed} modified"

        # Time & Dates
        now = datetime.now()
        moon_name, moon_icon = self._calc_moon_phase()

        # Weather lookup
        city_param = quote(str(self.config["city"]).strip()) if self.config["city"] else ""
        weather_data = await self._fetch_json(f"https://wttr.in/{city_param}?format=j1", timeout=3)
        w_condition = self.strings["no_val"]
        w_temp = self.strings["no_val"]
        w_feels = self.strings["no_val"]
        w_humidity = self.strings["no_val"]
        w_pressure = self.strings["no_val"]
        w_wind = self.strings["no_val"]
        if isinstance(weather_data, dict) and "current_condition" in weather_data:
            cond = weather_data["current_condition"][0]
            w_condition = cond.get("weatherDesc", [{}])[0].get("value", self.strings["no_val"])
            w_temp = f"{cond.get('temp_C', '0')}°C"
            w_feels = f"{cond.get('FeelsLikeC', '0')}°C"
            w_humidity = f"{cond.get('humidity', '0')}%"
            w_pressure = f"{round(float(cond.get('pressure', 1013)) * 0.750062)} mmHg"
            w_wind = f"{cond.get('windspeedKmph', '0')} km/h"

        # Quotes and Facts
        fact_data = await self._fetch_json("https://uselessfacts.jsph.pl/random.json?language=en", timeout=3)
        random_fact = fact_data.get("text", self.strings["no_val"]) if isinstance(fact_data, dict) else self.strings["no_val"]

        quote_data = await self._fetch_json("https://api.quotable.io/random", timeout=3)
        rand_quote = (
            f"«{quote_data.get('content')}» — {quote_data.get('author')}"
            if isinstance(quote_data, dict) and "content" in quote_data
            else self.strings["no_val"]
        )

        cat_data = await self._fetch_json("https://meowfacts.herokuapp.com/", timeout=3)
        cat_fact = cat_data.get("data", [self.strings["no_val"]])[0] if isinstance(cat_data, dict) else self.strings["no_val"]

        kanye_data = await self._fetch_json("https://api.kanye.rest/", timeout=3)
        kanye_quote = kanye_data.get("quote", self.strings["no_val"]) if isinstance(kanye_data, dict) else self.strings["no_val"]

        advice_data = await self._fetch_json("https://api.adviceslip.com/advice", timeout=3)
        advice = advice_data.get("slip", {}).get("advice", self.strings["no_val"]) if isinstance(advice_data, dict) else self.strings["no_val"]

        public_ip = (await self._fetch_text("https://api.ipify.org", timeout=3)) or self.strings["no_val"]

        # GitHub stats
        gh_user = str(self.config["github_user"]).strip()
        gh_repos = self.strings["no_val"]
        gh_followers = self.strings["no_val"]
        if gh_user:
            gh_data = await self._fetch_json(f"https://api.github.com/users/{quote(gh_user)}", timeout=3)
            if isinstance(gh_data, dict):
                gh_repos = str(gh_data.get("public_repos", self.strings["no_val"]))
                gh_followers = str(gh_data.get("followers", self.strings["no_val"]))

        # Build mapping
        bio = getattr(getattr(user_full, "full_user", None), "about", "") or self.strings["no_bio"]
        premium_str = self.strings["no_premium"]
        if getattr(me, "premium", False):
            premium_str = "Premium"

        ping_ms = round((time.monotonic() - start_time) * 1000)

        return {
            # Profile
            "username": f"@{me.username}" if me.username else self.strings["no_val"],
            "name": utils.escape_html(me.first_name or ""),
            "surname": utils.escape_html(me.last_name or ""),
            "bio_description": utils.escape_html(bio),
            "bio": utils.escape_html(bio),
            "user_id": str(me.id),
            "id": str(me.id),
            "phone_number": str(getattr(me, "phone", "") or self.strings["no_val"]),
            "phone": str(getattr(me, "phone", "") or self.strings["no_val"]),
            "dc_id": str(getattr(me, "photo", None) and getattr(me.photo, "dc_id", 2) or 2),
            "amount_stars": str(stars_balance),
            "stars": str(stars_balance),
            "premium_check": premium_str,
            "premium": premium_str,
            "me": f'<b><a href="tg://user?id={me.id}">{utils.escape_html(get_display_name(me))}</a></b>',

            # Currency / Crypto
            "dollars_in_rub": f"{usd_rub:.2f} ₽",
            "rub_in_dollars": f"{1 / usd_rub:.4f} $" if usd_rub else "0",
            "euros_in_rub": f"{eur_rub:.2f} ₽",
            "rub_in_euros": f"{1 / eur_rub:.4f} €" if eur_rub else "0",
            "yuan_in_rub": f"{cny_rub:.2f} ₽",
            "tenge_in_rub": f"{kzt_rub:.4f} ₽",
            "rub_in_tenge": f"{1 / kzt_rub:.2f} ₸" if kzt_rub else "0",
            "usdt_in_rub": f"{usdt_rub:.2f} ₽",
            "rub_in_usdt": f"{1 / usdt_rub:.4f} USDT" if usdt_rub else "0",
            "ton_in_rub": f"{ton_rub:.2f} ₽",
            "rub_in_ton": f"{1 / ton_rub:.4f} TON" if ton_rub else "0",
            "btc_in_rub": f"{btc_rub:,.0f} ₽",
            "eth_in_rub": f"{eth_rub:,.0f} ₽",
            "stars_in_rub": f"{star_rub:.2f} ₽",
            "stars_in_ton": f"{star_ton:.5f} TON",
            "stars_in_usdt": f"{star_usdt:.4f} $",

            # System
            "host": socket.gethostname(),
            "user": getpass.getuser(),
            "kernel": platform.release(),
            "os": self._os_pretty_name(),
            "os_uptime": os_uptime,
            "internet_usage": f"↑ {sent_gb} GB │ ↓ {recv_gb} GB",
            "speedtest": self.strings["no_val"],
            "shell": Path(os.environ.get("SHELL", "/bin/sh")).name,
            "gpu": self.strings["no_val"],
            "disk": f"{disk.used // (1024**3)} / {disk.total // (1024**3)} GB ({disk.percent}%)",
            "local_ip": local_ip,
            "public_ip": public_ip,
            "ip_info": public_ip,
            "user_and_hostname": f"{getpass.getuser()}@{socket.gethostname()}",
            "battery": battery,
            "cpu_usage": str(round(cpu_pct, 1)),
            "ram_usage": f"{round(mem.used / (1024**3), 1)} / {round(mem.total / (1024**3), 1)} GB ({round(mem.percent)}%)",

            # Software & Git
            "py_ver": sys.version.split()[0],
            "python_version": sys.version.split()[0],
            "htl_ver": telethon.__version__,
            "telethon_version": telethon.__version__,
            "layer": str(getattr(telethon.tl.alltlobjects, "LAYER", 199)),
            "ver": ".".join(map(str, getattr(version, "__version__", (3, 0, 0)))),
            "version": ".".join(map(str, getattr(version, "__version__", (3, 0, 0)))),
            "build": utils.get_commit_url(),
            "branch": git_branch,
            "git_branch": git_branch,
            "git_status": git_status,
            "pfx": utils.escape_html(self.get_prefix()),
            "prefix": utils.escape_html(self.get_prefix()),
            "upd": self.strings["up_to_date"],
            "update_status": self.strings["up_to_date"],
            "uptime": utils.formatted_uptime(),
            "ping": str(ping_ms),

            # Date & Time
            "time": now.strftime("%H:%M:%S"),
            "date": now.strftime("%d.%m.%Y"),
            "day_of_the_week": now.strftime("%A"),
            "data_and_time": now.strftime("%d.%m.%Y %H:%M"),
            "data_and_time_and_day_of_the_week": now.strftime("%d.%m.%Y %H:%M (%A)"),
            "moon_phase": f"{moon_icon} {moon_name}",
            "zodiac_sign": self._calc_zodiac(),

            # Weather
            "weather": w_condition,
            "outdoor_temperature": w_temp,
            "weather_and_temperature": f"{w_condition}, {w_temp}",
            "weather_emoji": "🌤",
            "feels_like": w_feels,
            "humidity": w_humidity,
            "pressure": w_pressure,
            "wind_speed": w_wind,

            # Personal
            "my_crypto_address": str(self.config["my_crypto_address"]) or self.strings["no_val"],
            "my_card_number": str(self.config["my_card_number"]) or self.strings["no_val"],
            "my_donate_site": str(self.config["my_donate_site"]) or self.strings["no_val"],
            "my_channel": str(self.config["my_channel"]) or self.strings["no_val"],
            "my_social_network": str(self.config["my_social_network"]) or self.strings["no_val"],

            # Music & GitHub & Fun
            "now_playing": self.strings["no_val"],
            "last_fm_user_and_now_playing": self.strings["no_val"],
            "song_name": self.strings["no_val"],
            "song_artist": self.strings["no_val"],
            "last_fm_user": str(self.config["lastfm_user"]) or self.strings["no_val"],
            "lastfm_stats": self.strings["no_val"],
            "github_repos": gh_repos,
            "github_followers": gh_followers,
            "random_fact": random_fact,
            "quote": rand_quote,
            "joke": random_fact,
            "advice": advice,
            "cat_fact": cat_fact,
            "kanye_quote": kanye_quote,
            "affirmation": advice,
        }

    def _render_template(self, template: str, mapping: dict[str, str]) -> str:
        def repl(match):
            key = match.group(1)
            return mapping.get(key, f"{{{key}}}")

        return re.sub(r"\{([a-zA-Z0-9_]+)\}", repl, template)

    @loader.command()
    async def info(self, message: Message):
        """Show Feroku runtime, status bar and placeholders."""
        start = time.monotonic()
        banner = str(self.config["banner_url"]).strip() or FEROKU_IMAGE_URL
        rich = bool(self.config["rich_mode"])

        placeholders = await self._build_placeholders(start)

        custom = self.config["custom_message"]
        if custom:
            rendered = self._render_template(str(custom), placeholders)
            if rich:
                await utils.answer_with_media_fallback(
                    message,
                    rich_message=f'<figure><img src="{utils.escape_html(banner)}"/></figure><p>{rendered}</p>',
                )
                return
            await utils.answer_with_media_fallback(
                message,
                rendered,
                photo=None if self.config["quote_media"] else banner,
                quote=bool(self.config["quote_media"]),
                invert_media=bool(self.config["invert_media"]),
            )
            return

        if rich:
            rendered = self._render_template(
                self.strings["rich_status_bar"].format(
                    img=f'<figure><img src="{utils.escape_html(banner)}"/></figure>' if banner else "",
                    **placeholders,
                ),
                placeholders,
            )
            await utils.answer_with_media_fallback(message, rich_message=rendered)
            return

        rendered = self._render_template(self.strings["default_status_bar"], placeholders)
        await utils.answer_with_media_fallback(
            message,
            rendered,
            photo=None if self.config["quote_media"] else banner,
            quote=bool(self.config["quote_media"]),
            invert_media=bool(self.config["invert_media"]),
        )
