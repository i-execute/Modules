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
from telethon.tl.types import InputMediaWebPage, Message
from telethon.utils import get_display_name

from .. import loader, utils, version
from ..assets import FEROKU_IMAGE_URL

logger = logging.getLogger(__name__)

STAR_USD = 0.022


class AsyncTTLCache:
    """Thread-safe and async-friendly TTL cache with max entries limit."""

    def __init__(self, max_size: int = 150, default_ttl: int = 300):
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
    """Placeholders: profile, time, weather, exchange rates, crypto, system, facts and quotes — for bio, name and status"""

    strings = {
        "name": "FInfo",
        "_cls_doc": "Placeholders: profile, time, weather, exchange rates, crypto, system, facts and quotes — for bio, name and status",
        "_cmd_doc_info": "Show Feroku runtime, status bar and placeholders.",
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
        "banner_url_doc": "Banner image URL used for link preview and Rich views.",
        "quote_media_doc": "Switch Info preview media to quote.",
        "invert_media_doc": "Invert preview media position.",
        "rich_mode_doc": "Render Info as a Rich message.",
        "city_doc": "City for weather placeholder (e.g. 'Berlin', 'Moscow').",
        "github_user_doc": "Default GitHub username for repos and followers placeholders.",
        "no_val": "N/A",
        "no_bio": "No bio",
        "no_premium": "No Premium",
        "up_to_date": "Up-to-date",
        "clean": "clean",
    }

    strings_ru = {
        "_cls_doc": "Плейсхолдеры: профиль, время, погода, курсы валют, крипта, система, факты и цитаты — для био, имени и статуса",
        "_cmd_doc_info": "Показать статус-бар или системную информацию Feroku.",
        "custom_message_doc": "Пользовательский шаблон. Оставьте пустым для стандартного.",
        "banner_url_doc": "Ссылка на баннер для превью.",
        "city_doc": "Город для погоды.",
        "github_user_doc": "Имя пользователя GitHub.",
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
        self._full_me_cache = None

    async def client_ready(self, client=None, db=None):
        self._register_all_placeholders()

    def on_dlmod(self):
        self._register_all_placeholders()

    async def on_unload(self):
        utils.unregister_placeholders(self.__class__.__name__)

    def _placeholder_definitions(self):
        return [
            ("username", self.get_username, "Username"),
            ("name", self.get_name, "First name"),
            ("surname", self.get_surname, "Last name"),
            ("bio_description", self.get_bio_description, "Bio"),
            ("bio", self.get_bio_description, "Bio"),
            ("user_id", self.get_user_id, "Account ID"),
            ("phone_number", self.get_phone_number, "Phone"),
            ("dc_id", self.get_dc_id, "DC ID"),
            ("amount_stars", self.get_amount_stars, "Stars balance"),
            ("premium_check", self.get_premium_check, "Premium expiry date"),
            ("me", self.get_me_link, "Formatted profile link"),
            ("dollars_in_rub", self.get_dollars_in_rub, "USD → RUB"),
            ("rub_in_dollars", self.get_rub_in_dollars, "RUB → USD"),
            ("euros_in_rub", self.get_euros_in_rub, "EUR → RUB"),
            ("rub_in_euros", self.get_rub_in_euros, "RUB → EUR"),
            ("yuan_in_rub", self.get_yuan_in_rub, "CNY → RUB"),
            ("tenge_in_rub", self.get_tenge_in_rub, "KZT → RUB"),
            ("rub_in_tenge", self.get_rub_in_tenge, "RUB → KZT"),
            ("usdt_in_rub", self.get_usdt_in_rub, "USDT → RUB"),
            ("rub_in_usdt", self.get_rub_in_usdt, "RUB → USDT"),
            ("ton_in_rub", self.get_ton_in_rub, "TON → RUB"),
            ("rub_in_ton", self.get_rub_in_ton, "RUB → TON"),
            ("btc_in_rub", self.get_btc_in_rub, "BTC → RUB"),
            ("eth_in_rub", self.get_eth_in_rub, "ETH → RUB"),
            ("stars_in_rub", self.get_stars_in_rub, "Star → RUB"),
            ("stars_in_ton", self.get_stars_in_ton, "Star → TON"),
            ("stars_in_usdt", self.get_stars_in_usdt, "Star → USDT"),
            ("host", self.get_host, "Hostname"),
            ("user", self.get_user, "System user"),
            ("kernel", self.get_kernel, "Kernel release"),
            ("os", self.get_os, "Operating system"),
            ("os_uptime", self.get_os_uptime, "System uptime"),
            ("internet_usage", self.get_internet_usage, "Traffic stats"),
            ("speedtest", self.get_speedtest, "Internet speed"),
            ("shell", self.get_shell, "Shell"),
            ("gpu", self.get_gpu, "GPU"),
            ("disk", self.get_disk, "Disk usage"),
            ("local_ip", self.get_local_ip, "Local IP"),
            ("public_ip", self.get_public_ip, "Public IP"),
            ("ip_info", self.get_ip_info, "IP geolocation"),
            ("user_and_hostname", self.get_user_and_hostname, "user@hostname"),
            ("battery", self.get_battery, "Battery level"),
            ("cpu_usage", self.get_cpu_usage, "CPU load"),
            ("ram_usage", self.get_ram_usage, "RAM usage"),
            ("py_ver", self.get_py_ver, "Python version"),
            ("python_version", self.get_py_ver, "Python version"),
            ("htl_ver", self.get_htl_ver, "Telethon version"),
            ("telethon_version", self.get_htl_ver, "Telethon version"),
            ("layer", self.get_layer, "Telethon layer"),
            ("ver", self.get_ver, "Feroku version"),
            ("version", self.get_ver, "Feroku version"),
            ("build", self.get_build, "Commit URL"),
            ("git_branch", self.get_git_branch, "Git branch"),
            ("branch", self.get_git_branch, "Git branch"),
            ("git_status", self.get_git_status, "Git status"),
            ("pfx", self.get_pfx, "Command prefix"),
            ("prefix", self.get_pfx, "Command prefix"),
            ("upd", self.get_upd, "Update status"),
            ("uptime", self.get_uptime, "Feroku process uptime"),
            ("ping", self.get_ping, "Response latency in ms"),
            ("time", self.get_time, "Time"),
            ("date", self.get_date, "Date"),
            ("day_of_the_week", self.get_day_of_the_week, "Day of week"),
            ("data_and_time", self.get_data_and_time, "Date & time"),
            ("data_and_time_and_day_of_the_week", self.get_data_and_time_and_day_of_the_week, "Date, time, weekday"),
            ("moon_phase", self.get_moon_phase, "Moon phase"),
            ("zodiac_sign", self.get_zodiac_sign, "Zodiac sign"),
            ("weather", self.get_weather, "Weather"),
            ("outdoor_temperature", self.get_outdoor_temperature, "Temperature"),
            ("weather_and_temperature", self.get_weather_and_temperature, "Weather & temperature"),
            ("weather_emoji", self.get_weather_emoji, "Weather emoji"),
            ("feels_like", self.get_feels_like, "Feels like"),
            ("humidity", self.get_humidity, "Humidity"),
            ("pressure", self.get_pressure, "Pressure"),
            ("wind_speed", self.get_wind_speed, "Wind speed"),
            ("my_crypto_address", self.get_my_crypto_address, "Crypto address"),
            ("my_card_number", self.get_my_card_number, "Card number"),
            ("my_donate_site", self.get_my_donate_site, "Donate link"),
            ("my_channel", self.get_my_channel, "Channel"),
            ("my_social_network", self.get_my_social_network, "Social link"),
            ("github_repos", self.get_github_repos, "Public GitHub repos"),
            ("github_followers", self.get_github_followers, "GitHub followers"),
            ("random_fact", self.get_random_fact, "Random fact"),
            ("quote", self.get_quote, "Random quote"),
            ("joke", self.get_joke, "Random joke"),
            ("advice", self.get_advice, "Random advice"),
            ("cat_fact", self.get_cat_fact, "Cat fact"),
            ("kanye_quote", self.get_kanye_quote, "Kanye West quote"),
            ("affirmation", self.get_affirmation, "Affirmation"),
        ]

    def _register_all_placeholders(self):
        for name, callback, description in self._placeholder_definitions():
            utils.register_placeholder(name, callback, description)

    async def _fetch_json(self, url: str, timeout: int = 4) -> dict | list | None:
        cached = await self._cache.get(url)
        if cached is not None:
            return cached
        try:
            to = aiohttp.ClientTimeout(total=timeout)
            async with aiohttp.ClientSession(timeout=to) as session:
                async with session.get(url, headers={"User-Agent": "Feroku/3.0"}) as resp:
                    if resp.status == 200:
                        data = await resp.json(content_type=None)
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

    async def _get_full_me(self):
        if self._full_me_cache:
            return self._full_me_cache
        try:
            me = getattr(self._client, "feroku_me", None) or await self._client.get_me()
            self._full_me_cache = await self._client(GetFullUserRequest(me.id))
        except Exception:
            pass
        return self._full_me_cache

    async def get_username(self, data=None) -> str:
        me = getattr(self._client, "feroku_me", None) or await self._client.get_me()
        return f"@{me.username}" if me and me.username else self.strings["no_val"]

    async def get_name(self, data=None) -> str:
        me = getattr(self._client, "feroku_me", None) or await self._client.get_me()
        return utils.escape_html(me.first_name or "") if me else self.strings["no_val"]

    async def get_surname(self, data=None) -> str:
        me = getattr(self._client, "feroku_me", None) or await self._client.get_me()
        return utils.escape_html(me.last_name or "") if me else ""

    async def get_bio_description(self, data=None) -> str:
        full = await self._get_full_me()
        bio = getattr(getattr(full, "full_user", None), "about", "") or self.strings["no_bio"]
        return utils.escape_html(bio)

    async def get_user_id(self, data=None) -> str:
        me = getattr(self._client, "feroku_me", None) or await self._client.get_me()
        return str(me.id) if me else self.strings["no_val"]

    async def get_phone_number(self, data=None) -> str:
        me = getattr(self._client, "feroku_me", None) or await self._client.get_me()
        phone = getattr(me, "phone", None)
        return str(phone) if phone else self.strings["no_val"]

    async def get_dc_id(self, data=None) -> str:
        try:
            return str(self._client.session.dc_id)
        except Exception:
            return "2"

    async def get_amount_stars(self, data=None) -> str:
        try:
            res = await self._client(GetStarsStatusRequest(peer="me"))
            return str(getattr(res, "balance", 0))
        except Exception:
            return "0"

    async def get_premium_check(self, data=None) -> str:
        me = getattr(self._client, "feroku_me", None) or await self._client.get_me()
        if not getattr(me, "premium", False):
            return self.strings["no_premium"]
        full = await self._get_full_me()
        until = getattr(getattr(full, "full_user", None), "premium_until", None)
        if until and until > time.time():
            days_left = max(0, round((until - time.time()) / 86400))
            dt = datetime.fromtimestamp(until).strftime("%d.%m.%Y")
            return f"{dt} ({days_left}d left)"
        return "Premium"

    async def get_me_link(self, data=None) -> str:
        me = getattr(self._client, "feroku_me", None) or await self._client.get_me()
        return f'<b><a href="tg://user?id={me.id}">{utils.escape_html(get_display_name(me))}</a></b>'

    async def get_dollars_in_rub(self, data=None) -> str:
        rates = await self._get_rates()
        return f"{rates.get('USD_RUB', 95.0):.2f} ₽"

    async def get_rub_in_dollars(self, data=None) -> str:
        rates = await self._get_rates()
        usd = rates.get("USD_RUB", 95.0)
        return f"{1 / usd:.4f} $" if usd else "0"

    async def get_euros_in_rub(self, data=None) -> str:
        rates = await self._get_rates()
        return f"{rates.get('EUR_RUB', 102.0):.2f} ₽"

    async def get_rub_in_euros(self, data=None) -> str:
        rates = await self._get_rates()
        eur = rates.get("EUR_RUB", 102.0)
        return f"{1 / eur:.4f} €" if eur else "0"

    async def get_yuan_in_rub(self, data=None) -> str:
        rates = await self._get_rates()
        return f"{rates.get('CNY_RUB', 13.0):.2f} ₽"

    async def get_tenge_in_rub(self, data=None) -> str:
        rates = await self._get_rates()
        return f"{rates.get('KZT_RUB', 0.19):.4f} ₽"

    async def get_rub_in_tenge(self, data=None) -> str:
        rates = await self._get_rates()
        kzt = rates.get("KZT_RUB", 0.19)
        return f"{1 / kzt:.2f} ₸" if kzt else "0"

    async def get_usdt_in_rub(self, data=None) -> str:
        rates = await self._get_rates()
        return f"{rates.get('USDT_RUB', rates.get('USD_RUB', 95.0)):.2f} ₽"

    async def get_rub_in_usdt(self, data=None) -> str:
        rates = await self._get_rates()
        usdt = rates.get("USDT_RUB", rates.get("USD_RUB", 95.0))
        return f"{1 / usdt:.4f} USDT" if usdt else "0"

    async def get_ton_in_rub(self, data=None) -> str:
        rates = await self._get_rates()
        return f"{rates.get('TON_RUB', 520.0):.2f} ₽"

    async def get_rub_in_ton(self, data=None) -> str:
        rates = await self._get_rates()
        ton = rates.get("TON_RUB", 520.0)
        return f"{1 / ton:.4f} TON" if ton else "0"

    async def get_btc_in_rub(self, data=None) -> str:
        rates = await self._get_rates()
        return f"{rates.get('BTC_RUB', 6500000.0):,.0f} ₽"

    async def get_eth_in_rub(self, data=None) -> str:
        rates = await self._get_rates()
        return f"{rates.get('ETH_RUB', 250000.0):,.0f} ₽"

    async def get_stars_in_rub(self, data=None) -> str:
        rates = await self._get_rates()
        return f"{STAR_USD * rates.get('USD_RUB', 95.0):.2f} ₽"

    async def get_stars_in_ton(self, data=None) -> str:
        rates = await self._get_rates()
        ton = rates.get("TON_RUB", 520.0)
        star_rub = STAR_USD * rates.get("USD_RUB", 95.0)
        return f"{star_rub / ton:.5f} TON" if ton else "0.006 TON"

    async def get_stars_in_usdt(self, data=None) -> str:
        return f"{STAR_USD:.4f} $"

    async def get_host(self, data=None) -> str:
        if isinstance(data, dict):
            for k in ("host", "hostname"):
                if k in data and data[k] not in (None, ""):
                    return str(data[k])
        return socket.gethostname()

    async def get_user(self, data=None) -> str:
        if isinstance(data, dict) and "user" in data and data["user"] not in (None, ""):
            return str(data["user"])
        return getpass.getuser()

    async def get_kernel(self, data=None) -> str:
        return platform.release()

    async def get_os(self, data=None) -> str:
        try:
            with open("/etc/os-release", encoding="utf-8") as f:
                for line in f:
                    if line.startswith("PRETTY_NAME="):
                        return line.split("=", 1)[1].strip().strip('"')
        except OSError:
            pass
        return platform.system() + " " + platform.release()

    async def get_os_uptime(self, data=None) -> str:
        s = int(time.time() - psutil.boot_time())
        d, s = divmod(s, 86400)
        h, s = divmod(s, 3600)
        m, _ = divmod(s, 60)
        return f"{d}d {h}h {m}m" if d else f"{h}h {m}m" if h else f"{m}m"

    async def get_internet_usage(self, data=None) -> str:
        net = psutil.net_io_counters()
        return f"↑ {round(net.bytes_sent / (1024**3), 2)} GB │ ↓ {round(net.bytes_recv / (1024**3), 2)} GB"

    async def get_speedtest(self, data=None) -> str:
        return self.strings["no_val"]

    async def get_shell(self, data=None) -> str:
        return Path(os.environ.get("SHELL", "/bin/sh")).name

    async def get_gpu(self, data=None) -> str:
        return self.strings["no_val"]

    async def get_disk(self, data=None) -> str:
        disk = psutil.disk_usage("/")
        return f"{disk.used // (1024**3)} / {disk.total // (1024**3)} GB ({disk.percent}%)"

    async def get_local_ip(self, data=None) -> str:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            s.close()
            return ip
        except Exception:
            return "127.0.0.1"

    async def get_public_ip(self, data=None) -> str:
        return (await self._fetch_text("https://api.ipify.org", timeout=3)) or self.strings["no_val"]

    async def get_ip_info(self, data=None) -> str:
        return await self.get_public_ip()

    async def get_user_and_hostname(self, data=None) -> str:
        return f"{getpass.getuser()}@{socket.gethostname()}"

    async def get_battery(self, data=None) -> str:
        try:
            if bat := psutil.sensors_battery():
                plugged = " (AC)" if bat.power_plugged else ""
                return f"{round(bat.percent)}%{plugged}"
        except Exception:
            pass
        return self.strings["no_val"]

    async def get_cpu_usage(self, data=None) -> str:
        return str(round(psutil.cpu_percent(interval=None), 1))

    async def get_ram_usage(self, data=None) -> str:
        mem = psutil.virtual_memory()
        return f"{round(mem.used / (1024**3), 1)} / {round(mem.total / (1024**3), 1)} GB ({round(mem.percent)}%)"

    async def get_py_ver(self, data=None) -> str:
        return sys.version.split()[0]

    async def get_htl_ver(self, data=None) -> str:
        return telethon.__version__

    async def get_layer(self, data=None) -> str:
        return str(getattr(telethon.tl.alltlobjects, "LAYER", 199))

    async def get_ver(self, data=None) -> str:
        return ".".join(map(str, getattr(version, "__version__", (3, 0, 0))))

    async def get_build(self, data=None) -> str:
        return utils.get_commit_url()

    async def get_git_branch(self, data=None) -> str:
        branch = getattr(version, "branch", "main")
        with contextlib.suppress(Exception):
            import git
            with git.Repo() as repo:
                branch = repo.active_branch.name
        return branch

    async def get_git_status(self, data=None) -> str:
        status = self.strings["clean"]
        with contextlib.suppress(Exception):
            import git
            with git.Repo() as repo:
                changed = len(repo.index.diff(None)) + len(repo.untracked_files)
                if changed > 0:
                    status = f"{changed} modified"
        return status

    async def get_pfx(self, data=None) -> str:
        return utils.escape_html(self.get_prefix())

    async def get_upd(self, data=None) -> str:
        return self.strings["up_to_date"]

    async def get_uptime(self, data=None) -> str:
        if isinstance(data, dict) and "uptime" in data and data["uptime"] not in (None, ""):
            return str(data["uptime"])
        return utils.formatted_uptime()

    async def get_ping(self, data=None) -> str:
        if isinstance(data, dict) and "ping" in data and data["ping"] not in (None, ""):
            return str(data["ping"])
        start = time.perf_counter()
        try:
            await self._client.get_me()
            return f"{round((time.perf_counter() - start) * 1000, 2)}"
        except Exception:
            return "1"

    async def get_time(self, data=None) -> str:
        return datetime.now().strftime("%H:%M:%S")

    async def get_date(self, data=None) -> str:
        return datetime.now().strftime("%d.%m.%Y")

    async def get_day_of_the_week(self, data=None) -> str:
        return datetime.now().strftime("%A")

    async def get_data_and_time(self, data=None) -> str:
        return datetime.now().strftime("%d.%m.%Y %H:%M")

    async def get_data_and_time_and_day_of_the_week(self, data=None) -> str:
        return datetime.now().strftime("%d.%m.%Y %H:%M (%A)")

    async def get_moon_phase(self, data=None) -> str:
        now = datetime.now(timezone.utc)
        diff = now - datetime(2001, 1, 1, tzinfo=timezone.utc)
        phase = ((diff.total_seconds() / 86400.0) % 29.53058867) / 29.53058867
        if phase < 0.06 or phase > 0.94:
            return "🌑 New Moon"
        if phase < 0.24:
            return "🌒 Waxing Crescent"
        if phase < 0.26:
            return "🌓 First Quarter"
        if phase < 0.44:
            return "🌔 Waxing Gibbous"
        if phase < 0.56:
            return "🌕 Full Moon"
        if phase < 0.74:
            return "🌖 Waning Gibbous"
        if phase < 0.76:
            return "🌗 Last Quarter"
        return "🌘 Waning Crescent"

    async def get_zodiac_sign(self, data=None) -> str:
        now = datetime.now()
        day, month = now.day, now.month
        zodiacs = [
            (1, 20, "Capricorn ♑"), (2, 19, "Aquarius ♒"), (3, 20, "Pisces ♓"),
            (4, 20, "Aries ♈"), (5, 21, "Taurus ♉"), (6, 21, "Gemini ♊"),
            (7, 22, "Cancer ♋"), (8, 23, "Leo ♌"), (9, 23, "Virgo ♍"),
            (10, 23, "Libra ♎"), (11, 22, "Scorpio ♏"), (12, 21, "Sagittarius ♐"),
            (12, 31, "Capricorn ♑"),
        ]
        for m, d, name in zodiacs:
            if month == m and day <= d:
                return name
        return "Capricorn ♑"

    async def _get_weather_data(self) -> dict | None:
        city_param = quote(str(self.config["city"]).strip()) if self.config["city"] else ""
        return await self._fetch_json(f"https://wttr.in/{city_param}?format=j1", timeout=3)

    async def get_weather(self, data=None) -> str:
        w = await self._get_weather_data()
        if isinstance(w, dict) and "current_condition" in w:
            return w["current_condition"][0].get("weatherDesc", [{}])[0].get("value", self.strings["no_val"])
        return self.strings["no_val"]

    async def get_outdoor_temperature(self, data=None) -> str:
        w = await self._get_weather_data()
        if isinstance(w, dict) and "current_condition" in w:
            return f"{w['current_condition'][0].get('temp_C', '0')}°C"
        return self.strings["no_val"]

    async def get_weather_and_temperature(self, data=None) -> str:
        cond = await self.get_weather()
        temp = await self.get_outdoor_temperature()
        return f"{cond}, {temp}" if cond != self.strings["no_val"] else self.strings["no_val"]

    async def get_weather_emoji(self, data=None) -> str:
        cond = (await self.get_weather()).lower()
        if any(k in cond for k in ("thunder", "гроза")):
            return "⛈️"
        if any(k in cond for k in ("rain", "drizzle", "shower", "дожд")):
            return "🌧️"
        if any(k in cond for k in ("snow", "снег")):
            return "❄️"
        if any(k in cond for k in ("fog", "mist", "туман")):
            return "🌫️"
        if any(k in cond for k in ("cloud", "overcast", "облач")):
            return "☁️"
        if any(k in cond for k in ("sun", "clear", "ясно")):
            return "☀️"
        return "🌤"

    async def get_feels_like(self, data=None) -> str:
        w = await self._get_weather_data()
        if isinstance(w, dict) and "current_condition" in w:
            return f"{w['current_condition'][0].get('FeelsLikeC', '0')}°C"
        return self.strings["no_val"]

    async def get_humidity(self, data=None) -> str:
        w = await self._get_weather_data()
        if isinstance(w, dict) and "current_condition" in w:
            return f"{w['current_condition'][0].get('humidity', '0')}%"
        return self.strings["no_val"]

    async def get_pressure(self, data=None) -> str:
        w = await self._get_weather_data()
        if isinstance(w, dict) and "current_condition" in w:
            p = float(w["current_condition"][0].get("pressure", 1013))
            return f"{round(p * 0.750062)} mmHg"
        return self.strings["no_val"]

    async def get_wind_speed(self, data=None) -> str:
        w = await self._get_weather_data()
        if isinstance(w, dict) and "current_condition" in w:
            return f"{w['current_condition'][0].get('windspeedKmph', '0')} km/h"
        return self.strings["no_val"]

    async def get_my_crypto_address(self, data=None) -> str:
        return str(self.config["my_crypto_address"]) or self.strings["no_val"]

    async def get_my_card_number(self, data=None) -> str:
        return str(self.config["my_card_number"]) or self.strings["no_val"]

    async def get_my_donate_site(self, data=None) -> str:
        return str(self.config["my_donate_site"]) or self.strings["no_val"]

    async def get_my_channel(self, data=None) -> str:
        return str(self.config["my_channel"]) or self.strings["no_val"]

    async def get_my_social_network(self, data=None) -> str:
        return str(self.config["my_social_network"]) or self.strings["no_val"]

    async def get_github_repos(self, data=None) -> str:
        user = str(self.config["github_user"]).strip()
        if user:
            gh = await self._fetch_json(f"https://api.github.com/users/{quote(user)}", timeout=3)
            if isinstance(gh, dict):
                return str(gh.get("public_repos", self.strings["no_val"]))
        return self.strings["no_val"]

    async def get_github_followers(self, data=None) -> str:
        user = str(self.config["github_user"]).strip()
        if user:
            gh = await self._fetch_json(f"https://api.github.com/users/{quote(user)}", timeout=3)
            if isinstance(gh, dict):
                return str(gh.get("followers", self.strings["no_val"]))
        return self.strings["no_val"]

    async def get_random_fact(self, data=None) -> str:
        fact = await self._fetch_json("https://uselessfacts.jsph.pl/random.json?language=en", timeout=3)
        return fact.get("text", self.strings["no_val"]) if isinstance(fact, dict) else self.strings["no_val"]

    async def get_quote(self, data=None) -> str:
        q = await self._fetch_json("https://api.quotable.io/random", timeout=3)
        return f"«{q.get('content')}» — {q.get('author')}" if isinstance(q, dict) and "content" in q else self.strings["no_val"]

    async def get_joke(self, data=None) -> str:
        return await self.get_random_fact()

    async def get_advice(self, data=None) -> str:
        adv = await self._fetch_json("https://api.adviceslip.com/advice", timeout=3)
        return adv.get("slip", {}).get("advice", self.strings["no_val"]) if isinstance(adv, dict) else self.strings["no_val"]

    async def get_cat_fact(self, data=None) -> str:
        cat = await self._fetch_json("https://meowfacts.herokuapp.com/", timeout=3)
        return cat.get("data", [self.strings["no_val"]])[0] if isinstance(cat, dict) else self.strings["no_val"]

    async def get_kanye_quote(self, data=None) -> str:
        kanye = await self._fetch_json("https://api.kanye.rest/", timeout=3)
        return kanye.get("quote", self.strings["no_val"]) if isinstance(kanye, dict) else self.strings["no_val"]

    async def get_affirmation(self, data=None) -> str:
        return await self.get_advice()

    @loader.command()
    async def info(self, message: Message):
        """Show Feroku runtime, status bar and placeholders."""
        start = time.perf_counter()
        banner_raw = str(self.config["banner_url"]).strip() if self.config["banner_url"] else ""
        rich = bool(self.config["rich_mode"])

        custom = self.config["custom_message"]
        template = str(custom) if custom else (self.strings["rich_status_bar"] if rich else self.strings["default_status_bar"])

        data = {
            "ping": round((time.perf_counter() - start) * 1000, 2),
            "uptime": utils.formatted_uptime(),
        }
        data = await utils.get_placeholders(data, template)
        data["ping"] = str(round((time.perf_counter() - start) * 1000, 2))
        if "{img}" in template:
            data["img"] = f'<figure><img src="{utils.escape_html(banner_raw)}"/></figure>' if banner_raw else ""

        def repl(match):
            key = match.group(1)
            return str(data.get(key, f"{{{key}}}"))

        rendered = re.sub(r"\{([a-zA-Z0-9_]+)\}", repl, template)

        if rich:
            await utils.answer_with_media_fallback(
                message,
                rich_message=rendered.replace("\n", "<br>"),
                reply_to=getattr(message, "reply_to_msg_id", None),
            )
            return

        preview_media = (
            InputMediaWebPage(
                url=banner_raw,
                optional=True,
                force_large_media=True,
            )
            if banner_raw
            else None
        )

        await utils.answer_with_media_fallback(
            message,
            rendered,
            file=preview_media,
            reply_to=getattr(message, "reply_to_msg_id", None),
            invert_media=bool(self.config["invert_media"]),
            link_preview=True,
        )
