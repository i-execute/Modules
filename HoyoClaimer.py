# CopyLeft 2026 github.com/i-execute
# Author: I_execute.t.me
# Licensed under AGPLv3.

__version__ = (1, 2, 0)
# meta developer: Execute_forge.t.me

import asyncio
import importlib
import json
import logging
import random
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from datetime import time as dtime

import aiohttp

from telethon.tl.functions.messages import EditForumTopicRequest, EditMessageRequest
from telethon.tl.types import InputMediaWebPage

from .. import loader, utils
from ..inline.types import InlineCall

logger = logging.getLogger(__name__)

CN_TZ = timezone(timedelta(hours=8))
RELOAD_BANNER = "https://raw.githubusercontent.com/i-execute/Modules/main/Storage/HoyoClaimer/Reloading.jpeg"
TOPIC_TITLE = "HoyoClaimer"
TOPIC_ICON = 5379673776369742437
HOYO_DOMAINS = ("hoyolab.com", "hoyoverse.com", "mihoyo.com", "miyoushe.com")
CN_DOMAINS = ("mihoyo.com", "miyoushe.com")
AUTH_UID = ("ltuid_v2", "ltuid", "account_id_v2", "account_id", "stuid", "login_uid")
AUTH_TOKEN = ("ltoken_v2", "ltoken", "cookie_token_v2", "cookie_token", "stoken", "login_ticket")
COOKIE_ATTRS = {"path", "domain", "expires", "max-age", "samesite", "secure", "httponly", "priority", "partitioned"}
TF = {"TRUE", "FALSE"}
GAMES = ("genshin", "hkrpg", "honkai3rd", "nap", "tot")
GAME_NAMES = {
    "genshin": "Genshin Impact",
    "hkrpg": "Honkai: Star Rail",
    "honkai3rd": "Honkai Impact 3rd",
    "nap": "Zenless Zone Zero",
    "tot": "Tears of Themis",
}
LANGS = [
    "ru-ru", "en-us", "zh-cn", "zh-tw", "de-de", "es-es", "fr-fr", "id-id",
    "it-it", "ja-jp", "ko-kr", "pt-pt", "th-th", "vi-vn", "tr-tr",
]
MAX_FILE = 2 * 1024 * 1024


def _esc(text):
    return utils.escape_html(str(text if text is not None else ""))


def _domain_ok(domain):
    domain = (domain or "").lower().lstrip(".")
    return not domain or any(domain == d or domain.endswith("." + d) for d in HOYO_DOMAINS)


def _add(found, name, value, domain=""):
    name = str(name or "").strip().strip("\"'")
    if not name or not re.fullmatch(r"[\w\-.$]+", name) or name.lower() in COOKIE_ATTRS:
        return
    found.append((name, str(value if value is not None else "").strip().strip("\"'"), str(domain or "")))


def _parse_header(text, found):
    text = re.sub(r"(?im)^\s*(?:set-)?cookie\s*:\s*", "", text)
    for chunk in re.split(r"[;\n\r]+", text):
        if "=" in chunk:
            name, value = chunk.split("=", 1)
            _add(found, name, value)


def _walk_json(data, found):
    if isinstance(data, list):
        for item in data:
            _walk_json(item, found)
        return
    if isinstance(data, str):
        if "=" in data:
            _parse_header(data, found)
        return
    if not isinstance(data, dict):
        return
    if "name" in data and "value" in data and not isinstance(data["value"], (dict, list)):
        _add(found, data["name"], data["value"], data.get("domain") or data.get("host") or "")
        return
    for key, value in data.items():
        if isinstance(value, (dict, list)):
            _walk_json(value, found)
        elif isinstance(value, str) and key.lower() in ("cookie", "cookies", "header", "cookie_header"):
            _parse_header(value, found)
        elif isinstance(value, (str, int)) and not isinstance(value, bool):
            _add(found, key, value)


def _parse_netscape(text, found):
    tokens = []
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("#HttpOnly_"):
            line = line[10:]
        elif line.startswith("#"):
            continue
        tokens.extend(line.split())
    i = 0
    while i + 6 < len(tokens):
        t = tokens
        if t[i + 1].upper() in TF and t[i + 3].upper() in TF and re.fullmatch(r"-?\d+(?:\.\d+)?", t[i + 4]):
            _add(found, t[i + 5], t[i + 6], t[i].replace("#HttpOnly_", ""))
            i += 7
        else:
            i += 1


def parse_cookies(raw):
    text = (raw or "").strip().lstrip("\ufeff")
    found, kind = [], None
    if text[:1] in "[{":
        try:
            _walk_json(json.loads(text), found)
            kind = "JSON"
        except ValueError:
            found = []
    if not found:
        _parse_netscape(text, found)
        kind = "Netscape" if found else None
    if not found:
        _parse_header(text, found)
        kind = "Header" if found else None
    cookies, domains = {}, set()
    for name, value, domain in found:
        if not _domain_ok(domain):
            continue
        cookies[name] = value
        if domain:
            domains.add(domain.lower().lstrip("."))
    return cookies, domains, kind


def _auth_uid(cookies):
    for key in AUTH_UID:
        value = cookies.get(key)
        if value and str(value).isdigit():
            return str(value)
    return None


def _has_token(cookies):
    return any(cookies.get(key) for key in AUTH_TOKEN)


EXPIRY_KEYS = ("expirationDate", "expires", "expiry", "expiresDate", "expires_at")


def _ts(value):
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        ts = float(value)
    else:
        text = str(value).strip()
        if not text:
            return None
        try:
            ts = float(text)
        except ValueError:
            try:
                dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                ts = dt.timestamp()
            except ValueError:
                return None
    if ts > 1e11:
        ts /= 1000
    return ts if ts > 1e9 else None


def _walk_expiry(data, out):
    if isinstance(data, list):
        for item in data:
            _walk_expiry(item, out)
        return
    if not isinstance(data, dict):
        return
    if "name" in data and "value" in data:
        for key in EXPIRY_KEYS:
            ts = _ts(data.get(key))
            if ts:
                out[str(data["name"]).strip()] = ts
                break
        return
    for value in data.values():
        if isinstance(value, (dict, list)):
            _walk_expiry(value, out)


def parse_expiry(raw, cookies):
    """Earliest expiry (unix time) among auth token cookies, or None if the export has no dates."""
    text = (raw or "").strip().lstrip("\ufeff")
    out = {}
    if text[:1] in "[{":
        try:
            _walk_expiry(json.loads(text), out)
        except ValueError:
            out = {}
    if not out:
        tokens = []
        for line in text.splitlines():
            line = line.strip()
            if line.startswith("#HttpOnly_"):
                line = line[10:]
            elif line.startswith("#"):
                continue
            tokens.extend(line.split())
        i = 0
        while i + 6 < len(tokens):
            t = tokens
            if t[i + 1].upper() in TF and t[i + 3].upper() in TF and re.fullmatch(r"-?\d+(?:\.\d+)?", t[i + 4]):
                ts = _ts(t[i + 4])
                if ts:
                    out[t[i + 5]] = ts
                i += 7
            else:
                i += 1
    stamps = [out[name] for name in AUTH_TOKEN if name in cookies and name in out]
    return int(min(stamps)) if stamps else None


def _fmt_left(seconds):
    seconds = max(int(seconds), 0)
    hours, rest = divmod(seconds, 3600)
    minutes = rest // 60
    if hours:
        return f"{hours}h {minutes}m"
    return f"{minutes}m"


@loader.tds
class HoyoClaimer(loader.Module):
    """Automatic HoYoLab daily check-in rewards claimer via cookies"""

    strings = {
        "name": "HoyoClaimer",

        "menu": (
            "<b>HoyoClaimer</b>\n"
            "<blockquote><b>Accounts:</b> {count}\n"
            "<b>Auto claim:</b> {auto}\n"
            "<b>Time:</b> <code>{time}</code> ({tz}){delay}\n"
            "<b>Next claim:</b> {next}</blockquote>\n"
            "{accounts}"
        ),
        "menu_empty": "<blockquote>No accounts. Reply <code>{prefix}hc</code> to a txt/json file or a message with HoYoLab cookies.</blockquote>",
        "menu_account": "<blockquote expandable><b>{nick}</b> | <code>{hid}</code> | {region}\n{games}</blockquote>",
        "delay": ", random delay up to {minutes} min",
        "next_in": "<code>{date}</code> (in {left})",
        "next_now": "soon",
        "next_off": "disabled",
        "auto_on": "on",
        "auto_off": "off",
        "region_os": "Global",
        "region_cn": "China",

        "account": (
            "<b>HoyoClaimer</b>\n"
            "<blockquote><b>{nick}</b> | HoYoLab ID <code>{hid}</code> | {region}\n"
            "<b>Added:</b> {added}\n"
            "<b>Cookies until:</b> {until}</blockquote>\n"
            "<blockquote expandable>{games}</blockquote>"
        ),
        "game_line": "<b>{game}</b>: {nick} | UID <code>{uid}</code> | {server} | Lv {level}\n{status}",
        "no_games": "No game accounts found, all games from config will be tried",
        "st_none": "Today: not claimed yet",
        "st_ok": "Today: claimed {amount}x {reward} (day {days})",
        "st_already": "Today: already claimed{reward}",
        "st_already_reward": ", {amount}x {reward} (day {days})",
        "st_captcha": "Captcha required, claim manually on HoYoLab",
        "st_cookies": "Cookies expired, update them",
        "st_nogame": "No game account bound",
        "st_error": "Error: {error}",
        "st_old": "Last: {date}, {status}",

        "info_title": "<b>HoyoClaimer | Status</b>\n<blockquote><b>{nick}</b></blockquote>\n<blockquote expandable>{lines}</blockquote>",
        "info_line": "<b>{game}</b>: today {signed}, {days} days this month, missed {missed}",
        "info_err": "<b>{game}</b>: {error}",
        "yes": "claimed",
        "no": "not claimed",

        "history_title": "<b>HoyoClaimer | History</b>\n<blockquote><b>{nick}</b></blockquote>\n{blocks}",
        "history_block": "<blockquote expandable><b>{game}</b>\n{lines}</blockquote>",
        "history_line": "<code>{date}</code> {amount}x {reward}",
        "history_empty": "empty",

        "claim_title": "<b>HoyoClaimer | Daily rewards</b>\n{blocks}",
        "claim_block": "<blockquote><b>{nick}</b>\n{lines}</blockquote>",
        "claim_line": "<b>{game}</b>: {status}",
        "claiming": "<b>HoyoClaimer</b>\n<blockquote>Claiming rewards...</blockquote>",
        "busy": "Claim is already running",

        "confirm_delete": "<b>HoyoClaimer</b>\n<blockquote>Delete account <b>{nick}</b> (<code>{hid}</code>)?</blockquote>",
        "deleted": "Account deleted",

        "reading": "<b>HoyoClaimer</b>\n<blockquote>Reading cookies...</blockquote>",
        "checking": "<b>HoyoClaimer</b>\n<blockquote>Checking cookies ({kind}, {count} cookies)...</blockquote>",
        "installing": "<b>HoyoClaimer</b>\n<blockquote>Installing genshin.py...</blockquote>",
        "install_fail": "<b>HoyoClaimer</b>\n<blockquote>Failed to install genshin.py:</blockquote>\n<pre>{error}</pre>",
        "too_big": "<b>HoyoClaimer</b>\n<blockquote>File is too large for a cookie file.</blockquote>",
        "no_cookies": "<b>HoyoClaimer</b>\n<blockquote>No HoYoLab cookies found. Supported: JSON (Cookie-Editor, EditThisCookie, Playwright, dict), Netscape cookies.txt, header string <code>a=b; c=d</code>.</blockquote>",
        "no_auth": (
            "<b>HoyoClaimer</b>\n"
            "<blockquote>Cookies have no HoYoLab auth tokens.</blockquote>\n"
            "<blockquote><b>Found ({count}):</b> <code>{names}</code></blockquote>\n"
            "<blockquote><b>Required:</b> <code>ltuid_v2</code> + <code>ltoken_v2</code> (or <code>account_id_v2</code> + <code>cookie_token_v2</code>).\n"
            "Log in on <b>hoyolab.com</b> and export cookies of that site, cookies of hoyoverse.com contain only device data.</blockquote>"
        ),
        "login_fail": "<b>HoyoClaimer</b>\n<blockquote>Login failed:</blockquote>\n<pre>{error}</pre>",
        "added": "<b>HoyoClaimer | Logged in</b>\n<blockquote><b>{nick}</b> | HoYoLab ID <code>{hid}</code> | {region}\nFormat: {kind}</blockquote>\n<blockquote expandable>{games}</blockquote>",
        "updated": "<b>HoyoClaimer | Cookies updated</b>\n<blockquote><b>{nick}</b> | HoYoLab ID <code>{hid}</code> | {region}\nFormat: {kind}</blockquote>\n<blockquote expandable>{games}</blockquote>",

        "log_login": "<b>HoyoClaimer | Logged in</b>\n<blockquote><b>{nick}</b> | HoYoLab ID <code>{hid}</code> | {region}</blockquote>\n<blockquote expandable>{games}</blockquote>",
        "log_login_fail": "<b>HoyoClaimer | Login failed</b>\n<blockquote>{error}</blockquote>",
        "log_claim": "<b>HoyoClaimer | Daily rewards ({mode})</b>\n<blockquote><b>{nick}</b> | <code>{hid}</code></blockquote>\n<blockquote>{lines}</blockquote>",
        "log_deleted": "<b>HoyoClaimer | Account deleted</b>\n<blockquote><b>{nick}</b> | <code>{hid}</code></blockquote>",
        "log_auto": "<b>HoyoClaimer | Auto claim finished</b>\n<blockquote>Accounts: {accounts} | claimed: {ok} | already: {already} | failed: {failed}</blockquote>",
        "topic_desc": "HoyoClaimer daily rewards log.",
        "greeting": (
            "<blockquote><b>HoyoClaimer installed</b></blockquote>\n"
            "<blockquote>This topic is the log of HoYoLab daily rewards: logins, claimed rewards and errors.</blockquote>\n"
            "<blockquote><b>Add an account:</b> reply <code>{prefix}hc</code> to a txt/json file or a message with HoYoLab cookies.\n"
            "<b>Menu:</b> <code>{prefix}hc</code></blockquote>\n"
            "<blockquote><b>Auto claim:</b> {auto}\n"
            "<b>Time:</b> <code>{time}</code> ({tz})</blockquote>"
        ),
        "reloaded": (
            "<blockquote><b>HoyoClaimer reloaded, everything works</b></blockquote>\n"
            "<blockquote>Version: <b>{version}</b>\n"
            "Accounts: <b>{count}</b>\n"
            "Auto claim: <b>{auto}</b>\n"
            "Next claim: {next}</blockquote>"
        ),
        "mode_auto": "auto",
        "mode_manual": "manual",

        "btn_claim_all": "Claim all now",
        "btn_claim": "Claim now",
        "btn_status": "Check status",
        "btn_history": "History",
        "btn_delete": "Delete",
        "btn_confirm": "Yes, delete",
        "btn_back": "Back",
        "btn_menu": "Menu",
        "btn_refresh": "Refresh",
        "btn_close": "Close",
        "btn_auto_on": "Auto claim: on",
        "btn_auto_off": "Auto claim: off",
        "not_found": "Account not found",

        "_cfg_time": "Daily claim time HH:MM in TIMEZONE. HoYoLab resets at 00:00 UTC+8",
        "_cfg_tz": "Timezone for CLAIM_TIME: UTC offset like +08:00, +03:00 or IANA name like Europe/Moscow",
        "_cfg_delay": "Random delay in minutes added to the claim time",
        "_cfg_auto": "Claim rewards automatically every day",
        "_cfg_games": "Games to claim (only games bound to the account are used)",
        "_cfg_lang": "Language of reward names",
        "_cfg_log": "Send logs to the HoyoClaimer topic of the asset channel",
        "_cfg_retries": "Retries on network errors and rate limits",
        "_cfg_proxy": "Proxy for HoYoLab requests, for example http://127.0.0.1:8080",
        "_cfg_expiry": "Days before cookie expiry to tag the owner (0 - off)",
        "until_unknown": "unknown",
        "expiry_soon": (
            "<blockquote><b>{tag}, cookies expire soon</b></blockquote>\n"
            "<blockquote><b>{nick}</b> | <code>{hid}</code>\n"
            "Expire: <code>{date}</code> ({left} left)\n"
            "Export fresh cookies and reply <code>{prefix}hc</code> to the file.</blockquote>"
        ),
        "expiry_dead": (
            "<blockquote><b>{tag}, cookies are no longer valid</b></blockquote>\n"
            "<blockquote><b>{nick}</b> | <code>{hid}</code>\n"
            "Auto claim does not work for this account until the cookies are updated.\n"
            "Export fresh cookies and reply <code>{prefix}hc</code> to the file.</blockquote>"
        ),
    }

    strings_ru = {
        "menu": (
            "<b>HoyoClaimer</b>\n"
            "<blockquote><b>Аккаунтов:</b> {count}\n"
            "<b>Автосбор:</b> {auto}\n"
            "<b>Время:</b> <code>{time}</code> ({tz}){delay}\n"
            "<b>Следующий сбор:</b> {next}</blockquote>\n"
            "{accounts}"
        ),
        "menu_empty": "<blockquote>Аккаунтов нет. Ответьте <code>{prefix}hc</code> на txt/json файл или сообщение с куками HoYoLab.</blockquote>",
        "menu_account": "<blockquote expandable><b>{nick}</b> | <code>{hid}</code> | {region}\n{games}</blockquote>",
        "delay": ", случайная задержка до {minutes} мин",
        "next_in": "<code>{date}</code> (через {left})",
        "next_now": "скоро",
        "next_off": "выключен",
        "auto_on": "вкл",
        "auto_off": "выкл",
        "region_os": "Global",
        "region_cn": "Китай",

        "account": (
            "<b>HoyoClaimer</b>\n"
            "<blockquote><b>{nick}</b> | HoYoLab ID <code>{hid}</code> | {region}\n"
            "<b>Добавлен:</b> {added}\n"
            "<b>Куки действуют до:</b> {until}</blockquote>\n"
            "<blockquote expandable>{games}</blockquote>"
        ),
        "game_line": "<b>{game}</b>: {nick} | UID <code>{uid}</code> | {server} | Ур. {level}\n{status}",
        "no_games": "Игровые аккаунты не найдены, будут проверены все игры из конфига",
        "st_none": "Сегодня: ещё не собрано",
        "st_ok": "Сегодня: собрано {amount}x {reward} (день {days})",
        "st_already": "Сегодня: уже собрано{reward}",
        "st_already_reward": ", {amount}x {reward} (день {days})",
        "st_captcha": "Нужна капча, соберите вручную на HoYoLab",
        "st_cookies": "Куки истекли, обновите их",
        "st_nogame": "Нет привязанного игрового аккаунта",
        "st_error": "Ошибка: {error}",
        "st_old": "Последний: {date}, {status}",

        "info_title": "<b>HoyoClaimer | Статус</b>\n<blockquote><b>{nick}</b></blockquote>\n<blockquote expandable>{lines}</blockquote>",
        "info_line": "<b>{game}</b>: сегодня {signed}, {days} дн. в этом месяце, пропущено {missed}",
        "info_err": "<b>{game}</b>: {error}",
        "yes": "собрано",
        "no": "не собрано",

        "history_title": "<b>HoyoClaimer | История</b>\n<blockquote><b>{nick}</b></blockquote>\n{blocks}",
        "history_block": "<blockquote expandable><b>{game}</b>\n{lines}</blockquote>",
        "history_line": "<code>{date}</code> {amount}x {reward}",
        "history_empty": "пусто",

        "claim_title": "<b>HoyoClaimer | Ежедневные награды</b>\n{blocks}",
        "claim_block": "<blockquote><b>{nick}</b>\n{lines}</blockquote>",
        "claim_line": "<b>{game}</b>: {status}",
        "claiming": "<b>HoyoClaimer</b>\n<blockquote>Собираю награды...</blockquote>",
        "busy": "Сбор уже идёт",

        "confirm_delete": "<b>HoyoClaimer</b>\n<blockquote>Удалить аккаунт <b>{nick}</b> (<code>{hid}</code>)?</blockquote>",
        "deleted": "Аккаунт удалён",

        "reading": "<b>HoyoClaimer</b>\n<blockquote>Читаю куки...</blockquote>",
        "checking": "<b>HoyoClaimer</b>\n<blockquote>Проверяю куки ({kind}, {count} шт.)...</blockquote>",
        "installing": "<b>HoyoClaimer</b>\n<blockquote>Устанавливаю genshin.py...</blockquote>",
        "install_fail": "<b>HoyoClaimer</b>\n<blockquote>Не удалось установить genshin.py:</blockquote>\n<pre>{error}</pre>",
        "too_big": "<b>HoyoClaimer</b>\n<blockquote>Файл слишком большой для файла куки.</blockquote>",
        "no_cookies": "<b>HoyoClaimer</b>\n<blockquote>Куки HoYoLab не найдены. Поддерживаются: JSON (Cookie-Editor, EditThisCookie, Playwright, словарь), Netscape cookies.txt, строка заголовка <code>a=b; c=d</code>.</blockquote>",
        "no_auth": (
            "<b>HoyoClaimer</b>\n"
            "<blockquote>В куках нет токенов авторизации HoYoLab.</blockquote>\n"
            "<blockquote><b>Найдено ({count}):</b> <code>{names}</code></blockquote>\n"
            "<blockquote><b>Нужны:</b> <code>ltuid_v2</code> + <code>ltoken_v2</code> (или <code>account_id_v2</code> + <code>cookie_token_v2</code>).\n"
            "Войдите на <b>hoyolab.com</b> и экспортируйте куки этого сайта, куки hoyoverse.com содержат только данные устройства.</blockquote>"
        ),
        "login_fail": "<b>HoyoClaimer</b>\n<blockquote>Не удалось войти:</blockquote>\n<pre>{error}</pre>",
        "added": "<b>HoyoClaimer | Вход выполнен</b>\n<blockquote><b>{nick}</b> | HoYoLab ID <code>{hid}</code> | {region}\nФормат: {kind}</blockquote>\n<blockquote expandable>{games}</blockquote>",
        "updated": "<b>HoyoClaimer | Куки обновлены</b>\n<blockquote><b>{nick}</b> | HoYoLab ID <code>{hid}</code> | {region}\nФормат: {kind}</blockquote>\n<blockquote expandable>{games}</blockquote>",

        "log_login": "<b>HoyoClaimer | Вход выполнен</b>\n<blockquote><b>{nick}</b> | HoYoLab ID <code>{hid}</code> | {region}</blockquote>\n<blockquote expandable>{games}</blockquote>",
        "log_login_fail": "<b>HoyoClaimer | Ошибка входа</b>\n<blockquote>{error}</blockquote>",
        "log_claim": "<b>HoyoClaimer | Ежедневные награды ({mode})</b>\n<blockquote><b>{nick}</b> | <code>{hid}</code></blockquote>\n<blockquote>{lines}</blockquote>",
        "log_deleted": "<b>HoyoClaimer | Аккаунт удалён</b>\n<blockquote><b>{nick}</b> | <code>{hid}</code></blockquote>",
        "log_auto": "<b>HoyoClaimer | Автосбор завершён</b>\n<blockquote>Аккаунтов: {accounts} | собрано: {ok} | уже было: {already} | ошибок: {failed}</blockquote>",
        "topic_desc": "Лог ежедневных наград HoyoClaimer.",
        "greeting": (
            "<blockquote><b>HoyoClaimer установлен</b></blockquote>\n"
            "<blockquote>Этот топик хранит лог ежедневных наград HoYoLab: входы в аккаунты, полученные награды и ошибки.</blockquote>\n"
            "<blockquote><b>Добавить аккаунт:</b> ответьте <code>{prefix}hc</code> на txt/json файл или сообщение с куки HoYoLab.\n"
            "<b>Меню:</b> <code>{prefix}hc</code></blockquote>\n"
            "<blockquote><b>Автосбор:</b> {auto}\n"
            "<b>Время:</b> <code>{time}</code> ({tz})</blockquote>"
        ),
        "reloaded": (
            "<blockquote><b>HoyoClaimer перезагружен, все работает</b></blockquote>\n"
            "<blockquote>Версия: <b>{version}</b>\n"
            "Аккаунтов: <b>{count}</b>\n"
            "Автосбор: <b>{auto}</b>\n"
            "Следующий сбор: {next}</blockquote>"
        ),
        "mode_auto": "авто",
        "mode_manual": "вручную",

        "btn_claim_all": "Собрать всё сейчас",
        "btn_claim": "Собрать сейчас",
        "btn_status": "Проверить статус",
        "btn_history": "История",
        "btn_delete": "Удалить",
        "btn_confirm": "Да, удалить",
        "btn_back": "Назад",
        "btn_menu": "Меню",
        "btn_refresh": "Обновить",
        "btn_close": "Закрыть",
        "btn_auto_on": "Автосбор: вкл",
        "btn_auto_off": "Автосбор: выкл",
        "not_found": "Аккаунт не найден",

        "_cfg_time": "Время ежедневного сбора ЧЧ:ММ в часовом поясе TIMEZONE. Сброс HoYoLab в 00:00 UTC+8",
        "_cfg_tz": "Часовой пояс для CLAIM_TIME: смещение вида +08:00, +03:00 или имя IANA вида Europe/Moscow",
        "_cfg_delay": "Случайная задержка в минутах к времени сбора",
        "_cfg_auto": "Собирать награды автоматически каждый день",
        "_cfg_games": "Игры для сбора (используются только привязанные к аккаунту)",
        "_cfg_lang": "Язык названий наград",
        "_cfg_log": "Отправлять логи в топик HoyoClaimer канала ассетов",
        "_cfg_retries": "Повторы при сетевых ошибках и лимитах",
        "_cfg_proxy": "Прокси для запросов HoYoLab, например http://127.0.0.1:8080",
        "_cfg_expiry": "За сколько дней до истечения куки тегать владельца (0 - выключено)",
        "until_unknown": "неизвестно",
        "expiry_soon": (
            "<blockquote><b>{tag}, куки скоро протухнут</b></blockquote>\n"
            "<blockquote><b>{nick}</b> | <code>{hid}</code>\n"
            "Истекают: <code>{date}</code> (осталось {left})\n"
            "Выгрузи свежие куки и ответь на файл командой <code>{prefix}hc</code>.</blockquote>"
        ),
        "expiry_dead": (
            "<blockquote><b>{tag}, куки больше не действуют</b></blockquote>\n"
            "<blockquote><b>{nick}</b> | <code>{hid}</code>\n"
            "Автосбор для этого аккаунта не работает, пока куки не обновлены.\n"
            "Выгрузи свежие куки и ответь на файл командой <code>{prefix}hc</code>.</blockquote>"
        ),
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "CLAIM_TIME",
                "00:05",
                lambda: self.strings["_cfg_time"],
                validator=loader.validators.RegExp(r"^([01]?\d|2[0-3]):[0-5]\d$"),
            ),
            loader.ConfigValue(
                "TIMEZONE",
                "+08:00",
                lambda: self.strings["_cfg_tz"],
                validator=loader.validators.String(),
            ),
            loader.ConfigValue(
                "RANDOM_DELAY",
                10,
                lambda: self.strings["_cfg_delay"],
                validator=loader.validators.Integer(minimum=0, maximum=180),
            ),
            loader.ConfigValue(
                "AUTO_CLAIM",
                True,
                lambda: self.strings["_cfg_auto"],
                validator=loader.validators.Boolean(),
            ),
            loader.ConfigValue(
                "GAMES",
                list(GAMES),
                lambda: self.strings["_cfg_games"],
                validator=loader.validators.MultiChoice(list(GAMES)),
            ),
            loader.ConfigValue(
                "REWARD_LANG",
                "ru-ru",
                lambda: self.strings["_cfg_lang"],
                validator=loader.validators.Choice(LANGS),
            ),
            loader.ConfigValue(
                "LOG_TOPIC",
                True,
                lambda: self.strings["_cfg_log"],
                validator=loader.validators.Boolean(),
            ),
            loader.ConfigValue(
                "RETRIES",
                3,
                lambda: self.strings["_cfg_retries"],
                validator=loader.validators.Integer(minimum=0, maximum=10),
            ),
            loader.ConfigValue(
                "EXPIRY_WARN_DAYS",
                2,
                lambda: self.strings["_cfg_expiry"],
                validator=loader.validators.Integer(minimum=0, maximum=14),
            ),
            loader.ConfigValue(
                "PROXY",
                "",
                lambda: self.strings["_cfg_proxy"],
                validator=loader.validators.String(),
            ),
        )
        self._g = None
        self._lib_lock = None
        self._claim_lock = None
        self._topic_id = None
        self._topic_lock = None
        self._startup_task = None
        self._scheduler = None
        self._acc = {}
        self._me = None
        self._expiry_checked = 0

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        self._lib_lock = asyncio.Lock()
        self._claim_lock = asyncio.Lock()
        self._acc = dict(self.get("accounts", {}) or {})
        self._scheduler = asyncio.ensure_future(self._schedule_loop())
        self._startup_task = asyncio.ensure_future(self._startup())

    async def on_unload(self):
        for task in (self._scheduler, self._startup_task):
            if task and not task.done():
                task.cancel()

    async def _startup(self):
        topic_id = None
        for attempt in range(6):
            try:
                topic_id = await self._ensure_topic()
                break
            except Exception as e:
                logger.warning(f"[HoyoClaimer] topic setup failed: {e}")
                await asyncio.sleep(10 * (attempt + 1))
        if topic_id:
            await self._notify(topic_id)

    async def _wait_channel(self, limit=120):
        channel = self._asset_channel()
        waited = 0
        while not channel and waited < limit:
            await asyncio.sleep(5)
            waited += 5
            channel = self._asset_channel()
        return channel

    async def _ensure_topic(self, fresh=False):
        if self._topic_lock is None:
            self._topic_lock = asyncio.Lock()
        async with self._topic_lock:
            if self._topic_id and not fresh:
                return self._topic_id
            channel = await self._wait_channel(0 if fresh else 120)
            if not channel:
                raise RuntimeError("asset channel not found")
            topic = await utils.asset_forum_topic(
                self._client,
                self._db,
                channel,
                TOPIC_TITLE,
                description=self.strings["topic_desc"],
                icon_emoji_id=TOPIC_ICON,
                invite_bot=True,
            )
            if getattr(topic, "icon_emoji_id", None) != TOPIC_ICON:
                try:
                    await self._client(EditForumTopicRequest(
                        peer=channel,
                        topic_id=topic.id,
                        icon_emoji_id=TOPIC_ICON,
                    ))
                except Exception as e:
                    logger.debug(f"[HoyoClaimer] topic icon not set: {e}")
            self._topic_id = topic.id
            return topic.id

    async def _notify(self, topic_id):
        channel = self._asset_channel()
        if not channel:
            return
        key = f"hoyoclaimer_greeted_{str(channel)[4:]}_{topic_id}"
        auto = self.strings["auto_on" if self.config["AUTO_CLAIM"] else "auto_off"]
        if self.get(key, False):
            text = self._s(
                "reloaded",
                version=".".join(map(str, __version__)),
                count=len(self._acc),
                auto=auto,
                next=self._next_text(),
            )
        else:
            self.set(key, True)
            text = self._s("greeting", auto=auto, time=self.config["CLAIM_TIME"], tz=self._tz_name())
        await self._send_with_preview(channel, topic_id, text)

    async def _send_with_preview(self, channel, topic_id, text):
        bot = self._bot()
        try:
            msg_text, entities = await bot._parse_message_text(text, "html")
            msg = await bot.send_message(
                channel,
                msg_text,
                formatting_entities=entities,
                reply_to=topic_id,
                link_preview=False,
            )
            try:
                peer = await bot.get_input_entity(channel)
                current = await bot.get_messages(channel, ids=msg.id)
                reply_markup = current.reply_markup if current else None
                await bot(EditMessageRequest(
                    peer=peer,
                    id=msg.id,
                    message=msg_text,
                    media=InputMediaWebPage(
                        url=RELOAD_BANNER,
                        optional=True,
                        force_large_media=True,
                    ),
                    invert_media=True,
                    reply_markup=reply_markup,
                    entities=entities,
                    no_webpage=False,
                ))
            except Exception as e:
                logger.error(f"[HoyoClaimer] Failed to add preview: {e}")
        except Exception as e:
            logger.error(f"[HoyoClaimer] Failed to send message with preview: {e}")

    def _s(self, key, **kwargs):
        try:
            return self.strings[key].format(prefix=self.get_prefix(), **kwargs)
        except (KeyError, IndexError):
            return self.strings[key]

    def _save(self):
        self.set("accounts", self._acc)

    def _bot(self):
        return getattr(self.inline, "_bot_client", None) or self.inline.bot.client

    def _asset_channel(self):
        cid = self._db.get("feroku.forums", "channel_id", None) or self._db.get(
            "heroku.forums", "channel_id", None
        )
        if not cid:
            return None
        cid = abs(int(cid))
        text = str(cid)
        if text.startswith("100") and len(text) > 12:
            cid = int(text[3:])
        return int(f"-100{cid}")

    async def _log(self, text, force=False):
        if not self.config["LOG_TOPIC"] and not force:
            return False
        channel = self._asset_channel()
        if not channel:
            return False
        for attempt in range(2):
            try:
                await self._ensure_topic(fresh=attempt > 0)
                try:
                    await self._bot().send_message(
                        channel, text, parse_mode="html", reply_to=self._topic_id, link_preview=False
                    )
                except Exception:
                    await self._client.send_message(
                        channel, text, parse_mode="html", reply_to=self._topic_id, link_preview=False
                    )
                return True
            except Exception as e:
                if attempt:
                    logger.warning(f"[HoyoClaimer] topic log failed: {e}")
        return False

    async def _alert(self, text):
        if await self._log(text, force=True):
            return
        try:
            await self._client.send_message("me", text, parse_mode="html", link_preview=False)
        except Exception as e:
            logger.warning(f"[HoyoClaimer] alert failed: {e}")

    async def _owner_tag(self):
        try:
            if self._me is None:
                self._me = await self._client.get_me()
            name = " ".join(x for x in (self._me.first_name, self._me.last_name) if x) or "owner"
            return f"<a href='tg://user?id={self._me.id}'>{_esc(name)}</a>"
        except Exception as e:
            logger.debug(f"[HoyoClaimer] owner tag failed: {e}")
            return "<b>!</b>"

    def _until_text(self, acc):
        exp = acc.get("expires")
        if not exp:
            return self.strings["until_unknown"]
        date = datetime.fromtimestamp(exp, self._tz()).strftime("%Y-%m-%d %H:%M")
        left = exp - time.time()
        if left <= 0:
            return f"<code>{date}</code>"
        return f"<code>{date}</code> ({_fmt_left(left)})"

    async def _send_expiry(self, uid, dead):
        acc = self._acc[uid]
        exp = acc.get("expires")
        left = (exp - time.time()) if exp else 0
        text = self._s(
            "expiry_dead" if dead else "expiry_soon",
            tag=await self._owner_tag(),
            nick=_esc(acc["nick"]),
            hid=acc["hid"],
            date=datetime.fromtimestamp(exp, self._tz()).strftime("%Y-%m-%d %H:%M") if exp else "?",
            left=_fmt_left(left),
        )
        acc["warned"] = time.time()
        if dead:
            acc["expires"] = None
        self._save()
        await self._alert(text)

    async def _expiry_alert(self, uid):
        acc = self._acc[uid]
        dead = False
        try:
            g = await self._lib()
            region, user, games = await self._login(g, acc["cookies"], [acc["region"]], uid)
        except Exception as e:
            if self._g is not None and isinstance(e, self._g.InvalidCookies):
                dead = True
            else:
                logger.warning(f"[HoyoClaimer] expiry recheck failed, will retry: {self._err(e)}")
                return
        if not dead:
            hid = getattr(user, "hoyolab_id", None) or acc.get("hid") or uid
            acc["nick"] = getattr(user, "nickname", None) or acc["nick"]
            acc["hid"] = str(hid)
            acc["region"] = region
            acc["games"] = self._game_info(games) or acc.get("games", [])
            if acc["expires"] <= time.time():
                acc["expires"] = None
                self._save()
                return
            self._save()
        await self._send_expiry(uid, dead)

    async def _check_expiry(self):
        now = time.time()
        if now - self._expiry_checked < 1800:
            return
        self._expiry_checked = now
        days = int(self.config["EXPIRY_WARN_DAYS"])
        if days <= 0:
            return
        for uid in list(self._acc):
            acc = self._acc.get(uid) or {}
            exp = acc.get("expires")
            if not exp or exp - now > days * 86400 or now - acc.get("warned", 0) < 86400:
                continue
            try:
                await self._expiry_alert(uid)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("[HoyoClaimer] expiry check failed")

    async def _lib(self, status=None):
        if self._g is not None:
            return self._g
        async with self._lib_lock:
            if self._g is not None:
                return self._g
            try:
                self._g = importlib.import_module("genshin")
                return self._g
            except ImportError:
                pass
            if status is not None:
                try:
                    await utils.answer(status, self.strings["installing"])
                except Exception:
                    pass
            proc = await asyncio.create_subprocess_exec(
                sys.executable, "-m", "pip", "install", "-U", "-q", "genshin",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            _, err = await proc.communicate()
            if proc.returncode != 0:
                raise ImportError(err.decode(errors="ignore")[-800:] or "pip failed")
            importlib.invalidate_caches()
            self._g = importlib.import_module("genshin")
            return self._g

    def _make_client(self, g, cookies, region, hid=None):
        lang = "zh-cn" if region == "cn" else self.config["REWARD_LANG"]
        proxy = (self.config["PROXY"] or "").strip() or None
        return g.Client(
            dict(cookies),
            lang=lang,
            region=g.Region.CHINESE if region == "cn" else g.Region.OVERSEAS,
            proxy=proxy,
            hoyolab_id=int(hid) if hid and str(hid).isdigit() else None,
        )

    def _account_client(self, g, uid):
        acc = self._acc[uid]
        client = self._make_client(g, acc["cookies"], acc["region"], uid)

        def on_update(cookies):
            if uid in self._acc:
                self._acc[uid]["cookies"] = dict(cookies)
                self._save()

        client.on_cookie_update = on_update
        return client

    def _err(self, e):
        text = re.sub(r"\s+", " ", str(e)).strip()
        return text or type(e).__name__

    async def _login(self, g, cookies, regions, hid):
        last = None
        for region in regions:
            client = self._make_client(g, cookies, region, hid)
            try:
                user = await client.get_hoyolab_user()
            except g.InvalidCookies as e:
                last = last or e
                continue
            try:
                games = await client.get_game_accounts()
            except g.GenshinException:
                games = []
            return region, user, games
        raise last or RuntimeError("login failed")

    def _game_info(self, games):
        result = []
        for acc in games:
            try:
                game = acc.game
            except Exception:
                game = None
            value = getattr(game, "value", None)
            if value not in GAMES:
                continue
            result.append({
                "game": value,
                "uid": acc.uid,
                "nick": acc.nickname,
                "server": acc.server_name,
                "level": acc.level,
            })
        return result

    def _today(self):
        return datetime.now(CN_TZ).date().isoformat()

    def _status_text(self, res, today=True):
        if not res:
            return self.strings["st_none"]
        state = res.get("status")
        if state == "ok":
            text = self._s("st_ok", amount=res.get("amount", 0), reward=_esc(res.get("reward", "")), days=res.get("days", "?"))
        elif state == "already":
            extra = ""
            if res.get("reward"):
                extra = self._s("st_already_reward", amount=res.get("amount", 0), reward=_esc(res["reward"]), days=res.get("days", "?"))
            text = self._s("st_already", reward=extra)
        elif state == "captcha":
            text = self.strings["st_captcha"]
        elif state == "cookies":
            text = self.strings["st_cookies"]
        elif state == "nogame":
            text = self.strings["st_nogame"]
        else:
            text = self._s("st_error", error=_esc(res.get("error", "")[:200]))
        if today and res.get("date") != self._today():
            return self._s("st_old", date=res.get("date", "?"), status=text)
        return text

    def _games_text(self, acc, short=False):
        last = acc.get("last", {})
        lines = []
        bound = acc.get("games") or []
        if not bound:
            lines.append(self.strings["no_games"])
            for game, res in last.items():
                lines.append(self._s("claim_line", game=GAME_NAMES.get(game, game), status=self._status_text(res)))
            return "\n".join(lines)
        for item in bound:
            status = self._status_text(last.get(item["game"]))
            if short:
                lines.append(self._s("claim_line", game=GAME_NAMES[item["game"]], status=status))
            else:
                lines.append(self._s(
                    "game_line",
                    game=GAME_NAMES[item["game"]],
                    nick=_esc(item["nick"]),
                    uid=item["uid"],
                    server=_esc(item["server"]),
                    level=item["level"],
                    status=status,
                ))
        return "\n".join(lines)

    def _region_name(self, region):
        return self.strings["region_cn"] if region == "cn" else self.strings["region_os"]

    async def _read_source(self, message):
        reply = await message.get_reply_message()
        args = (utils.get_args_raw(message) or "").strip()
        if reply and reply.document:
            if (reply.file.size or 0) > MAX_FILE:
                return None, "too_big"
            data = await reply.download_media(file=bytes)
            for enc in ("utf-8-sig", "utf-16", "cp1251", "latin-1"):
                try:
                    text = data.decode(enc)
                    if "\x00" not in text:
                        return text, None
                except UnicodeDecodeError:
                    continue
            return data.decode("utf-8", errors="ignore"), None
        if reply and reply.raw_text:
            return reply.raw_text, None
        if args:
            return args, None
        return None, None

    @loader.command(
        ru_doc="Реплай на txt/json файл или сообщение с куками HoYoLab - добавить аккаунт. Без реплая - меню",
        en_doc="Reply to a txt/json file or message with HoYoLab cookies - add an account. Without reply - menu",
    )
    async def hc(self, message):
        """Reply to a txt/json file or message with HoYoLab cookies - add an account. Without reply - menu"""
        raw, error = await self._read_source(message)
        if error:
            await utils.answer(message, self.strings[error])
            return
        if raw is None:
            text, markup = self._menu()
            await self.inline.form(text=text, message=message, reply_markup=markup, silent=True)
            return
        status = await utils.answer(message, self.strings["reading"])
        cookies, domains, kind = parse_cookies(raw)
        if not cookies:
            await utils.answer(status, self.strings["no_cookies"])
            return
        uid = _auth_uid(cookies)
        if not uid or not _has_token(cookies):
            names = ", ".join(list(cookies)[:40])
            await utils.answer(status, self._s("no_auth", count=len(cookies), names=_esc(names)))
            await self._log(self._s("log_login_fail", error=_esc(f"no auth tokens, cookies: {names}")[:900]))
            return
        await utils.answer(status, self._s("checking", kind=kind, count=len(cookies)))
        try:
            g = await self._lib(status)
        except Exception as e:
            await utils.answer(status, self._s("install_fail", error=_esc(self._err(e))))
            return
        if any(d.endswith(CN_DOMAINS) for d in domains):
            regions = ["cn"]
        elif domains:
            regions = ["os"]
        else:
            regions = ["os", "cn"]
        try:
            region, user, games = await self._login(g, cookies, regions, uid)
        except Exception as e:
            err = self._err(e)
            await utils.answer(status, self._s("login_fail", error=_esc(err)))
            await self._log(self._s("log_login_fail", error=_esc(f"{uid}: {err}")))
            return
        existed = uid in self._acc
        old = self._acc.get(uid, {})
        expires = parse_expiry(raw, cookies)
        hid = getattr(user, "hoyolab_id", None) or uid
        self._acc[uid] = {
            "cookies": cookies,
            "region": region,
            "nick": getattr(user, "nickname", None) or str(hid),
            "hid": str(hid),
            "games": self._game_info(games),
            "added": old.get("added") or int(time.time()),
            "last": old.get("last", {}),
            "expires": expires,
            "warned": 0,
        }
        self._save()
        acc = self._acc[uid]
        fields = dict(
            nick=_esc(acc["nick"]),
            hid=acc["hid"],
            region=self._region_name(region),
            kind=kind,
            games=self._games_text(acc),
        )
        await self._log(self._s("log_login", **fields))
        markup = [
            [{"text": self.strings["btn_claim"], "callback": self._cb_claim, "args": (uid,), "style": "success"}],
            [{"text": self.strings["btn_menu"], "callback": self._cb_menu, "style": "primary"}],
        ]
        await self.inline.form(
            text=self._s("updated" if existed else "added", **fields),
            message=status,
            reply_markup=markup,
            silent=True,
        )

    def _tz(self):
        raw = str(self.config["TIMEZONE"] or "").strip()
        m = re.fullmatch(r"(?:UTC|GMT)?\s*([+-])\s*(\d{1,2})(?::?(\d{2}))?", raw, re.I)
        if m:
            delta = timedelta(hours=int(m.group(2)), minutes=int(m.group(3) or 0))
            if delta <= timedelta(hours=14):
                return timezone(-delta if m.group(1) == "-" else delta)
        if raw.upper() in ("UTC", "GMT", "Z"):
            return timezone.utc
        if raw:
            try:
                from zoneinfo import ZoneInfo

                return ZoneInfo(raw)
            except Exception:
                pass
        return CN_TZ

    def _tz_name(self):
        tz = self._tz()
        if hasattr(tz, "key"):
            return tz.key
        offset = tz.utcoffset(None)
        minutes = int(offset.total_seconds() // 60)
        sign = "-" if minutes < 0 else "+"
        h, m = divmod(abs(minutes), 60)
        return f"UTC{sign}{h:02d}:{m:02d}"

    def _due(self, day):
        hours, minutes = map(int, str(self.config["CLAIM_TIME"]).split(":"))
        delay = int(self.config["RANDOM_DELAY"])
        jitter = random.Random(f"{day.isoformat()}:{getattr(self._client, 'tg_id', 0)}").randint(0, delay) if delay else 0
        return datetime.combine(day, dtime(hours, minutes), tzinfo=self._tz()) + timedelta(minutes=jitter)

    def _next_run(self):
        now = datetime.now(self._tz())
        day = now.date()
        if self.get("last_run") == day.isoformat():
            day += timedelta(days=1)
        return self._due(day), now

    async def _schedule_loop(self):
        await asyncio.sleep(15)
        while True:
            try:
                if self.config["AUTO_CLAIM"] and self._acc:
                    now = datetime.now(self._tz())
                    today = now.date().isoformat()
                    if self.get("last_run") != today and now >= self._due(now.date()):
                        self.set("last_run", today)
                        await self._claim_all(auto=True)
                if self._acc:
                    await self._check_expiry()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("[HoyoClaimer] scheduler error")
            await asyncio.sleep(30)

    async def _claim_game(self, g, client, game):
        retries = int(self.config["RETRIES"])
        today = self._today()
        for attempt in range(retries + 1):
            try:
                try:
                    reward = await client.claim_daily_reward(game=g.Game(game))
                    state = "ok"
                except g.AlreadyClaimed:
                    reward = None
                    state = "already"
                result = {"status": state, "date": today}
                try:
                    info = await client.get_reward_info(game=g.Game(game))
                    result["days"] = info.claimed_rewards
                    if reward is None and info.signed_in and info.claimed_rewards:
                        monthly = await client.get_monthly_rewards(game=g.Game(game))
                        reward = monthly[info.claimed_rewards - 1]
                except Exception:
                    pass
                if reward is not None:
                    result["reward"] = reward.name
                    result["amount"] = reward.amount
                return result
            except g.DailyGeetestTriggered:
                return {"status": "captcha", "date": today}
            except g.InvalidCookies as e:
                return {"status": "cookies", "date": today, "error": self._err(e)}
            except (g.VisitsTooFrequently, g.TooManyRequests, aiohttp.ClientError, asyncio.TimeoutError) as e:
                if attempt >= retries:
                    return {"status": "error", "date": today, "error": self._err(e)}
                await asyncio.sleep(5 * (attempt + 1))
            except g.GenshinException as e:
                if getattr(e, "retcode", 0) in (-10002, -1073, 1009):
                    return {"status": "nogame", "date": today, "error": self._err(e)}
                return {"status": "error", "date": today, "error": self._err(e)}
            except Exception as e:
                logger.exception("[HoyoClaimer] claim failed")
                return {"status": "error", "date": today, "error": self._err(e)}
        return {"status": "error", "date": today, "error": "retries exceeded"}

    async def _claim_account(self, g, uid, mode):
        acc = self._acc[uid]
        client = self._account_client(g, uid)
        wanted = [x for x in GAMES if x in (self.config["GAMES"] or [])]
        bound = [x["game"] for x in acc.get("games") or []]
        games = [x for x in wanted if x in bound] if bound else wanted
        results = {}
        for index, game in enumerate(games):
            if index:
                await asyncio.sleep(2)
            res = await self._claim_game(g, client, game)
            if not bound and res["status"] == "nogame":
                continue
            prev = acc.get("last", {}).get(game) or {}
            if res["status"] == "already" and prev.get("status") == "ok" and prev.get("date") == res["date"]:
                res = dict(prev, days=res.get("days", prev.get("days")))
            results[game] = res
            if res["status"] == "cookies":
                break
        acc.setdefault("last", {}).update(results)
        self._save()
        lines = "\n".join(
            self._s("claim_line", game=GAME_NAMES[game], status=self._status_text(res))
            for game, res in results.items()
        ) or self.strings["no_games"]
        await self._log(self._s(
            "log_claim",
            mode=self.strings[f"mode_{mode}"],
            nick=_esc(acc["nick"]),
            hid=acc["hid"],
            lines=lines,
        ))
        if any(r["status"] == "cookies" for r in results.values()) and time.time() - acc.get("warned", 0) > 43200:
            await self._send_expiry(uid, True)
        return results, lines

    async def _claim_all(self, auto=False, only=None):
        g = await self._lib()
        mode = "auto" if auto else "manual"
        blocks = []
        stats = {"ok": 0, "already": 0, "failed": 0}
        async with self._claim_lock:
            for uid in list(only or self._acc):
                if uid not in self._acc:
                    continue
                results, lines = await self._claim_account(g, uid, mode)
                for res in results.values():
                    if res["status"] == "ok":
                        stats["ok"] += 1
                    elif res["status"] == "already":
                        stats["already"] += 1
                    else:
                        stats["failed"] += 1
                blocks.append(self._s("claim_block", nick=_esc(self._acc[uid]["nick"]), lines=lines))
        if auto and len(self._acc) > 1:
            await self._log(self._s("log_auto", accounts=len(self._acc), **stats))
        return self._s("claim_title", blocks="\n".join(blocks))

    def _next_text(self):
        if not self.config["AUTO_CLAIM"]:
            return self.strings["next_off"]
        due, now = self._next_run()
        left = (due - now).total_seconds()
        if left <= 0:
            return self.strings["next_now"]
        return self._s("next_in", date=due.strftime("%Y-%m-%d %H:%M"), left=_fmt_left(left))

    def _menu(self):
        auto = bool(self.config["AUTO_CLAIM"])
        nxt = self._next_text()
        delay = int(self.config["RANDOM_DELAY"])
        accounts = "\n".join(
            self._s(
                "menu_account",
                nick=_esc(acc["nick"]),
                hid=acc["hid"],
                region=self._region_name(acc["region"]),
                games=self._games_text(acc, short=True),
            )
            for acc in self._acc.values()
        ) or self._s("menu_empty")
        text = self._s(
            "menu",
            count=len(self._acc),
            auto=self.strings["auto_on" if auto else "auto_off"],
            time=self.config["CLAIM_TIME"],
            tz=self._tz_name(),
            delay=self._s("delay", minutes=delay) if delay else "",
            next=nxt,
            accounts=accounts,
        )
        markup = []
        if self._acc:
            markup.append([{"text": self.strings["btn_claim_all"], "callback": self._cb_claim_all, "style": "success"}])
        row = []
        for uid, acc in self._acc.items():
            row.append({"text": acc["nick"][:30], "callback": self._cb_account, "args": (uid,), "style": "primary"})
            if len(row) == 2:
                markup.append(row)
                row = []
        if row:
            markup.append(row)
        markup.append([
            {"text": self.strings["btn_auto_on" if auto else "btn_auto_off"], "callback": self._cb_toggle, "style": "primary"},
            {"text": self.strings["btn_refresh"], "callback": self._cb_menu, "style": "primary"},
        ])
        markup.append([{"text": self.strings["btn_close"], "action": "close", "style": "danger"}])
        return text, markup

    def _account_view(self, uid):
        acc = self._acc[uid]
        text = self._s(
            "account",
            nick=_esc(acc["nick"]),
            hid=acc["hid"],
            region=self._region_name(acc["region"]),
            added=datetime.fromtimestamp(acc.get("added", 0), CN_TZ).strftime("%Y-%m-%d"),
            until=self._until_text(acc),
            games=self._games_text(acc),
        )
        markup = [
            [{"text": self.strings["btn_claim"], "callback": self._cb_claim, "args": (uid,), "style": "success"}],
            [
                {"text": self.strings["btn_status"], "callback": self._cb_status, "args": (uid,), "style": "primary"},
                {"text": self.strings["btn_history"], "callback": self._cb_history, "args": (uid,), "style": "primary"},
            ],
            [
                {"text": self.strings["btn_delete"], "callback": self._cb_delete, "args": (uid,), "style": "danger"},
                {"text": self.strings["btn_back"], "callback": self._cb_menu},
            ],
        ]
        return text, markup

    def _back(self, uid):
        return [[{"text": self.strings["btn_back"], "callback": self._cb_account, "args": (uid,)}]]

    async def _cb_menu(self, call: InlineCall):
        text, markup = self._menu()
        await call.edit(text, reply_markup=markup)

    async def _cb_toggle(self, call: InlineCall):
        self.config["AUTO_CLAIM"] = not self.config["AUTO_CLAIM"]
        await self._cb_menu(call)

    async def _cb_account(self, call: InlineCall, uid):
        if uid not in self._acc:
            await call.answer(self.strings["not_found"], show_alert=True)
            return
        text, markup = self._account_view(uid)
        await call.edit(text, reply_markup=markup)

    async def _cb_claim_all(self, call: InlineCall):
        if self._claim_lock.locked():
            await call.answer(self.strings["busy"], show_alert=True)
            return
        await call.edit(self.strings["claiming"])
        text = await self._claim_all()
        await call.edit(text, reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_menu}]])

    async def _cb_claim(self, call: InlineCall, uid):
        if uid not in self._acc:
            await call.answer(self.strings["not_found"], show_alert=True)
            return
        if self._claim_lock.locked():
            await call.answer(self.strings["busy"], show_alert=True)
            return
        await call.edit(self.strings["claiming"])
        text = await self._claim_all(only=[uid])
        await call.edit(text, reply_markup=self._back(uid))

    async def _cb_status(self, call: InlineCall, uid):
        if uid not in self._acc:
            await call.answer(self.strings["not_found"], show_alert=True)
            return
        g = await self._lib()
        acc = self._acc[uid]
        client = self._account_client(g, uid)
        try:
            user = await client.get_hoyolab_user()
            acc["nick"] = user.nickname or acc["nick"]
            acc["games"] = self._game_info(await client.get_game_accounts()) or acc.get("games", [])
            self._save()
        except Exception:
            pass
        lines = []
        games = [x["game"] for x in acc.get("games") or []] or [x for x in GAMES if x in (self.config["GAMES"] or [])]
        for game in games:
            try:
                info = await client.get_reward_info(game=g.Game(game))
                lines.append(self._s(
                    "info_line",
                    game=GAME_NAMES[game],
                    signed=self.strings["yes" if info.signed_in else "no"],
                    days=info.claimed_rewards,
                    missed=max(info.missed_rewards, 0),
                ))
            except Exception as e:
                lines.append(self._s("info_err", game=GAME_NAMES[game], error=_esc(self._err(e)[:200])))
        await call.edit(
            self._s("info_title", nick=_esc(acc["nick"]), lines="\n".join(lines)),
            reply_markup=self._back(uid),
        )

    async def _cb_history(self, call: InlineCall, uid):
        if uid not in self._acc:
            await call.answer(self.strings["not_found"], show_alert=True)
            return
        g = await self._lib()
        acc = self._acc[uid]
        client = self._account_client(g, uid)
        games = [x["game"] for x in acc.get("games") or []] or [x for x in GAMES if x in (self.config["GAMES"] or [])]
        blocks = []
        for game in games:
            try:
                items = await client.claimed_rewards(limit=10, game=g.Game(game)).flatten()
                lines = "\n".join(
                    self._s(
                        "history_line",
                        date=item.time.strftime("%Y-%m-%d"),
                        amount=item.amount,
                        reward=_esc(item.name),
                    )
                    for item in items
                ) or self.strings["history_empty"]
            except Exception as e:
                lines = self._s("st_error", error=_esc(self._err(e)[:200]))
            blocks.append(self._s("history_block", game=GAME_NAMES[game], lines=lines))
        await call.edit(
            self._s("history_title", nick=_esc(acc["nick"]), blocks="\n".join(blocks)),
            reply_markup=self._back(uid),
        )

    async def _cb_delete(self, call: InlineCall, uid):
        if uid not in self._acc:
            await call.answer(self.strings["not_found"], show_alert=True)
            return
        acc = self._acc[uid]
        await call.edit(
            self._s("confirm_delete", nick=_esc(acc["nick"]), hid=acc["hid"]),
            reply_markup=[[
                {"text": self.strings["btn_confirm"], "callback": self._cb_delete_ok, "args": (uid,), "style": "danger"},
                {"text": self.strings["btn_back"], "callback": self._cb_account, "args": (uid,)},
            ]],
        )

    async def _cb_delete_ok(self, call: InlineCall, uid):
        acc = self._acc.pop(uid, None)
        self._save()
        if acc:
            await self._log(self._s("log_deleted", nick=_esc(acc["nick"]), hid=acc["hid"]))
        await call.answer(self.strings["deleted"])
        await self._cb_menu(call)