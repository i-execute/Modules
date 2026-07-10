__version__ = (1, 0, 0)
# meta developer: I_execute.t.me
# meta banner: https://raw.githubusercontent.com/i-execute/Modules/main/Storage/ShorBot/MetaBanner.jpeg

import sys
import time
import math
import random
import asyncio
import logging

from .. import loader, utils

try:
    import aiohttp
except ImportError:
    aiohttp = None

logger = logging.getLogger(__name__)

DEPS = ["aiohttp"]


def _install_deps():
    import importlib, subprocess, os
    pip = os.path.join(os.path.dirname(sys.executable), "pip")
    if not os.path.exists(pip):
        pip = "pip"
    lines = []
    for pkg in DEPS:
        try:
            subprocess.run(
                [pip, "install", "-U", pkg, "--break-system-packages", "-q"],
                capture_output=True, text=True, timeout=120,
            )
            importlib.invalidate_caches()
            importlib.import_module(pkg)
            lines.append(f"{pkg}: OK")
        except Exception as e:
            lines.append(f"{pkg}: FAIL ({e})")
    return "\n".join(lines)


def _escape_html(t):
    if not t:
        return ""
    return str(t).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _gcd(a, b):
    while b:
        a, b = b, a % b
    return a


def _is_prime(n):
    if n < 2:
        return False
    if n < 4:
        return True
    if n % 2 == 0:
        return False
    i = 3
    while i * i <= n:
        if n % i == 0:
            return False
        i += 2
    return True


def _is_prime_power(n):
    for p in range(2, int(math.isqrt(n)) + 2):
        if not _is_prime(p):
            continue
        val = p
        while val < n:
            val *= p
        if val == n:
            return True
    return False


def _classical_order_one_step(a, n, cap=10_000):
    r = 1
    val = a % n
    limit = min(n, cap)
    while val != 1:
        val = (val * a) % n
        r += 1
        if r > limit:
            return None
    return r


def _shor_one_attempt(a, n):
    g = _gcd(a, n)
    step = {"a": a}

    if g != 1:
        f1, f2 = g, n // g
        step["shortcut_gcd"] = g
        step["factors"] = (f1, f2)
        return step, True

    r = _classical_order_one_step(a, n)
    step["period_r"] = r

    if not r or r % 2 != 0:
        step["result"] = "period not suitable"
        return step, False

    x = pow(a, r // 2, n)
    if x == n - 1:
        step["result"] = "x = -1 mod N, retry"
        return step, False

    f1 = _gcd(x - 1, n)
    f2 = _gcd(x + 1, n)
    step["candidate_factors"] = (f1, f2)

    if f1 not in (1, n) and n % f1 == 0:
        step["factors"] = (f1, n // f1)
        return step, True
    if f2 not in (1, n) and n % f2 == 0:
        step["factors"] = (f2, n // f2)
        return step, True

    step["result"] = "candidates gave no factors"
    return step, False


def _check_trivial(n):
    if n < 15:
        return "error", "N too small, need >= 15"
    if n > 99_999_999_999_999:
        return "error", "N too large, max 14 digits (99 999 999 999 999)"
    if n % 2 == 0:
        return "trivial", (2, n // 2)
    if _is_prime(n):
        return "error", f"{n} is prime, nothing to factorize"
    if _is_prime_power(n):
        return "error", f"{n} is prime power, Shor's algorithm does not apply"
    return None, None


REPO_LINKS = (
    '<p>Module repo: <a href="https://github.com/i-execute/Modules">github.com/i-execute/Modules</a></p>'
    '<p>Original Shor simulation: '
    '<a href="https://github.com/SidRichardsQuantum/Shors_Algorithm_Simulation">'
    'github.com/SidRichardsQuantum/Shors_Algorithm_Simulation</a></p>'
)

RSA_DETAILS = (
    "<details><summary>Why this won't break RSA tomorrow</summary>"
    "<p>This module runs a <b>classical simulation</b> of Shor's period-finding algorithm — "
    "there is no quantum speedup here. A real quantum implementation would require "
    "thousands of logical qubits (and millions of physical ones with error correction), "
    "which does not exist yet. RSA is safe for now.</p>"
    + REPO_LINKS
    + "</details>"
)

THINKING_POOL = [
    "Computing gcd(a, N)...",
    "Finding period r via classical simulation...",
    "Checking r parity...",
    "Computing x = a^(r/2) mod N...",
    "Checking candidates gcd(x+-1, N)...",
    "Analyzing factor candidates...",
    "Trying next base a...",
    "Checking if gcd is nontrivial...",
]


def _thinking_for(attempt_idx: int, a: int) -> str:
    base = THINKING_POOL[attempt_idx % len(THINKING_POOL)]
    return f"Attempt {attempt_idx + 1}: a = {a} -- {base}"


def _table_rows_html(attempts: list) -> str:
    rows = ""
    for st in attempts:
        idx = st["attempt"]
        a = st["a"]

        if "shortcut_gcd" in st:
            g_val = st["shortcut_gcd"]
            f1, f2 = st["factors"]
            r_cell = "shortcut"
            res = f"<b>{f1} x {f2}</b>"
        else:
            g_val = _gcd(a, st["n"])
            r_val = st.get("period_r", "?")
            r_cell = _escape_html(str(r_val))
            cand = st.get("candidate_factors")
            if st.get("factors"):
                f1, f2 = st["factors"]
                res = f"<b>{f1} x {f2}</b>"
            elif cand:
                res = f"candidates: {cand[0]}, {cand[1]}"
            else:
                res = _escape_html(st.get("result", "-"))

        rows += (
            f"<tr>"
            f"<td>{idx}</td>"
            f"<td>{a}</td>"
            f"<td>{g_val}</td>"
            f"<td>{r_cell}</td>"
            f"<td>{res}</td>"
            f"</tr>"
        )
    return rows


def _build_table(rows_html: str) -> str:
    return (
        "<table>"
        "<tr><th>No.</th><th>a</th><th>gcd(a,N)</th><th>period r</th><th>result</th></tr>"
        + rows_html
        + "</table>"
    )


def _frame_html(n: int, attempts: list, thinking: str) -> str:
    head = f"<h1>Factorization N = {n}</h1>"
    tg = f"<tg-thinking>{_escape_html(thinking)}</tg-thinking>"
    table = _build_table(_table_rows_html(attempts)) if attempts else ""
    return head + tg + table


def _final_html(n: int, attempts: list, success: bool, factors=None) -> str:
    head = f"<h1>Factorization N = {n}</h1>"
    table = _build_table(_table_rows_html(attempts)) if attempts else ""
    code = (
        "<pre><code>"
        "x = a^(r/2) mod N\n"
        "factor1 = gcd(x-1, N)\n"
        "factor2 = gcd(x+1, N)"
        "</code></pre>"
    )

    if success and factors:
        f1, f2 = factors
        result = f"<h2>Result</h2><p><b>{n} = {f1} x {f2}</b></p>"
    else:
        result = f"<p>Tried {len(attempts)} attempts -- failed to factorize, try again.</p>"

    return head + table + code + result + RSA_DETAILS


def _trivial_html(n: int, f1: int, f2: int) -> str:
    return (
        f"<h1>Factorization N = {n}</h1>"
        "<p>N is even -- trivial case, no algorithm needed.</p>"
        f"<p><b>{n} = {f1} x {f2}</b></p>"
        + RSA_DETAILS
    )


def _plain_fallback(n: int, attempts: list, success: bool, factors=None) -> str:
    lines = [f"<b>Factorization N = {n}</b>", ""]
    for st in attempts:
        lines.append(
            f"Attempt {st['attempt']}: a={st['a']}, "
            f"r={st.get('period_r', st.get('shortcut_gcd', '-'))}, "
            f"{st.get('result', '')}"
        )
    if success and factors:
        f1, f2 = factors
        lines += ["", f"<b>{n} = {f1} x {f2}</b>"]
    else:
        lines += ["", "Failed to factorize, try again."]
    lines += [
        "",
        "Repo: https://github.com/i-execute/Modules",
        "Simulation: https://github.com/SidRichardsQuantum/Shors_Algorithm_Simulation",
    ]
    return "\n".join(lines)


class TgBotAPI:
    def __init__(self, token):
        self.token = token
        self.base = f"https://api.telegram.org/bot{token}"
        self._session = None

    async def _get_session(self):
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def call(self, method, payload=None, timeout=30):
        s = await self._get_session()
        async with s.post(
            f"{self.base}/{method}",
            json=payload or {},
            timeout=aiohttp.ClientTimeout(total=timeout),
        ) as r:
            return await r.json()

    async def get_me(self):
        return await self.call("getMe")

    async def get_updates(self, offset=None, timeout=25):
        payload = {"timeout": timeout}
        if offset is not None:
            payload["offset"] = offset
        return await self.call("getUpdates", payload, timeout=timeout + 10)

    async def send_message(self, chat_id, text, parse_mode="HTML"):
        payload = {
            "chat_id": chat_id,
            "text": text[:4000],
            "parse_mode": parse_mode,
        }
        return await self.call("sendMessage", payload)

    async def send_draft(self, chat_id, draft_id, html):
        return await self.call(
            "sendRichMessageDraft",
            {"chat_id": chat_id, "draft_id": draft_id, "rich_message": {"html": html}},
        )

    async def send_rich(self, chat_id, html):
        return await self.call(
            "sendRichMessage",
            {"chat_id": chat_id, "rich_message": {"html": html}},
        )


@loader.tds
class ShorBotMod(loader.Module):
    """Demonstration of Shor's algorithm via Telegram bot with RichMessage"""

    strings = {
        "name": "ShorBot",
        "main_menu": (
            "<b>ShorBot</b>\n"
            "<blockquote>"
            "Status: {status}\n"
            "Token: {token}"
            "</blockquote>"
        ),
        "status_running": "running",
        "status_stopped": "stopped",
        "btn_status": "Status",
        "btn_token": "Token",
        "btn_start": "Start",
        "btn_stop": "Stop",
        "btn_back": "Back",
        "btn_set_token": "Set Token",
        "btn_close": "Close",
        "token_menu": (
            "<b>Bot Token</b>\n"
            "<blockquote>Paste the token you received from @BotFather</blockquote>"
        ),
        "input_token": "Bot token:",
        "installing": (
            "<b>Installing dependencies</b>\n"
            "<blockquote>Please wait...</blockquote>"
        ),
        "token_started": (
            "<b>Bot started</b>\n"
            "<blockquote><code>{log}</code></blockquote>"
        ),
        "start_failed": (
            "<b>Start failed</b>\n"
            "<blockquote>{error}</blockquote>"
        ),
        "need_token": (
            "<b>Invalid token</b>\n"
            "<blockquote>Check the token format and try again</blockquote>"
        ),
        "start_message": (
            "<b>ShorBot</b>\n"
            "Send a composite number N to factorize.\n"
            "<blockquote>Examples: 21, 35, 91\nMax: 14 digits</blockquote>"
        ),
        "number_too_large": (
            "<b>Number too large</b>\n"
            "<blockquote>Maximum is 14 digits (99 999 999 999 999)</blockquote>"
        ),
        "invalid_number": (
            "<b>Invalid input</b>\n"
            "<blockquote>Send a valid positive integer</blockquote>"
        ),
    }

    strings_ru = {
        "name": "ShorBot",
        "main_menu": (
            "<b>ShorBot</b>\n"
            "<blockquote>"
            "Статус: {status}\n"
            "Токен: {token}"
            "</blockquote>"
        ),
        "status_running": "запущен",
        "status_stopped": "остановлен",
        "btn_status": "Статус",
        "btn_token": "Токен",
        "btn_start": "Старт",
        "btn_stop": "Стоп",
        "btn_back": "Назад",
        "btn_set_token": "Задать токен",
        "btn_close": "Закрыть",
        "token_menu": (
            "<b>Токен бота</b>\n"
            "<blockquote>Вставь токен, полученный от @BotFather</blockquote>"
        ),
        "input_token": "Токен бота:",
        "installing": (
            "<b>Установка зависимостей</b>\n"
            "<blockquote>Пожалуйста, подождите...</blockquote>"
        ),
        "token_started": (
            "<b>Бот запущен</b>\n"
            "<blockquote><code>{log}</code></blockquote>"
        ),
        "start_failed": (
            "<b>Ошибка запуска</b>\n"
            "<blockquote>{error}</blockquote>"
        ),
        "need_token": (
            "<b>Некорректный токен</b>\n"
            "<blockquote>Проверь формат токена и попробуй снова</blockquote>"
        ),
        "start_message": (
            "<b>ShorBot</b>\n"
            "Пришли составное число N для факторизации.\n"
            "<blockquote>Примеры: 21, 35, 91\nМаксимум: 14 цифр</blockquote>"
        ),
        "number_too_large": (
            "<b>Число слишком большое</b>\n"
            "<blockquote>Максимум — 14 цифр (99 999 999 999 999)</blockquote>"
        ),
        "invalid_number": (
            "<b>Некорректный ввод</b>\n"
            "<blockquote>Пришли корректное положительное целое число</blockquote>"
        ),
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "BOT_TOKEN",
                "",
                "Bot token for ShorBot (from @BotFather)",
                validator=loader.validators.Hidden(),
            ),
        )
        self._bot = None
        self._running = False
        self._poll_task = None

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, _install_deps)
        if self.config["BOT_TOKEN"]:
            try:
                await self._launch(self.config["BOT_TOKEN"])
            except Exception as e:
                logger.error(f"[SHORBOT] autorun failed: {e}")

    def _extract_token(self, text):
        import re
        m = re.search(r"\b(\d{8,10}:[A-Za-z0-9_-]{35})\b", text or "")
        return m.group(1) if m else None

    async def _launch(self, token):
        if self._poll_task:
            self._poll_task.cancel()
        self._bot = TgBotAPI(token)
        me = await self._bot.get_me()
        if not me.get("ok"):
            raise Exception(me.get("description", "getMe failed"))
        self._running = True
        self._poll_task = asyncio.ensure_future(self._poll_loop())
        return me["result"]

    async def _stop(self):
        self._running = False
        if self._poll_task:
            self._poll_task.cancel()
            self._poll_task = None
        if self._bot:
            await self._bot.close()
            self._bot = None

    async def _poll_loop(self):
        offset = None
        while self._running:
            try:
                res = await self._bot.get_updates(offset=offset)
                if not res.get("ok"):
                    await asyncio.sleep(3)
                    continue
                updates = res["result"]
                for upd in updates:
                    offset = upd["update_id"] + 1
                    asyncio.ensure_future(self._handle_update(upd))
                if not updates:
                    await asyncio.sleep(0.5)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[SHORBOT] poll error: {e}")
                await asyncio.sleep(3)

    async def _handle_update(self, upd):
        try:
            if "message" not in upd:
                return

            msg = upd["message"]
            chat_id = msg["chat"]["id"]
            text = (msg.get("text") or "").strip()

            if text == "/start":
                await self._bot.send_message(chat_id, self.strings["start_message"])
                return

            if not text.isdigit():
                await self._bot.send_message(chat_id, self.strings["invalid_number"])
                return

            n = int(text)

            if n > 99_999_999_999_999:
                await self._bot.send_message(chat_id, self.strings["number_too_large"])
                return

            await self._stream_factorize(chat_id, n)

        except Exception as e:
            logger.error(f"[SHORBOT] handle_update error: {e}")

    async def _stream_factorize(self, chat_id: int, n: int):
        draft_id = int(time.time())
        head = f"<h1>Factorization N = {n}</h1>"

        kind, val = _check_trivial(n)

        if kind == "error":
            await self._bot.send_message(chat_id, f"<b>Error:</b> {_escape_html(val)}")
            return

        if kind == "trivial":
            f1, f2 = val
            final_html = _trivial_html(n, f1, f2)
            rich_res = await self._bot.send_rich(chat_id, final_html)
            if not rich_res.get("ok"):
                logger.warning(f"[SHORBOT] trivial rich failed: {rich_res}")
                await self._bot.send_message(chat_id, f"N is even.\n<b>{n} = {f1} x {f2}</b>")
            return

        await self._safe_draft(
            chat_id, draft_id,
            head + "<tg-thinking>Starting Shor's algorithm, trying base a...</tg-thinking>",
        )

        attempts = []
        success = False
        factors = None
        max_tries = 100

        for attempt_idx in range(max_tries):
            a = random.randint(2, n - 1)

            step, found = _shor_one_attempt(a, n)
            step["attempt"] = attempt_idx + 1
            step["n"] = n
            attempts.append(step)

            if found:
                factors = step["factors"]
                f1, f2 = factors
                thinking = f"Successfully found factors: {f1} x {f2}"
                await self._safe_draft(chat_id, draft_id, _frame_html(n, attempts, thinking))
                success = True
                break

            thinking = _thinking_for(attempt_idx, a)
            await self._safe_draft(chat_id, draft_id, _frame_html(n, attempts, thinking))
            await asyncio.sleep(0.5)

        final_html = _final_html(n, attempts, success, factors)
        rich_res = await self._bot.send_rich(chat_id, final_html)

        if not rich_res.get("ok"):
            logger.warning(f"[SHORBOT] sendRichMessage failed: {rich_res}")
            await self._bot.send_message(
                chat_id,
                _plain_fallback(n, attempts, success, factors),
            )

    async def _safe_draft(self, chat_id: int, draft_id: int, html: str):
        try:
            res = await self._bot.send_draft(chat_id, draft_id, html)
            if not res.get("ok"):
                logger.warning(f"[SHORBOT] draft failed: {res}")
        except Exception as e:
            logger.warning(f"[SHORBOT] draft exception: {e}")

    def _fmt_menu(self):
        return self.strings["main_menu"].format(
            status=self.strings["status_running"] if self._running else self.strings["status_stopped"],
            token="set" if self.config["BOT_TOKEN"] else "not set",
        )

    def _main_markup(self):
        return [
            [
                {"text": self.strings["btn_status"], "callback": self._cb_status, "style": "primary"},
                {"text": self.strings["btn_token"], "callback": self._cb_token_menu, "style": "primary"},
            ],
            [
                {
                    "text": self.strings["btn_stop"] if self._running else self.strings["btn_start"],
                    "callback": self._cb_toggle,
                    "style": "danger" if self._running else "success",
                },
                {"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"},
            ],
        ]

    @loader.command(
        ru_doc="Панель управления ShorBot",
        en_doc="ShorBot control panel",
    )
    async def shorbot(self, message):
        """ShorBot control panel"""
        await self.inline.form(
            text=self._fmt_menu(),
            message=message,
            reply_markup=self._main_markup(),
            silent=True,
        )

    async def _cb_status(self, call):
        await call.edit(text=self._fmt_menu(), reply_markup=self._main_markup())

    async def _cb_back_main(self, call):
        await call.edit(text=self._fmt_menu(), reply_markup=self._main_markup())

    async def _cb_close(self, call):
        await call.delete()

    async def _cb_toggle(self, call):
        if self._running:
            await self._stop()
        elif self.config["BOT_TOKEN"]:
            try:
                await self._launch(self.config["BOT_TOKEN"])
            except Exception as e:
                await call.answer(str(e)[:200], show_alert=True)
        await call.edit(text=self._fmt_menu(), reply_markup=self._main_markup())

    async def _cb_token_menu(self, call):
        await call.edit(
            text=self.strings["token_menu"],
            reply_markup=[
                [
                    {
                        "text": self.strings["btn_set_token"],
                        "input": self.strings["input_token"],
                        "handler": self._cb_set_token,
                        "style": "success",
                    }
                ],
                [{"text": self.strings["btn_back"], "callback": self._cb_back_main, "style": "danger"}],
            ],
        )

    async def _cb_set_token(self, call, token_input: str):
        token = self._extract_token(token_input.strip()) or token_input.strip()
        if ":" not in token:
            await call.answer(self.strings["need_token"], show_alert=True)
            return
        await call.edit(text=self.strings["installing"])
        self.config["BOT_TOKEN"] = token
        try:
            me = await self._launch(token)
            await call.edit(
                text=self.strings["token_started"].format(
                    log=f"@{me['username']} ({me['id']}): OK"
                ),
                reply_markup=[[
                    {"text": self.strings["btn_back"], "callback": self._cb_back_main, "style": "primary"}
                ]],
            )
        except Exception as e:
            await call.edit(
                text=self.strings["start_failed"].format(error=str(e)[:200]),
                reply_markup=[[
                    {"text": self.strings["btn_back"], "callback": self._cb_back_main, "style": "danger"}
                ]],
            )

    async def on_unload(self):
        await self._stop()