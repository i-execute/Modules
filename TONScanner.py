__version__ = (1, 0, 0)
# meta developer: FireJester.t.me

import re
import time
import logging
import asyncio
import subprocess
import sys
import datetime

from aiogram.types import (
    InlineQuery,
    InlineQueryResultArticle,
    InputTextMessageContent,
)

from .. import loader, utils

logger = logging.getLogger(__name__)


def _ensure_deps():
    try:
        __import__("aiohttp")
    except ImportError:
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "aiohttp", "-q"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )


_ensure_deps()

import aiohttp

BANNER = "https://github.com/FireJester/Media/raw/main/Banner_for_inline_query_in_TONScanner.jpeg"

TONAPI_BASE = "https://tonapi.io/v2"

TON_ADDR_RE = re.compile(r"^[UEk0-9A-Za-z_-]{48}$")

CACHE_TTL = 120


def escape_html(t):
    return (t or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def nano_to_ton(nano):
    if nano is None:
        return 0.0
    return int(nano) / 1_000_000_000


def fmt_ton(val):
    if val == 0:
        return "0"
    if val < 0.001:
        return f"{val:.9f}"
    return f"{val:.4f}"


def fmt_rub(val):
    if val < 0.01:
        return "0"
    return f"{val:,.2f}".replace(",", " ")


def ts_to_str(ts):
    if not ts:
        return "N/A"
    return datetime.datetime.utcfromtimestamp(ts).strftime("%d.%m.%Y %H:%M UTC")


async def _api(path, timeout=15):
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                f"{TONAPI_BASE}{path}",
                timeout=aiohttp.ClientTimeout(total=timeout),
            ) as r:
                if r.status != 200:
                    return None
                return await r.json(content_type=None)
    except Exception:
        return None


async def _fetch_all_txs(addr):
    all_txs = []
    before_lt = None
    for _ in range(50):
        url = f"/blockchain/accounts/{addr}/transactions?limit=100"
        if before_lt:
            url += f"&before_lt={before_lt}"
        data = await _api(url)
        if not data:
            break
        txs = data.get("transactions", [])
        if not txs:
            break
        all_txs.extend(txs)
        if len(txs) < 100:
            break
        before_lt = txs[-1].get("lt")
        if not before_lt:
            break
    return all_txs


async def scan_wallet(addr):
    acc_data, rates_data, jettons_data, nfts_data = await asyncio.gather(
        _api(f"/accounts/{addr}"),
        _api("/rates?tokens=ton&currencies=rub,usd"),
        _api(f"/accounts/{addr}/jettons"),
        _api(f"/accounts/{addr}/nfts?limit=50"),
    )

    if not acc_data:
        return None

    balance_nano = acc_data.get("balance", 0)
    balance_ton = nano_to_ton(balance_nano)
    status = acc_data.get("status", "unknown")
    name = acc_data.get("name")
    is_scam = acc_data.get("is_scam", False)
    is_wallet = acc_data.get("is_wallet", False)
    last_activity = acc_data.get("last_activity")
    interfaces = acc_data.get("interfaces", [])

    rub_price = 0.0
    usd_price = 0.0
    diff_24h_rub = ""
    diff_7d_rub = ""
    diff_30d_rub = ""
    if rates_data:
        rates = rates_data.get("rates", {}).get("TON", {})
        prices = rates.get("prices", {})
        usd_price = float(prices.get("USD", 0))
        rub_price = float(prices.get("RUB", 0))
        diff_24h_rub = rates.get("diff_24h", {}).get("RUB", "")
        diff_7d_rub = rates.get("diff_7d", {}).get("RUB", "")
        diff_30d_rub = rates.get("diff_30d", {}).get("RUB", "")

    all_txs = await _fetch_all_txs(addr)

    total_in = 0
    total_out = 0
    total_fees = 0
    in_count = 0
    out_count = 0

    for tx in all_txs:
        total_fees += int(tx.get("total_fees", 0) or 0)
        in_msg = tx.get("in_msg", {})
        in_val = int(in_msg.get("value", 0) or 0)
        if in_val > 0:
            total_in += in_val
            in_count += 1
        for out_msg in tx.get("out_msgs", []):
            out_val = int(out_msg.get("value", 0) or 0)
            if out_val > 0:
                total_out += out_val
                out_count += 1

    total_in_ton = nano_to_ton(total_in)
    total_out_ton = nano_to_ton(total_out)
    total_fees_ton = nano_to_ton(total_fees)
    volume_ton = total_in_ton + total_out_ton

    jetton_count = 0
    jetton_list = []
    if jettons_data:
        balances = jettons_data.get("balances", [])
        jetton_count = len(balances)
        for jt in balances[:10]:
            meta = jt.get("jetton", {})
            jb = int(jt.get("balance", 0) or 0)
            decimals = int(meta.get("decimals", 0) or 0)
            real_b = jb / (10 ** decimals) if decimals > 0 else jb
            sym = meta.get("symbol", "?")
            jetton_list.append(f"{real_b:g} {sym}")

    nft_count = 0
    nft_list = []
    if nfts_data:
        items = nfts_data.get("nft_items", [])
        nft_count = len(items)
        for nft in items[:10]:
            meta = nft.get("metadata", {})
            col = nft.get("collection", {})
            nft_name = meta.get("name", "Unknown")
            col_name = col.get("name", "")
            nft_list.append((nft_name, col_name))

    first_tx_time = None
    if all_txs:
        utimes = [tx.get("utime", 0) for tx in all_txs if tx.get("utime")]
        if utimes:
            first_tx_time = min(utimes)

    return {
        "addr": addr,
        "balance_ton": balance_ton,
        "balance_rub": balance_ton * rub_price,
        "balance_usd": balance_ton * usd_price,
        "rub_price": rub_price,
        "usd_price": usd_price,
        "diff_24h_rub": diff_24h_rub,
        "diff_7d_rub": diff_7d_rub,
        "diff_30d_rub": diff_30d_rub,
        "status": status,
        "name": name,
        "is_scam": is_scam,
        "is_wallet": is_wallet,
        "interfaces": interfaces,
        "last_activity": last_activity,
        "first_tx_time": first_tx_time,
        "tx_count": len(all_txs),
        "in_count": in_count,
        "out_count": out_count,
        "total_in_ton": total_in_ton,
        "total_out_ton": total_out_ton,
        "volume_ton": volume_ton,
        "total_fees_ton": total_fees_ton,
        "total_in_rub": total_in_ton * rub_price,
        "total_out_rub": total_out_ton * rub_price,
        "volume_rub": volume_ton * rub_price,
        "fees_rub": total_fees_ton * rub_price,
        "jetton_count": jetton_count,
        "jetton_list": jetton_list,
        "nft_count": nft_count,
        "nft_list": nft_list,
    }


def build_message(d):
    lines = []
    lines.append("<b>TONScanner</b>")
    lines.append("")
    lines.append(f"<b>Address:</b> <code>{d['addr']}</code>")
    if d["name"]:
        lines.append(f"<b>Name:</b> {escape_html(d['name'])}")
    lines.append(f"<b>Status:</b> {d['status']}")
    w_type = ", ".join(d["interfaces"]) if d["interfaces"] else "unknown"
    lines.append(f"<b>Type:</b> {w_type}")
    if d["is_scam"]:
        lines.append("<b>SCAM</b>")
    lines.append("")

    lines.append(
        f"<b>Balance:</b> <code>{fmt_ton(d['balance_ton'])}</code> TON"
        f" (<code>{fmt_rub(d['balance_rub'])}</code> RUB)"
    )
    lines.append(
        f"<b>Rate:</b> 1 TON = {d['rub_price']:.2f} RUB / {d['usd_price']:.4f} USD"
    )
    if d["diff_24h_rub"] or d["diff_7d_rub"] or d["diff_30d_rub"]:
        parts = []
        if d["diff_24h_rub"]:
            parts.append(f"24h: {d['diff_24h_rub']}")
        if d["diff_7d_rub"]:
            parts.append(f"7d: {d['diff_7d_rub']}")
        if d["diff_30d_rub"]:
            parts.append(f"30d: {d['diff_30d_rub']}")
        lines.append(f"<b>Change RUB:</b> {' | '.join(parts)}")
    lines.append("")

    lines.append(f"<b>Transactions:</b> {d['tx_count']}")
    lines.append(
        f"<b>Incoming:</b> {d['in_count']} txs / "
        f"<code>{fmt_ton(d['total_in_ton'])}</code> TON"
        f" (<code>{fmt_rub(d['total_in_rub'])}</code> RUB)"
    )
    lines.append(
        f"<b>Outgoing:</b> {d['out_count']} txs / "
        f"<code>{fmt_ton(d['total_out_ton'])}</code> TON"
        f" (<code>{fmt_rub(d['total_out_rub'])}</code> RUB)"
    )
    lines.append(
        f"<b>Volume:</b> <code>{fmt_ton(d['volume_ton'])}</code> TON"
        f" (<code>{fmt_rub(d['volume_rub'])}</code> RUB)"
    )
    lines.append(
        f"<b>Fees:</b> <code>{fmt_ton(d['total_fees_ton'])}</code> TON"
        f" (<code>{fmt_rub(d['fees_rub'])}</code> RUB)"
    )
    lines.append("")

    if d["jetton_count"] > 0:
        lines.append(f"<b>Jettons ({d['jetton_count']}):</b>")
        for jt in d["jetton_list"]:
            lines.append(f"  <code>{jt}</code>")
        lines.append("")

    if d["nft_count"] > 0:
        lines.append(f"<b>NFTs ({d['nft_count']}):</b>")
        for nft_name, col_name in d["nft_list"]:
            if col_name:
                lines.append(f"  {escape_html(nft_name)} | {escape_html(col_name)}")
            else:
                lines.append(f"  {escape_html(nft_name)}")
        lines.append("")

    if d["last_activity"]:
        lines.append(f"<b>Last activity:</b> {ts_to_str(d['last_activity'])}")
    if d["first_tx_time"]:
        lines.append(f"<b>First transaction:</b> {ts_to_str(d['first_tx_time'])}")
    lines.append("")
    lines.append(f'<a href="https://tonviewer.com/{d["addr"]}">tonviewer.com</a>')

    return "\n".join(lines)


@loader.tds
class TONScanner(loader.Module):
    """TON wallet scanner via inline query"""

    strings = {
        "name": "TONScanner",
        "hint_title": "TONScanner",
        "hint_desc": "Paste TON address",
        "hint_msg": "<b>TONScanner:</b> Paste a TON wallet address",
        "invalid_title": "Invalid address",
        "invalid_desc": "This does not look like a valid TON address",
        "invalid_msg": "<b>TONScanner:</b> Invalid TON address format",
        "loading_title": "Scanning...",
        "loading_desc": "Fetching wallet data, wait a few seconds",
        "loading_msg": "<b>TONScanner:</b> Scanning wallet... Try again in a few seconds.",
        "err_title": "Error",
        "err_not_found": "Wallet not found or API error",
    }

    strings_ru = {
        "hint_title": "TONScanner",
        "hint_desc": "Вставьте адрес TON кошелька",
        "hint_msg": "<b>TONScanner:</b> Вставьте адрес TON кошелька",
        "invalid_title": "Неверный адрес",
        "invalid_desc": "Это не похоже на валидный TON адрес",
        "invalid_msg": "<b>TONScanner:</b> Неверный формат TON адреса",
        "loading_title": "Сканирую...",
        "loading_desc": "Получаю данные кошелька, подождите несколько секунд",
        "loading_msg": "<b>TONScanner:</b> Сканирую кошелек... Повторите запрос через несколько секунд.",
        "err_title": "Ошибка",
        "err_not_found": "Кошелек не найден или ошибка API",
    }

    def __init__(self):
        self.inline_bot = None
        self._pending = {}
        self._cache = {}

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        if hasattr(self, "inline") and hasattr(self.inline, "bot"):
            self.inline_bot = self.inline.bot

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

    async def _scan_task(self, addr, cache_key):
        try:
            result = await scan_wallet(addr)
            if not result:
                data = {"error": self.strings["err_not_found"]}
            else:
                data = {"message": build_message(result), "addr": addr}
            self._cache_set(cache_key, data)
            return data
        except Exception as e:
            data = {"error": str(e)[:80]}
            self._cache_set(cache_key, data)
            return data

    @loader.inline_handler(
        ru_doc="Сканировать TON кошелек",
        en_doc="Scan TON wallet",
    )
    async def ton_inline_handler(self, query: InlineQuery):
        """Scan TON wallet"""
        text = query.query.strip()
        if text.lower().startswith("ton"):
            text = text[3:].strip()

        if not text:
            await self._answer_hint(query)
            return

        addr = text.strip()
        if not TON_ADDR_RE.match(addr):
            await self._answer_invalid(query)
            return

        cache_key = f"ton_{addr}"

        cached = self._cache_get(cache_key)
        if cached:
            if "error" in cached:
                await self._answer_error(query, cached["error"])
                return
            if "message" in cached:
                await self._answer_result(query, cached)
                return

        if cache_key in self._pending:
            fut = self._pending[cache_key]
            if fut.done():
                self._pending.pop(cache_key, None)
                try:
                    res = fut.result()
                except Exception:
                    res = {"error": "Internal error"}
                if "error" in res:
                    await self._answer_error(query, res["error"])
                elif "message" in res:
                    await self._answer_result(query, res)
                else:
                    await self._answer_error(query, "Unknown error")
                return
            await self._answer_loading(query)
            return

        self._pending[cache_key] = asyncio.ensure_future(
            self._scan_task(addr, cache_key)
        )
        await self._answer_loading(query)

    async def _answer_hint(self, query):
        try:
            await self.inline_bot.answer_inline_query(
                inline_query_id=query.id,
                results=[
                    InlineQueryResultArticle(
                        id=f"h_{int(time.time())}",
                        title=self.strings["hint_title"],
                        description=self.strings["hint_desc"],
                        input_message_content=InputTextMessageContent(
                            message_text=self.strings["hint_msg"],
                            parse_mode="HTML",
                        ),
                        thumbnail_url=BANNER,
                        thumbnail_width=640,
                        thumbnail_height=360,
                    )
                ],
                cache_time=0,
                is_personal=True,
            )
        except Exception:
            pass

    async def _answer_invalid(self, query):
        try:
            await self.inline_bot.answer_inline_query(
                inline_query_id=query.id,
                results=[
                    InlineQueryResultArticle(
                        id=f"inv_{int(time.time())}",
                        title=self.strings["invalid_title"],
                        description=self.strings["invalid_desc"],
                        input_message_content=InputTextMessageContent(
                            message_text=self.strings["invalid_msg"],
                            parse_mode="HTML",
                        ),
                        thumbnail_url=BANNER,
                        thumbnail_width=640,
                        thumbnail_height=360,
                    )
                ],
                cache_time=0,
                is_personal=True,
            )
        except Exception:
            pass

    async def _answer_loading(self, query):
        try:
            await self.inline_bot.answer_inline_query(
                inline_query_id=query.id,
                results=[
                    InlineQueryResultArticle(
                        id=f"ld_{int(time.time())}",
                        title=self.strings["loading_title"],
                        description=self.strings["loading_desc"],
                        input_message_content=InputTextMessageContent(
                            message_text=self.strings["loading_msg"],
                            parse_mode="HTML",
                        ),
                        thumbnail_url=BANNER,
                        thumbnail_width=640,
                        thumbnail_height=360,
                    )
                ],
                cache_time=0,
                is_personal=True,
            )
        except Exception:
            pass

    async def _answer_result(self, query, data):
        try:
            await self.inline_bot.answer_inline_query(
                inline_query_id=query.id,
                results=[
                    InlineQueryResultArticle(
                        id=f"r_{int(time.time())}",
                        title="TONScanner",
                        description=f"Wallet: {data.get('addr', '?')[:20]}...",
                        input_message_content=InputTextMessageContent(
                            message_text=data["message"],
                            parse_mode="HTML",
                            disable_web_page_preview=True,
                        ),
                        thumbnail_url=BANNER,
                        thumbnail_width=640,
                        thumbnail_height=360,
                    )
                ],
                cache_time=0,
                is_personal=True,
            )
        except Exception:
            pass

    async def _answer_error(self, query, err):
        try:
            await self.inline_bot.answer_inline_query(
                inline_query_id=query.id,
                results=[
                    InlineQueryResultArticle(
                        id=f"e_{int(time.time())}",
                        title=self.strings["err_title"],
                        description=str(err)[:100],
                        input_message_content=InputTextMessageContent(
                            message_text=f"<b>TONScanner:</b> {escape_html(str(err))}",
                            parse_mode="HTML",
                        ),
                        thumbnail_url=BANNER,
                        thumbnail_width=640,
                        thumbnail_height=360,
                    )
                ],
                cache_time=0,
                is_personal=True,
            )
        except Exception:
            pass

    async def on_unload(self):
        for fut in self._pending.values():
            fut.cancel()
        self._pending.clear()
        self._cache.clear()