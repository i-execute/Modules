__version__ = (1, 3, 0)
# meta developer: I_execute.t.me
# meta banner: https://raw.githubusercontent.com/i-execute/Modules/main/Storage/GRAMScanner/MetaBanner.jpeg

import re
import logging
import asyncio
import datetime
import html

import aiohttp

from telethon.tl.types import (
    InputBotInlineResult,
    InputBotInlineMessageText,
    InputWebDocument,
)
from telethon.utils import html as tl_html

from .. import loader, utils

logger = logging.getLogger(__name__)

BANNER = "https://raw.githubusercontent.com/i-execute/Modules/main/Storage/GRAMScanner/InlineQuery.png"

TONAPI_BASE = "https://tonapi.io/v2"

GRAM_ADDR_RE = re.compile(r"^[UEk0-9A-Za-z_-]{48}$")


def escape_html(t):
    return html.escape(t or "")


def nano_to_gram(nano):
    if nano is None:
        return 0.0
    return int(nano) / 1_000_000_000


def fmt_gram(val):
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
    balance_gram = nano_to_gram(balance_nano)
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

    total_in_gram = nano_to_gram(total_in)
    total_out_gram = nano_to_gram(total_out)
    total_fees_gram = nano_to_gram(total_fees)
    volume_gram = total_in_gram + total_out_gram

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
        "balance_gram": balance_gram,
        "balance_rub": balance_gram * rub_price,
        "balance_usd": balance_gram * usd_price,
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
        "total_in_gram": total_in_gram,
        "total_out_gram": total_out_gram,
        "volume_gram": volume_gram,
        "total_fees_gram": total_fees_gram,
        "total_in_rub": total_in_gram * rub_price,
        "total_out_rub": total_out_gram * rub_price,
        "volume_rub": volume_gram * rub_price,
        "fees_rub": total_fees_gram * rub_price,
        "jetton_count": jetton_count,
        "jetton_list": jetton_list,
        "nft_count": nft_count,
        "nft_list": nft_list,
    }


def build_message(d):
    lines = []
    lines.append(f'<b><a href="https://tonviewer.com/{d["addr"]}">GRAMScanner</a></b>')

    info_lines = []
    info_lines.append(f"<b>Address:</b> <code>{d['addr']}</code>")
    if d["name"]:
        info_lines.append(f"<b>Name:</b> {escape_html(d['name'])}")
    info_lines.append(f"<b>Status:</b> {d['status']}")
    w_type = ", ".join(d["interfaces"]) if d["interfaces"] else "unknown"
    info_lines.append(f"<b>Type:</b> {w_type}")
    if d["is_scam"]:
        info_lines.append("<b>SCAM</b>")
    lines.append("<blockquote>" + "\n".join(info_lines) + "</blockquote>")

    bal_lines = []
    bal_lines.append(
        f"<b>Balance:</b> <code>{fmt_gram(d['balance_gram'])}</code> GRAM"
        f" (<code>{fmt_rub(d['balance_rub'])}</code> RUB)"
    )
    bal_lines.append(
        f"<b>Rate:</b> 1 GRAM = {d['rub_price']:.2f} RUB / {d['usd_price']:.4f} USD"
    )
    if d["diff_24h_rub"] or d["diff_7d_rub"] or d["diff_30d_rub"]:
        parts = []
        if d["diff_24h_rub"]:
            parts.append(f"24h: {d['diff_24h_rub']}")
        if d["diff_7d_rub"]:
            parts.append(f"7d: {d['diff_7d_rub']}")
        if d["diff_30d_rub"]:
            parts.append(f"30d: {d['diff_30d_rub']}")
        bal_lines.append(f"<b>Change RUB:</b> {' | '.join(parts)}")
    lines.append("<blockquote>" + "\n".join(bal_lines) + "</blockquote>")

    tx_lines = []
    tx_lines.append(f"<b>Transactions:</b> {d['tx_count']}")
    tx_lines.append(
        f"<b>Incoming:</b> {d['in_count']} txs / "
        f"<code>{fmt_gram(d['total_in_gram'])}</code> GRAM"
        f" (<code>{fmt_rub(d['total_in_rub'])}</code> RUB)"
    )
    tx_lines.append(
        f"<b>Outgoing:</b> {d['out_count']} txs / "
        f"<code>{fmt_gram(d['total_out_gram'])}</code> GRAM"
        f" (<code>{fmt_rub(d['total_out_rub'])}</code> RUB)"
    )
    tx_lines.append(
        f"<b>Volume:</b> <code>{fmt_gram(d['volume_gram'])}</code> GRAM"
        f" (<code>{fmt_rub(d['volume_rub'])}</code> RUB)"
    )
    tx_lines.append(
        f"<b>Fees:</b> <code>{fmt_gram(d['total_fees_gram'])}</code> GRAM"
        f" (<code>{fmt_rub(d['fees_rub'])}</code> RUB)"
    )
    lines.append("<blockquote>" + "\n".join(tx_lines) + "</blockquote>")

    if d["jetton_count"] > 0:
        jt_lines = [f"<b>Jettons ({d['jetton_count']}):</b>"]
        for jt in d["jetton_list"]:
            jt_lines.append(f"  <code>{jt}</code>")
        lines.append("<blockquote>" + "\n".join(jt_lines) + "</blockquote>")

    if d["nft_count"] > 0:
        nft_lines = [f"<b>NFTs ({d['nft_count']}):</b>"]
        for nft_name, col_name in d["nft_list"]:
            if col_name:
                nft_lines.append(f"  {escape_html(nft_name)} | {escape_html(col_name)}")
            else:
                nft_lines.append(f"  {escape_html(nft_name)}")
        lines.append("<blockquote>" + "\n".join(nft_lines) + "</blockquote>")

    time_lines = []
    if d["last_activity"]:
        time_lines.append(f"<b>Last activity:</b> {ts_to_str(d['last_activity'])}")
    if d["first_tx_time"]:
        time_lines.append(f"<b>First transaction:</b> {ts_to_str(d['first_tx_time'])}")
    if time_lines:
        lines.append("<blockquote>" + "\n".join(time_lines) + "</blockquote>")

    return "\n".join(lines)


@loader.tds
class GRAMScanner(loader.Module):
    """GRAM wallet scanner via inline query"""

    strings = {
        "name": "GRAMScanner",
        "hint_title": "GRAMScanner",
        "hint_desc": "Paste GRAM address",
        "hint_msg": "<b>GRAMScanner:</b> Paste a GRAM wallet address",
        "invalid_title": "Invalid address",
        "invalid_desc": "This does not look like a valid GRAM address",
        "invalid_msg": "<b>GRAMScanner:</b> Invalid GRAM address format",
        "loading_title": "Scanning...",
        "loading_desc": "Fetching wallet data, wait a few seconds",
        "loading_msg": "<b>GRAMScanner:</b> Scanning wallet... Try again in a few seconds.",
        "err_title": "Error",
        "err_not_found": "Wallet not found or API error",
    }

    strings_ru = {
        "hint_title": "GRAMScanner",
        "hint_desc": "Вставьте адрес GRAM кошелька",
        "hint_msg": "<b>GRAMScanner:</b> Вставьте адрес GRAM кошелька",
        "invalid_title": "Неверный адрес",
        "invalid_desc": "Это не похоже на валидный GRAM адрес",
        "invalid_msg": "<b>GRAMScanner:</b> Неверный формат GRAM адреса",
        "loading_title": "Сканирую...",
        "loading_desc": "Получаю данные кошелька, подождите несколько секунд",
        "loading_msg": "<b>GRAMScanner:</b> Сканирую кошелек... Повторите запрос через несколько секунд.",
        "err_title": "Ошибка",
        "err_not_found": "Кошелек не найден или ошибка API",
    }

    def __init__(self):
        self._pending = {}

    async def client_ready(self, client, db):
        self._client = client
        self._db = db

    async def _scan_task(self, addr):
        try:
            result = await scan_wallet(addr)
            if not result:
                return {"error": self.strings["err_not_found"]}
            return {"message": build_message(result), "addr": addr}
        except Exception as e:
            return {"error": str(e)[:80]}

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
        ru_doc="Сканировать GRAM кошелек",
        en_doc="Scan GRAM wallet",
    )
    async def gram_inline_handler(self, query):
        """Scan GRAM wallet"""
        text = query.query.strip()
        if text.lower().startswith("gram"):
            text = text[4:].strip()

        if not text:
            await query.answer(
                results=[self._make_article(
                    "hint",
                    self.strings["hint_title"],
                    self.strings["hint_desc"],
                    self.strings["hint_msg"],
                )],
                cache_time=0,
                private=True,
            )
            return

        addr = text.strip()
        if not GRAM_ADDR_RE.match(addr):
            await query.answer(
                results=[self._make_article(
                    "inv",
                    self.strings["invalid_title"],
                    self.strings["invalid_desc"],
                    self.strings["invalid_msg"],
                )],
                cache_time=0,
                private=True,
            )
            return

        task_key = f"gram_{addr}"

        if task_key in self._pending:
            fut = self._pending[task_key]
            if fut.done():
                self._pending.pop(task_key, None)
                try:
                    res = fut.result()
                except Exception:
                    res = {"error": "Internal error"}

                if "error" in res:
                    await query.answer(
                        results=[self._make_article(
                            "err",
                            self.strings["err_title"],
                            str(res["error"])[:100],
                            f"<b>GRAMScanner:</b> {escape_html(str(res['error']))}",
                        )],
                        cache_time=0,
                        private=True,
                    )
                elif "message" in res:
                    await query.answer(
                        results=[self._make_article(
                            "res",
                            "GRAMScanner",
                            f"Wallet: {res.get('addr', '?')[:20]}...",
                            res["message"],
                        )],
                        cache_time=0,
                        private=True,
                    )
                else:
                    await query.answer(
                        results=[self._make_article(
                            "unk",
                            self.strings["err_title"],
                            "Unknown error",
                            "<b>GRAMScanner:</b> Unknown error",
                        )],
                        cache_time=0,
                        private=True,
                    )
                return

            await query.answer(
                results=[self._make_article(
                    "ld",
                    self.strings["loading_title"],
                    self.strings["loading_desc"],
                    self.strings["loading_msg"],
                )],
                cache_time=0,
                private=True,
            )
            return

        self._pending[task_key] = asyncio.ensure_future(self._scan_task(addr))

        await query.answer(
            results=[self._make_article(
                "ld",
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