__version__ = (1, 1, 0)
# meta developer: FireJester.t.me
# requires: aiohttp

import time
import logging
import asyncio
import html
from html.parser import HTMLParser

from aiogram.types import (
    InlineQuery,
    InlineQueryResultArticle,
    InputTextMessageContent,
)

from .. import loader, utils

logger = logging.getLogger(__name__)

BANNER = "https://github.com/FireJester/Modules/raw/main/Assets/DevTool/Inline_query.png"
TL_BASE = "https://tl.telethon.dev"
METHODS_CACHE_TTL = 86400
DETAIL_CACHE_TTL = 3600
PAGE_SIZE = 10


class _LinksParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links = []
        self._cur_href = None
        self._in_a = False
        self._buf = ""

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            a = dict(attrs)
            href = a.get("href", "")
            if href and not href.startswith("http") and not href.startswith("#"):
                self._cur_href = href
                self._in_a = True
                self._buf = ""

    def handle_endtag(self, tag):
        if tag == "a" and self._in_a:
            self._in_a = False
            if self._cur_href:
                self.links.append((self._cur_href, self._buf.strip()))
            self._cur_href = None
            self._buf = ""

    def handle_data(self, data):
        if self._in_a:
            self._buf += data


class _DetailParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self._paragraphs = []
        self._in_p = False
        self._p_buf = ""

        self._table_rows = []
        self._row = []
        self._in_td = False
        self._td_buf = ""

        self._pres = []
        self._in_pre = False
        self._pre_buf = ""
        self._pre_count = 0

    def handle_starttag(self, tag, attrs):
        if tag == "p":
            self._in_p = True
            self._p_buf = ""
        elif tag == "tr":
            self._row = []
        elif tag == "td":
            self._in_td = True
            self._td_buf = ""
        elif tag == "pre":
            self._in_pre = True
            self._pre_buf = ""

    def handle_endtag(self, tag):
        if tag == "p":
            self._in_p = False
            t = self._p_buf.strip()
            if t:
                self._paragraphs.append(t)
        elif tag == "td":
            self._in_td = False
            self._row.append(self._td_buf.strip())
        elif tag == "tr":
            if self._row:
                self._table_rows.append(self._row[:])
                self._row = []
        elif tag == "pre":
            self._in_pre = False
            self._pres.append(self._pre_buf)
            self._pre_count += 1

    def handle_data(self, data):
        if self._in_p:
            self._p_buf += data
        if self._in_td:
            self._td_buf += data
        if self._in_pre:
            self._pre_buf += data

    @property
    def description(self):
        for p in self._paragraphs:
            if "---functions---" not in p:
                return p.strip()
        return ""

    @property
    def params(self):
        rows = []
        for row in self._table_rows:
            if len(row) >= 2 and row[0] and row[0] not in ("", "Name", "Type"):
                rows.append(row)
        return rows

    @property
    def example(self):
        if self._pres:
            return self._pres[-1].strip()
        return ""


async def _fetch(url: str, timeout: int = 20):
    import aiohttp
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as r:
                if r.status != 200:
                    return None
                return await r.text()
    except Exception as ex:
        logger.debug("_fetch error %s: %s", url, ex)
        return None


async def _collect_methods_from_page(page_url: str, base_url: str):
    text = await _fetch(page_url)
    if not text:
        return []
    p = _LinksParser()
    p.feed(text)
    results = []
    for href, text_content in p.links:
        if href.endswith("index.html") or not href.endswith(".html"):
            continue
        if not text_content.endswith("Request"):
            continue
        if base_url.endswith("/"):
            abs_href = base_url + href
        else:
            abs_href = base_url.rsplit("/", 1)[0] + "/" + href
        rel = abs_href.replace(TL_BASE, "")
        results.append({"name": text_content, "href": rel})
    return results


async def _load_index():
    root_url = TL_BASE + "/methods/"
    text = await _fetch(root_url)
    if not text:
        return []

    p = _LinksParser()
    p.feed(text)

    category_urls = []
    root_methods = []

    for href, label in p.links:
        if not href.endswith(".html"):
            continue
        if href.endswith("index.html") and "/" in href:
            category_urls.append(TL_BASE + "/methods/" + href)
        elif not href.startswith("../") and label.endswith("Request"):
            rel = "/methods/" + href
            root_methods.append({"name": label, "href": rel})

    category_results = await asyncio.gather(
        *[_collect_methods_from_page(cu, cu.rsplit("/", 1)[0] + "/") for cu in category_urls],
        return_exceptions=True,
    )

    all_methods = list(root_methods)
    for res in category_results:
        if isinstance(res, list):
            all_methods.extend(res)

    seen = set()
    deduped = []
    for m in all_methods:
        if m["href"] not in seen:
            seen.add(m["href"])
            deduped.append(m)

    return deduped


async def _load_detail(href: str):
    url = TL_BASE + href
    text = await _fetch(url)
    if not text:
        return None

    p = _DetailParser()
    p.feed(text)

    filename = href.rstrip("/").split("/")[-1].replace(".html", "")
    name = filename

    return {
        "name": name,
        "description": p.description,
        "params": p.params,
        "example": p.example,
        "url": url,
    }


def _search(items, query: str):
    q = query.lower().strip()
    if not q:
        return items
    name_hits = []
    for item in items:
        n = item.get("name", "").lower()
        if q in n:
            name_hits.append(item)
    return name_hits


def _build_result_message(detail: dict, display_name: str, url: str) -> str:
    e = html.escape
    lines = []

    lines.append('<b><a href="' + url + '">DevTool</a> - ' + e(display_name) + "</b>")

    if detail["description"]:
        lines.append("<blockquote>" + e(detail["description"]) + "</blockquote>")

    if detail["params"]:
        lines.append("<b>Parameters:</b>")
        param_lines = []
        for row in detail["params"][:10]:
            param_name = row[0] if len(row) > 0 else ""
            param_type = row[1] if len(row) > 1 else ""
            optional = " <i>(opt.)</i>" if len(row) > 2 and "optional" in row[2].lower() else ""
            if param_name:
                param_lines.append("  -> <code>" + e(param_name) + "</code> - " + e(param_type) + optional)
        lines.append("<blockquote>" + "\n".join(param_lines) + "</blockquote>")

    if detail["example"]:
        lines.append("<b>Example:</b>")
        lines.append('<pre><code class="language-DevTool">' + e(detail["example"]) + "</code></pre>")

    return "\n".join(lines)


@loader.tds
class DevTool(loader.Module):
    """Telethon methods reference"""

    strings = {
        "name": "DevTool",
        "hint_title": "DevTool - Telethon Docs",
        "hint_desc": "Start typing a method name...",
        "hint_msg": (
            "<b>DevTool:</b> Start typing a Telethon method name\n"
            "Example: <code>SendMessage</code>, <code>GetHistory</code>"
        ),
        "loading_title": "Loading...",
        "loading_desc": "Building method index, please retry",
        "loading_msg": (
            "<b>DevTool:</b> Building method index... "
            "Please retry in a few seconds."
        ),
        "no_results_title": "Nothing found",
        "no_results_desc": "Try a different query",
        "no_results_msg": "<b>DevTool:</b> No methods found for <code>{query}</code>.",
    }

    strings_ru = {
        "hint_title": "DevTool - Telethon Docs",
        "hint_desc": "Начните вводить название метода...",
        "hint_msg": (
            "<b>DevTool:</b> Начните вводить название метода Telethon\n"
            "Например: <code>SendMessage</code>, <code>GetHistory</code>"
        ),
        "loading_title": "Загрузка...",
        "loading_desc": "Собираю базу методов, повторите запрос",
        "loading_msg": (
            "<b>DevTool:</b> Собираю базу методов... "
            "Повторите запрос через несколько секунд."
        ),
        "no_results_title": "Ничего не найдено",
        "no_results_desc": "Попробуйте другой запрос",
        "no_results_msg": "<b>DevTool:</b> Методы по запросу <code>{query}</code> не найдены.",
    }

    def __init__(self):
        self.inline_bot = None
        self._index_cache = None
        self._index_ts = 0
        self._index_lock = None
        self._detail_cache = {}

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        self._index_lock = asyncio.Lock()
        if hasattr(self, "inline") and hasattr(self.inline, "bot"):
            self.inline_bot = self.inline.bot
        asyncio.ensure_future(self._get_index())

    async def _get_index(self):
        async with self._index_lock:
            now = time.time()
            if self._index_cache and (now - self._index_ts) < METHODS_CACHE_TTL:
                return self._index_cache
            items = await _load_index()
            if items:
                self._index_cache = items
                self._index_ts = now
                logger.debug("DevTool: loaded %d methods", len(items))
            return self._index_cache or None

    async def _get_detail(self, href: str):
        entry = self._detail_cache.get(href)
        if entry and (time.time() - entry["ts"]) < DETAIL_CACHE_TTL:
            return entry["data"]
        detail = await _load_detail(href)
        if detail:
            self._detail_cache[href] = {"data": detail, "ts": time.time()}
        return detail

    async def _answer(self, query, results, next_offset=""):
        try:
            await self.inline_bot.answer_inline_query(
                inline_query_id=query.id,
                results=results,
                cache_time=0,
                is_personal=True,
                next_offset=next_offset,
            )
        except Exception as ex:
            logger.debug("answer_inline_query error: %s", ex)

    def _make_article(self, uid, title, description, message_text):
        return InlineQueryResultArticle(
            id=uid,
            title=title,
            description=description,
            input_message_content=InputTextMessageContent(
                message_text=message_text,
                parse_mode="HTML",
                disable_web_page_preview=True,
            ),
            thumbnail_url=BANNER,
            thumbnail_width=640,
            thumbnail_height=360,
        )

    @loader.inline_handler(
        ru_doc="Справочник методов Telethon",
        en_doc="Telethon methods reference",
    )
    async def dev_inline_handler(self, query: InlineQuery):
        """Telethon methods reference"""
        raw = query.query.strip()
        if raw.lower().startswith("dev"):
            raw = raw[3:].strip()

        if not raw:
            await self._answer(query, [
                self._make_article(
                    "hint_" + str(int(time.time())),
                    self.strings["hint_title"],
                    self.strings["hint_desc"],
                    self.strings["hint_msg"],
                )
            ])
            return

        offset = int(query.offset or 0)
        index = await self._get_index()

        if not index:
            await self._answer(query, [
                self._make_article(
                    "ld_" + str(int(time.time())),
                    self.strings["loading_title"],
                    self.strings["loading_desc"],
                    self.strings["loading_msg"],
                )
            ])
            return

        results_all = _search(index, raw)

        if not results_all:
            await self._answer(query, [
                self._make_article(
                    "nr_" + str(int(time.time())),
                    self.strings["no_results_title"],
                    self.strings["no_results_desc"],
                    self.strings["no_results_msg"].format(query=html.escape(raw)),
                )
            ])
            return

        page = results_all[offset: offset + PAGE_SIZE]
        next_off = str(offset + PAGE_SIZE) if (offset + PAGE_SIZE) < len(results_all) else ""

        hrefs = [item["href"] for item in page]
        details = await asyncio.gather(
            *[self._get_detail(h) for h in hrefs],
            return_exceptions=True,
        )

        articles = []
        for item, detail in zip(page, details):
            display_name = item.get("name", "?")
            url = TL_BASE + item["href"]

            if isinstance(detail, dict) and detail:
                msg = _build_result_message(detail, display_name, url)
            else:
                msg = (
                    '<b><a href="' + url + '">DevTool</a> - ' + html.escape(display_name) + "</b>"
                )

            articles.append(self._make_article(
                uid="m_" + display_name + "_" + str(offset),
                title=display_name,
                description="Telethon method",
                message_text=msg,
            ))

        await self._answer(query, articles, next_offset=next_off)

    async def on_unload(self):
        self._index_cache = None
        self._detail_cache.clear()