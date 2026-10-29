import asyncio
import hashlib
import os
import re
import urllib.parse
import urllib.request
from aiohttp import web, ClientSession, WSMsgType

PATH = "__PATH__"
BACKEND = "ws://127.0.0.1:__BACKEND_PORT__" + PATH
MASK_KIND = "__MASK_KIND__"
MASK_URL = "__MASK_URL__"
LOADING_HTML = "__LOADING_URL__"
MASK_FILES = __MASK_FILES__
ASSET_HOSTS = {"raw.githubusercontent.com", "github.com", "objects.githubusercontent.com"}
EXTRA_HOSTS = {item.strip().lower() for item in os.environ.get("XRAY_MASK_HOSTS", "").split(",") if item.strip()}
HTML_LIMIT = 256 * 1024
ASSET_LIMIT = 8 * 1024 * 1024
CACHE_TTL = 300.0
CACHE_BYTES = 24 * 1024 * 1024
CACHE_ENTRIES = 32
ASSET_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
DIGEST = re.compile(r"^[0-9a-f]{64}$")
TYPES = {
    ".js": "text/javascript; charset=utf-8",
    ".mjs": "text/javascript; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".json": "application/json",
    ".map": "application/json",
    ".svg": "image/svg+xml",
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".webp": "image/webp",
    ".gif": "image/gif",
    ".woff": "font/woff",
    ".woff2": "font/woff2",
    ".ttf": "font/ttf",
    ".mp3": "audio/mpeg",
    ".wav": "audio/wav",
}
FALLBACK = (
    "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">"
    "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">"
    "<title>Downloads</title><style>html,body{margin:0;height:100%;background:#0b0d10;color:#e7e7e8;"
    "font:16px system-ui,sans-serif}main{height:100%;display:grid;place-items:center}"
    "b{font-weight:600;font-size:22px;letter-spacing:.4px}</style></head>"
    "<body><main><b>Downloads</b></main></body></html>"
)
CACHE = {}
CACHE_LOCK = asyncio.Lock()


def allowed(url):
    parsed = urllib.parse.urlsplit(str(url or ""))
    host = (parsed.hostname or "").lower()
    if host not in ASSET_HOSTS and host not in EXTRA_HOSTS:
        return False
    if parsed.scheme == "https":
        return True
    return host in EXTRA_HOSTS


def expected_digest(key):
    value = str(MASK_FILES.get(key) or "").strip().lower()
    return value if DIGEST.match(value) else ""


def download(url, limit):
    if not allowed(url):
        raise ValueError("host is not allowed")
    request = urllib.request.Request(url, headers={"User-Agent": "curl/8.5.0"})
    with urllib.request.urlopen(request, timeout=15) as response:
        body = response.read(limit + 1)
    if len(body) > limit:
        raise ValueError("the body is too large")
    return body


async def fetch(url, limit, key):
    body = await asyncio.to_thread(download, url, limit)
    want = expected_digest(key)
    if want and hashlib.sha256(body).hexdigest() != want:
        raise ValueError("the digest does not match for " + key)
    return body


async def cached(key, url, limit):
    now = asyncio.get_running_loop().time()
    async with CACHE_LOCK:
        hit = CACHE.get(key)
        if hit and hit[0] > now:
            return hit[1]
        if hit:
            CACHE.pop(key, None)
    body = await fetch(url, limit, key)
    total = sum(len(item[1]) for item in CACHE.values()) + len(body)
    async with CACHE_LOCK:
        while CACHE and (total > CACHE_BYTES or len(CACHE) >= CACHE_ENTRIES):
            old = next(iter(CACHE))
            total -= len(CACHE[old][1])
            CACHE.pop(old, None)
        CACHE[key] = (now + CACHE_TTL, body)
    return body


def html_response(body):
    return web.Response(text=body, content_type="text/html", headers={"Cache-Control": "no-store"})


async def index(request):
    if MASK_KIND == "hole":
        try:
            body = await cached("index.html", MASK_URL.rstrip("/") + "/index.html", HTML_LIMIT)
            return html_response(body.decode("utf-8", "replace"))
        except Exception:
            return html_response(FALLBACK)
    try:
        body = await cached("loading", LOADING_HTML, HTML_LIMIT)
        return html_response(body.decode("utf-8", "replace"))
    except Exception:
        return html_response(FALLBACK)


async def gate_jsx(request):
    if MASK_KIND == "hole":
        raise web.HTTPNotFound()
    try:
        body = await cached("gate", MASK_URL, HTML_LIMIT)
        return web.Response(text=body.decode("utf-8", "replace"), content_type="application/javascript", headers={"Cache-Control": "no-store"})
    except Exception:
        return web.Response(text="window.App=function(){return React.createElement('main',null)};", content_type="application/javascript")


async def asset(request):
    name = str(request.match_info.get("name") or "")
    if not ASSET_NAME.match(name) or "/" in name or name.startswith("."):
        raise web.HTTPNotFound()
    extension = os.path.splitext(name)[1].lower()
    content_type = TYPES.get(extension)
    if not content_type:
        raise web.HTTPNotFound()
    key = "assets/" + name
    if key not in MASK_FILES:
        raise web.HTTPNotFound()
    try:
        body = await cached(key, MASK_URL.rstrip("/") + "/" + key, ASSET_LIMIT)
    except Exception:
        raise web.HTTPNotFound()
    return web.Response(body=body, headers={"Content-Type": content_type, "Cache-Control": "public, max-age=300"})


async def favicon(request):
    raise web.HTTPNotFound()


async def health(request):
    return web.Response(text="ok\n")


async def proxy(request):
    client_ws = web.WebSocketResponse(autoping=False, heartbeat=30)
    await client_ws.prepare(request)
    try:
        async with ClientSession() as session:
            async with session.ws_connect(BACKEND, autoping=False, heartbeat=30) as backend_ws:
                async def forward(source, target):
                    async for message in source:
                        if message.type == WSMsgType.BINARY:
                            await target.send_bytes(message.data)
                        elif message.type == WSMsgType.TEXT:
                            await target.send_str(message.data)
                        elif message.type == WSMsgType.PING:
                            await target.ping()
                        elif message.type == WSMsgType.PONG:
                            await target.pong()
                        elif message.type in (WSMsgType.CLOSE, WSMsgType.CLOSED, WSMsgType.ERROR):
                            break
                await asyncio.gather(forward(client_ws, backend_ws), forward(backend_ws, client_ws), return_exceptions=True)
    except Exception:
        if not client_ws.closed:
            await client_ws.close(code=1011, message=b"backend unavailable")
    return client_ws


app = web.Application()
app.router.add_get(PATH, proxy)
app.router.add_get("/", index)
app.router.add_get("/gate.jsx", gate_jsx)
app.router.add_get("/assets/{name}", asset)
app.router.add_get("/favicon.ico", favicon)
app.router.add_get("/health", health)
web.run_app(app, host="127.0.0.1", port=__SITE_PORT__)
