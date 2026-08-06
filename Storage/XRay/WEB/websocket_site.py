import asyncio
import urllib.request
from aiohttp import web, ClientSession, WSMsgType

PATH = "__PATH__"
BACKEND = "ws://127.0.0.1:__BACKEND_PORT__" + PATH
GATE_JSX = "__MASK_URL__"
LOADING_HTML = "https://raw.githubusercontent.com/i-execute/Modules/main/Storage/XRay/WEB/Loading.html?v=loading-v4"

async def fetch_text(url, timeout=10):
    def read():
        with urllib.request.urlopen(url, timeout=timeout) as response:
            return response.read().decode("utf-8")
    return await asyncio.to_thread(read)

async def index(request):
    try:
        html = await fetch_text(LOADING_HTML)
    except Exception:
        html = "<!doctype html><title>Connecting</title><body style='margin:0;background:#111214;color:#e7e7e8;font:16px system-ui;display:grid;place-items:center;min-height:100vh'>Preparing your connection.</body>"
    return web.Response(text=html, content_type="text/html", headers={"Cache-Control": "no-store"})

async def gate_jsx(request):
    try:
        body = await fetch_text(GATE_JSX)
        return web.Response(text=body, content_type="application/javascript", headers={"Cache-Control": "no-store"})
    except Exception:
        return web.Response(text="window.App=function(){return React.createElement('main',null)};", content_type="application/javascript")

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
app.router.add_get('/', index)
app.router.add_get('/gate.jsx', gate_jsx)
web.run_app(app, host='127.0.0.1', port=__SITE_PORT__)
