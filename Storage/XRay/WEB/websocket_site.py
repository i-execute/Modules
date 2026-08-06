import asyncio
import urllib.request
from aiohttp import web, ClientSession, WSMsgType

PATH = "__PATH__"
BACKEND = "ws://127.0.0.1:__BACKEND_PORT__" + PATH
GATE_JSX = "__MASK_URL__"

HTML = """<!doctype html>
<html><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"><title>Secure channel</title>
<style>
html,body,#root{margin:0;width:100%;min-height:100%;background:#05070a;color:#d7e2ea}
body{font-family:Inter,Arial,sans-serif}.gate{min-height:100vh;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:14px;background:radial-gradient(circle at 50% 35%,rgba(56,189,248,.12),transparent 48%),#05070a}.title,.subtitle{font-family:monospace;letter-spacing:.12em;text-transform:uppercase}.title{font-size:14px;color:#7dd3fc}.subtitle{font-size:11px;color:#5f7982}
</style></head><body><div id=\"root\"><main class=\"gate\"><div class=\"title\">secure channel</div><div class=\"subtitle\">online</div></main></div>
<script>
(function(){
  function load(src){return new Promise(function(resolve,reject){var s=document.createElement('script');s.src=src;s.onload=resolve;s.onerror=reject;document.head.appendChild(s);});}
  load('https://unpkg.com/react@18/umd/react.production.min.js').then(function(){return load('https://unpkg.com/react-dom@18/umd/react-dom.production.min.js');}).then(function(){return load('https://unpkg.com/@babel/standalone/babel.min.js');}).then(function(){return fetch('/gate.jsx');}).then(function(r){if(!r.ok)throw Error('gate '+r.status);return r.text();}).then(function(src){var compiled=Babel.transform(src,{presets:['react']}).code;new Function('React','ReactDOM',compiled+';if(window.App)ReactDOM.createRoot(document.getElementById('+'\"root\"'+')).render(React.createElement(window.App));')(window.React,window.ReactDOM);}).catch(function(){});
})();
</script></body></html>"""

async def gate_jsx(request):
    try:
        with urllib.request.urlopen(GATE_JSX, timeout=10) as response:
            body = response.read()
        return web.Response(body=body, content_type="application/javascript")
    except Exception:
        return web.Response(text="window.App=function(){return React.createElement('main',null,'secure channel')};", content_type="application/javascript")

async def index(request):
    return web.Response(text=HTML, content_type="text/html")

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
