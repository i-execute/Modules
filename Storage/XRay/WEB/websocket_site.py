import asyncio
import urllib.request
from aiohttp import web, ClientSession, WSMsgType

PATH = "__PATH__"
BACKEND = "ws://127.0.0.1:__BACKEND_PORT__" + PATH
GATE_JSX = "__MASK_URL__"

HTML = r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"><title>Connecting</title>
<style>
:root{color-scheme:dark;--bg:#080b0b;--ink:#eef4ed;--mute:#93a49a;--line:rgba(214,235,211,.16);--leaf:#9fbd5f;--ember:#d9964a}*{box-sizing:border-box}html,body,#root{margin:0;min-height:100%;background:var(--bg)}body{overflow:hidden;font-family:ui-sans-serif,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:var(--ink)}.loader{position:fixed;inset:0;display:grid;place-items:center;padding:24px;background:radial-gradient(60% 46% at 50% 44%,rgba(154,186,94,.11),transparent 72%),var(--bg)}.loader__card{width:min(360px,100%);text-align:left}.loader__mark{position:relative;width:46px;height:46px;margin:0 0 28px;border:1px solid var(--line);border-radius:50%;background:linear-gradient(135deg,rgba(159,189,95,.17),rgba(217,150,74,.06))}.loader__mark:before,.loader__mark:after{position:absolute;content:"";background:var(--leaf)}.loader__mark:before{width:2px;height:18px;left:22px;top:8px;transform:rotate(35deg);transform-origin:bottom}.loader__mark:after{width:7px;height:7px;border-radius:100% 0 100% 0;left:25px;top:12px;transform:rotate(14deg)}.loader__eyebrow{margin:0 0 9px;color:var(--mute);font:11px/1.2 ui-monospace,SFMono-Regular,Menlo,monospace;letter-spacing:.15em;text-transform:uppercase}.loader h1{margin:0;font-size:clamp(30px,8vw,42px);font-weight:500;letter-spacing:-.045em}.loader__row{display:flex;align-items:center;gap:11px;margin-top:25px;color:var(--mute);font-size:13px}.loader__track{flex:1;height:1px;background:var(--line);overflow:hidden}.loader__track i{display:block;width:36%;height:100%;background:linear-gradient(90deg,var(--leaf),var(--ember));animation:scan 1.1s ease-in-out infinite}.loader__status{min-width:70px;text-align:right;font:11px/1 ui-monospace,SFMono-Regular,Menlo,monospace;letter-spacing:.08em;text-transform:uppercase}@keyframes scan{from{transform:translateX(-115%)}to{transform:translateX(315%)}}@media(prefers-reduced-motion:reduce){.loader__track i{animation:none;transform:translateX(90%)}}
</style></head><body><div id="root"><main class="loader" aria-live="polite"><div class="loader__card"><div class="loader__mark" aria-hidden="true"></div><p class="loader__eyebrow">private network</p><h1>Preparing your connection.</h1><div class="loader__row"><div class="loader__track"><i></i></div><span class="loader__status">loading</span></div></div></main></div>
<script>
(function(){
 const root=document.getElementById('root'), fallback=root.innerHTML;
 const load=s=>new Promise((ok,no)=>{const e=document.createElement('script');e.src=s;e.onload=ok;e.onerror=no;document.head.appendChild(e)});
 const fail=()=>{root.innerHTML=fallback};
 Promise.resolve().then(()=>load('https://unpkg.com/react@18.3.1/umd/react.production.min.js')).then(()=>load('https://unpkg.com/react-dom@18.3.1/umd/react-dom.production.min.js')).then(()=>load('https://unpkg.com/@babel/standalone@7.25.9/babel.min.js')).then(()=>fetch('/gate.jsx',{cache:'no-store'})).then(r=>{if(!r.ok)throw Error('cover '+r.status);return r.text()}).then(src=>{const js=Babel.transform(src,{presets:['react']}).code;new Function('React','ReactDOM',js+';if(!window.App)throw Error("App missing");ReactDOM.createRoot(document.getElementById("root")).render(React.createElement(window.App));')(window.React,window.ReactDOM)}).catch(fail);
})();
</script></body></html>"""

async def gate_jsx(request):
    try:
        with urllib.request.urlopen(GATE_JSX, timeout=10) as response:
            body = response.read()
        return web.Response(body=body, content_type="application/javascript", headers={"Cache-Control": "no-store"})
    except Exception:
        return web.Response(text="window.App=function(){return React.createElement('main',null)};", content_type="application/javascript")

async def index(request):
    return web.Response(text=HTML, content_type="text/html", headers={"Cache-Control": "no-store"})

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
