__version__ = (1, 0, 0)
# meta developer: I_execute.t.me

import asyncio
import json
import logging
import os
import platform
import shutil
import stat
import subprocess
import tempfile
import threading

from telethon.tl.types import Message

from .. import loader, utils
from ..inline.types import InlineCall

logger = logging.getLogger(__name__)

CF_REPO = "cloudflare/cloudflared"
VERSIONS_PER_PAGE = 5


def _escape(text):
    if not text:
        return ""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _cf_arch():
    m = platform.machine().lower()
    if "arm" in m or "aarch64" in m:
        return "arm64"
    return "amd64"


@loader.tds
class WebDeployer(loader.Module):
    """Deploy .js/.jsx files to temporary Cloudflare domains"""

    strings = {
        "name": "WebDeployer",
        "main_menu": (
            "<b>WebDeployer</b>\n"
            "<blockquote>"
            "cloudflared: {cf_status}\n"
            "Sites: {sites_count}"
            "</blockquote>"
        ),
        "setup_menu": (
            "<b>Setup</b>\n"
            "<blockquote>"
            "cloudflared: {cf_status}\n"
            "GitHub token: {gh_status}"
            "</blockquote>"
        ),
        "cf_install_menu": (
            "<b>cloudflared Installation</b>\n"
            "<blockquote>"
            "Installed: {current}\n"
            "Select version:"
            "</blockquote>"
        ),
        "cf_installing": (
            "<b>Installing cloudflared</b>\n"
            "<blockquote>Version: {version}\nPlease wait...</blockquote>"
        ),
        "install_done": (
            "<b>Installation Complete</b>\n"
            "<blockquote>cloudflared {version} installed</blockquote>"
        ),
        "install_fail": (
            "<b>Installation Failed</b>\n"
            "<blockquote>{error}</blockquote>"
        ),
        "collecting_versions": "<b>Collecting versions...</b>",
        "no_reply": "<b>Reply to a .js or .jsx file</b>",
        "wrong_type": "<b>File must be .js or .jsx</b>",
        "no_cf": (
            "<b>cloudflared not installed</b>\n"
            "<blockquote>Use .wb to open Setup</blockquote>"
        ),
        "downloading": "<b>Downloading file</b>\n<blockquote><code>{name}</code></blockquote>",
        "deploying": (
            "<b>Deploying</b>\n"
            "<blockquote>File: <code>{name}</code>\nPlease wait...</blockquote>"
        ),
        "deploy_fail": (
            "<b>Deploy Failed</b>\n"
            "<blockquote>{error}</blockquote>"
        ),
        "deployed": (
            "<b>Site Deployed</b>\n"
            "<blockquote>"
            "File: <code>{name}</code>\n"
            "URL: <code>{url}</code>"
            "</blockquote>"
        ),
        "sites_menu": (
            "<b>Active Sites</b>\n"
            "<blockquote>Total: {count}</blockquote>"
        ),
        "no_sites": (
            "<b>No active sites</b>\n"
            "<blockquote>Reply to .js/.jsx file with .wb to deploy</blockquote>"
        ),
        "site_detail": (
            "<b>Site Info</b>\n"
            "<blockquote>"
            "File: <code>{name}</code>\n"
            "URL: <code>{url}</code>\n"
            "Port: <code>{port}</code>"
            "</blockquote>"
        ),
        "site_stopped": (
            "<b>Site Stopped</b>\n"
            "<blockquote>URL: <code>{url}</code></blockquote>"
        ),
        "site_not_found": "<b>Site not found</b>",
        "token_set": (
            "<b>GitHub token saved</b>\n"
            "<blockquote>Rate limit raised to 5000 req/h</blockquote>"
        ),
        "token_cleared": "<b>GitHub token cleared</b>",
        "cf_fail": "<b>Failed to get tunnel URL</b>",
        "btn_setup": "Setup",
        "btn_back": "Back",
        "btn_close": "Close",
        "btn_sites": "Active Sites",
        "btn_stop": "Stop Site",
        "btn_set_token": "Set GitHub Token",
        "btn_clear_token": "Clear Token",
        "btn_install_cf": "Install / Update cloudflared",
        "btn_left": "←",
        "btn_right": "→",
        "input_token": "Paste GitHub Personal Access Token:",
    }

    strings_ru = {
        "main_menu": (
            "<b>WebDeployer</b>\n"
            "<blockquote>"
            "cloudflared: {cf_status}\n"
            "Сайтов: {sites_count}"
            "</blockquote>"
        ),
        "setup_menu": (
            "<b>Настройка</b>\n"
            "<blockquote>"
            "cloudflared: {cf_status}\n"
            "GitHub токен: {gh_status}"
            "</blockquote>"
        ),
        "cf_install_menu": (
            "<b>Установка cloudflared</b>\n"
            "<blockquote>"
            "Установлен: {current}\n"
            "Выберите версию:"
            "</blockquote>"
        ),
        "cf_installing": (
            "<b>Установка cloudflared</b>\n"
            "<blockquote>Версия: {version}\nПодождите...</blockquote>"
        ),
        "install_done": (
            "<b>Установка завершена</b>\n"
            "<blockquote>cloudflared {version} установлен</blockquote>"
        ),
        "install_fail": (
            "<b>Ошибка установки</b>\n"
            "<blockquote>{error}</blockquote>"
        ),
        "collecting_versions": "<b>Сбор версий...</b>",
        "no_reply": "<b>Ответьте на .js или .jsx файл</b>",
        "wrong_type": "<b>Файл должен быть .js или .jsx</b>",
        "no_cf": (
            "<b>cloudflared не установлен</b>\n"
            "<blockquote>Используйте .wb для Setup</blockquote>"
        ),
        "downloading": "<b>Скачивание файла</b>\n<blockquote><code>{name}</code></blockquote>",
        "deploying": (
            "<b>Деплой</b>\n"
            "<blockquote>Файл: <code>{name}</code>\nПодождите...</blockquote>"
        ),
        "deploy_fail": (
            "<b>Ошибка деплоя</b>\n"
            "<blockquote>{error}</blockquote>"
        ),
        "deployed": (
            "<b>Сайт задеплоен</b>\n"
            "<blockquote>"
            "Файл: <code>{name}</code>\n"
            "URL: <code>{url}</code>"
            "</blockquote>"
        ),
        "sites_menu": (
            "<b>Активные сайты</b>\n"
            "<blockquote>Всего: {count}</blockquote>"
        ),
        "no_sites": (
            "<b>Нет активных сайтов</b>\n"
            "<blockquote>Ответьте на .js/.jsx файл командой .wb для деплоя</blockquote>"
        ),
        "site_detail": (
            "<b>Информация о сайте</b>\n"
            "<blockquote>"
            "Файл: <code>{name}</code>\n"
            "URL: <code>{url}</code>\n"
            "Порт: <code>{port}</code>"
            "</blockquote>"
        ),
        "site_stopped": (
            "<b>Сайт остановлен</b>\n"
            "<blockquote>URL: <code>{url}</code></blockquote>"
        ),
        "site_not_found": "<b>Сайт не найден</b>",
        "token_set": (
            "<b>GitHub токен сохранён</b>\n"
            "<blockquote>Лимит запросов повышен до 5000/ч</blockquote>"
        ),
        "token_cleared": "<b>GitHub токен очищен</b>",
        "cf_fail": "<b>Не удалось получить URL туннеля</b>",
        "btn_setup": "Настройка",
        "btn_back": "Назад",
        "btn_close": "Закрыть",
        "btn_sites": "Активные сайты",
        "btn_stop": "Остановить сайт",
        "btn_set_token": "Установить GitHub токен",
        "btn_clear_token": "Очистить токен",
        "btn_install_cf": "Установить / Обновить cloudflared",
        "btn_left": "←",
        "btn_right": "→",
        "input_token": "Вставьте GitHub Personal Access Token:",
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "PORT_START",
                9000,
                "Starting port for local HTTP servers",
                validator=loader.validators.Integer(minimum=1024),
            ),
        )
        self._root = None
        self._cf_bin = None
        self._db = None
        self._client = None
        self._active = {}
        self._releases_cache = None

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        me = await client.get_me()
        self._root = os.path.join(tempfile.gettempdir(), f"webdeploy_{me.id}")
        self._cf_bin = os.path.join(self._root, "cloudflared")
        os.makedirs(self._root, exist_ok=True)
        for site_id in list(self._get_sites().keys()):
            site = self._get_sites()[site_id]
            pid = site.get("pid")
            if pid:
                try:
                    os.kill(pid, 0)
                except OSError:
                    self._remove_site(site_id)

    async def on_unload(self):
        for site_id in list(self._active.keys()):
            self._kill_site(site_id)

    def _cf_installed(self):
        return bool(self._cf_bin and os.path.isfile(self._cf_bin))

    def _cf_version(self):
        if not self._cf_installed():
            return "not installed"
        return self._db.get("WebDeployer", "cf_version", "unknown")

    def _gh_token(self):
        return self._db.get("WebDeployer", "gh_token", "")

    def _gh_headers(self):
        headers = []
        token = self._gh_token()
        if token:
            headers += ["-H", f"Authorization: Bearer {token}"]
        return headers

    def _get_sites(self) -> dict:
        return self._db.get("WebDeployer", "sites", {})

    def _set_sites(self, sites: dict):
        self._db.set("WebDeployer", "sites", sites)

    def _add_site(self, site_id: str, data: dict):
        sites = self._get_sites()
        sites[site_id] = data
        self._set_sites(sites)

    def _remove_site(self, site_id: str):
        sites = self._get_sites()
        sites.pop(site_id, None)
        self._set_sites(sites)

    def _kill_site(self, site_id: str):
        procs = self._active.pop(site_id, {})
        for key in ("http_proc", "cf_proc"):
            proc = procs.get(key)
            if proc:
                try:
                    proc.terminate()
                except Exception:
                    pass
        site = self._get_sites().get(site_id, {})
        site_dir = site.get("dir")
        if site_dir and os.path.isdir(site_dir):
            shutil.rmtree(site_dir, ignore_errors=True)
        self._remove_site(site_id)

    def _next_port(self) -> int:
        used = set()
        for site in self._get_sites().values():
            p = site.get("port")
            if p:
                used.add(p)
        port = int(self.config["PORT_START"])
        while port in used:
            port += 1
        return port

    async def _curl(self, *args, timeout=15):
        p = await asyncio.create_subprocess_exec(
            "curl", "-sL", "--max-time", str(timeout), *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        out, err = await p.communicate()
        return p.returncode, out, err

    async def _gh_get_releases(self, force: bool = False):
        if self._releases_cache is not None and not force:
            return self._releases_cache
        rc, out, _ = await self._curl(
            *self._gh_headers(),
            f"https://api.github.com/repos/{CF_REPO}/releases",
        )
        if rc != 0:
            return []
        try:
            data = json.loads(out.decode())
            if isinstance(data, list):
                self._releases_cache = data[:20]
                return self._releases_cache
        except Exception:
            pass
        return []

    async def _install_cf_tagged(self, tag: str):
        arch = _cf_arch()
        asset_name = f"cloudflared-linux-{arch}"
        rc, out, _ = await self._curl(
            *self._gh_headers(),
            f"https://api.github.com/repos/{CF_REPO}/releases/tags/{tag}",
        )
        if rc != 0:
            return False, "GitHub API request failed"
        try:
            data = json.loads(out.decode())
        except Exception:
            return False, "GitHub API bad response"
        if "message" in data:
            return False, f"GitHub API: {data['message']}"

        download_url = None
        for asset in data.get("assets", []):
            if asset.get("name", "") == asset_name:
                download_url = asset["browser_download_url"]
                break

        if not download_url:
            names = [a.get("name", "") for a in data.get("assets", [])]
            return False, f"No binary '{asset_name}' in assets: {names}"

        p = await asyncio.create_subprocess_exec(
            "wget", "-q", "--max-redirect=15", "-O", self._cf_bin, download_url,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        _, err = await asyncio.wait_for(p.communicate(), timeout=300)
        size = os.path.getsize(self._cf_bin) if os.path.isfile(self._cf_bin) else 0
        if p.returncode != 0 or size < 100_000:
            return False, f"Download failed (size={size}): {err.decode()[:200]}"

        os.chmod(self._cf_bin, os.stat(self._cf_bin).st_mode | stat.S_IEXEC)
        self._db.set("WebDeployer", "cf_version", tag)
        self._releases_cache = None
        return True, tag

    def _build_html(self, js_path: str, filename: str) -> str:
        with open(js_path, "r", encoding="utf-8", errors="replace") as f:
            js_code = f.read()
        return f"""<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1.0"/>
<title>{_escape(filename)}</title>
<script src="https://unpkg.com/react@18/umd/react.development.js"></script>
<script src="https://unpkg.com/react-dom@18/umd/react-dom.development.js"></script>
<script src="https://unpkg.com/@babel/standalone/babel.min.js"></script>
</head>
<body>
<div id="root"></div>
<script type="text/babel">
{js_code}
const domNode = document.getElementById('root');
const root = ReactDOM.createRoot(domNode);
if (typeof App !== 'undefined') {{
  root.render(React.createElement(App));
}}
</script>
</body>
</html>"""

    def _start_http_server(self, serve_dir: str, port: int) -> subprocess.Popen:
        import sys as _sys
        return subprocess.Popen(
            [_sys.executable, "-m", "http.server", str(port), "--directory", serve_dir],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    def _start_cf_tunnel(self, port: int, result_holder: list):
        try:
            proc = subprocess.Popen(
                [self._cf_bin, "tunnel", "--url", f"http://localhost:{port}", "--no-autoupdate"],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
            result_holder.append(proc)
            for line in proc.stdout:
                if "trycloudflare.com" in line:
                    for part in line.split():
                        if part.startswith("https://") and "trycloudflare.com" in part:
                            result_holder.insert(0, part.strip())
                            return
        except Exception as e:
            logger.error(f"[WebDeployer] cloudflared: {e}")

    async def _cb_close(self, call: InlineCall):
        await call.delete()

    async def _cb_main_menu(self, call: InlineCall):
        sites = self._get_sites()
        await call.edit(
            self.strings["main_menu"].format(
                cf_status=_escape(self._cf_version()),
                sites_count=len(sites),
            ),
            reply_markup=[
                [{"text": self.strings["btn_sites"], "callback": self._cb_sites_menu, "style": "primary"}],
                [{"text": self.strings["btn_setup"], "callback": self._cb_setup_menu, "style": "primary"}],
                [{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}],
            ],
        )

    async def _cb_setup_menu(self, call: InlineCall):
        await call.edit(
            self.strings["setup_menu"].format(
                cf_status=_escape(self._cf_version()),
                gh_status="set" if self._gh_token() else "not set",
            ),
            reply_markup=[
                [{"text": self.strings["btn_install_cf"], "callback": self._cb_cf_versions, "args": (0,), "style": "primary"}],
                [{"text": self.strings["btn_set_token"], "callback": self._cb_set_token_input, "style": "primary"}],
                [{"text": self.strings["btn_clear_token"], "callback": self._cb_clear_token, "style": "danger"}],
                [{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "primary"}],
            ],
        )

    async def _cb_cf_versions(self, call: InlineCall, page: int = 0):
        await call.edit(self.strings["collecting_versions"])
        releases = await self._gh_get_releases()
        if not releases:
            await call.edit(
                self.strings["install_fail"].format(
                    error="Failed to fetch releases. Set a GitHub token in Setup to raise rate limit."
                ),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_setup_menu, "style": "primary"}]],
            )
            return

        current = self._cf_version()
        total_pages = max(1, (len(releases) + VERSIONS_PER_PAGE - 1) // VERSIONS_PER_PAGE)
        page = max(0, min(page, total_pages - 1))
        start = page * VERSIONS_PER_PAGE
        page_releases = releases[start:start + VERSIONS_PER_PAGE]

        markup = []
        for release in page_releases:
            tag = release.get("tag_name", "")
            if not tag:
                continue
            label = f"{tag} (current)" if tag == current else tag
            markup.append([{
                "text": label,
                "callback": self._cb_install_cf_tagged,
                "args": (tag,),
                "style": "primary",
            }])

        nav_row = []
        if page > 0:
            nav_row.append({
                "text": self.strings["btn_left"],
                "callback": self._cb_cf_versions,
                "args": (page - 1,),
                "style": "primary",
            })
        nav_row.append({
            "text": f"{page + 1}/{total_pages}",
            "callback": self._cb_noop,
            "style": "primary",
        })
        if page < total_pages - 1:
            nav_row.append({
                "text": self.strings["btn_right"],
                "callback": self._cb_cf_versions,
                "args": (page + 1,),
                "style": "primary",
            })
        markup.append(nav_row)
        markup.append([{"text": self.strings["btn_back"], "callback": self._cb_setup_menu, "style": "primary"}])

        await call.edit(
            self.strings["cf_install_menu"].format(current=_escape(current)),
            reply_markup=markup,
        )

    async def _cb_noop(self, call: InlineCall):
        await call.answer()

    async def _cb_install_cf_tagged(self, call: InlineCall, tag: str):
        await call.edit(self.strings["cf_installing"].format(version=_escape(tag)))
        ok, result = await self._install_cf_tagged(tag)
        if ok:
            await call.edit(
                self.strings["install_done"].format(version=_escape(result)),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_setup_menu, "style": "primary"}]],
            )
        else:
            await call.edit(
                self.strings["install_fail"].format(error=_escape(result)),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_setup_menu, "style": "primary"}]],
            )

    async def _cb_set_token_input(self, call: InlineCall):
        await call.edit(
            "<b>GitHub Personal Access Token</b>\n"
            "<blockquote>Create at: github.com/settings/tokens\nNo scopes needed</blockquote>",
            reply_markup=[[{
                "text": self.strings["input_token"],
                "input": self.strings["input_token"],
                "handler": self._cb_save_token,
                "style": "primary",
            }]],
        )

    async def _cb_save_token(self, call: InlineCall, token: str):
        token = token.strip()
        if token:
            self._db.set("WebDeployer", "gh_token", token)
            await call.edit(
                self.strings["token_set"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_setup_menu, "style": "primary"}]],
            )
        else:
            await self._cb_setup_menu(call)

    async def _cb_clear_token(self, call: InlineCall):
        self._db.set("WebDeployer", "gh_token", "")
        await call.edit(
            self.strings["token_cleared"],
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_setup_menu, "style": "primary"}]],
        )

    async def _cb_sites_menu(self, call: InlineCall):
        sites = self._get_sites()
        if not sites:
            await call.edit(
                self.strings["no_sites"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "primary"}]],
            )
            return

        markup = []
        for site_id, site in sites.items():
            label = site.get("name", site_id[:8])
            markup.append([{
                "text": label,
                "callback": self._cb_site_detail,
                "args": (site_id,),
                "style": "primary",
            }])
        markup.append([{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "primary"}])

        await call.edit(
            self.strings["sites_menu"].format(count=len(sites)),
            reply_markup=markup,
        )

    async def _cb_site_detail(self, call: InlineCall, site_id: str):
        site = self._get_sites().get(site_id)
        if not site:
            await call.edit(
                self.strings["site_not_found"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_sites_menu, "style": "primary"}]],
            )
            return

        await call.edit(
            self.strings["site_detail"].format(
                name=_escape(site.get("name", "?")),
                url=site.get("url", "?"),
                port=site.get("port", "?"),
            ),
            reply_markup=[
                [{"text": self.strings["btn_stop"], "callback": self._cb_stop_site, "args": (site_id,), "style": "danger"}],
                [{"text": self.strings["btn_back"], "callback": self._cb_sites_menu, "style": "primary"}],
            ],
        )

    async def _cb_stop_site(self, call: InlineCall, site_id: str):
        site = self._get_sites().get(site_id, {})
        url = site.get("url", "?")
        self._kill_site(site_id)
        await call.edit(
            self.strings["site_stopped"].format(url=url),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_sites_menu, "style": "primary"}]],
        )

    @loader.command(
        ru_doc="Реплай на .js/.jsx для деплоя | без реплая — меню",
        en_doc="Reply to .js/.jsx to deploy | without reply — menu",
    )
    async def wd(self, message: Message):
        """Reply to .js/.jsx to deploy | without reply — menu"""
        reply = await message.get_reply_message()

        if not reply or not reply.media:
            sites = self._get_sites()
            await self.inline.form(
                text=self.strings["main_menu"].format(
                    cf_status=_escape(self._cf_version()),
                    sites_count=len(sites),
                ),
                message=message,
                reply_markup=[
                    [{"text": self.strings["btn_sites"], "callback": self._cb_sites_menu, "style": "primary"}],
                    [{"text": self.strings["btn_setup"], "callback": self._cb_setup_menu, "style": "primary"}],
                    [{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}],
                ],
                silent=True,
            )
            return

        if not self._cf_installed():
            await utils.answer(message, self.strings["no_cf"])
            return

        doc = getattr(reply.media, "document", None)
        if not doc:
            await utils.answer(message, self.strings["wrong_type"])
            return

        filename = ""
        for attr in getattr(doc, "attributes", []):
            fn = getattr(attr, "file_name", None)
            if fn:
                filename = fn
                break

        if not filename:
            await utils.answer(message, self.strings["wrong_type"])
            return

        ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
        if ext not in ("js", "jsx"):
            await utils.answer(message, self.strings["wrong_type"])
            return

        m = await utils.answer(message, self.strings["downloading"].format(name=_escape(filename)))
        if isinstance(m, list):
            m = m[0]

        site_dir = os.path.join(self._root, f"site_{utils.rand(8)}")
        os.makedirs(site_dir, exist_ok=True)
        js_path = os.path.join(site_dir, filename)

        try:
            await reply.download_media(file=js_path)
        except Exception as e:
            shutil.rmtree(site_dir, ignore_errors=True)
            await m.edit(
                self.strings["deploy_fail"].format(error=_escape(str(e)[:300])),
                parse_mode="html",
            )
            return

        await m.edit(
            self.strings["deploying"].format(name=_escape(filename)),
            parse_mode="html",
        )

        try:
            html = self._build_html(js_path, filename)
        except Exception as e:
            shutil.rmtree(site_dir, ignore_errors=True)
            await m.edit(
                self.strings["deploy_fail"].format(error=_escape(str(e)[:300])),
                parse_mode="html",
            )
            return

        with open(os.path.join(site_dir, "index.html"), "w", encoding="utf-8") as f:
            f.write(html)

        port = self._next_port()
        http_proc = self._start_http_server(site_dir, port)
        await asyncio.sleep(1)

        result_holder = []
        cf_thread = threading.Thread(
            target=self._start_cf_tunnel,
            args=(port, result_holder),
            daemon=True,
        )
        cf_thread.start()

        url = None
        for _ in range(30):
            await asyncio.sleep(1)
            if result_holder and isinstance(result_holder[0], str):
                url = result_holder[0]
                break

        if not url:
            http_proc.terminate()
            shutil.rmtree(site_dir, ignore_errors=True)
            await m.edit(self.strings["cf_fail"], parse_mode="html")
            return

        cf_proc = None
        for item in result_holder:
            if hasattr(item, "terminate"):
                cf_proc = item
                break

        site_id = utils.rand(12)
        self._active[site_id] = {
            "http_proc": http_proc,
            "cf_proc": cf_proc,
        }
        self._add_site(site_id, {
            "name": filename,
            "url": url,
            "port": port,
            "pid": http_proc.pid,
            "dir": site_dir,
        })

        await m.delete()
        await self.inline.form(
            text=self.strings["deployed"].format(
                name=_escape(filename),
                url=url,
            ),
            message=message,
            reply_markup=[
                [{"text": self.strings["btn_stop"], "callback": self._cb_stop_site, "args": (site_id,), "style": "danger"}],
                [{"text": self.strings["btn_sites"], "callback": self._cb_sites_menu, "style": "primary"}],
                [{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}],
            ],
            silent=True,
        )