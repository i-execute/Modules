# CopyLeft 2026 github.com/i-execute
# Author: I_execute.t.me
# Licensed under AGPLv3.

__version__ = (1, 0, 0)
# meta developer: Execute_forge.t.me

import asyncio
import json
import logging
import os
import platform
import re
import shutil
import socket
import stat
import subprocess
import sys
import tarfile
import tempfile
import zipfile

from telethon.tl.functions.messages import EditMessageRequest
from telethon.tl.types import InputMediaWebPage, Message

from .. import loader, utils
from ..inline.types import InlineCall

logger = logging.getLogger(__name__)

CF_REPO = "cloudflare/cloudflared"
VERSIONS_PER_PAGE = 5
ARCHIVE_SUFFIXES = (".tar.gz", ".tgz", ".tar", ".zip")
PREFERRED_ROOT_NAMES = ("dist", "build", "public", "out", "www")
SKIP_DIR_NAMES = {"node_modules", "__pycache__", ".git", ".svn", ".hg"}
RELOADING_MEDIA_URL = "https://raw.githubusercontent.com/i-execute/Modules/main/Storage/WebDeployer/Reloading.jpeg"
TOPIC_ICON_EMOJI_ID = 5463122435425448565


def _escape(text):
    if not text:
        return ""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _cf_arch():
    m = platform.machine().lower()
    if "arm" in m or "aarch64" in m:
        return "arm64"
    return "amd64"


def _archive_kind(filename: str):
    lower = filename.lower()
    if lower.endswith(".zip"):
        return "zip"
    if lower.endswith(".tar.gz") or lower.endswith(".tgz") or lower.endswith(".tar"):
        return "tar"
    return None


def _safe_extract(archive_path: str, dest_dir: str, kind: str):
    os.makedirs(dest_dir, exist_ok=True)
    abs_dest = os.path.abspath(dest_dir)
    if kind == "zip":
        with zipfile.ZipFile(archive_path) as zf:
            for member in zf.infolist():
                target = os.path.abspath(os.path.join(dest_dir, member.filename))
                if target != abs_dest and not target.startswith(abs_dest + os.sep):
                    raise ValueError(f"Unsafe path in archive: {member.filename}")
            zf.extractall(dest_dir)
    else:
        with tarfile.open(archive_path, "r:*") as tf:
            for member in tf.getmembers():
                target = os.path.abspath(os.path.join(dest_dir, member.name))
                if target != abs_dest and not target.startswith(abs_dest + os.sep):
                    raise ValueError(f"Unsafe path in archive: {member.name}")
            tf.extractall(dest_dir)


def _find_site_index(root_dir: str):
    candidates = []
    for dirpath, dirnames, filenames in os.walk(root_dir):
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIR_NAMES and not d.startswith(".")]
        if "index.html" in filenames:
            candidates.append(os.path.join(dirpath, "index.html"))

    if not candidates:
        return None
    if len(candidates) == 1:
        return candidates[0]

    for name in PREFERRED_ROOT_NAMES:
        for path in candidates:
            if os.path.basename(os.path.dirname(path)) == name:
                return path

    candidates.sort(key=lambda p: p.count(os.sep))
    return candidates[0]


@loader.tds
class WebDeployer(loader.Module):
    """Deploy .js/.jsx/.ts/.tsx/.html files or .zip/.tar.gz archives to temporary Cloudflare domains"""

    strings = {
        "name": "WebDeployer",
        "reloaded": "<b>WebDeployer reloaded</b>",
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
        "no_reply": "<b>Reply to a .js, .jsx, .ts, .tsx, .html, .zip or .tar.gz file</b>",
        "wrong_type": "<b>File must be .js, .jsx, .ts, .tsx, .html, .zip, .tar.gz or .tgz</b>",
        "no_cf": (
            "<b>cloudflared not installed</b>\n"
            "<blockquote>Use .wd to open Setup</blockquote>"
        ),
        "downloading": (
            "<b>Downloading file</b>\n"
            "<blockquote><code>{name}</code></blockquote>"
        ),
        "extracting": (
            "<b>Extracting archive</b>\n"
            "<blockquote><code>{name}</code></blockquote>"
        ),
        "no_index": (
            "<b>No index.html found in archive</b>"
        ),
        "deploying": (
            "<b>Deploying</b>\n"
            "<blockquote>"
            "File: <code>{name}</code>\n"
            "Please wait..."
            "</blockquote>"
        ),
        "deploy_fail": (
            "<b>Deploy Failed</b>\n"
            "<blockquote><code>{error}</code></blockquote>"
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
            "<blockquote>Reply to .js/.jsx/.ts/.tsx/.html file with .wd to deploy</blockquote>"
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
        "site_not_found": (
            "<b>Site not found</b>\n"
            "<blockquote>It may have already been stopped</blockquote>"
        ),
        "token_set": (
            "<b>GitHub token saved</b>\n"
            "<blockquote>Rate limit raised to 5000 req/h</blockquote>"
        ),
        "token_cleared": "<b>GitHub token cleared</b>",
        "cf_fail": (
            "<b>Failed to get tunnel URL</b>\n"
            "<blockquote>cloudflared did not return a trycloudflare.com URL in 30 seconds</blockquote>"
        ),
        "btn_setup": "Setup",
        "btn_back": "Back",
        "btn_close": "Close",
        "btn_sites": "Active Sites",
        "btn_stop": "Stop Site",
        "btn_open_site": "Open Site",
        "btn_set_token": "Set GitHub Token",
        "btn_clear_token": "Clear Token",
        "btn_install_cf": "Install / Update cloudflared",
        "btn_left": "←",
        "btn_right": "→",
        "input_token": "Paste GitHub Personal Access Token:",
    }

    strings_ru = {
        "reloaded": "<b>WebDeployer перезагружен</b>",
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
        "no_reply": "<b>Ответьте на .js, .jsx, .ts, .tsx, .html, .zip или .tar.gz файл</b>",
        "wrong_type": "<b>Файл должен быть .js, .jsx, .ts, .tsx, .html, .zip, .tar.gz или .tgz</b>",
        "no_cf": (
            "<b>cloudflared не установлен</b>\n"
            "<blockquote>Используйте .wd для Setup</blockquote>"
        ),
        "downloading": (
            "<b>Скачивание файла</b>\n"
            "<blockquote><code>{name}</code></blockquote>"
        ),
        "extracting": (
            "<b>Распаковка архива</b>\n"
            "<blockquote><code>{name}</code></blockquote>"
        ),
        "no_index": (
            "<b>В архиве не найден index.html</b>"
        ),
        "deploying": (
            "<b>Деплой</b>\n"
            "<blockquote>"
            "Файл: <code>{name}</code>\n"
            "Подождите..."
            "</blockquote>"
        ),
        "deploy_fail": (
            "<b>Ошибка деплоя</b>\n"
            "<blockquote><code>{error}</code></blockquote>"
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
            "<blockquote>Ответьте на .js/.jsx/.ts/.tsx/.html файл командой .wd для деплоя</blockquote>"
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
        "site_not_found": (
            "<b>Сайт не найден</b>\n"
            "<blockquote>Возможно, он уже был остановлен</blockquote>"
        ),
        "token_set": (
            "<b>GitHub токен сохранён</b>\n"
            "<blockquote>Лимит запросов повышен до 5000/ч</blockquote>"
        ),
        "token_cleared": "<b>GitHub токен очищен</b>",
        "cf_fail": (
            "<b>Не удалось получить URL туннеля</b>\n"
            "<blockquote>cloudflared не вернул trycloudflare.com URL за 30 секунд</blockquote>"
        ),
        "btn_setup": "Настройка",
        "btn_back": "Назад",
        "btn_close": "Закрыть",
        "btn_sites": "Активные сайты",
        "btn_stop": "Остановить сайт",
        "btn_open_site": "Открыть сайт",
        "btn_set_token": "Установить GitHub токен",
        "btn_clear_token": "Очистить токен",
        "btn_install_cf": "Установить / Обновить cloudflared",
        "btn_left": "←",
        "btn_right": "→",
        "input_token": "Вставьте GitHub Personal Access Token:",
    }

    def __init__(self):
        self.config = loader.ModuleConfig()
        self._root = None
        self._cf_bin = None
        self._db = None
        self._client = None
        self._releases_cache = None
        self._logger_topic = None
        self._asset_channel = None

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        me = await client.get_me()
        tg_user_id = me.id
        self._root = os.path.join(os.path.expanduser("~"), ".cloudflared_on_userbot", str(tg_user_id))
        self._cf_bin = os.path.join(self._root, "cloudflared")
        os.makedirs(self._root, mode=0o700, exist_ok=True)

        self._asset_channel = self._db.get("heroku.forums", "channel_id", None)
        if self._asset_channel:
            try:
                self._logger_topic = await utils.asset_forum_topic(
                    self._client,
                    self._db,
                    self._asset_channel,
                    "WebDeployer",
                    description="WebDeployer notifications: deploys, cleanups, errors",
                    icon_emoji_id=TOPIC_ICON_EMOJI_ID,
                )
            except Exception as e:
                logger.error(f"[WD] Failed to create/get forum topic: {e}")

        if self._logger_topic and self._asset_channel:
            chat_id = int(f"-100{self._asset_channel}")
            greeting_key = f"wd_greeted_{self._asset_channel}_{self._logger_topic.id}"
            already_greeted = self.get(greeting_key, False)
            if already_greeted:
                await self._send_with_preview(chat_id, self.strings["reloaded"])
            else:
                self.set(greeting_key, True)

        await self._reattach_sites()

    async def _send_with_preview(self, chat_id, text):
        try:
            msg_text, entities = await self.inline.bot._parse_message_text(text, "html")
            msg = await self.inline.bot.send_message(
                chat_id,
                msg_text,
                parse_mode=None,
                entities=entities,
                message_thread_id=self._logger_topic.id,
            )
            if msg:
                try:
                    peer = await self.inline.bot.get_input_entity(chat_id)
                    current_msg = await self.inline.bot.get_messages(chat_id, ids=msg.id)
                    reply_markup = current_msg.reply_markup if current_msg else None
                    await self.inline.bot(EditMessageRequest(
                        peer=peer,
                        id=msg.id,
                        message=msg_text,
                        media=InputMediaWebPage(
                            url=RELOADING_MEDIA_URL,
                            optional=True,
                            force_large_media=True,
                        ),
                        invert_media=True,
                        reply_markup=reply_markup,
                        entities=entities,
                        no_webpage=False,
                    ))
                except Exception as e:
                    logger.error(f"[WD] Failed to add preview: {e}")
        except Exception as e:
            logger.error(f"[WD] Failed to send message with preview: {e}")

    async def _notify(self, text: str):
        if not self._logger_topic or not self._asset_channel:
            return
        chat_id = int(f"-100{self._asset_channel}")
        try:
            await self.inline.bot.send_message(
                chat_id,
                text,
                parse_mode="html",
                message_thread_id=self._logger_topic.id,
            )
        except Exception as e:
            logger.error(f"[WD] Failed to send topic notification: {e}")

    @property
    def _is_root(self) -> bool:
        return os.name == "posix" and os.geteuid() == 0

    @property
    def _systemd_dir(self) -> str:
        if self._is_root:
            return "/etc/systemd/system"
        return os.path.join(os.path.expanduser("~"), ".config", "systemd", "user")

    async def _systemctl(self, *args: str):
        try:
            cmd = ["systemctl"] if self._is_root else ["systemctl", "--user"]
            proc = await asyncio.create_subprocess_exec(
                *cmd, *args,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            )
            out, err = await proc.communicate()
            return proc.returncode == 0, (out + err).decode(errors="replace").strip()
        except Exception as e:
            return False, str(e)

    def _unit_name(self, site_id: str, kind: str) -> str:
        safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", site_id)
        return f"wd-{safe}-{kind}.service"

    def _write_unit(self, unit: str, description: str, command: list, work_dir: str, log_path: str):
        os.makedirs(self._systemd_dir, mode=0o700, exist_ok=True)
        quoted = " ".join(subprocess.list2cmdline([part]) for part in command)
        content = (
            "[Unit]\n"
            f"Description={description}\n"
            "After=network-online.target\nWants=network-online.target\n\n"
            "[Service]\nType=simple\n"
            f"WorkingDirectory={work_dir}\n"
            f"ExecStart={quoted}\n"
            "Restart=on-failure\nRestartSec=3\n"
            f"StandardOutput=append:{log_path}\nStandardError=append:{log_path}\n\n"
            f"[Install]\nWantedBy={'multi-user.target' if self._is_root else 'default.target'}\n"
        )
        with open(os.path.join(self._systemd_dir, unit), "w", encoding="utf-8") as f:
            f.write(content)

    def _remove_unit_file(self, unit: str):
        path = os.path.join(self._systemd_dir, unit)
        if os.path.exists(path):
            try:
                os.unlink(path)
            except OSError:
                pass

    async def _unit_active(self, unit: str) -> bool:
        ok, out = await self._systemctl("is-active", "--quiet", unit)
        return ok and out == ""

    async def _start_unit(self, unit: str):
        ok, out = await self._systemctl("daemon-reload")
        if not ok:
            return False, out
        return await self._systemctl("enable", "--now", unit)

    async def _stop_unit(self, unit: str):
        await self._systemctl("disable", "--now", unit)

    async def _wait_for_tunnel_url(self, log_path: str, timeout: int = 30):
        for _ in range(timeout):
            await asyncio.sleep(1)
            try:
                with open(log_path, "r", encoding="utf-8", errors="replace") as f:
                    content = f.read()
            except OSError:
                continue
            for line in content.splitlines():
                if "trycloudflare.com" in line:
                    for part in line.split():
                        if part.startswith("https://") and "trycloudflare.com" in part:
                            return part.strip()
        return None

    async def _reattach_sites(self):
        for site_id, site in list(self._get_sites().items()):
            http_unit = site.get("http_unit")
            cf_unit = site.get("cf_unit")
            if not http_unit or not cf_unit:
                self._remove_site(site_id)
                continue
            if await self._unit_active(http_unit):
                continue
            await self._stop_unit(cf_unit)
            self._remove_unit_file(http_unit)
            self._remove_unit_file(cf_unit)
            await self._systemctl("daemon-reload")
            site_dir = site.get("dir")
            if site_dir and os.path.isdir(site_dir):
                shutil.rmtree(site_dir, ignore_errors=True)
            self._remove_site(site_id)
            await self._notify(
                f"<b>Cleaned up stale site</b>\n<blockquote>{_escape(site.get('name', site_id))}</blockquote>"
            )

    def _cf_installed(self):
        return bool(self._cf_bin and os.path.isfile(self._cf_bin) and os.access(self._cf_bin, os.X_OK))

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

    async def _stop_site(self, site_id: str):
        site = self._get_sites().get(site_id, {})
        http_unit = site.get("http_unit")
        cf_unit = site.get("cf_unit")
        if http_unit:
            await self._stop_unit(http_unit)
            self._remove_unit_file(http_unit)
        if cf_unit:
            await self._stop_unit(cf_unit)
            self._remove_unit_file(cf_unit)
        await self._systemctl("daemon-reload")
        site_dir = site.get("dir")
        if site_dir and os.path.isdir(site_dir):
            shutil.rmtree(site_dir, ignore_errors=True)
        self._remove_site(site_id)

    def _unique_site_name(self, name: str) -> str:
        existing = {site.get("name") for site in self._get_sites().values()}
        if name not in existing:
            return name
        if "." in name:
            base, ext = name.rsplit(".", 1)
            ext = f".{ext}"
        else:
            base, ext = name, ""
        n = 2
        while f"{base}-{n}{ext}" in existing:
            n += 1
        return f"{base}-{n}{ext}"

    def _next_port(self) -> int:
        used = {site.get("port") for site in self._get_sites().values() if site.get("port")}
        for _ in range(20):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                sock.bind(("127.0.0.1", 0))
                port = sock.getsockname()[1]
            finally:
                sock.close()
            if port not in used:
                return port
        raise RuntimeError("could not find a free port")

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

        tmp_fd, tmp_path = tempfile.mkstemp(prefix="cloudflared_", dir=self._root)
        os.close(tmp_fd)
        try:
            p = await asyncio.create_subprocess_exec(
                "wget", "-q", "--max-redirect=15", "-O", tmp_path, download_url,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            _, err = await asyncio.wait_for(p.communicate(), timeout=300)
            size = os.path.getsize(tmp_path) if os.path.isfile(tmp_path) else 0
            if p.returncode != 0 or size < 100_000:
                return False, f"Download failed (size={size}): {err.decode()[:200]}"
            shutil.copy2(tmp_path, self._cf_bin)
            os.chmod(self._cf_bin, 0o755)
        finally:
            try:
                os.remove(tmp_path)
            except OSError:
                pass
        self._db.set("WebDeployer", "cf_version", tag)
        self._releases_cache = None
        return True, tag

    def _build_html_from_js(self, js_path: str, filename: str, ext: str = "jsx") -> str:
        with open(js_path, "r", encoding="utf-8", errors="replace") as f:
            js_code = f.read()
        is_ts = ext in ("ts", "tsx")
        presets = "typescript,react" if is_ts else "react"
        script_type = "text/babel"
        return f"""<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1.0"/>
<title>{_escape(filename)}</title>
<script src="https://telegram.org/js/telegram-web-app.js"></script>
<script src="https://unpkg.com/react@18/umd/react.development.js"></script>
<script src="https://unpkg.com/react-dom@18/umd/react-dom.development.js"></script>
<script src="https://unpkg.com/@babel/standalone/babel.min.js"></script>
</head>
<body>
<div id="root"></div>
<script type="{script_type}" data-presets="{presets}" data-plugins="proposal-class-properties">
{js_code}
const domNode = document.getElementById('root');
const root = ReactDOM.createRoot(domNode);
if (typeof App !== 'undefined') {{
  root.render(React.createElement(App));
}}
</script>
</body>
</html>"""

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
                [{"text": self.strings["btn_open_site"], "url": site.get("url", ""), "style": "success"}],
                [{"text": self.strings["btn_stop"], "callback": self._cb_stop_site, "args": (site_id,), "style": "danger"}],
                [{"text": self.strings["btn_back"], "callback": self._cb_sites_menu, "style": "primary"}],
            ],
        )

    async def _cb_stop_site(self, call: InlineCall, site_id: str):
        site = self._get_sites().get(site_id, {})
        url = site.get("url", "?")
        await self._stop_site(site_id)
        await self._notify(
            f"<b>Site stopped</b>\n<blockquote>{_escape(site.get('name', site_id))}\n{url}</blockquote>"
        )
        await call.edit(
            self.strings["site_stopped"].format(url=url),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_sites_menu, "style": "primary"}]],
        )

    @loader.command(
        ru_doc="Реплай на .js/.jsx/.ts/.tsx/.html для деплоя | без реплая — меню",
        en_doc="Reply to .js/.jsx/.ts/.tsx/.html to deploy | without reply — menu",
    )
    async def wd(self, message: Message):
        """Reply to .js/.jsx/.ts/.tsx/.html to deploy | without reply — menu"""
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

        display_name = self._unique_site_name(filename)

        archive_kind = _archive_kind(filename)
        if archive_kind:
            ext = "archive"
        else:
            ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
            if ext not in ("js", "jsx", "ts", "tsx", "html"):
                await utils.answer(message, self.strings["wrong_type"])
                return

        m = await utils.answer(message, self.strings["downloading"].format(name=_escape(filename)))
        if isinstance(m, list):
            m = m[0]

        site_dir = os.path.join(self._root, f"site_{utils.rand(8)}")
        os.makedirs(site_dir, exist_ok=True)
        file_path = os.path.join(site_dir, filename)

        try:
            await reply.download_media(file=file_path)
        except Exception as e:
            shutil.rmtree(site_dir, ignore_errors=True)
            await m.edit(
                self.strings["deploy_fail"].format(error=_escape(str(e)[:400])),
                parse_mode="html",
            )
            return

        if archive_kind:
            await m.edit(
                self.strings["extracting"].format(name=_escape(filename)),
                parse_mode="html",
            )
        else:
            await m.edit(
                self.strings["deploying"].format(name=_escape(filename)),
                parse_mode="html",
            )

        try:
            if archive_kind:
                extract_dir = os.path.join(site_dir, "extracted")
                _safe_extract(file_path, extract_dir, archive_kind)
                os.remove(file_path)
                index_path = _find_site_index(extract_dir)
                if not index_path:
                    shutil.rmtree(site_dir, ignore_errors=True)
                    await m.edit(self.strings["no_index"], parse_mode="html")
                    return
                serve_dir = os.path.dirname(index_path)
                await m.edit(
                    self.strings["deploying"].format(name=_escape(filename)),
                    parse_mode="html",
                )
            elif ext in ("js", "jsx", "ts", "tsx"):
                html = self._build_html_from_js(file_path, filename, ext)
                with open(os.path.join(site_dir, "index.html"), "w", encoding="utf-8") as f:
                    f.write(html)
                serve_dir = site_dir
            else:
                dest = os.path.join(site_dir, "index.html")
                if file_path != dest:
                    shutil.copy2(file_path, dest)
                serve_dir = site_dir
        except Exception as e:
            shutil.rmtree(site_dir, ignore_errors=True)
            await m.edit(
                self.strings["deploy_fail"].format(error=_escape(str(e)[:400])),
                parse_mode="html",
            )
            return

        port = self._next_port()
        site_id = utils.rand(12)
        http_unit = self._unit_name(site_id, "http")
        cf_unit = self._unit_name(site_id, "cf")
        http_log = os.path.join(site_dir, "http.log")
        cf_log = os.path.join(site_dir, "cf.log")

        self._write_unit(
            http_unit,
            f"WebDeployer HTTP server {site_id}",
            [sys.executable, "-m", "http.server", str(port), "--directory", serve_dir],
            serve_dir,
            http_log,
        )
        ok, out = await self._start_unit(http_unit)
        if not ok:
            self._remove_unit_file(http_unit)
            shutil.rmtree(site_dir, ignore_errors=True)
            await m.edit(
                self.strings["deploy_fail"].format(error=_escape(out[:400])),
                parse_mode="html",
            )
            return

        self._write_unit(
            cf_unit,
            f"WebDeployer cloudflared tunnel {site_id}",
            [self._cf_bin, "tunnel", "--url", f"http://localhost:{port}", "--no-autoupdate"],
            self._root,
            cf_log,
        )
        ok, out = await self._start_unit(cf_unit)
        if not ok:
            await self._stop_unit(http_unit)
            self._remove_unit_file(http_unit)
            self._remove_unit_file(cf_unit)
            shutil.rmtree(site_dir, ignore_errors=True)
            await m.edit(
                self.strings["deploy_fail"].format(error=_escape(out[:400])),
                parse_mode="html",
            )
            return

        url = await self._wait_for_tunnel_url(cf_log)
        if not url:
            await self._stop_unit(http_unit)
            await self._stop_unit(cf_unit)
            self._remove_unit_file(http_unit)
            self._remove_unit_file(cf_unit)
            await self._systemctl("daemon-reload")
            shutil.rmtree(site_dir, ignore_errors=True)
            await m.edit(self.strings["cf_fail"], parse_mode="html")
            return

        self._add_site(site_id, {
            "name": display_name,
            "url": url,
            "port": port,
            "dir": site_dir,
            "http_unit": http_unit,
            "cf_unit": cf_unit,
        })

        await m.delete()
        await self._notify(
            f"<b>Site deployed</b>\n<blockquote>{_escape(display_name)}\n{url}</blockquote>"
        )
        await self.inline.form(
            text=self.strings["deployed"].format(
                name=_escape(display_name),
                url=url,
            ),
            message=message,
            reply_markup=[
                [{"text": self.strings["btn_open_site"], "url": url, "style": "success"}],
                [{"text": self.strings["btn_stop"], "callback": self._cb_stop_site, "args": (site_id,), "style": "danger"}],
                [{"text": self.strings["btn_sites"], "callback": self._cb_sites_menu, "style": "primary"}],
                [{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}],
            ],
            silent=True,
        )