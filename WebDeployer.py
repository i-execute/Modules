# CopyLeft 2026 github.com/i-execute
# Author: I_execute.t.me
# Licensed under AGPLv3.

__version__ = (1, 2, 0)
# meta developer: Execute_forge.t.me

import asyncio
import glob
import gzip
import json
import logging
import os
import platform
import re
import secrets
import shutil
import socket
import stat
import subprocess
import sys
import tarfile
import tempfile
import time
import zipfile

from telethon.tl.functions.messages import EditForumTopicRequest, EditMessageRequest
from telethon.tl.types import InputMediaWebPage, Message

from .. import loader, utils
from ..inline.types import InlineCall

logger = logging.getLogger(__name__)

async def _retry_topic_send(send, refresh):
    try:
        return await send()
    except Exception:
        await refresh()
        return await send()


async def _apply_topic_icon(client, peer, topic, icon_id):
    if getattr(topic, "icon_emoji_id", None) == icon_id:
        return
    try:
        await client(EditForumTopicRequest(peer=peer, topic_id=topic.id, icon_emoji_id=icon_id))
    except Exception:
        pass


CF_REPO = "cloudflare/cloudflared"
VERSIONS_PER_PAGE = 5
ARCHIVE_SUFFIXES = (
    ".tar.gz",
    ".tgz",
    ".tar",
    ".tar.bz2",
    ".tbz2",
    ".tar.xz",
    ".txz",
    ".zip",
    ".gz",
    ".bz2",
    ".xz",
)
PREFERRED_ROOT_NAMES = ("dist", "build", "public", "out", "www", ".output/public")
SKIP_DIR_NAMES = {"node_modules", "__pycache__", ".git", ".svn", ".hg", ".next", ".cache"}
RELOADING_MEDIA_URL = "https://raw.githubusercontent.com/i-execute/Modules/main/Storage/WebDeployer/Reloading.jpeg"
GREETING_MEDIA_URL = "https://raw.githubusercontent.com/i-execute/Modules/main/Storage/WebDeployer/Greetings.jpeg"
TOPIC_ICON_EMOJI_ID = 5386654279341747455


EXTRA_PATH_DIRS = (
    "~/.local/bin",
    "~/bin",
    "~/.npm-global/bin",
    "~/.volta/bin",
    "~/.fnm/aliases/default/bin",
    "~/.asdf/shims",
    "~/.local/share/pnpm",
    "~/.yarn/bin",
    "~/.bun/bin",
    "~/n/bin",
    "/usr/local/sbin",
    "/usr/local/bin",
    "/usr/sbin",
    "/usr/bin",
    "/sbin",
    "/bin",
    "/usr/games",
    "/usr/local/games",
    "/snap/bin",
    "/opt/node/bin",
    "/usr/local/node/bin",
    "/usr/lib/node_modules/.bin",
    "/home/linuxbrew/.linuxbrew/bin",
)
NVM_GLOBS = (
    "~/.nvm/versions/node/*/bin",
    "~/.local/share/fnm/node-versions/*/installation/bin",
    "~/.fnm/node-versions/*/installation/bin",
    "~/.asdf/installs/nodejs/*/bin",
    "~/.local/share/nvm/*/bin",
    "~/.nodenv/versions/*/bin",
)


def _version_key(path):
    nums = re.findall(r"\d+", path)
    return [int(n) for n in nums]


def _split_path(value):
    return [x for x in (value or "").split(os.pathsep) if x]


def _merge_paths(*groups):
    seen = set()
    result = []
    for group in groups:
        for item in group:
            item = os.path.expanduser(item)
            if not item or item in seen:
                continue
            seen.add(item)
            result.append(item)
    return result


def _discover_node_dirs():
    found = []
    for pattern in NVM_GLOBS:
        matches = sorted(glob.glob(os.path.expanduser(pattern)), key=_version_key, reverse=True)
        found.extend(matches)
    return found


def _login_shell_path():
    shells = []
    env_shell = os.environ.get("SHELL")
    if env_shell:
        shells.append(env_shell)
    shells += ["/bin/bash", "/bin/sh"]
    seen = set()
    for sh in shells:
        if sh in seen or not os.path.exists(sh):
            continue
        seen.add(sh)
        for flags in ("-lic", "-lc", "-ic"):
            try:
                out = subprocess.run(
                    [sh, flags, 'printf "%s" "$PATH"'],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.DEVNULL,
                    stdin=subprocess.DEVNULL,
                    timeout=8,
                ).stdout.decode(errors="replace").strip()
            except Exception:
                continue
            if out and "/" in out:
                lines = [ln for ln in out.splitlines() if ln.strip()]
                if lines:
                    return _split_path(lines[-1])
    return []


def _build_full_path():
    return os.pathsep.join(
        _merge_paths(
            _split_path(os.environ.get("PATH", "")),
            _login_shell_path(),
            _discover_node_dirs(),
            EXTRA_PATH_DIRS,
        )
    )


def _which(name, path_value):
    return shutil.which(name, path=path_value)


SITE_HEAL_INTERVAL = 120
SITE_HEAL_COOLDOWN = 600
REFRESH_BUTTON = "Refresh link"

GUARD_ROOT = "root"
GUARD_DOCKER = "docker"


def _in_docker():
    if os.path.isfile("/.dockerenv"):
        return True
    try:
        with open("/proc/1/cgroup", "rt") as handle:
            data = handle.read()
        if any(marker in data for marker in ("docker", "kubepods", "containerd", "podman", "lxc")):
            return True
    except Exception:
        pass
    try:
        with open("/proc/1/environ", "rb") as handle:
            environment = handle.read()
        if b"container=" in environment:
            return True
    except Exception:
        pass
    return False


def _guard_reason() -> str:
    if os.name == "posix" and os.geteuid() == 0:
        return GUARD_ROOT
    if _in_docker():
        return GUARD_DOCKER
    return ""


def _write_private(path: str, data: str) -> None:
    handle = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        os.fchmod(handle, 0o600)
        with os.fdopen(handle, "w", encoding="utf-8") as stream:
            stream.write(data)
    except Exception:
        try:
            os.close(handle)
        except OSError:
            pass
        raise


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
    if (
        lower.endswith(".tar.gz")
        or lower.endswith(".tgz")
        or lower.endswith(".tar")
        or lower.endswith(".tar.bz2")
        or lower.endswith(".tbz2")
        or lower.endswith(".tar.xz")
        or lower.endswith(".txz")
        or lower.endswith(".gz")
        or lower.endswith(".bz2")
        or lower.endswith(".xz")
    ):
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
                mode = (member.external_attr >> 16) & 0o170000
                if mode == 0o120000:
                    raise ValueError(f"Unsafe link in archive: {member.filename}")
            zf.extractall(dest_dir)
    else:
        try:
            with tarfile.open(archive_path, "r:*") as tf:
                for member in tf.getmembers():
                    target = os.path.abspath(os.path.join(dest_dir, member.name))
                    if target != abs_dest and not target.startswith(abs_dest + os.sep):
                        raise ValueError(f"Unsafe path in archive: {member.name}")
                    if member.issym() or member.islnk() or member.isdev():
                        raise ValueError(f"Unsafe link in archive: {member.name}")
                if hasattr(tarfile, "data_filter"):
                    tf.extractall(dest_dir, filter="data")
                else:
                    tf.extractall(dest_dir)
        except (tarfile.ReadError, tarfile.CompressionError):
            # Fallback if it is a single compressed file (e.g. gzip)
            base = os.path.basename(archive_path)
            out_name = base
            for ext in (".gz", ".bz2", ".xz"):
                if out_name.lower().endswith(ext):
                    out_name = out_name[: -len(ext)]
                    break
            out_path = os.path.join(dest_dir, out_name)
            with gzip.open(archive_path, "rb") as f_in:
                with open(out_path, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)


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


def _generate_node_runner():
    return r"""import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { spawn } from 'node:child_process';

const PORT = parseInt(process.env.PORT || '5173', 10);
const BACKEND_PORT = parseInt(process.env.BACKEND_PORT || '8787', 10);
const STATIC_DIR = process.env.STATIC_DIR || '';
const BACKEND_DIR = process.env.BACKEND_DIR || '';
const BACKEND_CMD = process.env.BACKEND_CMD || '';

const MIME_TYPES = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'application/javascript; charset=utf-8',
  '.mjs': 'application/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.gif': 'image/gif',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon',
  '.woff': 'font/woff',
  '.woff2': 'font/woff2',
  '.ttf': 'font/ttf',
  '.webp': 'image/webp',
  '.wasm': 'application/wasm',
  '.map': 'application/json',
  '.txt': 'text/plain; charset=utf-8',
  '.xml': 'application/xml',
  '.mp3': 'audio/mpeg',
  '.mp4': 'video/mp4',
  '.webm': 'video/webm',
  '.otf': 'font/otf',
  '.avif': 'image/avif',
  '.glb': 'model/gltf-binary',
  '.gltf': 'model/gltf+json',
};

let backendProcess = null;
if (BACKEND_DIR && BACKEND_CMD) {
  const cmd = process.env.NODE_BIN || process.execPath;
  console.log(`Starting backend in ${BACKEND_DIR}: ${cmd} ${BACKEND_CMD}`);
  backendProcess = spawn(cmd, [BACKEND_CMD], {
    cwd: BACKEND_DIR,
    env: { ...process.env, PORT: String(BACKEND_PORT) },
    stdio: 'inherit',
  });
  backendProcess.on('error', (err) => {
    console.error(`Backend spawn error: ${err.message}`);
  });
  backendProcess.on('exit', (code) => {
    console.log(`Backend process exited with code ${code}`);
  });
}

function serveStatic(req, res, pathname) {
  if (!STATIC_DIR) {
    res.writeHead(404, { 'Content-Type': 'text/plain' });
    res.end('Not Found');
    return;
  }
  let decoded = pathname;
  try {
    decoded = decodeURIComponent(pathname);
  } catch (e) {}
  let safePath = path.normalize(decoded).replace(/^([\\/]|\.\.(?=[\\/]|$))+/, '');
  let filePath = path.join(STATIC_DIR, safePath);
  if (!path.resolve(filePath).startsWith(path.resolve(STATIC_DIR))) {
    res.writeHead(403, { 'Content-Type': 'text/plain' });
    res.end('Forbidden');
    return;
  }

  if (fs.existsSync(filePath) && fs.statSync(filePath).isDirectory()) {
    filePath = path.join(filePath, 'index.html');
  }

  if (!fs.existsSync(filePath) || !fs.statSync(filePath).isFile()) {
    const fallback = path.join(STATIC_DIR, 'index.html');
    if (fs.existsSync(fallback)) {
      filePath = fallback;
    } else {
      res.writeHead(404, { 'Content-Type': 'text/plain' });
      res.end('404 Not Found');
      return;
    }
  }

  const ext = path.extname(filePath).toLowerCase();
  const contentType = MIME_TYPES[ext] || 'application/octet-stream';
  res.writeHead(200, { 'Content-Type': contentType });
  fs.createReadStream(filePath).pipe(res);
}

const server = http.createServer((req, res) => {
  const parsedUrl = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  const pathname = parsedUrl.pathname;

  if (BACKEND_DIR && pathname.startsWith('/api')) {
    const proxyReq = http.request(
      {
        hostname: '127.0.0.1',
        port: BACKEND_PORT,
        path: req.url,
        method: req.method,
        headers: {
          ...req.headers,
          host: `127.0.0.1:${BACKEND_PORT}`,
          'x-forwarded-for': req.socket.remoteAddress,
          'x-forwarded-proto': 'https',
        },
      },
      (proxyRes) => {
        res.writeHead(proxyRes.statusCode, proxyRes.headers);
        proxyRes.pipe(res);
      }
    );
    proxyReq.setTimeout(60000, () => proxyReq.destroy(new Error('timeout')));
    proxyReq.on('error', (err) => {
      console.error('Proxy error:', err.message);
      if (!res.headersSent) res.writeHead(502, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Backend unreachable', details: err.message }));
    });
    req.pipe(proxyReq);
  } else {
    serveStatic(req, res, pathname);
  }
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`Server listening on 0.0.0.0:${PORT}`);
});

process.on('SIGTERM', () => {
  if (backendProcess) backendProcess.kill('SIGTERM');
  server.close(() => process.exit(0));
});
process.on('SIGINT', () => {
  if (backendProcess) backendProcess.kill('SIGINT');
  server.close(() => process.exit(0));
});
"""


@loader.tds
class WebDeployer(loader.Module):
    """Deploy .js/.jsx/.ts/.tsx/.html files or archives (.zip, .tar.gz, .tgz, .gz, full-stack Node/Vite) to temporary Cloudflare domains"""

    strings = {
        "name": "WebDeployer",
        "url_moved": "<b>The site link changed</b>\n<blockquote>Site: {name}\nBefore: <code>{previous}</code>\nNow: {url}</blockquote>",
        "site_down": "<b>The site did not come back by itself</b>\n<blockquote>Site: {name}\nOpen the sites menu and deploy it again, the log is in its directory.</blockquote>",
        "btn_refresh": "Refresh link",
        "guard_title": "<b>WebDeployer is disabled on this server</b>",
        "guard_root": "The userbot is running as root (uid 0). WebDeployer refuses to start in this mode.",
        "guard_docker": "The userbot is running inside a container (Docker, Podman, LXC or Kubernetes). WebDeployer refuses to start in this mode.",
        "guard_why": "Why: this module starts long living web daemons for your sites. Under root a single bug is a full server takeover, and inside a container the site escapes the isolation the container promises. A userbot is not a system service.",
        "guard_how": "What to do: run the userbot as a regular user with a working <code>systemctl --user</code> session, the module creates only user units. Keep root off limits and do not run it in Docker.",
        "reloaded": "<blockquote><b>WebDeployer reloaded</b></blockquote>\n<blockquote>Version: <b>{version}</b>\nSaved sites: <b>{sites}</b></blockquote>",
        "greeting": "<blockquote><b>WebDeployer is ready</b></blockquote>\n<blockquote>Version: <b>{version}</b>\nSaved sites: <b>{sites}</b>\nDeployments and errors will be logged here.\nUse <code>{prefix}wd</code> to manage sites and your GitHub token.</blockquote>",
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
        "no_reply": "<b>Reply to a .js, .jsx, .ts, .tsx, .html, .zip, .tar.gz or .gz file</b>",
        "wrong_type": "<b>File must be .js, .jsx, .ts, .tsx, .html, .zip, .tar.gz, .tgz or .gz</b>",
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
        "building": (
            "<b>Building project</b>\n"
            "<blockquote><code>{name}</code>\n{step}</blockquote>"
        ),
        "no_index": (
            "<b>No index.html or launchable server found in archive</b>"
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
            "<blockquote>Reply to .js/.jsx/.ts/.tsx/.html/archive file with .wd to deploy</blockquote>"
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
        "url_moved": "<b>Ссылка сайта изменилась</b>\n<blockquote>Сайт: {name}\nБыло: <code>{previous}</code>\nСтало: {url}</blockquote>",
        "site_down": "<b>Сайт сам не поднялся</b>\n<blockquote>Сайт: {name}\nОткройте меню сайтов и задеплойте заново, лог лежит в его каталоге.</blockquote>",
        "btn_refresh": "Обновить ссылку",
        "guard_title": "<b>WebDeployer отключён на этом сервере</b>",
        "guard_root": "Юзербот запущен от root (uid 0). WebDeployer в таком режиме работать отказывается.",
        "guard_docker": "Юзербот запущен внутри контейнера (Docker, Podman, LXC или Kubernetes). WebDeployer в таком режиме работать отказывается.",
        "guard_why": "Почему: модуль поднимает долгоживущие веб-демоны для ваших сайтов. Под root одна ошибка это полный захват сервера, а внутри контейнера сайт выходит за границы обещанной изоляции. Юзербот не является системной службой.",
        "guard_how": "Что делать: запустите юзербот обычным пользователем с рабочей сессией <code>systemctl --user</code>, модуль создаёт только пользовательские юниты. Root не использовать, в Docker не запускать.",
        "reloaded": "<blockquote><b>WebDeployer перезагружен</b></blockquote>\n<blockquote>Версия: <b>{version}</b>\nСохранено сайтов: <b>{sites}</b></blockquote>",
        "greeting": "<blockquote><b>WebDeployer готов к работе</b></blockquote>\n<blockquote>Версия: <b>{version}</b>\nСохранено сайтов: <b>{sites}</b>\nВ этот топик будут приходить логи деплоев и ошибок.\nИспользуйте <code>{prefix}wd</code>, чтобы управлять сайтами и GitHub-токеном.</blockquote>",
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
        "no_reply": "<b>Ответьте на .js, .jsx, .ts, .tsx, .html, .zip, .tar.gz или .gz файл</b>",
        "wrong_type": "<b>Файл должен быть .js, .jsx, .ts, .tsx, .html, .zip, .tar.gz, .tgz или .gz</b>",
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
        "building": (
            "<b>Сборка проекта</b>\n"
            "<blockquote><code>{name}</code>\n{step}</blockquote>"
        ),
        "no_index": (
            "<b>В архиве не найден index.html или исполняемый сервер</b>"
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
            "<blockquote>Ответьте на .js/.jsx/.ts/.tsx/.html или архив командой .wd для деплоя</blockquote>"
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
        self._full_path = None

    def _get_full_path(self, refresh=False):
        if self._full_path is None or refresh:
            self._full_path = _build_full_path()
        return self._full_path

    def _proc_env(self, extra=None):
        env = dict(os.environ)
        env["PATH"] = self._get_full_path()
        env.pop("NODE_ENV", None)
        env["CI"] = "1"
        env["npm_config_fund"] = "false"
        env["npm_config_audit"] = "false"
        env["npm_config_update_notifier"] = "false"
        env["npm_config_loglevel"] = "error"
        if extra:
            env.update(extra)
        return env

    def _resolve_bin(self, name):
        found = _which(name, self._get_full_path())
        if not found:
            found = _which(name, _build_full_path())
        return found

    def _ensure_root(self):
        os.makedirs(self._root, mode=0o700, exist_ok=True)

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        me = await client.get_me()
        tg_user_id = me.id
        self._root = os.path.join(os.path.expanduser("~"), ".cloudflared_on_userbot", str(tg_user_id))
        self._cf_bin = os.path.join(self._root, "cloudflared")
        self._ensure_root()
        await asyncio.get_event_loop().run_in_executor(None, self._get_full_path, True)

        self._site_task = None
        self._site_heal_attempts = {}
        self._site_down_notified = set()
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
                await _apply_topic_icon(self._client, self._asset_channel, self._logger_topic, TOPIC_ICON_EMOJI_ID)
            except Exception as e:
                logger.error(f"[WD] Failed to create/get forum topic: {e}")

        if self._logger_topic and self._asset_channel:
            chat_id = int(f"-100{self._asset_channel}")
            greeting_key = f"wd_greeted_{self._asset_channel}_{self._logger_topic.id}"
            already_greeted = self.get(greeting_key, False)
            values = {
                "sites": len(self._get_sites()),
                "version": ".".join(map(str, __version__)),
                "prefix": self.get_prefix(),
            }
            if already_greeted:
                await self._send_with_preview(chat_id, self.strings["reloaded"].format(**values))
            else:
                self.set(greeting_key, True)
                await self._send_with_preview(
                    chat_id,
                    self.strings["greeting"].format(**values),
                    is_greeting=True,
                )

        await self._reattach_sites()
        self._start_site_monitor()

    async def _refresh_webdeployer_topic(self):
        self._logger_topic = await utils.asset_forum_topic(
            self._client,
            self._db,
            self._asset_channel,
            "WebDeployer",
            description="WebDeployer notifications: deploys, cleanups, errors",
            icon_emoji_id=TOPIC_ICON_EMOJI_ID,
        )
        await _apply_topic_icon(self._client, self._asset_channel, self._logger_topic, TOPIC_ICON_EMOJI_ID)

    async def _send_with_preview(self, chat_id, text, is_greeting=False):
        try:
            msg_text, entities = await self.inline.bot._parse_message_text(text, "html")
            msg = await _retry_topic_send(
                lambda: self.inline.bot.send_message(
                    chat_id,
                    msg_text,
                    parse_mode=None,
                    entities=entities,
                    message_thread_id=self._logger_topic.id,
                ),
                self._refresh_webdeployer_topic,
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
                            url=GREETING_MEDIA_URL if is_greeting else RELOADING_MEDIA_URL,
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
            await _retry_topic_send(
                lambda: self.inline.bot.send_message(
                    chat_id,
                    text,
                    parse_mode="html",
                    message_thread_id=self._logger_topic.id,
                ),
                self._refresh_webdeployer_topic,
            )
        except Exception as e:
            logger.error(f"[WD] Failed to send topic notification: {e}")

    def _guard_message(self) -> str:
        reason = _guard_reason()
        if not reason:
            return ""
        head = self.strings["guard_title"] + "\n"
        body = self.strings["guard_root"] if reason == GUARD_ROOT else self.strings["guard_docker"]
        return head + body + "\n\n" + self.strings["guard_why"] + "\n\n" + self.strings["guard_how"]

    async def _guard_deny(self, message) -> bool:
        text = self._guard_message()
        if not text:
            return False
        await utils.answer(message, text)
        return True

    async def on_unload(self):
        if self._site_task:
            self._site_task.cancel()
            try:
                await self._site_task
            except asyncio.CancelledError:
                pass
            self._site_task = None

    @property
    def _systemd_dir(self) -> str:
        return os.path.join(os.path.expanduser("~"), ".config", "systemd", "user")

    def _systemctl_env(self):
        env = dict(os.environ)
        if os.name == "posix":
            uid = os.geteuid()
            runtime = env.get("XDG_RUNTIME_DIR") or f"/run/user/{uid}"
            env["XDG_RUNTIME_DIR"] = runtime
            env.setdefault("DBUS_SESSION_BUS_ADDRESS", f"unix:path={runtime}/bus")
        return env

    async def _systemctl(self, *args: str):
        try:
            cmd = ["systemctl", "--user"]
            proc = await asyncio.create_subprocess_exec(
                *cmd, *args,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
                env=self._systemctl_env(),
            )
            out, err = await proc.communicate()
            return proc.returncode == 0, (out + err).decode(errors="replace").strip()
        except Exception as e:
            return False, str(e)

    def _unit_name(self, site_id: str, kind: str) -> str:
        safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", site_id)
        return f"wd-{safe}-{kind}.service"

    def _write_unit(
        self,
        unit: str,
        description: str,
        command: list,
        work_dir: str,
        log_path: str,
        env_vars: dict = None,
    ):
        os.makedirs(self._systemd_dir, mode=0o700, exist_ok=True)
        command = list(command)
        if command and not os.path.isabs(command[0]):
            resolved = self._resolve_bin(command[0])
            if resolved:
                command[0] = resolved
        quoted = " ".join(self._systemd_quote(part) for part in command)
        merged_env = {"PATH": self._get_full_path(), "HOME": os.path.expanduser("~")}
        if env_vars:
            merged_env.update({k: str(v) for k, v in env_vars.items()})
        env_lines = ""
        for k, v in merged_env.items():
            safe = str(v).replace("\\", "\\\\").replace('"', '\\"').replace("%", "%%").replace("$", "$$")
            env_lines += f'Environment="{k}={safe}"\n'

        content = (
            "[Unit]\n"
            f"Description={description}\n"
            "After=network-online.target\nWants=network-online.target\nStartLimitIntervalSec=0\n\n"
            "[Service]\nType=simple\n"
            f"WorkingDirectory={work_dir}\n"
            f"{env_lines}"
            f"ExecStart={quoted}\n"
            "Restart=on-failure\nRestartSec=3\n"
            f"StandardOutput=append:{log_path}\nStandardError=append:{log_path}\n\n"
            "[Install]\nWantedBy=default.target\n"
        )
        _write_private(os.path.join(self._systemd_dir, unit), content)

    @staticmethod
    def _systemd_quote(part: str) -> str:
        part = str(part).replace("\\", "\\\\").replace('"', '\\"').replace("%", "%%").replace("$", "$$")
        return f'"{part}"'

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

    async def _wait_for_tunnel_url(self, log_path: str, timeout: int = 60):
        pattern = re.compile(r"https://[a-z0-9-]+\.trycloudflare\.com")
        for _ in range(timeout):
            await asyncio.sleep(1)
            try:
                with open(log_path, "r", encoding="utf-8", errors="replace") as f:
                    content = f.read()
            except OSError:
                continue
            for match in pattern.finditer(content):
                url = match.group(0)
                if url != "https://api.trycloudflare.com":
                    return url
        return None

    async def _wait_for_port(self, port: int, unit: str = None, timeout: int = 60):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                reader, writer = await asyncio.wait_for(asyncio.open_connection("127.0.0.1", port), timeout=2)
                writer.close()
                try:
                    await writer.wait_closed()
                except Exception:
                    pass
                return True
            except Exception:
                pass
            if unit:
                ok, out = await self._systemctl("is-failed", unit)
                if ok and out.strip() == "failed":
                    return False
            await asyncio.sleep(1)
        return False

    @staticmethod
    def _tail_log(path: str, limit: int = 600) -> str:
        try:
            with open(path, "r", encoding="utf-8", errors="replace") as f:
                data = f.read()
        except OSError:
            return ""
        return data[-limit:].strip()

    def _read_site_host(self, cf_log: str) -> str:
        if not cf_log:
            return ""
        text = self._tail_log(cf_log, 20000)
        matches = re.findall(r"https://([a-z0-9-]+\.trycloudflare\.com)", text, re.I)
        return "https://" + matches[-1] if matches else ""

    async def _refresh_site_url(self, site_id: str) -> str:
        sites = self._get_sites()
        site = sites.get(site_id)
        if not site:
            return ""
        fresh = self._read_site_host(site.get("cf_log") or "")
        if not fresh or fresh == site.get("url"):
            return ""
        previous = site.get("url") or ""
        site["url"] = fresh
        sites[site_id] = site
        self._set_sites(sites)
        logger.info("[WD] the tunnel of %s moved from %s to %s", site_id, previous or "none", fresh)
        await self._notify(
            self.strings["url_moved"].format(
                name=_escape(site.get("name", site_id)),
                previous=_escape(previous or "none"),
                url=fresh,
            )
        )
        return fresh

    async def _restart_site(self, site_id: str) -> bool:
        site = self._get_sites().get(site_id, {})
        http_unit = site.get("http_unit")
        cf_unit = site.get("cf_unit")
        port = site.get("port")
        if not http_unit or not cf_unit or not port:
            return False
        ok, _ = await self._start_unit(http_unit)
        if not ok and not await self._unit_active(http_unit):
            logger.warning("[WD] the app unit of %s refused to start", site_id)
            return False
        if not await self._wait_for_port(port, http_unit):
            logger.warning("[WD] the app of %s does not answer on port %s", site_id, port)
            return False
        ok, _ = await self._start_unit(cf_unit)
        if not ok and not await self._unit_active(cf_unit):
            logger.warning("[WD] the tunnel unit of %s refused to start", site_id)
            return False
        await self._wait_for_tunnel_url(site.get("cf_log") or "", timeout=40)
        await self._refresh_site_url(site_id)
        return True

    async def _heal_sites(self) -> None:
        for site_id, site in list(self._get_sites().items()):
            http_unit = site.get("http_unit")
            cf_unit = site.get("cf_unit")
            if not http_unit or not cf_unit:
                continue
            if await self._unit_active(http_unit) and await self._unit_active(cf_unit):
                self._site_heal_attempts.pop(site_id, None)
                self._site_down_notified.discard(site_id)
                await self._refresh_site_url(site_id)
                continue
            now = time.time()
            last = self._site_heal_attempts.get(site_id, 0.0)
            if now - last < SITE_HEAL_COOLDOWN:
                continue
            self._site_heal_attempts[site_id] = now
            if await self._restart_site(site_id):
                self._site_heal_attempts.pop(site_id, None)
                self._site_down_notified.discard(site_id)
                continue
            if site_id not in self._site_down_notified:
                self._site_down_notified.add(site_id)
                await self._notify(
                    self.strings["site_down"].format(name=_escape(site.get("name", site_id)))
                )

    def _start_site_monitor(self):
        async def site_loop():
            while True:
                await asyncio.sleep(SITE_HEAL_INTERVAL)
                try:
                    await self._heal_sites()
                except Exception as e:
                    logger.warning(f"[WD] the site monitor failed: {e}")

        if self._site_task:
            self._site_task.cancel()
        self._site_task = asyncio.create_task(site_loop())

    async def _reattach_sites(self):
        for site_id, site in list(self._get_sites().items()):
            http_unit = site.get("http_unit")
            cf_unit = site.get("cf_unit")
            if not http_unit or not cf_unit:
                self._remove_site(site_id)
                continue
            if await self._unit_active(http_unit):
                await self._refresh_site_url(site_id)
                continue
            if await self._restart_site(site_id):
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

    def _next_port(self, exclude=()) -> int:
        used = {site.get("port") for site in self._get_sites().values() if site.get("port")}
        used.update(exclude)
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

    async def _run_command(self, cmd: list, cwd: str, timeout: int = 900):
        cmd = list(cmd)
        resolved = self._resolve_bin(cmd[0]) if not os.path.isabs(cmd[0]) else cmd[0]
        if not resolved:
            return 127, f"{cmd[0]}: not found in PATH\nPATH={self._get_full_path()}"
        cmd[0] = resolved
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                cwd=cwd,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                env=self._proc_env(),
            )
        except Exception as e:
            return 127, f"{cmd[0]}: {e}"
        try:
            out, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout)
            return proc.returncode, (out or b"").decode(errors="replace")
        except asyncio.TimeoutError:
            try:
                proc.kill()
            except Exception:
                pass
            return -1, "Command timed out"

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
        reason = _guard_reason()
        if reason:
            raise RuntimeError(f"refused: {reason}")
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

        self._ensure_root()
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

    @staticmethod
    def _npm_install_args(p_dir: str):
        if os.path.exists(os.path.join(p_dir, "package-lock.json")) or os.path.exists(os.path.join(p_dir, "npm-shrinkwrap.json")):
            return ["npm", "ci", "--no-audit", "--no-fund", "--include=dev"]
        return ["npm", "install", "--no-audit", "--no-fund", "--include=dev"]

    async def _npm_install(self, p_dir: str, label: str):
        rc, out = await self._run_command(self._npm_install_args(p_dir), p_dir)
        if rc != 0:
            rc, out = await self._run_command(
                ["npm", "install", "--no-audit", "--no-fund", "--include=dev", "--legacy-peer-deps"], p_dir
            )
        if rc != 0:
            raise RuntimeError(f"npm install ({label}) failed:\n{out[-400:]}")

    async def _setup_project(self, site_dir: str, extract_dir: str, port: int, m: Message, filename: str):
        reason = _guard_reason()
        if reason:
            raise RuntimeError(f"refused: {reason}")
        pkg_dirs = []
        for dirpath, dirnames, filenames_in_dir in os.walk(extract_dir):
            dirnames[:] = [d for d in dirnames if d not in SKIP_DIR_NAMES and not d.startswith(".")]
            if "package.json" in filenames_in_dir:
                pkg_dirs.append(dirpath)

        backend_dir = None
        frontend_dir = None
        for p in pkg_dirs:
            base = os.path.basename(p).lower()
            if base in ("backend", "server", "api"):
                backend_dir = p
            elif base in ("frontend", "client", "web", "app", "ui"):
                frontend_dir = p

        node_bin = self._resolve_bin("node")
        npm_bin = self._resolve_bin("npm")
        has_node = bool(node_bin and npm_bin)
        if pkg_dirs and not has_node:
            raise RuntimeError(
                "node/npm not found.\nPATH=" + self._get_full_path()[:300]
            )

        if backend_dir and frontend_dir and has_node:
            await m.edit(
                self.strings["building"].format(name=_escape(filename), step="npm install (backend & frontend)..."),
                parse_mode="html",
            )
            env_example = os.path.join(backend_dir, ".env.example")
            env_file = os.path.join(backend_dir, ".env")
            if os.path.exists(env_example) and not os.path.exists(env_file):
                shutil.copy2(env_example, env_file)

            if os.path.exists(env_file):
                with open(env_file, "r", encoding="utf-8", errors="replace") as f:
                    env_content = f.read()
                if "SESSION_JWT_SECRET=" in env_content and not re.search(r"SESSION_JWT_SECRET=\S+", env_content):
                    secret = secrets.token_hex(32)
                    env_content = re.sub(r"SESSION_JWT_SECRET=.*", f"SESSION_JWT_SECRET={secret}", env_content)
                if "DEV_MODE=" in env_content:
                    env_content = re.sub(r"DEV_MODE=.*", "DEV_MODE=true", env_content)
                with open(env_file, "w", encoding="utf-8") as f:
                    f.write(env_content)

            await self._npm_install(backend_dir, "backend")
            await self._npm_install(frontend_dir, "frontend")

            with open(os.path.join(frontend_dir, "package.json"), "r", encoding="utf-8", errors="replace") as f:
                fe_pkg = json.load(f)
            static_dir = frontend_dir
            if "scripts" in fe_pkg and "build" in fe_pkg["scripts"]:
                await m.edit(
                    self.strings["building"].format(name=_escape(filename), step="npm run build (frontend)..."),
                    parse_mode="html",
                )
                rc, out = await self._run_command(["npm", "run", "build"], frontend_dir)
                if rc != 0:
                    raise RuntimeError(f"npm run build (frontend) failed:\n{out[-400:]}")
                for cand in ("dist", "build", "out", "public"):
                    cd = os.path.join(frontend_dir, cand)
                    if os.path.isdir(cd) and os.path.exists(os.path.join(cd, "index.html")):
                        static_dir = cd
                        break

            backend_port = self._next_port(exclude={port})
            runner_file = os.path.join(site_dir, "_runner.mjs")
            with open(runner_file, "w", encoding="utf-8") as f:
                f.write(_generate_node_runner())

            env_vars = {
                "PORT": str(port),
                "BACKEND_PORT": str(backend_port),
                "STATIC_DIR": static_dir,
                "BACKEND_DIR": backend_dir,
                "BACKEND_CMD": f"{self._backend_entry(backend_dir)}",
                "NODE_BIN": node_bin,
                "NPM_BIN": npm_bin,
            }
            return [node_bin, runner_file], site_dir, env_vars

        if len(pkg_dirs) == 1 and has_node:
            p_dir = pkg_dirs[0]
            with open(os.path.join(p_dir, "package.json"), "r", encoding="utf-8", errors="replace") as f:
                pkg_data = json.load(f)

            await m.edit(
                self.strings["building"].format(name=_escape(filename), step="npm install..."),
                parse_mode="html",
            )
            await self._npm_install(p_dir, "project")

            scripts = pkg_data.get("scripts", {})
            if "build" in scripts:
                await m.edit(
                    self.strings["building"].format(name=_escape(filename), step="npm run build..."),
                    parse_mode="html",
                )
                rc, out = await self._run_command(["npm", "run", "build"], p_dir)
                if rc != 0:
                    raise RuntimeError(f"npm run build failed:\n{out[-400:]}")

                for cand in ("dist", "build", "out", "public"):
                    cd = os.path.join(p_dir, cand)
                    if os.path.isdir(cd) and os.path.exists(os.path.join(cd, "index.html")):
                        return [sys.executable, "-m", "http.server", str(port), "--directory", cd], cd, {}

            if "start" in scripts:
                return [npm_bin, "start"], p_dir, {"PORT": str(port)}
            main_entry = pkg_data.get("main")
            entries = [main_entry] if main_entry else []
            entries += ["server.js", "index.js", "app.js", "main.js"]
            for entry in entries:
                if entry and os.path.exists(os.path.join(p_dir, entry)):
                    return [node_bin, entry], p_dir, {"PORT": str(port)}

        index_path = _find_site_index(extract_dir)
        if not index_path:
            return None, None, None

        serve_dir = os.path.dirname(index_path)
        return [sys.executable, "-m", "http.server", str(port), "--directory", serve_dir], serve_dir, {}

    @staticmethod
    def _backend_entry(backend_dir: str) -> str:
        try:
            with open(os.path.join(backend_dir, "package.json"), "r", encoding="utf-8", errors="replace") as f:
                pkg = json.load(f)
        except Exception:
            pkg = {}
        main_entry = pkg.get("main")
        for entry in ([main_entry] if main_entry else []) + ["server.js", "index.js", "app.js", "main.js"]:
            if entry and os.path.exists(os.path.join(backend_dir, entry)):
                return entry
        return "server.js"

    async def _fail_deploy(self, m, filename, error, site_dir, log_path=None):
        logger.error(f"[WD] Deploy failed for {filename}: {error}")
        await self._notify(
            f"<b>Deploy failed</b>\n<blockquote>{_escape(filename)}\n<code>{_escape(error[-500:])}</code></blockquote>"
        )
        shutil.rmtree(site_dir, ignore_errors=True)
        try:
            await m.edit(
                self.strings["deploy_fail"].format(error=_escape(error[-400:])),
                parse_mode="html",
            )
        except Exception:
            pass

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
        for site_id in list(self._get_sites()):
            await self._refresh_site_url(site_id)
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
        await self._refresh_site_url(site_id)
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
                [{"text": self.strings["btn_refresh"], "callback": self._cb_refresh_site, "args": (site_id,), "style": "primary"}],
                [{"text": self.strings["btn_stop"], "callback": self._cb_stop_site, "args": (site_id,), "style": "danger"}],
                [{"text": self.strings["btn_back"], "callback": self._cb_sites_menu, "style": "primary"}],
            ],
        )

    async def _cb_refresh_site(self, call: InlineCall, site_id: str):
        site = self._get_sites().get(site_id)
        if not site:
            await call.edit(
                self.strings["site_not_found"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_sites_menu, "style": "primary"}]],
            )
            return
        fresh = await self._refresh_site_url(site_id)
        if not fresh and not await self._unit_active(site.get("cf_unit") or ""):
            if await self._restart_site(site_id):
                fresh = self._get_sites().get(site_id, {}).get("url", "")
        await self._cb_site_detail(call, site_id)

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
        ru_doc="Реплай на .js/.jsx/.ts/.tsx/.html или архив для деплоя | без реплая открывает меню",
        en_doc="Reply to .js/.jsx/.ts/.tsx/.html or archive to deploy | without reply opens the menu",
    )
    async def wd(self, message: Message):
        """Reply to .js/.jsx/.ts/.tsx/.html or archive to deploy | without reply opens the menu"""
        if await self._guard_deny(message):
            return
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
            await self._fail_deploy(m, filename, str(e)[:400], site_dir)
            return

        port = self._next_port()
        cmd = None
        work_dir = site_dir
        env_vars = {}

        try:
            if archive_kind:
                await m.edit(
                    self.strings["extracting"].format(name=_escape(filename)),
                    parse_mode="html",
                )
                extract_dir = os.path.join(site_dir, "extracted")
                _safe_extract(file_path, extract_dir, archive_kind)
                try:
                    os.remove(file_path)
                except OSError:
                    pass

                cmd, work_dir, env_vars = await self._setup_project(site_dir, extract_dir, port, m, filename)
                if not cmd:
                    shutil.rmtree(site_dir, ignore_errors=True)
                    await m.edit(self.strings["no_index"], parse_mode="html")
                    return
            elif ext in ("js", "jsx", "ts", "tsx"):
                html = self._build_html_from_js(file_path, filename, ext)
                with open(os.path.join(site_dir, "index.html"), "w", encoding="utf-8") as f:
                    f.write(html)
                work_dir = site_dir
                cmd = [sys.executable, "-m", "http.server", str(port), "--directory", site_dir]
            else:
                dest = os.path.join(site_dir, "index.html")
                if file_path != dest:
                    shutil.copy2(file_path, dest)
                work_dir = site_dir
                cmd = [sys.executable, "-m", "http.server", str(port), "--directory", site_dir]

            await m.edit(
                self.strings["deploying"].format(name=_escape(filename)),
                parse_mode="html",
            )
        except Exception as e:
            logger.exception("[WD] deploy failed")
            await self._fail_deploy(m, filename, str(e)[:400], site_dir)
            return

        site_id = utils.rand(12)
        http_unit = self._unit_name(site_id, "app")
        cf_unit = self._unit_name(site_id, "cf")
        http_log = os.path.join(site_dir, "app.log")
        cf_log = os.path.join(site_dir, "cf.log")

        self._write_unit(
            http_unit,
            f"WebDeployer App server {site_id}",
            cmd,
            work_dir,
            http_log,
            env_vars=env_vars,
        )
        ok, out = await self._start_unit(http_unit)
        if not ok:
            self._remove_unit_file(http_unit)
            await self._fail_deploy(m, filename, out[:400], site_dir, http_log)
            return

        if not await self._wait_for_port(port, http_unit):
            tail = self._tail_log(http_log) or "app did not open port"
            await self._stop_unit(http_unit)
            self._remove_unit_file(http_unit)
            await self._systemctl("daemon-reload")
            await self._fail_deploy(m, filename, tail, site_dir, http_log)
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
            await self._systemctl("daemon-reload")
            await self._fail_deploy(m, filename, out[:400], site_dir, cf_log)
            return

        url = await self._wait_for_tunnel_url(cf_log)
        if not url:
            await self._stop_unit(http_unit)
            await self._stop_unit(cf_unit)
            self._remove_unit_file(http_unit)
            self._remove_unit_file(cf_unit)
            await self._systemctl("daemon-reload")
            tail = self._tail_log(cf_log)
            await self._notify(
                f"<b>Tunnel failed</b>\n<blockquote>{_escape(display_name)}\n<code>{_escape(tail[-500:])}</code></blockquote>"
            )
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
            "http_log": http_log,
            "cf_log": cf_log,
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