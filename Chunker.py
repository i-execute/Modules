__version__ = (1, 2, 0)
# meta developer: I_execute.t.me

import os
import re
import asyncio
import logging
import time
import tempfile
import json
import shutil

from telethon.tl.types import Message

from .. import loader, utils
from ..inline.types import InlineCall

logger = logging.getLogger(__name__)

CHUNKER_REPO = "HiveGamesOSS/Chunker"
VERSIONS_PER_PAGE = 5
FORMAT_ID_RE = re.compile(r"(?<![A-Z_])(JAVA|BEDROCK)((?:_R?\d+){1,4})(?![_\dR])")


def _escape(text):
    if not text:
        return ""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _format_bytes(b):
    if b < 1024:
        return f"{b} B"
    if b < 1024 * 1024:
        return f"{b / 1024:.1f} KB"
    if b < 1024 * 1024 * 1024:
        return f"{b / (1024 * 1024):.1f} MB"
    return f"{b / (1024 * 1024 * 1024):.2f} GB"


def _format_time(seconds):
    m, s = divmod(int(seconds), 60)
    ms = int((seconds - int(seconds)) * 100)
    if m > 0:
        return f"{m}:{s:02d}.{ms:02d}"
    return f"{s}.{ms:02d}s"


@loader.tds
class Chunker(loader.Module):
    """Bedrock <-> Java Minecraft world converter via Chunker CLI"""

    strings = {
        "name": "Chunker",
        "setup_menu": (
            "<b>Chunker</b>\n"
            "<blockquote>"
            "Chunker CLI: {chunker_status}\n"
            "GitHub token: {gh_status}"
            "</blockquote>"
        ),
        "chunker_install_menu": (
            "<b>Chunker CLI Installation</b>\n"
            "<blockquote>"
            "Installed: {current}\n"
            "Select version:"
            "</blockquote>"
        ),
        "chunker_installing": (
            "<b>Installing Chunker CLI</b>\n"
            "<blockquote>Version: {version}\nPlease wait...</blockquote>"
        ),
        "install_done": (
            "<b>Installation Complete</b>\n"
            "<blockquote>Chunker {version} installed</blockquote>"
        ),
        "install_fail": (
            "<b>Installation Failed</b>\n"
            "<blockquote>{error}</blockquote>"
        ),
        "collecting_versions": "<b>Collecting versions...</b>",
        "no_reply": "<b>Reply to a .zip or .mcworld file</b>",
        "wrong_type": "<b>File must be .zip or .mcworld</b>",
        "too_large": (
            "<b>File too large</b>\n"
            "<blockquote>Max: 2 GB\nSize: <code>{size}</code></blockquote>"
        ),
        "downloading": "<b>Downloading world</b>\n<blockquote><code>{time}</code></blockquote>",
        "direction_menu": (
            "<b>World Downloaded</b>\n"
            "<blockquote>"
            "File: <code>{name}</code>\n"
            "Size: <code>{size}</code>\n"
            "Select conversion direction:"
            "</blockquote>"
        ),
        "version_menu": (
            "<b>Select target version</b>\n"
            "<blockquote>Converting to {direction}</blockquote>"
        ),
        "converting": (
            "<b>Converting</b>\n"
            "<blockquote>Target: {version}\n<code>{time}</code></blockquote>"
        ),
        "uploading": "<b>Uploading result</b>\n<blockquote><code>{time}</code></blockquote>",
        "done": (
            "<b>Conversion Complete</b>\n"
            "<blockquote>"
            "Source: <code>{src}</code>\n"
            "Target: {version}\n"
            "Download: <code>{dl_time}</code>\n"
            "Convert: <code>{cv_time}</code>\n"
            "Upload: <code>{ul_time}</code>"
            "</blockquote>"
        ),
        "convert_fail": (
            "<b>Conversion Failed</b>\n"
            "<blockquote>{error}</blockquote>"
        ),
        "no_chunker": (
            "<b>Chunker CLI not installed</b>\n"
            "<blockquote>Use .chunk without reply to open Setup</blockquote>"
        ),
        "busy": (
            "<b>Already converting</b>\n"
            "<blockquote>Wait for current conversion to finish</blockquote>"
        ),
        "token_set": (
            "<b>GitHub token saved</b>\n"
            "<blockquote>Rate limit raised to 5000 req/h</blockquote>"
        ),
        "token_cleared": "<b>GitHub token cleared</b>",
        "btn_setup": "Setup",
        "btn_back": "Back",
        "btn_close": "Close",
        "btn_cancel": "Cancel",
        "btn_left": "⬅️",
        "btn_right": "➡️",
        "btn_install_other": "Choose Version",
        "btn_reinstall_other": "Choose Version",
        "btn_set_token": "Set GitHub Token",
        "btn_clear_token": "Clear Token",
        "btn_to_java": "Bedrock -> Java",
        "btn_to_bedrock": "Java -> Bedrock",
        "input_token": "Paste GitHub Personal Access Token:",
    }

    strings_ru = {
        "setup_menu": (
            "<b>Chunker</b>\n"
            "<blockquote>"
            "Chunker CLI: {chunker_status}\n"
            "GitHub токен: {gh_status}"
            "</blockquote>"
        ),
        "chunker_install_menu": (
            "<b>Установка Chunker CLI</b>\n"
            "<blockquote>"
            "Установлен: {current}\n"
            "Выберите версию:"
            "</blockquote>"
        ),
        "chunker_installing": (
            "<b>Установка Chunker CLI</b>\n"
            "<blockquote>Версия: {version}\nПодождите...</blockquote>"
        ),
        "install_done": (
            "<b>Установка завершена</b>\n"
            "<blockquote>Chunker {version} установлен</blockquote>"
        ),
        "install_fail": (
            "<b>Ошибка установки</b>\n"
            "<blockquote>{error}</blockquote>"
        ),
        "collecting_versions": "<b>Сбор версий...</b>",
        "no_reply": "<b>Ответьте на .zip или .mcworld файл</b>",
        "wrong_type": "<b>Файл должен быть .zip или .mcworld</b>",
        "too_large": (
            "<b>Файл слишком большой</b>\n"
            "<blockquote>Макс: 2 ГБ\nРазмер: <code>{size}</code></blockquote>"
        ),
        "downloading": "<b>Скачивание мира</b>\n<blockquote><code>{time}</code></blockquote>",
        "direction_menu": (
            "<b>Мир скачан</b>\n"
            "<blockquote>"
            "Файл: <code>{name}</code>\n"
            "Размер: <code>{size}</code>\n"
            "Выберите направление конвертации:"
            "</blockquote>"
        ),
        "version_menu": (
            "<b>Выберите целевую версию</b>\n"
            "<blockquote>Конвертация в {direction}</blockquote>"
        ),
        "converting": (
            "<b>Конвертация</b>\n"
            "<blockquote>Цель: {version}\n<code>{time}</code></blockquote>"
        ),
        "uploading": "<b>Загрузка результата</b>\n<blockquote><code>{time}</code></blockquote>",
        "done": (
            "<b>Конвертация завершена</b>\n"
            "<blockquote>"
            "Источник: <code>{src}</code>\n"
            "Цель: {version}\n"
            "Скачивание: <code>{dl_time}</code>\n"
            "Конвертация: <code>{cv_time}</code>\n"
            "Загрузка: <code>{ul_time}</code>"
            "</blockquote>"
        ),
        "convert_fail": (
            "<b>Ошибка конвертации</b>\n"
            "<blockquote>{error}</blockquote>"
        ),
        "no_chunker": (
            "<b>Chunker CLI не установлен</b>\n"
            "<blockquote>Используйте .chunk без реплая для Setup</blockquote>"
        ),
        "busy": (
            "<b>Уже конвертируется</b>\n"
            "<blockquote>Дождитесь завершения текущей конвертации</blockquote>"
        ),
        "token_set": (
            "<b>GitHub токен сохранён</b>\n"
            "<blockquote>Лимит запросов повышен до 5000/ч</blockquote>"
        ),
        "token_cleared": "<b>GitHub токен очищен</b>",
        "btn_setup": "Настройка",
        "btn_back": "Назад",
        "btn_close": "Закрыть",
        "btn_cancel": "Отмена",
        "btn_left": "⬅️",
        "btn_right": "➡️",
        "btn_install_other": "Выбрать версию",
        "btn_reinstall_other": "Выбрать версию",
        "btn_set_token": "Установить GitHub токен",
        "btn_clear_token": "Очистить токен",
        "btn_to_java": "Bedrock -> Java",
        "btn_to_bedrock": "Java -> Bedrock",
        "input_token": "Вставьте GitHub Personal Access Token:",
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "XMX_MB", 1500,
                "Лимит памяти (-Xmx) для java при конвертации, в МБ",
                validator=loader.validators.Integer(minimum=256),
            ),
        )
        self._root = None
        self._chunker_path = None
        self._sessions: dict = {}
        self._versions_cache = None

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        self._me = await client.get_me()
        self._root = os.path.join(tempfile.gettempdir(), f"MCWorld_{self._me.id}")
        self._chunker_path = os.path.join(self._root, "chunker-cli.jar")
        os.makedirs(self._root, exist_ok=True)

    def _chunker_installed(self):
        return bool(self._chunker_path and os.path.isfile(self._chunker_path))

    def _get_xmx_mb(self):
        try:
            return max(256, int(self.config["XMX_MB"]))
        except Exception:
            return 1500

    def _chunker_version(self):
        if not self._chunker_installed():
            return "not installed"
        return self._db.get("MCW", "chunker_version", "unknown")

    @staticmethod
    def _format_key_to_label(prefix: str, digits_part: str) -> str:
        nums = digits_part.strip("_").split("_")
        nums = [n[1:] if n.startswith("R") else n for n in nums]
        ver_str = ".".join(nums)
        label_prefix = "Java" if prefix == "JAVA" else "Bedrock"
        return f"{label_prefix} {ver_str}"

    @staticmethod
    def _version_sort_key(ver_key: str):
        nums = ver_key.split("_")[1:]
        nums = [n[1:] if n.startswith("R") else n for n in nums]
        try:
            return tuple(int(n) for n in nums) + (0,) * (6 - len(nums))
        except ValueError:
            return (0,) * 6

    async def _parse_chunker_formats(self, force: bool = False):
        if self._versions_cache is not None and not force:
            return self._versions_cache, None

        if not self._chunker_installed():
            return None, "Chunker не установлен. Установите его в .chunk -> Setup."

        java_bin = shutil.which("java") or "java"
        cmd = [java_bin, "-jar", self._chunker_path, "-f", "INVALID_FORMAT_LIST_TRIGGER", "-i", "/tmp", "-o", "/tmp"]
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout_b, stderr_b = await asyncio.wait_for(proc.communicate(), timeout=30)
        except Exception as e:
            return None, f"Failed to run chunker: {e}"

        combined = (stdout_b or b"").decode(errors="replace") + "\n" + (stderr_b or b"").decode(errors="replace")

        found_java = {}
        found_bedrock = {}
        for m in FORMAT_ID_RE.finditer(combined):
            prefix, digits_part = m.group(1), m.group(2)
            ver_key = f"{prefix}{digits_part}"
            label = self._format_key_to_label(prefix, digits_part)
            target = found_java if prefix == "JAVA" else found_bedrock
            target[ver_key] = label

        if not found_java and not found_bedrock:
            return None, "Could not parse versions from chunker output"

        java_list = sorted(found_java.items(), key=lambda kv: self._version_sort_key(kv[0]), reverse=True)
        bedrock_list = sorted(found_bedrock.items(), key=lambda kv: self._version_sort_key(kv[0]), reverse=True)

        result = {
            "java": [(label, key) for key, label in java_list],
            "bedrock": [(label, key) for key, label in bedrock_list],
        }
        self._versions_cache = result
        return result, None

    def _gh_token(self):
        return self._db.get("MCW", "gh_token", "")

    def _gh_headers(self):
        headers = []
        token = self._gh_token()
        if token:
            headers += ["-H", f"Authorization: Bearer {token}"]
        return headers

    async def _curl(self, *args, timeout=15):
        p = await asyncio.create_subprocess_exec(
            "curl", "-sL", "--max-time", str(timeout), *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        out, err = await p.communicate()
        return p.returncode, out, err

    async def _gh_get_releases(self):
        rc, out, _ = await self._curl(
            *self._gh_headers(),
            f"https://api.github.com/repos/{CHUNKER_REPO}/releases",
        )
        if rc != 0:
            return []
        try:
            data = json.loads(out.decode())
            if isinstance(data, list):
                return data[:10]
        except Exception:
            pass
        return []

    async def _install_tagged(self, tag: str):
        rc, out, _ = await self._curl(
            *self._gh_headers(),
            f"https://api.github.com/repos/{CHUNKER_REPO}/releases/tags/{tag}",
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
            name = asset.get("name", "").lower()
            if name.endswith(".jar") and "cli" in name:
                download_url = asset["browser_download_url"]
                break

        if not download_url:
            names = [a.get("name", "") for a in data.get("assets", [])]
            return False, f"No CLI jar in assets: {names}"

        dl_path = os.path.join(self._root, "chunker-cli.jar")
        p2 = await asyncio.create_subprocess_exec(
            "wget", "-q", "--max-redirect=15", "-O", dl_path, download_url,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        _, err2 = await asyncio.wait_for(p2.communicate(), timeout=300)
        size = os.path.getsize(dl_path) if os.path.isfile(dl_path) else 0
        if p2.returncode != 0 or size < 100_000:
            return False, f"Download failed (size={size}): {err2.decode()[:200]}"

        self._db.set("MCW", "chunker_version", tag)
        self._versions_cache = None
        return True, tag

    async def _timer_loop(self, editable, key, start, stop, is_call=True, **kw):
        while not stop.is_set():
            elapsed = time.time() - start
            txt = self.strings[key].format(time=_format_time(elapsed), **kw)
            try:
                if is_call:
                    await editable.edit(txt)
                else:
                    await editable.edit(txt, parse_mode="html")
            except Exception:
                pass
            try:
                await asyncio.wait_for(stop.wait(), timeout=1.7)
                break
            except asyncio.TimeoutError:
                pass

    async def _cb_close(self, call: InlineCall):
        await call.delete()

    async def _cb_setup_menu(self, call: InlineCall):
        installed = self._chunker_installed()
        btn_o = self.strings["btn_reinstall_other"] if installed else self.strings["btn_install_other"]

        await call.edit(
            self.strings["setup_menu"].format(
                chunker_status=_escape(self._chunker_version()),
                gh_status="set" if self._gh_token() else "not set",
            ),
            reply_markup=[
                [{"text": btn_o, "callback": self._cb_chunker_versions, "style": "primary"}],
                [{"text": self.strings["btn_set_token"], "callback": self._cb_set_token_input, "style": "primary"}],
                [{"text": self.strings["btn_clear_token"], "callback": self._cb_clear_token, "style": "danger"}],
                [{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}],
            ]
        )

    async def _cb_chunker_versions(self, call: InlineCall):
        await call.edit(self.strings["collecting_versions"])
        releases = await self._gh_get_releases()
        if not releases:
            await call.edit(
                self.strings["install_fail"].format(
                    error="Failed to fetch releases. Set a GitHub token in Setup to raise rate limit."
                ),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_setup_menu, "style": "primary"}]]
            )
            return

        current = self._chunker_version()
        markup = []
        for release in releases:
            tag = release.get("tag_name", "")
            if not tag:
                continue
            label = f"{tag} (current)" if tag == current else tag
            markup.append([{
                "text": label,
                "callback": self._cb_install_tagged,
                "args": (tag,),
                "style": "primary",
            }])
        markup.append([{"text": self.strings["btn_back"], "callback": self._cb_setup_menu, "style": "primary"}])

        await call.edit(
            self.strings["chunker_install_menu"].format(current=_escape(current)),
            reply_markup=markup,
        )

    async def _cb_install_tagged(self, call: InlineCall, tag: str):
        await call.edit(self.strings["chunker_installing"].format(version=_escape(tag)))
        ok, result = await self._install_tagged(tag)
        if ok:
            await call.edit(
                self.strings["install_done"].format(version=_escape(result)),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_setup_menu, "style": "primary"}]]
            )
        else:
            await call.edit(
                self.strings["install_fail"].format(error=_escape(result)),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_setup_menu, "style": "primary"}]]
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
            }]]
        )

    async def _cb_save_token(self, call: InlineCall, token: str):
        token = token.strip()
        if token:
            self._db.set("MCW", "gh_token", token)
            await call.edit(
                self.strings["token_set"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_setup_menu, "style": "primary"}]]
            )
        else:
            await self._cb_setup_menu(call)

    async def _cb_clear_token(self, call: InlineCall):
        self._db.set("MCW", "gh_token", "")
        await call.edit(
            self.strings["token_cleared"],
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_setup_menu, "style": "primary"}]]
        )

    @loader.command(
        ru_doc="Реплай на .zip/.mcworld для конвертации мира",
        en_doc="Reply to .zip/.mcworld to convert world",
    )
    async def chunk(self, message: Message):
        """Reply to .zip/.mcworld to convert world"""
        reply = await message.get_reply_message()

        if not reply or not reply.media:
            await self.inline.form(
                text=self.strings["setup_menu"].format(
                    chunker_status=_escape(self._chunker_version()),
                    gh_status="set" if self._gh_token() else "not set",
                ),
                message=message,
                reply_markup=[
                    [{"text": self.strings["btn_setup"], "callback": self._cb_setup_menu, "style": "primary"}],
                    [{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}],
                ],
                silent=True,
            )
            return

        if not self._chunker_installed():
            await utils.answer(message, self.strings["no_chunker"])
            return

        chat_id = utils.get_chat_id(message)
        if chat_id in self._sessions:
            await utils.answer(message, self.strings["busy"])
            return

        doc = getattr(reply.media, "document", None)
        if not doc:
            await utils.answer(message, self.strings["wrong_type"])
            return

        filename = "world"
        for attr in getattr(doc, "attributes", []):
            fn = getattr(attr, "file_name", None)
            if fn:
                filename = fn
                break

        ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
        if ext not in ("zip", "mcworld"):
            await utils.answer(message, self.strings["wrong_type"])
            return

        file_size = getattr(doc, "size", 0) or 0
        if file_size > 2 * 1024 * 1024 * 1024:
            await utils.answer(message, self.strings["too_large"].format(size=_format_bytes(file_size)))
            return

        m = await utils.answer(message, self.strings["downloading"].format(time="0.00s"))
        if isinstance(m, list):
            m = m[0]

        stop_dl = asyncio.Event()
        dl_start = time.time()
        timer = asyncio.ensure_future(
            self._timer_loop(m, "downloading", dl_start, stop_dl, is_call=False)
        )

        tmp_dir = os.path.join(self._root, f"tmp_{chat_id}_{int(time.time())}")
        os.makedirs(tmp_dir, exist_ok=True)
        world_path = os.path.join(tmp_dir, filename)

        try:
            await reply.download_media(file=world_path)
            dl_elapsed = time.time() - dl_start
            stop_dl.set()
            await timer

            actual_size = os.path.getsize(world_path) if os.path.isfile(world_path) else file_size

            self._sessions[chat_id] = {
                "path": world_path,
                "name": filename,
                "size": actual_size,
                "tmp_dir": tmp_dir,
                "dl_elapsed": dl_elapsed,
            }

            await m.delete()

            await self.inline.form(
                text=self.strings["direction_menu"].format(
                    name=_escape(filename),
                    size=_format_bytes(actual_size),
                ),
                message=message,
                reply_markup=[
                    [{"text": self.strings["btn_to_java"], "callback": self._cb_version_menu, "args": (chat_id, "java"), "style": "primary"}],
                    [{"text": self.strings["btn_to_bedrock"], "callback": self._cb_version_menu, "args": (chat_id, "bedrock"), "style": "primary"}],
                    [{"text": self.strings["btn_cancel"], "callback": self._cb_cancel, "args": (chat_id,), "style": "danger"}],
                ],
                silent=True,
            )

        except Exception as e:
            stop_dl.set()
            try:
                await timer
            except Exception:
                pass
            self._sessions.pop(chat_id, None)
            shutil.rmtree(tmp_dir, ignore_errors=True)
            try:
                await m.edit(
                    self.strings["convert_fail"].format(error=_escape(str(e)[:300])),
                    parse_mode="html",
                )
            except Exception:
                pass

    async def _cb_cancel(self, call: InlineCall, chat_id: int):
        session = self._sessions.pop(chat_id, None)
        if session:
            shutil.rmtree(session["tmp_dir"], ignore_errors=True)
        await call.delete()

    async def _cb_version_menu(self, call: InlineCall, chat_id: int, direction: str):
        if chat_id not in self._sessions:
            await call.edit(
                self.strings["convert_fail"].format(error="Session expired, use .chunk again"),
                reply_markup=[[{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}]]
            )
            return

        await call.edit(self.strings["collecting_versions"])
        versions_data, err = await self._parse_chunker_formats()
        if err:
            await call.edit(
                self.strings["convert_fail"].format(error=_escape(err)),
                reply_markup=[[{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}]]
            )
            return

        await self._render_version_page(call, chat_id, direction, page=0)

    async def _render_version_page(self, call: InlineCall, chat_id: int, direction: str, page: int):
        versions_data = self._versions_cache
        if not versions_data:
            await call.edit(
                self.strings["convert_fail"].format(error="Versions cache is empty, try again"),
                reply_markup=[[{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}]]
            )
            return

        versions = versions_data["java"] if direction == "java" else versions_data["bedrock"]
        direction_label = "Java" if direction == "java" else "Bedrock"

        total_pages = max(1, (len(versions) + VERSIONS_PER_PAGE - 1) // VERSIONS_PER_PAGE)
        page = max(0, min(page, total_pages - 1))
        start = page * VERSIONS_PER_PAGE
        page_versions = versions[start:start + VERSIONS_PER_PAGE]

        markup = []
        for ver_label, ver_key in page_versions:
            markup.append([{
                "text": ver_label,
                "callback": self._cb_convert,
                "args": (chat_id, ver_label, ver_key),
                "style": "primary",
            }])

        left_btn = {"text": self.strings["btn_left"], "callback": self._cb_version_page, "args": (chat_id, direction, page - 1)}
        right_btn = {"text": self.strings["btn_right"], "callback": self._cb_version_page, "args": (chat_id, direction, page + 1)}
        if page > 0:
            left_btn["style"] = "primary"
        if page < total_pages - 1:
            right_btn["style"] = "primary"
        markup.append([{"text": f"{page + 1}/{total_pages}", "callback": self._cb_noop, "style": "primary"}])
        markup.append([left_btn, right_btn])
        markup.append([{"text": self.strings["btn_cancel"], "callback": self._cb_cancel, "args": (chat_id,), "style": "danger"}])

        await call.edit(
            self.strings["version_menu"].format(direction=_escape(direction_label)),
            reply_markup=markup,
        )

    async def _cb_noop(self, call: InlineCall):
        await call.answer()

    async def _cb_version_page(self, call: InlineCall, chat_id: int, direction: str, page: int):
        if chat_id not in self._sessions:
            await call.answer()
            return
        await self._render_version_page(call, chat_id, direction, page)

    async def _cb_convert(self, call: InlineCall, chat_id: int, ver_label: str, ver_key: str):
        session = self._sessions.pop(chat_id, None)
        if not session:
            await call.edit(
                self.strings["convert_fail"].format(error="Session expired, use .chunk again"),
                reply_markup=[[{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}]]
            )
            return

        world_path = session["path"]
        tmp_dir = session["tmp_dir"]
        filename = session["name"]
        dl_elapsed = session["dl_elapsed"]
        out_dir = os.path.join(tmp_dir, "output")
        os.makedirs(out_dir, exist_ok=True)

        ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
        if ext in ("zip", "mcworld"):
            unzip_dir = os.path.join(tmp_dir, "unpacked")
            os.makedirs(unzip_dir, exist_ok=True)
            proc_unzip = await asyncio.create_subprocess_exec(
                "python3", "-m", "zipfile", "-e", world_path, unzip_dir,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            _, unzip_err = await proc_unzip.communicate()
            if proc_unzip.returncode != 0:
                await call.edit(
                    self.strings["convert_fail"].format(error=_escape(f"Unzip failed: {unzip_err.decode()[:200]}")),
                    reply_markup=[[{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}]]
                )
                shutil.rmtree(tmp_dir, ignore_errors=True)
                return

            input_dir = unzip_dir
            for root_d, dirs, files in os.walk(unzip_dir):
                if "level.dat" in files:
                    input_dir = root_d
                    break
        else:
            input_dir = world_path

        cv_start = time.time()
        stop_cv = asyncio.Event()
        cv_timer = asyncio.ensure_future(
            self._timer_loop(call, "converting", cv_start, stop_cv, version=_escape(ver_label))
        )

        try:
            java_bin = shutil.which("java") or "java"
            xmx_mb = self._get_xmx_mb()
            cmd = [java_bin, f"-Xmx{xmx_mb}M", "-jar", self._chunker_path, "-i", input_dir, "-f", ver_key, "-o", out_dir]
            logger.info("Chunker cmd: %s", " ".join(cmd))
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout_b, stderr_b = await asyncio.wait_for(proc.communicate(), timeout=3600)
            cv_elapsed = time.time() - cv_start
            stop_cv.set()
            await cv_timer

            stdout_text = (stdout_b or b"").decode(errors="replace").strip()
            stderr_text = (stderr_b or b"").decode(errors="replace").strip()
            logger.info("Chunker exit=%d stdout=%s stderr=%s", proc.returncode, stdout_text, stderr_text)

            async def _send_log_and_fail(reason: str):
                log_path = os.path.join(tmp_dir, "chunker_debug.txt")
                with open(log_path, "w") as lf:
                    lf.write(f"CMD: {' '.join(cmd)}\n")
                    lf.write(f"EXIT: {proc.returncode}\n\n")
                    lf.write(f"STDOUT:\n{stdout_text}\n\nSTDERR:\n{stderr_text}\n")
                try:
                    await self._client.send_file(
                        call.form["chat"],
                        log_path,
                        caption=self.strings["convert_fail"].format(error=_escape(reason)),
                        parse_mode="html",
                    )
                except Exception:
                    pass
                await call.edit(
                    self.strings["convert_fail"].format(error=_escape(reason)),
                    reply_markup=[[{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}]]
                )
                shutil.rmtree(tmp_dir, ignore_errors=True)

            if proc.returncode != 0:
                await _send_log_and_fail(f"exit code {proc.returncode}")
                return

            out_files = []
            for root_d, _, files in os.walk(out_dir):
                for f in files:
                    out_files.append(os.path.join(root_d, f))

            if not out_files:
                await _send_log_and_fail("No output files produced")
                return

            if len(out_files) == 1:
                result_path = out_files[0]
            else:
                base = filename.rsplit(".", 1)[0] if "." in filename else filename
                result_path = os.path.join(tmp_dir, f"{base}_{ver_key}.zip")
                proc2 = await asyncio.create_subprocess_exec(
                    "python3", "-m", "zipfile", "-c", result_path, out_dir,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                await proc2.communicate()

            ul_start = time.time()
            stop_ul = asyncio.Event()
            ul_timer = asyncio.ensure_future(
                self._timer_loop(call, "uploading", ul_start, stop_ul)
            )

            try:
                await self._client.send_file(
                    call.form["chat"],
                    result_path,
                    caption=self.strings["done"].format(
                        src=_escape(filename),
                        version=_escape(ver_label),
                        dl_time=_format_time(dl_elapsed),
                        cv_time=_format_time(cv_elapsed),
                        ul_time=_format_time(time.time() - ul_start),
                    ),
                    parse_mode="html",
                )
                ul_elapsed = time.time() - ul_start
                stop_ul.set()
                await ul_timer

                await call.edit(
                    self.strings["done"].format(
                        src=_escape(filename),
                        version=_escape(ver_label),
                        dl_time=_format_time(dl_elapsed),
                        cv_time=_format_time(cv_elapsed),
                        ul_time=_format_time(ul_elapsed),
                    ),
                    reply_markup=[[{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}]]
                )
            except Exception as e:
                stop_ul.set()
                await ul_timer
                await call.edit(
                    self.strings["convert_fail"].format(error=_escape(str(e)[:300])),
                    reply_markup=[[{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}]]
                )

        except asyncio.TimeoutError:
            stop_cv.set()
            try:
                await cv_timer
            except Exception:
                pass
            await call.edit(
                self.strings["convert_fail"].format(error="Conversion timeout (10 min)"),
                reply_markup=[[{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}]]
            )
        except Exception as e:
            stop_cv.set()
            try:
                await cv_timer
            except Exception:
                pass
            await call.edit(
                self.strings["convert_fail"].format(error=_escape(str(e)[:300])),
                reply_markup=[[{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}]]
            )
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)