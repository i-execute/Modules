__version__ = (1, 0, 0)
# meta developer: I_execute.t.me
# meta banner: https://raw.githubusercontent.com/i-execute/Modules/main/Storage/AutoAvka/MetaBanner.jpeg

try:
    from herokutl.errors import common
    import herokutl.tl.tlobject as tlobj
    common.ScamDetectionError = Exception
    tlobj.TLObject.__new__ = lambda cls, *a, **k: object.__new__(cls)
except Exception:
    pass

import logging
import asyncio
import aiohttp
import io
import time
from datetime import datetime, timezone, timedelta

from telethon import TelegramClient, functions, types
from telethon.errors import FloodWaitError

from .. import loader, utils

logger = logging.getLogger(__name__)


@loader.tds
class AutoAvka(loader.Module):
    """Automatic avatar changer based on session activity"""

    strings = {
        "name": "AutoAvka",
        "help": "<b>AutoAvka Help:</b>\n<blockquote>.avka - Open control menu\n.avkashow - Download and show current avatars</blockquote>",
        "config_missing": "<b>Error:</b> Please set ONLINE_AVKA_URL and OFFLINE_AVKA_URL in config",
        "session_not_selected": "Error: No session selected",
        "avatars_sent": "Avatars sent",
        "no_avatars": "No avatars to show",
        "menu_title": "AutoAvka Control",
        "status_enabled": "Status: Enabled",
        "status_disabled": "Status: Disabled",
        "status_started": "Loop: Running",
        "status_not_started": "Loop: Stopped",
        "btn_toggle_on": "Disable",
        "btn_toggle_off": "Enable",
        "btn_select": "Select Session",
        "btn_cancel": "Cancel",
        "session_list_title": "chose session for target:",
        "btn_back": "Back",
        "btn_next": "Next",
        "btn_select_session": "Select",
        "session_info_title": "<b>Session Info</b>\n<blockquote>{}</blockquote>",
        "testflight_wait": "Testflight, please wait",
        "testflight_success": "Testflight completed successfully",
        "testflight_partial": "Testflight partial success\n{}",
        "testflight_fail": "Testflight failed\n{}",
        "monitor_started": "Monitor started",
        "monitor_stopped": "Monitor stopped",
        "avatar_switched": "Avatar switched to {}",
        "error_download": "Error downloading image",
        "error_upload": "Error uploading avatar",
        "error_delete": "Error deleting avatar",
    }

    strings_ru = {
        "name": "AutoAvka",
        "help": "<b>AutoAvka использование:</b>\n<blockquote>.avka - Открыть меню управления\n.avkashow - Скачать и показать текущие аватарки</blockquote>",
        "config_missing": "<b>Ошибка:</b> Укажите ONLINE_AVKA_URL и OFFLINE_AVKA_URL в конфиге",
        "session_not_selected": "Ошибка: Сессия не выбрана",
        "avatars_sent": "Аватарки отправлены",
        "no_avatars": "Нет аватарок для показа",
        "menu_title": "Управление AutoAvka",
        "status_enabled": "Статус: Включено",
        "status_disabled": "Статус: Выключено",
        "status_started": "Цикл: Запущен",
        "status_not_started": "Цикл: Остановлен",
        "btn_toggle_on": "Выключить",
        "btn_toggle_off": "Включить",
        "btn_select": "Выбрать сессию",
        "btn_cancel": "Отмена",
        "session_list_title": "chose session for target:",
        "btn_back": "Назад",
        "btn_next": "Далее",
        "btn_select_session": "Выбрать",
        "session_info_title": "<b>Инфа о сессии</b>\n<blockquote>{}</blockquote>",
        "testflight_wait": "Testflight, please wait",
        "testflight_success": "Testflight завершен успешно",
        "testflight_partial": "Testflight частично успешен\n{}",
        "testflight_fail": "Testflight не удался\n{}",
        "monitor_started": "Мониторинг запущен",
        "monitor_stopped": "Мониторинг остановлен",
        "avatar_switched": "Аватарка изменена на {}",
        "error_download": "Ошибка загрузки изображения",
        "error_upload": "Ошибка загрузки аватарки",
        "error_delete": "Ошибка удаления аватарки",
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "UPDATE_AFTER", 30, "Minutes after offline to switch avatar",
                validator=loader.validators.Integer(minimum=1),
            ),
            loader.ConfigValue(
                "REQUEST_RETRY", 60, "Seconds between authorization checks",
                validator=loader.validators.Integer(minimum=10),
            ),
            loader.ConfigValue(
                "ONLINE_AVKA_URL", "", "URL for online avatar",
                validator=loader.validators.String(),
            ),
            loader.ConfigValue(
                "OFFLINE_AVKA_URL", "", "URL for offline avatar",
                validator=loader.validators.String(),
            ),
        )

        self._monitor_task = None
        self._current_online = None
        self._last_check = 0
        self._db = None

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        if self._get_enabled() and self._get_started():
            await self._start_monitor()

    async def on_unload(self):
        if self._monitor_task:
            self._monitor_task.cancel()
            self._monitor_task = None

    def _get_session_hash(self):
        return self._db.get("AutoAvka", "selected_session_hash", 0)

    def _set_session_hash(self, value):
        self._db.set("AutoAvka", "selected_session_hash", value)

    def _get_enabled(self):
        return self._db.get("AutoAvka", "enabled", False)

    def _set_enabled(self, value):
        self._db.set("AutoAvka", "enabled", value)

    def _get_started(self):
        return self._db.get("AutoAvka", "started", False)

    def _set_started(self, value):
        self._db.set("AutoAvka", "started", value)

    async def _download_file(self, url):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        return await response.read()
        except Exception as e:
            logger.error(f"Download error: {e}")
        return None

    async def _convert_video(self, data):
        import tempfile
        import os
        tmp_in = None
        tmp_out = None
        try:
            with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as f:
                f.write(data)
                tmp_in = f.name
            tmp_out = tmp_in + "_out.mp4"
            proc = await asyncio.create_subprocess_exec(
                "ffmpeg", "-y",
                "-i", tmp_in,
                "-vf", "crop=min(iw\\,ih):min(iw\\,ih),scale=512:512",
                "-c:v", "libx264",
                "-pix_fmt", "yuv420p",
                "-movflags", "+faststart",
                "-an",
                tmp_out,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            _, stderr = await proc.communicate()
            if proc.returncode != 0:
                logger.error(f"ffmpeg error: {stderr.decode()}")
                return None
            with open(tmp_out, "rb") as f:
                return f.read()
        except Exception as e:
            logger.error(f"Convert error: {e}")
            return None
        finally:
            for p in (tmp_in, tmp_out):
                if p:
                    try:
                        os.unlink(p)
                    except Exception:
                        pass

    async def _upload_avatar(self, data):
        try:
            converted = await self._convert_video(data)
            if converted:
                data = converted
            else:
                logger.warning("ffmpeg conversion failed, using original data")

            file = await self._client.upload_file(
                io.BytesIO(data),
                file_name="avatar.mp4"
            )

            await self._client(
                functions.photos.UploadProfilePhotoRequest(
                    video=file
                )
            )

            return True

        except Exception as e:
            logger.error(f"Upload error: {e}")
            return False

    async def _delete_avatars(self):
        try:
            photos = await self._client(functions.photos.GetUserPhotosRequest(
                user_id=types.InputPeerSelf(), offset=0, max_id=0, limit=10
            ))
            if photos.photos:
                ids = [types.InputPhoto(id=p.id, access_hash=p.access_hash, file_reference=p.file_reference) for p in photos.photos]
                await self._client(functions.photos.DeletePhotosRequest(id=ids))
                return True
        except Exception as e:
            logger.error(f"Delete error: {e}")
        return False

    async def _get_authorizations(self):
        try:
            result = await self._client(functions.account.GetAuthorizationsRequest())
            logger.info(f"GetAuthorizationsRequest success: {len(result.authorizations)} sessions")
            return result.authorizations
        except FloodWaitError as e:
            logger.warning(f"FloodWaitError: {e.seconds} seconds")
            raise
        except Exception as e:
            logger.info(f"GetAuthorizationsRequest failed: {e}")
            return None

    async def _start_monitor(self):
        if self._monitor_task:
            self._monitor_task.cancel()
        self._current_online = None
        self._monitor_task = asyncio.create_task(self._monitor_loop())

    async def _monitor_loop(self):
        while self._get_enabled():
            try:
                now = datetime.now(timezone.utc)
                auths = await self._get_authorizations()
                if not auths:
                    await asyncio.sleep(self.config["REQUEST_RETRY"])
                    continue

                target = None
                for auth in auths:
                    if auth.hash == self._get_session_hash():
                        target = auth
                        break

                if not target:
                    logger.info("Selected session not found in authorizations")
                    await asyncio.sleep(self.config["REQUEST_RETRY"])
                    continue

                is_online = False
                if target.date_active:
                    delta = now - target.date_active
                    minutes_ago = delta.total_seconds() / 60
                    if minutes_ago < self.config["UPDATE_AFTER"]:
                        is_online = True

                if is_online != self._current_online or self._current_online is None:
                    self._current_online = is_online
                    url = self.config["ONLINE_AVKA_URL"] if is_online else self.config["OFFLINE_AVKA_URL"]
                    data = await self._download_file(url)
                    if data:
                        await self._delete_avatars()
                        success = await self._upload_avatar(data)
                        if success:
                            logger.info(f"Avatar switched to {'online' if is_online else 'offline'}")
                        else:
                            logger.error("Failed to upload avatar on state change")

                await asyncio.sleep(self.config["REQUEST_RETRY"])
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Monitor loop error: {e}")
                await asyncio.sleep(self.config["REQUEST_RETRY"])

    async def _testflight(self, call):
        await call.edit(self.strings("testflight_wait"))
        errors = []
        offline_data = await self._download_file(self.config["OFFLINE_AVKA_URL"])
        online_data = await self._download_file(self.config["ONLINE_AVKA_URL"])
        offline_ok = False
        online_ok = False

        if offline_data:
            if await self._upload_avatar(offline_data):
                offline_ok = True
            else:
                errors.append("Offline upload failed")

        if online_data:
            if await self._upload_avatar(online_data):
                online_ok = True
            else:
                errors.append("Online upload failed")

        if offline_ok and online_ok:
            await self._delete_avatars()
            self._set_started(True)
            await self._start_monitor()
            await call.edit(self.strings("testflight_success"))
        elif offline_ok or online_ok:
            await self._delete_avatars()
            log = "\n".join(errors)
            await call.edit(self.strings("testflight_partial").format(log))
        else:
            log = "\n".join(errors)
            await call.edit(self.strings("testflight_fail").format(log))

    async def _cb_toggle(self, call):
        self._set_enabled(not self._get_enabled())
        if self._get_enabled() and self._get_started():
            await self._start_monitor()
        elif not self._get_enabled():
            if self._monitor_task:
                self._monitor_task.cancel()
                self._monitor_task = None
        await self._show_main_menu(call)

    async def _cb_cancel(self, call):
        try:
            await call.delete()
        except Exception:
            pass

    async def _show_main_menu(self, call):
        status = self.strings("status_enabled") if self._get_enabled() else self.strings("status_disabled")
        loop_status = self.strings("status_started") if self._get_started() else self.strings("status_not_started")
        text = f"{self.strings('menu_title')}\n\n{status}\n{loop_status}"
        markup = []
        if self._get_started():
            toggle_text = self.strings("btn_toggle_on") if self._get_enabled() else self.strings("btn_toggle_off")
            toggle_style = "danger" if self._get_enabled() else "success"
            markup.append([{"text": toggle_text, "callback": self._cb_toggle, "style": toggle_style}])
        markup.append([{"text": self.strings("btn_select"), "callback": self._cb_select_session, "style": "primary"}])
        markup.append([{"text": self.strings("btn_cancel"), "callback": self._cb_cancel, "style": "danger"}])
        await call.edit(text, reply_markup=markup)

    async def _cb_select_session(self, call):
        await self._show_session_list(call, 0)

    async def _show_session_list(self, call, page):
        auths = await self._get_authorizations()
        if not auths:
            await call.answer("No sessions found", show_alert=True)
            return

        per_page = 3
        total_pages = (len(auths) + per_page - 1) // per_page
        start = page * per_page
        end = start + per_page
        sessions = auths[start:end]

        markup = []
        for auth in sessions:
            markup.append([{"text": auth.app_name, "callback": self._cb_session_info, "args": (auth.hash,)}])

        nav_row = []
        if page > 0:
            nav_row.append({"text": self.strings("btn_back"), "callback": self._cb_session_page, "args": (page - 1,), "style": "primary"})
        if page < total_pages - 1:
            nav_row.append({"text": self.strings("btn_next"), "callback": self._cb_session_page, "args": (page + 1,), "style": "primary"})

        if nav_row:
            markup.append(nav_row)

        await call.edit(self.strings("session_list_title"), reply_markup=markup)

    async def _cb_session_page(self, call, page):
        await self._show_session_list(call, page)

    async def _cb_session_info(self, call, session_hash):
        auths = await self._get_authorizations()
        target = None
        for auth in auths:
            if auth.hash == session_hash:
                target = auth
                break

        if not target:
            await call.answer("Session not found", show_alert=True)
            return

        now = datetime.now(timezone.utc)
        ago = "unknown"
        if target.date_active:
            delta = now - target.date_active
            ago = f"{delta.days}d {(delta.seconds // 3600)}h {((delta.seconds % 3600) // 60)}m ago"

        info = (
            f"app: {target.app_name}\n"
            f"api_id: {target.api_id}\n"
            f"device: {target.device_model}\n"
            f"platform: {target.platform}\n"
            f"system: {target.system_version}\n"
            f"last_active: {target.date_active}\n"
            f"online: {ago}\n"
            f"country: {target.country}"
        )

        markup = [
            [
                {"text": self.strings("btn_back"), "callback": self._cb_select_session, "style": "primary"},
                {"text": self.strings("btn_select_session"), "callback": self._cb_confirm_select, "args": (session_hash,), "style": "success"},
            ],
            [{"text": self.strings("btn_cancel"), "callback": self._cb_cancel, "style": "danger"}],
        ]

        await call.edit(self.strings("session_info_title").format(info), reply_markup=markup)

    async def _cb_confirm_select(self, call, session_hash):
        self._set_session_hash(session_hash)
        await self._testflight(call)

    @loader.command(
        ru_doc="Открыть меню управления", 
        en_doc="Open control menu", 
    )
    async def avka(self, message):
        """Open control menu"""
        if not self.config["ONLINE_AVKA_URL"] or not self.config["OFFLINE_AVKA_URL"]:
            await utils.answer(message, self.strings("config_missing"))
            return

        status = self.strings("status_enabled") if self._get_enabled() else self.strings("status_disabled")
        loop_status = self.strings("status_started") if self._get_started() else self.strings("status_not_started")
        text = f"{self.strings('menu_title')}\n\n{status}\n{loop_status}"
        markup = []
        if self._get_started():
            toggle_text = self.strings("btn_toggle_on") if self._get_enabled() else self.strings("btn_toggle_off")
            toggle_style = "danger" if self._get_enabled() else "success"
            markup.append([{"text": toggle_text, "callback": self._cb_toggle, "style": toggle_style}])
        markup.append([{"text": self.strings("btn_select"), "callback": self._cb_select_session, "style": "primary"}])
        markup.append([{"text": self.strings("btn_cancel"), "callback": self._cb_cancel, "style": "danger"}])
        await self.inline.form(text=text, message=message, reply_markup=markup, silent=True)

    @loader.command(
        ru_doc="Скачать и показать аватарки", 
        en_doc="Download and show current avatars", 
    )
    async def avkashow(self, message):
        """Download and show current avatars"""
        if not self.config["ONLINE_AVKA_URL"] or not self.config["OFFLINE_AVKA_URL"]:
            await utils.answer(message, self.strings("config_missing"))
            return

        online_data = await self._download_file(self.config["ONLINE_AVKA_URL"])
        offline_data = await self._download_file(self.config["OFFLINE_AVKA_URL"])

        if not online_data and not offline_data:
            await utils.answer(message, self.strings("no_avatars"))
            return

        files = []
        if online_data:
            converted = await self._convert_video(online_data)
            if converted:
                online_data = converted
            f = io.BytesIO(online_data)
            f.name = "online.mp4"
            files.append(f)
        if offline_data:
            converted = await self._convert_video(offline_data)
            if converted:
                offline_data = converted
            f = io.BytesIO(offline_data)
            f.name = "offline.mp4"
            files.append(f)

        await self._client.send_file(message.chat_id, files, caption=self.strings("avatars_sent"))