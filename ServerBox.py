__version__ = (2, 3, 0)
# meta developer: I_execute.t.me
# meta banner: https://raw.githubusercontent.com/i-execute/Modules/main/Storage/ServerBox/MetaBanner.jpeg

import logging
import asyncio
import socket
import time

from telethon.errors import FloodWaitError
from telethon.tl.functions.messages import EditMessageRequest
from telethon.tl.types import InputMediaWebPage
from .. import loader, utils

logger = logging.getLogger(__name__)

RELOADING_MEDIA_URL = "https://raw.githubusercontent.com/i-execute/Modules/main/Storage/ServerBox/Reloading.jpeg"


@loader.tds
class ServerBox(loader.Module):
    """Server resource monitor with CPU, RAM, swap alerts"""

    strings = {
        "name": "ServerBox",
        "reloaded": "<blockquote><b>ServerBox module successfully reloaded, everything works</b></blockquote>",
        "cpu_alert": (
            "<pre><code class=\"language-serverbox\">"
            "<b>CPU ALERT</b>\n"
            "----------------\n"
            "Usage:     {cpu}%\n"
            "Threshold: {threshold}%\n"
            "Host:      {hostname}"
            "</code></pre>"
        ),
        "ram_alert": (
            "<pre><code class=\"language-serverbox\">"
            "<b>RAM ALERT</b>\n"
            "----------------\n"
            "Usage:     {ram}%\n"
            "Used:      {used} MB\n"
            "Total:     {total} MB\n"
            "Threshold: {threshold}%\n"
            "Host:      {hostname}"
            "</code></pre>"
        ),
        "ram_process_alert": (
            "<pre><code class=\"language-serverbox\">"
            "<b>RAM PROCESS ALERT</b>\n"
            "----------------\n"
            "Usage:     {used} MB\n"
            "Threshold: {threshold} MB\n"
            "Host:      {hostname}"
            "</code></pre>"
        ),
        "swap_alert": (
            "<pre><code class=\"language-serverbox\">"
            "<b>SWAP ALERT</b>\n"
            "----------------\n"
            "Usage:     {swap}%\n"
            "Used:      {used} MB\n"
            "Total:     {total} MB\n"
            "Threshold: {threshold}%\n"
            "Host:      {hostname}"
            "</code></pre>"
        ),
        "status": (
            "<pre><code class=\"language-serverbox\">"
            "SERVER STATUS\n"
            "----------------\n"
            "Host:      {hostname}\n"
            "CPU:       {cpu}%\n"
            "RAM:       {ram}% ({ram_used}/{ram_total} MB)\n"
            "Swap:      {swap}"
            "</code></pre>"
        ),
        "monitor_started": "<blockquote><b>ServerBox:</b> Monitoring started.</blockquote>",
        "monitor_stopped": "<blockquote><b>ServerBox:</b> Monitoring stopped.</blockquote>",
    }

    strings_ru = {
        "name": "ServerBox",
        "reloaded": "<blockquote><b>Модуль ServerBox был успешно перезагружен, все воркает</b></blockquote>",
        "cpu_alert": (
            "<pre><code class=\"language-serverbox\">"
            "<b>CPU ALERT</b>\n"
            "----------------\n"
            "Usage:     {cpu}%\n"
            "Threshold: {threshold}%\n"
            "Host:      {hostname}"
            "</code></pre>"
        ),
        "ram_alert": (
            "<pre><code class=\"language-serverbox\">"
            "<b>RAM ALERT</b>\n"
            "----------------\n"
            "Usage:     {ram}%\n"
            "Used:      {used} MB\n"
            "Total:     {total} MB\n"
            "Threshold: {threshold}%\n"
            "Host:      {hostname}"
            "</code></pre>"
        ),
        "ram_process_alert": (
            "<pre><code class=\"language-serverbox\">"
            "<b>RAM PROCESS ALERT</b>\n"
            "----------------\n"
            "Usage:     {used} MB\n"
            "Threshold: {threshold} MB\n"
            "Host:      {hostname}"
            "</code></pre>"
        ),
        "swap_alert": (
            "<pre><code class=\"language-serverbox\">"
            "<b>SWAP ALERT</b>\n"
            "----------------\n"
            "Usage:     {swap}%\n"
            "Used:      {used} MB\n"
            "Total:     {total} MB\n"
            "Threshold: {threshold}%\n"
            "Host:      {hostname}"
            "</code></pre>"
        ),
        "status": (
            "<pre><code class=\"language-serverbox\">"
            "SERVER STATUS\n"
            "----------------\n"
            "Host:      {hostname}\n"
            "CPU:       {cpu}%\n"
            "RAM:       {ram}% ({ram_used}/{ram_total} MB)\n"
            "Swap:      {swap}"
            "</code></pre>"
        ),
        "monitor_started": "<blockquote><b>ServerBox:</b> Мониторинг запущен.</blockquote>",
        "monitor_stopped": "<blockquote><b>ServerBox:</b> Мониторинг остановлен.</blockquote>",
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "cpu_threshold",
                80.0,
                "CPU usage alert threshold (percent)",
                validator=loader.validators.Float(),
            ),
            loader.ConfigValue(
                "ram_system_threshold",
                80.0,
                "System RAM usage alert threshold (percent)",
                validator=loader.validators.Float(),
            ),
            loader.ConfigValue(
                "ram_process_threshold",
                500.0,
                "Process RAM usage alert threshold (MB)",
                validator=loader.validators.Float(),
            ),
            loader.ConfigValue(
                "swap_threshold",
                50.0,
                "Swap usage alert threshold (percent)",
                validator=loader.validators.Float(),
            ),
            loader.ConfigValue(
                "notification_interval",
                5,
                "Notification interval (seconds)",
                validator=loader.validators.Integer(minimum=1),
            ),
        )

        self._owner = None
        self._logger_topic = None
        self._asset_channel = None
        self._monitor_task = None
        self._last_cpu_alert = 0
        self._last_ram_system_alert = 0
        self._last_ram_process_alert = 0
        self._last_swap_alert = 0

    async def client_ready(self):
        self._owner = await self._client.get_me()
        self._asset_channel = self._db.get("heroku.forums", "channel_id", None)

        if not self._asset_channel:
            logger.warning("[ServerBox] heroku.forums channel_id not found, logging disabled.")
            return

        try:
            self._logger_topic = await utils.asset_forum_topic(
                self._client,
                self._db,
                self._asset_channel,
                "ServerBox",
                description="Server resource monitoring alerts.",
                icon_emoji_id=5188466187448650036,
            )
        except Exception as e:
            logger.error(f"[ServerBox] Failed to create/get forum topic: {e}")
            return

        chat_id = int(f"-100{self._asset_channel}")
        greeting_key = f"serverbox_greeted_{self._asset_channel}_{self._logger_topic.id}"
        already_greeted = self.get(greeting_key, False)

        if already_greeted:
            await self._send_with_preview(chat_id, self.strings["reloaded"])
        else:
            self.set(greeting_key, True)

        self._start_monitor()

    async def _send_with_preview(self, chat_id, text):
        try:
            msg_text, entities = await self.inline.bot._parse_message_text(text, "html")
            msg = await self._send_with_flood_wait(
                self.inline.bot.send_message,
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
                    logger.error(f"[ServerBox] Failed to add preview: {e}")
        except Exception as e:
            logger.error(f"[ServerBox] Failed to send message with preview: {e}")

    async def _send_with_flood_wait(self, coro_func, *args, **kwargs):
        for attempt in range(5):
            try:
                return await coro_func(*args, **kwargs)
            except FloodWaitError as e:
                await asyncio.sleep(e.seconds + 1)
            except Exception:
                raise
        return None

    async def _send_log(self, text: str):
        if not self._logger_topic or not self._asset_channel:
            return
        try:
            await self._send_with_flood_wait(
                self.inline.bot.send_message,
                int(f"-100{self._asset_channel}"),
                text,
                disable_web_page_preview=True,
                parse_mode="HTML",
                message_thread_id=self._logger_topic.id,
            )
        except Exception as e:
            logger.error(f"[ServerBox] Failed to send log: {e}")

    def _get_hostname(self):
        try:
            return socket.gethostname()
        except Exception:
            return "unknown"

    def _get_cpu(self):
        return float(utils.get_cpu_usage())

    def _get_ram(self):
        return utils.get_ram_usage_system()

    def _get_ram_process(self):
        return utils.get_ram_usage()

    def _get_swap(self):
        return utils.get_swap_usage()

    def _start_monitor(self):
        if self._monitor_task is None or self._monitor_task.done():
            self._monitor_task = asyncio.create_task(self._monitor_loop())
            logger.info("[ServerBox] Monitor started.")

    def _stop_monitor(self):
        if self._monitor_task and not self._monitor_task.done():
            self._monitor_task.cancel()
            logger.info("[ServerBox] Monitor stopped.")

    async def _monitor_loop(self):
        while True:
            try:
                await asyncio.sleep(1)

                hostname = self._get_hostname()
                now = time.time()
                interval = self.config["notification_interval"]

                cpu = self._get_cpu()
                if cpu >= self.config["cpu_threshold"]:
                    if now - self._last_cpu_alert >= interval:
                        await self._send_log(
                            self.strings["cpu_alert"].format(
                                cpu=round(cpu, 1),
                                threshold=self.config["cpu_threshold"],
                                hostname=hostname,
                            )
                        )
                        self._last_cpu_alert = now

                ram = self._get_ram()
                if "error" not in ram and ram["percent"] >= self.config["ram_system_threshold"]:
                    if now - self._last_ram_system_alert >= interval:
                        await self._send_log(
                            self.strings["ram_alert"].format(
                                ram=ram["percent"],
                                used=ram["used"],
                                total=ram["total"],
                                threshold=self.config["ram_system_threshold"],
                                hostname=hostname,
                            )
                        )
                        self._last_ram_system_alert = now

                ram_process = self._get_ram_process()
                if ram_process >= self.config["ram_process_threshold"]:
                    if now - self._last_ram_process_alert >= interval:
                        await self._send_log(
                            self.strings["ram_process_alert"].format(
                                used=round(ram_process, 1),
                                threshold=self.config["ram_process_threshold"],
                                hostname=hostname,
                            )
                        )
                        self._last_ram_process_alert = now

                swap = self._get_swap()
                if "error" not in swap and swap["percent"] >= self.config["swap_threshold"]:
                    if now - self._last_swap_alert >= interval:
                        await self._send_log(
                            self.strings["swap_alert"].format(
                                swap=swap["percent"],
                                used=swap["used"],
                                total=swap["total"],
                                threshold=self.config["swap_threshold"],
                                hostname=hostname,
                            )
                        )
                        self._last_swap_alert = now

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[ServerBox] Monitor loop error: {e}")
                await asyncio.sleep(5)

    async def on_unload(self):
        self._stop_monitor()