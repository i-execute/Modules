__version__ = (2, 0, 0)
# meta developer: I_execute.t.me 

import logging
import asyncio
import socket
import psutil

from telethon.errors import FloodWaitError
from .. import loader, utils

logger = logging.getLogger(__name__)

GREETING_MEDIA_URL = "https://raw.githubusercontent.com/i-execute/Modules/main/Storage/Logger/Greetings.jpeg"


@loader.tds
class ServerBox(loader.Module):
    """Server resource monitor - CPU, RAM, swap, hostname"""

    strings = {
        "name": "ServerBox",
        "greeting_first": (
            "<blockquote><b>ServerBox active.</b></blockquote>\n"
            "<blockquote>Resource monitoring is running. Alerts will be sent here when thresholds are exceeded.</blockquote>"
        ),
        "reloaded": "<blockquote><b>ServerBox module reloaded, monitoring resumed.</b></blockquote>",
        "cpu_alert": (
            "<pre><code class=\"language-serverbox\">"
            "CPU ALERT\n"
            "----------------\n"
            "Usage:     {cpu}%\n"
            "Threshold: {threshold}%\n"
            "Host:      {hostname}"
            "</code></pre>"
        ),
        "ram_alert": (
            "<pre><code class=\"language-serverbox\">"
            "RAM ALERT\n"
            "----------------\n"
            "Usage:     {ram}%\n"
            "Used:      {used} MB\n"
            "Total:     {total} MB\n"
            "Threshold: {threshold}%\n"
            "Host:      {hostname}"
            "</code></pre>"
        ),
        "swap_alert": (
            "<pre><code class=\"language-serverbox\">"
            "SWAP ALERT\n"
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
            "Swap:      {swap}% ({swap_used}/{swap_total} MB)"
            "</code></pre>"
        ),
        "monitor_started": "<blockquote><b>ServerBox:</b> Monitoring started.</blockquote>",
        "monitor_stopped": "<blockquote><b>ServerBox:</b> Monitoring stopped.</blockquote>",
    }

    strings_ru = {
        "name": "ServerBox",
        "greeting_first": (
            "<blockquote><b>ServerBox активен.</b></blockquote>\n"
            "<blockquote>Мониторинг ресурсов запущен. Алерты будут отправляться сюда при превышении порогов.</blockquote>"
        ),
        "reloaded": "<blockquote><b>Модуль ServerBox перезагружен, мониторинг возобновлён.</b></blockquote>",
        "cpu_alert": (
            "<pre><code class=\"language-serverbox\">"
            "CPU ALERT\n"
            "----------------\n"
            "Usage:     {cpu}%\n"
            "Threshold: {threshold}%\n"
            "Host:      {hostname}"
            "</code></pre>"
        ),
        "ram_alert": (
            "<pre><code class=\"language-serverbox\">"
            "RAM ALERT\n"
            "----------------\n"
            "Usage:     {ram}%\n"
            "Used:      {used} MB\n"
            "Total:     {total} MB\n"
            "Threshold: {threshold}%\n"
            "Host:      {hostname}"
            "</code></pre>"
        ),
        "swap_alert": (
            "<pre><code class=\"language-serverbox\">"
            "SWAP ALERT\n"
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
            "Swap:      {swap}% ({swap_used}/{swap_total} MB)"
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
                "ram_threshold",
                80.0,
                "RAM usage alert threshold (percent)",
                validator=loader.validators.Float(),
            ),
            loader.ConfigValue(
                "swap_threshold",
                50.0,
                "Swap usage alert threshold (percent)",
                validator=loader.validators.Float(),
            ),
            loader.ConfigValue(
                "check_interval",
                10,
                "Resource check interval (seconds)",
                validator=loader.validators.Integer(minimum=3),
            ),
        )

        self._owner = None
        self._logger_topic = None
        self._asset_channel = None
        self._monitor_task = None

        psutil.cpu_percent(interval=None)

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

        try:
            await self._send_with_flood_wait(
                self.inline.bot.send_photo,
                int(f"-100{self._asset_channel}"),
                photo=GREETING_MEDIA_URL,
                caption=self.strings["greeting_first"],
                message_thread_id=self._logger_topic.id,
            )
        except Exception:
            try:
                await self._send_with_flood_wait(
                    self.inline.bot.send_message,
                    int(f"-100{self._asset_channel}"),
                    self.strings["reloaded"],
                    message_thread_id=self._logger_topic.id,
                )
            except Exception as e:
                logger.error(f"[ServerBox] Failed to send greeting: {e}")

        self._start_monitor()

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
        return psutil.cpu_percent(interval=1)

    def _get_ram(self):
        vm = psutil.virtual_memory()
        return {
            "percent": vm.percent,
            "used": round(vm.used / 1024 / 1024),
            "total": round(vm.total / 1024 / 1024),
        }

    def _get_swap(self):
        sw = psutil.swap_memory()
        return {
            "percent": sw.percent,
            "used": round(sw.used / 1024 / 1024),
            "total": round(sw.total / 1024 / 1024),
        }

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
                interval = max(3, self.config["check_interval"])
                await asyncio.sleep(interval)

                hostname = self._get_hostname()

                cpu = self._get_cpu()
                if cpu >= self.config["cpu_threshold"]:
                    await self._send_log(
                        self.strings["cpu_alert"].format(
                            cpu=round(cpu, 1),
                            threshold=self.config["cpu_threshold"],
                            hostname=hostname,
                        )
                    )

                ram = self._get_ram()
                if ram["percent"] >= self.config["ram_threshold"]:
                    await self._send_log(
                        self.strings["ram_alert"].format(
                            ram=round(ram["percent"], 1),
                            used=ram["used"],
                            total=ram["total"],
                            threshold=self.config["ram_threshold"],
                            hostname=hostname,
                        )
                    )

                swap = self._get_swap()
                if swap["total"] > 0 and swap["percent"] >= self.config["swap_threshold"]:
                    await self._send_log(
                        self.strings["swap_alert"].format(
                            swap=round(swap["percent"], 1),
                            used=swap["used"],
                            total=swap["total"],
                            threshold=self.config["swap_threshold"],
                            hostname=hostname,
                        )
                    )

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[ServerBox] Monitor loop error: {e}")
                await asyncio.sleep(5)

    async def on_unload(self):
        self._stop_monitor()