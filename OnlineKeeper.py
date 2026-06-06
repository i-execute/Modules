__version__ = (1, 0, 1)
# meta developer: I_execute.t.me

import asyncio
import logging

from telethon.tl.functions.account import UpdateStatusRequest
from telethon.tl.types import Message

from .. import loader, utils
from ..inline.types import InlineCall

logger = logging.getLogger(__name__)


@loader.tds
class OnlineKeeper(loader.Module):
    """Keep your account online"""

    strings = {
        "name": "OnlineKeeper",
        "status_online": (
            "<b>OnlineKeeper Status</b>\n"
            "<b>Mode:</b> Active\n"
            "<b>Account:</b> Online\n"
            "<b>Update interval:</b> {interval} seconds"
        ),
        "status_offline": (
            "<b>OnlineKeeper Status</b>\n\n"
            "<b>Mode:</b> Inactive\n"
            "<b>Account:</b> Offline"
        ),
        "btn_disable": "Disable",
        "btn_enable": "Enable",
        "enabling": "<b>Activating OnlineKeeper...</b>\n\nYour account will appear online",
        "disabling": "<b>Deactivating OnlineKeeper...</b>\n\nYour account will go offline",
    }

    strings_ru = {
        "status_online": (
            "<b>Статус OnlineKeeper</b>\n"
            "<b>Режим:</b> Активен\n"
            "<b>Аккаунт:</b> Онлайн\n"
            "<b>Интервал обновления:</b> {interval} секунд"
        ),
        "status_offline": (
            "<b>Статус OnlineKeeper</b>\n\n"
            "<b>Режим:</b> Неактивен\n"
            "<b>Аккаунт:</b> Оффлайн"
        ),
        "btn_disable": "Выключить",
        "btn_enable": "Включить",
        "enabling": "<b>Активация OnlineKeeper...</b>\n\nВаш аккаунт появится в сети",
        "disabling": "<b>Деактивация OnlineKeeper...</b>\n\nВаш аккаунт пропадет из сети",
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "AUTORUNNER",
                True,
                "Auto-start OnlineKeeper after restart",
                validator=loader.validators.Boolean(),
            ),
            loader.ConfigValue(
                "UPDATE_INTERVAL",
                15,
                "Status update interval in seconds",
                validator=loader.validators.Integer(minimum=5, maximum=600),
            ),
        )
        self._online_task = None

    async def client_ready(self, client, db):
        self._client = client
        self._db = db

        if not self.config["AUTORUNNER"]:
            self._db.set(self.name, "keep_online", False)
            return

        if self._db.get(self.name, "keep_online", False):
            await self._start_online_loop()

    async def on_unload(self):
        await self._stop_online_loop()

    async def _start_online_loop(self):
        if self._online_task and not self._online_task.done():
            return
        self._db.set(self.name, "keep_online", True)
        self._online_task = asyncio.create_task(self._online_update_loop())

    async def _stop_online_loop(self):
        self._db.set(self.name, "keep_online", False)
        if self._online_task:
            self._online_task.cancel()
            try:
                await self._online_task
            except asyncio.CancelledError:
                pass
            self._online_task = None

    async def _online_update_loop(self):
        while self._db.get(self.name, "keep_online", False):
            try:
                await self._client(UpdateStatusRequest(offline=False))
                await asyncio.sleep(self.config["UPDATE_INTERVAL"])
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[OnlineKeeper] Status update error: {e}")
                await asyncio.sleep(30)

    @loader.command()
    async def ok(self, message: Message):
        """Toggle OnlineKeeper status"""
        is_active = self._db.get(self.name, "keep_online", False)

        await self.inline.form(
            text=self.strings["status_online"].format(interval=self.config["UPDATE_INTERVAL"]) if is_active else self.strings["status_offline"],
            message=message,
            reply_markup=[
                [
                    {
                        "text": self.strings["btn_disable"] if is_active else self.strings["btn_enable"],
                        "callback": self._toggle_callback,
                        "style": "danger" if is_active else "success",
                    }
                ]
            ],
        )

    async def _toggle_callback(self, call: InlineCall):
        is_active = self._db.get(self.name, "keep_online", False)

        if is_active:
            await call.edit(self.strings["disabling"])
            await self._stop_online_loop()
            await asyncio.sleep(1)
            await call.edit(
                text=self.strings["status_offline"],
                reply_markup=[
                    [
                        {
                            "text": self.strings["btn_enable"],
                            "callback": self._toggle_callback,
                            "style": "success",
                        }
                    ]
                ],
            )
        else:
            await call.edit(self.strings["enabling"])
            await self._start_online_loop()
            await asyncio.sleep(1)
            await call.edit(
                text=self.strings["status_online"].format(interval=self.config["UPDATE_INTERVAL"]),
                reply_markup=[
                    [
                        {
                            "text": self.strings["btn_disable"],
                            "callback": self._toggle_callback,
                            "style": "danger",
                        }
                    ]
                ],
            )