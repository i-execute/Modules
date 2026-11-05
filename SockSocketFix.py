# CopyLeft 2026 github.com/i-execute
# Licensed under AGPLv3.

__version__ = (1, 0, 0)
# meta developer: Execute_forge.t.me

"""SockSocketFix — лечит PySocks-патч сокетов внутри процесса юзербота.

Проблема: PySocks (его ставит telethon для MTProto-прокси) подменяет
socket.socket на socks.socksocket. Как только кто-то вызывает
socks.set_default_proxy(...) (например, скрипт через evaluate или модуль),
ВСЕ новые сокеты процесса молча заворачиваются в этот прокси:
- прямые подключения к внешним прокси виснут (петля: прокси просит
  подключиться к самому себе),
- aiohttp/requests/yt-dlp падают таймаутами,
- провайдеры LowAgentMod умирают с ProxyTimeoutError.

Модуль при старте сбрасывает global default (set_default_proxy()) и держит
его чистым: каждые N секунд проверка, при необходимости сброс. Телеграм-клиенту
не мешает — он задаёт прокси на каждый сокет явно (socksocket.set_proxy),
глобальный default ему не нужен.
"""

import asyncio
import logging
import socket

from telethon.tl.types import Message

from .. import loader, utils

logger = logging.getLogger(__name__)


def _socks_state():
    """(patched, proxy_tuple) — состояние PySocks на сейчас."""
    try:
        import socks
    except ImportError:
        return False, None
    proxy = getattr(socks.socksocket, "default_proxy", None)
    patched = socket.socket is not None and "socks.socksocket" in str(socket.socket)
    return patched, proxy


def _clean_socks():
    """Сбросить глобальный default PySocks (telethon не страдает)."""
    try:
        import socks
        if socks.socksocket.default_proxy and socks.socksocket.default_proxy[0] is not None:
            socks.set_default_proxy()
            logger.warning(
                "[SockFix] cleared PySocks default proxy %s", socks.socksocket.default_proxy
            )
            return True
    except Exception as e:
        logger.debug("[SockFix] clean error: %s", e)
    return False


@loader.tds
class SockSocketFix(loader.Module):
    """Сбрасывает PySocks-патч сокетов, чинит прямые прокси-подключения"""

    strings = {
        "name": "SockSocketFix",
        "status_ok": (
            "<b>SockSocketFix</b>\n"
            "<b>socket.socket:</b> <code>{sockcls}</code>\n"
            "<b>PySocks default:</b> <code>{proxy}</code>\n"
            "<b>Восстановлений:</b> <code>{fixes}</code>\n"
            "<b>Watcher:</b> {watcher}"
        ),
        "fixed": (
            "<b>SockSocketFix</b>\n"
            "🧹 Найден и сброшен левый PySocks default:\n"
            "<blockquote><code>{proxy}</code></blockquote>\n"
            "Сокеты снова ходят напрямую."
        ),
        "nothing": (
            "<b>SockSocketFix</b>\n"
            "✅ PySocks-патч чист — глобальный прокси не установлен"
        ),
        "test_progress": "<b>Тестирую подключение к прокси…</b>",
        "test_ok": (
            "<b>SockSocketFix</b>\n"
            "✅ SOCKS-рукопожатие с <code>{host}:{port}</code> прошло: <code>{reply}</code>\n"
            "Прямые прокси-подключения работают"
        ),
        "test_fail": (
            "<b>SockSocketFix</b>\n"
            "❌ <code>{host}:{port}</code> не ответил: <code>{error}</code>\n"
            "<i>Патч уже сброшен — причина, скорее всего, в самом прокси/сети</i>"
        ),
    }

    strings_ru = {
        "status_ok": (
            "<b>SockSocketFix</b>\n"
            "<b>socket.socket:</b> <code>{sockcls}</code>\n"
            "<b>PySocks default:</b> <code>{proxy}</code>\n"
            "<b>Восстановлений:</b> <code>{fixes}</code>\n"
            "<b>Watcher:</b> {watcher}"
        ),
        "fixed": (
            "<b>SockSocketFix</b>\n"
            "🧹 Найден и сброшен левый PySocks default:\n"
            "<blockquote><code>{proxy}</code></blockquote>\n"
            "Сокеты снова ходят напрямую."
        ),
        "nothing": "<b>SockSocketFix</b>\n✅ Патч чист — глобальный прокси не установлен",
        "test_progress": "<b>Тестирую подключение к прокси…</b>",
        "test_ok": (
            "<b>SockSocketFix</b>\n"
            "✅ SOCKS-рукопожатие с <code>{host}:{port}</code> прошло: <code>{reply}</code>"
        ),
        "test_fail": (
            "<b>SockSocketFix</b>\n"
            "❌ <code>{host}:{port}</code> не ответил: <code>{error}</code>"
        ),
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "WATCH",
                True,
                "Следить и сбрасывать PySocks default автоматически",
                validator=loader.validators.Boolean(),
            ),
            loader.ConfigValue(
                "WATCH_INTERVAL",
                30,
                "Интервал проверки в секундах",
                validator=loader.validators.Integer(minimum=5, maximum=3600),
            ),
            loader.ConfigValue(
                "TEST_PROXY",
                "socks5://31.77.12.187:39261",
                "Прокси для команды .socktest (host:port или socks5://host:port)",
            ),
        )
        self._fixes = 0
        self._watch_task = None

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        # стартовый сброс: лечим состояние сразу после загрузки модуля
        if _clean_socks():
            self._fixes += 1
            logger.info("[SockFix] startup clean done")
        if self.config["WATCH"]:
            self._start_watch()

    async def on_unload(self):
        if self._watch_task:
            self._watch_task.cancel()
            try:
                await self._watch_task
            except asyncio.CancelledError:
                pass
            self._watch_task = None

    def _start_watch(self):
        if self._watch_task and not self._watch_task.done():
            return
        self._watch_task = asyncio.create_task(self._watch_loop())

    async def _watch_loop(self):
        while True:
            try:
                if _clean_socks():
                    self._fixes += 1
                    logger.warning("[SockFix] auto-cleared PySocks default (total %d)", self._fixes)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.debug("[SockFix] watch error: %s", e)
            await asyncio.sleep(self.config["WATCH_INTERVAL"])

    @staticmethod
    def _fmt_proxy(proxy):
        if not proxy or proxy[0] is None:
            return "нет (чисто)"
        ptype = {1: "SOCKS4", 2: "SOCKS5", 3: "HTTP"}.get(proxy[0], str(proxy[0]))
        auth = " с логином" if (len(proxy) > 4 and proxy[4]) else ""
        return f"{ptype} {proxy[1]}:{proxy[2]}{auth}"

    @loader.command()
    async def sockfix(self, message: Message):
        """Сбросить PySocks-патч прямо сейчас и показать статус"""
        proxy_before = _socks_state()[1]
        if _clean_socks():
            self._fixes += 1
            await utils.answer(
                message,
                self.strings["fixed"].format(proxy=self._fmt_proxy(proxy_before)),
            )
        else:
            await utils.answer(message, self.strings["nothing"])

    @loader.command()
    async def socktest(self, message: Message):
        """Проверить прямое SOCKS-подключение (по умолчанию TEST_PROXY)"""
        args = utils.get_args(message)
        target = " ".join(args).strip() or self.config["TEST_PROXY"]
        raw = target.replace("socks5://", "").replace("socks4://", "").replace("http://", "")
        if ":" not in raw:
            await utils.answer(
                message,
                self.strings["test_fail"].format(host=target, port="?", error="нужен host:port"),
            )
            return
        host, port = raw.rsplit(":", 1)
        # сначала гарантированно чистим патч — тест именно прямого подключения
        if _clean_socks():
            self._fixes += 1
        await utils.answer(message, self.strings["test_progress"])
        try:
            fut = asyncio.open_connection(host, int(port))
            reader, writer = await asyncio.wait_for(fut, timeout=12)
            writer.write(b"\x05\x01\x00")
            await writer.drain()
            reply = await asyncio.wait_for(reader.readexactly(2), timeout=10)
            await utils.answer(
                message,
                self.strings["test_ok"].format(host=host, port=port, reply=reply.hex()),
            )
            writer.close()
        except Exception as e:
            await utils.answer(
                message,
                self.strings["test_fail"].format(host=host, port=port, error=f"{type(e).__name__}: {e}"[:120]),
            )

    @loader.command()
    async def sockstat(self, message: Message):
        """Статус патча сокетов"""
        patched, proxy = _socks_state()
        watcher = "включён" if (self._watch_task and not self._watch_task.done()) else "выключен"
        await utils.answer(
            message,
            self.strings["status_ok"].format(
                sockcls=socket.socket,
                proxy=self._fmt_proxy(proxy),
                fixes=self._fixes,
                watcher=watcher,
            ),
        )
