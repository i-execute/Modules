__version__ = (3, 0, 0)
# meta developer: FireJester.t.me

import logging
import asyncio
import time
import re
import random
import os
import shutil
import tempfile
from datetime import datetime, timezone, timedelta
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import (
    MessageMediaWebPage,
    MessageMediaContact,
    MessageMediaGeo,
    MessageMediaGeoLive,
    MessageMediaPoll,
    DocumentAttributeAudio,
    DocumentAttributeVideo,
    PeerUser,
    InputUser,
    InputPeerUser,
)
from telethon.tl.functions.messages import (
    UpdatePinnedMessageRequest,
    ReadHistoryRequest,
)
from telethon.tl.functions.payments import GetSavedStarGiftsRequest
from telethon.errors import FloodWaitError
from .. import loader, utils

logger = logging.getLogger(__name__)

TYPING_SPEED_MIN = 0.08
TYPING_SPEED_MAX = 0.16
MEDIA_DELAY_MIN = 1.5
MEDIA_DELAY_MAX = 3.0
TYPING_DELAY_MIN = 0.5
TYPING_DELAY_MAX = 120.0
PIN_DELAY = 3
WORK_DURATION = 10 * 60
BREAK_DURATION = 2 * 60
STATUS_UPDATE_INTERVAL = 60
TRANSIT_WAIT = 3
TRANSIT_SEARCH_LIMIT = 10
MAX_MESSAGE_LENGTH = 4096
PARSE_CHUNK_SIZE = 100
PARSE_CHUNK_DELAY = 1
FLOOD_WAIT_EXTRA = 5
PROGRESS_SAVE_EVERY = 10
FETCH_BATCH_SIZE = 100
CHUNK_FILE_SIZE = 1000
CHUNK_OVERLAP = 100
WORK_CHECK_INTERVAL = 60
STRING_SESSION_PATTERN = re.compile(r'1[A-Za-z0-9_\-]{200,}={0,2}')

SKIP_MEDIA_TYPES = (
    MessageMediaContact,
    MessageMediaGeo,
    MessageMediaGeoLive,
    MessageMediaPoll,
)


def _ts(offset):
    if offset >= 0:
        return f"+{offset}"
    return str(offset)


def _typing_delay(text):
    if not text:
        return random.uniform(MEDIA_DELAY_MIN, MEDIA_DELAY_MAX)
    speed = random.uniform(TYPING_SPEED_MIN, TYPING_SPEED_MAX)
    delay = len(text) * speed
    if delay < TYPING_DELAY_MIN:
        delay = random.uniform(TYPING_DELAY_MIN, 1.0)
    if delay > TYPING_DELAY_MAX:
        delay = random.uniform(90, TYPING_DELAY_MAX)
    return delay


def _parse_time(s):
    parts = s.split(":")
    if len(parts) != 2:
        return None
    try:
        h, m = int(parts[0]), int(parts[1])
        if 0 <= h <= 23 and 0 <= m <= 59:
            return (h, m)
    except ValueError:
        pass
    return None


@loader.tds
class ChatRecovery(loader.Module):
    """Recover deleted chat history between two accounts"""

    strings = {
        "name": "ChatRecovery",
    }

    strings_en = {
        "line": "--------------------",
        "help": (
            "<b>ChatRecovery</b>\n\n"
            "<code>{prefix}recovery start</code>\n"
            "<code>{prefix}recovery from id</code>\n"
            "<code>{prefix}recovery session donor string</code>\n"
            "<code>{prefix}recovery session recipient string</code>\n"
            "<code>{prefix}recovery parsing username</code>\n"
            "<code>{prefix}recovery timezone -12..12</code>\n"
            "<code>{prefix}recovery work 8:00 22:00</code>\n"
            "<code>{prefix}recovery now</code>\n"
            "<code>{prefix}recovery resume</code>\n"
            "<code>{prefix}recovery status</code>\n"
            "<code>{prefix}recovery terminate</code>\n"
            "<code>{prefix}recovery forcerm</code>\n"
        ),
        "status_template": (
            "<b>Chat Recovery</b>\n"
            "{line}\n"
            "Deleted ID: <code>{deleted_id}</code>\n"
            "Donor ID: <code>{donor_id}</code>\n"
            "Recipient ID: <code>{recipient_id}</code>\n"
            "Work hours: <code>{work_hours}</code>\n"
            "{line}\n"
            "{extra}"
        ),
        "debug_template": (
            "<b>Chat Recovery - Processing</b>\n"
            "{line}\n"
            "<b>Stage 1:</b> {stage1_status}\n"
            "Status: {stage1_detail}\n"
            "Analyzed: {analyzed} / Total: {total}\n"
            "{line}\n"
            "<b>Stage 2:</b> {stage2_status}\n"
            "Status: {stage2_detail}\n"
            "Processing: {processed} / {total_process}\n"
            "Skipped: {skipped}\n"
            "Albums: {albums}\n"
            "{line}\n"
            "FINISH: {finish_time}\n"
            "Work: {work_hours}\n"
            "Timezone: UTC{tz}\n"
            "{line}"
        ),
        "err_no_start": "<b>Use {prefix}recovery start first</b>",
        "err_running": "<b>Process running. Use {prefix}recovery terminate</b>",
        "err_not_ready": "<b>Fill all fields: deleted ID, donor session, recipient session, work hours</b>",
        "err_no_session": "<b>Provide StringSession in args or reply</b>",
        "err_invalid_tz": "<b>Timezone must be -12 to +12</b>",
        "err_no_progress": "<b>No saved progress to resume</b>",
        "err_invalid_work": "<b>Format: {prefix}recovery work 8:00 22:00</b>",
        "err_no_work": "<b>Set work hours first: {prefix}recovery work 8:00 22:00</b>",
        "terminated": "<b>Process terminated.</b>",
        "forcerm_done": "<b>All data wiped.</b>",
        "done": (
            "<b>Recovery complete!</b>\n"
            "Processed: {processed}\n"
            "Skipped: {skipped}\n"
            "Albums: {albums}\n"
            "Time: {elapsed}"
        ),
        "break_msg": "Break {minutes} min...",
        "resume_msg": "Resuming...",
        "flood_msg": "FloodWait {seconds}s, waiting...",
        "outside_work": "Outside work hours. Sleeping until {wake_time}...",
        "ready_to_go": (
            "Recovering chat between "
            "<a href='tg://user?id={donor_id}'>donor</a> and "
            "<a href='tg://user?id={recipient_id}'>recipient</a>\n"
            "Use <code>{prefix}recovery now</code>"
        ),
        "parsing_start": "<b>Parsing entities from gifts of @{username}...</b>",
        "parsing_done": (
            "<b>Parsing done!</b>\n"
            "Resolved on donor: {donor_count}\n"
            "Resolved on recipient: {recipient_count}\n"
            "Total unique users: {total}"
        ),
        "parsing_no_user": "<b>Provide username</b>",
        "parsing_no_clients": "<b>Connect donor and recipient first</b>",
        "resolve_start": "<b>Resolving entities...</b>",
        "resolve_fail": (
            "<b>Entity resolve failed:</b> <code>{err}</code>\n\n"
            "Try <code>{prefix}recovery parsing username</code> to cache entities"
        ),
        "resumed": "<b>Resumed from message {idx}/{total}</b>",
        "progress_saved": "<b>Progress saved.</b>",
        "work_set": "<b>Work hours set: {start} - {end}</b>",
        "status_active": (
            "<b>Recovery Status</b>\n"
            "{line}\n"
            "State: <b>ACTIVE</b>\n"
            "Processed: {processed} / {total}\n"
            "Skipped: {skipped}\n"
            "Albums: {albums}\n"
            "FINISH: {finish_time}\n"
            "Work: {work_hours}\n"
            "{line}"
        ),
        "status_saved": (
            "<b>Recovery Status</b>\n"
            "{line}\n"
            "State: <b>PAUSED (progress saved)</b>\n"
            "Processed: {processed} / {total}\n"
            "Skipped: {skipped}\n"
            "Albums: {albums}\n"
            "Work: {work_hours}\n"
            "{line}\n"
            "Use <code>{prefix}recovery resume</code> to continue"
        ),
        "status_none": "<b>No active or saved recovery process</b>",
        "provide_id": "<b>Provide deleted account ID</b>",
        "id_must_be_number": "<b>ID must be a number</b>",
        "connection_error": "<b>Connection error:</b> <code>{err}</code>",
        "tz_set": "<b>Timezone: UTC{tz}</b>",
    }

    strings_ru = {
        "line": "--------------------",
        "help": (
            "<b>ChatRecovery</b>\n\n"
            "<code>{prefix}recovery start</code>\n"
            "<code>{prefix}recovery from id</code>\n"
            "<code>{prefix}recovery session donor string</code>\n"
            "<code>{prefix}recovery session recipient string</code>\n"
            "<code>{prefix}recovery parsing username</code>\n"
            "<code>{prefix}recovery timezone -12..12</code>\n"
            "<code>{prefix}recovery work 8:00 22:00</code>\n"
            "<code>{prefix}recovery now</code>\n"
            "<code>{prefix}recovery resume</code>\n"
            "<code>{prefix}recovery status</code>\n"
            "<code>{prefix}recovery terminate</code>\n"
            "<code>{prefix}recovery forcerm</code>\n"
        ),
        "status_template": (
            "<b>Chat Recovery</b>\n"
            "{line}\n"
            "ID удалённого: <code>{deleted_id}</code>\n"
            "ID донора: <code>{donor_id}</code>\n"
            "ID получателя: <code>{recipient_id}</code>\n"
            "Рабочие часы: <code>{work_hours}</code>\n"
            "{line}\n"
            "{extra}"
        ),
        "debug_template": (
            "<b>Chat Recovery - Обработка</b>\n"
            "{line}\n"
            "<b>Этап 1:</b> {stage1_status}\n"
            "Статус: {stage1_detail}\n"
            "Проанализировано: {analyzed} / Всего: {total}\n"
            "{line}\n"
            "<b>Этап 2:</b> {stage2_status}\n"
            "Статус: {stage2_detail}\n"
            "Обработано: {processed} / {total_process}\n"
            "Пропущено: {skipped}\n"
            "Альбомы: {albums}\n"
            "{line}\n"
            "ЗАВЕРШЕНИЕ: {finish_time}\n"
            "Работа: {work_hours}\n"
            "Часовой пояс: UTC{tz}\n"
            "{line}"
        ),
        "err_no_start": "<b>Сначала используйте {prefix}recovery start</b>",
        "err_running": "<b>Процесс запущен. Используйте {prefix}recovery terminate</b>",
        "err_not_ready": "<b>Заполните все поля: ID удалённого, сессия донора, сессия получателя, рабочие часы</b>",
        "err_no_session": "<b>Укажите StringSession в аргументах или реплае</b>",
        "err_invalid_tz": "<b>Часовой пояс должен быть от -12 до +12</b>",
        "err_no_progress": "<b>Нет сохранённого прогресса для продолжения</b>",
        "err_invalid_work": "<b>Формат: {prefix}recovery work 8:00 22:00</b>",
        "err_no_work": "<b>Сначала установите рабочие часы: {prefix}recovery work 8:00 22:00</b>",
        "terminated": "<b>Процесс остановлен.</b>",
        "forcerm_done": "<b>Все данные удалены.</b>",
        "done": (
            "<b>Восстановление завершено!</b>\n"
            "Обработано: {processed}\n"
            "Пропущено: {skipped}\n"
            "Альбомы: {albums}\n"
            "Время: {elapsed}"
        ),
        "break_msg": "Перерыв {minutes} мин...",
        "resume_msg": "Продолжение...",
        "flood_msg": "FloodWait {seconds}с, ожидание...",
        "outside_work": "Вне рабочих часов. Сон до {wake_time}...",
        "ready_to_go": (
            "Восстановление чата между "
            "<a href='tg://user?id={donor_id}'>донором</a> и "
            "<a href='tg://user?id={recipient_id}'>получателем</a>\n"
            "Используйте <code>{prefix}recovery now</code>"
        ),
        "parsing_start": "<b>Парсинг сущностей из подарков @{username}...</b>",
        "parsing_done": (
            "<b>Парсинг завершён!</b>\n"
            "Найдено у донора: {donor_count}\n"
            "Найдено у получателя: {recipient_count}\n"
            "Всего уникальных: {total}"
        ),
        "parsing_no_user": "<b>Укажите юзернейм</b>",
        "parsing_no_clients": "<b>Сначала подключите донора и получателя</b>",
        "resolve_start": "<b>Разрешение сущностей...</b>",
        "resolve_fail": (
            "<b>Ошибка разрешения сущностей:</b> <code>{err}</code>\n\n"
            "Попробуйте <code>{prefix}recovery parsing username</code> для кэширования"
        ),
        "resumed": "<b>Продолжение с сообщения {idx}/{total}</b>",
        "progress_saved": "<b>Прогресс сохранён.</b>",
        "work_set": "<b>Рабочие часы установлены: {start} - {end}</b>",
        "status_active": (
            "<b>Статус восстановления</b>\n"
            "{line}\n"
            "Состояние: <b>АКТИВНО</b>\n"
            "Обработано: {processed} / {total}\n"
            "Пропущено: {skipped}\n"
            "Альбомы: {albums}\n"
            "ЗАВЕРШЕНИЕ: {finish_time}\n"
            "Работа: {work_hours}\n"
            "{line}"
        ),
        "status_saved": (
            "<b>Статус восстановления</b>\n"
            "{line}\n"
            "Состояние: <b>ПАУЗА (прогресс сохранён)</b>\n"
            "Обработано: {processed} / {total}\n"
            "Пропущено: {skipped}\n"
            "Альбомы: {albums}\n"
            "Работа: {work_hours}\n"
            "{line}\n"
            "Используйте <code>{prefix}recovery resume</code> для продолжения"
        ),
        "status_none": "<b>Нет активного или сохранённого процесса восстановления</b>",
        "provide_id": "<b>Укажите ID удалённого аккаунта</b>",
        "id_must_be_number": "<b>ID должен быть числом</b>",
        "connection_error": "<b>Ошибка подключения:</b> <code>{err}</code>",
        "tz_set": "<b>Часовой пояс: UTC{tz}</b>",
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            "TIMEZONE_OFFSET", 3, "UTC offset",
            "API_ID", 2040, "Telegram API ID",
            "API_HASH", "b18441a1ff607e10a989891a5462e627", "Telegram API Hash",
        )
        self._active = False
        self._recovering = False
        self._status_msg = None
        self._deleted_id = None
        self._donor_session = None
        self._recipient_session = None
        self._donor_id = None
        self._recipient_id = None
        self._donor_client = None
        self._recipient_client = None
        self._chat_id = None
        self._total = 0
        self._analyzed = 0
        self._processed = 0
        self._skipped = 0
        self._albums = 0
        self._stage = 0
        self._finish_estimate = "calculating..."
        self._last_status_update = 0
        self._recovery_task = None
        self._start_time = 0
        self._donor_peer_deleted = None
        self._donor_peer_recipient = None
        self._recipient_peer_donor = None
        self._last_processed_id = 0
        self._id_map = {}
        self._work_start = None
        self._work_end = None
        self._temp_dir = None

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        self._temp_dir = os.path.join(tempfile.gettempdir(), "chat_recovery")
        os.makedirs(self._temp_dir, exist_ok=True)

    async def on_unload(self):
        await self._full_cleanup()
        self._wipe_temp()

    def _wipe_temp(self):
        if self._temp_dir and os.path.exists(self._temp_dir):
            try:
                shutil.rmtree(self._temp_dir)
            except Exception:
                pass

    def _wipe_all(self):
        self._wipe_temp()
        self._db.set("ChatRecovery", "progress", None)
        self._db.set("ChatRecovery", "id_map", None)
        self._db.set("ChatRecovery", "work_hours", None)

    def _get_chunk_path(self, idx):
        return os.path.join(self._temp_dir, f"chunk_{idx:04d}.dat")

    def _write_chunk(self, idx, entries):
        path = self._get_chunk_path(idx)
        with open(path, "w") as f:
            for e in entries:
                f.write(f"{e['msg_id']}|{e['sender_id']}|{e['grouped_id']}\n")

    def _read_chunk(self, idx):
        path = self._get_chunk_path(idx)
        if not os.path.exists(path):
            return []
        entries = []
        with open(path, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                parts = line.split("|")
                entries.append({
                    "msg_id": int(parts[0]),
                    "sender_id": int(parts[1]),
                    "grouped_id": int(parts[2]),
                })
        return entries

    def _count_chunks(self):
        if not self._temp_dir or not os.path.exists(self._temp_dir):
            return 0
        count = 0
        while os.path.exists(self._get_chunk_path(count)):
            count += 1
        return count

    def _save_progress(self):
        self._db.set("ChatRecovery", "progress", {
            "deleted_id": self._deleted_id,
            "donor_session": self._donor_session,
            "recipient_session": self._recipient_session,
            "donor_id": self._donor_id,
            "recipient_id": self._recipient_id,
            "last_processed_id": self._last_processed_id,
            "processed": self._processed,
            "skipped": self._skipped,
            "albums": self._albums,
            "total": self._total,
            "stage": self._stage,
            "analyzed": self._analyzed,
        })
        self._save_id_map()

    def _save_id_map(self):
        self._db.set("ChatRecovery", "id_map", {
            str(k): v for k, v in self._id_map.items()
        })

    def _load_id_map(self):
        raw = self._db.get("ChatRecovery", "id_map", None)
        if raw:
            self._id_map = {int(k): v for k, v in raw.items()}
        else:
            self._id_map = {}

    def _load_progress(self):
        data = self._db.get("ChatRecovery", "progress", None)
        if not data:
            return False
        self._deleted_id = data.get("deleted_id")
        self._donor_session = data.get("donor_session")
        self._recipient_session = data.get("recipient_session")
        self._donor_id = data.get("donor_id")
        self._recipient_id = data.get("recipient_id")
        self._last_processed_id = data.get("last_processed_id", 0)
        self._processed = data.get("processed", 0)
        self._skipped = data.get("skipped", 0)
        self._albums = data.get("albums", 0)
        self._total = data.get("total", 0)
        self._stage = data.get("stage", 0)
        self._analyzed = data.get("analyzed", 0)
        self._load_id_map()
        self._load_work_hours()
        return True

    def _clear_progress(self):
        self._db.set("ChatRecovery", "progress", None)
        self._db.set("ChatRecovery", "id_map", None)

    def _save_work_hours(self):
        if self._work_start and self._work_end:
            self._db.set("ChatRecovery", "work_hours", {
                "start": list(self._work_start),
                "end": list(self._work_end),
            })

    def _load_work_hours(self):
        data = self._db.get("ChatRecovery", "work_hours", None)
        if data:
            self._work_start = tuple(data["start"])
            self._work_end = tuple(data["end"])

    def _work_hours_str(self):
        if self._work_start and self._work_end:
            return f"{self._work_start[0]:02d}:{self._work_start[1]:02d} - {self._work_end[0]:02d}:{self._work_end[1]:02d}"
        return "---"

    def _now_tz(self):
        tz = timezone(timedelta(hours=self._get_tz()))
        return datetime.now(tz)

    def _is_work_time(self):
        if not self._work_start or not self._work_end:
            return False
        now = self._now_tz()
        current = (now.hour, now.minute)
        start = self._work_start
        end = self._work_end
        if start <= end:
            return start <= current < end
        else:
            return current >= start or current < end

    def _seconds_until_work(self):
        if not self._work_start:
            return 0
        now = self._now_tz()
        target = now.replace(
            hour=self._work_start[0],
            minute=self._work_start[1],
            second=0, microsecond=0,
        )
        if target <= now:
            target += timedelta(days=1)
        return (target - now).total_seconds()

    async def _wait_for_work_time(self):
        while not self._is_work_time():
            if not self._recovering:
                return
            secs = self._seconds_until_work()
            wake = self._now_tz() + timedelta(seconds=secs)
            wake_str = wake.strftime("%H:%M:%S")
            await self._force_update(
                self._build_debug() + "\n\n"
                + self.strings["outside_work"].format(wake_time=wake_str)
            )
            self._save_progress()
            sleep_time = min(secs, WORK_CHECK_INTERVAL)
            await asyncio.sleep(sleep_time)

    async def _full_cleanup(self):
        self._active = False
        self._recovering = False
        if self._recovery_task and not self._recovery_task.done():
            self._recovery_task.cancel()
            try:
                await self._recovery_task
            except (asyncio.CancelledError, Exception):
                pass
        for c in [self._donor_client, self._recipient_client]:
            if c:
                try:
                    await c.disconnect()
                except Exception:
                    pass
        self._donor_client = None
        self._recipient_client = None
        self._status_msg = None
        self._deleted_id = None
        self._donor_session = None
        self._recipient_session = None
        self._donor_id = None
        self._recipient_id = None
        self._total = 0
        self._analyzed = 0
        self._processed = 0
        self._skipped = 0
        self._albums = 0
        self._stage = 0
        self._finish_estimate = "calculating..."
        self._recovery_task = None
        self._donor_peer_deleted = None
        self._donor_peer_recipient = None
        self._recipient_peer_donor = None
        self._last_processed_id = 0
        self._id_map = {}

    def _get_tz(self):
        return self.config.get("TIMEZONE_OFFSET", 3)

    def _build_status(self, extra=""):
        return self.strings["status_template"].format(
            line=self.strings["line"],
            deleted_id=self._deleted_id or "---",
            donor_id=self._donor_id or "---",
            recipient_id=self._recipient_id or "---",
            work_hours=self._work_hours_str(),
            extra=extra,
        )

    def _build_debug(self):
        if self._stage == 0:
            s1s, s1d, s2s, s2d = "wait", "wait", "wait", "wait"
        elif self._stage == 1:
            s1s, s1d, s2s, s2d = "now", "processing", "wait", "wait"
        elif self._stage == 2:
            s1s, s1d, s2s, s2d = "done", "done", "recovering", "processing"
        else:
            s1s, s1d, s2s, s2d = "done", "done", "done", "done"
        return self.strings["debug_template"].format(
            line=self.strings["line"],
            stage1_status=s1s, stage1_detail=s1d,
            analyzed=self._analyzed, total=self._total,
            stage2_status=s2s, stage2_detail=s2d,
            processed=self._processed, total_process=self._total,
            skipped=self._skipped, albums=self._albums,
            finish_time=self._finish_estimate,
            work_hours=self._work_hours_str(),
            tz=_ts(self._get_tz()),
        )

    async def _update_status_msg(self, text=None):
        if not self._status_msg:
            return
        now = time.time()
        if text is None:
            if now - self._last_status_update < STATUS_UPDATE_INTERVAL:
                return
        self._last_status_update = now
        try:
            content = text if text else self._build_debug()
            self._status_msg = await utils.answer(self._status_msg, content)
        except Exception:
            pass

    async def _force_update(self, text=None):
        self._last_status_update = 0
        await self._update_status_msg(text)

    def _all_ready(self):
        return all([
            self._deleted_id,
            self._donor_session,
            self._recipient_session,
            self._work_start,
            self._work_end,
        ])

    def _extra_text(self):
        if self._all_ready():
            prefix = self.get_prefix()
            return self.strings["ready_to_go"].format(
                donor_id=self._donor_id, recipient_id=self._recipient_id,
                prefix=prefix,
            )
        return ""

    async def _connect_client(self, session_str):
        client = TelegramClient(
            StringSession(session_str),
            int(self.config["API_ID"]),
            self.config["API_HASH"],
        )
        await client.connect()
        me = await client.get_me()
        return client, me.id

    def _find_session(self, text):
        if not text:
            return None
        m = STRING_SESSION_PATTERN.search(text)
        return m.group(0) if m else None

    async def _safe_call(self, coro):
        while True:
            try:
                return await coro
            except FloodWaitError as e:
                wait = e.seconds + FLOOD_WAIT_EXTRA
                await self._force_update(
                    self._build_debug() + "\n\n"
                    + self.strings["flood_msg"].format(seconds=wait)
                )
                self._save_progress()
                await asyncio.sleep(wait)

    async def _mark_read(self, client, peer, msg_id):
        try:
            await client(ReadHistoryRequest(peer=peer, max_id=msg_id))
        except Exception:
            pass

    async def _type_and_send(self, client, peer, text, **kwargs):
        delay = _typing_delay(text)
        try:
            async with client.action(peer, "typing"):
                await asyncio.sleep(delay)
        except Exception:
            await asyncio.sleep(delay)
        return await self._safe_call(
            client.send_message(peer, text, **kwargs)
        )

    async def _type_and_send_file(self, client, peer, file, text=None, **kwargs):
        if text:
            delay = _typing_delay(text)
        else:
            delay = random.uniform(MEDIA_DELAY_MIN, MEDIA_DELAY_MAX)
        try:
            async with client.action(peer, "typing"):
                await asyncio.sleep(delay)
        except Exception:
            await asyncio.sleep(delay)
        return await self._safe_call(
            client.send_file(peer, file, **kwargs)
        )

    async def _parse_gifts_for_client(self, client, username):
        found = set()
        try:
            target = await client.get_entity(username)
            result = await client(GetSavedStarGiftsRequest(
                peer=target, offset="", limit=100,
            ))
            if hasattr(result, 'users') and result.users:
                for user in result.users:
                    if hasattr(user, 'id') and hasattr(user, 'access_hash'):
                        found.add(user.id)
                        try:
                            inp = InputUser(user.id, user.access_hash)
                            await client.get_entity(inp)
                        except Exception:
                            pass
        except Exception as e:
            logger.warning(f"[RECOVERY] Gift parse: {e}")
        return found

    async def _resolve_via_dialog(self, client, target_id):
        try:
            return await client.get_input_entity(target_id)
        except Exception:
            pass
        try:
            async for dialog in client.iter_dialogs():
                if dialog.entity and hasattr(dialog.entity, 'id'):
                    if dialog.entity.id == target_id:
                        return await client.get_input_entity(dialog.entity)
        except Exception:
            pass
        return None

    async def _resolve_via_messages(self, client, chat_entity, target_id):
        try:
            async for msg in client.iter_messages(chat_entity, limit=50):
                if msg.sender_id == target_id:
                    try:
                        sender = await msg.get_sender()
                        if sender:
                            return await client.get_input_entity(sender)
                    except Exception:
                        pass
        except Exception:
            pass
        return None

    async def _resolve_all_entities(self):
        donor = self._donor_client
        recipient = self._recipient_client

        self._donor_peer_deleted = await self._resolve_via_dialog(donor, self._deleted_id)
        if not self._donor_peer_deleted:
            try:
                peer = InputPeerUser(self._deleted_id, 0)
                await donor.get_messages(peer, limit=1)
                self._donor_peer_deleted = peer
            except Exception:
                pass
        if not self._donor_peer_deleted:
            async for dialog in donor.iter_dialogs():
                if dialog.entity and hasattr(dialog.entity, 'id'):
                    if dialog.entity.id == self._deleted_id:
                        self._donor_peer_deleted = await donor.get_input_entity(dialog.entity)
                        break

        self._donor_peer_recipient = await self._resolve_via_dialog(donor, self._recipient_id)
        if not self._donor_peer_recipient:
            self._donor_peer_recipient = await self._resolve_via_messages(
                donor, self._recipient_id, self._recipient_id
            )

        self._recipient_peer_donor = await self._resolve_via_dialog(recipient, self._donor_id)
        if not self._recipient_peer_donor:
            self._recipient_peer_donor = await self._resolve_via_messages(
                recipient, self._donor_id, self._donor_id
            )

        errors = []
        if not self._donor_peer_deleted:
            errors.append(f"donor->deleted({self._deleted_id})")
        if not self._donor_peer_recipient:
            errors.append(f"donor->recipient({self._recipient_id})")
        if not self._recipient_peer_donor:
            errors.append(f"recipient->donor({self._donor_id})")
        if errors:
            raise Exception(f"Cannot resolve: {', '.join(errors)}")

    @loader.command(
        ru_doc="Управление восстановлением чата",
        en_doc="Chat recovery manager",
    )
    async def recovery(self, message):
        """Chat recovery manager"""
        args = utils.get_args_raw(message).split()
        prefix = self.get_prefix()
        if not args:
            await utils.answer(
                message,
                self.strings["help"].format(prefix=prefix),
            )
            return
        cmd = args[0].lower()
        if cmd == "start":
            await self._cmd_start(message)
        elif cmd == "from":
            await self._cmd_from(message, args)
        elif cmd == "session":
            await self._cmd_session(message, args)
        elif cmd == "parsing":
            await self._cmd_parsing(message, args)
        elif cmd == "timezone":
            await self._cmd_timezone(message, args)
        elif cmd == "work":
            await self._cmd_work(message, args)
        elif cmd == "now":
            await self._cmd_now(message)
        elif cmd == "resume":
            await self._cmd_resume(message)
        elif cmd == "status":
            await self._cmd_status(message)
        elif cmd == "terminate":
            await self._cmd_terminate(message)
        elif cmd == "forcerm":
            await self._cmd_forcerm(message)
        else:
            await utils.answer(
                message,
                self.strings["help"].format(prefix=prefix),
            )

    async def _cmd_start(self, message):
        if self._recovering:
            prefix = self.get_prefix()
            return await utils.answer(
                message,
                self.strings["err_running"].format(prefix=prefix),
            )
        await self._full_cleanup()
        self._active = True
        self._chat_id = message.chat_id
        self._load_work_hours()
        self._status_msg = await utils.answer(message, self._build_status())

    async def _cmd_from(self, message, args):
        prefix = self.get_prefix()
        if not self._active:
            return await utils.answer(
                message,
                self.strings["err_no_start"].format(prefix=prefix),
            )
        if self._recovering:
            return await utils.answer(
                message,
                self.strings["err_running"].format(prefix=prefix),
            )
        if len(args) < 2:
            return await utils.answer(message, self.strings["provide_id"])
        try:
            self._deleted_id = int(args[1])
        except ValueError:
            return await utils.answer(message, self.strings["id_must_be_number"])
        await message.delete()
        await self._force_update(self._build_status(self._extra_text()))

    async def _cmd_session(self, message, args):
        prefix = self.get_prefix()
        if not self._active:
            return await utils.answer(
                message,
                self.strings["err_no_start"].format(prefix=prefix),
            )
        if self._recovering:
            return await utils.answer(
                message,
                self.strings["err_running"].format(prefix=prefix),
            )
        if len(args) < 2:
            return await utils.answer(
                message,
                self.strings["help"].format(prefix=prefix),
            )
        role = args[1].lower()
        if role not in ("donor", "recipient"):
            return await utils.answer(
                message,
                self.strings["help"].format(prefix=prefix),
            )
        session_str = None
        if len(args) > 2:
            session_str = self._find_session(" ".join(args[2:]))
        if not session_str:
            reply = await message.get_reply_message()
            if reply:
                session_str = self._find_session(reply.text or "")
        if not session_str:
            return await utils.answer(message, self.strings["err_no_session"])
        try:
            client, uid = await self._connect_client(session_str)
        except Exception as e:
            return await utils.answer(
                message,
                self.strings["connection_error"].format(err=str(e)),
            )
        if role == "donor":
            if self._donor_client:
                try:
                    await self._donor_client.disconnect()
                except Exception:
                    pass
            self._donor_client = client
            self._donor_session = session_str
            self._donor_id = uid
        else:
            if self._recipient_client:
                try:
                    await self._recipient_client.disconnect()
                except Exception:
                    pass
            self._recipient_client = client
            self._recipient_session = session_str
            self._recipient_id = uid
        await message.delete()
        await self._force_update(self._build_status(self._extra_text()))

    async def _cmd_parsing(self, message, args):
        prefix = self.get_prefix()
        if not self._active:
            return await utils.answer(
                message,
                self.strings["err_no_start"].format(prefix=prefix),
            )
        if not self._donor_client and not self._recipient_client:
            return await utils.answer(message, self.strings["parsing_no_clients"])
        if len(args) < 2:
            return await utils.answer(message, self.strings["parsing_no_user"])
        username = args[1].lstrip('@')
        status = await utils.answer(
            message, self.strings["parsing_start"].format(username=username),
        )
        donor_count = 0
        recipient_count = 0
        total_found = set()
        if self._donor_client:
            found = await self._parse_gifts_for_client(self._donor_client, username)
            donor_count = len(found)
            total_found.update(found)
        await asyncio.sleep(0.5)
        if self._recipient_client:
            found = await self._parse_gifts_for_client(self._recipient_client, username)
            recipient_count = len(found)
            total_found.update(found)
        if self._donor_client and self._recipient_id:
            await self._resolve_via_dialog(self._donor_client, self._recipient_id)
        if self._recipient_client and self._donor_id:
            await self._resolve_via_dialog(self._recipient_client, self._donor_id)
        if self._donor_client and self._deleted_id:
            await self._resolve_via_dialog(self._donor_client, self._deleted_id)
        await utils.answer(status, self.strings["parsing_done"].format(
            donor_count=donor_count, recipient_count=recipient_count, total=len(total_found),
        ))

    async def _cmd_timezone(self, message, args):
        if len(args) < 2:
            return await utils.answer(message, self.strings["err_invalid_tz"])
        try:
            val = int(args[1].replace("+", ""))
            if not -12 <= val <= 12:
                raise ValueError
            self.config["TIMEZONE_OFFSET"] = val
            await utils.answer(
                message,
                self.strings["tz_set"].format(tz=_ts(val)),
            )
        except ValueError:
            await utils.answer(message, self.strings["err_invalid_tz"])

    async def _cmd_work(self, message, args):
        prefix = self.get_prefix()
        if len(args) < 3:
            return await utils.answer(
                message,
                self.strings["err_invalid_work"].format(prefix=prefix),
            )
        start = _parse_time(args[1])
        end = _parse_time(args[2])
        if not start or not end:
            return await utils.answer(
                message,
                self.strings["err_invalid_work"].format(prefix=prefix),
            )
        self._work_start = start
        self._work_end = end
        self._save_work_hours()
        start_str = f"{start[0]:02d}:{start[1]:02d}"
        end_str = f"{end[0]:02d}:{end[1]:02d}"
        await utils.answer(message, self.strings["work_set"].format(start=start_str, end=end_str))
        if self._active and self._status_msg:
            await self._force_update(self._build_status(self._extra_text()))

    async def _cmd_terminate(self, message):
        if self._recovering:
            self._save_progress()
        await self._full_cleanup()
        await utils.answer(message, self.strings["terminated"])

    async def _cmd_forcerm(self, message):
        await self._full_cleanup()
        self._wipe_all()
        await utils.answer(message, self.strings["forcerm_done"])

    async def _cmd_status(self, message):
        prefix = self.get_prefix()
        if self._recovering:
            await utils.answer(message, self.strings["status_active"].format(
                line=self.strings["line"],
                processed=self._processed,
                total=self._total,
                skipped=self._skipped,
                albums=self._albums,
                finish_time=self._finish_estimate,
                work_hours=self._work_hours_str(),
            ))
            return
        data = self._db.get("ChatRecovery", "progress", None)
        if data:
            self._load_work_hours()
            await utils.answer(message, self.strings["status_saved"].format(
                line=self.strings["line"],
                processed=data.get("processed", 0),
                total=data.get("total", 0),
                skipped=data.get("skipped", 0),
                albums=data.get("albums", 0),
                work_hours=self._work_hours_str(),
                prefix=prefix,
            ))
            return
        await utils.answer(message, self.strings["status_none"])

    async def _cmd_now(self, message):
        prefix = self.get_prefix()
        if not self._active:
            return await utils.answer(
                message,
                self.strings["err_no_start"].format(prefix=prefix),
            )
        if self._recovering:
            return await utils.answer(
                message,
                self.strings["err_running"].format(prefix=prefix),
            )
        if not self._all_ready():
            return await utils.answer(message, self.strings["err_not_ready"])
        await self._ensure_clients()
        await utils.answer(message, self.strings["resolve_start"])
        try:
            await self._resolve_all_entities()
        except Exception as e:
            return await utils.answer(
                message,
                self.strings["resolve_fail"].format(err=str(e), prefix=prefix),
            )
        self._last_processed_id = 0
        self._id_map = {}
        self._processed = 0
        self._skipped = 0
        self._albums = 0
        self._total = 0
        self._analyzed = 0
        self._clear_progress()
        self._recovering = True
        self._start_time = time.time()
        self._status_msg = await utils.answer(message, self._build_debug())
        self._recovery_task = asyncio.create_task(self._recovery_loop())

    async def _cmd_resume(self, message):
        prefix = self.get_prefix()
        if self._recovering:
            return await utils.answer(
                message,
                self.strings["err_running"].format(prefix=prefix),
            )
        if not self._load_progress():
            return await utils.answer(message, self.strings["err_no_progress"])
        self._active = True
        self._chat_id = message.chat_id
        await self._ensure_clients()
        try:
            await self._resolve_all_entities()
        except Exception as e:
            return await utils.answer(
                message,
                self.strings["resolve_fail"].format(err=str(e), prefix=prefix),
            )
        self._recovering = True
        self._start_time = time.time()
        self._status_msg = await utils.answer(
            message,
            self.strings["resumed"].format(idx=self._processed, total=self._total),
        )
        self._recovery_task = asyncio.create_task(self._recovery_loop())

    async def _ensure_clients(self):
        if not self._donor_client or not await self._check_client(self._donor_client):
            self._donor_client, self._donor_id = await self._connect_client(self._donor_session)
        if not self._recipient_client or not await self._check_client(self._recipient_client):
            self._recipient_client, self._recipient_id = await self._connect_client(self._recipient_session)

    async def _check_client(self, client):
        try:
            await client.get_me()
            return True
        except Exception:
            return False

    async def _recovery_loop(self):
        try:
            await self._do_recovery()
        except asyncio.CancelledError:
            self._save_progress()
        except Exception as e:
            logger.error(f"[RECOVERY] {e}", exc_info=True)
            self._save_progress()
            try:
                prefix = self.get_prefix()
                await self._force_update(
                    f"<b>Error:</b> <code>{e}</code>\n"
                    f"{self.strings['progress_saved']}\n"
                    f"Use <code>{prefix}recovery resume</code>"
                )
            except Exception:
                pass
        finally:
            self._recovering = False

    async def _do_recovery(self):
        donor = self._donor_client
        recipient = self._recipient_client
        dp_deleted = self._donor_peer_deleted
        dp_recipient = self._donor_peer_recipient
        rp_donor = self._recipient_peer_donor

        if self._stage < 2:
            self._stage = 1
            self._analyzed = 0
            await self._force_update()

            for f in os.listdir(self._temp_dir):
                if f.startswith("chunk_"):
                    os.remove(os.path.join(self._temp_dir, f))

            buffer = []
            chunk_idx = 0
            chunk_count = 0
            last_grouped = 0

            async for msg in donor.iter_messages(dp_deleted, reverse=True):
                buffer.append({
                    "msg_id": msg.id,
                    "sender_id": msg.sender_id or 0,
                    "grouped_id": msg.grouped_id or 0,
                })
                self._analyzed += 1
                self._total = self._analyzed
                chunk_count += 1

                if chunk_count >= PARSE_CHUNK_SIZE:
                    chunk_count = 0
                    await self._update_status_msg()
                    await asyncio.sleep(PARSE_CHUNK_DELAY)

                if len(buffer) >= CHUNK_FILE_SIZE:
                    current_grouped = msg.grouped_id or 0
                    if current_grouped == 0 or current_grouped != last_grouped:
                        self._write_chunk(chunk_idx, buffer)
                        chunk_idx += 1
                        buffer = []

                last_grouped = msg.grouped_id or 0

            if buffer:
                self._write_chunk(chunk_idx, buffer)

            self._total = self._analyzed
            self._stage = 2
            self._save_progress()
            await self._force_update()

        num_chunks = self._count_chunks()
        self._stage = 2
        await self._force_update()

        work_start = time.time()

        for ci in range(num_chunks):
            entries = self._read_chunk(ci)
            if not entries:
                continue

            if self._last_processed_id:
                skip_idx = -1
                for idx, e in enumerate(entries):
                    if e["msg_id"] == self._last_processed_id:
                        skip_idx = idx
                        break
                if skip_idx >= 0:
                    entries = entries[skip_idx + 1:]
                elif entries[-1]["msg_id"] <= self._last_processed_id:
                    continue

            i = 0
            while i < len(entries):
                if not self._recovering:
                    self._save_progress()
                    return

                await self._wait_for_work_time()
                if not self._recovering:
                    self._save_progress()
                    return

                elapsed_work = time.time() - work_start
                if elapsed_work >= WORK_DURATION:
                    self._save_progress()
                    await self._force_update(
                        self._build_debug() + "\n\n"
                        + self.strings["break_msg"].format(minutes=BREAK_DURATION // 60)
                    )
                    await asyncio.sleep(BREAK_DURATION)
                    work_start = time.time()
                    await self._force_update(
                        self._build_debug() + "\n\n" + self.strings["resume_msg"]
                    )

                entry = entries[i]
                donor_id = self._donor_id
                deleted_id = self._deleted_id

                is_donor_msg = (entry["sender_id"] == donor_id)
                is_deleted_msg = (entry["sender_id"] == deleted_id)

                if not is_donor_msg and not is_deleted_msg:
                    self._skipped += 1
                    self._processed += 1
                    self._last_processed_id = entry["msg_id"]
                    i += 1
                    await self._update_status_msg()
                    continue

                if entry["grouped_id"]:
                    group_id = entry["grouped_id"]
                    album_entries = [entry]
                    j = i + 1
                    while j < len(entries) and entries[j]["grouped_id"] == group_id:
                        album_entries.append(entries[j])
                        j += 1

                    album_ids = [ae["msg_id"] for ae in album_entries]
                    try:
                        album_messages = await self._safe_call(
                            donor.get_messages(dp_deleted, ids=album_ids)
                        )
                        album_messages = [m for m in album_messages if m is not None]

                        if album_messages:
                            first_msg = album_messages[0]
                            if first_msg.action is not None:
                                self._skipped += len(album_entries)
                                self._processed += len(album_entries)
                            else:
                                new_msgs = await self._send_album(
                                    album_msgs=album_messages,
                                    is_donor_msg=is_donor_msg,
                                    donor=donor,
                                    recipient=recipient,
                                    dp_recipient=dp_recipient,
                                    rp_donor=rp_donor,
                                )
                                if new_msgs:
                                    for orig, new in zip(album_messages, new_msgs):
                                        self._id_map[orig.id] = new.id
                                    if is_donor_msg:
                                        await self._mark_read(
                                            recipient, rp_donor, new_msgs[-1].id
                                        )
                                        await self._mark_read(
                                            donor, dp_recipient, new_msgs[-1].id
                                        )
                                    else:
                                        await self._mark_read(
                                            donor, dp_recipient, new_msgs[-1].id
                                        )
                                        await self._mark_read(
                                            recipient, rp_donor, new_msgs[-1].id
                                        )
                                    for am in album_messages:
                                        if am.pinned:
                                            pin_id = self._id_map.get(am.id)
                                            if pin_id:
                                                await self._do_pin(donor, dp_recipient, pin_id)
                                                await asyncio.sleep(PIN_DELAY)
                                self._albums += 1
                                self._processed += len(album_entries)
                        else:
                            self._skipped += len(album_entries)
                            self._processed += len(album_entries)
                    except Exception as e:
                        logger.warning(f"[RECOVERY] Album {group_id}: {e}")
                        try:
                            for ae in album_entries:
                                await self._process_single_by_id(
                                    ae, donor, recipient,
                                    dp_deleted, dp_recipient, rp_donor,
                                    is_donor_msg,
                                )
                        except Exception:
                            self._skipped += len(album_entries)
                            self._processed += len(album_entries)

                    for ae in album_entries:
                        self._last_processed_id = ae["msg_id"]
                    i = j
                    self._calc_finish()
                    await self._update_status_msg()
                    if self._processed % PROGRESS_SAVE_EVERY == 0:
                        self._save_progress()
                    continue

                await self._process_single_by_id(
                    entry, donor, recipient,
                    dp_deleted, dp_recipient, rp_donor,
                    is_donor_msg,
                )
                self._last_processed_id = entry["msg_id"]
                i += 1
                self._calc_finish()
                await self._update_status_msg()
                if self._processed % PROGRESS_SAVE_EVERY == 0:
                    self._save_progress()

        self._stage = 3
        elapsed = round(time.time() - self._start_time)
        m, s = elapsed // 60, elapsed % 60
        self._clear_progress()
        self._wipe_temp()
        os.makedirs(self._temp_dir, exist_ok=True)
        await self._force_update(
            self.strings["done"].format(
                processed=self._processed, skipped=self._skipped,
                albums=self._albums, elapsed=f"{m}m {s}s",
            )
        )
        self._recovering = False

    async def _process_single_by_id(
        self, entry, donor, recipient, dp_deleted, dp_recipient, rp_donor, is_donor_msg
    ):
        try:
            msgs = await self._safe_call(
                donor.get_messages(dp_deleted, ids=[entry["msg_id"]])
            )
            msg = msgs[0] if msgs else None
            if not msg:
                self._skipped += 1
                self._processed += 1
                return
            if msg.action is not None:
                self._skipped += 1
                self._processed += 1
                return
            new_msg = await self._send_single(
                msg=msg,
                is_donor_msg=is_donor_msg,
                donor=donor,
                recipient=recipient,
                dp_recipient=dp_recipient,
                rp_donor=rp_donor,
            )
            if new_msg:
                self._id_map[msg.id] = new_msg.id
                if is_donor_msg:
                    await self._mark_read(recipient, rp_donor, new_msg.id)
                    await self._mark_read(donor, dp_recipient, new_msg.id)
                else:
                    await self._mark_read(donor, dp_recipient, new_msg.id)
                    await self._mark_read(recipient, rp_donor, new_msg.id)
                if msg.pinned:
                    await self._do_pin(donor, dp_recipient, new_msg.id)
                    await asyncio.sleep(PIN_DELAY)
            self._processed += 1
        except Exception as e:
            logger.warning(f"[RECOVERY] Skip {entry['msg_id']}: {e}")
            self._skipped += 1
            self._processed += 1

    def _get_text_html(self, msg):
        if not msg.message:
            return None
        try:
            if msg.entities:
                from telethon.extensions import html
                return html.unparse(msg.message, msg.entities)
        except Exception:
            pass
        return msg.message

    def _safe_text(self, text):
        if not text:
            return text
        if len(text) > MAX_MESSAGE_LENGTH:
            return text[:MAX_MESSAGE_LENGTH - 20] + "\n\n[truncated]"
        return text

    async def _send_single(
        self, msg, is_donor_msg, donor, recipient, dp_recipient, rp_donor
    ):
        if is_donor_msg:
            sender, target = donor, dp_recipient
        else:
            sender, target = recipient, rp_donor

        reply_to = None
        if msg.reply_to and msg.reply_to.reply_to_msg_id:
            reply_to = self._id_map.get(msg.reply_to.reply_to_msg_id)

        text = self._safe_text(self._get_text_html(msg))

        if msg.fwd_from:
            fwd = self._fwd_text(msg, text)
            return await self._type_and_send(
                sender, target, self._safe_text(fwd),
                parse_mode="html", reply_to=reply_to,
            )

        if msg.media and isinstance(msg.media, SKIP_MEDIA_TYPES):
            label = type(msg.media).__name__.replace("MessageMedia", "")
            fallback = f"[{label}]"
            if text:
                fallback = f"{text}\n\n[{label}]"
            return await self._type_and_send(
                sender, target, self._safe_text(fallback),
                parse_mode="html", reply_to=reply_to,
            )

        has_media = msg.media is not None and not isinstance(msg.media, MessageMediaWebPage)

        if has_media:
            return await self._send_media(
                msg=msg,
                is_donor_msg=is_donor_msg,
                donor=donor,
                recipient=recipient,
                dp_recipient=dp_recipient,
                rp_donor=rp_donor,
                reply_to=reply_to,
                text=text,
            )
        elif text:
            try:
                return await self._type_and_send(
                    sender, target, text,
                    parse_mode="html", reply_to=reply_to,
                )
            except Exception:
                raw = msg.raw_text or ""
                return await self._type_and_send(
                    sender, target, self._safe_text(raw),
                    reply_to=reply_to,
                )
        return None

    async def _send_album(
        self, album_msgs, is_donor_msg, donor, recipient, dp_recipient, rp_donor
    ):
        if is_donor_msg:
            sender, target = donor, dp_recipient
        else:
            sender, target = recipient, rp_donor

        reply_to = None
        first = album_msgs[0]
        if first.reply_to and first.reply_to.reply_to_msg_id:
            reply_to = self._id_map.get(first.reply_to.reply_to_msg_id)

        media_list = []
        captions = []
        for am in album_msgs:
            if am.media and not isinstance(am.media, (MessageMediaWebPage,) + SKIP_MEDIA_TYPES):
                media_list.append(am.media)
                captions.append(self._safe_text(self._get_text_html(am)) or "")
            else:
                media_list.append(am.media)
                captions.append("")

        combined_text = " ".join(c for c in captions if c)
        delay = _typing_delay(combined_text if combined_text else None)
        try:
            async with sender.action(target, "typing"):
                await asyncio.sleep(delay)
        except Exception:
            await asyncio.sleep(delay)

        if is_donor_msg:
            results = await self._safe_call(
                sender.send_file(
                    target, media_list, caption=captions,
                    reply_to=reply_to, parse_mode="html",
                )
            )
            if not isinstance(results, list):
                results = [results]
            return results
        else:
            transit_msgs = await self._safe_call(
                donor.send_file(dp_recipient, media_list)
            )
            if not isinstance(transit_msgs, list):
                transit_msgs = [transit_msgs]

            await asyncio.sleep(TRANSIT_WAIT)

            transit_ids = [t.id for t in transit_msgs]
            recipient_medias = []
            async for m in recipient.iter_messages(
                rp_donor, limit=TRANSIT_SEARCH_LIMIT + len(transit_msgs)
            ):
                if m.media:
                    recipient_medias.append(m.media)
                if len(recipient_medias) >= len(transit_msgs):
                    break

            recipient_medias.reverse()

            if len(recipient_medias) >= len(media_list):
                send_medias = recipient_medias[:len(media_list)]
            else:
                send_medias = recipient_medias or media_list

            results = await self._safe_call(
                recipient.send_file(
                    rp_donor, send_medias, caption=captions,
                    reply_to=reply_to, parse_mode="html",
                )
            )
            if not isinstance(results, list):
                results = [results]

            await asyncio.sleep(1)
            try:
                await donor.delete_messages(dp_recipient, transit_ids)
            except Exception:
                pass

            return results

    async def _send_media(
        self, msg, is_donor_msg, donor, recipient,
        dp_recipient, rp_donor, reply_to, text
    ):
        media = msg.media
        voice = self._is_voice(msg)
        video_note = self._is_video_note(msg)

        send_kw = {}
        if text:
            send_kw["caption"] = text
        if reply_to:
            send_kw["reply_to"] = reply_to
        if voice:
            send_kw["voice_note"] = True
        if video_note:
            send_kw["video_note"] = True
        send_kw["parse_mode"] = "html"

        if is_donor_msg:
            return await self._type_and_send_file(
                donor, dp_recipient, media, text=text, **send_kw,
            )
        else:
            transit = await self._safe_call(donor.send_file(dp_recipient, media))
            await asyncio.sleep(TRANSIT_WAIT)

            transit_on_recipient = None
            try:
                async for m in recipient.iter_messages(rp_donor, limit=TRANSIT_SEARCH_LIMIT):
                    if m.media:
                        transit_on_recipient = m
                        break
            except Exception:
                pass

            if transit_on_recipient and transit_on_recipient.media:
                r_kw = {}
                if text:
                    r_kw["caption"] = text
                if reply_to:
                    r_kw["reply_to"] = reply_to
                if voice:
                    r_kw["voice_note"] = True
                if video_note:
                    r_kw["video_note"] = True
                r_kw["parse_mode"] = "html"
                new_msg = await self._type_and_send_file(
                    recipient, rp_donor, transit_on_recipient.media,
                    text=text, **r_kw,
                )
            else:
                fallback = text or "[media transfer failed]"
                new_msg = await self._type_and_send(
                    recipient, rp_donor, fallback,
                    parse_mode="html", reply_to=reply_to,
                )

            await asyncio.sleep(1)
            try:
                await donor.delete_messages(dp_recipient, [transit.id])
            except Exception:
                pass

            return new_msg

    def _is_voice(self, msg):
        if not msg.document:
            return False
        for attr in msg.document.attributes:
            if isinstance(attr, DocumentAttributeAudio) and attr.voice:
                return True
        return False

    def _is_video_note(self, msg):
        if not msg.document:
            return False
        for attr in msg.document.attributes:
            if isinstance(attr, DocumentAttributeVideo) and attr.round_message:
                return True
        return False

    def _fwd_text(self, msg, original_text):
        fwd = msg.fwd_from
        from_name = "Unknown"
        if fwd.from_id:
            if isinstance(fwd.from_id, PeerUser):
                uid = fwd.from_id.user_id
                from_name = f'<a href="tg://user?id={uid}">User {uid}</a>'
            else:
                from_name = str(fwd.from_id)
        elif fwd.from_name:
            from_name = fwd.from_name
        body = original_text or ""
        return f"<i>Forwarded from {from_name}:</i>\n{body}"

    async def _do_pin(self, client, peer, msg_id):
        try:
            await self._safe_call(
                client(UpdatePinnedMessageRequest(
                    peer=peer, id=msg_id, silent=True,
                ))
            )
        except Exception as e:
            logger.warning(f"[RECOVERY] Pin failed: {e}")

    def _calc_finish(self):
        if self._processed <= 0 or self._total <= 0:
            self._finish_estimate = "calculating..."
            return
        elapsed = time.time() - self._start_time
        avg = elapsed / max(self._processed, 1)
        remaining = self._total - self._processed
        remaining_time = remaining * avg
        if self._work_start and self._work_end:
            start_h, start_m = self._work_start
            end_h, end_m = self._work_end
            if (start_h, start_m) <= (end_h, end_m):
                work_hours_per_day = (end_h * 60 + end_m - start_h * 60 - start_m) / 60
            else:
                work_hours_per_day = (24 * 60 - start_h * 60 - start_m + end_h * 60 + end_m) / 60
            work_secs_per_day = work_hours_per_day * 3600
            if work_secs_per_day > 0:
                work_days = remaining_time / work_secs_per_day
                total_remaining = work_days * 24 * 3600
            else:
                total_remaining = remaining_time
        else:
            cycles = remaining_time / WORK_DURATION
            breaks = max(0, int(cycles) - 1)
            total_remaining = remaining_time + breaks * BREAK_DURATION
        tz = timezone(timedelta(hours=self._get_tz()))
        finish = datetime.now(tz) + timedelta(seconds=total_remaining)
        self._finish_estimate = finish.strftime("%d.%m.%Y %H:%M:%S")