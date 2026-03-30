__version__ = (1, 0, 0)
# meta developer: FireJester.t.me

import logging
import time

from datetime import datetime, timezone, timedelta

from telethon.tl.types import (
    Message,
    InputMediaWebPage,
    User,
    Channel,
    Chat,
    ChannelForbidden,
    ChatForbidden,
)

from .. import loader, utils

logger = logging.getLogger(__name__)


@loader.tds
class Statistics(loader.Module):
    """Account statistics and activity tracker"""

    strings = {
        "name": "Statistics",
    }

    strings_en = {
        "help": (
            "<b>Statistics - Account Statistics</b>\n\n"
            "<code>{prefix}stat</code> - account statistics\n"
            "<code>{prefix}statd</code> - today activity\n"
            "<code>{prefix}statw</code> - weekly activity\n"
            "<code>{prefix}statm</code> - monthly activity\n"
            "<code>{prefix}staty</code> - yearly activity\n"
        ),
        "account_stats": (
            "<b>Account Statistics</b>\n\n"
            "<blockquote>"
            "<b>Folders:</b> <code>{folders}</code>\n"
            "<b>Total chats:</b> <code>{chats}</code>\n"
            "<b>Archived:</b> <code>{archived}</code>"
            "</blockquote>\n\n"
            "<blockquote>"
            "<b>Users:</b> <code>{users}</code>\n"
            "<b>Bots:</b> <code>{bots}</code>\n"
            "<b>Groups:</b> <code>{groups}</code>\n"
            "<b>Channels:</b> <code>{channels}</code>\n"
            "<b>Deleted accounts:</b> <code>{deleted}</code>"
            "</blockquote>\n\n"
            "<blockquote>"
            "<b>Contacts:</b> <code>{contacts}</code>\n"
            "<b>Blocked:</b> <code>{blocked}</code>"
            "</blockquote>\n\n"
            "<blockquote>"
            "<b>Timezone:</b> UTC{timezone_str}\n"
            "<b>Current time:</b> {current_time}"
            "</blockquote>"
        ),
        "activity_stats": (
            "<b>Activity - {period_name}</b>\n\n"
            "<blockquote>"
            "<b>Commands:</b> <code>{commands}</code>\n"
            "<b>Messages:</b> <code>{messages}</code>\n"
            "<b>Stickers:</b> <code>{stickers}</code>\n"
            "<b>Photos:</b> <code>{photos}</code>\n"
            "<b>Videos:</b> <code>{videos}</code>\n"
            "<b>Inline queries:</b> <code>{inline}</code>"
            "</blockquote>\n\n"
            "<blockquote>"
            "<b>Period:</b> {period_range}\n"
            "<b>Timezone:</b> UTC{timezone_str}\n"
            "<b>Current time:</b> {current_time}"
            "</blockquote>"
        ),
        "period_today": "Today",
        "period_week": "This week",
        "period_month": "This month",
        "period_year": "This year",
        "loading": "<b>Loading statistics...</b>",
    }

    strings_ru = {
        "help": (
            "<b>Statistics - Статистика аккаунта</b>\n\n"
            "<code>{prefix}stat</code> - статистика аккаунта\n"
            "<code>{prefix}statd</code> - активность за сегодня\n"
            "<code>{prefix}statw</code> - активность за неделю\n"
            "<code>{prefix}statm</code> - активность за месяц\n"
            "<code>{prefix}staty</code> - активность за год\n"
        ),
        "account_stats": (
            "<b>Статистика аккаунта</b>\n\n"
            "<blockquote>"
            "<b>Папки:</b> <code>{folders}</code>\n"
            "<b>Всего чатов:</b> <code>{chats}</code>\n"
            "<b>В архиве:</b> <code>{archived}</code>"
            "</blockquote>\n\n"
            "<blockquote>"
            "<b>Пользователи:</b> <code>{users}</code>\n"
            "<b>Боты:</b> <code>{bots}</code>\n"
            "<b>Группы:</b> <code>{groups}</code>\n"
            "<b>Каналы:</b> <code>{channels}</code>\n"
            "<b>Удаленные аккаунты:</b> <code>{deleted}</code>"
            "</blockquote>\n\n"
            "<blockquote>"
            "<b>Контакты:</b> <code>{contacts}</code>\n"
            "<b>Заблокировано:</b> <code>{blocked}</code>"
            "</blockquote>\n\n"
            "<blockquote>"
            "<b>Часовой пояс:</b> UTC{timezone_str}\n"
            "<b>Текущее время:</b> {current_time}"
            "</blockquote>"
        ),
        "activity_stats": (
            "<b>Активность - {period_name}</b>\n\n"
            "<blockquote>"
            "<b>Команды:</b> <code>{commands}</code>\n"
            "<b>Сообщения:</b> <code>{messages}</code>\n"
            "<b>Стикеры:</b> <code>{stickers}</code>\n"
            "<b>Фото:</b> <code>{photos}</code>\n"
            "<b>Видео:</b> <code>{videos}</code>\n"
            "<b>Инлайн запросы:</b> <code>{inline}</code>"
            "</blockquote>\n\n"
            "<blockquote>"
            "<b>Период:</b> {period_range}\n"
            "<b>Часовой пояс:</b> UTC{timezone_str}\n"
            "<b>Текущее время:</b> {current_time}"
            "</blockquote>"
        ),
        "period_today": "Сегодня",
        "period_week": "Эта неделя",
        "period_month": "Этот месяц",
        "period_year": "Этот год",
        "loading": "<b>Загрузка статистики...</b>",
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "timezone_offset", 3, "UTC offset (-12 to 12)",
                validator=loader.validators.Integer(minimum=-12, maximum=12),
            ),
            loader.ConfigValue(
                "banner_url", "", "Banner image URL for message preview",
                validator=loader.validators.String(),
            ),
        )

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        self._me = await client.get_me()
        if not self._db.get("Statistics", "events"):
            self._db.set("Statistics", "events", [])

    def _tz(self):
        return timezone(timedelta(hours=self.config["timezone_offset"]))

    def _now(self):
        return datetime.now(self._tz())

    def _now_str(self):
        return self._now().strftime("%d.%m.%Y %H:%M:%S")

    def _tz_label(self):
        o = self.config["timezone_offset"]
        return f"+{o}" if o >= 0 else str(o)

    def _period_start(self, period):
        now = self._now()
        if period == "day":
            return now.replace(hour=0, minute=0, second=0, microsecond=0)
        elif period == "week":
            start = now - timedelta(days=now.weekday())
            return start.replace(hour=0, minute=0, second=0, microsecond=0)
        elif period == "month":
            return now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        elif period == "year":
            return now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        return now

    def _period_range_str(self, period):
        start = self._period_start(period)
        now = self._now()
        return f"{start.strftime('%d.%m.%Y %H:%M')} - {now.strftime('%d.%m.%Y %H:%M')}"

    def _record_event(self, event_type):
        events = self._db.get("Statistics", "events") or []
        events.append({
            "type": event_type,
            "ts": time.time(),
        })
        self._db.set("Statistics", "events", events)

    def _count_events(self, period):
        events = self._db.get("Statistics", "events") or []
        start_ts = self._period_start(period).timestamp()
        counts = {
            "commands": 0,
            "messages": 0,
            "stickers": 0,
            "photos": 0,
            "videos": 0,
            "inline": 0,
        }
        for ev in events:
            if ev["ts"] >= start_ts:
                t = ev["type"]
                if t in counts:
                    counts[t] += 1
        return counts

    def _cleanup_events(self):
        events = self._db.get("Statistics", "events") or []
        cutoff = self._period_start("year").timestamp()
        cleaned = [ev for ev in events if ev["ts"] >= cutoff]
        if len(cleaned) != len(events):
            self._db.set("Statistics", "events", cleaned)

    async def _get_media(self):
        url = self.config["banner_url"]
        if url:
            return InputMediaWebPage(url, optional=True)
        return None

    async def _answer_with_media(self, message, text):
        media = await self._get_media()
        try:
            await utils.answer(
                message,
                text,
                file=media,
                parse_mode="html",
                invert_media=True,
            )
        except Exception:
            await utils.answer(message, text, parse_mode="html")

    async def _count_dialogs(self):
        users = 0
        bots = 0
        groups = 0
        channels = 0
        deleted = 0
        total = 0
        archived = 0

        try:
            dialogs = await self._client.get_dialogs(limit=None)
            total = len(dialogs)
            for d in dialogs:
                ent = d.entity
                if isinstance(ent, (ChannelForbidden, ChatForbidden)):
                    continue
                if isinstance(ent, User):
                    if getattr(ent, "deleted", False):
                        deleted += 1
                    elif getattr(ent, "bot", False):
                        bots += 1
                    elif not getattr(ent, "is_self", False):
                        users += 1
                elif isinstance(ent, Channel):
                    if getattr(ent, "broadcast", False):
                        channels += 1
                    else:
                        groups += 1
                elif isinstance(ent, Chat):
                    groups += 1
        except Exception:
            pass

        try:
            archived_dialogs = await self._client.get_dialogs(folder=1, limit=None)
            archived = len(archived_dialogs)
        except Exception:
            pass

        return {
            "users": users,
            "bots": bots,
            "groups": groups,
            "channels": channels,
            "deleted": deleted,
            "chats": total,
            "archived": archived,
        }

    @loader.watcher("out", "only_messages")
    async def watcher(self, message: Message):
        """Activity tracker"""
        if not getattr(message, "out", False):
            return

        text = getattr(message, "raw_text", "") or ""
        prefixes = self.allmodules.get_prefixes()

        is_command = False
        for p in prefixes:
            if text.startswith(p):
                is_command = True
                break

        if is_command:
            self._record_event("commands")

        self._record_event("messages")

        if getattr(message, "sticker", None):
            self._record_event("stickers")
        elif getattr(message, "photo", None):
            self._record_event("photos")
        elif getattr(message, "video", None):
            self._record_event("videos")

        if getattr(message, "via_bot_id", None):
            self._record_event("inline")

    @loader.command(
        ru_doc="Статистика аккаунта",
        en_doc="Account statistics",
    )
    async def stat(self, message: Message):
        """Account statistics"""
        args = utils.get_args_raw(message)
        prefix = self.get_prefix()

        if args:
            await self._answer_with_media(
                message,
                self.strings["help"].format(prefix=prefix),
            )
            return

        await utils.answer(message, self.strings["loading"], parse_mode="html")

        dialog_counts = await self._count_dialogs()

        folders = 0
        try:
            from telethon.tl.functions.messages import GetDialogFiltersRequest
            r = await self._client(GetDialogFiltersRequest())
            fs = getattr(r, "filters", r)
            folders = len([f for f in fs if hasattr(f, "id")])
        except Exception:
            pass

        contacts = 0
        try:
            from telethon.tl.functions.contacts import GetContactsRequest
            result = await self._client(GetContactsRequest(hash=0))
            if hasattr(result, "users"):
                contacts = len(result.users)
        except Exception:
            pass

        blocked = 0
        try:
            from telethon.tl.functions.contacts import GetBlockedRequest
            result = await self._client(GetBlockedRequest(offset=0, limit=1))
            blocked = getattr(result, "count", 0)
        except Exception:
            pass

        await self._answer_with_media(
            message,
            self.strings["account_stats"].format(
                folders=folders,
                chats=dialog_counts["chats"],
                archived=dialog_counts["archived"],
                users=dialog_counts["users"],
                bots=dialog_counts["bots"],
                groups=dialog_counts["groups"],
                channels=dialog_counts["channels"],
                deleted=dialog_counts["deleted"],
                contacts=contacts,
                blocked=blocked,
                timezone_str=self._tz_label(),
                current_time=self._now_str(),
            ),
        )

    @loader.command(
        ru_doc="Активность за сегодня",
        en_doc="Today activity",
    )
    async def statd(self, message: Message):
        """Today activity"""
        self._cleanup_events()
        counts = self._count_events("day")
        await self._answer_with_media(
            message,
            self.strings["activity_stats"].format(
                period_name=self.strings["period_today"],
                commands=counts["commands"],
                messages=counts["messages"],
                stickers=counts["stickers"],
                photos=counts["photos"],
                videos=counts["videos"],
                inline=counts["inline"],
                period_range=self._period_range_str("day"),
                timezone_str=self._tz_label(),
                current_time=self._now_str(),
            ),
        )

    @loader.command(
        ru_doc="Активность за неделю",
        en_doc="Weekly activity",
    )
    async def statw(self, message: Message):
        """Weekly activity"""
        self._cleanup_events()
        counts = self._count_events("week")
        await self._answer_with_media(
            message,
            self.strings["activity_stats"].format(
                period_name=self.strings["period_week"],
                commands=counts["commands"],
                messages=counts["messages"],
                stickers=counts["stickers"],
                photos=counts["photos"],
                videos=counts["videos"],
                inline=counts["inline"],
                period_range=self._period_range_str("week"),
                timezone_str=self._tz_label(),
                current_time=self._now_str(),
            ),
        )

    @loader.command(
        ru_doc="Активность за месяц",
        en_doc="Monthly activity",
    )
    async def statm(self, message: Message):
        """Monthly activity"""
        self._cleanup_events()
        counts = self._count_events("month")
        await self._answer_with_media(
            message,
            self.strings["activity_stats"].format(
                period_name=self.strings["period_month"],
                commands=counts["commands"],
                messages=counts["messages"],
                stickers=counts["stickers"],
                photos=counts["photos"],
                videos=counts["videos"],
                inline=counts["inline"],
                period_range=self._period_range_str("month"),
                timezone_str=self._tz_label(),
                current_time=self._now_str(),
            ),
        )

    @loader.command(
        ru_doc="Активность за год",
        en_doc="Yearly activity",
    )
    async def staty(self, message: Message):
        """Yearly activity"""
        self._cleanup_events()
        counts = self._count_events("year")
        await self._answer_with_media(
            message,
            self.strings["activity_stats"].format(
                period_name=self.strings["period_year"],
                commands=counts["commands"],
                messages=counts["messages"],
                stickers=counts["stickers"],
                photos=counts["photos"],
                videos=counts["videos"],
                inline=counts["inline"],
                period_range=self._period_range_str("year"),
                timezone_str=self._tz_label(),
                current_time=self._now_str(),
            ),
        )