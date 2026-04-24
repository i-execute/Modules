__version__ = (1, 1, 0)
# meta developer: I_execute.t.me

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
        "help": (
            "<b>Statistics - Account Statistics</b>\n\n"
            "<code>{prefix}stat</code> - account statistics\n"
            "<code>{prefix}statd</code> - today activity\n"
            "<code>{prefix}statw</code> - weekly activity\n"
            "<code>{prefix}statm</code> - monthly activity\n"
            "<code>{prefix}staty</code> - yearly activity\n"
        ),
        "account_stats": (
            "<b>Account Statistics</b>\n"
            "<blockquote><b>Folders:</b> <code>{folders}</code>\n"
            "<b>Total chats:</b> <code>{chats}</code>\n"
            "<b>Archived:</b> <code>{archived}</code></blockquote>\n"
            "<blockquote><b>Users:</b> <code>{users}</code>\n"
            "<b>Bots:</b> <code>{bots}</code>\n"
            "<b>Groups:</b> <code>{groups}</code>\n"
            "<b>Channels:</b> <code>{channels}</code>\n"
            "<b>Deleted accounts:</b> <code>{deleted}</code></blockquote>\n"
            "<blockquote><b>Contacts:</b> <code>{contacts}</code>\n"
            "<b>Blocked:</b> <code>{blocked}</code></blockquote>\n"
            "<blockquote><b>Timezone:</b> UTC{timezone_str}\n"
            "<b>Current time:</b> {current_time}</blockquote>"
        ),
        "activity_stats": (
            "<b>Activity - {period_name}</b>\n"
            "<blockquote><b>Commands:</b> <code>{commands}</code>\n"
            "<b>Messages:</b> <code>{messages}</code>\n"
            "<b>Stickers:</b> <code>{stickers}</code>\n"
            "<b>Photos:</b> <code>{photos}</code>\n"
            "<b>Videos:</b> <code>{videos}</code>\n"
            "<b>Inline queries:</b> <code>{inline}</code></blockquote>\n"
            "<blockquote><b>Period:</b> {period_range}\n"
            "<b>Timezone:</b> UTC{timezone_str}\n"
            "<b>Current time:</b> {current_time}</blockquote>"
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
            "<b>Статистика аккаунта</b>\n"
            "<blockquote><b>Папки:</b> <code>{folders}</code>\n"
            "<b>Всего чатов:</b> <code>{chats}</code>\n"
            "<b>В архиве:</b> <code>{archived}</code></blockquote>\n"
            "<blockquote><b>Пользователи:</b> <code>{users}</code>\n"
            "<b>Боты:</b> <code>{bots}</code>\n"
            "<b>Группы:</b> <code>{groups}</code>\n"
            "<b>Каналы:</b> <code>{channels}</code>\n"
            "<b>Удаленные аккаунты:</b> <code>{deleted}</code></blockquote>\n"
            "<blockquote><b>Контакты:</b> <code>{contacts}</code>\n"
            "<b>Заблокировано:</b> <code>{blocked}</code></blockquote>\n"
            "<blockquote><b>Часовой пояс:</b> UTC{timezone_str}\n"
            "<b>Текущее время:</b> {current_time}</blockquote>"
        ),
        "activity_stats": (
            "<b>Активность - {period_name}</b>\n"
            "<blockquote><b>Команды:</b> <code>{commands}</code>\n"
            "<b>Сообщения:</b> <code>{messages}</code>\n"
            "<b>Стикеры:</b> <code>{stickers}</code>\n"
            "<b>Фото:</b> <code>{photos}</code>\n"
            "<b>Видео:</b> <code>{videos}</code>\n"
            "<b>Инлайн запросы:</b> <code>{inline}</code></blockquote>\n"
            "<blockquote><b>Период:</b> {period_range}\n"
            "<b>Часовой пояс:</b> UTC{timezone_str}\n"
            "<b>Текущее время:</b> {current_time}</blockquote>"
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
        self._ensure_counters()

    def _ensure_counters(self):
        periods = ["day", "week", "month", "year"]
        types = ["commands", "messages", "stickers", "photos", "videos", "inline"]
        for period in periods:
            for t in types:
                key = f"cnt_{period}_{t}"
                if self._db.get("Statistics", key) is None:
                    self._db.set("Statistics", key, 0)
            reset_key = f"cnt_{period}_reset_ts"
            if self._db.get("Statistics", reset_key) is None:
                start_ts = int(self._period_start(period).timestamp())
                self._db.set("Statistics", reset_key, start_ts)

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

    def _maybe_reset_period(self, period):
        reset_key = f"cnt_{period}_reset_ts"
        stored_ts = self._db.get("Statistics", reset_key) or 0
        current_start_ts = int(self._period_start(period).timestamp())
        if current_start_ts > stored_ts:
            types = ["commands", "messages", "stickers", "photos", "videos", "inline"]
            for t in types:
                self._db.set("Statistics", f"cnt_{period}_{t}", 0)
            self._db.set("Statistics", reset_key, current_start_ts)

    def _increment(self, event_type):
        periods = ["day", "week", "month", "year"]
        for period in periods:
            self._maybe_reset_period(period)
            key = f"cnt_{period}_{event_type}"
            val = self._db.get("Statistics", key) or 0
            self._db.set("Statistics", key, val + 1)

    def _get_counts(self, period):
        self._maybe_reset_period(period)
        types = ["commands", "messages", "stickers", "photos", "videos", "inline"]
        counts = {}
        for t in types:
            counts[t] = self._db.get("Statistics", f"cnt_{period}_{t}") or 0
        return counts

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
            self._increment("commands")

        self._increment("messages")

        if getattr(message, "sticker", None):
            self._increment("stickers")
        elif getattr(message, "photo", None):
            self._increment("photos")
        elif getattr(message, "video", None):
            self._increment("videos")

        if getattr(message, "via_bot_id", None):
            self._increment("inline")

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
                contacts = max(0, len(result.users) - 1)
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
        counts = self._get_counts("day")
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
        counts = self._get_counts("week")
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
        counts = self._get_counts("month")
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
        counts = self._get_counts("year")
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