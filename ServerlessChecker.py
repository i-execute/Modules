# CopyLeft 2026 github.com/i-execute
# Author: I_execute.t.me
# Licensed under AGPLv3.

__version__ = (1, 0, 0)
# meta developer: Execute_forge.t.me

import asyncio
import json
import logging
import re
import urllib.error
import urllib.parse
import urllib.request

from telethon.tl.types import Message

from .. import loader, utils

logger = logging.getLogger(__name__)

WEBHOOK_URL = "https://cloud.telegram.org/app{app_id}/webhook"
WEBHOOK_HOST = "cloud.telegram.org"
WEBHOOK_SCHEME = "https"
WEBHOOK_TIMEOUT = 15
MAX_TARGETS = 10

SERVERLESS_TYPES = {"INTERNAL_ERROR"}
MISSING_TYPES = {"APP_NOT_FOUND"}

USERNAME_RE = re.compile(r"^[A-Za-z0-9_]{4,32}$")
BOT_ID_RE = re.compile(r"^[0-9]{5,20}$")
LINK_RE = re.compile(r"^(?:https?://)?t(?:elegram)?\.me/([A-Za-z0-9_]{4,32})/?$", re.IGNORECASE)


def _escape(text):
    if text is None:
        return ""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _parse_target(raw):
    value = (raw or "").strip()
    if not value:
        return None
    link = LINK_RE.match(value)
    if link:
        return link.group(1)
    value = value.lstrip("@")
    if BOT_ID_RE.match(value):
        return int(value)
    if USERNAME_RE.match(value):
        return value
    return None


def _webhook_status(app_id):
    url = WEBHOOK_URL.format(app_id=int(app_id))
    parts = urllib.parse.urlsplit(url)
    if parts.scheme != WEBHOOK_SCHEME or parts.hostname != WEBHOOK_HOST:
        raise RuntimeError("the webhook host is not allowed")
    request = urllib.request.Request(url, headers={"User-Agent": "curl/8.5.0"})
    try:
        with urllib.request.urlopen(request, timeout=WEBHOOK_TIMEOUT) as response:
            body = response.read(4096)
            status = response.status
    except urllib.error.HTTPError as error:
        body = error.read(4096)
        status = error.code
    kind = ""
    try:
        payload = json.loads(body.decode("utf-8", "replace"))
        if isinstance(payload, dict):
            kind = str(payload.get("error_type") or "")
    except Exception:
        kind = ""
    return status, kind


def _verdict(status, kind):
    if kind in SERVERLESS_TYPES or (not kind and status == 401):
        return "serverless"
    if kind in MISSING_TYPES or (not kind and status == 404):
        return "missing"
    return "unknown"


@loader.tds
class ServerlessChecker(loader.Module):
    """Detects whether a Telegram bot runs on Telegram serverless infrastructure"""

    strings = {
        "name": "ServerlessChecker",
        "no_args": (
            "<b>ServerlessChecker</b> | no target\n"
            "<blockquote>Pass a bot username, link or id\n"
            "<code>{prefix}slch @bot</code>\n"
            "<code>{prefix}slch 8627643530</code>\n"
            "<code>{prefix}slch @bot1 @bot2 8627643530</code></blockquote>"
        ),
        "too_many": (
            "<b>ServerlessChecker</b> | too many targets\n"
            "<blockquote>Up to <code>{limit}</code> bots per call</blockquote>"
        ),
        "bad_target": (
            "<b>ServerlessChecker</b> | cannot parse <code>{target}</code>\n"
            "<blockquote>Use <code>@username</code>, <code>t.me/username</code> or a numeric id</blockquote>"
        ),
        "not_found": (
            "<b>ServerlessChecker</b> | <code>{target}</code>\n"
            "<blockquote>Entity could not be resolved</blockquote>"
        ),
        "not_a_bot": (
            "<b>ServerlessChecker</b> | <code>{target}</code>\n"
            "<blockquote>This account is not a bot</blockquote>"
        ),
        "checking": "<b>ServerlessChecker</b> | checking <code>{count}</code> bot(s)...",
        "result": (
            "<b>ServerlessChecker</b> | <code>{handle}</code>\n"
            "<blockquote>"
            "<b>Bot ID</b> | <code>{bot_id}</code>\n"
            "<b>Verdict</b> | {verdict}\n"
            "<b>Webhook</b> | <code>{status}</code>{kind}"
            "</blockquote>\n"
            "{entity}"
        ),
        "result_line": "<b>{handle}</b> | {verdict} | <code>{status}</code>",
        "verdict_serverless": "<b>runs in serverless</b>",
        "verdict_missing": "<b>no serverless</b>",
        "verdict_unknown": "<b>unknown</b>",
        "kind_line": "\n<b>Response</b> | <code>{kind}</code>",
        "entity": "<blockquote expandable><code>{dump}</code></blockquote>",
        "failed": (
            "<b>ServerlessChecker</b> | request failed\n"
            "<blockquote><code>{error}</code></blockquote>"
        ),
    }

    strings_ru = {
        "no_args": (
            "<b>ServerlessChecker</b> | нет цели\n"
            "<blockquote>Укажите юзернейм, ссылку или id бота\n"
            "<code>{prefix}slch @bot</code>\n"
            "<code>{prefix}slch 8627643530</code>\n"
            "<code>{prefix}slch @bot1 @bot2 8627643530</code></blockquote>"
        ),
        "too_many": (
            "<b>ServerlessChecker</b> | слишком много целей\n"
            "<blockquote>Не больше <code>{limit}</code> ботов за вызов</blockquote>"
        ),
        "bad_target": (
            "<b>ServerlessChecker</b> | не разобрать <code>{target}</code>\n"
            "<blockquote>Подойдёт <code>@username</code>, <code>t.me/username</code> или числовой id</blockquote>"
        ),
        "not_found": (
            "<b>ServerlessChecker</b> | <code>{target}</code>\n"
            "<blockquote>Сущность не найдена</blockquote>"
        ),
        "not_a_bot": (
            "<b>ServerlessChecker</b> | <code>{target}</code>\n"
            "<blockquote>Это не бот</blockquote>"
        ),
        "checking": "<b>ServerlessChecker</b> | проверяю <code>{count}</code> бот(ов)...",
        "result": (
            "<b>ServerlessChecker</b> | <code>{handle}</code>\n"
            "<blockquote>"
            "<b>ID бота</b> | <code>{bot_id}</code>\n"
            "<b>Вердикт</b> | {verdict}\n"
            "<b>Webhook</b> | <code>{status}</code>{kind}"
            "</blockquote>\n"
            "{entity}"
        ),
        "result_line": "<b>{handle}</b> | {verdict} | <code>{status}</code>",
        "verdict_serverless": "<b>работает в serverless</b>",
        "verdict_missing": "<b>serverless выключен</b>",
        "verdict_unknown": "<b>неизвестно</b>",
        "kind_line": "\n<b>Ответ</b> | <code>{kind}</code>",
        "entity": "<blockquote expandable><code>{dump}</code></blockquote>",
        "failed": (
            "<b>ServerlessChecker</b> | запрос не удался\n"
            "<blockquote><code>{error}</code></blockquote>"
        ),
    }

    def _verdict_text(self, verdict):
        return self.strings[f"verdict_{verdict}"]

    async def _resolve(self, target):
        try:
            entity = await self._client.get_entity(target)
        except Exception:
            return None
        return entity

    async def _probe(self, bot_id):
        try:
            return await asyncio.to_thread(_webhook_status, bot_id)
        except Exception as error:
            raise RuntimeError(f"{type(error).__name__}: {error}") from error

    @loader.command()
    async def slch(self, message: Message):
        """Check if bots run on Telegram serverless: .slch @bot or .slch 1234567890"""
        args = utils.get_args_raw(message)
        prefix = self.get_prefix()
        targets = [item for item in re.split(r"[\s,]+", args.strip()) if item]
        if not targets:
            await utils.answer(message, self.strings["no_args"].format(prefix=prefix))
            return
        if len(targets) > MAX_TARGETS:
            await utils.answer(message, self.strings["too_many"].format(limit=MAX_TARGETS))
            return

        parsed = []
        for raw in targets:
            target = _parse_target(raw)
            if target is None:
                await utils.answer(message, self.strings["bad_target"].format(target=_escape(raw)))
                return
            parsed.append(target)

        status = await utils.answer(
            message,
            self.strings["checking"].format(count=len(parsed)),
        )

        resolved = []
        for target in parsed:
            entity = await self._resolve(target)
            if entity is None:
                await utils.answer(status, self.strings["not_found"].format(target=_escape(target)))
                return
            if not getattr(entity, "bot", False):
                handle = getattr(entity, "username", None) or getattr(entity, "id", target)
                await utils.answer(status, self.strings["not_a_bot"].format(target=_escape(handle)))
                return
            resolved.append(entity)

        lines = []
        single = None
        for entity in resolved:
            bot_id = int(entity.id)
            try:
                code, kind = await self._probe(bot_id)
            except Exception as error:
                await utils.answer(status, self.strings["failed"].format(error=_escape(error)))
                return
            verdict = _verdict(code, kind)
            handle = "@" + entity.username if getattr(entity, "username", None) else str(bot_id)
            if len(resolved) == 1:
                extra = self.strings["kind_line"].format(kind=_escape(kind)) if kind else ""
                single = self.strings["result"].format(
                    handle=_escape(handle),
                    bot_id=bot_id,
                    verdict=self._verdict_text(verdict),
                    status=code,
                    kind=extra,
                    entity=self.strings["entity"].format(dump=_escape(repr(entity))),
                )
            lines.append(
                self.strings["result_line"].format(
                    handle=_escape(handle),
                    verdict=self._verdict_text(verdict),
                    status=code,
                )
            )

        if single:
            await utils.answer(status, single)
            return
        await utils.answer(status, "\n".join(lines))
