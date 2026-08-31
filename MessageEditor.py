__version__ = (1, 1, 0)
# meta developer: I_execute.t.me

import asyncio
import re

from telethon.tl.types import Message
from telethon.utils import get_display_name

from .. import loader, utils


def _apply_rich_parser(text: str) -> str:
    lines = text.split("\n")
    result = []

    for line in lines:
        stripped = line.strip()

        if stripped.startswith("- ") and len(stripped) > 2:
            item = stripped[2:].strip()
            result.append(f"<ul><li>{item}</li></ul>")
            continue

        if stripped.startswith("\u2014 ") and len(stripped) > 2:
            item = stripped[2:].strip()
            result.append(f"<ul><li>[x] {item}</li></ul>")
            continue

        line = re.sub(
            r"(\w+)\*\*",
            lambda m: f"<tg-math-block>{m.group(1)}^2</tg-math-block>",
            line,
        )
        line = re.sub(
            r"(\w+)\^(\d+)",
            lambda m: f"<tg-math-block>{m.group(1)}^{m.group(2)}</tg-math-block>",
            line,
        )

        line = re.sub(
            r"(\d+)\s*/\s*(\d+)",
            lambda m: f"<tg-math-block>\\frac{{{m.group(1)}}}{{{m.group(2)}}} = {int(m.group(1)) // int(m.group(2))}</tg-math-block>"
            if int(m.group(2)) != 0 and int(m.group(1)) % int(m.group(2)) == 0
            else f"<tg-math-block>\\frac{{{m.group(1)}}}{{{m.group(2)}}}</tg-math-block>",
            line,
        )
        line = re.sub(
            r"(\d+)\s*:\s*(\d+)",
            lambda m: f"<tg-math-block>{m.group(1)}:{m.group(2)} = {int(m.group(1)) // int(m.group(2))}</tg-math-block>"
            if int(m.group(2)) != 0
            else f"<tg-math-block>{m.group(1)}:{m.group(2)}</tg-math-block>",
            line,
        )
        line = re.sub(
            r"(\d+)\s*\*\s*([a-zA-Z])|([a-zA-Z])\s*\*\s*(\d+)",
            lambda m: f"<tg-math-block>{m.group(1) or m.group(4)}{m.group(2) or m.group(3)}</tg-math-block>",
            line,
        )
        line = re.sub(
            r"(\d+)\s*\+\s*(\d+)",
            lambda m: f"<tg-math-block>{m.group(1)}+{m.group(2)} = {int(m.group(1)) + int(m.group(2))}</tg-math-block>",
            line,
        )
        line = re.sub(
            r"(\d+)\s*-\s*(\d+)",
            lambda m: f"<tg-math-block>{m.group(1)}-{m.group(2)} = {int(m.group(1)) - int(m.group(2))}</tg-math-block>",
            line,
        )

        line = re.sub(
            r"(?<![<\w])([a-zA-Z])(?![\w>])",
            lambda m: f"<tg-math-block>{m.group(1)}</tg-math-block>",
            line,
        )

        line = re.sub(
            r"(?<!\d)(\d+)(?!\d)",
            lambda m: f"<tg-math-block>{m.group(1)}</tg-math-block>",
            line,
        )

        result.append(line)

    return "\n".join(result)


@loader.tds
class MessageEditor(loader.Module):
    """Edit messages with formatting"""

    strings = {
        "name": "MessageEditor",
        "no_reply": "<h3>Use this command in reply to a message</h3>",
        "not_owner": "<h3>Are you serious?</h3>",
        "edit_failed": "<h3>Message can no longer be edited</h3>",
    }

    strings_ru = {
        "no_reply": "<h3>Используй команду в реплай на сообщение</h3>",
        "not_owner": "<h3>Ты серьезно?</h3>",
        "edit_failed": "<h3>Сообщение уже нельзя редактировать</h3>",
    }

    async def _check_and_edit(self, message: Message, entity_type):
        if not message.is_reply:
            msg = await utils.answer(message, self.strings["no_reply"])
            await asyncio.sleep(5)
            await msg.delete()
            return

        reply = await message.get_reply_message()
        me = await message.client.get_me()

        if reply.sender_id != me.id:
            msg = await utils.answer(message, self.strings["not_owner"])
            await asyncio.sleep(5)
            await msg.delete()
            return

        try:
            from telethon.tl.types import (
                MessageEntityBold,
                MessageEntityCode,
                MessageEntityPre,
                MessageEntityBlockquote,
            )

            text = reply.message or ""
            entities = list(reply.entities) if reply.entities else []

            if entity_type == "bold":
                new_entity = MessageEntityBold(offset=0, length=len(text))
                entities.insert(0, new_entity)
                await reply.edit(text, formatting_entities=entities)

            elif entity_type == "code":
                new_entity = MessageEntityCode(offset=0, length=len(text))
                entities.insert(0, new_entity)
                await reply.edit(text, formatting_entities=entities)

            elif entity_type == "pre":
                new_entity = MessageEntityPre(offset=0, length=len(text), language="")
                entities.insert(0, new_entity)
                await reply.edit(text, formatting_entities=entities)

            elif entity_type == "quote":
                new_entity = MessageEntityBlockquote(offset=0, length=len(text))
                entities.insert(0, new_entity)
                await reply.edit(text, formatting_entities=entities)

            elif entity_type == "quote_expandable":
                new_entity = MessageEntityBlockquote(offset=0, length=len(text), collapsed=True)
                entities.insert(0, new_entity)
                await reply.edit(text, formatting_entities=entities)

            elif entity_type in ("p1", "p2", "p3", "p4", "p5", "p6"):
                level = entity_type[1]
                parsed = _apply_rich_parser(text)
                rich = f"<h{level}>{parsed}</h{level}>"
                await reply.edit(rich, parse_mode=None)

            elif entity_type == "rc":
                rich = f'<pre><code class="language-">{text}</code></pre>'
                await reply.edit(rich, parse_mode=None)

            elif entity_type == "rq":
                sender = await reply.get_sender()
                display = get_display_name(sender) if sender else ""
                parsed = _apply_rich_parser(text)
                rich = f"<blockquote>{parsed}<cite>{display}</cite></blockquote>"
                await reply.edit(rich, parse_mode=None)

            elif entity_type == "rqc":
                sender = await reply.get_sender()
                display = get_display_name(sender) if sender else ""
                parsed = _apply_rich_parser(text)
                rich = f"<blockquote>{parsed}<cite><i>{display}</i></cite></blockquote>"
                await reply.edit(rich, parse_mode=None)

            elif entity_type == "t":
                rich = f'<table><tr><td align="center">{text}</td></tr></table>'
                await reply.edit(rich, parse_mode=None)

            elif entity_type == "tb":
                rich = f'<table><tr><th align="center">{text}</th></tr></table>'
                await reply.edit(rich, parse_mode=None)

            else:
                return

            await message.delete()

        except Exception as e:
            msg = await utils.answer(message, f"{self.strings['edit_failed']}: {str(e)}")
            await asyncio.sleep(5)
            await msg.delete()

    @loader.command(
        ru_doc="Сделать текст жирным",
        en_doc="Make text bold",
    )
    async def b(self, message: Message):
        """Make text bold"""
        await self._check_and_edit(message, "bold")

    @loader.command(
        ru_doc="Сделать текст кодом",
        en_doc="Make text code",
    )
    async def c(self, message: Message):
        """Make text code"""
        await self._check_and_edit(message, "pre")

    @loader.command(
        ru_doc="Сделать текст моноширинным",
        en_doc="Make text monospace",
    )
    async def m(self, message: Message):
        """Make text monospace"""
        await self._check_and_edit(message, "code")

    @loader.command(
        ru_doc="Сделать текст цитатой",
        en_doc="Make text quote",
    )
    async def q(self, message: Message):
        """Make text quote"""
        await self._check_and_edit(message, "quote")

    @loader.command(
        ru_doc="Сделать текст сворачиваемой цитатой",
        en_doc="Make text expandable quote",
    )
    async def qe(self, message: Message):
        """Make text expandable quote"""
        await self._check_and_edit(message, "quote_expandable")

    @loader.command(
        ru_doc="Параграф H1 (рич)",
        en_doc="Paragraph H1 (rich)",
    )
    async def p1(self, message: Message):
        """Paragraph H1 (rich)"""
        await self._check_and_edit(message, "p1")

    @loader.command(
        ru_doc="Параграф H2 (рич)",
        en_doc="Paragraph H2 (rich)",
    )
    async def p2(self, message: Message):
        """Paragraph H2 (rich)"""
        await self._check_and_edit(message, "p2")

    @loader.command(
        ru_doc="Параграф H3 (рич)",
        en_doc="Paragraph H3 (rich)",
    )
    async def p3(self, message: Message):
        """Paragraph H3 (rich)"""
        await self._check_and_edit(message, "p3")

    @loader.command(
        ru_doc="Параграф H4 (рич)",
        en_doc="Paragraph H4 (rich)",
    )
    async def p4(self, message: Message):
        """Paragraph H4 (rich)"""
        await self._check_and_edit(message, "p4")

    @loader.command(
        ru_doc="Параграф H5 (рич)",
        en_doc="Paragraph H5 (rich)",
    )
    async def p5(self, message: Message):
        """Paragraph H5 (rich)"""
        await self._check_and_edit(message, "p5")

    @loader.command(
        ru_doc="Параграф H6 (рич)",
        en_doc="Paragraph H6 (rich)",
    )
    async def p6(self, message: Message):
        """Paragraph H6 (rich)"""
        await self._check_and_edit(message, "p6")

    @loader.command(
        ru_doc="Блок кода (рич)",
        en_doc="Code block (rich)",
    )
    async def rc(self, message: Message):
        """Code block (rich)"""
        await self._check_and_edit(message, "rc")

    @loader.command(
        ru_doc="Цитата с именем автора (рич)",
        en_doc="Quote with author name (rich)",
    )
    async def rq(self, message: Message):
        """Quote with author name (rich)"""
        await self._check_and_edit(message, "rq")

    @loader.command(
        ru_doc="Цитата с именем автора курсивом (рич)",
        en_doc="Quote with italic author name (rich)",
    )
    async def rqc(self, message: Message):
        """Quote with italic author name (rich)"""
        await self._check_and_edit(message, "rqc")

    @loader.command(
        ru_doc="Ячейка таблицы td (рич)",
        en_doc="Table cell td (rich)",
    )
    async def t(self, message: Message):
        """Table cell td (rich)"""
        await self._check_and_edit(message, "t")

    @loader.command(
        ru_doc="Заголовок таблицы th (рич)",
        en_doc="Table header th (rich)",
    )
    async def tb(self, message: Message):
        """Table header th (rich)"""
        await self._check_and_edit(message, "tb")