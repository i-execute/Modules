__version__ = (1, 0, 0)
# meta developer: I_execute.t.me

from telethon.tl.types import Message
from .. import loader, utils
import asyncio


@loader.tds
class MessageEditor(loader.Module):
    """Edit messages with formatting"""

    strings = {
        "name": "MessageEditor",
        "no_reply": "Use this command in reply to a message",
        "not_owner": "Are you serious?",
        "edit_failed": "Message can no longer be edited",
    }

    strings_ru = {
        "no_reply": "Используй команду в реплай на сообщение",
        "not_owner": "Ты серьезно?",
        "edit_failed": "Сообщение уже нельзя редактировать",
    }

    async def _check_and_edit(self, message: Message, format_func):
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

        text = reply.text or ""
        formatted_text = format_func(text)

        try:
            await reply.edit(formatted_text, parse_mode="HTML")
            await message.delete()
        except Exception:
            msg = await utils.answer(message, self.strings["edit_failed"])
            await asyncio.sleep(5)
            await msg.delete()

    @loader.command(
        ru_doc="Сделать текст жирным",
        en_doc="Make text bold",
    )
    async def b(self, message: Message):
        """Make text bold"""
        await self._check_and_edit(message, lambda text: f"<b>{utils.escape_html(text)}</b>")

    @loader.command(
        ru_doc="Сделать текст кодом",
        en_doc="Make text code",
    )
    async def c(self, message: Message):
        """Make text code"""
        await self._check_and_edit(message, lambda text: f"<pre>{utils.escape_html(text)}</pre>")

    @loader.command(
        ru_doc="Сделать текст моноширинным",
        en_doc="Make text monospace",
    )
    async def m(self, message: Message):
        """Make text monospace"""
        await self._check_and_edit(message, lambda text: f"<code>{utils.escape_html(text)}</code>")

    @loader.command(
        ru_doc="Сделать текст цитатой",
        en_doc="Make text quote",
    )
    async def q(self, message: Message):
        """Make text quote"""
        await self._check_and_edit(message, lambda text: f"<blockquote>{utils.escape_html(text)}</blockquote>")

    @loader.command(
        ru_doc="Сделать текст сворачиваемой цитатой",
        en_doc="Make text expandable quote",
    )
    async def qe(self, message: Message):
        """Make text expandable quote"""
        await self._check_and_edit(message, lambda text: f"<blockquote expandable>{utils.escape_html(text)}</blockquote>")