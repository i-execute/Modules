__version__ = (1, 2, 1)
# meta developer: I_execute.t.me

import re
import time
import logging
import aiohttp

from herokutl import events
from herokutl.tl.types import (
    InputBotInlineResult,
    InputBotInlineMessageMediaInvoice,
    InputBotInlineMessageText,
    InputWebDocument,
    UpdateBotPrecheckoutQuery,
    UpdateNewMessage,
    MessageService,
    MessageActionPaymentSentMe,
    Invoice,
    LabeledPrice,
    DataJSON,
)
from herokutl.tl.functions.messages import SetBotPrecheckoutResultsRequest

from .. import loader, utils

logger = logging.getLogger(__name__)

STARS_PATTERN = re.compile(r'^\s*(\d+)\s*$')


@loader.tds
class DepInvoice(loader.Module):
    """Create Telegram Stars invoices via inline mode"""

    strings = {
        "name": "DepInvoice",
        "no_bot": "<b>[ERR]</b> Inline bot not found. Enable inline mode in config.",
        "refund_usage": "<b>[USAGE]</b> <code>{prefix}refund [user_id] [charge_id]</code>",
        "refund_invalid_args": "<b>[ERR]</b> Invalid arguments. User ID must be a number.",
        "refund_success": "<b>[OK]</b> Refund completed successfully",
        "refund_error": "<b>[ERR]</b> Refund failed: <code>{error}</code>",

        "inline_hint_title": "DepInvoice",
        "inline_hint_desc": "Enter Stars amount (e.g., 100)",
        "inline_hint_msg": "Enter Stars amount",

        "inline_title": "Payment {stars} Stars",
        "inline_desc": "Tap to create invoice",

        "invoice_title_default": "Purchase {stars} Stars",
        "invoice_desc_default": "Payment for {stars} Telegram Stars",

        "log_payment_received": (
            "<blockquote><b>PAYMENT RECEIVED</b></blockquote>\n"
            "<blockquote><b>Amount:</b> {stars} Stars</blockquote>\n"
            "<blockquote><b>User ID:</b> <code>{user_id}</code></blockquote>\n"
            "<blockquote><b>Charge ID:</b> <code>{charge_id}</code></blockquote>\n"
            "<blockquote><b>Provider Charge ID:</b> <code>{provider_charge_id}</code></blockquote>"
        ),
        "log_refund_success": (
            "<blockquote><b>REFUND COMPLETED</b></blockquote>\n"
            "<blockquote><b>User ID:</b> <code>{user_id}</code></blockquote>\n"
            "<blockquote><b>Charge ID:</b> <code>{charge_id}</code></blockquote>"
        ),
        "log_refund_error": (
            "<blockquote><b>REFUND FAILED</b></blockquote>\n"
            "<blockquote><b>User ID:</b> <code>{user_id}</code></blockquote>\n"
            "<blockquote><b>Charge ID:</b> <code>{charge_id}</code></blockquote>\n"
            "<blockquote><b>Error:</b> <code>{error}</code></blockquote>"
        ),
    }

    strings_ru = {**strings}

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue("invoice_title", "", "Invoice title (use {stars} placeholder)", validator=loader.validators.String()),
            loader.ConfigValue("invoice_description", "", "Invoice description (use {stars} placeholder)", validator=loader.validators.String()),
            loader.ConfigValue("invoice_banner", None, "Invoice banner URL (None = no banner)", validator=loader.validators.String()),
            loader.ConfigValue("inline_banner", None, "Inline preview banner URL (None = no preview)", validator=loader.validators.String()),
        )

        self._bot_token = None
        self._client = None
        self._db = None
        self._logger_topic = None
        self._asset_channel = None
        self._payment_handler = None

    async def client_ready(self, client, db):
        self._client = client
        self._db = db

        if hasattr(self, "inline") and hasattr(self.inline, "_bot_client"):
            try:
                self._bot_token = getattr(self.inline, "_token", getattr(self.inline.bot, "token", None))
            except Exception as e:
                logger.error(f"[DepInvoice] Error getting bot token: {e}")

            self._payment_handler = self._raw_payment_handler
            self.inline._bot_client.add_event_handler(
                self._payment_handler,
                events.Raw(UpdateNewMessage),
            )
            logger.info("[DepInvoice] Payment handler registered on bot client")

        self._asset_channel = self._db.get("heroku.forums", "channel_id", None)

        if self._asset_channel:
            try:
                self._logger_topic = await utils.asset_forum_topic(
                    self._client, self._db, self._asset_channel, "DepInvoice",
                    description="Payment and refund logs",
                    icon_emoji_id=5188466187448650036,
                )
            except Exception as e:
                logger.error(f"[DepInvoice] Failed to create/get forum topic: {e}")

    async def on_unload(self):
        if self._payment_handler and hasattr(self, "inline") and hasattr(self.inline, "_bot_client"):
            try:
                self.inline._bot_client.remove_event_handler(self._payment_handler)
                logger.info("[DepInvoice] Payment handler removed from bot client")
            except Exception as e:
                logger.error(f"[DepInvoice] Error removing payment handler: {e}")

    async def _raw_payment_handler(self, update):
        msg = getattr(update, "message", None)
        if not isinstance(msg, MessageService):
            return
        if not isinstance(getattr(msg, "action", None), MessageActionPaymentSentMe):
            return
        try:
            action = msg.action
            stars = action.total_amount
            charge_id = action.charge.id if action.charge else "unknown"
            provider_charge_id = action.charge.provider_charge_id if action.charge else "unknown"
            user_id = getattr(getattr(msg, "peer_id", None), "user_id", None)

            await self._send_log(self.strings["log_payment_received"].format(
                stars=stars,
                user_id=user_id,
                charge_id=charge_id,
                provider_charge_id=provider_charge_id,
            ))
            logger.info(f"[DepInvoice] Payment: {stars} stars from {user_id}, charge_id={charge_id}")
        except Exception as e:
            logger.error(f"[DepInvoice] Payment handler error: {e}", exc_info=True)

    @loader.need_update("pre_checkout_query")
    async def _on_precheckout(self, update: UpdateBotPrecheckoutQuery):
        try:
            await self.inline._bot_client(
                SetBotPrecheckoutResultsRequest(query_id=update.query_id, success=True, error=None)
            )
        except Exception as e:
            logger.error(f"[DepInvoice] Pre-checkout error: {e}", exc_info=True)

    async def _send_log(self, text: str):
        if not self._logger_topic or not self._asset_channel:
            return
        try:
            await self.inline.bot.send_message(
                int(f"-100{self._asset_channel}"),
                text,
                disable_web_page_preview=True,
                parse_mode="HTML",
                message_thread_id=self._logger_topic.id,
            )
        except Exception as e:
            logger.error(f"[DepInvoice] Failed to send log: {e}", exc_info=True)

    async def _refund_via_api(self, user_id: int, charge_id: str) -> tuple:
        if not self._bot_token:
            return False, "Bot token not found"
        url = f"https://api.telegram.org/bot{self._bot_token}/refundStarPayment"
        payload = {"user_id": user_id, "telegram_payment_charge_id": charge_id}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    data = await resp.json()
                    if data.get("ok"):
                        return True, None
                    return False, data.get("description", "Unknown error")
        except Exception as e:
            return False, str(e)

    @loader.command(ru_doc="[user_id] [charge_id] - Вернуть Stars пользователю", en_doc="[user_id] [charge_id] - Refund Stars to user")
    async def refund(self, message):
        if not self._bot_token:
            await utils.answer(message, self.strings["no_bot"])
            return
        args = utils.get_args_raw(message).strip().split()
        if len(args) != 2:
            await utils.answer(message, self.strings["refund_usage"].format(prefix=utils.escape_html(self.get_prefix())))
            return
        try:
            user_id = int(args[0])
        except ValueError:
            await utils.answer(message, self.strings["refund_invalid_args"])
            return
        charge_id = args[1].strip()
        status_msg = await utils.answer(message, "<b>[WAIT]</b> Processing refund...")
        success, error = await self._refund_via_api(user_id, charge_id)
        if success:
            await utils.answer(status_msg, self.strings["refund_success"])
            await self._send_log(self.strings["log_refund_success"].format(user_id=user_id, charge_id=charge_id))
        else:
            await utils.answer(status_msg, self.strings["refund_error"].format(error=utils.escape_html(str(error))))
            await self._send_log(self.strings["log_refund_error"].format(
                user_id=user_id, charge_id=charge_id, error=utils.escape_html(str(error))
            ))

    def _get_invoice_text(self, field: str, stars: int) -> str:
        custom = self.config[f"invoice_{field}"]
        template = custom if custom else self.strings[f"invoice_{field}_default"]
        try:
            return template.format(stars=stars)
        except Exception:
            return template

    def _make_web_document(self, url: str, mime_type: str = "image/jpeg"):
        if not url:
            return None
        return InputWebDocument(url=url, size=0, mime_type=mime_type, attributes=[])

    @loader.inline_handler(ru_doc="Создание инвойса на Stars", en_doc="Create Stars invoice")
    async def dep_inline_handler(self, query):
        text = query.query.strip()
        if text.lower().startswith("dep"):
            text = text[3:].strip()
        if not text:
            await self._answer_hint(query)
            return
        match = STARS_PATTERN.match(text)
        if not match:
            await self._answer_hint(query)
            return
        stars = int(match.group(1))
        if stars <= 0 or stars > 100000:
            await self._answer_hint(query)
            return
        await self._answer_invoice(query, stars)

    async def _answer_hint(self, query):
        try:
            thumb = self._make_web_document(self.config["inline_banner"])
            result = InputBotInlineResult(
                id=f"hint_{int(time.time())}",
                type="article",
                title=self.strings["inline_hint_title"],
                description=self.strings["inline_hint_desc"],
                thumb=thumb,
                send_message=InputBotInlineMessageText(message=self.strings["inline_hint_msg"], no_webpage=True),
            )
            await query.answer(results=[result], cache_time=0, private=True)
        except Exception as e:
            logger.error(f"[DepInvoice] Hint error: {e}", exc_info=True)

    async def _answer_invoice(self, query, stars: int):
        try:
            title = self._get_invoice_text("title", stars)
            description = self._get_invoice_text("description", stars)
            photo = self._make_web_document(self.config["invoice_banner"])
            thumb = self._make_web_document(self.config["inline_banner"])

            invoice = Invoice(
                currency="XTR",
                prices=[LabeledPrice(label=f"{stars} Stars", amount=stars)],
                test=False,
            )

            result = InputBotInlineResult(
                id=f"inv_{stars}_{int(time.time())}",
                type="article",
                title=self.strings["inline_title"].format(stars=stars),
                description=self.strings["inline_desc"],
                thumb=thumb,
                send_message=InputBotInlineMessageMediaInvoice(
                    title=title,
                    description=description,
                    photo=photo,
                    invoice=invoice,
                    payload=f"dep_{stars}_{int(time.time())}".encode("UTF-8"),
                    provider="",
                    provider_data=DataJSON(data="{}"),
                ),
            )
            await query.answer(results=[result], cache_time=0, private=True)
        except Exception as e:
            logger.error(f"[DepInvoice] Invoice error: {e}", exc_info=True)