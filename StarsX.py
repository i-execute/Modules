__version__ = (1, 3, 5)
# meta developer: FireJester.t.me

import os
import re
import io
import time
import logging
import tempfile
import shutil
import secrets
import hashlib
import aiohttp
import asyncio

from telethon import TelegramClient, events, functions
from telethon.sessions import StringSession
from telethon.tl.types import (
    Message,
    InputPeerSelf,
    TextWithEntities,
    DataJSON,
    InputMediaInvoice,
    InputWebDocument,
    Invoice,
    LabeledPrice as TLLabeledPrice,
    UpdateBotPrecheckoutQuery,
    InputStickerSetID,
    InputStickerSetEmpty,
    DocumentAttributeFilename,
    DocumentAttributeCustomEmoji,
    DocumentAttributeImageSize,
    DocumentAttributeSticker,
)
from telethon.tl.functions.payments import (
    GetStarsStatusRequest,
    GetPaymentFormRequest,
    SendStarsFormRequest,
    GetStarGiftsRequest,
    GetSavedStarGiftsRequest,
    GetUniqueStarGiftRequest,
    ExportInvoiceRequest,
    GetStarGiftUpgradePreviewRequest,
    GetResaleStarGiftsRequest,
)
from telethon.tl.functions.messages import (
    SetBotPrecheckoutResultsRequest,
    GetStickerSetRequest,
)
from telethon.tl.types import InputInvoiceStarGift
from telethon.errors import BadRequestError, RPCError

from aiogram import Bot, Router
from aiogram.types import (
    Message as AiogramMessage,
    InlineQuery,
    InlineQueryResultArticle,
    InputInvoiceMessageContent,
    LabeledPrice,
    Update,
)

from .. import loader, utils

logger = logging.getLogger(__name__)

CHUNK_SIZE_LIST = 30
CHUNK_SIZE_COLLECTIONS = 30
CHUNK_SIZE_USER_GIFTS = 10
MAX_ACCOUNTS = 10

PREMIUM_COSTS = {3: 1000, 6: 1500, 12: 2500}

GIFT_ID_PATTERN = re.compile(r'^\d{19}$')
BOT_TOKEN_PATTERN = re.compile(r'\b\d{8,10}:[A-Za-z0-9_-]{35}\b')
STRING_SESSION_PATTERN = re.compile(r'1[A-Za-z0-9_-]{200,}={0,2}')
RESOLUTION_PATTERN = re.compile(r'(\d+)\s*[xX\u0425\u0445*]\s*(\d+)')
NFT_LINK_PATTERN = re.compile(r'(?:https?://t\.me/nft/)?([A-Za-z][\w-]+-\d+)$')
COLLECTION_FROM_SLUG = re.compile(r'^([A-Za-z][\w-]+?)-(\d+)$')

_MODULE_INSTANCE = None


def escape_html(text):
    if not text:
        return ""
    return str(text).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')


def get_full_name(user):
    if not user:
        return "Unknown"
    first = getattr(user, 'first_name', '') or ''
    last = getattr(user, 'last_name', '') or ''
    return f"{first} {last}".strip() or "Unknown"


def get_username(entity):
    if hasattr(entity, "username") and entity.username:
        return entity.username
    if hasattr(entity, "usernames") and entity.usernames:
        for u in entity.usernames:
            if getattr(u, "active", False):
                return u.username
        return entity.usernames[0].username
    return None


def _is_custom_emoji_doc(doc):
    for attr in getattr(doc, 'attributes', []):
        if isinstance(attr, DocumentAttributeCustomEmoji):
            return True
    return False


def generate_star_invoice(
    stars_amount, title, description, payload, start_parameter,
    photo_url=None, photo_width=512, photo_height=512,
):
    params = {
        "title": title, "description": description, "payload": payload,
        "provider_token": "", "currency": "XTR",
        "prices": [{"label": f"{stars_amount} Stars", "amount": stars_amount}],
        "start_parameter": start_parameter, "is_flexible": False,
    }
    if photo_url:
        params["photo_url"] = photo_url
        params["photo_width"] = photo_width
        params["photo_height"] = photo_height
    return params


@loader.tds
class StarsX(loader.Module):
    """Telegram Stars, gifts and invoice management"""

    strings = {
        "name": "StarsX",
    }

    strings_en = {
        "no_inline_bot": "Inline-bot not found. Enable it in config.",
        "invalid_args": "Invalid arguments.",
        "error": "Error: {error}",
        "success": "Success!",
        "loading": "Loading...",
        "not_found": "Not found.",

        "show_help": (
            "<b>Show Command</b>\n\n"
            "<code>{prefix}show [ID/index]</code> - send gift sticker\n"
            "<code>{prefix}show [NFT link]</code> - send NFT gift sticker\n"
            "<code>{prefix}show list user</code> - gifts from user account\n"
            "<code>{prefix}show list bot</code> - gifts from inline bot\n"
            "<code>{prefix}show list [N]</code> - gifts from account N\n"
            "<code>{prefix}show get [username/ID]</code> - saved gifts of a user\n"
            "<code>{prefix}show get sticker [username/ID]</code> - saved gifts as stickers\n"
            "<code>{prefix}show collections</code> - list NFT collections\n"
            "<code>{prefix}show all [collection/NFT link]</code> - ordinary model stickers\n"
            "<code>{prefix}show all ord [collection/NFT link]</code> - ordinary model stickers\n"
            "<code>{prefix}show all craft [collection/NFT link]</code> - crafted model stickers\n"
            "<code>{prefix}show stop</code> - stop sending stickers"
        ),
        "show_checking": "Searching gift info...",
        "show_invalid_arg": "Invalid argument. Use index (0-{max}) or 19-digit ID.",
        "show_not_found": "Gift not found.",
        "show_no_gifts": "No gifts available.",
        "show_download_error": "Sticker download error: {error}",
        "show_list_header": "<b>Available gifts ({count}):</b> ({source})",
        "show_list_chunk": "<b>Part {current}/{total}</b>\n\n",
        "show_list_item": "Index: <code>{index}</code>, ID: <code>{id}</code>, Price: {stars}",
        "show_list_bot_item": "#{index} - ID: <code>{id}</code>, {stars}{limited}",
        "show_list_empty": "No gifts available.",
        "show_list_loading": "Loading gifts list...",
        "show_get_loading": "Loading user gifts...",
        "show_get_header": "<b>Gifts of {user} ({count}):</b>",
        "show_get_chunk": "<b>Part {current}/{total}</b>\n\n",
        "show_get_item": "#{index} - ID: <code>{id}</code>",
        "show_get_item_unique": "#{index} - ID: <code>{id}</code> (NFT: {title})",
        "show_get_empty": "User has no saved gifts.",
        "show_get_sticker_loading": "Loading user gifts as stickers...",
        "show_get_sticker_done": "Sent {sent} unique gift stickers.",
        "show_hidden_gift_info": (
            "<b>Hidden gift</b> (not in public catalog)\n"
            "ID: <code>{id}</code>\n"
            "Price: {stars}"
        ),
        "show_nft_gift_info": (
            "<b>NFT Gift:</b> {title}\n"
            "Slug: <code>{slug}</code>\n"
            "Model: {model}\n"
            "Pattern: {pattern}\n"
            "Backdrop: {backdrop}"
        ),
        "show_nft_loading": "Loading NFT gift...",
        "show_nft_not_found": "NFT gift not found or invalid slug.",
        "show_collections_loading": "Loading NFT collections...",
        "show_collections_header": "<b>NFT Collections ({count}):</b>",
        "show_collections_item": "<code>{title}</code> - {stars} + {upgrade} upgrade",
        "show_collections_empty": "No NFT collections found.",
        "show_all_loading": "Loading collection {name}...",
        "show_all_discovering_crafted": "Discovering crafted sticker sets for {name}...",
        "show_all_info": (
            "<b>{title}</b> [{mode}]\n"
            "Stickers: {count}\n\n"
            "Sending..."
        ),
        "show_all_info_crafted_multi": (
            "<b>{title}</b> [crafted]\n"
            "Sets: {sets_count}\n"
            "Total stickers: {count}\n\n"
            "Sending..."
        ),
        "show_all_done": "<b>{title}</b> [{mode}] - sent {sent}/{total}",
        "show_all_not_found": "Collection <code>{name}</code> not found. Use <code>{prefix}show collections</code>",
        "show_all_no_set": "Could not find sticker set for this collection.",
        "show_all_no_crafted": "No crafted sticker sets found for <b>{name}</b>.",
        "show_all_stopped": "Stopped. Sent {sent}/{total}.",
        "show_stop_done": "Stopped.",
        "show_stop_nothing": "Nothing to stop.",

        "stars_help": (
            "<b>Stars Command</b>\n\n"
            "<code>{prefix}stars balance user</code> - your star balance\n"
            "<code>{prefix}stars balance bot</code> - inline bot balance\n"
            "<code>{prefix}stars balance [N]</code> - account N balance\n"
            "<code>{prefix}stars invoice resolution [W]x[H]</code> - set resolution\n"
            "<code>{prefix}stars invoice photo [URL]</code> - set photo URL\n"
            "<code>{prefix}stars invoice text [1/2] [text]</code> - set invoice text"
        ),
        "stars_balance_loading": "Getting balance...",
        "stars_balance_result": "<b>Balance [{source}]:</b> {amount}",
        "stars_balance_error": "Balance error: {error}",
        "stars_invoice_res_set": "<b>Resolution set:</b> {width}x{height}",
        "stars_invoice_res_invalid": "Invalid format. Use: {prefix}stars invoice resolution 512x512",
        "stars_invoice_photo_set": "<b>Photo URL set:</b> {url}",
        "stars_invoice_photo_reset": "Photo URL reset.",
        "stars_invoice_photo_invalid": "Specify URL: {prefix}stars invoice photo https://...",
        "stars_invoice_text_set": "<b>Text #{num} set:</b> {text}",
        "stars_invoice_text_reset": "Text #{num} reset.",
        "stars_invoice_text_invalid": "Invalid format. Use: {prefix}stars invoice text [1/2] [text]",

        "gift_help": (
            "<b>Gift Command</b>\n\n"
            "<code>{prefix}gift from [user/bot/N] to [user/channel] [target] [gift] [count] [comment]</code>\n\n"
            "Sources: user, bot, N (account number)\n"
            "Gift: index (0-max), 19-digit ID, prem_3, prem_6, prem_12\n"
            "Count: number of gifts to send\n\n"
            "Gift IDs: https://t.me/FIRE_GIFT_ID"
        ),
        "gift_sending": "Sending gift {current}/{total}...\nMode: {mode}",
        "gift_sent": (
            "<b>Gift sent!</b>\n\n"
            "From: {from_info}\n"
            "To: <code>{target}</code> ({to_type})\n"
            "Gift: {gift_info}\n"
            "Count: {count}\n"
            "Comment: {comment}"
        ),
        "gift_partial": (
            "<b>Partially sent!</b>\n\n"
            "Success: {success}/{total}\n"
            "Errors: {errors}"
        ),
        "gift_error": "Gift error: {error}",
        "gift_balance_low": "Not enough stars!",
        "gift_invalid_gift": (
            "Invalid gift. Use index (0-{max}), 19-digit ID, or prem_3/prem_6/prem_12.\n"
            "Gift IDs: https://t.me/FIRE_GIFT_ID"
        ),
        "gift_invalid_count": "Invalid count!",
        "gift_usage": (
            "Invalid format. See {prefix}gift for help.\n"
            "Gift IDs: https://t.me/FIRE_GIFT_ID"
        ),
        "gift_account_not_found": "Account [{num}] not found.",
        "gift_premium_only_bot": "Premium gifts can only be sent from bots.",
        "gift_premium_sending": "Sending Premium {months}m ({stars}) {current}/{total}...",

        "starsx_help": (
            "<b>StarsX Account Management</b>\n\n"
            "<code>{prefix}starsx add [session/token]</code> - add account (reply or args)\n"
            "<code>{prefix}starsx remove [N]</code> - remove account N\n"
            "<code>{prefix}starsx remove -force</code> - remove all accounts\n"
            "<code>{prefix}starsx list</code> - list all accounts\n"
            "<code>{prefix}starsx info [N]</code> - account N info\n\n"
            "Max accounts: {max}"
        ),
        "starsx_testing": "Testing credentials...",
        "starsx_added": (
            "<b>Account added!</b>\n\n"
            "Number: <b>[{num}]</b>\n"
            "Type: {type}\n"
            "Name: {name}\n"
            "ID: <code>{id}</code>\n"
            "Username: {username}"
        ),
        "starsx_exists": "Account already added as [{num}].",
        "starsx_invalid": "Invalid session/token format.",
        "starsx_no_creds": "No session or token provided.",
        "starsx_max_reached": "Maximum accounts ({max}) reached!",
        "starsx_not_authorized": "Session/token not authorized.",
        "starsx_removed": "Account [{num}] removed.",
        "starsx_removed_all": "All accounts removed.",
        "starsx_not_found": "Account [{num}] not found.",
        "starsx_list_header": "<b>Accounts ({count}/{max}):</b>\n\n",
        "starsx_list_item": "[{num}] {type} | {name} | <code>{id}</code>",
        "starsx_list_empty": "No accounts added.",
        "starsx_info": (
            "<blockquote><b>Account [{num}]</b>\n\n"
            "Type: {type}\n"
            "Name: {name}\n"
            "ID: <code>{id}</code>\n"
            "Username: {username}</blockquote>"
        ),

        "refund_help": (
            "<b>Refund Command</b>\n\n"
            "<code>{prefix}refund bot [user_id] [charge_id]</code> - refund via inline bot\n"
            "<code>{prefix}refund [N] [user_id] [charge_id]</code> - refund via account N"
        ),
        "refund_success": "<b>Refund complete!</b>\nUser: <code>{user_id}</code>\nCharge: <code>{charge_id}</code>",
        "refund_error": "Refund error: {error}",
        "refund_not_a_bot": "Account [{num}] is not a bot!",

        "link_help": (
            "<b>Link Command</b>\n\n"
            "<code>{prefix}link for [all/ID/username] [gift] [count] [comment]</code> - gift link\n"
            "<code>{prefix}link for [N] [amount]</code> - invoice link for bot N"
        ),
        "link_invoice_created": "<b>Invoice link created!</b>\n\nBot: [{num}]\nAmount: {amount}",
        "link_not_enough_stars": "Not enough stars! Need: {need}, Have: {have}",
        "link_invalid_gift": "Invalid gift!",
        "link_not_a_bot": "Account [{num}] is not a bot!",
        "link_gift_anchor": "Gift",
        "link_error": "Link error: {error}",
        "link_bot_session_missing": "Bot [{num}] session not active. Re-add the bot.",

        "bot_start": "Bot ready! Use inline mode for invoices.",
        "bot_gift_claimed": "Gift claimed! Sending...",
        "bot_gift_sent": "Gift sent successfully!",
        "bot_gift_error": "Gift error: {error}",
        "bot_gift_not_for_you": "This gift is not for you!",
        "bot_gift_already_claimed": "This gift was already claimed!",
        "bot_gift_expired": "This gift link has expired!",
        "bot_refund_success": "<b>Refund complete!</b>\nUser: <code>{user_id}</code>\nCharge: <code>{charge_id}</code>",
        "bot_refund_error": "Refund error: {error}",
        "bot_invoice_sent": "Invoice for {amount} sent!",

        "inline_title": "Payment {stars}",
        "inline_description": "Click to create invoice",
        "invoice_title_default": "Purchase {stars} Stars",
        "invoice_desc_default": "Payment for {stars} Telegram Stars",
    }

    strings_ru = {
        "no_inline_bot": "Инлайн-бот не найден. Включите его в конфиге.",
        "invalid_args": "Неверные аргументы.",
        "error": "Ошибка: {error}",
        "success": "Успешно!",
        "loading": "Загрузка...",
        "not_found": "Не найдено.",

        "show_help": (
            "<b>Команда Show</b>\n\n"
            "<code>{prefix}show [ID/индекс]</code> - отправить стикер подарка\n"
            "<code>{prefix}show [NFT ссылка]</code> - отправить стикер NFT подарка\n"
            "<code>{prefix}show list user</code> - подарки из аккаунта пользователя\n"
            "<code>{prefix}show list bot</code> - подарки из инлайн бота\n"
            "<code>{prefix}show list [N]</code> - подарки из аккаунта N\n"
            "<code>{prefix}show get [username/ID]</code> - сохранённые подарки пользователя\n"
            "<code>{prefix}show get sticker [username/ID]</code> - сохранённые подарки как стикеры\n"
            "<code>{prefix}show collections</code> - список NFT коллекций\n"
            "<code>{prefix}show all [коллекция/NFT ссылка]</code> - обычные стикеры моделей\n"
            "<code>{prefix}show all ord [коллекция/NFT ссылка]</code> - обычные стикеры моделей\n"
            "<code>{prefix}show all craft [коллекция/NFT ссылка]</code> - крафтовые стикеры моделей\n"
            "<code>{prefix}show stop</code> - остановить отправку стикеров"
        ),
        "show_checking": "Поиск информации о подарке...",
        "show_invalid_arg": "Неверный аргумент. Используйте индекс (0-{max}) или 19-значный ID.",
        "show_not_found": "Подарок не найден.",
        "show_no_gifts": "Нет доступных подарков.",
        "show_download_error": "Ошибка загрузки стикера: {error}",
        "show_list_header": "<b>Доступные подарки ({count}):</b> ({source})",
        "show_list_chunk": "<b>Часть {current}/{total}</b>\n\n",
        "show_list_item": "Индекс: <code>{index}</code>, ID: <code>{id}</code>, Цена: {stars}",
        "show_list_bot_item": "#{index} - ID: <code>{id}</code>, {stars}{limited}",
        "show_list_empty": "Нет доступных подарков.",
        "show_list_loading": "Загрузка списка подарков...",
        "show_get_loading": "Загрузка подарков пользователя...",
        "show_get_header": "<b>Подарки {user} ({count}):</b>",
        "show_get_chunk": "<b>Часть {current}/{total}</b>\n\n",
        "show_get_item": "#{index} - ID: <code>{id}</code>",
        "show_get_item_unique": "#{index} - ID: <code>{id}</code> (NFT: {title})",
        "show_get_empty": "У пользователя нет сохранённых подарков.",
        "show_get_sticker_loading": "Загрузка подарков пользователя как стикеров...",
        "show_get_sticker_done": "Отправлено {sent} уникальных стикеров подарков.",
        "show_hidden_gift_info": (
            "<b>Скрытый подарок</b> (не в публичном каталоге)\n"
            "ID: <code>{id}</code>\n"
            "Цена: {stars}"
        ),
        "show_nft_gift_info": (
            "<b>NFT Подарок:</b> {title}\n"
            "Slug: <code>{slug}</code>\n"
            "Модель: {model}\n"
            "Паттерн: {pattern}\n"
            "Фон: {backdrop}"
        ),
        "show_nft_loading": "Загрузка NFT подарка...",
        "show_nft_not_found": "NFT подарок не найден или неверный slug.",
        "show_collections_loading": "Загрузка NFT коллекций...",
        "show_collections_header": "<b>NFT Коллекции ({count}):</b>",
        "show_collections_item": "<code>{title}</code> - {stars} + {upgrade} апгрейд",
        "show_collections_empty": "NFT коллекции не найдены.",
        "show_all_loading": "Загрузка коллекции {name}...",
        "show_all_discovering_crafted": "Поиск крафтовых стикерсетов для {name}...",
        "show_all_info": (
            "<b>{title}</b> [{mode}]\n"
            "Стикеров: {count}\n\n"
            "Отправка..."
        ),
        "show_all_info_crafted_multi": (
            "<b>{title}</b> [crafted]\n"
            "Сетов: {sets_count}\n"
            "Всего стикеров: {count}\n\n"
            "Отправка..."
        ),
        "show_all_done": "<b>{title}</b> [{mode}] - отправлено {sent}/{total}",
        "show_all_not_found": "Коллекция <code>{name}</code> не найдена. Используйте <code>{prefix}show collections</code>",
        "show_all_no_set": "Не удалось найти набор стикеров для этой коллекции.",
        "show_all_no_crafted": "Крафтовые стикерсеты для <b>{name}</b> не найдены.",
        "show_all_stopped": "Остановлено. Отправлено {sent}/{total}.",
        "show_stop_done": "Остановлено.",
        "show_stop_nothing": "Нечего останавливать.",

        "stars_help": (
            "<b>Команда Stars</b>\n\n"
            "<code>{prefix}stars balance user</code> - ваш баланс звёзд\n"
            "<code>{prefix}stars balance bot</code> - баланс инлайн бота\n"
            "<code>{prefix}stars balance [N]</code> - баланс аккаунта N\n"
            "<code>{prefix}stars invoice resolution [W]x[H]</code> - установить разрешение\n"
            "<code>{prefix}stars invoice photo [URL]</code> - установить URL фото\n"
            "<code>{prefix}stars invoice text [1/2] [текст]</code> - установить текст инвойса"
        ),
        "stars_balance_loading": "Получение баланса...",
        "stars_balance_result": "<b>Баланс [{source}]:</b> {amount}",
        "stars_balance_error": "Ошибка баланса: {error}",
        "stars_invoice_res_set": "<b>Разрешение установлено:</b> {width}x{height}",
        "stars_invoice_res_invalid": "Неверный формат. Используйте: {prefix}stars invoice resolution 512x512",
        "stars_invoice_photo_set": "<b>URL фото установлен:</b> {url}",
        "stars_invoice_photo_reset": "URL фото сброшен.",
        "stars_invoice_photo_invalid": "Укажите URL: {prefix}stars invoice photo https://...",
        "stars_invoice_text_set": "<b>Текст #{num} установлен:</b> {text}",
        "stars_invoice_text_reset": "Текст #{num} сброшен.",
        "stars_invoice_text_invalid": "Неверный формат. Используйте: {prefix}stars invoice text [1/2] [текст]",

        "gift_help": (
            "<b>Команда Gift</b>\n\n"
            "<code>{prefix}gift from [user/bot/N] to [user/channel] [цель] [подарок] [кол-во] [комментарий]</code>\n\n"
            "Источники: user, bot, N (номер аккаунта)\n"
            "Подарок: индекс (0-max), 19-значный ID, prem_3, prem_6, prem_12\n"
            "Кол-во: количество подарков\n\n"
            "ID подарков: https://t.me/FIRE_GIFT_ID"
        ),
        "gift_sending": "Отправка подарка {current}/{total}...\nРежим: {mode}",
        "gift_sent": (
            "<b>Подарок отправлен!</b>\n\n"
            "От: {from_info}\n"
            "Кому: <code>{target}</code> ({to_type})\n"
            "Подарок: {gift_info}\n"
            "Кол-во: {count}\n"
            "Комментарий: {comment}"
        ),
        "gift_partial": (
            "<b>Частично отправлено!</b>\n\n"
            "Успешно: {success}/{total}\n"
            "Ошибки: {errors}"
        ),
        "gift_error": "Ошибка подарка: {error}",
        "gift_balance_low": "Недостаточно звёзд!",
        "gift_invalid_gift": (
            "Неверный подарок. Используйте индекс (0-{max}), 19-значный ID, или prem_3/prem_6/prem_12.\n"
            "ID подарков: https://t.me/FIRE_GIFT_ID"
        ),
        "gift_invalid_count": "Неверное количество!",
        "gift_usage": (
            "Неверный формат. См. {prefix}gift для помощи.\n"
            "ID подарков: https://t.me/FIRE_GIFT_ID"
        ),
        "gift_account_not_found": "Аккаунт [{num}] не найден.",
        "gift_premium_only_bot": "Премиум подарки можно отправлять только через ботов.",
        "gift_premium_sending": "Отправка Premium {months}м ({stars}) {current}/{total}...",

        "starsx_help": (
            "<b>Управление аккаунтами StarsX</b>\n\n"
            "<code>{prefix}starsx add [сессия/токен]</code> - добавить аккаунт (реплай или аргументы)\n"
            "<code>{prefix}starsx remove [N]</code> - удалить аккаунт N\n"
            "<code>{prefix}starsx remove -force</code> - удалить все аккаунты\n"
            "<code>{prefix}starsx list</code> - список всех аккаунтов\n"
            "<code>{prefix}starsx info [N]</code> - информация об аккаунте N\n\n"
            "Макс. аккаунтов: {max}"
        ),
        "starsx_testing": "Проверка учётных данных...",
        "starsx_added": (
            "<b>Аккаунт добавлен!</b>\n\n"
            "Номер: <b>[{num}]</b>\n"
            "Тип: {type}\n"
            "Имя: {name}\n"
            "ID: <code>{id}</code>\n"
            "Username: {username}"
        ),
        "starsx_exists": "Аккаунт уже добавлен как [{num}].",
        "starsx_invalid": "Неверный формат сессии/токена.",
        "starsx_no_creds": "Не указана сессия или токен.",
        "starsx_max_reached": "Достигнут максимум аккаунтов ({max})!",
        "starsx_not_authorized": "Сессия/токен не авторизованы.",
        "starsx_removed": "Аккаунт [{num}] удалён.",
        "starsx_removed_all": "Все аккаунты удалены.",
        "starsx_not_found": "Аккаунт [{num}] не найден.",
        "starsx_list_header": "<b>Аккаунты ({count}/{max}):</b>\n\n",
        "starsx_list_item": "[{num}] {type} | {name} | <code>{id}</code>",
        "starsx_list_empty": "Нет добавленных аккаунтов.",
        "starsx_info": (
            "<blockquote><b>Аккаунт [{num}]</b>\n\n"
            "Тип: {type}\n"
            "Имя: {name}\n"
            "ID: <code>{id}</code>\n"
            "Username: {username}</blockquote>"
        ),

        "refund_help": (
            "<b>Команда Refund</b>\n\n"
            "<code>{prefix}refund bot [user_id] [charge_id]</code> - возврат через инлайн бота\n"
            "<code>{prefix}refund [N] [user_id] [charge_id]</code> - возврат через аккаунт N"
        ),
        "refund_success": "<b>Возврат выполнен!</b>\nПользователь: <code>{user_id}</code>\nCharge: <code>{charge_id}</code>",
        "refund_error": "Ошибка возврата: {error}",
        "refund_not_a_bot": "Аккаунт [{num}] не является ботом!",

        "link_help": (
            "<b>Команда Link</b>\n\n"
            "<code>{prefix}link for [all/ID/username] [подарок] [кол-во] [комментарий]</code> - ссылка на подарок\n"
            "<code>{prefix}link for [N] [сумма]</code> - ссылка на инвойс для бота N"
        ),
        "link_invoice_created": "<b>Ссылка на инвойс создана!</b>\n\nБот: [{num}]\nСумма: {amount}",
        "link_not_enough_stars": "Недостаточно звёзд! Нужно: {need}, Есть: {have}",
        "link_invalid_gift": "Неверный подарок!",
        "link_not_a_bot": "Аккаунт [{num}] не является ботом!",
        "link_gift_anchor": "Подарок",
        "link_error": "Ошибка ссылки: {error}",
        "link_bot_session_missing": "Сессия бота [{num}] не активна. Добавьте бота заново.",

        "bot_start": "Бот готов! Используйте инлайн режим для инвойсов.",
        "bot_gift_claimed": "Подарок получен! Отправка...",
        "bot_gift_sent": "Подарок успешно отправлен!",
        "bot_gift_error": "Ошибка подарка: {error}",
        "bot_gift_not_for_you": "Этот подарок не для вас!",
        "bot_gift_already_claimed": "Этот подарок уже был получен!",
        "bot_gift_expired": "Срок действия этой ссылки истёк!",
        "bot_refund_success": "<b>Возврат выполнен!</b>\nПользователь: <code>{user_id}</code>\nCharge: <code>{charge_id}</code>",
        "bot_refund_error": "Ошибка возврата: {error}",
        "bot_invoice_sent": "Инвойс на {amount} отправлен!",

        "inline_title": "Оплата {stars}",
        "inline_description": "Нажмите для создания инвойса",
        "invoice_title_default": "Покупка {stars} Stars",
        "invoice_desc_default": "Оплата {stars} Telegram Stars",
    }

    def __init__(self):
        super().__init__()
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "invoice_photo", "",
                "Invoice photo URL",
                validator=loader.validators.String(),
            ),
            loader.ConfigValue(
                "invoice_res_w", 512,
                "Invoice photo width",
                validator=loader.validators.Integer(minimum=1),
            ),
            loader.ConfigValue(
                "invoice_res_h", 512,
                "Invoice photo height",
                validator=loader.validators.Integer(minimum=1),
            ),
            loader.ConfigValue(
                "invoice_text_1", "",
                "Invoice title template ({stars} placeholder)",
                validator=loader.validators.String(),
            ),
            loader.ConfigValue(
                "invoice_text_2", "",
                "Invoice description template ({stars} placeholder)",
                validator=loader.validators.String(),
            ),
        )

        self.inline_bot = None
        self.inline_bot_username = None
        self._owner_id = None
        self._router = None
        self._aiogram_bot = None

        self._accounts = {}
        self._next_num = 1
        self._bot_clients = {}
        self._bot_tasks = {}
        self._aiogram_bots = {}

        self._gifts_cache = None
        self._gifts_cache_time = 0
        self._max_gift_index = 0

        self._collections_cache = {}
        self._collections_name_map = {}

        self._pending_gifts = {}
        self._temp_dir = None

        self._show_stop = False

    async def client_ready(self, client, db):
        global _MODULE_INSTANCE
        _MODULE_INSTANCE = self
        self._client = client
        self._db = db

        me = await client.get_me()
        self._owner_id = me.id

        self._temp_dir = os.path.join(tempfile.gettempdir(), f"StarsX_{self._owner_id}")
        os.makedirs(self._temp_dir, exist_ok=True)

        self._pending_gifts = self._db.get("StarsX", "pending_gifts", {})
        await self._load_accounts_from_db()

        if hasattr(self, "inline") and hasattr(self.inline, "bot"):
            self.inline_bot = self.inline.bot
            try:
                bot_info = await self.inline_bot.get_me()
                self.inline_bot_username = bot_info.username
            except Exception:
                pass

            try:
                from aiogram.client.bot import DefaultBotProperties
                from aiogram.enums import ParseMode
                self._aiogram_bot = Bot(
                    self.inline_bot.token,
                    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
                )
            except Exception:
                self._aiogram_bot = None

            await self._unpatch_handlers()
            await self._setup_payment_handler()

    async def _load_accounts_from_db(self):
        self._accounts = {}
        self._next_num = 1

        for item in (self._db.get("StarsX", "sessions_data", []) or []):
            if not isinstance(item, str):
                continue
            try:
                parts = item.split("|", 3)
                if len(parts) < 4:
                    continue
                user_id, name = int(parts[0]), parts[1]
                username = parts[2] if parts[2] != "None" else None
                credential = parts[3]
                num = self._next_num
                self._next_num += 1
                client = TelegramClient(
                    StringSession(credential),
                    api_id=self._client.api_id, api_hash=self._client.api_hash,
                )
                await client.connect()
                if not await client.is_user_authorized():
                    await client.disconnect()
                    continue
                self._accounts[num] = {
                    "type": "user", "credential": credential,
                    "user_id": user_id, "name": name,
                    "username": username, "client": client,
                }
            except Exception:
                pass

        for item in (self._db.get("StarsX", "bot_tokens_data", []) or []):
            if not isinstance(item, str):
                continue
            try:
                parts = item.split("|", 3)
                if len(parts) < 4:
                    continue
                user_id, name = int(parts[0]), parts[1]
                username = parts[2] if parts[2] != "None" else None
                credential = parts[3]
                num = self._next_num
                self._next_num += 1
                await self._start_bot_session(num, credential, user_id, name, username)
            except Exception:
                pass

    async def _start_bot_session(self, num, credential, user_id, name, username):
        session_path = os.path.join(self._temp_dir, f"bot_{num}")
        bot_client = TelegramClient(
            session_path,
            api_id=self._client.api_id, api_hash=self._client.api_hash,
        )
        await bot_client.start(bot_token=credential)
        self._bot_clients[num] = bot_client

        async def precheckout_handler(event):
            try:
                await bot_client(SetBotPrecheckoutResultsRequest(
                    query_id=event.query_id,
                    success=True,
                    error=None,
                ))
            except Exception:
                pass

        bot_client.add_event_handler(
            precheckout_handler,
            events.Raw(types=UpdateBotPrecheckoutQuery),
        )

        try:
            from aiogram import Bot as AiogramBotClass
            from aiogram.client.bot import DefaultBotProperties
            from aiogram.enums import ParseMode
            abot = AiogramBotClass(
                credential,
                default=DefaultBotProperties(parse_mode=ParseMode.HTML)
            )
            self._aiogram_bots[num] = abot
        except Exception:
            pass

        async def _run_bot():
            try:
                await bot_client.run_until_disconnected()
            except Exception:
                pass

        task = asyncio.ensure_future(_run_bot())
        self._bot_tasks[num] = task

        self._accounts[num] = {
            "type": "bot", "credential": credential,
            "user_id": user_id, "name": name,
            "username": username, "client": bot_client,
        }

    def _save_accounts_to_db(self):
        sessions, tokens = [], []
        for num in sorted(self._accounts.keys()):
            acc = self._accounts[num]
            entry = f"{acc['user_id']}|{acc['name']}|{acc.get('username') or 'None'}|{acc['credential']}"
            (sessions if acc["type"] == "user" else tokens).append(entry)
        self._db.set("StarsX", "sessions_data", sessions)
        self._db.set("StarsX", "bot_tokens_data", tokens)

    def _save_pending_gifts(self):
        self._db.set("StarsX", "pending_gifts", self._pending_gifts)

    def _renumber_accounts(self):
        old = dict(sorted(self._accounts.items()))
        old_bots = dict(self._bot_clients)
        old_tasks = dict(self._bot_tasks)
        old_aiogram = dict(self._aiogram_bots)
        self._accounts, self._bot_clients, self._bot_tasks, self._aiogram_bots = {}, {}, {}, {}
        new_num = 1
        for old_num, acc in old.items():
            self._accounts[new_num] = acc
            if old_num in old_bots:
                self._bot_clients[new_num] = old_bots[old_num]
            if old_num in old_tasks:
                self._bot_tasks[new_num] = old_tasks[old_num]
            if old_num in old_aiogram:
                self._aiogram_bots[new_num] = old_aiogram[old_num]
            new_num += 1
        self._next_num = new_num
        self._save_accounts_to_db()

    async def _setup_payment_handler(self):
        try:
            dp = self.inline._dp
            bot = self.inline_bot

            if hasattr(bot, 'session') and not hasattr(bot.session.make_request, '_is_patched_starsx'):
                original = bot.session.make_request
                bot.session._starsx_original_make_request = original

                async def patched(session_self, method, *a, **kw):
                    if method.__class__.__name__ == "GetUpdates":
                        cur = getattr(method, 'allowed_updates', None) or []
                        for t in ["pre_checkout_query", "shipping_query"]:
                            if t not in cur:
                                cur.append(t)
                        method.allowed_updates = cur
                    return await original(session_self, method, *a, **kw)

                patched._is_patched_starsx = True
                bot.session.make_request = patched

            if not hasattr(dp.feed_update, '_is_patched_starsx'):
                original_feed = dp.feed_update
                dp._starsx_original_feed_update = original_feed

                async def patched_feed(bot_inst, update: Update, **kw):
                    if hasattr(update, 'pre_checkout_query') and update.pre_checkout_query:
                        q = update.pre_checkout_query
                        try:
                            await bot_inst.answer_pre_checkout_query(pre_checkout_query_id=q.id, ok=True)
                        except Exception:
                            try:
                                await bot_inst.answer_pre_checkout_query(
                                    pre_checkout_query_id=q.id, ok=False, error_message="Error")
                            except Exception:
                                pass
                        return True
                    return await original_feed(bot_inst, update, **kw)

                patched_feed._is_patched_starsx = True
                dp.feed_update = patched_feed

            if self._router:
                try:
                    dp._sub_routers.remove(self._router)
                except Exception:
                    pass
            self._router = Router(name="starsx_payment")
            dp.include_router(self._router)
            try:
                await bot.delete_webhook(drop_pending_updates=False)
            except Exception:
                pass
        except Exception:
            pass

    async def _unpatch_handlers(self):
        try:
            dp = self.inline._dp
            bot = self.inline_bot
            if hasattr(dp.feed_update, '_is_patched_starsx') and hasattr(dp, '_starsx_original_feed_update'):
                dp.feed_update = dp._starsx_original_feed_update
            if hasattr(bot, 'session') and hasattr(bot.session.make_request, '_is_patched_starsx'):
                if hasattr(bot.session, '_starsx_original_make_request'):
                    bot.session.make_request = bot.session._starsx_original_make_request
        except Exception:
            pass

    async def _get_gifts_catalog(self, force=False):
        if not force and self._gifts_cache and (time.time() - self._gifts_cache_time) < 300:
            return self._gifts_cache
        try:
            r = await self._client(GetStarGiftsRequest(hash=0))
            self._gifts_cache = r.gifts
            self._gifts_cache_time = time.time()
            self._max_gift_index = len(self._gifts_cache) - 1 if self._gifts_cache else 0
            return self._gifts_cache
        except Exception:
            return None

    async def _load_collections(self):
        self._collections_cache = {}
        self._collections_name_map = {}
        gifts = await self._get_gifts_catalog(force=True)
        if not gifts:
            return
        for g in gifts:
            if not (hasattr(g, 'upgrade_stars') and g.upgrade_stars):
                continue
            title = getattr(g, 'title', None)
            if not title:
                continue
            self._collections_cache[g.id] = {
                'title': title,
                'stars': g.stars,
                'upgrade': g.upgrade_stars,
            }
            low = title.lower()
            nospace = low.replace(' ', '').replace('-', '')
            self._collections_name_map[low] = g.id
            self._collections_name_map[nospace] = g.id

    def _find_collection_id(self, query):
        q = query.strip().lower().replace(' ', '').replace('-', '')
        if q in self._collections_name_map:
            return self._collections_name_map[q]
        for key, gid in self._collections_name_map.items():
            if q in key or key in q:
                return gid
        return None

    async def _get_model_stickerset(self, gift_id):
        try:
            preview = await self._client(GetStarGiftUpgradePreviewRequest(gift_id=gift_id))
        except Exception:
            return None, None
        attrs = getattr(preview, 'sample_attributes', [])
        for attr in attrs:
            if type(attr).__name__ != 'StarGiftAttributeModel':
                continue
            doc = getattr(attr, 'document', None)
            if not doc:
                continue
            for da in getattr(doc, 'attributes', []):
                ss = getattr(da, 'stickerset', None)
                if ss and hasattr(ss, 'id') and hasattr(ss, 'access_hash'):
                    try:
                        result = await self._client(GetStickerSetRequest(
                            stickerset=InputStickerSetID(id=ss.id, access_hash=ss.access_hash),
                            hash=0,
                        ))
                        set_title = getattr(result.set, 'short_name', '?')
                        return result, set_title
                    except Exception:
                        return None, None
        return None, None

    async def _get_default_set_id(self, gift_id):
        try:
            preview = await self._client(GetStarGiftUpgradePreviewRequest(gift_id=gift_id))
        except Exception:
            return None, None
        for attr in getattr(preview, 'sample_attributes', []):
            if type(attr).__name__ != 'StarGiftAttributeModel':
                continue
            doc = getattr(attr, 'document', None)
            if not doc:
                continue
            for da in getattr(doc, 'attributes', []):
                ss = getattr(da, 'stickerset', None)
                if ss and hasattr(ss, 'id'):
                    return ss.id, ss.access_hash
        return None, None

    async def _fetch_sticker_set(self, ss_id, ss_ah):
        try:
            return await self._client(GetStickerSetRequest(
                stickerset=InputStickerSetID(id=ss_id, access_hash=ss_ah),
                hash=0,
            ))
        except Exception:
            return None

    async def _get_all_model_doc_ids(self, gift_id):
        try:
            resale = await self._client(GetResaleStarGiftsRequest(
                gift_id=gift_id, offset="", limit=1,
                sort_by_price=True, sort_by_num=False,
                attributes_hash=0, attributes=None,
            ))
        except Exception:
            return []
        ids = []
        for c in getattr(resale, 'counters', []):
            a = getattr(c, 'attribute', None)
            if a and type(a).__name__ == 'StarGiftAttributeIdModel':
                d = getattr(a, 'document_id', None)
                if d:
                    ids.append(d)
        return ids

    async def _get_stickerset_from_slug(self, slug):
        try:
            r = await self._client(GetUniqueStarGiftRequest(slug=slug))
            gift = r.gift if hasattr(r, 'gift') else r
            for attr in getattr(gift, 'attributes', []):
                if type(attr).__name__ != 'StarGiftAttributeModel':
                    continue
                doc = getattr(attr, 'document', None)
                if not doc:
                    continue
                for da in getattr(doc, 'attributes', []):
                    ss = getattr(da, 'stickerset', None)
                    if ss and hasattr(ss, 'id'):
                        return ss.id, ss.access_hash
        except Exception:
            pass
        return None, None

    async def _find_crafted_slug_filtered(self, gift_id, doc_id):
        from telethon.tl.types import StarGiftAttributeIdModel
        try:
            resale = await self._client(GetResaleStarGiftsRequest(
                gift_id=gift_id, offset="", limit=1,
                sort_by_price=True, sort_by_num=False,
                attributes_hash=0,
                attributes=[StarGiftAttributeIdModel(document_id=doc_id)],
            ))
            gifts = getattr(resale, 'gifts', [])
            if gifts:
                return getattr(gifts[0], 'slug', None)
        except Exception:
            pass
        return None

    async def _find_crafted_slug_paginated(self, gift_id, target_doc_ids):
        offset = ""
        for _ in range(30):
            try:
                resale = await self._client(GetResaleStarGiftsRequest(
                    gift_id=gift_id, offset=offset, limit=100,
                    sort_by_price=True, sort_by_num=False,
                    attributes_hash=0, attributes=None,
                ))
            except Exception:
                break
            for g in getattr(resale, 'gifts', []):
                for attr in getattr(g, 'attributes', []):
                    if type(attr).__name__ == 'StarGiftAttributeModel':
                        doc = getattr(attr, 'document', None)
                        if doc and doc.id in target_doc_ids:
                            slug = getattr(g, 'slug', None)
                            if slug:
                                return slug
            offset = getattr(resale, 'next_offset', '') or ''
            if not offset:
                break
            await asyncio.sleep(0.5)
        return None

    async def _discover_crafted_sets(self, gift_id):
        def_id, def_ah = await self._get_default_set_id(gift_id)
        default_set = await self._fetch_sticker_set(def_id, def_ah) if def_id else None
        default_doc_ids = {d.id for d in default_set.documents} if default_set else set()

        all_doc_ids = await self._get_all_model_doc_ids(gift_id)
        remaining = set(all_doc_ids) - default_doc_ids

        if not remaining:
            return []

        found_ss_ids = {def_id} if def_id else set()
        crafted_sets = []
        attempts = 0

        while remaining and attempts < 10:
            attempts += 1
            slug = None

            for doc_id in list(remaining)[:15]:
                slug = await self._find_crafted_slug_filtered(gift_id, doc_id)
                if slug:
                    break
                await asyncio.sleep(0.2)

            if not slug:
                slug = await self._find_crafted_slug_paginated(gift_id, remaining)

            if not slug:
                break

            ss_id, ss_ah = await self._get_stickerset_from_slug(slug)
            if not ss_id or ss_id in found_ss_ids:
                remaining.discard(next(iter(remaining)))
                continue

            found_ss_ids.add(ss_id)
            result = await self._fetch_sticker_set(ss_id, ss_ah)
            if result:
                crafted_sets.append(result)
                remaining -= {d.id for d in result.documents}
            else:
                break

            await asyncio.sleep(0.3)

        return crafted_sets

    async def _get_bot_gifts(self, bot_token):
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(f"https://api.telegram.org/bot{bot_token}/getAvailableGifts") as r:
                    d = await r.json()
                    if d.get("ok"):
                        return d["result"].get("gifts", [])
        except Exception:
            pass
        return None

    async def _get_saved_user_gifts(self, peer, limit=10):
        all_gifts, offset = [], ""
        while True:
            try:
                r = await self._client(GetSavedStarGiftsRequest(peer=peer, offset=offset, limit=limit))
                if not r.gifts:
                    break
                all_gifts.extend(r.gifts)
                if len(r.gifts) < limit:
                    break
                offset = getattr(r, 'next_offset', "") or ""
                if not offset:
                    break
                await asyncio.sleep(0.3)
            except Exception:
                break
        return all_gifts

    def _is_gift_id(self, value):
        return bool(GIFT_ID_PATTERN.match(str(value)))

    def _is_premium_arg(self, arg):
        return arg.lower() in ("prem_3", "prem_6", "prem_12")

    def _parse_premium_arg(self, arg):
        m = {"prem_3": 3, "prem_6": 6, "prem_12": 12}
        months = m.get(arg.lower())
        if months:
            return months, PREMIUM_COSTS[months]
        return None, None

    def _validate_gift_arg(self, arg, gifts_list=None):
        if self._is_premium_arg(arg):
            return arg.lower(), "premium"
        try:
            val = int(arg)
        except ValueError:
            return None, "not_number"
        max_idx = len(gifts_list) - 1 if gifts_list else self._max_gift_index
        if val <= max_idx:
            return val, "index"
        if self._is_gift_id(arg):
            return val, "id"
        return None, "invalid"

    def _parse_nft_slug(self, text):
        text = text.strip()
        m = NFT_LINK_PATTERN.match(text)
        if m:
            return m.group(1)
        if re.match(r'^[A-Za-z][\w-]+-\d+$', text):
            return text
        return None

    def _collection_from_slug(self, slug):
        m = COLLECTION_FROM_SLUG.match(slug)
        if m:
            return m.group(1)
        return None

    async def _get_user_balance(self, client):
        try:
            s = await client(GetStarsStatusRequest(peer=InputPeerSelf()))
            return True, s.balance.amount if hasattr(s.balance, 'amount') else s.balance
        except Exception as e:
            return False, str(e)

    async def _get_bot_balance(self, bot_token):
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(f"https://api.telegram.org/bot{bot_token}/getMyStarBalance") as r:
                    d = await r.json()
                    if d.get("ok"):
                        return True, d["result"].get("amount", 0)
                    return False, str(d)
        except Exception as e:
            return False, str(e)

    async def _send_gift_from_user(self, client, target_id, gift_id, is_channel, text=None):
        try:
            target = await client.get_input_entity(target_id)
            inv = InputInvoiceStarGift(target, int(gift_id), message=TextWithEntities(text or "", []))
            form = await client(GetPaymentFormRequest(inv))
            await client(SendStarsFormRequest(form.form_id, inv))
            return {"ok": True}
        except BadRequestError as e:
            return {"ok": False, "error": "BALANCE_TOO_LOW" if "BALANCE_TOO_LOW" in str(e) else str(e)}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    async def _send_gift_from_bot(self, bot_token, target_id, gift_id, is_channel, text=None):
        try:
            p = {"gift_id": str(gift_id)}
            if is_channel:
                p["chat_id"] = int(f"-100{target_id}") if target_id > 0 else target_id
            else:
                p["user_id"] = target_id
            if text:
                p["text"] = text[:255]
            async with aiohttp.ClientSession() as s:
                async with s.post(f"https://api.telegram.org/bot{bot_token}/sendGift", json=p) as r:
                    return await r.json()
        except Exception as e:
            return {"ok": False, "description": str(e)}

    async def _send_premium_from_bot(self, bot_token, target_id, months, stars, text=None, aiogram_bot=None):
        try:
            if aiogram_bot:
                params = {
                    "user_id": target_id,
                    "month_count": months,
                    "star_count": stars,
                }
                if text:
                    params["text"] = text[:255]
                    params["text_parse_mode"] = "HTML"
                await aiogram_bot.gift_premium_subscription(**params)
                return {"ok": True}
            else:
                p = {
                    "user_id": target_id,
                    "month_count": months,
                    "star_count": stars,
                }
                if text:
                    p["text"] = text[:255]
                    p["text_parse_mode"] = "HTML"
                async with aiohttp.ClientSession() as s:
                    async with s.post(f"https://api.telegram.org/bot{bot_token}/giftPremiumSubscription", json=p) as r:
                        return await r.json()
        except Exception as e:
            err = str(e)
            return {"ok": False, "description": err, "error": err}

    async def _refund_via_bot_api(self, bot_token, user_id, charge_id):
        try:
            async with aiohttp.ClientSession() as s:
                async with s.post(
                    f"https://api.telegram.org/bot{bot_token}/refundStarPayment",
                    json={"user_id": user_id, "telegram_payment_charge_id": charge_id},
                ) as r:
                    d = await r.json()
                    return (True, None) if d.get("ok") else (False, d.get("description", "Unknown"))
        except Exception as e:
            return False, str(e)

    async def _export_invoice_link(self, bot_client, stars, photo_url=None, res_w=512, res_h=512):
        photo = None
        if photo_url:
            photo = InputWebDocument(url=photo_url, size=0, mime_type="image/jpeg", attributes=[])
        invoice = Invoice(
            currency="XTR",
            prices=[TLLabeledPrice(label=f"{stars} Stars", amount=stars)],
            test=False,
        )
        media = InputMediaInvoice(
            title=self._get_invoice_text(1, stars),
            description=self._get_invoice_text(2, stars),
            invoice=invoice,
            payload=f"invoice_{stars}_{int(time.time())}".encode(),
            provider=None,
            provider_data=DataJSON(data="{}"),
            photo=photo,
            start_param=f"invoice_{stars}",
        )
        r = await bot_client(ExportInvoiceRequest(invoice_media=media))
        return r.url

    def _get_invoice_text(self, num, stars):
        custom = self.config[f"invoice_text_{num}"]
        defaults = {1: self.strings["invoice_title_default"], 2: self.strings["invoice_desc_default"]}
        template = custom if custom else defaults.get(num, "")
        try:
            return template.format(stars=stars)
        except Exception:
            return template

    def _generate_gift_token(self, target, gift_id, count, comment):
        secret = secrets.token_hex(16)
        token = hashlib.sha256(f"{target}:{gift_id}:{count}:{comment}:{secret}".encode()).hexdigest()[:24]
        self._pending_gifts[token] = {
            "target": target, "gift_id": gift_id, "count": count,
            "comment": comment, "created": time.time(), "claimed": False,
        }
        self._save_pending_gifts()
        return token

    def _validate_gift_token(self, token, user_id, username=None):
        if token not in self._pending_gifts:
            return False, "expired"
        gd = self._pending_gifts[token]
        if gd.get("claimed"):
            return False, "claimed"
        if time.time() - gd.get("created", 0) > 86400:
            del self._pending_gifts[token]
            self._save_pending_gifts()
            return False, "expired"
        target = gd.get("target", "")
        if target != "all":
            if target.isdigit():
                if int(target) != user_id:
                    return False, "not_for_you"
            else:
                if not username or username.lower() != target.lstrip("@").lower():
                    return False, "not_for_you"
        return True, gd

    def _mark_gift_claimed(self, token):
        if token in self._pending_gifts:
            self._pending_gifts[token]["claimed"] = True
            self._save_pending_gifts()

    async def _is_forum_chat(self, message):
        if message.is_private:
            return False
        try:
            chat = await message.get_chat()
            return getattr(chat, 'forum', False)
        except Exception:
            return False

    def _get_topic_id(self, message):
        rt = getattr(message, 'reply_to', None)
        if rt:
            top = getattr(rt, 'reply_to_top_id', None)
            if top:
                return top
            mid = getattr(rt, 'reply_to_msg_id', None)
            if mid and getattr(rt, 'forum_topic', False):
                return mid
        return None

    async def _get_reply_id(self, message):
        is_forum = await self._is_forum_chat(message)
        topic_id = self._get_topic_id(message) if is_forum else None
        reply = await message.get_reply_message()
        return reply.id if reply else (topic_id or None)

    async def _resolve_target(self, target_str):
        try:
            entity = await self._client.get_entity(
                int(target_str) if target_str.lstrip("-").isdigit() else target_str
            )
            return entity.id, entity
        except Exception:
            return None, None

    async def _try_delete(self, msg):
        try:
            if msg:
                await msg.delete()
        except Exception:
            pass

    async def _send_sticker_doc(self, chat_id, doc, reply_id=None):
        is_ce = _is_custom_emoji_doc(doc)

        if not is_ce:
            try:
                data = await self._client.download_media(doc, bytes)
                if not data:
                    return False
                f = io.BytesIO(data)
                f.name = "sticker.tgs"
                await self._client.send_file(chat_id, f, reply_to=reply_id)
                return True
            except Exception:
                return False

        try:
            data = await self._client.download_media(doc, bytes)
            if not data:
                return False

            f = io.BytesIO(data)
            f.name = "sticker.tgs"

            alt_emoji = ""
            for attr in getattr(doc, 'attributes', []):
                if isinstance(attr, DocumentAttributeCustomEmoji):
                    alt_emoji = getattr(attr, 'alt', '') or ''

            attrs = [
                DocumentAttributeImageSize(w=512, h=512),
                DocumentAttributeSticker(
                    alt=alt_emoji or '⭐',
                    stickerset=InputStickerSetEmpty(),
                    mask=None,
                    mask_coords=None,
                ),
            ]

            await self._client.send_file(
                chat_id, f,
                reply_to=reply_id,
                attributes=attrs,
                force_document=False,
            )
            return True
        except Exception:
            try:
                data = await self._client.download_media(doc, bytes)
                if data:
                    f = io.BytesIO(data)
                    f.name = "sticker.tgs"
                    await self._client.send_file(
                        chat_id, f,
                        reply_to=reply_id,
                        force_document=True,
                    )
                    return True
            except Exception:
                pass
            return False

    async def _send_sticker_docs(self, message, docs, reply_id):
        sent = 0
        for doc in docs:
            if self._show_stop:
                return sent, True
            ok = await self._send_sticker_doc(message.chat_id, doc, reply_id)
            if ok:
                sent += 1
            if sent % 10 == 0 and sent > 0:
                await asyncio.sleep(1)
            else:
                await asyncio.sleep(0.3)
        return sent, False

    @loader.command(
        ru_doc="Показать стикер подарка или список подарков",
        en_doc="Show gift sticker or list gifts",
    )
    async def show(self, message: Message):
        """Show gift sticker or list gifts"""
        args = utils.get_args_raw(message).strip()
        prefix = self.get_prefix()
        if not args:
            await utils.answer(message, self.strings["show_help"].format(prefix=prefix))
            return
        parts = args.split()
        cmd = parts[0].lower()

        if cmd == "stop":
            if self._show_stop:
                await utils.answer(message, self.strings["show_stop_nothing"])
            else:
                self._show_stop = True
                await utils.answer(message, self.strings["show_stop_done"])
            return

        if cmd == "collections":
            await self._show_collections(message)
            return

        if cmd == "all":
            if len(parts) < 2:
                await utils.answer(message, self.strings["show_help"].format(prefix=prefix))
                return
            mode_arg = parts[1].lower()
            if mode_arg in ("craft", "ord"):
                if len(parts) < 3:
                    await utils.answer(message, self.strings["show_help"].format(prefix=prefix))
                    return
                query = " ".join(parts[2:])
                await self._show_all_models(message, query, mode_arg)
            else:
                query = " ".join(parts[1:])
                await self._show_all_models(message, query, "ord")
            return

        if cmd == "list":
            if len(parts) < 2:
                await utils.answer(message, self.strings["show_help"].format(prefix=prefix))
                return
            await self._show_list(message, parts[1].lower())
            return

        if cmd == "get":
            if len(parts) < 2:
                await utils.answer(message, self.strings["show_help"].format(prefix=prefix))
                return
            if parts[1].lower() == "sticker" and len(parts) >= 3:
                await self._show_get_user_stickers(message, parts[2])
                return
            await self._show_get_user_gifts(message, parts[1])
            return

        slug = self._parse_nft_slug(args)
        if slug:
            await self._show_nft_sticker(message, slug)
            return

        await self._show_sticker(message, args)

    async def _show_nft_sticker(self, message, slug):
        status_msg = await utils.answer(message, self.strings["show_nft_loading"])
        reply_id = await self._get_reply_id(message)

        try:
            r = await self._client(GetUniqueStarGiftRequest(slug=slug))
            gift = r.gift if hasattr(r, 'gift') else r
        except Exception:
            await utils.answer(status_msg, self.strings["show_nft_not_found"])
            return

        model_name = pattern_name = backdrop_name = "?"
        model_doc = None

        attrs = getattr(gift, 'attributes', None) or []
        for attr in attrs:
            atype = type(attr).__name__
            if atype == "StarGiftAttributeModel":
                model_name = getattr(attr, 'name', '?')
                model_doc = getattr(attr, 'document', None)
            elif atype == "StarGiftAttributePattern":
                pattern_name = getattr(attr, 'name', '?')
            elif atype == "StarGiftAttributeBackdrop":
                backdrop_name = getattr(attr, 'name', '?')

        if model_doc:
            await self._send_sticker_doc(message.chat_id, model_doc, reply_id)

        info = self.strings["show_nft_gift_info"].format(
            title=getattr(gift, 'title', '?'), slug=slug,
            model=model_name, pattern=pattern_name, backdrop=backdrop_name,
        )
        await self._client.send_message(
            message.chat_id, f"<blockquote>{info}</blockquote>",
            parse_mode="html", reply_to=reply_id,
        )
        await self._try_delete(status_msg)

    async def _show_sticker(self, message, arg):
        status_msg = await utils.answer(message, self.strings["show_checking"])
        reply_id = await self._get_reply_id(message)

        gifts = await self._get_gifts_catalog(force=True)
        if not gifts:
            await utils.answer(status_msg, self.strings["show_no_gifts"])
            return

        gift_val, gift_type = self._validate_gift_arg(arg, gifts)
        if gift_type in ("not_number", "invalid", "premium"):
            await utils.answer(status_msg, self.strings["show_invalid_arg"].format(max=self._max_gift_index))
            return

        if gift_type == "index":
            selected = gifts[gift_val]
            try:
                ok = await self._send_sticker_doc(message.chat_id, selected.sticker, reply_id)
                if ok:
                    await self._try_delete(status_msg)
                else:
                    await utils.answer(status_msg, self.strings["show_download_error"].format(error="send failed"))
            except Exception as e:
                await utils.answer(status_msg, self.strings["show_download_error"].format(error=str(e)))
            return

        for i, g in enumerate(gifts):
            if g.id == gift_val:
                try:
                    ok = await self._send_sticker_doc(message.chat_id, g.sticker, reply_id)
                    if ok:
                        await self._try_delete(status_msg)
                    else:
                        await utils.answer(status_msg, self.strings["show_download_error"].format(error="send failed"))
                except Exception as e:
                    await utils.answer(status_msg, self.strings["show_download_error"].format(error=str(e)))
                return

        try:
            me_input = await self._client.get_input_entity(self._owner_id)
            inv = InputInvoiceStarGift(me_input, int(gift_val), message=TextWithEntities("", []))
            form = await self._client(GetPaymentFormRequest(inv))
            price = "?"
            if hasattr(form, 'invoice') and form.invoice and form.invoice.prices:
                price = form.invoice.prices[0].amount
            await self._client.send_message(
                message.chat_id,
                f"<blockquote>{self.strings['show_hidden_gift_info'].format(id=gift_val, stars=price)}</blockquote>",
                parse_mode="html", reply_to=reply_id,
            )
            await self._try_delete(status_msg)
            return
        except RPCError as e:
            if "STARGIFT_INVALID" not in str(e):
                await utils.answer(status_msg, self.strings["show_not_found"])
                return
        except Exception:
            pass

        try:
            own_saved = await self._get_saved_user_gifts(InputPeerSelf(), limit=100)
            for sg in own_saved:
                gift = sg.gift
                if type(gift).__name__ == "StarGiftUnique":
                    gid = getattr(gift, 'id', None)
                    if gid == gift_val:
                        slug = getattr(gift, 'slug', None)
                        if slug:
                            await self._show_nft_sticker(message, slug)
                            await self._try_delete(status_msg)
                            return
        except Exception:
            pass

        await utils.answer(status_msg, self.strings["show_not_found"])

    async def _show_collections(self, message):
        status_msg = await utils.answer(message, self.strings["show_collections_loading"])
        reply_id = await self._get_reply_id(message)

        await self._load_collections()

        if not self._collections_cache:
            await utils.answer(status_msg, self.strings["show_collections_empty"])
            return

        sorted_items = sorted(
            self._collections_cache.items(),
            key=lambda x: x[1]['stars'],
            reverse=True,
        )

        lines = []
        for i, (gid, info) in enumerate(sorted_items, 1):
            lines.append(
                f"{i}. {self.strings['show_collections_item'].format(title=info['title'], stars=info['stars'], upgrade=info['upgrade'])}"
            )

        await utils.answer(status_msg, self.strings["show_collections_header"].format(count=len(self._collections_cache)))

        total = len(lines)
        chunks_count = (total + CHUNK_SIZE_COLLECTIONS - 1) // CHUNK_SIZE_COLLECTIONS
        for i in range(0, total, CHUNK_SIZE_COLLECTIONS):
            part = (i // CHUNK_SIZE_COLLECTIONS) + 1
            chunk = lines[i:i + CHUNK_SIZE_COLLECTIONS]
            header = f"<b>Part {part}/{chunks_count}</b>\n\n" if chunks_count > 1 else ""
            await self._client.send_message(
                message.chat_id,
                f"<blockquote expandable>{header}{chr(10).join(chunk)}</blockquote>",
                parse_mode="html", reply_to=reply_id,
            )
            await asyncio.sleep(0.5)

    async def _show_all_models(self, message, query, mode="ord"):
        prefix = self.get_prefix()
        slug = self._parse_nft_slug(query)
        if slug:
            collection_name = self._collection_from_slug(slug)
            if collection_name:
                query = collection_name

        if not self._collections_cache:
            await self._load_collections()

        gift_id = self._find_collection_id(query)
        if not gift_id or gift_id not in self._collections_cache:
            await utils.answer(message, self.strings["show_all_not_found"].format(name=query, prefix=prefix))
            return

        info = self._collections_cache[gift_id]
        title = info['title']

        if mode == "craft":
            status_msg = await utils.answer(
                message,
                self.strings["show_all_discovering_crafted"].format(name=title),
            )
        else:
            status_msg = await utils.answer(
                message,
                self.strings["show_all_loading"].format(name=title),
            )

        reply_id = await self._get_reply_id(message)
        self._show_stop = False

        if mode == "craft":
            crafted_sets = await self._discover_crafted_sets(gift_id)
            if not crafted_sets:
                await utils.answer(status_msg, self.strings["show_all_no_crafted"].format(name=title))
                return

            all_docs = []
            for cs in crafted_sets:
                all_docs.extend(cs.documents)
            total = len(all_docs)

            if len(crafted_sets) > 1:
                await utils.answer(
                    status_msg,
                    self.strings["show_all_info_crafted_multi"].format(
                        title=title,
                        sets_count=len(crafted_sets),
                        count=total,
                    ),
                )
            else:
                await utils.answer(
                    status_msg,
                    self.strings["show_all_info"].format(
                        title=title, mode="crafted", count=total,
                    ),
                )

            sent, stopped = await self._send_sticker_docs(message, all_docs, reply_id)

            if stopped:
                await self._client.send_message(
                    message.chat_id,
                    self.strings["show_all_stopped"].format(sent=sent, total=total),
                    parse_mode="html", reply_to=reply_id,
                )
                self._show_stop = False
                return

            await self._client.send_message(
                message.chat_id,
                self.strings["show_all_done"].format(title=title, mode="crafted", sent=sent, total=total),
                parse_mode="html", reply_to=reply_id,
            )

        else:
            ss_result, set_name = await self._get_model_stickerset(gift_id)
            if not ss_result:
                await utils.answer(status_msg, self.strings["show_all_no_set"])
                return

            docs = ss_result.documents
            total = len(docs)

            await utils.answer(
                status_msg,
                self.strings["show_all_info"].format(
                    title=title, mode="ordinary", count=total,
                ),
            )

            sent, stopped = await self._send_sticker_docs(message, docs, reply_id)

            if stopped:
                await self._client.send_message(
                    message.chat_id,
                    self.strings["show_all_stopped"].format(sent=sent, total=total),
                    parse_mode="html", reply_to=reply_id,
                )
                self._show_stop = False
                return

            await self._client.send_message(
                message.chat_id,
                self.strings["show_all_done"].format(title=title, mode="ordinary", sent=sent, total=total),
                parse_mode="html", reply_to=reply_id,
            )

    async def _show_get_user_stickers(self, message, target):
        status_msg = await utils.answer(message, self.strings["show_get_sticker_loading"])
        reply_id = await self._get_reply_id(message)

        target_id, target_entity = await self._resolve_target(target)
        if not target_id:
            await utils.answer(status_msg, self.strings["not_found"])
            return

        try:
            peer = await self._client.get_input_entity(target_id)
        except Exception:
            await utils.answer(status_msg, self.strings["not_found"])
            return

        saved = await self._get_saved_user_gifts(peer, limit=CHUNK_SIZE_USER_GIFTS)
        if not saved:
            await utils.answer(status_msg, self.strings["show_get_empty"])
            return

        self._show_stop = False
        sent_ids = set()
        sent = 0

        for sg in saved:
            if self._show_stop:
                self._show_stop = False
                break

            gift = sg.gift
            gtype = type(gift).__name__

            if gtype == "StarGiftUnique":
                gid = getattr(gift, 'id', None)
                if gid in sent_ids:
                    continue
                sent_ids.add(gid)
                slug = getattr(gift, 'slug', None)
                if slug:
                    try:
                        r = await self._client(GetUniqueStarGiftRequest(slug=slug))
                        g = r.gift if hasattr(r, 'gift') else r
                        attrs = getattr(g, 'attributes', None) or []
                        for attr in attrs:
                            if type(attr).__name__ == "StarGiftAttributeModel":
                                doc = getattr(attr, 'document', None)
                                if doc:
                                    ok = await self._send_sticker_doc(message.chat_id, doc, reply_id)
                                    if ok:
                                        sent += 1
                                break
                    except Exception:
                        pass
            else:
                gid = getattr(gift, 'id', None)
                if gid in sent_ids:
                    continue
                sent_ids.add(gid)
                sticker = getattr(gift, 'sticker', None)
                if sticker:
                    ok = await self._send_sticker_doc(message.chat_id, sticker, reply_id)
                    if ok:
                        sent += 1

            if sent % 5 == 0 and sent > 0:
                await asyncio.sleep(1)
            else:
                await asyncio.sleep(0.3)

        await utils.answer(status_msg, self.strings["show_get_sticker_done"].format(sent=sent))

    async def _show_get_user_gifts(self, message, target):
        status_msg = await utils.answer(message, self.strings["show_get_loading"])
        reply_id = await self._get_reply_id(message)

        target_id, target_entity = await self._resolve_target(target)
        if not target_id:
            await utils.answer(status_msg, self.strings["not_found"])
            return

        try:
            peer = await self._client.get_input_entity(target_id)
        except Exception:
            await utils.answer(status_msg, self.strings["not_found"])
            return

        saved = await self._get_saved_user_gifts(peer, limit=CHUNK_SIZE_USER_GIFTS)
        if not saved:
            await utils.answer(status_msg, self.strings["show_get_empty"])
            return

        user_name = get_full_name(target_entity) if target_entity else str(target_id)
        await utils.answer(status_msg, self.strings["show_get_header"].format(
            user=escape_html(user_name), count=len(saved),
        ))

        lines = []
        for i, sg in enumerate(saved):
            gift = sg.gift
            gtype = type(gift).__name__
            if gtype == "StarGiftUnique":
                gid = getattr(gift, 'id', '?')
                title = getattr(gift, 'title', '?')
                lines.append(self.strings["show_get_item_unique"].format(index=i + 1, id=gid, title=title))
            else:
                gid = getattr(gift, 'id', '?')
                lines.append(self.strings["show_get_item"].format(index=i + 1, id=gid))

        total = len(lines)
        chunks_count = (total + CHUNK_SIZE_LIST - 1) // CHUNK_SIZE_LIST
        for i in range(0, total, CHUNK_SIZE_LIST):
            part = (i // CHUNK_SIZE_LIST) + 1
            chunk = lines[i:i + CHUNK_SIZE_LIST]
            header = self.strings["show_get_chunk"].format(current=part, total=chunks_count)
            await self._client.send_message(
                message.chat_id,
                f"<blockquote expandable>{header}{chr(10).join(chunk)}</blockquote>",
                parse_mode="html", reply_to=reply_id,
            )
            await asyncio.sleep(0.5)

    async def _show_list(self, message, source):
        status_msg = await utils.answer(message, self.strings["show_list_loading"])
        reply_id = await self._get_reply_id(message)
        gifts, source_name = None, ""

        if source == "user":
            gifts = await self._get_gifts_catalog(force=True)
            source_name = "User"
        elif source == "bot":
            if not self.inline_bot:
                await utils.answer(status_msg, self.strings["no_inline_bot"])
                return
            gifts = await self._get_bot_gifts(self.inline_bot.token)
            source_name = "Inline Bot"
        else:
            try:
                num = int(source)
                acc = self._accounts.get(num)
                if not acc:
                    await utils.answer(status_msg, self.strings["starsx_not_found"].format(num=num))
                    return
                if acc["type"] == "user":
                    cl = acc.get("client")
                    try:
                        gifts = (await cl(GetStarGiftsRequest(hash=0))).gifts if cl else None
                    except Exception:
                        pass
                    if not gifts:
                        gifts = await self._get_gifts_catalog(force=True)
                else:
                    gifts = await self._get_bot_gifts(acc["credential"])
                source_name = f"Account [{num}]"
            except ValueError:
                prefix = self.get_prefix()
                await utils.answer(status_msg, self.strings["show_help"].format(prefix=prefix))
                return

        if not gifts:
            await utils.answer(status_msg, self.strings["show_list_empty"])
            return

        is_bot = isinstance(gifts, list) and gifts and isinstance(gifts[0], dict)
        lines = []
        for i, g in enumerate(gifts):
            if is_bot:
                gid = g.get("id", "?")
                stars = g.get("star_count", 0)
                lim = f" (left: {g['remaining_count']})" if g.get("remaining_count") is not None else ""
                lines.append(self.strings["show_list_bot_item"].format(index=i, id=gid, stars=stars, limited=lim))
            else:
                lines.append(self.strings["show_list_item"].format(index=i, id=g.id, stars=g.stars))

        await utils.answer(status_msg, self.strings["show_list_header"].format(count=len(gifts), source=source_name))

        total = len(lines)
        chunks_count = (total + CHUNK_SIZE_LIST - 1) // CHUNK_SIZE_LIST
        for i in range(0, total, CHUNK_SIZE_LIST):
            part = (i // CHUNK_SIZE_LIST) + 1
            chunk = lines[i:i + CHUNK_SIZE_LIST]
            header = self.strings["show_list_chunk"].format(current=part, total=chunks_count)
            await self._client.send_message(
                message.chat_id,
                f"<blockquote expandable>{header}{chr(10).join(chunk)}</blockquote>",
                parse_mode="html", reply_to=reply_id,
            )
            await asyncio.sleep(0.5)

    @loader.command(
        ru_doc="Управление балансом и настройками инвойсов",
        en_doc="Manage balance and invoice settings",
    )
    async def stars(self, message: Message):
        """Manage balance and invoice settings"""
        args = utils.get_args_raw(message).strip()
        prefix = self.get_prefix()
        if not args:
            await utils.answer(message, self.strings["stars_help"].format(prefix=prefix))
            return
        parts = args.split()
        cmd = parts[0].lower()
        if cmd == "balance" and len(parts) >= 2:
            await self._stars_balance(message, parts[1].lower())
        elif cmd == "invoice" and len(parts) >= 2:
            await self._stars_invoice(message, parts[1].lower(), parts[2:])
        else:
            await utils.answer(message, self.strings["stars_help"].format(prefix=prefix))

    async def _stars_balance(self, message, source):
        status_msg = await utils.answer(message, self.strings["stars_balance_loading"])
        if source == "user":
            ok, r = await self._get_user_balance(self._client)
            sn = "User"
        elif source == "bot":
            if not self.inline_bot:
                await utils.answer(status_msg, self.strings["no_inline_bot"])
                return
            ok, r = await self._get_bot_balance(self.inline_bot.token)
            sn = "Inline Bot"
        else:
            try:
                num = int(source)
                acc = self._accounts.get(num)
                if not acc:
                    await utils.answer(status_msg, self.strings["starsx_not_found"].format(num=num))
                    return
                if acc["type"] == "user":
                    cl = acc.get("client")
                    if not cl:
                        await utils.answer(status_msg, self.strings["error"].format(error="Session not connected"))
                        return
                    ok, r = await self._get_user_balance(cl)
                    sn = f"Account [{num}] (User)"
                else:
                    ok, r = await self._get_bot_balance(acc["credential"])
                    sn = f"Account [{num}] (Bot)"
            except ValueError:
                prefix = self.get_prefix()
                await utils.answer(status_msg, self.strings["stars_help"].format(prefix=prefix))
                return
        if ok:
            await utils.answer(status_msg, self.strings["stars_balance_result"].format(source=sn, amount=r))
        else:
            await utils.answer(status_msg, self.strings["stars_balance_error"].format(error=r))

    async def _stars_invoice(self, message, subcmd, args):
        prefix = self.get_prefix()
        if subcmd == "resolution":
            if not args:
                await utils.answer(message, self.strings["stars_invoice_res_invalid"].format(prefix=prefix))
                return
            m = RESOLUTION_PATTERN.search(" ".join(args))
            if not m:
                await utils.answer(message, self.strings["stars_invoice_res_invalid"].format(prefix=prefix))
                return
            w, h = int(m.group(1)), int(m.group(2))
            self.config["invoice_res_w"] = w
            self.config["invoice_res_h"] = h
            await utils.answer(message, self.strings["stars_invoice_res_set"].format(width=w, height=h))
        elif subcmd == "photo":
            if not args:
                self.config["invoice_photo"] = ""
                await utils.answer(message, self.strings["stars_invoice_photo_reset"])
                return
            url = args[0]
            if not url.startswith("http"):
                await utils.answer(message, self.strings["stars_invoice_photo_invalid"].format(prefix=prefix))
                return
            self.config["invoice_photo"] = url
            await utils.answer(message, self.strings["stars_invoice_photo_set"].format(url=url))
        elif subcmd == "text":
            if not args:
                await utils.answer(message, self.strings["stars_invoice_text_invalid"].format(prefix=prefix))
                return
            try:
                num = int(args[0])
                assert num in (1, 2)
            except Exception:
                await utils.answer(message, self.strings["stars_invoice_text_invalid"].format(prefix=prefix))
                return
            if len(args) < 2:
                self.config[f"invoice_text_{num}"] = ""
                await utils.answer(message, self.strings["stars_invoice_text_reset"].format(num=num))
            else:
                t = " ".join(args[1:])
                self.config[f"invoice_text_{num}"] = t
                await utils.answer(message, self.strings["stars_invoice_text_set"].format(num=num, text=t))
        else:
            await utils.answer(message, self.strings["stars_help"].format(prefix=prefix))

    @loader.command(
        ru_doc="Отправить подарок пользователю или каналу",
        en_doc="Send a gift to a user or channel",
    )
    async def gift(self, message: Message):
        """Send a gift to a user or channel"""
        args = utils.get_args_raw(message).strip()
        prefix = self.get_prefix()
        if not args:
            await utils.answer(message, self.strings["gift_help"].format(prefix=prefix))
            return
        parts = args.split()
        if len(parts) < 7 or parts[0].lower() != "from" or parts[2].lower() != "to":
            await utils.answer(message, self.strings["gift_usage"].format(prefix=prefix))
            return

        from_src, to_type = parts[1].lower(), parts[3].lower()
        target_str, gift_arg, count_arg = parts[4], parts[5], parts[6]
        comment = " ".join(parts[7:]) if len(parts) > 7 else None

        if to_type not in ("user", "channel"):
            await utils.answer(message, self.strings["gift_usage"].format(prefix=prefix))
            return
        try:
            count = int(count_arg)
            assert count >= 1
        except Exception:
            await utils.answer(message, self.strings["gift_invalid_count"])
            return

        is_premium = self._is_premium_arg(gift_arg)

        if is_premium:
            months, stars_cost = self._parse_premium_arg(gift_arg)
            if not months:
                await utils.answer(message, self.strings["gift_invalid_gift"].format(max=0))
                return

            bot_token, aiogram_bot, from_info = None, None, ""
            if from_src == "bot":
                if not self.inline_bot:
                    await utils.answer(message, self.strings["no_inline_bot"])
                    return
                bot_token = self.inline_bot.token
                aiogram_bot = self._aiogram_bot
                from_info = "Inline Bot"
            else:
                try:
                    num = int(from_src)
                    acc = self._accounts.get(num)
                    if not acc:
                        await utils.answer(message, self.strings["gift_account_not_found"].format(num=num))
                        return
                    if acc["type"] != "bot":
                        await utils.answer(message, self.strings["gift_premium_only_bot"])
                        return
                    bot_token = acc["credential"]
                    aiogram_bot = self._aiogram_bots.get(num)
                    from_info = f"Account [{num}] (Bot)"
                except ValueError:
                    await utils.answer(message, self.strings["gift_premium_only_bot"])
                    return

            target_id, _ = await self._resolve_target(target_str)
            if not target_id:
                await utils.answer(message, self.strings["not_found"])
                return

            success, errors, status_msg = 0, [], message
            for i in range(count):
                status_msg = await utils.answer(status_msg, self.strings["gift_premium_sending"].format(
                    months=months, stars=stars_cost, current=i + 1, total=count,
                ))
                r = await self._send_premium_from_bot(bot_token, target_id, months, stars_cost, comment, aiogram_bot)
                if r.get("ok"):
                    success += 1
                else:
                    err = r.get("description") or r.get("error") or "Unknown"
                    errors.append(err)
                    if "BALANCE_TOO_LOW" in err:
                        break
                if count > 1 and i < count - 1:
                    await asyncio.sleep(0.5)

            gift_info = f"Premium {months}m ({stars_cost})"
            if success == count:
                await utils.answer(status_msg, self.strings["gift_sent"].format(
                    from_info=from_info, target=target_str, to_type=to_type,
                    gift_info=gift_info, count=count,
                    comment=escape_html(comment) if comment else "-",
                ))
            elif success > 0:
                await utils.answer(status_msg, self.strings["gift_partial"].format(
                    success=success, total=count, errors="; ".join(set(errors[:3])),
                ))
            else:
                err = errors[0] if errors else "Unknown"
                if "BALANCE_TOO_LOW" in err:
                    await utils.answer(status_msg, self.strings["gift_balance_low"])
                else:
                    await utils.answer(status_msg, self.strings["gift_error"].format(error=err))
            return

        gifts = await self._get_gifts_catalog(force=True)
        if not gifts:
            await utils.answer(message, self.strings["show_no_gifts"])
            return

        gv, gt = self._validate_gift_arg(gift_arg, gifts)
        if gv is None:
            await utils.answer(message, self.strings["gift_invalid_gift"].format(max=len(gifts) - 1))
            return

        gift_id = gifts[gv].id if gt == "index" else gv
        gift_info = f"Index: {gv} (ID: <code>{gift_id}</code>)" if gt == "index" else f"ID: <code>{gift_id}</code>"

        target_id, _ = await self._resolve_target(target_str)
        if not target_id:
            await utils.answer(message, self.strings["not_found"])
            return

        is_channel = to_type == "channel"
        client, bot_token, from_info = None, None, ""

        if from_src == "user":
            client, from_info = self._client, "User (self)"
        elif from_src == "bot":
            if not self.inline_bot:
                await utils.answer(message, self.strings["no_inline_bot"])
                return
            bot_token, from_info = self.inline_bot.token, "Inline Bot"
        else:
            try:
                num = int(from_src)
                acc = self._accounts.get(num)
                if not acc:
                    await utils.answer(message, self.strings["gift_account_not_found"].format(num=num))
                    return
                if acc["type"] == "user":
                    client = acc.get("client")
                    if not client:
                        await utils.answer(message, self.strings["error"].format(error="Session not connected"))
                        return
                    from_info = f"Account [{num}] (User)"
                else:
                    bot_token, from_info = acc["credential"], f"Account [{num}] (Bot)"
            except ValueError:
                await utils.answer(message, self.strings["gift_usage"].format(prefix=prefix))
                return

        success, errors, status_msg = 0, [], message
        for i in range(count):
            status_msg = await utils.answer(status_msg, self.strings["gift_sending"].format(
                current=i + 1, total=count, mode="Bot API" if bot_token else "User API",
            ))
            if bot_token:
                r = await self._send_gift_from_bot(bot_token, target_id, str(gift_id), is_channel, comment)
                if r.get("ok"):
                    success += 1
                else:
                    errors.append(r.get("description", "Unknown"))
            else:
                r = await self._send_gift_from_user(client, target_id, gift_id, is_channel, comment)
                if r.get("ok"):
                    success += 1
                else:
                    errors.append(r.get("error", "Unknown"))
                    if "BALANCE_TOO_LOW" in r.get("error", ""):
                        break
            if count > 1 and i < count - 1:
                await asyncio.sleep(0.5)

        if success == count:
            await utils.answer(status_msg, self.strings["gift_sent"].format(
                from_info=from_info, target=target_str, to_type=to_type,
                gift_info=gift_info, count=count, comment=escape_html(comment) if comment else "-",
            ))
        elif success > 0:
            await utils.answer(status_msg, self.strings["gift_partial"].format(
                success=success, total=count, errors="; ".join(set(errors[:3])),
            ))
        else:
            err = errors[0] if errors else "Unknown"
            if "BALANCE_TOO_LOW" in err:
                await utils.answer(status_msg, self.strings["gift_balance_low"])
            else:
                await utils.answer(status_msg, self.strings["gift_error"].format(error=err))

    @loader.command(
        ru_doc="Управление подключёнными аккаунтами",
        en_doc="Manage connected accounts",
    )
    async def starsx(self, message: Message):
        """Manage connected accounts"""
        args = utils.get_args_raw(message).strip()
        prefix = self.get_prefix()
        if not args:
            await utils.answer(message, self.strings["starsx_help"].format(max=MAX_ACCOUNTS, prefix=prefix))
            return
        parts = args.split()
        cmd = parts[0].lower()

        if cmd == "add":
            await self._starsx_add(message, " ".join(parts[1:]))
        elif cmd == "remove":
            if len(parts) < 2:
                await utils.answer(message, self.strings["invalid_args"])
                return
            if parts[1] == "-force":
                await self._starsx_remove_all(message)
            else:
                try:
                    await self._starsx_remove(message, int(parts[1]))
                except ValueError:
                    await utils.answer(message, self.strings["invalid_args"])
        elif cmd == "list":
            await self._starsx_list(message)
        elif cmd == "info" and len(parts) >= 2:
            try:
                await self._starsx_info(message, int(parts[1]))
            except ValueError:
                await utils.answer(message, self.strings["invalid_args"])
        else:
            await utils.answer(message, self.strings["starsx_help"].format(max=MAX_ACCOUNTS, prefix=prefix))

    async def _starsx_add(self, message, args_text):
        credential, cred_type = None, None
        for text in [args_text, (await message.get_reply_message() or type('', (), {'text': None})).text or ""]:
            if credential:
                break
            mt = BOT_TOKEN_PATTERN.search(text)
            ms = STRING_SESSION_PATTERN.search(text)
            if mt:
                credential, cred_type = mt.group(0), "bot"
            elif ms:
                credential, cred_type = ms.group(0), "user"

        if not credential:
            await utils.answer(message, self.strings["starsx_no_creds"])
            return
        if len(self._accounts) >= MAX_ACCOUNTS:
            await utils.answer(message, self.strings["starsx_max_reached"].format(max=MAX_ACCOUNTS))
            return

        status_msg = await utils.answer(message, self.strings["starsx_testing"])
        try:
            if cred_type == "bot":
                async with aiohttp.ClientSession() as s:
                    async with s.get(f"https://api.telegram.org/bot{credential}/getMe") as r:
                        d = await r.json()
                        if not d.get("ok"):
                            await utils.answer(status_msg, self.strings["starsx_not_authorized"])
                            return
                        bi = d["result"]
                        user_id, name, username = bi["id"], bi.get("first_name", "Bot"), bi.get("username")

                for en, acc in self._accounts.items():
                    if acc.get("user_id") == user_id:
                        await utils.answer(status_msg, self.strings["starsx_exists"].format(num=en))
                        return

                num = self._next_num
                self._next_num += 1
                await self._start_bot_session(num, credential, user_id, name, username)
                self._save_accounts_to_db()
            else:
                cl = TelegramClient(StringSession(credential), api_id=self._client.api_id,
                                    api_hash=self._client.api_hash)
                await cl.connect()
                if not await cl.is_user_authorized():
                    await utils.answer(status_msg, self.strings["starsx_not_authorized"])
                    await cl.disconnect()
                    return
                me = await cl.get_me()
                user_id, name, username = me.id, get_full_name(me), get_username(me)

                for en, acc in self._accounts.items():
                    if acc.get("user_id") == user_id:
                        await utils.answer(status_msg, self.strings["starsx_exists"].format(num=en))
                        await cl.disconnect()
                        return

                num = self._next_num
                self._next_num += 1
                self._accounts[num] = {
                    "type": "user", "credential": credential,
                    "user_id": user_id, "name": name, "username": username, "client": cl,
                }
                self._save_accounts_to_db()

            await utils.answer(status_msg, self.strings["starsx_added"].format(
                num=num, type="Bot" if cred_type == "bot" else "User",
                name=escape_html(name), id=user_id, username=f"@{username}" if username else "-",
            ))
        except Exception as e:
            await utils.answer(status_msg, self.strings["error"].format(error=str(e)[:200]))

    async def _starsx_remove(self, message, num):
        acc = self._accounts.get(num)
        if not acc:
            await utils.answer(message, self.strings["starsx_not_found"].format(num=num))
            return
        cl = acc.get("client")
        if cl:
            try:
                await cl.disconnect()
            except Exception:
                pass
        if num in self._bot_tasks:
            self._bot_tasks[num].cancel()
            del self._bot_tasks[num]
        self._bot_clients.pop(num, None)
        if num in self._aiogram_bots:
            try:
                await self._aiogram_bots[num].session.close()
            except Exception:
                pass
            del self._aiogram_bots[num]
        del self._accounts[num]
        self._renumber_accounts()
        await utils.answer(message, self.strings["starsx_removed"].format(num=num))

    async def _starsx_remove_all(self, message):
        for acc in self._accounts.values():
            cl = acc.get("client")
            if cl:
                try:
                    await cl.disconnect()
                except Exception:
                    pass
        for t in self._bot_tasks.values():
            t.cancel()
        for abot in self._aiogram_bots.values():
            try:
                await abot.session.close()
            except Exception:
                pass
        self._bot_clients, self._bot_tasks, self._accounts, self._aiogram_bots = {}, {}, {}, {}
        self._next_num = 1
        self._save_accounts_to_db()
        await utils.answer(message, self.strings["starsx_removed_all"])

    async def _starsx_list(self, message):
        if not self._accounts:
            await utils.answer(message, self.strings["starsx_list_empty"])
            return
        lines = []
        for num, acc in sorted(self._accounts.items()):
            lines.append(self.strings["starsx_list_item"].format(
                num=num, type="Bot" if acc["type"] == "bot" else "User",
                name=escape_html(acc.get("name", "Unknown")), id=acc.get("user_id", 0),
            ))
        header = self.strings["starsx_list_header"].format(count=len(self._accounts), max=MAX_ACCOUNTS)
        await utils.answer(message, f"{header}<blockquote expandable>{chr(10).join(lines)}</blockquote>")

    async def _starsx_info(self, message, num):
        acc = self._accounts.get(num)
        if not acc:
            await utils.answer(message, self.strings["starsx_not_found"].format(num=num))
            return
        await self._client.send_message(
            message.chat_id,
            self.strings["starsx_info"].format(
                num=num, type="Bot" if acc["type"] == "bot" else "User",
                name=escape_html(acc.get("name", "Unknown")), id=acc.get("user_id", 0),
                username=f"@{acc['username']}" if acc.get("username") else "-",
            ),
            parse_mode="html",
        )

    @loader.command(
        ru_doc="Возврат звёзд через бота",
        en_doc="Refund stars via bot",
    )
    async def refund(self, message: Message):
        """Refund stars via bot"""
        args = utils.get_args_raw(message).strip()
        prefix = self.get_prefix()
        if not args:
            await utils.answer(message, self.strings["refund_help"].format(prefix=prefix))
            return
        parts = args.split()
        if len(parts) < 3:
            await utils.answer(message, self.strings["refund_help"].format(prefix=prefix))
            return

        source = parts[0].lower()
        try:
            uid, cid = int(parts[1]), parts[2].strip()
        except ValueError:
            await utils.answer(message, self.strings["invalid_args"])
            return

        if source == "bot":
            if not self.inline_bot:
                await utils.answer(message, self.strings["no_inline_bot"])
                return
            token = self.inline_bot.token
        else:
            try:
                num = int(source)
                acc = self._accounts.get(num)
                if not acc:
                    await utils.answer(message, self.strings["starsx_not_found"].format(num=num))
                    return
                if acc["type"] != "bot":
                    await utils.answer(message, self.strings["refund_not_a_bot"].format(num=num))
                    return
                token = acc["credential"]
            except ValueError:
                await utils.answer(message, self.strings["refund_help"].format(prefix=prefix))
                return

        ok, err = await self._refund_via_bot_api(token, uid, cid)
        if ok:
            await utils.answer(message, self.strings["refund_success"].format(user_id=uid, charge_id=cid))
        else:
            await utils.answer(message, self.strings["refund_error"].format(error=err))

    @loader.command(
        ru_doc="Создать ссылку на подарок или инвойс",
        en_doc="Create a gift or invoice link",
    )
    async def link(self, message: Message):
        """Create a gift or invoice link"""
        args = utils.get_args_raw(message).strip()
        prefix = self.get_prefix()
        if not args:
            await utils.answer(message, self.strings["link_help"].format(prefix=prefix))
            return
        parts = args.split()
        if len(parts) < 3 or parts[0].lower() != "for":
            await utils.answer(message, self.strings["link_help"].format(prefix=prefix))
            return

        target = parts[1]
        try:
            num = int(target)
            if num in self._accounts and self._accounts[num]["type"] == "bot":
                amount = int(parts[2])
                await self._link_invoice(message, num, amount)
                return
        except ValueError:
            pass

        if len(parts) < 4:
            await utils.answer(message, self.strings["link_help"].format(prefix=prefix))
            return

        gift_arg, count_arg = parts[2], parts[3]
        comment = " ".join(parts[4:]) if len(parts) > 4 else ""
        try:
            count = int(count_arg)
            assert count >= 1
        except Exception:
            await utils.answer(message, self.strings["gift_invalid_count"])
            return

        gifts = await self._get_gifts_catalog(force=True)
        if not gifts:
            await utils.answer(message, self.strings["show_no_gifts"])
            return

        gv, gt = self._validate_gift_arg(gift_arg, gifts)
        if gv is None or gt == "premium":
            await utils.answer(message, self.strings["link_invalid_gift"])
            return

        if gt == "index":
            gift_id, gift_stars = gifts[gv].id, gifts[gv].stars
        else:
            gift_id, gift_stars = gv, 0
            for g in gifts:
                if g.id == gift_id:
                    gift_stars = g.stars
                    break

        if not self.inline_bot:
            await utils.answer(message, self.strings["no_inline_bot"])
            return

        ok, balance = await self._get_bot_balance(self.inline_bot.token)
        if ok and gift_stars > 0:
            need = gift_stars * count
            if balance < need:
                await utils.answer(message, self.strings["link_not_enough_stars"].format(need=need, have=balance))
                return

        token = self._generate_gift_token(target.lower() if target.lower() == "all" else target, gift_id, count, comment)
        link_url = f"https://t.me/{self.inline_bot_username}?start=gift_{token}"
        reply_id = await self._get_reply_id(message)

        try:
            await message.delete()
        except Exception:
            pass

        try:
            for g in gifts:
                if g.id == gift_id:
                    await self._send_sticker_doc(message.chat_id, g.sticker, reply_id)
                    break
        except Exception:
            pass

        await self._client.send_message(
            message.chat_id,
            f'<a href="{link_url}">{self.strings["link_gift_anchor"]}</a>',
            parse_mode="html", reply_to=reply_id,
        )

    async def _link_invoice(self, message, num, amount):
        acc = self._accounts.get(num)
        if not acc:
            await utils.answer(message, self.strings["starsx_not_found"].format(num=num))
            return
        if acc["type"] != "bot":
            await utils.answer(message, self.strings["link_not_a_bot"].format(num=num))
            return
        bc = self._bot_clients.get(num)
        if not bc:
            await utils.answer(message, self.strings["link_bot_session_missing"].format(num=num))
            return
        try:
            url = await self._export_invoice_link(
                bc, amount, self.config["invoice_photo"] or None,
                self.config["invoice_res_w"], self.config["invoice_res_h"],
            )
            await utils.answer(message,
                self.strings["link_invoice_created"].format(num=num, amount=amount) + f'\n\n<a href="{url}">Open</a>',
            )
        except Exception as e:
            await utils.answer(message, self.strings["link_error"].format(error=str(e)[:200]))

    @loader.inline_handler(
        ru_doc="[сумма] - создать инвойс",
        en_doc="[amount] - create invoice",
    )
    async def stars_inline_handler(self, query: InlineQuery):
        """[amount] - create invoice"""
        m = re.match(r'^stars\s*(\d+)$', query.query.strip().lower())
        if not m:
            return
        stars_amount = int(m.group(1))
        if stars_amount <= 0 or stars_amount > 10000:
            return

        photo = self.config["invoice_photo"]
        params = {
            "title": self._get_invoice_text(1, stars_amount),
            "description": self._get_invoice_text(2, stars_amount),
            "payload": f"inline_{stars_amount}_{query.from_user.id}_{int(time.time())}",
            "provider_token": "", "currency": "XTR",
            "prices": [LabeledPrice(label=f"{stars_amount} Stars", amount=stars_amount)],
        }
        if photo:
            params.update(photo_url=photo, photo_width=self.config["invoice_res_w"],
                          photo_height=self.config["invoice_res_h"])

        try:
            await self.inline_bot.answer_inline_query(
                query.id,
                [InlineQueryResultArticle(
                    id=f"stars_{stars_amount}_{int(time.time())}",
                    title=self.strings["inline_title"].format(stars=stars_amount),
                    description=self.strings["inline_description"],
                    input_message_content=InputInvoiceMessageContent(**params),
                    thumbnail_url=photo or None,
                )],
                cache_time=1, is_personal=True,
            )
        except Exception:
            pass

    async def aiogram_watcher(self, message: AiogramMessage):
        if hasattr(message, "successful_payment") and message.successful_payment:
            await message.answer(
                self.strings["bot_invoice_sent"].format(amount=message.successful_payment.total_amount),
                parse_mode="HTML")
            return

        if not message.text or not self.inline_bot:
            return

        uid, text = message.from_user.id, message.text.strip()

        if text.startswith("/start"):
            parts = text.split(maxsplit=1)
            if len(parts) > 1:
                param = parts[1]
                if param.startswith("invoice_"):
                    try:
                        s = int(param.split("_")[1])
                        if s > 0:
                            await self._send_inline_invoice(uid, s)
                            return
                    except Exception:
                        pass
                if param.startswith("gift_"):
                    token = param[5:]
                    valid, result = self._validate_gift_token(token, uid, message.from_user.username)
                    if not valid:
                        msgs = {
                            "expired": self.strings["bot_gift_expired"],
                            "claimed": self.strings["bot_gift_already_claimed"],
                            "not_for_you": self.strings["bot_gift_not_for_you"],
                        }
                        await message.answer(msgs.get(result, self.strings["bot_gift_error"].format(error=result)),
                                             parse_mode="HTML")
                        return
                    gd = result
                    await message.answer(self.strings["bot_gift_claimed"], parse_mode="HTML")
                    success = 0
                    for _ in range(gd["count"]):
                        r = await self._send_gift_from_bot(self.inline_bot.token, uid, str(gd["gift_id"]), False,
                                                           gd["comment"])
                        if r.get("ok"):
                            success += 1
                        await asyncio.sleep(0.3)
                    self._mark_gift_claimed(token)
                    if success == gd["count"]:
                        await message.answer(self.strings["bot_gift_sent"], parse_mode="HTML")
                    else:
                        await message.answer(
                            self.strings["bot_gift_error"].format(error=f"Sent {success}/{gd['count']}"),
                            parse_mode="HTML")
                    return
            await message.answer(self.strings["bot_start"], parse_mode="HTML")
            return

        if text.startswith("/refund") and uid == self._owner_id:
            parts = text.split()
            if len(parts) < 3:
                await message.answer("Usage: /refund user_id charge_id", parse_mode="HTML")
                return
            try:
                await self.inline_bot.refund_star_payment(user_id=int(parts[1]),
                                                          telegram_payment_charge_id=parts[2].strip())
                await message.answer(
                    self.strings["bot_refund_success"].format(user_id=parts[1], charge_id=parts[2]),
                    parse_mode="HTML")
            except Exception as e:
                await message.answer(self.strings["bot_refund_error"].format(error=str(e)), parse_mode="HTML")

    async def _send_inline_invoice(self, user_id, stars):
        try:
            photo = self.config["invoice_photo"]
            await self.inline_bot.send_invoice(
                chat_id=user_id,
                **generate_star_invoice(
                    stars, self._get_invoice_text(1, stars), self._get_invoice_text(2, stars),
                    f"stars_{stars}_{int(time.time())}", f"invoice_{stars}",
                    photo_url=photo or None,
                    photo_width=self.config["invoice_res_w"], photo_height=self.config["invoice_res_h"],
                ),
            )
        except Exception:
            pass

    async def on_unload(self):
        global _MODULE_INSTANCE
        for acc in self._accounts.values():
            cl = acc.get("client")
            if cl:
                try:
                    await cl.disconnect()
                except Exception:
                    pass
        for t in self._bot_tasks.values():
            t.cancel()
        for abot in self._aiogram_bots.values():
            try:
                await abot.session.close()
            except Exception:
                pass
        if self._aiogram_bot:
            try:
                await self._aiogram_bot.session.close()
            except Exception:
                pass
        self._bot_clients, self._bot_tasks, self._aiogram_bots = {}, {}, {}
        if self._temp_dir and os.path.exists(self._temp_dir):
            shutil.rmtree(self._temp_dir, ignore_errors=True)
        await self._unpatch_handlers()
        _MODULE_INSTANCE = None