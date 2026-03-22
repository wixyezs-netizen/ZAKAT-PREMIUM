# bot_zakat.py — ZAKAT PREMIUM Shop
import logging
import asyncio
import aiohttp
import hashlib
import hmac
import time
import random
import json
import os
from datetime import datetime, timedelta
from urllib.parse import parse_qs, unquote, quote
from collections import OrderedDict
from typing import Optional, Dict, Any

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton,
    LabeledPrice, PreCheckoutQuery
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.client.default import DefaultBotProperties

# ========== ЛОГИРОВАНИЕ ==========
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ========== КОНФИГУРАЦИЯ ==========
class Config:
    BOT_TOKEN: str = os.environ.get("BOT_TOKEN", "8364248036:AAEhPvRkINz08OIRhzfC37BaX8ti8bUdipc")
    CRYPTOBOT_TOKEN: str = os.environ.get("CRYPTOBOT_TOKEN", "493276:AAtS7R1zYy0gaPw8eax1EgiWo0tdnd6dQ9c")
    YOOMONEY_ACCESS_TOKEN: str = os.environ.get("YOOMONEY_ACCESS_TOKEN", "4100118889570559.3288B2E716CEEB922A26BD6BEAC58648FBFB680CCF64E4E1447D714D6FB5EA5F01F1478FAC686BEF394C8A186C98982DE563C1ABCDF9F2F61D971B61DA3C7E486CA818F98B9E0069F1C0891E090DD56A11319D626A40F0AE8302A8339DED9EB7969617F191D93275F64C4127A3ECB7AED33FCDE91CA68690EB7534C67E6C219E")
    YOOMONEY_WALLET: str = os.environ.get("YOOMONEY_WALLET", "4100118889570559")

    SUPPORT_CHAT_USERNAME = os.environ.get("SUPPORT_CHAT_USERNAME", "ZakatManager")

    ADMIN_IDS = set()
    ADMIN_ID = 0
    SUPPORT_CHAT_ID = 0

    MAX_PENDING_ORDERS = 1000
    ORDER_EXPIRY_SECONDS = 3600
    RATE_LIMIT_SECONDS = 2
    MAX_PAYMENT_CHECK_ATTEMPTS = 5
    PAYMENT_CHECK_INTERVAL = 5

    @classmethod
    def init(cls):
        if not cls.BOT_TOKEN:
            raise ValueError("BOT_TOKEN is required!")

        admin_ids_str = os.environ.get("ADMIN_ID", "")
        admin_ids_list = [
            int(x.strip())
            for x in admin_ids_str.split(",")
            if x.strip().isdigit()
        ]

        if not admin_ids_list:
            raise ValueError("ADMIN_ID is required!")

        cls.ADMIN_ID = admin_ids_list[0]
        cls.SUPPORT_CHAT_ID = (
            admin_ids_list[1]
            if len(admin_ids_list) >= 2
            else int(os.environ.get("SUPPORT_CHAT_ID", str(cls.ADMIN_ID)))
        )
        cls.ADMIN_IDS = set(admin_ids_list)

        if not cls.CRYPTOBOT_TOKEN:
            logger.warning("CRYPTOBOT_TOKEN not set — crypto payments disabled")
        if not cls.YOOMONEY_ACCESS_TOKEN:
            logger.warning("YOOMONEY_ACCESS_TOKEN not set — card payments disabled")
        if not cls.YOOMONEY_WALLET:
            logger.warning("YOOMONEY_WALLET not set — card payments disabled")


# ========== ХРАНИЛИЩЕ ДАННЫХ ==========
class OrderStorage:
    def __init__(self, max_pending=1000, expiry_seconds=3600):
        self._pending = OrderedDict()
        self._confirmed = {}
        self._lock = asyncio.Lock()
        self._max_pending = max_pending
        self._expiry_seconds = expiry_seconds

    async def add_pending(self, order_id, order_data):
        async with self._lock:
            await self._cleanup_expired()
            if len(self._pending) >= self._max_pending:
                self._pending.popitem(last=False)
            self._pending[order_id] = order_data

    async def get_pending(self, order_id):
        async with self._lock:
            return self._pending.get(order_id)

    async def confirm(self, order_id, extra_data):
        async with self._lock:
            if order_id in self._confirmed:
                return False
            order = self._pending.pop(order_id, None)
            if order is None:
                return False
            self._confirmed[order_id] = {**order, **extra_data}
            return True

    async def is_confirmed(self, order_id):
        async with self._lock:
            return order_id in self._confirmed

    async def get_confirmed(self, order_id):
        async with self._lock:
            return self._confirmed.get(order_id)

    async def remove_pending(self, order_id):
        async with self._lock:
            return self._pending.pop(order_id, None)

    async def get_stats(self):
        async with self._lock:
            return {
                "pending": len(self._pending),
                "confirmed": len(self._confirmed)
            }

    async def get_recent_pending(self, limit=5):
        async with self._lock:
            return list(self._pending.items())[-limit:]

    async def _cleanup_expired(self):
        now = time.time()
        expired = [
            oid for oid, data in self._pending.items()
            if now - data.get("created_at", 0) > self._expiry_seconds
        ]
        for oid in expired:
            del self._pending[oid]
        if expired:
            logger.info("Cleaned up %d expired orders", len(expired))


class RateLimiter:
    def __init__(self, interval=2.0):
        self._last_action = {}
        self._interval = interval

    def check(self, user_id):
        now = time.time()
        last = self._last_action.get(user_id, 0)
        if now - last < self._interval:
            return False
        self._last_action[user_id] = now
        if len(self._last_action) > 10000:
            cutoff = now - self._interval * 10
            self._last_action = {
                uid: t for uid, t in self._last_action.items()
                if t > cutoff
            }
        return True


# ========== ПРОДУКТЫ ==========
PRODUCTS = {
    # ===== PREMIUM АККАУНТЫ =====
    "prem_week": {
        "name": "💎 Premium аккаунт",
        "period_text": "НЕДЕЛЮ",
        "price": 199,
        "price_stars": 220,
        "price_gold": 550,
        "price_nft": 450,
        "price_crypto_usdt": 3,
        "platform": "Аккаунт",
        "period": "НЕДЕЛЮ",
        "platform_code": "prem",
        "emoji": "💎",
        "duration": "7 дней"
    },
    "prem_month": {
        "name": "💎 Premium аккаунт",
        "period_text": "МЕСЯЦ",
        "price": 449,
        "price_stars": 470,
        "price_gold": 1100,
        "price_nft": 900,
        "price_crypto_usdt": 6,
        "platform": "Аккаунт",
        "period": "МЕСЯЦ",
        "platform_code": "prem",
        "emoji": "💎",
        "duration": "30 дней"
    },
    "prem_forever": {
        "name": "💎 Premium аккаунт",
        "period_text": "НАВСЕГДА",
        "price": 890,
        "price_stars": 900,
        "price_gold": 2200,
        "price_nft": 1800,
        "price_crypto_usdt": 12,
        "platform": "Аккаунт",
        "period": "НАВСЕГДА",
        "platform_code": "prem",
        "emoji": "💎",
        "duration": "Навсегда"
    },
    # ===== ПОДПИСКИ =====
    "sub_week": {
        "name": "📱 Подписка на сервис",
        "period_text": "НЕДЕЛЮ",
        "price": 149,
        "price_stars": 160,
        "price_gold": 400,
        "price_nft": 350,
        "price_crypto_usdt": 2,
        "platform": "Подписка",
        "period": "НЕДЕЛЮ",
        "platform_code": "sub",
        "emoji": "📱",
        "duration": "7 дней"
    },
    "sub_month": {
        "name": "📱 Подписка на сервис",
        "period_text": "МЕСЯЦ",
        "price": 349,
        "price_stars": 370,
        "price_gold": 850,
        "price_nft": 700,
        "price_crypto_usdt": 5,
        "platform": "Подписка",
        "period": "МЕСЯЦ",
        "platform_code": "sub",
        "emoji": "📱",
        "duration": "30 дней"
    },
    "sub_forever": {
        "name": "📱 Подписка на сервис",
        "period_text": "НАВСЕГДА",
        "price": 690,
        "price_stars": 700,
        "price_gold": 1700,
        "price_nft": 1400,
        "price_crypto_usdt": 9,
        "platform": "Подписка",
        "period": "НАВСЕГДА",
        "platform_code": "sub",
        "emoji": "📱",
        "duration": "Навсегда"
    },
    # ===== КЛЮЧИ =====
    "key_week": {
        "name": "🔑 Ключ активации",
        "period_text": "НЕДЕЛЮ",
        "price": 179,
        "price_stars": 190,
        "price_gold": 480,
        "price_nft": 400,
        "price_crypto_usdt": 2,
        "platform": "Ключ",
        "period": "НЕДЕЛЮ",
        "platform_code": "key",
        "emoji": "🔑",
        "duration": "7 дней"
    },
    "key_month": {
        "name": "🔑 Ключ активации",
        "period_text": "МЕСЯЦ",
        "price": 399,
        "price_stars": 420,
        "price_gold": 950,
        "price_nft": 800,
        "price_crypto_usdt": 5,
        "platform": "Ключ",
        "period": "МЕСЯЦ",
        "platform_code": "key",
        "emoji": "🔑",
        "duration": "30 дней"
    },
    "key_forever": {
        "name": "🔑 Ключ активации",
        "period_text": "НАВСЕГДА",
        "price": 790,
        "price_stars": 800,
        "price_gold": 1900,
        "price_nft": 1600,
        "price_crypto_usdt": 10,
        "platform": "Ключ",
        "period": "НАВСЕГДА",
        "platform_code": "key",
        "emoji": "🔑",
        "duration": "Навсегда"
    }
}


# ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==========
def generate_order_id():
    raw = "{}_{}_{}" .format(time.time(), random.randint(100000, 999999), os.urandom(4).hex())
    return hashlib.sha256(raw.encode()).hexdigest()[:12]


def generate_license_key(order_id, user_id):
    raw = "{}_{}_{}" .format(order_id, user_id, os.urandom(8).hex())
    h = hashlib.sha256(raw.encode()).hexdigest()[:16].upper()
    return "ZKT-{}-{}-{}-{}".format(h[:4], h[4:8], h[8:12], h[12:16])


def is_admin(user_id):
    return user_id in Config.ADMIN_IDS


def find_product(platform_code, period):
    for p in PRODUCTS.values():
        if p['platform_code'] == platform_code and p['period'] == period:
            return p
    return None


def find_product_by_id(product_id):
    return PRODUCTS.get(product_id)


def create_payment_link(amount, order_id, product_name):
    comment = "Заказ {}: {}".format(order_id, product_name)
    safe_targets = quote(comment, safe='')
    success_url = quote('https://t.me/zakat_premium_bot?start=success', safe='')
    return (
        "https://yoomoney.ru/quickpay/confirm.xml"
        "?receiver={}"
        "&quickpay-form=shop"
        "&targets={}"
        "&sum={}"
        "&label={}"
        "&successURL={}"
        "&paymentType=AC"
    ).format(Config.YOOMONEY_WALLET, safe_targets, amount, order_id, success_url)


# ========== ПЛАТЁЖНЫЕ СЕРВИСЫ ==========
class YooMoneyService:
    @staticmethod
    async def get_balance():
        if not Config.YOOMONEY_ACCESS_TOKEN:
            return None
        headers = {"Authorization": "Bearer {}".format(Config.YOOMONEY_ACCESS_TOKEN)}
        try:
            timeout = aiohttp.ClientTimeout(total=15)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get("https://yoomoney.ru/api/account-info", headers=headers) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return float(data.get('balance', 0))
                    else:
                        body = await resp.text()
                        logger.error("YooMoney account-info %s: %s", resp.status, body)
        except Exception as e:
            logger.error("YooMoney balance error: %s", e)
        return None

    @staticmethod
    async def check_payment(order_id, expected_amount, order_time):
        if not Config.YOOMONEY_ACCESS_TOKEN:
            return False
        headers = {"Authorization": "Bearer {}".format(Config.YOOMONEY_ACCESS_TOKEN)}
        data = {"type": "deposition", "records": 100}
        try:
            timeout = aiohttp.ClientTimeout(total=20)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post("https://yoomoney.ru/api/operation-history", headers=headers, data=data) as resp:
                    if resp.status != 200:
                        return False
                    result = await resp.json()
                    operations = result.get("operations", [])
                    for op in operations:
                        if (op.get("label") == order_id and op.get("status") == "success"
                                and abs(float(op.get("amount", 0)) - expected_amount) <= 5):
                            return True
                    for op in operations:
                        if op.get("status") != "success":
                            continue
                        op_amount = float(op.get("amount", 0))
                        if abs(op_amount - expected_amount) > 2:
                            continue
                        try:
                            dt_str = op.get("datetime", "")
                            op_time = datetime.fromisoformat(dt_str.replace("Z", "+00:00")).timestamp()
                            if abs(op_time - order_time) <= 1800:
                                return True
                        except (ValueError, TypeError):
                            pass
        except Exception as e:
            logger.error("YooMoney check error: %s", e)
        return False


class CryptoBotService:
    BASE_URL = "https://pay.crypt.bot/api"

    @staticmethod
    async def create_invoice(amount_usdt, order_id, description):
        if not Config.CRYPTOBOT_TOKEN:
            return None
        headers = {"Crypto-Pay-API-Token": Config.CRYPTOBOT_TOKEN, "Content-Type": "application/json"}
        data = {
            "asset": "USDT", "amount": str(amount_usdt),
            "description": description[:256], "payload": order_id,
            "paid_btn_name": "callback",
            "paid_btn_url": "https://t.me/zakat_premium_bot?start=paid_{}".format(order_id)
        }
        try:
            timeout = aiohttp.ClientTimeout(total=15)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(CryptoBotService.BASE_URL + "/createInvoice", headers=headers, json=data) as resp:
                    if resp.status == 200:
                        result = await resp.json()
                        if result.get("ok"):
                            inv = result["result"]
                            return {"invoice_id": inv.get("invoice_id"), "pay_url": inv.get("pay_url"), "amount": inv.get("amount")}
                    body = await resp.text()
                    logger.error("CryptoBot createInvoice %s: %s", resp.status, body)
        except Exception as e:
            logger.error("CryptoBot API error: %s", e)
        return None

    @staticmethod
    async def check_invoice(invoice_id):
        if not Config.CRYPTOBOT_TOKEN:
            return False
        headers = {"Crypto-Pay-API-Token": Config.CRYPTOBOT_TOKEN, "Content-Type": "application/json"}
        try:
            timeout = aiohttp.ClientTimeout(total=15)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(CryptoBotService.BASE_URL + "/getInvoices", headers=headers, json={"invoice_ids": [invoice_id]}) as resp:
                    if resp.status == 200:
                        result = await resp.json()
                        if result.get("ok"):
                            items = result.get("result", {}).get("items", [])
                            if items:
                                return items[0].get("status") == "paid"
        except Exception as e:
            logger.error("CryptoBot check error: %s", e)
        return False


# ========== ИНИЦИАЛИЗАЦИЯ ==========
Config.init()

bot = Bot(token=Config.BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
orders = OrderStorage(max_pending=Config.MAX_PENDING_ORDERS, expiry_seconds=Config.ORDER_EXPIRY_SECONDS)
rate_limiter = RateLimiter(interval=Config.RATE_LIMIT_SECONDS)


# ========== СОСТОЯНИЯ ==========
class OrderState(StatesGroup):
    main_menu = State()
    choosing_category = State()
    choosing_subscription = State()
    choosing_payment = State()


# ========== КЛАВИАТУРЫ ==========
def start_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🛒 Купить товар", callback_data="buy_product")],
        [InlineKeyboardButton(text="ℹ️ О магазине", callback_data="about")],
        [InlineKeyboardButton(text="💬 Поддержка", url="https://t.me/{}".format(Config.SUPPORT_CHAT_USERNAME))]
    ])


def category_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💎 Premium аккаунты", callback_data="category_prem")],
        [InlineKeyboardButton(text="📱 Подписки на сервисы", callback_data="category_sub")],
        [InlineKeyboardButton(text="🔑 Ключи активации", callback_data="category_key")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="back_to_start")]
    ])


def subscription_keyboard(category):
    prices = {
        "prem": [
            ("⚡ НЕДЕЛЯ — 199₽", "sub_prem_week"),
            ("🔥 МЕСЯЦ — 449₽", "sub_prem_month"),
            ("💎 НАВСЕГДА — 890₽", "sub_prem_forever"),
        ],
        "sub": [
            ("⚡ НЕДЕЛЯ — 149₽", "sub_sub_week"),
            ("🔥 МЕСЯЦ — 349₽", "sub_sub_month"),
            ("💎 НАВСЕГДА — 690₽", "sub_sub_forever"),
        ],
        "key": [
            ("⚡ НЕДЕЛЯ — 179₽", "sub_key_week"),
            ("🔥 МЕСЯЦ — 399₽", "sub_key_month"),
            ("💎 НАВСЕГДА — 790₽", "sub_key_forever"),
        ]
    }
    buttons = [[InlineKeyboardButton(text=text, callback_data=cb)] for text, cb in prices.get(category, [])]
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="buy_product")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def payment_methods_keyboard(product):
    pc = product['platform_code']
    p = product['period']
    buttons = [
        [InlineKeyboardButton(text="💳 Картой", callback_data="pay_yoomoney_{}_{}".format(pc, p))],
        [InlineKeyboardButton(text="⭐ Telegram Stars", callback_data="pay_stars_{}_{}".format(pc, p))],
        [InlineKeyboardButton(text="₿ Криптобот", callback_data="pay_crypto_{}_{}".format(pc, p))],
        [InlineKeyboardButton(text="💰 GOLD", callback_data="pay_gold_{}_{}".format(pc, p))],
        [InlineKeyboardButton(text="🎨 NFT", callback_data="pay_nft_{}_{}".format(pc, p))],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="back_to_subscription")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def payment_keyboard(payment_url, order_id):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 Оплатить картой", url=payment_url)],
        [InlineKeyboardButton(text="✅ Проверить оплату", callback_data="checkym_{}".format(order_id))],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="restart")]
    ])


def crypto_payment_keyboard(invoice_url, order_id):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="₿ Оплатить криптой", url=invoice_url)],
        [InlineKeyboardButton(text="✅ Проверить платеж", callback_data="checkcr_{}".format(order_id))],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="restart")]
    ])


def support_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💬 Поддержка", url="https://t.me/{}".format(Config.SUPPORT_CHAT_USERNAME))],
        [InlineKeyboardButton(text="🔄 Новая покупка", callback_data="restart")]
    ])


def success_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💬 Поддержка", url="https://t.me/{}".format(Config.SUPPORT_CHAT_USERNAME))],
        [InlineKeyboardButton(text="🔄 Купить ещё", callback_data="restart")]
    ])


def about_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Назад", callback_data="back_to_start")]
    ])


def admin_confirm_keyboard(order_id):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Подтвердить", callback_data="admin_confirm_{}".format(order_id))],
        [InlineKeyboardButton(text="❌ Отклонить", callback_data="admin_reject_{}".format(order_id))]
    ])


def manual_payment_keyboard(support_url, sent_callback):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💬 Перейти к оплате", url=support_url)],
        [InlineKeyboardButton(text="✅ Я написал", callback_data=sent_callback)],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="restart")]
    ])


# ========== БИЗНЕС-ЛОГИКА ==========
async def process_successful_payment(order_id, source="API"):
    order = await orders.get_pending(order_id)
    if not order:
        if await orders.is_confirmed(order_id):
            logger.info("Order %s already confirmed", order_id)
        return False

    product = order["product"]
    user_id = order["user_id"]
    license_key = generate_license_key(order_id, user_id)

    confirmed = await orders.confirm(order_id, {
        'confirmed_at': time.time(), 'confirmed_by': source, 'license_key': license_key
    })
    if not confirmed:
        return False

    success_text = (
        "🎉 <b>Оплата подтверждена!</b>\n\n"
        "🔥 Добро пожаловать в ZAKAT PREMIUM!\n\n"
        "📦 <b>Ваша покупка:</b>\n"
        "{emoji} {name}\n"
        "⏱️ Срок: {duration}\n"
        "🔍 Метод: {source}\n\n"
        "🔑 <b>Ваш лицензионный ключ:</b>\n"
        "<code>{key}</code>\n\n"
        "📋 <b>Инструкция:</b>\n"
        "1️⃣ Скопируйте ключ выше\n"
        "2️⃣ Активируйте в личном кабинете\n"
        "3️⃣ Наслаждайтесь Premium! 💎\n\n"
        "💬 Поддержка: @{support}"
    ).format(emoji=product['emoji'], name=product['name'], duration=product['duration'],
             source=source, key=license_key, support=Config.SUPPORT_CHAT_USERNAME)

    try:
        await bot.send_message(user_id, success_text, reply_markup=success_keyboard())
    except Exception as e:
        logger.error("Error sending to user %s: %s", user_id, e)

    now_str = datetime.now().strftime('%d.%m.%Y %H:%M')
    admin_text = (
        "💎 <b>НОВАЯ ПРОДАЖА ({source})</b>\n\n"
        "👤 {user_name}\n🆔 {user_id}\n"
        "📦 {pname} ({dur})\n💰 {amount} {currency}\n"
        "🔑 <code>{key}</code>\n📅 {now}"
    ).format(source=source, user_name=order['user_name'], user_id=user_id,
             pname=product['name'], dur=product['duration'],
             amount=order.get('amount', product['price']), currency=order.get('currency', '₽'),
             key=license_key, now=now_str)
    for aid in Config.ADMIN_IDS:
        try:
            await bot.send_message(aid, admin_text)
        except Exception as e:
            logger.error("Error notifying admin %s: %s", aid, e)
    return True


async def send_admin_notification(user, product, payment_method, price, order_id):
    now_str = datetime.now().strftime('%d.%m.%Y %H:%M')
    message = (
        "🔔 <b>НОВЫЙ ЗАКАЗ — ZAKAT PREMIUM</b>\n\n"
        "👤 {fn}\n🆔 <code>{uid}</code>\n"
        "📦 {pn} ({dur})\n💰 {price}\n💳 {pm}\n"
        "🆔 <code>{oid}</code>\n\n📅 {now}"
    ).format(fn=user.full_name, uid=user.id, pn=product['name'], dur=product['duration'],
             price=price, pm=payment_method, oid=order_id, now=now_str)
    for aid in Config.ADMIN_IDS:
        try:
            await bot.send_message(aid, message, reply_markup=admin_confirm_keyboard(order_id))
        except Exception as e:
            logger.error("Error sending to admin %s: %s", aid, e)


async def send_start_message(target, state):
    text = (
        "🔥 <b>ZAKAT PREMIUM — Магазин цифровых товаров</b>\n\n"
        "✨ <b>Что у нас есть:</b>\n"
        "💎 Premium аккаунты лучших сервисов\n"
        "📱 Подписки (Netflix, Spotify, YouTube...)\n"
        "🔑 Ключи активации (Windows, Office, антивирусы)\n\n"
        "🛡️ Гарантия на все товары\n"
        "⚡ Моментальная выдача после оплаты\n"
        "💬 Поддержка 24/7 — @{support}\n\n"
        "🚀 <b>Выбирай и покупай прямо сейчас:</b>"
    ).format(support=Config.SUPPORT_CHAT_USERNAME)
    if isinstance(target, types.Message):
        await target.answer(text, reply_markup=start_keyboard())
    elif isinstance(target, types.CallbackQuery):
        try:
            await target.message.edit_text(text, reply_markup=start_keyboard())
        except Exception:
            await target.message.answer(text, reply_markup=start_keyboard())
    await state.set_state(OrderState.main_menu)


async def send_category_message(target, state):
    text = (
        "🛒 <b>Выберите категорию товаров:</b>\n\n"
        "💎 <b>Premium аккаунты</b> — топовые сервисы\n"
        "📱 <b>Подписки</b> — стриминг, музыка, видео\n"
        "🔑 <b>Ключи активации</b> — софт и программы"
    )
    if isinstance(target, types.CallbackQuery):
        try:
            await target.message.edit_text(text, reply_markup=category_keyboard())
        except Exception:
            await target.message.answer(text, reply_markup=category_keyboard())
    await state.set_state(OrderState.choosing_category)


# ========== ОБРАБОТЧИКИ ==========
@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    args = message.text.split()
    if len(args) > 1:
        deep_link = args[1]
        if deep_link.startswith("buy_stars_"):
            product_id = deep_link.replace("buy_stars_", "", 1)
            product = find_product_by_id(product_id)
            if product:
                order_id = generate_order_id()
                await orders.add_pending(order_id, {
                    "user_id": message.from_user.id, "user_name": message.from_user.full_name,
                    "product": product, "amount": product['price_stars'],
                    "currency": "⭐", "payment_method": "Telegram Stars",
                    "status": "pending", "created_at": time.time()
                })
                await bot.send_invoice(
                    chat_id=message.from_user.id,
                    title="ZAKAT — {}".format(product['name']),
                    description="{} на {}".format(product['name'], product['duration']),
                    payload="stars_{}".format(order_id),
                    provider_token="", currency="XTR",
                    prices=[LabeledPrice(label="XTR", amount=product['price_stars'])],
                    start_parameter="zakat_payment"
                )
                return
    await send_start_message(message, state)


@dp.callback_query(F.data == "buy_product")
async def buy_product(callback: types.CallbackQuery, state: FSMContext):
    await send_category_message(callback, state)
    await callback.answer()


@dp.callback_query(F.data == "about")
async def about_shop(callback: types.CallbackQuery):
    text = (
        "📋 <b>О магазине ZAKAT PREMIUM</b>\n\n"
        "🔥 <b>ZAKAT PREMIUM</b> — надёжный магазин цифровых товаров\n\n"
        "🛡️ <b>Наши преимущества:</b>\n"
        "• 💎 Только проверенные товары\n"
        "• ⚡ Моментальная выдача 24/7\n"
        "• 🔄 Гарантия замены\n"
        "• 💬 Быстрая поддержка\n"
        "• 🔒 Безопасные платежи\n\n"
        "📌 <b>Правила:</b>\n"
        "• Все продажи окончательны\n"
        "• Замена при проблемах — 24 часа\n"
        "• Менять данные аккаунтов запрещено\n\n"
        "💬 Поддержка: @{support}"
    ).format(support=Config.SUPPORT_CHAT_USERNAME)
    await callback.message.edit_text(text, reply_markup=about_keyboard())
    await callback.answer()


@dp.callback_query(F.data.startswith("category_"))
async def process_category(callback: types.CallbackQuery, state: FSMContext):
    category = callback.data.split("_")[1]
    if category not in ("prem", "sub", "key"):
        await callback.answer("❌ Ошибка", show_alert=True)
        return
    await state.update_data(category=category)
    info = {
        "prem": ("💎 <b>Premium аккаунты</b>",
                 "Доступ к лучшим сервисам мира.\nПолный функционал, проверенные аккаунты."),
        "sub": ("📱 <b>Подписки на сервисы</b>",
                "Netflix, Spotify, YouTube Premium и др.\nМоментальная активация."),
        "key": ("🔑 <b>Ключи активации</b>",
                "Windows, Office, антивирусы, софт.\nЛицензионные ключи.")
    }
    title, desc = info[category]
    text = (
        "{title}\n\n{desc}\n\n"
        "💰 <b>Выберите тариф:</b>"
    ).format(title=title, desc=desc)
    await callback.message.edit_text(text, reply_markup=subscription_keyboard(category))
    await state.set_state(OrderState.choosing_subscription)
    await callback.answer()


@dp.callback_query(F.data.startswith("sub_"))
async def process_subscription(callback: types.CallbackQuery, state: FSMContext):
    parts = callback.data.split("_")
    if len(parts) < 3:
        await callback.answer("❌ Ошибка", show_alert=True)
        return
    product = find_product_by_id("{}_{}".format(parts[1], parts[2]))
    if not product:
        await callback.answer("❌ Не найден", show_alert=True)
        return
    await state.update_data(selected_product=product)
    text = (
        "🛒 <b>Оформление заказа — ZAKAT PREMIUM</b>\n\n"
        "{emoji} <b>{name}</b>\n⏱️ {duration}\n\n"
        "💰 <b>Стоимость:</b>\n"
        "💳 Картой: {price} ₽\n"
        "⭐ Stars: {stars} ⭐\n"
        "₿ Крипта: {crypto} USDT\n"
        "💰 GOLD: {gold} 🪙\n"
        "🎨 NFT: {nft} 🖼️\n\n"
        "🎯 <b>Выберите способ оплаты:</b>"
    ).format(emoji=product['emoji'], name=product['name'], duration=product['duration'],
             price=product['price'], stars=product['price_stars'], crypto=product['price_crypto_usdt'],
             gold=product['price_gold'], nft=product['price_nft'])
    await callback.message.edit_text(text, reply_markup=payment_methods_keyboard(product))
    await state.set_state(OrderState.choosing_payment)
    await callback.answer()


# ========== ОПЛАТА КАРТОЙ ==========
@dp.callback_query(F.data.startswith("pay_yoomoney_"))
async def process_yoomoney_payment(callback: types.CallbackQuery):
    if not Config.YOOMONEY_WALLET:
        await callback.answer("❌ Недоступно", show_alert=True)
        return
    parts = callback.data.split("_")
    if len(parts) < 4:
        return
    product = find_product(parts[2], parts[3])
    if not product:
        return
    if not rate_limiter.check(callback.from_user.id):
        await callback.answer("⏳ Подождите...", show_alert=True)
        return
    order_id = generate_order_id()
    amount = product["price"]
    payment_url = create_payment_link(amount, order_id, "{} ({})".format(product['name'], product['duration']))
    await orders.add_pending(order_id, {
        "user_id": callback.from_user.id, "user_name": callback.from_user.full_name,
        "product": product, "amount": amount, "currency": "₽",
        "payment_method": "Картой", "status": "pending", "created_at": time.time()
    })
    text = (
        "💳 <b>Оплата картой — ZAKAT PREMIUM</b>\n\n"
        "{emoji} {name}\n⏱️ {dur}\n"
        "💰 <b>{amount} ₽</b>\n🆔 <code>{oid}</code>\n\n"
        "1️⃣ Нажмите «Оплатить»\n"
        "2️⃣ Оплатите картой\n3️⃣ Нажмите «Проверить»"
    ).format(emoji=product['emoji'], name=product['name'], dur=product['duration'], amount=amount, oid=order_id)
    await callback.message.edit_text(text, reply_markup=payment_keyboard(payment_url, order_id))
    await send_admin_notification(callback.from_user, product, "💳 Картой", "{} ₽".format(amount), order_id)
    await callback.answer()


# ========== ПРОВЕРКА ЮMONEY ==========
@dp.callback_query(F.data.startswith("checkym_"))
async def check_yoomoney_callback(callback: types.CallbackQuery):
    order_id = callback.data.replace("checkym_", "", 1)
    order = await orders.get_pending(order_id)
    if not order:
        if await orders.is_confirmed(order_id):
            await callback.answer("✅ Уже подтверждён!", show_alert=True)
        else:
            await callback.answer("❌ Не найден", show_alert=True)
        return
    if not rate_limiter.check(callback.from_user.id):
        await callback.answer("⏳ Подождите...", show_alert=True)
        return
    await callback.answer("🔍 Проверяем...")
    checking_msg = await callback.message.edit_text(
        "🔄 <b>Проверка платежа...</b>\n⏳ Подождите 15-25 секунд..."
    )
    payment_found = False
    for attempt in range(Config.MAX_PAYMENT_CHECK_ATTEMPTS):
        payment_found = await YooMoneyService.check_payment(order_id, order["amount"], order.get("created_at", time.time()))
        if payment_found:
            break
        await asyncio.sleep(Config.PAYMENT_CHECK_INTERVAL)
    if payment_found:
        success = await process_successful_payment(order_id, "Автопроверка")
        if success:
            await checking_msg.edit_text("✅ <b>Платеж найден!</b>\n📨 Проверьте сообщение ⬆️", reply_markup=support_keyboard())
        else:
            await checking_msg.edit_text("✅ Уже обработан", reply_markup=support_keyboard())
    else:
        product = order['product']
        payment_url = create_payment_link(order["amount"], order_id, "{} ({})".format(product['name'], product['duration']))
        await checking_msg.edit_text(
            "⏳ <b>Платеж не найден</b>\n\n💰 {amount} ₽\n🆔 <code>{oid}</code>\n\n⏰ Попробуйте через 1-2 мин".format(amount=order['amount'], oid=order_id),
            reply_markup=payment_keyboard(payment_url, order_id)
        )


# ========== ОПЛАТА STARS ==========
@dp.callback_query(F.data.startswith("pay_stars_"))
async def process_stars_payment(callback: types.CallbackQuery):
    parts = callback.data.split("_")
    if len(parts) < 4:
        return
    product = find_product(parts[2], parts[3])
    if not product:
        return
    if not rate_limiter.check(callback.from_user.id):
        await callback.answer("⏳ Подождите...", show_alert=True)
        return
    order_id = generate_order_id()
    await orders.add_pending(order_id, {
        "user_id": callback.from_user.id, "user_name": callback.from_user.full_name,
        "product": product, "amount": product['price_stars'], "currency": "⭐",
        "payment_method": "Telegram Stars", "status": "pending", "created_at": time.time()
    })
    await bot.send_invoice(
        chat_id=callback.from_user.id,
        title="ZAKAT — {}".format(product['name']),
        description="{} на {}".format(product['name'], product['duration']),
        payload="stars_{}".format(order_id),
        provider_token="", currency="XTR",
        prices=[LabeledPrice(label="XTR", amount=product['price_stars'])],
        start_parameter="zakat_payment"
    )
    await send_admin_notification(callback.from_user, product, "⭐ Stars", "{} ⭐".format(product['price_stars']), order_id)
    try:
        await callback.message.delete()
    except Exception:
        pass
    await callback.answer()


@dp.pre_checkout_query()
async def pre_checkout_query_handler(pcq: PreCheckoutQuery):
    await bot.answer_pre_checkout_query(pcq.id, ok=True)


@dp.message(F.successful_payment)
async def successful_payment(message: types.Message):
    payload = message.successful_payment.invoice_payload
    if payload.startswith("stars_"):
        await process_successful_payment(payload.replace("stars_", "", 1), "Telegram Stars")


# ========== ОПЛАТА КРИПТО ==========
@dp.callback_query(F.data.startswith("pay_crypto_"))
async def process_crypto_payment(callback: types.CallbackQuery):
    if not Config.CRYPTOBOT_TOKEN:
        await callback.answer("❌ Недоступно", show_alert=True)
        return
    parts = callback.data.split("_")
    if len(parts) < 4:
        return
    product = find_product(parts[2], parts[3])
    if not product:
        return
    if not rate_limiter.check(callback.from_user.id):
        await callback.answer("⏳ Подождите...", show_alert=True)
        return
    order_id = generate_order_id()
    amount_usdt = product["price_crypto_usdt"]
    invoice_data = await CryptoBotService.create_invoice(amount_usdt, order_id, "ZAKAT {} ({})".format(product['name'], product['duration']))
    if not invoice_data:
        await callback.answer("❌ Ошибка инвойса", show_alert=True)
        return
    await orders.add_pending(order_id, {
        "user_id": callback.from_user.id, "user_name": callback.from_user.full_name,
        "product": product, "amount": amount_usdt, "currency": "USDT",
        "payment_method": "CryptoBot", "status": "pending",
        "invoice_id": invoice_data["invoice_id"], "created_at": time.time()
    })
    text = (
        "₿ <b>Криптооплата — ZAKAT PREMIUM</b>\n\n"
        "{emoji} {name}\n⏱️ {dur}\n💰 <b>{amount} USDT</b>\n🆔 <code>{oid}</code>\n\n"
        "1️⃣ Нажмите «Оплатить»\n"
        "2️⃣ Выберите валюту\n3️⃣ Нажмите «Проверить»"
    ).format(emoji=product['emoji'], name=product['name'], dur=product['duration'], amount=amount_usdt, oid=order_id)
    await callback.message.edit_text(text, reply_markup=crypto_payment_keyboard(invoice_data["pay_url"], order_id))
    await send_admin_notification(callback.from_user, product, "₿ CryptoBot", "{} USDT".format(amount_usdt), order_id)
    await callback.answer()


@dp.callback_query(F.data.startswith("checkcr_"))
async def check_crypto_callback(callback: types.CallbackQuery):
    order_id = callback.data.replace("checkcr_", "", 1)
    order = await orders.get_pending(order_id)
    if not order:
        if await orders.is_confirmed(order_id):
            await callback.answer("✅ Уже оплачено!", show_alert=True)
        else:
            await callback.answer("❌ Не найден", show_alert=True)
        return
    if not rate_limiter.check(callback.from_user.id):
        await callback.answer("⏳", show_alert=True)
        return
    await callback.answer("🔍 Проверяем...")
    invoice_id = order.get("invoice_id")
    if not invoice_id:
        return
    if await CryptoBotService.check_invoice(invoice_id):
        success = await process_successful_payment(order_id, "CryptoBot")
        if success:
            await callback.message.edit_text("✅ <b>Криптоплатеж подтверждён!</b>\n📨 Ключ отправлен ⬆️", reply_markup=support_keyboard())
    else:
        await callback.answer("⏳ Не подтверждён. Попробуйте через минуту.", show_alert=True)


# ========== ОПЛАТА GOLD / NFT ==========
@dp.callback_query(F.data.startswith("pay_gold_"))
async def process_gold_payment(callback: types.CallbackQuery):
    await _process_manual_payment(callback, "gold")


@dp.callback_query(F.data.startswith("pay_nft_"))
async def process_nft_payment(callback: types.CallbackQuery):
    await _process_manual_payment(callback, "nft")


async def _process_manual_payment(callback, method):
    parts = callback.data.split("_")
    if len(parts) < 4:
        return
    product = find_product(parts[2], parts[3])
    if not product:
        return
    if not rate_limiter.check(callback.from_user.id):
        await callback.answer("⏳", show_alert=True)
        return
    cfg = {
        "gold": {"name": "GOLD", "icon": "💰", "price_key": "price_gold", "emoji": "🪙"},
        "nft": {"name": "NFT", "icon": "🎨", "price_key": "price_nft", "emoji": "🖼️"}
    }[method]
    price = product[cfg["price_key"]]
    chat_message = (
        "Привет! Хочу купить в ZAKAT PREMIUM: "
        "{name} на {period} ({platform}). "
        "Готов купить за {price} {method}"
    ).format(name=product['name'], period=product['period_text'], platform=product['platform'], price=price, method=cfg['name'])
    support_url = "https://t.me/{}?text={}".format(Config.SUPPORT_CHAT_USERNAME, quote(chat_message, safe=''))
    order_id = generate_order_id()
    await orders.add_pending(order_id, {
        "user_id": callback.from_user.id, "user_name": callback.from_user.full_name,
        "product": product, "amount": price, "currency": cfg["name"],
        "payment_method": cfg["name"], "status": "pending", "created_at": time.time()
    })
    text = (
        "{icon} <b>Оплата {mname} — ZAKAT PREMIUM</b>\n\n"
        "{emoji} {pname}\n⏱️ {dur}\n💰 <b>{price} {mname}</b>\n\n"
        "📝 <b>Сообщение:</b>\n<code>{msg}</code>\n\n"
        "1️⃣ Напишите менеджеру @{support}\n"
        "2️⃣ Ожидайте обработки"
    ).format(icon=cfg['icon'], mname=cfg['name'], emoji=product['emoji'], pname=product['name'],
             dur=product['duration'], price=price, msg=chat_message, support=Config.SUPPORT_CHAT_USERNAME)
    await callback.message.edit_text(text, reply_markup=manual_payment_keyboard(support_url, "{}_sent".format(method)))
    await send_admin_notification(callback.from_user, product, "{} {}".format(cfg['icon'], cfg['name']), "{} {}".format(price, cfg['emoji']), order_id)
    await callback.answer()


@dp.callback_query(F.data.in_({"gold_sent", "nft_sent"}))
async def manual_payment_sent(callback: types.CallbackQuery):
    mname = "GOLD" if callback.data == "gold_sent" else "NFT"
    icon = "💰" if callback.data == "gold_sent" else "🎨"
    text = (
        "✅ <b>Отлично!</b>\n\n"
        "{icon} Ваш {mname} заказ принят\n"
        "⏱️ Обработка: до 30 мин\n\n"
        "💬 Менеджер: @{support}"
    ).format(icon=icon, mname=mname, support=Config.SUPPORT_CHAT_USERNAME)
    await callback.message.edit_text(text, reply_markup=support_keyboard())
    await callback.answer()


# ========== АДМИН ==========
@dp.callback_query(F.data.startswith("admin_confirm_"))
async def admin_confirm(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("❌", show_alert=True)
        return
    order_id = callback.data.replace("admin_confirm_", "", 1)
    success = await process_successful_payment(order_id, "👨‍💼 Админ")
    if success:
        await callback.message.edit_text(
            "✅ <b>Подтверждён</b>\n🆔 {}\n👨‍💼 {}".format(order_id, callback.from_user.full_name))
        await callback.answer("✅")
    else:
        await callback.answer("❌ Не найден", show_alert=True)


@dp.callback_query(F.data.startswith("admin_reject_"))
async def admin_reject(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("❌", show_alert=True)
        return
    order_id = callback.data.replace("admin_reject_", "", 1)
    order = await orders.remove_pending(order_id)
    if order:
        await callback.message.edit_text("❌ <b>Отклонён</b>\n🆔 {}".format(order_id))
        try:
            await bot.send_message(order['user_id'],
                "❌ <b>Заказ отклонён</b>\n💬 Свяжитесь с @{}".format(Config.SUPPORT_CHAT_USERNAME))
        except Exception:
            pass
    await callback.answer("❌")


@dp.message(Command("orders"))
async def cmd_orders(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    stats = await orders.get_stats()
    text = "📊 <b>СТАТИСТИКА ZAKAT PREMIUM</b>\n\n⏳ Ожидают: {}\n".format(stats['pending'])
    for oid, order in await orders.get_recent_pending(5):
        t = datetime.fromtimestamp(order['created_at']).strftime('%H:%M')
        text += "• {} | {} | {}\n".format(t, order['user_name'], order['product']['name'])
    text += "\n✅ Подтверждено: {}\n".format(stats['confirmed'])
    balance = await YooMoneyService.get_balance()
    if balance is not None:
        text += "💰 Баланс YooMoney: {} ₽".format(balance)
    await message.answer(text)


@dp.message(Command("help"))
async def cmd_help(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    await message.answer(
        "<b>🔐 Команды ZAKAT PREMIUM:</b>\n\n"
        "/orders — Статистика заказов\n"
        "/help — Справка"
    )


# ========== НАВИГАЦИЯ ==========
@dp.callback_query(F.data == "restart")
async def restart_order(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await send_start_message(callback, state)
    await callback.answer()


@dp.callback_query(F.data == "back_to_start")
async def back_to_start(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await send_start_message(callback, state)
    await callback.answer()


@dp.callback_query(F.data == "back_to_subscription")
async def back_to_subscription(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    category = data.get("category", "prem")
    titles = {
        "prem": "💎 <b>Premium аккаунты</b>",
        "sub": "📱 <b>Подписки на сервисы</b>",
        "key": "🔑 <b>Ключи активации</b>"
    }
    text = "{}\n\n💰 <b>Выберите тариф:</b>".format(titles.get(category, "ZAKAT PREMIUM"))
    await callback.message.edit_text(text, reply_markup=subscription_keyboard(category))
    await state.set_state(OrderState.choosing_subscription)
    await callback.answer()


# ========== ЗАПУСК ==========
async def main():
    logger.info("=" * 50)
    logger.info("ZAKAT PREMIUM SHOP BOT")
    logger.info("=" * 50)
    logger.info("ADMIN_IDS: %s", Config.ADMIN_IDS)
    logger.info("SUPPORT: @%s", Config.SUPPORT_CHAT_USERNAME)

    try:
        me = await bot.get_me()
        logger.info("Bot: @%s", me.username)
        for key, product in PRODUCTS.items():
            logger.info("%s %s (%s) — %s RUB / %s Stars / %s Gold",
                        product['emoji'], product['name'], product['duration'],
                        product['price'], product['price_stars'], product['price_gold'])
        logger.info("Bot starting polling...")
        await dp.start_polling(bot)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    except Exception as e:
        logger.error("Fatal error: %s", e)
        import traceback
        traceback.print_exc()
    finally:
        await bot.session.close()
        logger.info("Bot stopped")


if __name__ == "__main__":
    asyncio.run(main())
