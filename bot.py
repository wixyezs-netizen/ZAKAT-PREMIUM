# bot.py — PMT Premium Cheat Shop
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
    BOT_TOKEN: str = os.environ.get("BOT_TOKEN", "8434646887:AAFs0Me2Vl3mNy81rV-nDTCQZxfO6N-dpBU")
    CRYPTOBOT_TOKEN: str = os.environ.get("CRYPTOBOT_TOKEN", "493276:AAtS7R1zYy0gaPw8eax1EgiWo0tdnd6dQ9c")
    YOOMONEY_ACCESS_TOKEN: str = os.environ.get("YOOMONEY_ACCESS_TOKEN", "4100118889570559.3288B2E716CEEB922A26BD6BEAC58648FBFB680CCF64E4E1447D714D6FB5EA5F01F1478FAC686BEF394C8A186C98982DE563C1ABCDF9F2F61D971B61DA3C7E486CA818F98B9E0069F1C0891E090DD56A11319D626A40F0AE8302A8339DED9EB7969617F191D93275F64C4127A3ECB7AED33FCDE91CA68690EB7534C67E6C219E")
    YOOMONEY_WALLET: str = os.environ.get("YOOMONEY_WALLET", "4100118889570559")

    SUPPORT_CHAT_USERNAME = os.environ.get("SUPPORT_CHAT_USERNAME", "PMThelp")
    DOWNLOAD_URL = os.environ.get("DOWNLOAD_URL", "https://go.linkify.ru/2GPF")

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
            raise ValueError("BOT_TOKEN environment variable is required!")

        admin_ids_str = os.environ.get("ADMIN_ID", "")
        admin_ids_list = [
            int(x.strip())
            for x in admin_ids_str.split(",")
            if x.strip().isdigit()
        ]

        if not admin_ids_list:
            raise ValueError("ADMIN_ID environment variable is required!")

        cls.ADMIN_ID = admin_ids_list[0]
        cls.SUPPORT_CHAT_ID = (
            admin_ids_list[1]
            if len(admin_ids_list) >= 2
            else int(os.environ.get("SUPPORT_CHAT_ID", str(cls.ADMIN_ID)))
        )
        cls.ADMIN_IDS = set(admin_ids_list)

        if not cls.CRYPTOBOT_TOKEN:
            logger.warning("CRYPTOBOT_TOKEN not set - crypto payments disabled")
        if not cls.YOOMONEY_ACCESS_TOKEN:
            logger.warning("YOOMONEY_ACCESS_TOKEN not set - card payments disabled")
        if not cls.YOOMONEY_WALLET:
            logger.warning("YOOMONEY_WALLET not set - card payments disabled")


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
    "apk_week": {
        "name": "\U0001f4f1 PMT Android",
        "period_text": "\u041d\u0415\u0414\u0415\u041b\u042e",
        "price": 205,
        "price_stars": 250,
        "price_gold": 650,
        "price_nft": 500,
        "price_crypto_usdt": 3,
        "platform": "Android",
        "period": "\u041d\u0415\u0414\u0415\u041b\u042e",
        "platform_code": "apk",
        "emoji": "\U0001f4f1",
        "duration": "7 \u0434\u043d\u0435\u0439"
    },
    "apk_month": {
        "name": "\U0001f4f1 PMT Android",
        "period_text": "\u041c\u0415\u0421\u042f\u0426",
        "price": 450,
        "price_stars": 450,
        "price_gold": 1200,
        "price_nft": 1000,
        "price_crypto_usdt": 6,
        "platform": "Android",
        "period": "\u041c\u0415\u0421\u042f\u0426",
        "platform_code": "apk",
        "emoji": "\U0001f4f1",
        "duration": "30 \u0434\u043d\u0435\u0439"
    },
    "apk_forever": {
        "name": "\U0001f4f1 PMT Android",
        "period_text": "\u041d\u0410\u0412\u0421\u0415\u0413\u0414\u0410",
        "price": 890,
        "price_stars": 900,
        "price_gold": 2200,
        "price_nft": 1800,
        "price_crypto_usdt": 12,
        "platform": "Android",
        "period": "\u041d\u0410\u0412\u0421\u0415\u0413\u0414\u0410",
        "platform_code": "apk",
        "emoji": "\U0001f4f1",
        "duration": "\u041d\u0430\u0432\u0441\u0435\u0433\u0434\u0430"
    },
    "ios_week": {
        "name": "\U0001f34e PMT iOS",
        "period_text": "\u041d\u0415\u0414\u0415\u041b\u042e",
        "price": 359,
        "price_stars": 350,
        "price_gold": 700,
        "price_nft": 550,
        "price_crypto_usdt": 5,
        "platform": "iOS",
        "period": "\u041d\u0415\u0414\u0415\u041b\u042e",
        "platform_code": "ios",
        "emoji": "\U0001f34e",
        "duration": "7 \u0434\u043d\u0435\u0439"
    },
    "ios_month": {
        "name": "\U0001f34e PMT iOS",
        "period_text": "\u041c\u0415\u0421\u042f\u0426",
        "price": 750,
        "price_stars": 750,
        "price_gold": 1400,
        "price_nft": 1200,
        "price_crypto_usdt": 10,
        "platform": "iOS",
        "period": "\u041c\u0415\u0421\u042f\u0426",
        "platform_code": "ios",
        "emoji": "\U0001f34e",
        "duration": "30 \u0434\u043d\u0435\u0439"
    },
    "ios_forever": {
        "name": "\U0001f34e PMT iOS",
        "period_text": "\u041d\u0410\u0412\u0421\u0415\u0413\u0414\u0410",
        "price": 1400,
        "price_stars": 1400,
        "price_gold": 2500,
        "price_nft": 2200,
        "price_crypto_usdt": 18,
        "platform": "iOS",
        "period": "\u041d\u0410\u0412\u0421\u0415\u0413\u0414\u0410",
        "platform_code": "ios",
        "emoji": "\U0001f34e",
        "duration": "\u041d\u0430\u0432\u0441\u0435\u0433\u0434\u0430"
    }
}


# ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==========
def generate_order_id():
    raw = "{}_{}_{}" .format(time.time(), random.randint(100000, 999999), os.urandom(4).hex())
    return hashlib.sha256(raw.encode()).hexdigest()[:12]


def generate_license_key(order_id, user_id):
    raw = "{}_{}_{}" .format(order_id, user_id, os.urandom(8).hex())
    h = hashlib.sha256(raw.encode()).hexdigest()[:16].upper()
    return "PMT-{}-{}-{}-{}".format(h[:4], h[4:8], h[8:12], h[12:16])


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
    comment = "\u0417\u0430\u043a\u0430\u0437 {}: {}".format(order_id, product_name)
    safe_targets = quote(comment, safe='')
    success_url = quote('https://t.me/pmt_bot?start=success', safe='')
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
            "paid_btn_url": "https://t.me/pmt_bot?start=paid_{}".format(order_id)
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
    choosing_platform = State()
    choosing_subscription = State()
    choosing_payment = State()


# ========== КЛАВИАТУРЫ ==========
def start_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="\U0001f3ae \u041a\u0443\u043f\u0438\u0442\u044c \u0447\u0438\u0442 Standoff 2", callback_data="buy_cheat")],
        [InlineKeyboardButton(text="\u2139\ufe0f \u041e \u043f\u0440\u043e\u0433\u0440\u0430\u043c\u043c\u0435", callback_data="about")],
        [InlineKeyboardButton(text="\U0001f4ac \u041f\u043e\u0434\u0434\u0435\u0440\u0436\u043a\u0430", url="https://t.me/{}".format(Config.SUPPORT_CHAT_USERNAME))]
    ])


def platform_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="\U0001f4f1 Android", callback_data="platform_apk")],
        [InlineKeyboardButton(text="\U0001f34e iOS", callback_data="platform_ios")],
        [InlineKeyboardButton(text="\u25c0\ufe0f \u041d\u0430\u0437\u0430\u0434", callback_data="back_to_start")]
    ])


def subscription_keyboard(platform):
    prices = {
        "apk": [
            ("\u26a1 \u041d\u0415\u0414\u0415\u041b\u042f \u2014 205\u20bd", "sub_apk_week"),
            ("\U0001f525 \u041c\u0415\u0421\u042f\u0426 \u2014 450\u20bd", "sub_apk_month"),
            ("\U0001f48e \u041d\u0410\u0412\u0421\u0415\u0413\u0414\u0410 \u2014 890\u20bd", "sub_apk_forever"),
        ],
        "ios": [
            ("\u26a1 \u041d\u0415\u0414\u0415\u041b\u042f \u2014 359\u20bd", "sub_ios_week"),
            ("\U0001f525 \u041c\u0415\u0421\u042f\u0426 \u2014 750\u20bd", "sub_ios_month"),
            ("\U0001f48e \u041d\u0410\u0412\u0421\u0415\u0413\u0414\u0410 \u2014 1400\u20bd", "sub_ios_forever"),
        ]
    }
    buttons = [[InlineKeyboardButton(text=text, callback_data=cb)] for text, cb in prices.get(platform, [])]
    buttons.append([InlineKeyboardButton(text="\u25c0\ufe0f \u041d\u0430\u0437\u0430\u0434", callback_data="buy_cheat")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def payment_methods_keyboard(product):
    pc = product['platform_code']
    p = product['period']
    buttons = [
        [InlineKeyboardButton(text="\U0001f4b3 \u041a\u0430\u0440\u0442\u043e\u0439", callback_data="pay_yoomoney_{}_{}".format(pc, p))],
        [InlineKeyboardButton(text="\u2b50 Telegram Stars", callback_data="pay_stars_{}_{}".format(pc, p))],
        [InlineKeyboardButton(text="\u20bf \u041a\u0440\u0438\u043f\u0442\u043e\u0431\u043e\u0442", callback_data="pay_crypto_{}_{}".format(pc, p))],
        [InlineKeyboardButton(text="\U0001f4b0 GOLD", callback_data="pay_gold_{}_{}".format(pc, p))],
        [InlineKeyboardButton(text="\U0001f3a8 NFT", callback_data="pay_nft_{}_{}".format(pc, p))],
        [InlineKeyboardButton(text="\u25c0\ufe0f \u041d\u0430\u0437\u0430\u0434", callback_data="back_to_subscription")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def payment_keyboard(payment_url, order_id):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="\U0001f4b3 \u041e\u043f\u043b\u0430\u0442\u0438\u0442\u044c \u043a\u0430\u0440\u0442\u043e\u0439", url=payment_url)],
        [InlineKeyboardButton(text="\u2705 \u041f\u0440\u043e\u0432\u0435\u0440\u0438\u0442\u044c \u043e\u043f\u043b\u0430\u0442\u0443", callback_data="checkym_{}".format(order_id))],
        [InlineKeyboardButton(text="\u274c \u041e\u0442\u043c\u0435\u043d\u0430", callback_data="restart")]
    ])


def crypto_payment_keyboard(invoice_url, order_id):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="\u20bf \u041e\u043f\u043b\u0430\u0442\u0438\u0442\u044c \u043a\u0440\u0438\u043f\u0442\u043e\u0439", url=invoice_url)],
        [InlineKeyboardButton(text="\u2705 \u041f\u0440\u043e\u0432\u0435\u0440\u0438\u0442\u044c \u043f\u043b\u0430\u0442\u0435\u0436", callback_data="checkcr_{}".format(order_id))],
        [InlineKeyboardButton(text="\u274c \u041e\u0442\u043c\u0435\u043d\u0430", callback_data="restart")]
    ])


def support_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="\U0001f4ac \u041f\u043e\u0434\u0434\u0435\u0440\u0436\u043a\u0430", url="https://t.me/{}".format(Config.SUPPORT_CHAT_USERNAME))],
        [InlineKeyboardButton(text="\U0001f504 \u041d\u043e\u0432\u0430\u044f \u043f\u043e\u043a\u0443\u043f\u043a\u0430", callback_data="restart")]
    ])


def download_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="\U0001f4e5 \u0421\u043a\u0430\u0447\u0430\u0442\u044c PMT", url=Config.DOWNLOAD_URL)],
        [InlineKeyboardButton(text="\U0001f4ac \u041f\u043e\u0434\u0434\u0435\u0440\u0436\u043a\u0430", url="https://t.me/{}".format(Config.SUPPORT_CHAT_USERNAME))],
        [InlineKeyboardButton(text="\U0001f504 \u041d\u043e\u0432\u0430\u044f \u043f\u043e\u043a\u0443\u043f\u043a\u0430", callback_data="restart")]
    ])


def about_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="\u25c0\ufe0f \u041d\u0430\u0437\u0430\u0434", callback_data="back_to_start")]
    ])


def admin_confirm_keyboard(order_id):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="\u2705 \u041f\u043e\u0434\u0442\u0432\u0435\u0440\u0434\u0438\u0442\u044c", callback_data="admin_confirm_{}".format(order_id))],
        [InlineKeyboardButton(text="\u274c \u041e\u0442\u043a\u043b\u043e\u043d\u0438\u0442\u044c", callback_data="admin_reject_{}".format(order_id))]
    ])


def manual_payment_keyboard(support_url, sent_callback):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="\U0001f4ac \u041f\u0435\u0440\u0435\u0439\u0442\u0438 \u043a \u043e\u043f\u043b\u0430\u0442\u0435", url=support_url)],
        [InlineKeyboardButton(text="\u2705 \u042f \u043d\u0430\u043f\u0438\u0441\u0430\u043b", callback_data=sent_callback)],
        [InlineKeyboardButton(text="\u274c \u041e\u0442\u043c\u0435\u043d\u0430", callback_data="restart")]
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
        "\U0001f389 <b>\u041e\u043f\u043b\u0430\u0442\u0430 \u043f\u043e\u0434\u0442\u0432\u0435\u0440\u0436\u0434\u0435\u043d\u0430!</b>\n\n"
        "\u2728 \u0414\u043e\u0431\u0440\u043e \u043f\u043e\u0436\u0430\u043b\u043e\u0432\u0430\u0442\u044c \u0432 PMT!\n\n"
        "\U0001f4e6 <b>\u0412\u0430\u0448\u0430 \u043f\u043e\u043a\u0443\u043f\u043a\u0430:</b>\n"
        "{emoji} {name}\n"
        "\u23f1\ufe0f \u0421\u0440\u043e\u043a: {duration}\n"
        "\U0001f50d \u041c\u0435\u0442\u043e\u0434: {source}\n\n"
        "\U0001f511 <b>\u0412\u0430\u0448 \u043b\u0438\u0446\u0435\u043d\u0437\u0438\u043e\u043d\u043d\u044b\u0439 \u043a\u043b\u044e\u0447:</b>\n"
        "<code>{key}</code>\n\n"
        "\U0001f4e5 <b>\u0421\u043a\u0430\u0447\u0438\u0432\u0430\u043d\u0438\u0435:</b>\n"
        "\U0001f447 \u041d\u0430\u0436\u043c\u0438\u0442\u0435 \u043a\u043d\u043e\u043f\u043a\u0443 \u043d\u0438\u0436\u0435\n\n"
        "\U0001f4ab <b>\u0410\u043a\u0442\u0438\u0432\u0430\u0446\u0438\u044f:</b>\n"
        "1\ufe0f\u20e3 \u0421\u043a\u0430\u0447\u0430\u0439\u0442\u0435 \u0444\u0430\u0439\u043b\n"
        "2\ufe0f\u20e3 \u0423\u0441\u0442\u0430\u043d\u043e\u0432\u0438\u0442\u0435 \u043f\u0440\u0438\u043b\u043e\u0436\u0435\u043d\u0438\u0435\n"
        "3\ufe0f\u20e3 \u0412\u0432\u0435\u0434\u0438\u0442\u0435 \u043a\u043b\u044e\u0447\n"
        "4\ufe0f\u20e3 \u041d\u0430\u0441\u043b\u0430\u0436\u0434\u0430\u0439\u0442\u0435\u0441\u044c \u0438\u0433\u0440\u043e\u0439! \U0001f3ae\n\n"
        "\U0001f4ac \u041f\u043e\u0434\u0434\u0435\u0440\u0436\u043a\u0430: @{support}"
    ).format(emoji=product['emoji'], name=product['name'], duration=product['duration'],
             source=source, key=license_key, support=Config.SUPPORT_CHAT_USERNAME)

    try:
        await bot.send_message(user_id, success_text, reply_markup=download_keyboard())
    except Exception as e:
        logger.error("Error sending to user %s: %s", user_id, e)

    now_str = datetime.now().strftime('%d.%m.%Y %H:%M')
    admin_text = (
        "\U0001f48e <b>\u041d\u041e\u0412\u0410\u042f \u041f\u0420\u041e\u0414\u0410\u0416\u0410 ({source})</b>\n\n"
        "\U0001f464 {user_name}\n\U0001f194 {user_id}\n"
        "\U0001f4e6 {pname} ({dur})\n\U0001f4b0 {amount} {currency}\n"
        "\U0001f511 <code>{key}</code>\n\U0001f4c5 {now}"
    ).format(source=source, user_name=order['user_name'], user_id=user_id,
             pname=product['name'], dur=product['duration'],
             amount=order.get('amount', product['price']), currency=order.get('currency', '\u20bd'),
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
        "\U0001f514 <b>\u041d\u041e\u0412\u042b\u0419 \u0417\u0410\u041a\u0410\u0417</b>\n\n"
        "\U0001f464 {fn}\n\U0001f194 <code>{uid}</code>\n"
        "\U0001f4e6 {pn} ({dur})\n\U0001f4b0 {price}\n\U0001f4b3 {pm}\n"
        "\U0001f194 <code>{oid}</code>\n\n\U0001f4c5 {now}"
    ).format(fn=user.full_name, uid=user.id, pn=product['name'], dur=product['duration'],
             price=price, pm=payment_method, oid=order_id, now=now_str)
    for aid in Config.ADMIN_IDS:
        try:
            await bot.send_message(aid, message, reply_markup=admin_confirm_keyboard(order_id))
        except Exception as e:
            logger.error("Error sending to admin %s: %s", aid, e)


async def send_start_message(target, state):
    text = (
        "\U0001f3af <b>PMT \u2014 \u041f\u0440\u0435\u043c\u0438\u0443\u043c \u0447\u0438\u0442 \u0434\u043b\u044f Standoff 2</b>\n\n"
        "\u2728 <b>\u0412\u043e\u0437\u043c\u043e\u0436\u043d\u043e\u0441\u0442\u0438:</b>\n"
        "\U0001f6e1\ufe0f \u041f\u0440\u043e\u0434\u0432\u0438\u043d\u0443\u0442\u0430\u044f \u0437\u0430\u0449\u0438\u0442\u0430 \u043e\u0442 \u0431\u0430\u043d\u043e\u0432\n"
        "\U0001f3af \u0423\u043c\u043d\u044b\u0439 AimBot \u0441 \u043d\u0430\u0441\u0442\u0440\u043e\u0439\u043a\u0430\u043c\u0438\n"
        "\U0001f441\ufe0f WallHack \u0438 ESP\n"
        "\U0001f4ca \u041f\u043e\u043b\u043d\u0430\u044f \u0438\u043d\u0444\u043e\u0440\u043c\u0430\u0446\u0438\u044f \u043e \u043f\u0440\u043e\u0442\u0438\u0432\u043d\u0438\u043a\u0430\u0445\n"
        "\u26a1 \u0411\u044b\u0441\u0442\u0440\u044b\u0435 \u043e\u0431\u043d\u043e\u0432\u043b\u0435\u043d\u0438\u044f\n\n"
        "\U0001f680 <b>\u041d\u0430\u0436\u043c\u0438\u0442\u0435 \u043a\u043d\u043e\u043f\u043a\u0443 \u043d\u0438\u0436\u0435 \u0434\u043b\u044f \u043f\u043e\u043a\u0443\u043f\u043a\u0438:</b>"
    )
    if isinstance(target, types.Message):
        await target.answer(text, reply_markup=start_keyboard())
    elif isinstance(target, types.CallbackQuery):
        try:
            await target.message.edit_text(text, reply_markup=start_keyboard())
        except Exception:
            await target.message.answer(text, reply_markup=start_keyboard())
    await state.set_state(OrderState.main_menu)


async def send_platform_message(target, state):
    text = (
        "\U0001f3ae <b>\u0412\u044b\u0431\u0435\u0440\u0438\u0442\u0435 \u043f\u043b\u0430\u0442\u0444\u043e\u0440\u043c\u0443:</b>\n\n"
        "\U0001f4f1 <b>Android</b> \u2014 APK \u0444\u0430\u0439\u043b\n"
        "\U0001f34e <b>iOS</b> \u2014 IPA \u0444\u0430\u0439\u043b"
    )
    if isinstance(target, types.CallbackQuery):
        try:
            await target.message.edit_text(text, reply_markup=platform_keyboard())
        except Exception:
            await target.message.answer(text, reply_markup=platform_keyboard())
    await state.set_state(OrderState.choosing_platform)


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
                    "currency": "\u2b50", "payment_method": "Telegram Stars",
                    "status": "pending", "created_at": time.time()
                })
                await bot.send_invoice(
                    chat_id=message.from_user.id,
                    title="PMT \u2014 {}".format(product['name']),
                    description="\u041f\u043e\u0434\u043f\u0438\u0441\u043a\u0430 \u043d\u0430 {} \u0434\u043b\u044f {}".format(product['duration'], product['platform']),
                    payload="stars_{}".format(order_id),
                    provider_token="", currency="XTR",
                    prices=[LabeledPrice(label="XTR", amount=product['price_stars'])],
                    start_parameter="pmt_payment"
                )
                return
    await send_start_message(message, state)


@dp.callback_query(F.data == "buy_cheat")
async def buy_cheat(callback: types.CallbackQuery, state: FSMContext):
    await send_platform_message(callback, state)
    await callback.answer()


@dp.callback_query(F.data == "about")
async def about_cheat(callback: types.CallbackQuery):
    text = (
        "\U0001f4cb <b>\u041f\u043e\u0434\u0440\u043e\u0431\u043d\u0430\u044f \u0438\u043d\u0444\u043e\u0440\u043c\u0430\u0446\u0438\u044f</b>\n\n"
        "\U0001f3ae <b>\u041d\u0430\u0437\u0432\u0430\u043d\u0438\u0435:</b> PMT\n"
        "\U0001f525 <b>\u0421\u0442\u0430\u0442\u0443\u0441:</b> \u0410\u043a\u0442\u0438\u0432\u043d\u043e \u043e\u0431\u043d\u043e\u0432\u043b\u044f\u0435\u0442\u0441\u044f\n\n"
        "\U0001f6e0\ufe0f <b>\u0424\u0443\u043d\u043a\u0446\u0438\u043e\u043d\u0430\u043b:</b>\n"
        "\u2022 \U0001f3af \u0423\u043c\u043d\u044b\u0439 AimBot\n\u2022 \U0001f441\ufe0f WallHack\n"
        "\u2022 \U0001f4cd ESP\n\u2022 \U0001f5fa\ufe0f \u041c\u0438\u043d\u0438-\u0440\u0430\u0434\u0430\u0440\n"
        "\u2022 \u2699\ufe0f \u0413\u0438\u0431\u043a\u0438\u0435 \u043d\u0430\u0441\u0442\u0440\u043e\u0439\u043a\u0438\n\n"
        "\U0001f6e1\ufe0f <b>\u0411\u0435\u0437\u043e\u043f\u0430\u0441\u043d\u043e\u0441\u0442\u044c:</b>\n"
        "\u2022 \u041e\u0431\u0445\u043e\u0434 \u0430\u043d\u0442\u0438\u0447\u0438\u0442\u043e\u0432\n\u2022 \u0420\u0435\u0433\u0443\u043b\u044f\u0440\u043d\u044b\u0435 \u043e\u0431\u043d\u043e\u0432\u043b\u0435\u043d\u0438\u044f\n\n"
        "\U0001f4ac \u041f\u043e\u0434\u0434\u0435\u0440\u0436\u043a\u0430: @{support}"
    ).format(support=Config.SUPPORT_CHAT_USERNAME)
    await callback.message.edit_text(text, reply_markup=about_keyboard())
    await callback.answer()


@dp.callback_query(F.data.startswith("platform_"))
async def process_platform(callback: types.CallbackQuery, state: FSMContext):
    platform = callback.data.split("_")[1]
    if platform not in ("apk", "ios"):
        await callback.answer("\u274c \u041e\u0448\u0438\u0431\u043a\u0430", show_alert=True)
        return
    await state.update_data(platform=platform)
    info = {
        "apk": ("\U0001f4f1 <b>PMT Android</b>",
                "\u2022 Android 10.0+\n\u2022 2 \u0413\u0411 \u043f\u0430\u043c\u044f\u0442\u0438\n\u2022 Root \u043d\u0435 \u043d\u0443\u0436\u0435\u043d",
                "\u2022 APK \u0444\u0430\u0439\u043b\n\u2022 \u0418\u043d\u0441\u0442\u0440\u0443\u043a\u0446\u0438\u044f\n\u2022 \u041f\u043e\u0434\u0434\u0435\u0440\u0436\u043a\u0430"),
        "ios": ("\U0001f34e <b>PMT iOS</b>",
                "\u2022 iOS 14.0 - 18.0\n\u2022 AltStore\n\u2022 Jailbreak \u043d\u0435 \u043d\u0443\u0436\u0435\u043d",
                "\u2022 IPA \u0444\u0430\u0439\u043b\n\u2022 \u0418\u043d\u0441\u0442\u0440\u0443\u043a\u0446\u0438\u044f\n\u2022 \u041f\u043e\u043c\u043e\u0449\u044c")
    }
    title, reqs, incl = info[platform]
    text = (
        "{title}\n\n\U0001f527 <b>\u0422\u0440\u0435\u0431\u043e\u0432\u0430\u043d\u0438\u044f:</b>\n{reqs}\n\n"
        "\U0001f4e6 <b>\u0427\u0442\u043e \u0432\u0445\u043e\u0434\u0438\u0442:</b>\n{incl}\n\n"
        "\U0001f4b0 <b>\u0412\u044b\u0431\u0435\u0440\u0438\u0442\u0435 \u0442\u0430\u0440\u0438\u0444:</b>"
    ).format(title=title, reqs=reqs, incl=incl)
    await callback.message.edit_text(text, reply_markup=subscription_keyboard(platform))
    await state.set_state(OrderState.choosing_subscription)
    await callback.answer()


@dp.callback_query(F.data.startswith("sub_"))
async def process_subscription(callback: types.CallbackQuery, state: FSMContext):
    parts = callback.data.split("_")
    if len(parts) < 3:
        await callback.answer("\u274c \u041e\u0448\u0438\u0431\u043a\u0430", show_alert=True)
        return
    product = find_product_by_id("{}_{}".format(parts[1], parts[2]))
    if not product:
        await callback.answer("\u274c \u041d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d", show_alert=True)
        return
    await state.update_data(selected_product=product)
    text = (
        "\U0001f6d2 <b>\u041e\u0444\u043e\u0440\u043c\u043b\u0435\u043d\u0438\u0435</b>\n\n"
        "{emoji} <b>{name}</b>\n\u23f1\ufe0f {duration}\n\n"
        "\U0001f48e <b>\u0421\u0442\u043e\u0438\u043c\u043e\u0441\u0442\u044c:</b>\n"
        "\U0001f4b3 \u041a\u0430\u0440\u0442\u043e\u0439: {price} \u20bd\n"
        "\u2b50 Stars: {stars} \u2b50\n"
        "\u20bf \u041a\u0440\u0438\u043f\u0442\u0430: {crypto} USDT\n"
        "\U0001f4b0 GOLD: {gold} \U0001fa99\n"
        "\U0001f3a8 NFT: {nft} \U0001f5bc\ufe0f\n\n"
        "\U0001f3af <b>\u0421\u043f\u043e\u0441\u043e\u0431 \u043e\u043f\u043b\u0430\u0442\u044b:</b>"
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
        await callback.answer("\u274c \u041d\u0435\u0434\u043e\u0441\u0442\u0443\u043f\u043d\u043e", show_alert=True)
        return
    parts = callback.data.split("_")
    if len(parts) < 4:
        return
    product = find_product(parts[2], parts[3])
    if not product:
        return
    if not rate_limiter.check(callback.from_user.id):
        await callback.answer("\u23f3 \u041f\u043e\u0434\u043e\u0436\u0434\u0438\u0442\u0435...", show_alert=True)
        return
    order_id = generate_order_id()
    amount = product["price"]
    payment_url = create_payment_link(amount, order_id, "{} ({})".format(product['name'], product['duration']))
    await orders.add_pending(order_id, {
        "user_id": callback.from_user.id, "user_name": callback.from_user.full_name,
        "product": product, "amount": amount, "currency": "\u20bd",
        "payment_method": "\u041a\u0430\u0440\u0442\u043e\u0439", "status": "pending", "created_at": time.time()
    })
    text = (
        "\U0001f4b3 <b>\u041e\u043f\u043b\u0430\u0442\u0430 \u043a\u0430\u0440\u0442\u043e\u0439</b>\n\n"
        "{emoji} {name}\n\u23f1\ufe0f {dur}\n"
        "\U0001f4b0 <b>{amount} \u20bd</b>\n\U0001f194 <code>{oid}</code>\n\n"
        "1\ufe0f\u20e3 \u041d\u0430\u0436\u043c\u0438\u0442\u0435 \u00ab\u041e\u043f\u043b\u0430\u0442\u0438\u0442\u044c\u00bb\n"
        "2\ufe0f\u20e3 \u041e\u043f\u043b\u0430\u0442\u0438\u0442\u0435\n3\ufe0f\u20e3 \u041d\u0430\u0436\u043c\u0438\u0442\u0435 \u00ab\u041f\u0440\u043e\u0432\u0435\u0440\u0438\u0442\u044c\u00bb"
    ).format(emoji=product['emoji'], name=product['name'], dur=product['duration'], amount=amount, oid=order_id)
    await callback.message.edit_text(text, reply_markup=payment_keyboard(payment_url, order_id))
    await send_admin_notification(callback.from_user, product, "\U0001f4b3 \u041a\u0430\u0440\u0442\u043e\u0439", "{} \u20bd".format(amount), order_id)
    await callback.answer()


# ========== ПРОВЕРКА ЮMONEY ==========
@dp.callback_query(F.data.startswith("checkym_"))
async def check_yoomoney_callback(callback: types.CallbackQuery):
    order_id = callback.data.replace("checkym_", "", 1)
    order = await orders.get_pending(order_id)
    if not order:
        if await orders.is_confirmed(order_id):
            await callback.answer("\u2705 \u0423\u0436\u0435 \u043f\u043e\u0434\u0442\u0432\u0435\u0440\u0436\u0434\u0435\u043d!", show_alert=True)
        else:
            await callback.answer("\u274c \u041d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d", show_alert=True)
        return
    if not rate_limiter.check(callback.from_user.id):
        await callback.answer("\u23f3 \u041f\u043e\u0434\u043e\u0436\u0434\u0438\u0442\u0435...", show_alert=True)
        return
    await callback.answer("\U0001f50d \u041f\u0440\u043e\u0432\u0435\u0440\u044f\u0435\u043c...")
    checking_msg = await callback.message.edit_text(
        "\U0001f504 <b>\u041f\u0440\u043e\u0432\u0435\u0440\u043a\u0430 \u043f\u043b\u0430\u0442\u0435\u0436\u0430...</b>\n\u23f3 \u041f\u043e\u0434\u043e\u0436\u0434\u0438\u0442\u0435 15-25 \u0441\u0435\u043a\u0443\u043d\u0434..."
    )
    payment_found = False
    for attempt in range(Config.MAX_PAYMENT_CHECK_ATTEMPTS):
        payment_found = await YooMoneyService.check_payment(order_id, order["amount"], order.get("created_at", time.time()))
        if payment_found:
            break
        await asyncio.sleep(Config.PAYMENT_CHECK_INTERVAL)
    if payment_found:
        success = await process_successful_payment(order_id, "\u0410\u0432\u0442\u043e\u043f\u0440\u043e\u0432\u0435\u0440\u043a\u0430")
        if success:
            await checking_msg.edit_text("\u2705 <b>\u041f\u043b\u0430\u0442\u0435\u0436 \u043d\u0430\u0439\u0434\u0435\u043d!</b>\n\U0001f4e8 \u041f\u0440\u043e\u0432\u0435\u0440\u044c\u0442\u0435 \u0441\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u0435 \u2b06\ufe0f", reply_markup=support_keyboard())
        else:
            await checking_msg.edit_text("\u2705 \u0423\u0436\u0435 \u043e\u0431\u0440\u0430\u0431\u043e\u0442\u0430\u043d", reply_markup=support_keyboard())
    else:
        product = order['product']
        payment_url = create_payment_link(order["amount"], order_id, "{} ({})".format(product['name'], product['duration']))
        await checking_msg.edit_text(
            "\u23f3 <b>\u041f\u043b\u0430\u0442\u0435\u0436 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d</b>\n\n\U0001f4b0 {amount} \u20bd\n\U0001f194 <code>{oid}</code>\n\n\u23f0 \u041f\u043e\u043f\u0440\u043e\u0431\u0443\u0439\u0442\u0435 \u0447\u0435\u0440\u0435\u0437 1-2 \u043c\u0438\u043d".format(amount=order['amount'], oid=order_id),
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
        await callback.answer("\u23f3 \u041f\u043e\u0434\u043e\u0436\u0434\u0438\u0442\u0435...", show_alert=True)
        return
    order_id = generate_order_id()
    await orders.add_pending(order_id, {
        "user_id": callback.from_user.id, "user_name": callback.from_user.full_name,
        "product": product, "amount": product['price_stars'], "currency": "\u2b50",
        "payment_method": "Telegram Stars", "status": "pending", "created_at": time.time()
    })
    await bot.send_invoice(
        chat_id=callback.from_user.id,
        title="PMT \u2014 {}".format(product['name']),
        description="\u041f\u043e\u0434\u043f\u0438\u0441\u043a\u0430 \u043d\u0430 {} \u0434\u043b\u044f {}".format(product['duration'], product['platform']),
        payload="stars_{}".format(order_id),
        provider_token="", currency="XTR",
        prices=[LabeledPrice(label="XTR", amount=product['price_stars'])],
        start_parameter="pmt_payment"
    )
    await send_admin_notification(callback.from_user, product, "\u2b50 Stars", "{} \u2b50".format(product['price_stars']), order_id)
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
        await callback.answer("\u274c \u041d\u0435\u0434\u043e\u0441\u0442\u0443\u043f\u043d\u043e", show_alert=True)
        return
    parts = callback.data.split("_")
    if len(parts) < 4:
        return
    product = find_product(parts[2], parts[3])
    if not product:
        return
    if not rate_limiter.check(callback.from_user.id):
        await callback.answer("\u23f3 \u041f\u043e\u0434\u043e\u0436\u0434\u0438\u0442\u0435...", show_alert=True)
        return
    order_id = generate_order_id()
    amount_usdt = product["price_crypto_usdt"]
    invoice_data = await CryptoBotService.create_invoice(amount_usdt, order_id, "PMT {} ({})".format(product['name'], product['duration']))
    if not invoice_data:
        await callback.answer("\u274c \u041e\u0448\u0438\u0431\u043a\u0430 \u0438\u043d\u0432\u043e\u0439\u0441\u0430", show_alert=True)
        return
    await orders.add_pending(order_id, {
        "user_id": callback.from_user.id, "user_name": callback.from_user.full_name,
        "product": product, "amount": amount_usdt, "currency": "USDT",
        "payment_method": "CryptoBot", "status": "pending",
        "invoice_id": invoice_data["invoice_id"], "created_at": time.time()
    })
    text = (
        "\u20bf <b>\u041a\u0440\u0438\u043f\u0442\u043e\u043e\u043f\u043b\u0430\u0442\u0430</b>\n\n"
        "{emoji} {name}\n\u23f1\ufe0f {dur}\n\U0001f4b0 <b>{amount} USDT</b>\n\U0001f194 <code>{oid}</code>\n\n"
        "1\ufe0f\u20e3 \u041d\u0430\u0436\u043c\u0438\u0442\u0435 \u00ab\u041e\u043f\u043b\u0430\u0442\u0438\u0442\u044c\u00bb\n"
        "2\ufe0f\u20e3 \u0412\u044b\u0431\u0435\u0440\u0438\u0442\u0435 \u0432\u0430\u043b\u044e\u0442\u0443\n3\ufe0f\u20e3 \u041d\u0430\u0436\u043c\u0438\u0442\u0435 \u00ab\u041f\u0440\u043e\u0432\u0435\u0440\u0438\u0442\u044c\u00bb"
    ).format(emoji=product['emoji'], name=product['name'], dur=product['duration'], amount=amount_usdt, oid=order_id)
    await callback.message.edit_text(text, reply_markup=crypto_payment_keyboard(invoice_data["pay_url"], order_id))
    await send_admin_notification(callback.from_user, product, "\u20bf CryptoBot", "{} USDT".format(amount_usdt), order_id)
    await callback.answer()


@dp.callback_query(F.data.startswith("checkcr_"))
async def check_crypto_callback(callback: types.CallbackQuery):
    order_id = callback.data.replace("checkcr_", "", 1)
    order = await orders.get_pending(order_id)
    if not order:
        if await orders.is_confirmed(order_id):
            await callback.answer("\u2705 \u0423\u0436\u0435 \u043e\u043f\u043b\u0430\u0447\u0435\u043d\u043e!", show_alert=True)
        else:
            await callback.answer("\u274c \u041d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d", show_alert=True)
        return
    if not rate_limiter.check(callback.from_user.id):
        await callback.answer("\u23f3", show_alert=True)
        return
    await callback.answer("\U0001f50d \u041f\u0440\u043e\u0432\u0435\u0440\u044f\u0435\u043c...")
    invoice_id = order.get("invoice_id")
    if not invoice_id:
        return
    if await CryptoBotService.check_invoice(invoice_id):
        success = await process_successful_payment(order_id, "CryptoBot")
        if success:
            await callback.message.edit_text("\u2705 <b>\u041a\u0440\u0438\u043f\u0442\u043e\u043f\u043b\u0430\u0442\u0435\u0436 \u043f\u043e\u0434\u0442\u0432\u0435\u0440\u0436\u0434\u0435\u043d!</b>\n\U0001f4e8 \u041a\u043b\u044e\u0447 \u043e\u0442\u043f\u0440\u0430\u0432\u043b\u0435\u043d \u2b06\ufe0f", reply_markup=support_keyboard())
    else:
        await callback.answer("\u23f3 \u041d\u0435 \u043f\u043e\u0434\u0442\u0432\u0435\u0440\u0436\u0434\u0435\u043d. \u041f\u043e\u043f\u0440\u043e\u0431\u0443\u0439\u0442\u0435 \u0447\u0435\u0440\u0435\u0437 \u043c\u0438\u043d\u0443\u0442\u0443.", show_alert=True)


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
        await callback.answer("\u23f3", show_alert=True)
        return
    cfg = {
        "gold": {"name": "GOLD", "icon": "\U0001f4b0", "price_key": "price_gold", "emoji": "\U0001fa99"},
        "nft": {"name": "NFT", "icon": "\U0001f3a8", "price_key": "price_nft", "emoji": "\U0001f5bc\ufe0f"}
    }[method]
    price = product[cfg["price_key"]]
    chat_message = (
        "\u041f\u0440\u0438\u0432\u0435\u0442! \u0425\u043e\u0447\u0443 \u043a\u0443\u043f\u0438\u0442\u044c \u0447\u0438\u0442 PMT \u043d\u0430 Standoff 2. "
        "\u041f\u043e\u0434\u043f\u0438\u0441\u043a\u0430 \u043d\u0430 {period} ({platform}). "
        "\u0413\u043e\u0442\u043e\u0432 \u043a\u0443\u043f\u0438\u0442\u044c \u0437\u0430 {price} {method}"
    ).format(period=product['period_text'], platform=product['platform'], price=price, method=cfg['name'])
    support_url = "https://t.me/{}?text={}".format(Config.SUPPORT_CHAT_USERNAME, quote(chat_message, safe=''))
    order_id = generate_order_id()
    await orders.add_pending(order_id, {
        "user_id": callback.from_user.id, "user_name": callback.from_user.full_name,
        "product": product, "amount": price, "currency": cfg["name"],
        "payment_method": cfg["name"], "status": "pending", "created_at": time.time()
    })
    text = (
        "{icon} <b>\u041e\u043f\u043b\u0430\u0442\u0430 {mname}</b>\n\n"
        "{emoji} {pname}\n\u23f1\ufe0f {dur}\n\U0001f4b0 <b>{price} {mname}</b>\n\n"
        "\U0001f4dd <b>\u0421\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u0435:</b>\n<code>{msg}</code>\n\n"
        "1\ufe0f\u20e3 \u041d\u0430\u043f\u0438\u0448\u0438\u0442\u0435 \u0432 \u043f\u043e\u0434\u0434\u0435\u0440\u0436\u043a\u0443\n"
        "2\ufe0f\u20e3 \u041e\u0436\u0438\u0434\u0430\u0439\u0442\u0435 \u043e\u0431\u0440\u0430\u0431\u043e\u0442\u043a\u0438"
    ).format(icon=cfg['icon'], mname=cfg['name'], emoji=product['emoji'], pname=product['name'],
             dur=product['duration'], price=price, msg=chat_message)
    await callback.message.edit_text(text, reply_markup=manual_payment_keyboard(support_url, "{}_sent".format(method)))
    await send_admin_notification(callback.from_user, product, "{} {}".format(cfg['icon'], cfg['name']), "{} {}".format(price, cfg['emoji']), order_id)
    await callback.answer()


@dp.callback_query(F.data.in_({"gold_sent", "nft_sent"}))
async def manual_payment_sent(callback: types.CallbackQuery):
    mname = "GOLD" if callback.data == "gold_sent" else "NFT"
    icon = "\U0001f4b0" if callback.data == "gold_sent" else "\U0001f3a8"
    text = (
        "\u2705 <b>\u041e\u0442\u043b\u0438\u0447\u043d\u043e!</b>\n\n"
        "{icon} \u0412\u0430\u0448 {mname} \u0437\u0430\u043a\u0430\u0437 \u043f\u0440\u0438\u043d\u044f\u0442\n"
        "\u23f1\ufe0f \u041e\u0431\u0440\u0430\u0431\u043e\u0442\u043a\u0430: \u0434\u043e 30 \u043c\u0438\u043d\n\n"
        "\U0001f4ac \u041f\u043e\u0434\u0434\u0435\u0440\u0436\u043a\u0430: @{support}"
    ).format(icon=icon, mname=mname, support=Config.SUPPORT_CHAT_USERNAME)
    await callback.message.edit_text(text, reply_markup=support_keyboard())
    await callback.answer()


# ========== АДМИН ==========
@dp.callback_query(F.data.startswith("admin_confirm_"))
async def admin_confirm(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("\u274c", show_alert=True)
        return
    order_id = callback.data.replace("admin_confirm_", "", 1)
    success = await process_successful_payment(order_id, "\U0001f468\u200d\U0001f4bc \u0410\u0434\u043c\u0438\u043d")
    if success:
        await callback.message.edit_text(
            "\u2705 <b>\u041f\u043e\u0434\u0442\u0432\u0435\u0440\u0436\u0434\u0435\u043d</b>\n\U0001f194 {}\n\U0001f468\u200d\U0001f4bc {}".format(order_id, callback.from_user.full_name))
        await callback.answer("\u2705")
    else:
        await callback.answer("\u274c \u041d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d", show_alert=True)


@dp.callback_query(F.data.startswith("admin_reject_"))
async def admin_reject(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("\u274c", show_alert=True)
        return
    order_id = callback.data.replace("admin_reject_", "", 1)
    order = await orders.remove_pending(order_id)
    if order:
        await callback.message.edit_text("\u274c <b>\u041e\u0442\u043a\u043b\u043e\u043d\u0435\u043d</b>\n\U0001f194 {}".format(order_id))
        try:
            await bot.send_message(order['user_id'],
                "\u274c <b>\u0417\u0430\u043a\u0430\u0437 \u043e\u0442\u043a\u043b\u043e\u043d\u0435\u043d</b>\n\U0001f4ac @{}".format(Config.SUPPORT_CHAT_USERNAME))
        except Exception:
            pass
    await callback.answer("\u274c")


@dp.message(Command("orders"))
async def cmd_orders(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    stats = await orders.get_stats()
    text = "\U0001f4ca <b>\u0421\u0422\u0410\u0422\u0418\u0421\u0422\u0418\u041a\u0410</b>\n\n\u23f3 \u041e\u0436\u0438\u0434\u0430\u044e\u0442: {}\n".format(stats['pending'])
    for oid, order in await orders.get_recent_pending(5):
        t = datetime.fromtimestamp(order['created_at']).strftime('%H:%M')
        text += "\u2022 {} | {} | {}\n".format(t, order['user_name'], order['product']['name'])
    text += "\n\u2705 \u041f\u043e\u0434\u0442\u0432\u0435\u0440\u0436\u0434\u0435\u043d\u043e: {}\n".format(stats['confirmed'])
    balance = await YooMoneyService.get_balance()
    if balance is not None:
        text += "\U0001f4b0 \u0411\u0430\u043b\u0430\u043d\u0441: {} \u20bd".format(balance)
    await message.answer(text)


@dp.message(Command("help"))
async def cmd_help(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    await message.answer("/orders \u2014 \u0421\u0442\u0430\u0442\u0438\u0441\u0442\u0438\u043a\u0430\n/help \u2014 \u0421\u043f\u0440\u0430\u0432\u043a\u0430")


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
    platform = data.get("platform", "apk")
    titles = {"apk": "\U0001f4f1 <b>PMT Android</b>", "ios": "\U0001f34e <b>PMT iOS</b>"}
    text = "{}\n\n\U0001f4b0 <b>\u0412\u044b\u0431\u0435\u0440\u0438\u0442\u0435 \u0442\u0430\u0440\u0438\u0444:</b>".format(titles.get(platform, "PMT"))
    await callback.message.edit_text(text, reply_markup=subscription_keyboard(platform))
    await state.set_state(OrderState.choosing_subscription)
    await callback.answer()


# ========== ЗАПУСК ==========
async def main():
    logger.info("=" * 50)
    logger.info("PMT PREMIUM CHEAT SHOP BOT")
    logger.info("=" * 50)
    logger.info("ADMIN_IDS: %s", Config.ADMIN_IDS)
    logger.info("SUPPORT: @%s", Config.SUPPORT_CHAT_USERNAME)
    logger.info("DOWNLOAD: %s", Config.DOWNLOAD_URL)

    try:
        me = await bot.get_me()
        logger.info("Bot: @%s", me.username)
        for key, product in PRODUCTS.items():
            logger.info("%s %s (%s) - %s RUB / %s Stars / %s Gold",
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
