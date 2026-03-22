# bot_zakat.py — ZAKAT Premium Cheat Shop
import logging
import asyncio
import aiohttp
import hashlib
import time
import random
import os
from datetime import datetime
from urllib.parse import quote
from collections import OrderedDict

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

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class Config:
    BOT_TOKEN = os.environ.get("BOT_TOKEN", "8364248036:AAEhPvRkINz08OIRhzfC37BaX8ti8bUdipc")
    CRYPTOBOT_TOKEN = os.environ.get("CRYPTOBOT_TOKEN", "493276:AAtS7R1zYy0gaPw8eax1EgiWo0tdnd6dQ9c")
    YOOMONEY_ACCESS_TOKEN = os.environ.get("YOOMONEY_ACCESS_TOKEN", "4100118889570559.3288B2E716CEEB922A26BD6BEAC58648FBFB680CCF64E4E1447D714D6FB5EA5F01F1478FAC686BEF394C8A186C98982DE563C1ABCDF9F2F61D971B61DA3C7E486CA818F98B9E0069F1C0891E090DD56A11319D626A40F0AE8302A8339DED9EB7969617F191D93275F64C4127A3ECB7AED33FCDE91CA68690EB7534C67E6C219E")
    YOOMONEY_WALLET = os.environ.get("YOOMONEY_WALLET", "4100118889570559")
    SUPPORT_CHAT_USERNAME = os.environ.get("SUPPORT_CHAT_USERNAME", "ZakatManager")
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
            raise ValueError("BOT_TOKEN is required!")
        admin_ids_str = os.environ.get("ADMIN_ID", "")
        admin_ids_list = [int(x.strip()) for x in admin_ids_str.split(",") if x.strip().isdigit()]
        if not admin_ids_list:
            raise ValueError("ADMIN_ID is required!")
        cls.ADMIN_ID = admin_ids_list[0]
        cls.SUPPORT_CHAT_ID = admin_ids_list[1] if len(admin_ids_list) >= 2 else int(os.environ.get("SUPPORT_CHAT_ID", str(cls.ADMIN_ID)))
        cls.ADMIN_IDS = set(admin_ids_list)


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

    async def remove_pending(self, order_id):
        async with self._lock:
            return self._pending.pop(order_id, None)

    async def get_stats(self):
        async with self._lock:
            return {"pending": len(self._pending), "confirmed": len(self._confirmed)}

    async def get_recent_pending(self, limit=5):
        async with self._lock:
            return list(self._pending.items())[-limit:]

    async def _cleanup_expired(self):
        now = time.time()
        expired = [oid for oid, data in self._pending.items() if now - data.get("created_at", 0) > self._expiry_seconds]
        for oid in expired:
            del self._pending[oid]


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
        return True


# ==================== ПРОДУКТЫ ZAKAT ====================
PRODUCTS = {
    "apk_week": {
        "name": "📱 ZAKAT Android",
        "period_text": "НЕДЕЛЮ",
        "price": 225,
        "price_stars": 300,
        "price_gold": 600,
        "price_crypto_rub": 225,
        "platform": "Android",
        "period": "НЕДЕЛЮ",
        "platform_code": "apk",
        "emoji": "📱",
        "duration": "7 дней"
    },
    "apk_month": {
        "name": "📱 ZAKAT Android",
        "period_text": "МЕСЯЦ",
        "price": 450,
        "price_stars": 500,
        "price_gold": 1000,
        "price_crypto_rub": 450,
        "platform": "Android",
        "period": "МЕСЯЦ",
        "platform_code": "apk",
        "emoji": "📱",
        "duration": "30 дней"
    },
    "apk_forever": {
        "name": "📱 ZAKAT Android",
        "period_text": "НАВСЕГДА",
        "price": 850,
        "price_stars": 900,
        "price_gold": 1900,
        "price_crypto_rub": 850,
        "platform": "Android",
        "period": "НАВСЕГДА",
        "platform_code": "apk",
        "emoji": "📱",
        "duration": "Навсегда"
    },
    "ios_week": {
        "name": "🍎 ZAKAT iOS",
        "period_text": "НЕДЕЛЮ",
        "price": 350,
        "price_stars": 350,
        "price_gold": 700,
        "price_crypto_rub": 350,
        "platform": "iOS",
        "period": "НЕДЕЛЮ",
        "platform_code": "ios",
        "emoji": "🍎",
        "duration": "7 дней"
    },
    "ios_month": {
        "name": "🍎 ZAKAT iOS",
        "period_text": "МЕСЯЦ",
        "price": 700,
        "price_stars": 700,
        "price_gold": 1400,
        "price_crypto_rub": 700,
        "platform": "iOS",
        "period": "МЕСЯЦ",
        "platform_code": "ios",
        "emoji": "🍎",
        "duration": "30 дней"
    },
    "ios_forever": {
        "name": "🍎 ZAKAT iOS",
        "period_text": "НАВСЕГДА",
        "price": 1600,
        "price_stars": 1600,
        "price_gold": 3000,
        "price_crypto_rub": 1600,
        "platform": "iOS",
        "period": "НАВСЕГДА",
        "platform_code": "ios",
        "emoji": "🍎",
        "duration": "Навсегда"
    }
}


def generate_order_id():
    raw = "{}_{}_{}" .format(time.time(), random.randint(100000, 999999), os.urandom(4).hex())
    return hashlib.sha256(raw.encode()).hexdigest()[:12]


def generate_license_key(order_id, user_id):
    raw = "{}_{}_{}" .format(order_id, user_id, os.urandom(8).hex())
    h = hashlib.sha256(raw.encode()).hexdigest()[:16].upper()
    return "ZAKAT-{}-{}-{}-{}".format(h[:4], h[4:8], h[8:12], h[12:16])


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
    success_url = quote('https://t.me/zakat_bot?start=success', safe='')
    return (
        "https://yoomoney.ru/quickpay/confirm.xml"
        "?receiver={}&quickpay-form=shop&targets={}&sum={}&label={}&successURL={}&paymentType=AC"
    ).format(Config.YOOMONEY_WALLET, safe_targets, amount, order_id, success_url)


class YooMoneyService:
    @staticmethod
    async def get_balance():
        if not Config.YOOMONEY_ACCESS_TOKEN:
            return None
        headers = {"Authorization": "Bearer {}".format(Config.YOOMONEY_ACCESS_TOKEN)}
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
                async with session.get("https://yoomoney.ru/api/account-info", headers=headers) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return float(data.get('balance', 0))
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
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as session:
                async with session.post("https://yoomoney.ru/api/operation-history", headers=headers, data=data) as resp:
                    if resp.status != 200:
                        return False
                    result = await resp.json()
                    for op in result.get("operations", []):
                        if op.get("label") == order_id and op.get("status") == "success" and abs(float(op.get("amount", 0)) - expected_amount) <= 5:
                            return True
                    for op in result.get("operations", []):
                        if op.get("status") != "success":
                            continue
                        if abs(float(op.get("amount", 0)) - expected_amount) > 2:
                            continue
                        try:
                            op_time = datetime.fromisoformat(op.get("datetime", "").replace("Z", "+00:00")).timestamp()
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
    async def create_invoice(amount_rub, order_id, description):
        if not Config.CRYPTOBOT_TOKEN:
            return None
        headers = {"Crypto-Pay-API-Token": Config.CRYPTOBOT_TOKEN, "Content-Type": "application/json"}
        data = {
            "currency_type": "fiat",
            "fiat": "RUB",
            "amount": str(amount_rub),
            "description": description[:256],
            "payload": order_id,
            "paid_btn_name": "callback",
            "paid_btn_url": "https://t.me/zakat_bot?start=paid_{}".format(order_id)
        }
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
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
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
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


Config.init()
bot = Bot(token=Config.BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
orders = OrderStorage(max_pending=Config.MAX_PENDING_ORDERS, expiry_seconds=Config.ORDER_EXPIRY_SECONDS)
rate_limiter = RateLimiter(interval=Config.RATE_LIMIT_SECONDS)


class OrderState(StatesGroup):
    main_menu = State()
    choosing_platform = State()
    choosing_subscription = State()
    choosing_payment = State()


# ==================== КЛАВИАТУРЫ ====================
def start_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎮 Купить чит Standoff 2", callback_data="buy_cheat")],
        [InlineKeyboardButton(text="ℹ️ О программе", callback_data="about")],
        [InlineKeyboardButton(text="💬 Поддержка", url="https://t.me/{}".format(Config.SUPPORT_CHAT_USERNAME))]
    ])


def platform_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📱 Android", callback_data="platform_apk")],
        [InlineKeyboardButton(text="🍎 iOS", callback_data="platform_ios")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="back_to_start")]
    ])


def subscription_keyboard(platform):
    prices = {
        "apk": [
            ("⚡ НЕДЕЛЯ — 225₽", "sub_apk_week"),
            ("🔥 МЕСЯЦ — 450₽", "sub_apk_month"),
            ("💎 НАВСЕГДА — 850₽", "sub_apk_forever"),
        ],
        "ios": [
            ("⚡ НЕДЕЛЯ — 350₽", "sub_ios_week"),
            ("🔥 МЕСЯЦ — 700₽", "sub_ios_month"),
            ("💎 НАВСЕГДА — 1600₽", "sub_ios_forever"),
        ]
    }
    buttons = [[InlineKeyboardButton(text=text, callback_data=cb)] for text, cb in prices.get(platform, [])]
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="buy_cheat")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def payment_methods_keyboard(product):
    pc = product['platform_code']
    p = product['period']
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 Картой — {} ₽".format(product['price']), callback_data="pay_yoomoney_{}_{}".format(pc, p))],
        [InlineKeyboardButton(text="⭐ Stars — {} ⭐".format(product['price_stars']), callback_data="pay_stars_{}_{}".format(pc, p))],
        [InlineKeyboardButton(text="₿ Криптобот — {} ₽".format(product['price_crypto_rub']), callback_data="pay_crypto_{}_{}".format(pc, p))],
        [InlineKeyboardButton(text="💰 GOLD — {} 🪙".format(product['price_gold']), callback_data="pay_gold_{}_{}".format(pc, p))],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="back_to_subscription")]
    ])


def payment_keyboard(payment_url, order_id):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 Оплатить картой", url=payment_url)],
        [InlineKeyboardButton(text="✅ Проверить оплату", callback_data="checkym_{}".format(order_id))],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="restart")]
    ])


def crypto_payment_keyboard(invoice_url, order_id):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="₿ Оплатить криптоботом", url=invoice_url)],
        [InlineKeyboardButton(text="✅ Проверить платеж", callback_data="checkcr_{}".format(order_id))],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="restart")]
    ])


def support_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💬 Поддержка", url="https://t.me/{}".format(Config.SUPPORT_CHAT_USERNAME))],
        [InlineKeyboardButton(text="🔄 Новая покупка", callback_data="restart")]
    ])


def download_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📥 Скачать ZAKAT", url=Config.DOWNLOAD_URL)],
        [InlineKeyboardButton(text="💬 Поддержка", url="https://t.me/{}".format(Config.SUPPORT_CHAT_USERNAME))],
        [InlineKeyboardButton(text="🔄 Новая покупка", callback_data="restart")]
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


# ==================== БИЗНЕС-ЛОГИКА ====================
async def process_successful_payment(order_id, source="API"):
    order = await orders.get_pending(order_id)
    if not order:
        if await orders.is_confirmed(order_id):
            logger.info("Order %s already confirmed", order_id)
        return False
    product = order["product"]
    user_id = order["user_id"]
    license_key = generate_license_key(order_id, user_id)
    confirmed = await orders.confirm(order_id, {'confirmed_at': time.time(), 'confirmed_by': source, 'license_key': license_key})
    if not confirmed:
        return False

    success_text = (
        "🎉 <b>Оплата подтверждена!</b>\n\n"
        "✨ Добро пожаловать в ZAKAT PREMIUM!\n\n"
        "📦 <b>Ваша покупка:</b>\n"
        "{emoji} {name}\n"
        "⏱️ Срок: {duration}\n"
        "🔍 Метод: {source}\n\n"
        "🔑 <b>Ваш лицензионный ключ:</b>\n"
        "<code>{key}</code>\n\n"
        "📥 <b>Скачивание:</b>\n"
        "👇 Нажмите кнопку ниже\n\n"
        "💫 <b>Активация:</b>\n"
        "1️⃣ Скачайте файл\n"
        "2️⃣ Установите приложение\n"
        "3️⃣ Введите ключ\n"
        "4️⃣ Наслаждайтесь игрой! 🎮\n\n"
        "💬 Поддержка: @{support}"
    ).format(emoji=product['emoji'], name=product['name'], duration=product['duration'],
             source=source, key=license_key, support=Config.SUPPORT_CHAT_USERNAME)
    try:
        await bot.send_message(user_id, success_text, reply_markup=download_keyboard())
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
        "🔔 <b>НОВЫЙ ЗАКАЗ</b>\n\n"
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
        "🎯 <b>ZAKAT — Премиум чит для Standoff 2</b>\n\n"
        "✨ <b>Возможности:</b>\n"
        "🛡️ Продвинутая защита от банов\n"
        "🎯 Умный AimBot с настройками\n"
        "👁️ WallHack и ESP\n"
        "📊 Полная информация о противниках\n"
        "⚡ Быстрые обновления\n\n"
        "🚀 <b>Нажмите кнопку ниже для покупки:</b>"
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
        "🎮 <b>Выберите платформу:</b>\n\n"
        "📱 <b>Android</b> — APK файл\n"
        "🍎 <b>iOS</b> — IPA файл"
    )
    if isinstance(target, types.CallbackQuery):
        try:
            await target.message.edit_text(text, reply_markup=platform_keyboard())
        except Exception:
            await target.message.answer(text, reply_markup=platform_keyboard())
    await state.set_state(OrderState.choosing_platform)


# ==================== ОБРАБОТЧИКИ ====================
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
                    description="Подписка на {} для {}".format(product['duration'], product['platform']),
                    payload="stars_{}".format(order_id),
                    provider_token="", currency="XTR",
                    prices=[LabeledPrice(label="XTR", amount=product['price_stars'])],
                    start_parameter="zakat_payment"
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
        "📋 <b>Подробная информация</b>\n\n"
        "🎮 <b>Название:</b> ZAKAT PREMIUM\n"
        "🔥 <b>Статус:</b> Активно обновляется\n\n"
        "🛠️ <b>Функционал:</b>\n"
        "• 🎯 Умный AimBot\n• 👁️ WallHack\n"
        "• 📍 ESP\n• 🗺️ Мини-радар\n"
        "• ⚙️ Гибкие настройки\n\n"
        "🛡️ <b>Безопасность:</b>\n"
        "• Обход античитов\n• Регулярные обновления\n\n"
        "💬 Поддержка: @{support}"
    ).format(support=Config.SUPPORT_CHAT_USERNAME)
    await callback.message.edit_text(text, reply_markup=about_keyboard())
    await callback.answer()


@dp.callback_query(F.data.startswith("platform_"))
async def process_platform(callback: types.CallbackQuery, state: FSMContext):
    platform = callback.data.split("_")[1]
    if platform not in ("apk", "ios"):
        await callback.answer("❌ Ошибка", show_alert=True)
        return
    await state.update_data(platform=platform)
    info = {
        "apk": ("📱 <b>ZAKAT Android</b>",
                "• Android 10.0+\n• 2 ГБ памяти\n• Root не нужен",
                "• APK файл\n• Инструкция\n• Поддержка"),
        "ios": ("🍎 <b>ZAKAT iOS</b>",
                "• iOS 14.0 - 18.0\n• AltStore\n• Jailbreak не нужен",
                "• IPA файл\n• Инструкция\n• Помощь")
    }
    title, reqs, incl = info[platform]
    text = "{title}\n\n🔧 <b>Требования:</b>\n{reqs}\n\n📦 <b>Что входит:</b>\n{incl}\n\n💰 <b>Выберите тариф:</b>".format(title=title, reqs=reqs, incl=incl)
    await callback.message.edit_text(text, reply_markup=subscription_keyboard(platform))
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
        "🛒 <b>Оформление</b>\n\n"
        "{emoji} <b>{name}</b>\n⏱️ {duration}\n\n"
        "💎 <b>Стоимость:</b>\n"
        "💳 Картой: {price} ₽\n"
        "⭐ Stars: {stars} ⭐\n"
        "₿ Криптобот: {crypto} ₽\n"
        "💰 GOLD: {gold} 🪙\n\n"
        "🎯 <b>Способ оплаты:</b>"
    ).format(emoji=product['emoji'], name=product['name'], duration=product['duration'],
             price=product['price'], stars=product['price_stars'], crypto=product['price_crypto_rub'],
             gold=product['price_gold'])
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
        "💳 <b>Оплата картой</b>\n\n"
        "{emoji} {name}\n⏱️ {dur}\n"
        "💰 <b>{amount} ₽</b>\n🆔 <code>{oid}</code>\n\n"
        "1️⃣ Нажмите «Оплатить»\n"
        "2️⃣ Оплатите\n3️⃣ Нажмите «Проверить»"
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
    checking_msg = await callback.message.edit_text("🔄 <b>Проверка платежа...</b>\n⏳ Подождите 15-25 секунд...")
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
        description="Подписка на {} для {}".format(product['duration'], product['platform']),
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
    amount_rub = product["price_crypto_rub"]
    invoice_data = await CryptoBotService.create_invoice(amount_rub, order_id, "ZAKAT {} ({})".format(product['name'], product['duration']))
    if not invoice_data:
        await callback.answer("❌ Ошибка инвойса", show_alert=True)
        return
    await orders.add_pending(order_id, {
        "user_id": callback.from_user.id, "user_name": callback.from_user.full_name,
        "product": product, "amount": amount_rub, "currency": "₽ (крипто)",
        "payment_method": "CryptoBot", "status": "pending",
        "invoice_id": invoice_data["invoice_id"], "created_at": time.time()
    })
    text = (
        "₿ <b>Криптооплата</b>\n\n"
        "{emoji} {name}\n⏱️ {dur}\n💰 <b>{amount} ₽</b>\n🆔 <code>{oid}</code>\n\n"
        "1️⃣ Нажмите «Оплатить»\n"
        "2️⃣ Выберите валюту\n3️⃣ Нажмите «Проверить»"
    ).format(emoji=product['emoji'], name=product['name'], dur=product['duration'], amount=amount_rub, oid=order_id)
    await callback.message.edit_text(text, reply_markup=crypto_payment_keyboard(invoice_data["pay_url"], order_id))
    await send_admin_notification(callback.from_user, product, "₿ CryptoBot", "{} ₽".format(amount_rub), order_id)
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
            await callback.message.edit_text("✅ <b>Криптоплатеж подтвержден!</b>\n📨 Ключ отправлен ⬆️", reply_markup=support_keyboard())
    else:
        await callback.answer("⏳ Не подтвержден. Попробуйте через минуту.", show_alert=True)


# ========== ОПЛАТА GOLD ==========
@dp.callback_query(F.data.startswith("pay_gold_"))
async def process_gold_payment(callback: types.CallbackQuery):
    parts = callback.data.split("_")
    if len(parts) < 4:
        return
    product = find_product(parts[2], parts[3])
    if not product:
        return
    if not rate_limiter.check(callback.from_user.id):
        await callback.answer("⏳", show_alert=True)
        return
    price = product["price_gold"]
    chat_message = (
        "Привет! Хочу купить чит ZAKAT на Standoff 2. "
        "Подписка на {period} ({platform}). "
        "Готов купить за {price} GOLD"
    ).format(period=product['period_text'], platform=product['platform'], price=price)
    support_url = "https://t.me/{}?text={}".format(Config.SUPPORT_CHAT_USERNAME, quote(chat_message, safe=''))
    order_id = generate_order_id()
    await orders.add_pending(order_id, {
        "user_id": callback.from_user.id, "user_name": callback.from_user.full_name,
        "product": product, "amount": price, "currency": "GOLD",
        "payment_method": "GOLD", "status": "pending", "created_at": time.time()
    })
    text = (
        "💰 <b>Оплата GOLD</b>\n\n"
        "{emoji} {pname}\n⏱️ {dur}\n💰 <b>{price} GOLD</b>\n\n"
        "📝 <b>Сообщение:</b>\n<code>{msg}</code>\n\n"
        "1️⃣ Напишите в поддержку\n"
        "2️⃣ Ожидайте обработки"
    ).format(emoji=product['emoji'], pname=product['name'], dur=product['duration'], price=price, msg=chat_message)
    await callback.message.edit_text(text, reply_markup=manual_payment_keyboard(support_url, "gold_sent"))
    await send_admin_notification(callback.from_user, product, "💰 GOLD", "{} 🪙".format(price), order_id)
    await callback.answer()


@dp.callback_query(F.data == "gold_sent")
async def gold_payment_sent(callback: types.CallbackQuery):
    text = (
        "✅ <b>Отлично!</b>\n\n"
        "💰 Ваш GOLD заказ принят\n"
        "⏱️ Обработка: до 30 мин\n\n"
        "💬 Поддержка: @{support}"
    ).format(support=Config.SUPPORT_CHAT_USERNAME)
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
        await callback.message.edit_text("✅ <b>Подтверждён</b>\n🆔 {}\n👨‍💼 {}".format(order_id, callback.from_user.full_name))
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
            await bot.send_message(order['user_id'], "❌ <b>Заказ отклонён</b>\n💬 @{}".format(Config.SUPPORT_CHAT_USERNAME))
        except Exception:
            pass
    await callback.answer("❌")


@dp.message(Command("orders"))
async def cmd_orders(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    stats = await orders.get_stats()
    text = "📊 <b>СТАТИСТИКА</b>\n\n⏳ Ожидают: {}\n".format(stats['pending'])
    for oid, order in await orders.get_recent_pending(5):
        t = datetime.fromtimestamp(order['created_at']).strftime('%H:%M')
        text += "• {} | {} | {}\n".format(t, order['user_name'], order['product']['name'])
    text += "\n✅ Подтверждено: {}\n".format(stats['confirmed'])
    balance = await YooMoneyService.get_balance()
    if balance is not None:
        text += "💰 Баланс: {} ₽".format(balance)
    await message.answer(text)


@dp.message(Command("help"))
async def cmd_help(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    await message.answer("/orders — Статистика\n/help — Справка")


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
    titles = {"apk": "📱 <b>ZAKAT Android</b>", "ios": "🍎 <b>ZAKAT iOS</b>"}
    text = "{}\n\n💰 <b>Выберите тариф:</b>".format(titles.get(platform, "ZAKAT"))
    await callback.message.edit_text(text, reply_markup=subscription_keyboard(platform))
    await state.set_state(OrderState.choosing_subscription)
    await callback.answer()


async def main():
    logger.info("=" * 50)
    logger.info("ZAKAT PREMIUM CHEAT SHOP BOT")
    logger.info("=" * 50)
    try:
        me = await bot.get_me()
        logger.info("Bot: @%s", me.username)
        logger.info("Bot starting polling...")
        await dp.start_polling(bot)
    except Exception as e:
        logger.error("Fatal error: %s", e)
    finally:
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
