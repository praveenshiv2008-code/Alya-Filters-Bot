# bot.py - Alya Filter Bot v2.0 (Full: Media+Buttons, Group Filters, All Commands)
import os
import sys
import json
import time
import asyncio
import logging
import zipfile
import io
import re
import random
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from typing import Optional, List, Dict, Any, Tuple
from difflib import SequenceMatcher

# ============================================================
# CRITICAL FIX: Create event loop BEFORE importing pyrogram
# ============================================================
try:
    loop = asyncio.get_event_loop()
except RuntimeError:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

from pyrogram import Client, filters, enums
from pyrogram.types import (
    Message, CallbackQuery, ChatJoinRequest,
    InlineKeyboardMarkup, InlineKeyboardButton
)
from pyrogram.errors import (
    FloodWait, UserIsBlocked, InputUserDeactivated,
    ChatAdminRequired, ChannelPrivate, PeerIdInvalid,
    UserNotParticipant, MessageDeleteForbidden,
    InviteHashExpired, BadRequest
)
from motor.motor_asyncio import AsyncIOMotorClient
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from aiohttp import web

# ============================================================
# CONFIGURATION
# ============================================================

API_ID = int(os.environ.get("API_ID", "0"))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")

MONGO_URI = os.environ.get("MONGO_URI", "")
DB_NAME = "telegram_search_bot"

OWNER_ID = int(os.environ.get("OWNER_ID", "7195555305"))
BACKUP_CHANNEL_ID = int(os.environ.get("BACKUP_CHANNEL_ID", "-1004413159220"))

LINK_EXPIRY_SECONDS = 1800
LINK_REUSE_WINDOW = 1500
BIO_UPDATE_INTERVAL = 14
SEARCH_CACHE_TTL = 300
SEARCH_RESULT_LIMIT = 10
RATE_LIMIT_SEARCHES = 5
RATE_LIMIT_WINDOW = 60
BROADCAST_SPEED = 25
TG_MAX_LENGTH = 4096
FUZZY_THRESHOLD = 0.75
LIST_PAGE_SIZE = 25
BACKUP_RETAIN_COUNT = 4
MAX_FILTERS_PER_USER = 50
MAX_REPLIES_PER_FILTER = 10

ALL_COMMANDS = [
    "start", "verify", "unverify", "channels", "list",
    "broadcast", "addad", "removead", "listads", "adstats",
    "ban", "unban", "userinfo", "addfj", "removefj", "listfj",
    "stats", "channelstats", "searchstats", "backup", "restore",
    "maintenance", "logs", "addmin", "radmin", "ladmin", "help",
    "search", "anime", "a", "s",
    "addfilter", "editfilter", "delfilter", "listfilters",
    "filterstats", "filterinfo", "addreply", "delreply",
    "filtergroup", "exportfilters", "importfilters",
    "groupfilters", "antilink", "addwhitelist", "delwhitelist",
    "whitelist", "linkstats", "welcome", "ping"
]

# ============================================================
# START IMAGES (4 total, random selection)
# ============================================================

START_IMAGES = [
    "https://files.catbox.moe/sla8rd.jpg",
    "https://files.catbox.moe/9tqcsy.jpg",
    "https://files.catbox.moe/xh6fdc.jpg",
    "https://files.catbox.moe/t3c8bc.jpg"
]

# ============================================================
# LOGGING
# ============================================================

if os.environ.get("RENDER"):
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler()]
    )
else:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('bot.log', encoding='utf-8')
        ]
    )

logger = logging.getLogger(__name__)

# ============================================================
# INITIALIZE BOT & DATABASE
# ============================================================

def validate_config():
    required = {
        "API_ID": API_ID,
        "API_HASH": API_HASH,
        "BOT_TOKEN": BOT_TOKEN,
        "MONGO_URI": MONGO_URI,
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        raise RuntimeError("Missing required: " + ", ".join(missing))
    if API_ID <= 0:
        raise RuntimeError("API_ID must be positive.")

validate_config()

app = Client("search_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
mongo_client = AsyncIOMotorClient(MONGO_URI)
db = mongo_client[DB_NAME]

# Collections
users_col = db["users"]
channels_col = db["channels"]
invite_links_col = db["invite_links"]
search_results_col = db["search_results"]
ads_col = db["advertisements"]
settings_col = db["settings"]
admins_col = db["admins"]
logs_col = db["logs"]
backup_records_col = db["backup_records"]
force_join_requests_col = db["force_join_requests"]
filters_col = db["filters"]
filter_logs_col = db["filter_logs"]
filter_groups_col = db["filter_groups"]
antilink_violations_col = db["antilink_violations"]
antilink_settings_col = db["antilink_settings"]
whitelist_col = db["whitelist"]

scheduler = AsyncIOScheduler()

# In-memory states
rate_limiter: Dict[int, List[float]] = defaultdict(list)
search_cache: Dict[str, dict] = {}
broadcast_state: Dict[str, Any] = {}
restore_state: Dict[int, dict] = {}
fj_pending: Dict[str, dict] = {}

# ============================================================
# DATABASE INDEXES
# ============================================================

async def setup_indexes():
    try:
        await users_col.create_index([("user_id", 1)], unique=True)
        await channels_col.create_index([("channel_id", 1)], unique=True)
        await invite_links_col.create_index([("channel_id", 1), ("status", 1), ("reuse_until", 1)])
        await invite_links_col.create_index([("expires_at", 1)])
        await search_results_col.create_index([("expires_at", 1)])
        await admins_col.create_index([("user_id", 1)], unique=True)
        await ads_col.create_index([("slot", 1)], unique=True)
        await backup_records_col.create_index([("type", 1), ("created_at", 1)])
        await force_join_requests_col.create_index([("user_id", 1), ("channel_id", 1)], unique=True)
        await filters_col.create_index([("keyword", 1)], unique=True)
        await filters_col.create_index([("group", 1)])
        await filters_col.create_index([("created_by", 1)])
        await filter_logs_col.create_index([("filter_id", 1)])
        await filter_logs_col.create_index([("used_at", 1)])
        await filter_groups_col.create_index([("name", 1)], unique=True)
        await antilink_violations_col.create_index([("user_id", 1)])
        await antilink_violations_col.create_index([("chat_id", 1)])
        await antilink_violations_col.create_index([("date", 1)])
        await whitelist_col.create_index([("domain", 1)], unique=True)
        await whitelist_col.create_index([("user_id", 1)])
        logger.info("Database indexes created")
    except Exception as e:
        logger.error(f"Index error: {e}")

# ============================================================
# SMALL CAPS CONVERTER
# ============================================================

SMALL_CAPS = {
    'a': 'ᴀ', 'b': 'ʙ', 'c': 'ᴄ', 'd': 'ᴅ', 'e': 'ᴇ', 'f': 'ꜰ',
    'g': 'ɢ', 'h': 'ʜ', 'i': 'ɪ', 'j': 'ᴊ', 'k': 'ᴋ', 'l': 'ʟ',
    'm': 'ᴍ', 'n': 'ɴ', 'o': 'ᴏ', 'p': 'ᴘ', 'q': 'ǫ', 'r': 'ʀ',
    's': 'ꜱ', 't': 'ᴛ', 'u': 'ᴜ', 'v': 'ᴠ', 'w': 'ᴡ', 'x': 'x',
    'y': 'ʏ', 'z': 'ᴢ'
}

def small_caps(text: str) -> str:
    """Convert text to small caps"""
    result = []
    for char in text:
        if char.lower() in SMALL_CAPS:
            if char.isupper():
                result.append(SMALL_CAPS[char.lower()].upper())
            else:
                result.append(SMALL_CAPS[char])
        else:
            result.append(char)
    return ''.join(result)

# ============================================================
# HELPER FUNCTIONS
# ============================================================

async def is_owner(user_id: int) -> bool:
    return user_id == OWNER_ID

async def is_admin(user_id: int) -> bool:
    if user_id == OWNER_ID:
        return True
    admin = await admins_col.find_one({"user_id": user_id})
    return admin is not None

async def save_user(user_id: int, username: str = None, first_name: str = None):
    await users_col.update_one(
        {"user_id": user_id},
        {"$set": {"user_id": user_id, "username": username,
                  "first_name": first_name, "last_active": datetime.now(timezone.utc)},
         "$setOnInsert": {"joined_at": datetime.now(timezone.utc),
                          "searches_count": 0, "is_banned": False,
                          "warn_count": 0, "commands_used": 0}},
        upsert=True
    )

async def log_action(action: str, user_id: int = None, details: str = None):
    await logs_col.insert_one({
        "action": action, "user_id": user_id,
        "details": details, "timestamp": datetime.now(timezone.utc)
    })

def check_rate_limit(user_id: int) -> bool:
    now = time.time()
    rate_limiter[user_id] = [t for t in rate_limiter[user_id] if now - t < RATE_LIMIT_WINDOW]
    if len(rate_limiter[user_id]) >= RATE_LIMIT_SEARCHES:
        return False
    rate_limiter[user_id].append(now)
    return True

async def get_force_join_channels() -> List[dict]:
    settings = await settings_col.find_one({"_id": "force_join"})
    if settings and "channels" in settings:
        return settings["channels"]
    return []

async def check_force_join(client: Client, user_id: int) -> Tuple[bool, list]:
    force_channels = await get_force_join_channels()
    if not force_channels:
        return True, []
    
    not_joined = []
    
    for ch in force_channels:
        channel_id = ch["channel_id"]
        fj_type = ch.get("type", "join")
        
        if fj_type == "request":
            request_doc = await force_join_requests_col.find_one({
                "user_id": user_id,
                "channel_id": channel_id
            })
            if request_doc:
                continue
            
            try:
                member = await client.get_chat_member(channel_id, user_id)
                if member.status not in [enums.ChatMemberStatus.BANNED, enums.ChatMemberStatus.LEFT]:
                    continue
            except UserNotParticipant:
                pass
            except Exception:
                pass
            
            not_joined.append(ch)
        else:
            try:
                member = await client.get_chat_member(channel_id, user_id)
                if member.status in [enums.ChatMemberStatus.BANNED, enums.ChatMemberStatus.LEFT]:
                    not_joined.append(ch)
            except UserNotParticipant:
                not_joined.append(ch)
            except Exception:
                pass
    
    return (len(not_joined) == 0), not_joined

async def get_active_ads() -> List[dict]:
    ads = []
    async for ad in ads_col.find({"active": True}).sort("slot", 1):
        ads.append(ad)
    return ads

def split_message(text: str, max_length: int = TG_MAX_LENGTH) -> List[str]:
    if len(text) <= max_length:
        return [text]
    parts = []
    while text:
        if len(text) <= max_length:
            parts.append(text)
            break
        split_pos = text.rfind('\n', 0, max_length)
        if split_pos == -1:
            split_pos = max_length
        parts.append(text[:split_pos])
        text = text[split_pos:].lstrip('\n')
    return parts

def format_time_delta(seconds: float) -> str:
    if seconds < 60:
        return f"{int(seconds)}s"
    elif seconds < 3600:
        m, s = divmod(int(seconds), 60)
        return f"{m}m {s}s"
    else:
        h, remainder = divmod(int(seconds), 3600)
        m, s = divmod(remainder, 60)
        return f"{h}h {m}m {s}s"

def is_link(text: str) -> bool:
    """Check if text contains a link"""
    url_pattern = re.compile(
        r'https?://'
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'
        r'localhost|'
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'
        r'(?::\d+)?'
        r'(?:/?|[/?]\S+)', re.IGNORECASE
    )
    
    if re.search(r'(?:^|\s)t\.me\/\S+', text, re.IGNORECASE):
        return True
    
    return bool(url_pattern.search(text))

def extract_domain(text: str) -> List[str]:
    domains = []
    url_pattern = re.compile(r'https?://([^/\s]+)', re.IGNORECASE)
    matches = url_pattern.findall(text)
    for match in matches:
        domain = match.replace('www.', '')
        domains.append(domain.lower())
    
    tme_pattern = re.compile(r'(?:^|\s)t\.me\/([^\s/]+)', re.IGNORECASE)
    tme_matches = tme_pattern.findall(text)
    for match in tme_matches:
        domains.append(f"t.me/{match}".lower())
    
    return domains

# ============================================================
# USER ROLE FUNCTIONS
# ============================================================

async def get_user_title(user_id: int) -> str:
    """Get user title based on role"""
    if user_id == OWNER_ID:
        return "Sᴇɴᴘᴀɪ"
    
    admin = await admins_col.find_one({"user_id": user_id})
    if admin:
        return "Sᴇɴᴘᴀɪ"
    
    return "Bᴀᴋᴀ"

# ============================================================
# ANTI-LINK FUNCTIONS
# ============================================================

async def get_antilink_settings() -> dict:
    settings = await antilink_settings_col.find_one({"_id": "settings"})
    if settings:
        return settings
    
    default_settings = {
        "_id": "settings",
        "enabled": True,
        "warn_limit": 3,
        "ban_duration": 10,
        "reset_time": 24,
        "action": "warn",
        "notify_admins": True,
        "log_channel": None
    }
    
    existing = await antilink_settings_col.find_one({"_id": "settings"})
    if not existing:
        await antilink_settings_col.insert_one(default_settings)
    
    return default_settings

async def update_antilink_settings(updates: dict):
    await antilink_settings_col.update_one(
        {"_id": "settings"},
        {"$set": updates},
        upsert=True
    )

async def get_user_violations(user_id: int, chat_id: int) -> dict:
    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    
    violation = await antilink_violations_col.find_one({
        "user_id": user_id,
        "chat_id": chat_id,
        "date": today
    })
    
    if violation:
        return {
            "count": violation.get("count", 0),
            "banned_until": violation.get("banned_until")
        }
    return {"count": 0, "banned_until": None}

async def increment_violation(user_id: int, chat_id: int, username: str = None) -> dict:
    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    
    await antilink_violations_col.update_one(
        {"user_id": user_id, "chat_id": chat_id, "date": today},
        {
            "$inc": {"count": 1},
            "$set": {"username": username, "last_violation": datetime.now(timezone.utc)},
            "$setOnInsert": {"date": today, "created_at": datetime.now(timezone.utc)}
        },
        upsert=True
    )
    
    violation = await antilink_violations_col.find_one({
        "user_id": user_id,
        "chat_id": chat_id,
        "date": today
    })
    
    return {"count": violation.get("count", 0) if violation else 1}

async def reset_user_violations(user_id: int, chat_id: int):
    await antilink_violations_col.delete_one({
        "user_id": user_id,
        "chat_id": chat_id
    })

async def set_user_banned(user_id: int, chat_id: int, duration: int):
    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    banned_until = datetime.now(timezone.utc) + timedelta(seconds=duration)
    
    await antilink_violations_col.update_one(
        {"user_id": user_id, "chat_id": chat_id, "date": today},
        {"$set": {"banned_until": banned_until}},
        upsert=True
    )
    return banned_until

async def get_whitelist() -> List[str]:
    whitelist = ["youtube.com", "youtu.be", "t.me", "telegram.me", 
                 "instagram.com", "facebook.com", "twitter.com", "x.com",
                 "github.com", "git.io", "google.com", "gmail.com"]
    async for doc in whitelist_col.find({"type": "domain"}):
        if doc.get("domain") not in whitelist:
            whitelist.append(doc.get("domain"))
    return whitelist

async def get_whitelist_users() -> List[int]:
    users = []
    async for doc in whitelist_col.find({"type": "user"}):
        users.append(doc.get("user_id"))
    return users

# ============================================================
# CHECK ADMIN FUNCTIONS
# ============================================================

async def check_admin(client: Client, message: Message):
    """Check if user is admin or owner"""
    user_id = message.from_user.id
    
    if user_id == OWNER_ID:
        return True
    
    admin = await admins_col.find_one({"user_id": user_id})
    if admin:
        return True
    
    await message.reply(ACCESS_DENIED.format(name=message.from_user.first_name))
    return False

async def check_owner(client: Client, message: Message):
    """Check if user is owner"""
    user_id = message.from_user.id
    
    if user_id == OWNER_ID:
        return True
    
    await message.reply(OWNER_DENIED.format(name=message.from_user.first_name))
    return False

# ============================================================
# WELCOME FUNCTIONS
# ============================================================

async def get_welcome_settings(chat_id: int) -> dict:
    settings = await settings_col.find_one({"_id": f"welcome_{chat_id}"})
    if settings:
        return settings
    
    default_settings = {
        "_id": f"welcome_{chat_id}",
        "enabled": True,
        "delete_service": True,
        "welcome_text": "<b>✦ ᴡᴇʟᴄᴏᴍᴇ {name}! ✦</b>\n\n<blockquote>🎀 ᴇɴᴊᴏʏ ʏᴏᴜʀ ꜱᴛᴀʏ ɪɴ ᴛʜᴇ ɢʀᴏᴜᴘ!</blockquote>",
        "send_to_admin": True,
        "admin_notify": "👤 <b>ɴᴇᴡ ᴜꜱᴇʀ ᴊᴏɪɴᴇᴅ!</b>\n\n<blockquote>📢 {name}\n🆔 <code>{user_id}</code></blockquote>"
    }
    await settings_col.insert_one(default_settings)
    return default_settings

async def update_welcome_settings(chat_id: int, updates: dict):
    await settings_col.update_one(
        {"_id": f"welcome_{chat_id}"},
        {"$set": updates},
        upsert=True
    )

# ============================================================
# BUTTON FUNCTIONS
# ============================================================

def get_start_menu_buttons():
    return [
        [
            InlineKeyboardButton("🇦", callback_data="alya_a"),
            InlineKeyboardButton("🇱", callback_data="alya_l"),
            InlineKeyboardButton("🇾", callback_data="alya_y"),
            InlineKeyboardButton("🇦", callback_data="alya_a2")
        ],
        [
            InlineKeyboardButton("💎 ᴀᴅᴅ ᴍᴇ ᴛᴏ ʏᴏᴜʀ ɢʀᴏᴜᴘ", url="https://t.me/Alya_Filter_Bot?startgroup=true")
        ],
        [
            InlineKeyboardButton("📜 ᴄᴏᴍᴍᴀɴᴅꜱ", callback_data="show_commands"),
            InlineKeyboardButton("📖 ᴀʙᴏᴜᴛ", callback_data="show_about")
        ],
        [
            InlineKeyboardButton("ℹ️ ʜᴇʟᴘ", callback_data="show_help")
        ]
    ]

def get_back_button():
    return [
        [InlineKeyboardButton("🔙 ʙᴀᴄᴋ", callback_data="back_to_start")]
    ]

def get_welcome_buttons():
    return [
        [InlineKeyboardButton("📜 ᴄᴏᴍᴍᴀɴᴅꜱ", callback_data="show_commands")],
        [InlineKeyboardButton("ℹ️ ᴀʙᴏᴜᴛ", callback_data="show_about")]
    ]

# ============================================================
# ACCESS DENIED MESSAGES
# ============================================================

ACCESS_DENIED = """
<b>✦ ᴀᴄᴄᴇꜱꜱ ᴅᴇɴɪᴇᴅ, {name}-ᴄʜᴀɴ! ✦</b>

<blockquote>🚫 <b>ᴀᴄᴄᴇꜱꜱ ᴅᴇɴɪᴇᴅ</b>

<i>"ʏᴏᴜʀ ɴᴏᴛ ᴍʏ ꜱᴇɴᴘᴀɪ, ꜱᴏ ꜰᴜᴄᴋ ᴏꜰꜰ"</i></blockquote>

╔═══════════════════════════════════════╗
║  💢 ɢᴇᴛ ᴏᴜᴛ ᴏꜰ ʜᴇʀᴇ,  💢          ║
║      ʙᴀᴋᴀ!  (｀Д´)                  ║
╚═══════════════════════════════════════╝

<blockquote>🎴 <i>"ᴏɴʟʏ ᴛʜᴇ ᴄʜᴏꜱᴇɴ ᴏɴᴇꜱ ᴍᴀʏ ᴘᴀꜱꜱ!"</i></blockquote>

～(╥﹏╥)～ <b>ᴘʟᴇᴀꜱᴇ ᴅᴏɴ'ᴛ ᴛʀʏ ᴀɢᴀɪɴ, ꜱᴇɴᴘᴀɪ!</b> ～(╥﹏╥)～
"""

OWNER_DENIED = """
👑 <b>✦ ᴏᴡɴᴇʀ ᴏɴʟʏ, {name}-ᴄʜᴀɴ! ✦</b> 👑

<blockquote>👑 <b>ᴏᴡɴᴇʀ ᴏɴʟʏ ᴄᴏᴍᴍᴀɴᴅ</b>

<i>"ʏᴏᴜʀ ɴᴏᴛ ᴍʏ ꜱᴇɴᴘᴀɪ, ꜱᴏ ꜰᴜᴄᴋ ᴏꜰꜰ"</i></blockquote>

💢 <b>ᴇᴠᴇɴ ᴀᴅᴍɪɴꜱ ᴄᴀɴ'ᴛ ᴜꜱᴇ ᴛʜɪꜱ!</b>

<blockquote>🎴 <i>"ᴏɴʟʏ ᴛʜᴇ ᴛʀᴜᴇ ᴍᴀꜱᴛᴇʀ ᴄᴀɴ ᴄᴏᴍᴍᴀɴᴅ ᴛʜɪꜱ ᴘᴏᴡᴇʀ!"</i></blockquote>

～(｀Д´)～ <b>ɢᴏ ᴀᴡᴀʏ, ʙᴀᴋᴀ!</b> ～(｀Д´)～
"""

# ============================================================
# SEND WELCOME WITH RANDOM IMAGE & SMALL CAPS
# ============================================================

async def send_welcome_with_image(client: Client, chat_id: int, user_name: str, edit_message_id: int = None):
    image_url = random.choice(START_IMAGES)
    current_hour = datetime.now(timezone.utc).hour + 5.5
    current_hour = current_hour % 24
    if 5 <= current_hour < 12:
        greeting = "Good Morning"
    elif 12 <= current_hour < 17:
        greeting = "Good Afternoon"
    elif 17 <= current_hour < 21:
        greeting = "Good Evening"
    else:
        greeting = "Good Night"

    raw_caption = (
        f"HEY {user_name}, {greeting}!\n\n"
        f"I AM THE MOST POWERFUL AUTO FILTER BOT WITH PREMIUM FEATURES,\n"
        f"JUST ADD ME TO YOUR GROUP AND ENJOY!\n\n"
        f"💎 DEVELOPED WITH 💎 BY PRIME CORE\n\n"
        f"~ (^∇^) ~ USE THE BUTTONS BELOW! ~ (^∇^) ~"
    )
    caption_small = small_caps(raw_caption)

    if edit_message_id:
        try:
            await client.edit_message_caption(
                chat_id=chat_id,
                message_id=edit_message_id,
                caption=caption_small,
                reply_markup=InlineKeyboardMarkup(get_start_menu_buttons())
            )
        except Exception as e:
            logger.error(f"Edit caption error: {e}")
            await client.send_photo(
                chat_id=chat_id,
                photo=image_url,
                caption=caption_small,
                reply_markup=InlineKeyboardMarkup(get_start_menu_buttons())
            )
    else:
        await client.send_photo(
            chat_id=chat_id,
            photo=image_url,
            caption=caption_small,
            reply_markup=InlineKeyboardMarkup(get_start_menu_buttons())
        )

# ============================================================
# START COMMAND
# ============================================================

@app.on_message(filters.command("start") & (filters.private | filters.group))
async def start_command(client: Client, message: Message):
    try:
        user_id = message.from_user.id
        await save_user(user_id, message.from_user.username, message.from_user.first_name)

        user_doc = await users_col.find_one({"user_id": user_id})
        if user_doc and user_doc.get("is_banned", False):
            await message.reply(
                "🚫 <b>✦ ʙᴀɴɴᴇᴅ, {name}-ꜱᴇɴᴘᴀɪ! ✦</b> 🚫\n\n"
                "ʏᴏᴜ ʜᴀᴠᴇ ʙᴇᴇɴ ʙᴀɴɴᴇᴅ ꜰʀᴏᴍ ᴜꜱɪɴɢ ᴛʜɪꜱ ʙᴏᴛ."
            )
            return

        is_group = message.chat.type in [enums.ChatType.GROUP, enums.ChatType.SUPERGROUP]
        if is_group:
            await message.reply(
                f"🌸 <b>✦ ᴡᴇʟᴄᴏᴍᴇ ᴛᴏ ᴛʜᴇ ɢʀᴏᴜᴘ, {message.from_user.first_name}-ꜱᴇɴᴘᴀɪ! ✦</b> 🌸\n\n"
                f"<blockquote>🎴 <b>ᴀʟʏᴀ ʙᴏᴛ ɪꜱ ʜᴇʀᴇ ᴛᴏ ʜᴇʟᴘ!</b>\n\n"
                f"📌 ᴜꜱᴇ /help ᴛᴏ ꜱᴇᴇ ᴄᴏᴍᴍᴀɴᴅꜱ</blockquote>",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("📜 ᴄᴏᴍᴍᴀɴᴅꜱ", callback_data="show_commands")],
                    [InlineKeyboardButton("ℹ️ ᴀʙᴏᴜᴛ", callback_data="show_about")]
                ])
            )
            return

        force_channels = await get_force_join_channels()
        if force_channels:
            not_joined = []
            for ch in force_channels:
                try:
                    member = await client.get_chat_member(ch["channel_id"], user_id)
                    if member.status in [enums.ChatMemberStatus.BANNED, enums.ChatMemberStatus.LEFT]:
                        not_joined.append(ch)
                except:
                    not_joined.append(ch)
            if not_joined:
                channels_text = ""
                for ch in not_joined:
                    channels_text += f"\n  📢 <b>{ch.get('title', 'Channel')}</b>\n  🔗 {ch.get('invite_url', 'https://t.me')}\n"
                buttons = []
                for ch in not_joined:
                    buttons.append([InlineKeyboardButton(
                        f"📢 {ch.get('title', 'Join Channel')}", 
                        url=ch.get("invite_url", "https://t.me")
                    )])
                buttons.append([InlineKeyboardButton("✅ ɪ ᴊᴏɪɴᴇᴅ", callback_data="check_join")])
                await message.reply(
                    f"<b>✦ ɴᴏᴛɪᴄᴇ, {message.from_user.first_name}-ꜱᴇɴᴘᴀɪ! ✦</b>\n\n"
                    f"<blockquote>🔒 <b>ᴊᴏɪɴ ʀᴇQᴜɪʀᴇᴅ</b>\n\n"
                    f"ᴘʟᴇᴀꜱᴇ ᴊᴏɪɴ ᴛʜᴇꜱᴇ ᴄʜᴀɴɴᴇʟꜱ ꜰɪʀꜱᴛ:\n"
                    f"{channels_text}</blockquote>",
                    reply_markup=InlineKeyboardMarkup(buttons)
                )
                return

        await client.send_chat_action(message.chat.id, enums.ChatAction.TYPING)
        await asyncio.sleep(1.5)
        await send_welcome_with_image(client, message.chat.id, message.from_user.first_name)

    except Exception as e:
        logger.error(f"Start error: {e}")
        await message.reply("❌ <b>ᴇʀʀᴏʀ!</b>\n\nꜱᴏᴍᴇᴛʜɪɴɢ ᴡᴇɴᴛ ᴡʀᴏɴɢ, ꜱᴇɴᴘᴀɪ!")

# ============================================================
# ALYA BUTTON CALLBACKS (Do Nothing)
# ============================================================

@app.on_callback_query(filters.regex("^alya_"))
async def alya_callback(client: Client, callback: CallbackQuery):
    pass

# ============================================================
# COMMANDS, ABOUT, HELP, BACK CALLBACKS
# ============================================================

@app.on_callback_query(filters.regex("^show_commands$"))
async def show_commands_callback(client: Client, callback: CallbackQuery):
    try:
        await callback.answer()
        commands_text = (
            "📜 <b>✦ ᴄᴏᴍᴍᴀɴᴅꜱ ʟɪꜱᴛ ✦</b> 📜\n\n"
            "<blockquote>🎯 <b>ᴜꜱᴇʀ ᴄᴏᴍᴍᴀɴᴅꜱ:</b>\n"
            "/start - ꜱᴛᴀʀᴛ ᴛʜᴇ ʙᴏᴛ\n"
            "/help - ꜱʜᴏᴡ ʜᴇʟᴘ\n"
            "/listfilters - ʟɪꜱᴛ ꜰɪʟᴛᴇʀꜱ\n"
            "/filterstats - ꜰɪʟᴛᴇʀ ꜱᴛᴀᴛꜱ\n"
            "/ping - ᴄʜᴇᴄᴋ ʙᴏᴛ ꜱᴛᴀᴛᴜꜱ</blockquote>\n\n"
            "<blockquote>🛡️ <b>ᴀᴅᴍɪɴ ᴄᴏᴍᴍᴀɴᴅꜱ:</b>\n"
            "/addfilter - ᴀᴅᴅ ꜰɪʟᴛᴇʀ\n"
            "/editfilter - ᴇᴅɪᴛ ꜰɪʟᴛᴇʀ\n"
            "/delfilter - ᴅᴇʟᴇᴛᴇ ꜰɪʟᴛᴇʀ\n"
            "/ban - ʙᴀɴ ᴜꜱᴇʀ\n"
            "/unban - ᴜɴʙᴀɴ ᴜꜱᴇʀ\n"
            "/broadcast - ʙʀᴏᴀᴅᴄᴀꜱᴛ ᴍꜱɢ\n"
            "/antilink - ᴀɴᴛɪ-ʟɪɴᴋ ꜱᴇᴛᴛɪɴɢꜱ\n"
            "/welcome - ᴡᴇʟᴄᴏᴍᴇ ꜱᴇᴛᴛɪɴɢꜱ</blockquote>\n\n"
            "<blockquote>👑 <b>ᴏᴡɴᴇʀ ᴄᴏᴍᴍᴀɴᴅꜱ:</b>\n"
            "/addadmin - ᴀᴅᴅ ᴀᴅᴍɪɴ\n"
            "/deladmin - ʀᴇᴍᴏᴠᴇ ᴀᴅᴍɪɴ\n"
            "/admins - ʟɪꜱᴛ ᴀᴅᴍɪɴꜱ</blockquote>\n\n"
            "💫 <b>ᴜꜱᴇ /help ꜰᴏʀ ᴅᴇᴛᴀɪʟꜱ!</b>"
        )
        await callback.message.edit_text(
            commands_text,
            reply_markup=InlineKeyboardMarkup(get_back_button())
        )
    except Exception as e:
        if "MESSAGE_NOT_MODIFIED" not in str(e):
            logger.error(f"Show commands error: {e}")

@app.on_callback_query(filters.regex("^show_about$"))
async def show_about_callback(client: Client, callback: CallbackQuery):
    try:
        await callback.answer()
        about_text = (
            "📖 <b>✦ ᴀʙᴏᴜᴛ ᴀʟʏᴀ ʙᴏᴛ ✦</b> 📖\n\n"
            "<blockquote>🎀 <b>ᴀʟʏᴀ ʙᴏᴛ ᴠ𝟮.𝟬</b> 🎀\n\n"
            "🔹 <b>ᴘᴏᴡᴇʀꜰᴜʟ ꜰɪʟᴛᴇʀ ʙᴏᴛ</b>\n"
            "🔹 <b>ᴀᴜᴛᴏ-ʀᴇᴘʟʏ ᴛᴏ ᴋᴇʏᴡᴏʀᴅꜱ</b>\n"
            "🔹 <b>ᴍᴇᴅɪᴀ ꜰɪʟᴛᴇʀꜱ (ᴘʜᴏᴛᴏ, ᴠɪᴅᴇᴏ, ꜱᴛɪᴄᴋᴇʀ)</b>\n"
            "🔹 <b>ᴀɴᴛɪ-ʟɪɴᴋ ᴘʀᴏᴛᴇᴄᴛɪᴏɴ</b>\n"
            "🔹 <b>ᴀᴜᴛᴏ ᴡᴇʟᴄᴏᴍᴇ ᴍꜱɢ</b>\n"
            "🔹 <b>ᴀᴅᴍɪɴ ᴍᴀɴᴀɢᴇᴍᴇɴᴛ</b></blockquote>\n\n"
            "<blockquote>💫 <b>ᴅᴇᴠᴇʟᴏᴘᴇᴅ ᴡɪᴛʜ ❤️ ʙʏ</b>\n"
            "👑 <b><a href='https://t.me/PrimeCoreHQ'>ᴘʀɪᴍᴇ ᴄᴏʀᴇ</a></b></blockquote>\n\n"
            "～(^▽^)～ <b>ᴛʜᴀɴᴋ ʏᴏᴜ ꜰᴏʀ ᴜꜱɪɴɢ ᴀʟʏᴀ ʙᴏᴛ!</b> ～(^▽^)～"
        )
        await callback.message.edit_text(
            about_text,
            reply_markup=InlineKeyboardMarkup(get_back_button())
        )
    except Exception as e:
        if "MESSAGE_NOT_MODIFIED" not in str(e):
            logger.error(f"Show about error: {e}")

@app.on_callback_query(filters.regex("^show_help$"))
async def show_help_callback(client: Client, callback: CallbackQuery):
    try:
        await callback.answer()
        help_text = (
            "ℹ️ <b>✦ ʜᴇʟᴘ ɢᴜɪᴅᴇ ✦</b> ℹ️\n\n"
            "<blockquote>🎯 <b>ʜᴏᴡ ᴛᴏ ᴜꜱᴇ:</b>\n\n"
            "📌 <b>ɪɴ ᴘʀɪᴠᴀᴛᴇ ᴄʜᴀᴛ:</b>\n"
            "ᴊᴜꜱᴛ ᴛʏᴘᴇ ᴀɴʏ ᴋᴇʏᴡᴏʀᴅ\n"
            "ᴇxᴀᴍᴘʟᴇ: `ʜᴇʟʟᴏ` → ʙᴏᴛ ʀᴇᴘʟɪᴇꜱ\n\n"
            "📌 <b>ɪɴ ɢʀᴏᴜᴘꜱ:</b>\n"
            "ᴀᴅᴍɪɴꜱ ᴀᴅᴅ ꜰɪʟᴛᴇʀꜱ\n"
            "ᴜꜱᴇʀꜱ ᴛʀɪɢɢᴇʀ ᴛʜᴇᴍ</blockquote>\n\n"
            "<blockquote>💢 <b>ᴛɪᴘ:</b>\n"
            "ꜱᴇɴᴅ /help ɪɴ ᴘʀɪᴠᴀᴛᴇ ꜰᴏʀ ᴅᴇᴛᴀɪʟꜱ!</blockquote>\n\n"
            "～(^▽^)～ <b>ᴇɴᴊᴏʏ ᴜꜱɪɴɢ ᴀʟʏᴀ ʙᴏᴛ!</b> ～(^▽^)～"
        )
        await callback.message.edit_text(
            help_text,
            reply_markup=InlineKeyboardMarkup(get_back_button())
        )
    except Exception as e:
        if "MESSAGE_NOT_MODIFIED" not in str(e):
            logger.error(f"Show help error: {e}")

@app.on_callback_query(filters.regex("^back_to_start$"))
async def back_to_start_callback(client: Client, callback: CallbackQuery):
    try:
        await callback.answer()
        await send_welcome_with_image(
            client,
            callback.from_user.id,
            callback.from_user.first_name,
            edit_message_id=callback.message.id
        )
    except Exception as e:
        if "MESSAGE_NOT_MODIFIED" not in str(e):
            logger.error(f"Back to start error: {e}")

@app.on_callback_query(filters.regex("^check_join$"))
async def check_join_callback(client: Client, callback: CallbackQuery):
    try:
        user_id = callback.from_user.id
        force_channels = await get_force_join_channels()
        not_joined = []
        for ch in force_channels:
            try:
                member = await client.get_chat_member(ch["channel_id"], user_id)
                if member.status in [enums.ChatMemberStatus.BANNED, enums.ChatMemberStatus.LEFT]:
                    not_joined.append(ch)
            except:
                not_joined.append(ch)
        if not not_joined:
            await callback.message.edit_text(
                f"✅ <b>✦ ᴠᴇʀɪꜰɪᴄᴀᴛɪᴏɴ ᴄᴏᴍᴘʟᴇᴛᴇ, {callback.from_user.first_name}-ꜱᴇɴᴘᴀɪ! ✦</b> ✅\n\n"
                f"<blockquote>🎴 <b>ʏᴏᴜ ᴄᴀɴ ɴᴏᴡ ᴜꜱᴇ ᴛʜᴇ ʙᴏᴛ!</b></blockquote>",
                reply_markup=InlineKeyboardMarkup(get_welcome_buttons())
            )
        else:
            names = ", ".join([ch.get("title", "Channel") for ch in not_joined])
            await callback.answer(f"❌ ꜱᴛɪʟʟ ɴᴏᴛ ᴊᴏɪɴᴇᴅ, ꜱᴇɴᴘᴀɪ! ᴘʟᴇᴀꜱᴇ ᴊᴏɪɴ: {names}", show_alert=True)
    except Exception as e:
        logger.error(f"Check join error: {e}")

# ============================================================
# WELCOME HANDLER (Group Only)
# ============================================================

@app.on_message(filters.group & (filters.new_chat_members | filters.left_chat_member))
async def handle_member_updates(client: Client, message: Message):
    try:
        if not message.chat.type in [enums.ChatType.GROUP, enums.ChatType.SUPERGROUP]:
            return
        if not message.new_chat_members and not message.left_chat_member:
            return
        chat_id = message.chat.id
        chat_title = message.chat.title
        settings = await get_welcome_settings(chat_id)
        if not settings.get("enabled", True):
            return
        if message.new_chat_members:
            for member in message.new_chat_members:
                if member.id == client.me.id:
                    continue
                member_name = member.first_name or "Uꜱᴇʀ"
                member_id = member.id
                welcome_text = settings.get("welcome_text", "<b>✦ ᴡᴇʟᴄᴏᴍᴇ {name}! ✦</b>")
                welcome_text = welcome_text.format(
                    name=member_name,
                    user_id=member_id,
                    chat_title=chat_title
                )
                await message.reply(welcome_text)
                await log_action("welcome_sent", member_id, f"Group: {chat_title} ({chat_id})")
                if settings.get("send_to_admin", True):
                    admin_notify = settings.get("admin_notify", "👤 <b>ɴᴇᴡ ᴜꜱᴇʀ ᴊᴏɪɴᴇᴅ!</b>\n\n<blockquote>📢 {name}\n🆔 <code>{user_id}</code></blockquote>")
                    admin_notify = admin_notify.format(
                        name=member_name,
                        user_id=member_id,
                        chat_title=chat_title
                    )
                    admin_list = [OWNER_ID]
                    async for admin in admins_col.find():
                        admin_list.append(admin["user_id"])
                    for admin_id in admin_list:
                        try:
                            await client.send_message(admin_id, admin_notify)
                        except Exception:
                            pass
                if settings.get("delete_service", True):
                    try:
                        await message.delete()
                    except Exception:
                        pass
        elif message.left_chat_member:
            if settings.get("delete_service", True):
                try:
                    await message.delete()
                except Exception:
                    pass
    except Exception as e:
        logger.error(f"Welcome handler error: {e}")

# ============================================================
# ANTI-LINK HANDLER (Full)
# ============================================================

@app.on_message(filters.text & (filters.group | filters.private))
async def advanced_antilink_handler(client: Client, message: Message):
    try:
        if not message.text:
            return
        if await is_admin(message.from_user.id):
            return
        settings = await get_antilink_settings()
        if not settings.get("enabled", True):
            return
        if not is_link(message.text):
            return
        domains = extract_domain(message.text)
        whitelist = await get_whitelist()
        whitelist_users = await get_whitelist_users()
        if message.from_user.id in whitelist_users:
            return
        for domain in domains:
            for wl in whitelist:
                if domain == wl or domain.endswith(f".{wl}"):
                    return
        violation_data = await get_user_violations(message.from_user.id, message.chat.id)
        violation_count = violation_data.get("count", 0)
        banned_until = violation_data.get("banned_until")
        user_title = await get_user_title(message.from_user.id)
        if banned_until:
            if datetime.now(timezone.utc) < banned_until:
                remaining = (banned_until - datetime.now(timezone.utc)).seconds
                if user_title == "Sᴇɴᴘᴀɪ":
                    ban_wait_msg = (
                        f"🚫 <b>✦ ʏᴏᴜ ᴀʀᴇ ʙᴀɴɴᴇᴅ, Sᴇɴᴘᴀɪ! ✦</b> 🚫\n\n"
                        f"<blockquote>ʏᴏᴜ ᴡᴇʀᴇ ʙᴀɴɴᴇᴅ ꜰᴏʀ ꜱᴇɴᴅɪɴɢ ʟɪɴᴋꜱ.\n\n"
                        f"⏱️ <b>ʀᴇᴍᴀɪɴɪɴɢ ᴛɪᴍᴇ:</b> {remaining}s</blockquote>\n"
                        f"💢 <b>ᴘʟᴇᴀꜱᴇ ᴡᴀɪᴛ, Sᴇɴᴘᴀɪ!</b> (｀Д´)"
                    )
                else:
                    ban_wait_msg = (
                        f"🚫 <b>✦ ʏᴏᴜ ᴀʀᴇ ʙᴀɴɴᴇᴅ, Bᴀᴋᴀ! ✦</b> 🚫\n\n"
                        f"<blockquote>ʏᴏᴜ ᴡᴇʀᴇ ʙᴀɴɴᴇᴅ ꜰᴏʀ ꜱᴇɴᴅɪɴɢ ʟɪɴᴋꜱ.\n\n"
                        f"⏱️ <b>ʀᴇᴍᴀɪɴɪɴɢ ᴛɪᴍᴇ:</b> {remaining}s</blockquote>\n"
                        f"💢 <b>ᴡᴀɪᴛ ʙᴇꜰᴏʀᴇ ꜱᴘᴀᴍᴍɪɴɢ ᴀɢᴀɪɴ, Bᴀᴋᴀ!</b> (｀Д´)"
                    )
                await message.reply(ban_wait_msg)
                try:
                    await message.delete()
                except:
                    pass
                return
        new_violation = await increment_violation(
            message.from_user.id,
            message.chat.id,
            message.from_user.username
        )
        new_count = new_violation.get("count", 0)
        warn_limit = settings.get("warn_limit", 3)
        ban_duration = settings.get("ban_duration", 10)
        notify_admins = settings.get("notify_admins", True)
        if notify_admins and new_count >= 1:
            admin_list = [OWNER_ID]
            async for admin in admins_col.find():
                admin_list.append(admin["user_id"])
            alert_msg = (
                f"⚠️ <b>✦ ʟɪɴᴋ ᴠɪᴏʟᴀᴛɪᴏɴ ᴀʟᴇʀᴛ! ✦</b> ⚠️\n\n"
                f"<blockquote>👤 <b>ᴜꜱᴇʀ:</b> {message.from_user.username or message.from_user.id}\n"
                f"🆔 <b>ᴜꜱᴇʀ ɪᴅ:</b> <code>{message.from_user.id}</code>\n"
                f"📢 <b>ᴄʜᴀᴛ:</b> {message.chat.title or 'Private'}\n"
                f"🆔 <b>ᴄʜᴀᴛ ɪᴅ:</b> <code>{message.chat.id}</code>\n"
                f"📊 <b>ᴠɪᴏʟᴀᴛɪᴏɴꜱ:</b> {new_count}/3\n"
                f"🔗 <b>ʟɪɴᴋ:</b> <code>{message.text[:100]}</code></blockquote>\n"
                f"💢 <b>{user_title} ɪꜱ ꜱᴘᴀᴍᴍɪɴɢ ʟɪɴᴋꜱ!</b> (｀Д´)"
            )
            for admin_id in admin_list:
                try:
                    await client.send_message(admin_id, alert_msg)
                except Exception:
                    pass
        if new_count >= warn_limit:
            banned_until = await set_user_banned(
                message.from_user.id,
                message.chat.id,
                ban_duration
            )
            try:
                await message.delete()
            except:
                pass
            if user_title == "Sᴇɴᴘᴀɪ":
                ban_msg = (
                    f"🚫 <b>✦ Sᴇɴᴘᴀɪ ɢᴏᴛ ʙᴀɴɴᴇᴅ! ✦</b> 🚫\n\n"
                    f"<blockquote>🔗 <b>ʟɪɴᴋ ᴅᴇᴛᴇᴄᴛᴇᴅ!</b>\n\n"
                    f"Sᴇɴᴘᴀɪ ʜᴀꜱ ʀᴇᴀᴄʜᴇᴅ ᴛʜᴇ ᴍᴀxɪᴍᴜᴍ ᴡᴀʀɴɪɴɢꜱ ({warn_limit}).\n\n"
                    f"⏱️ <b>Bᴀɴɴᴇᴅ ꜰᴏʀ {ban_duration} ꜱᴇᴄᴏɴᴅꜱ!</b></blockquote>\n"
                    f"💢 <b>Eᴠᴇɴ Sᴇɴᴘᴀɪ ᴄᴀɴ'ᴛ ʙʀᴇᴀᴋ ᴛʜᴇ ʀᴜʟᴇꜱ!</b> (｀Д´)"
                )
            else:
                ban_msg = (
                    f"🚫 <b>✦ ʏᴏᴜ'ʀᴇ ʙᴀɴɴᴇᴅ, Bᴀᴋᴀ! ✦</b> 🚫\n\n"
                    f"<blockquote>🔗 <b>ʟɪɴᴋ ᴅᴇᴛᴇᴄᴛᴇᴅ!</b>\n\n"
                    f"ʏᴏᴜ ʜᴀᴠᴇ ʀᴇᴀᴄʜᴇᴅ ᴛʜᴇ ᴍᴀxɪᴍᴜᴍ ᴡᴀʀɴɪɴɢꜱ ({warn_limit}).\n\n"
                    f"⏱️ <b>ʏᴏᴜ ᴀʀᴇ ʙᴀɴɴᴇᴅ ꜰᴏʀ {ban_duration} ꜱᴇᴄᴏɴᴅꜱ!</b></blockquote>\n"
                    f"💢 <b>ɴᴇxᴛ ᴛɪᴍᴇ ɪᴛ ᴡɪʟʟ ʙᴇ ᴘᴇʀᴍᴀɴᴇɴᴛ, Bᴀᴋᴀ!</b> (｀Д´)"
                )
            await message.reply(ban_msg)
            if notify_admins:
                ban_alert = (
                    f"🚫 <b>✦ ᴜꜱᴇʀ ʙᴀɴɴᴇᴅ! ✦</b> 🚫\n\n"
                    f"<blockquote>👤 <b>ᴜꜱᴇʀ:</b> {message.from_user.username or message.from_user.id}\n"
                    f"🆔 <b>ɪᴅ:</b> <code>{message.from_user.id}</code>\n"
                    f"📢 <b>ᴄʜᴀᴛ:</b> {message.chat.title or 'Private'}\n"
                    f"⏱️ <b>ʙᴀɴɴᴇᴅ ꜰᴏʀ:</b> {ban_duration}s\n"
                    f"📊 <b>ᴠɪᴏʟᴀᴛɪᴏɴꜱ:</b> {new_count}\n"
                    f"👑 <b>ʀᴏʟᴇ:</b> {user_title}</blockquote>\n"
                    f"💢 <b>{user_title} ɢᴏᴛ ʙᴀɴɴᴇᴅ ꜰᴏʀ ꜱᴘᴀᴍᴍɪɴɢ ʟɪɴᴋꜱ!</b> (｀Д´)"
                )
                admin_list = [OWNER_ID]
                async for admin in admins_col.find():
                    admin_list.append(admin["user_id"])
                for admin_id in admin_list:
                    try:
                        await client.send_message(admin_id, ban_alert)
                    except:
                        pass
            await log_action("antilink_ban", message.from_user.id, 
                             f"Chat: {message.chat.id}, Violations: {new_count}, Role: {user_title}")
        else:
            remaining_warnings = warn_limit - new_count
            try:
                await message.delete()
            except:
                pass
            if user_title == "Sᴇɴᴘᴀɪ":
                warn_msg = (
                    f"⚠️ <b>✦ Wᴀʀɴɪɴɢ, Sᴇɴᴘᴀɪ! ✦</b> ⚠️\n\n"
                    f"<blockquote>🔗 <b>ʟɪɴᴋ ᴅᴇᴛᴇᴄᴛᴇᴅ!</b>\n\n"
                    f"Pʟᴇᴀꜱᴇ ᴅᴏ ɴᴏᴛ ꜱᴇɴᴅ ʟɪɴᴋꜱ ɪɴ ᴛʜɪꜱ ᴄʜᴀᴛ.\n\n"
                    f"⚠️ <b>Wᴀʀɴɪɴɢ {new_count}/{warn_limit}</b>\n"
                    f"⏳ <b>Rᴇᴍᴀɪɴɪɴɢ ᴡᴀʀɴɪɴɢꜱ:</b> {remaining_warnings}</blockquote>\n"
                    f"💢 <b>Sᴇɴᴘᴀɪ, ᴘʟᴇᴀꜱᴇ ꜱᴛᴏᴘ!</b> (｀Д´)\n\n"
                    f"～(╥﹏╥)～ <b>Dᴏɴ'ᴛ ᴍᴀᴋᴇ ᴍᴇ ʙᴀɴ ʏᴏᴜ, Sᴇɴᴘᴀɪ!</b> ～(╥﹏╥)～"
                )
            else:
                warn_msg = (
                    f"⚠️ <b>✦ Wᴀʀɴɪɴɢ, Bᴀᴋᴀ! ✦</b> ⚠️\n\n"
                    f"<blockquote>🔗 <b>ʟɪɴᴋ ᴅᴇᴛᴇᴄᴛᴇᴅ!</b>\n\n"
                    f"Pʟᴇᴀꜱᴇ ᴅᴏ ɴᴏᴛ ꜱᴇɴᴅ ʟɪɴᴋꜱ ɪɴ ᴛʜɪꜱ ᴄʜᴀᴛ.\n\n"
                    f"⚠️ <b>Wᴀʀɴɪɴɢ {new_count}/{warn_limit}</b>\n"
                    f"⏳ <b>Rᴇᴍᴀɪɴɪɴɢ ᴡᴀʀɴɪɴɢꜱ:</b> {remaining_warnings}</blockquote>\n"
                    f"💢 <b>Sᴛᴏᴘ ɪᴛ, Bᴀᴋᴀ!</b> (｀Д´)\n\n"
                    f"～(╥﹏╥)～ <b>Dᴏɴ'ᴛ ᴍᴀᴋᴇ ᴍᴇ ʙᴀɴ ʏᴏᴜ!</b> ～(╥﹏╥)～"
                )
            await message.reply(warn_msg)
            await log_action("antilink_warn", message.from_user.id, 
                             f"Chat: {message.chat.id}, Warning: {new_count}/{warn_limit}, Role: {user_title}")
    except Exception as e:
        logger.error(f"Advanced anti-link error: {e}")

# ============================================================
# FILTER COMMANDS (Full Set with Button Support)
# ============================================================

# --- Add Filter (extended with button support) ---
@app.on_message(filters.command("addfilter") & filters.private)
async def add_filter_command(client: Client, message: Message):
    try:
        if not await check_admin(client, message):
            return

        if message.reply_to_message:
            replied = message.reply_to_message
            if len(message.command) < 2:
                await message.reply("📁 Usage: /addfilter keyword (reply to media)")
                return
            keyword = message.command[1].lower().strip()
            existing = await filters_col.find_one({"keyword": keyword})
            if existing:
                await message.reply(f"⚠️ Filter `{keyword}` already exists!")
                return

            media_type = None
            media_file_id = None
            caption = replied.caption or ""

            if replied.photo:
                media_type = "photo"
                media_file_id = replied.photo.file_id
            elif replied.video:
                media_type = "video"
                media_file_id = replied.video.file_id
            elif replied.document:
                media_type = "document"
                media_file_id = replied.document.file_id
            elif replied.sticker:
                media_type = "sticker"
                media_file_id = replied.sticker.file_id
            elif replied.animation:
                media_type = "animation"
                media_file_id = replied.animation.file_id
            elif replied.audio:
                media_type = "audio"
                media_file_id = replied.audio.file_id
            else:
                await message.reply("❌ Unsupported media type!")
                return

            # Capture optional button from command
            button_text = None
            button_url = None
            # format: /addfilter keyword [button_text] [button_url]
            if len(message.command) >= 3:
                button_text = message.command[2]
            if len(message.command) >= 4:
                button_url = message.command[3]
            # If only one extra arg, treat as button_text and use default URL? Actually better: require both or none.
            # We'll treat: if button_text provided but no button_url, we set button_url = button_text? No.
            # Instead: if len == 3, we treat as button_text and url = button_text (for simplicity)
            if len(message.command) == 3:
                button_url = button_text  # Treat as URL if only one extra
                button_text = "🔗 Click"
            # But user might provide both.

            filter_data = {
                "keyword": keyword,
                "media_type": media_type,
                "media_file_id": media_file_id,
                "caption": caption,
                "button_text": button_text,
                "button_url": button_url,
                "created_by": message.from_user.id,
                "created_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc),
                "usage_count": 0,
                "group": "general",
                "is_active": True
            }
            await filters_col.insert_one(filter_data)
            await message.reply(f"✅ Filter `{keyword}` added with media and button.")
            await log_action("filter_added_media", message.from_user.id, f"Keyword: {keyword}")
        else:
            # Text filter with optional button
            parts = message.text.split(None, 4)  # up to 5 parts
            if len(parts) < 3:
                await message.reply(
                    "📁 Usage:\n"
                    "/addfilter keyword reply_text [button_text] [button_url]\n"
                    "Example: /addfilter hello Hi there! Visit us https://t.me/example"
                )
                return
            keyword = parts[1].lower().strip()
            reply_text = parts[2]
            button_text = parts[3] if len(parts) > 3 else None
            button_url = parts[4] if len(parts) > 4 else None

            existing = await filters_col.find_one({"keyword": keyword})
            if existing:
                await message.reply(f"⚠️ Filter `{keyword}` already exists!")
                return

            # Save filter
            await filters_col.insert_one({
                "keyword": keyword,
                "reply_text": reply_text,
                "replies": reply_text.split("|") if "|" in reply_text else [reply_text],
                "button_text": button_text,
                "button_url": button_url,
                "created_by": message.from_user.id,
                "created_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc),
                "usage_count": 0,
                "group": "general",
                "media_type": "text",
                "media_file_id": None,
                "is_active": True
            })
            await message.reply(f"✅ Filter `{keyword}` added.")
            await log_action("filter_added", message.from_user.id, f"Keyword: {keyword}")
    except Exception as e:
        logger.error(f"Add filter error: {e}")
        await message.reply(f"❌ Error: {e}")

# --- Edit Filter ---
@app.on_message(filters.command("editfilter") & filters.private)
async def edit_filter_command(client: Client, message: Message):
    try:
        if not await check_admin(client, message):
            return
        if len(message.command) < 3:
            await message.reply("❌ Usage: /editfilter keyword new_reply")
            return
        keyword = message.command[1].lower().strip()
        new_reply = " ".join(message.command[2:])
        filter_doc = await filters_col.find_one({"keyword": keyword})
        if not filter_doc:
            await message.reply(f"❌ Filter `{keyword}` not found.")
            return
        await filters_col.update_one(
            {"keyword": keyword},
            {"$set": {
                "reply_text": new_reply,
                "replies": new_reply.split("|") if "|" in new_reply else [new_reply],
                "updated_at": datetime.now(timezone.utc)
            }}
        )
        await message.reply(f"✅ Filter `{keyword}` updated.")
        await log_action("filter_edited", message.from_user.id, f"Keyword: {keyword}")
    except Exception as e:
        logger.error(f"Edit filter error: {e}")
        await message.reply(f"❌ Error: {e}")

# --- Delete Filter ---
@app.on_message(filters.command("delfilter") & filters.private)
async def delete_filter_command(client: Client, message: Message):
    try:
        if not await check_admin(client, message):
            return
        if len(message.command) < 2:
            await message.reply("❌ Usage: /delfilter keyword")
            return
        keyword = message.command[1].lower().strip()
        filter_doc = await filters_col.find_one({"keyword": keyword})
        if not filter_doc:
            await message.reply(f"❌ Filter `{keyword}` not found.")
            return
        await filters_col.delete_one({"keyword": keyword})
        await message.reply(f"✅ Filter `{keyword}` deleted.")
        await log_action("filter_deleted", message.from_user.id, f"Keyword: {keyword}")
    except Exception as e:
        logger.error(f"Delete filter error: {e}")
        await message.reply(f"❌ Error: {e}")

# --- List Filters ---
@app.on_message(filters.command("listfilters") & (filters.private | filters.group))
async def list_filters_command(client: Client, message: Message):
    try:
        filters_list = await filters_col.find({"is_active": True}).sort("keyword", 1).to_list(length=None)
        if not filters_list:
            await message.reply("📭 No filters found.")
            return
        groups = defaultdict(list)
        for f in filters_list:
            groups[f.get("group", "general")].append(f)
        text = "📋 All Filters:\n\n"
        total = 0
        for group_name, f_list in groups.items():
            text += f"📁 {group_name.upper()} ({len(f_list)})\n"
            for f in f_list[:10]:
                usage = f.get("usage_count", 0)
                replies = len(f.get("replies", [f.get("reply_text", "")]))
                media_icon = "📷" if f.get("media_type") == "photo" else "🎬" if f.get("media_type") == "video" else "📄" if f.get("media_type") == "document" else "🎨" if f.get("media_type") == "sticker" else "📝"
                button_indicator = "🔗" if f.get("button_text") else ""
                text += f"   {media_icon} `{f['keyword']}` 👁️{usage} 💬{replies} {button_indicator}\n"
            if len(f_list) > 10:
                text += f"   ... and {len(f_list)-10} more\n"
            text += "\n"
            total += len(f_list)
        text += f"📊 Total: {total} filters"
        for part in split_message(text):
            await message.reply(part)
    except Exception as e:
        logger.error(f"List filters error: {e}")
        await message.reply(f"❌ Error: {e}")

# --- Filter Stats ---
@app.on_message(filters.command("filterstats") & filters.private)
async def filter_stats_command(client: Client, message: Message):
    try:
        if not await check_admin(client, message):
            return
        total_filters = await filters_col.count_documents({"is_active": True})
        total_usage = await filter_logs_col.count_documents({})
        today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        today_usage = await filter_logs_col.count_documents({"used_at": {"$gte": today}})
        week = datetime.now(timezone.utc) - timedelta(days=7)
        week_usage = await filter_logs_col.count_documents({"used_at": {"$gte": week}})
        top_filters = await filters_col.find({"is_active": True}).sort("usage_count", -1).limit(10).to_list(length=10)
        top_text = ""
        if top_filters:
            top_text = "\n\n🏆 Top 10 Most Used:\n"
            for i, f in enumerate(top_filters, 1):
                top_text += f"   {i}. `{f['keyword']}` - {f.get('usage_count', 0)} uses\n"
        groups_dist = await filters_col.distinct("group")
        groups_text = "\n\n📁 Group Distribution:\n"
        for group in groups_dist:
            count = await filters_col.count_documents({"group": group, "is_active": True})
            groups_text += f"   • {group}: {count} filters\n"
        await message.reply(
            f"📊 Filter Statistics:\n\n📌 Total Filters: {total_filters}\n"
            f"👁️ Total Uses: {total_usage}\n📅 Today: {today_usage}\n📆 This Week: {week_usage}{top_text}{groups_text}"
        )
    except Exception as e:
        logger.error(f"Filter stats error: {e}")
        await message.reply(f"❌ Error: {e}")

# --- Add Reply ---
@app.on_message(filters.command("addreply") & filters.private)
async def add_reply_command(client: Client, message: Message):
    try:
        if not await check_admin(client, message):
            return
        if len(message.command) < 3:
            await message.reply("❌ Usage: /addreply keyword new_reply")
            return
        keyword = message.command[1].lower().strip()
        new_reply = " ".join(message.command[2:])
        filter_doc = await filters_col.find_one({"keyword": keyword})
        if not filter_doc:
            await message.reply(f"❌ Filter `{keyword}` not found.")
            return
        current = filter_doc.get("replies", [filter_doc.get("reply_text", "")])
        if len(current) >= MAX_REPLIES_PER_FILTER:
            await message.reply(f"❌ Max replies per filter ({MAX_REPLIES_PER_FILTER}) reached.")
            return
        current.append(new_reply)
        await filters_col.update_one(
            {"keyword": keyword},
            {"$set": {"replies": current, "reply_text": " | ".join(current), "updated_at": datetime.now(timezone.utc)}}
        )
        await message.reply(f"✅ Reply added to `{keyword}`.")
        await log_action("reply_added", message.from_user.id, f"Keyword: {keyword}")
    except Exception as e:
        logger.error(f"Add reply error: {e}")
        await message.reply(f"❌ Error: {e}")

# --- Delete Reply ---
@app.on_message(filters.command("delreply") & filters.private)
async def delete_reply_command(client: Client, message: Message):
    try:
        if not await check_admin(client, message):
            return
        if len(message.command) < 3:
            await message.reply("❌ Usage: /delreply keyword reply_number")
            return
        keyword = message.command[1].lower().strip()
        try:
            reply_num = int(message.command[2]) - 1
        except ValueError:
            await message.reply("❌ Invalid reply number.")
            return
        filter_doc = await filters_col.find_one({"keyword": keyword})
        if not filter_doc:
            await message.reply(f"❌ Filter `{keyword}` not found.")
            return
        current = filter_doc.get("replies", [filter_doc.get("reply_text", "")])
        if reply_num < 0 or reply_num >= len(current):
            await message.reply(f"❌ Reply number {reply_num+1} not found.")
            return
        deleted = current.pop(reply_num)
        await filters_col.update_one(
            {"keyword": keyword},
            {"$set": {"replies": current, "reply_text": " | ".join(current) if current else "No replies", "updated_at": datetime.now(timezone.utc)}}
        )
        await message.reply(f"✅ Reply deleted from `{keyword}`.")
        await log_action("reply_deleted", message.from_user.id, f"Keyword: {keyword}")
    except Exception as e:
        logger.error(f"Delete reply error: {e}")
        await message.reply(f"❌ Error: {e}")

# --- Filter Group ---
@app.on_message(filters.command("filtergroup") & filters.private)
async def filter_group_command(client: Client, message: Message):
    try:
        if not await check_admin(client, message):
            return
        if len(message.command) < 3:
            await message.reply("❌ Usage: /filtergroup keyword group_name")
            return
        keyword = message.command[1].lower().strip()
        group_name = message.command[2].lower().strip()
        filter_doc = await filters_col.find_one({"keyword": keyword})
        if not filter_doc:
            await message.reply(f"❌ Filter `{keyword}` not found.")
            return
        await filters_col.update_one(
            {"keyword": keyword},
            {"$set": {"group": group_name, "updated_at": datetime.now(timezone.utc)}}
        )
        await message.reply(f"✅ Group updated for `{keyword}`.")
        await log_action("filter_group", message.from_user.id, f"Keyword: {keyword}")
    except Exception as e:
        logger.error(f"Filter group error: {e}")
        await message.reply(f"❌ Error: {e}")

# --- Export Filters ---
@app.on_message(filters.command("exportfilters") & filters.private)
async def export_filters_command(client: Client, message: Message):
    try:
        if not await check_admin(client, message):
            return
        status_msg = await message.reply("📦 Exporting filters...")
        filters_list = await filters_col.find().to_list(length=None)
        if not filters_list:
            await status_msg.edit_text("📭 No filters to export.")
            return
        export_data = []
        for f in filters_list:
            export_data.append({
                "keyword": f["keyword"],
                "replies": f.get("replies", [f.get("reply_text", "")]),
                "group": f.get("group", "general"),
                "media_type": f.get("media_type", "text"),
                "media_file_id": f.get("media_file_id"),
                "caption": f.get("caption", ""),
                "button_text": f.get("button_text"),
                "button_url": f.get("button_url"),
                "created_by": f.get("created_by", "unknown")
            })
        json_data = json.dumps(export_data, indent=2, ensure_ascii=False)
        filename = f"filters_export_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(json_data)
        await client.send_document(
            message.from_user.id,
            filename,
            caption=f"📦 Filters Export\n\n📊 Total Filters: {len(export_data)}"
        )
        os.remove(filename)
        await status_msg.delete()
        await log_action("filters_exported", message.from_user.id, f"Count: {len(export_data)}")
    except Exception as e:
        logger.error(f"Export filters error: {e}")
        await message.reply(f"❌ Error: {e}")

# --- Import Filters ---
@app.on_message(filters.command("importfilters") & filters.private)
async def import_filters_command(client: Client, message: Message):
    try:
        if not await check_admin(client, message):
            return
        await message.reply(
            "📤 Import Filters\n\nSend me the JSON file exported from this bot.\n\n⚠️ Existing filters with same keywords will be **overwritten**."
        )
    except Exception as e:
        logger.error(f"Import filters error: {e}")
        await message.reply(f"❌ Error: {e}")

@app.on_message(filters.document & filters.private)
async def import_filters_file_handler(client: Client, message: Message):
    try:
        if not message.document.file_name.endswith('.json'):
            return
        if not await check_admin(client, message):
            return
        status_msg = await message.reply("📥 Downloading file...")
        file_path = await message.download()
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                import_data = json.load(f)
        except Exception as e:
            await status_msg.edit_text(f"❌ Invalid JSON file: {e}")
            os.remove(file_path)
            return
        if not isinstance(import_data, list):
            await status_msg.edit_text("❌ Invalid format. Expected array of filters.")
            os.remove(file_path)
            return
        imported = 0
        for filter_data in import_data:
            try:
                keyword = filter_data.get("keyword", "").lower().strip()
                replies = filter_data.get("replies", [])
                if not keyword or not replies:
                    continue
                existing = await filters_col.find_one({"keyword": keyword})
                doc = {
                    "keyword": keyword,
                    "reply_text": " | ".join(replies),
                    "replies": replies,
                    "group": filter_data.get("group", "general"),
                    "media_type": filter_data.get("media_type", "text"),
                    "media_file_id": filter_data.get("media_file_id"),
                    "caption": filter_data.get("caption", ""),
                    "button_text": filter_data.get("button_text"),
                    "button_url": filter_data.get("button_url"),
                    "created_by": message.from_user.id,
                    "created_at": datetime.now(timezone.utc),
                    "updated_at": datetime.now(timezone.utc),
                    "usage_count": 0,
                    "is_active": True
                }
                if existing:
                    await filters_col.update_one({"keyword": keyword}, {"$set": doc})
                else:
                    await filters_col.insert_one(doc)
                imported += 1
            except Exception as e:
                logger.error(f"Import error: {e}")
        os.remove(file_path)
        await status_msg.edit_text(f"✅ Import Complete!\n\n✅ Imported: {imported}")
        await log_action("filters_imported", message.from_user.id, f"Imported: {imported}")
    except Exception as e:
        logger.error(f"Import filters file error: {e}")
        await message.reply(f"❌ Error: {e}")

# ============================================================
# GROUP FILTER AUTO-REPLY HANDLER (with Buttons)
# ============================================================

@app.on_message(filters.text & filters.group & ~filters.command(ALL_COMMANDS))
async def group_filter_reply_handler(client: Client, message: Message):
    try:
        if not message.text or len(message.text) < 2:
            return
        # Check if group filters are enabled
        setting = await settings_col.find_one({"_id": "group_filters"})
        if setting and setting.get("enabled", False) == False:
            return

        user_doc = await users_col.find_one({"user_id": message.from_user.id})
        if user_doc and user_doc.get("is_banned", False):
            return

        text = message.text.lower().strip()
        filters_list = await filters_col.find({"is_active": True}).to_list(length=None)

        for f in filters_list:
            keyword = f['keyword']
            pattern = r'\b' + re.escape(keyword) + r'\b'
            if re.search(pattern, text):
                reply = f.get("reply_text", "")
                if "|" in reply:
                    replies = reply.split("|")
                    reply = random.choice(replies)

                media_file_id = f.get("media_file_id")
                caption = f.get("caption", reply) if not reply else reply
                button_text = f.get("button_text")
                button_url = f.get("button_url")

                reply_markup = None
                if button_text and button_url:
                    reply_markup = InlineKeyboardMarkup([
                        [InlineKeyboardButton(button_text, url=button_url)]
                    ])

                if media_file_id and f.get("media_type"):
                    await client.send_cached_media(
                        chat_id=message.chat.id,
                        file_id=media_file_id,
                        caption=caption,
                        reply_markup=reply_markup
                    )
                else:
                    await message.reply(reply, reply_markup=reply_markup)

                # Update usage
                await filters_col.update_one(
                    {"keyword": keyword},
                    {"$inc": {"usage_count": 1}}
                )
                await filter_logs_col.insert_one({
                    "filter_id": f['_id'],
                    "keyword": keyword,
                    "user_id": message.from_user.id,
                    "username": message.from_user.username,
                    "message_text": message.text[:500],
                    "chat_id": message.chat.id,
                    "chat_type": "group",
                    "used_at": datetime.now(timezone.utc)
                })
                break
    except Exception as e:
        logger.error(f"Group filter reply error: {e}")

# ============================================================
# PRIVATE FILTER AUTO-REPLY HANDLER (with Buttons)
# ============================================================

@app.on_message(filters.text & filters.private & ~filters.command(ALL_COMMANDS))
async def filter_reply_handler(client: Client, message: Message):
    try:
        if not message.text or len(message.text) < 2:
            return
        user_doc = await users_col.find_one({"user_id": message.from_user.id})
        if user_doc and user_doc.get("is_banned", False):
            return
        text = message.text.lower().strip()
        filters_list = await filters_col.find({"is_active": True}).to_list(length=None)
        for f in filters_list:
            keyword = f['keyword']
            pattern = r'\b' + re.escape(keyword) + r'\b'
            if re.search(pattern, text):
                reply = f.get("reply_text", "")
                if "|" in reply:
                    replies = reply.split("|")
                    reply = random.choice(replies)

                media_file_id = f.get("media_file_id")
                caption = f.get("caption", reply) if not reply else reply
                button_text = f.get("button_text")
                button_url = f.get("button_url")

                reply_markup = None
                if button_text and button_url:
                    reply_markup = InlineKeyboardMarkup([
                        [InlineKeyboardButton(button_text, url=button_url)]
                    ])

                if media_file_id and f.get("media_type"):
                    await client.send_cached_media(
                        chat_id=message.chat.id,
                        file_id=media_file_id,
                        caption=caption,
                        reply_markup=reply_markup
                    )
                else:
                    await message.reply(reply, reply_markup=reply_markup)

                await filters_col.update_one(
                    {"keyword": keyword},
                    {"$inc": {"usage_count": 1}}
                )
                await filter_logs_col.insert_one({
                    "filter_id": f['_id'],
                    "keyword": keyword,
                    "user_id": message.from_user.id,
                    "username": message.from_user.username,
                    "message_text": message.text[:500],
                    "chat_type": "private",
                    "used_at": datetime.now(timezone.utc)
                })
                break
    except Exception as e:
        logger.error(f"Filter reply error: {e}")

# ============================================================
# OTHER COMMANDS (Verify, Unverify, Channels, List, Broadcast, Ads, Ban, Unban, Userinfo, Force Join, Stats, Backup, Restore, Maintenance, Logs, Admin Management, Search, Help, etc.)
# ============================================================

# These are exactly the same as in the previous full code.
# I'll include them in the final answer but will keep them concise.

# --- Verify ---
@app.on_message(filters.command("verify") & filters.private)
async def verify_channel(client: Client, message: Message):
    try:
        if not await is_admin(message.from_user.id):
            await message.reply("❌ Only admins can use this.")
            return
        if len(message.command) < 2:
            await message.reply("❌ Usage: /verify channel_id")
            return
        channel_id = int(message.command[1])
        existing = await channels_col.find_one({"channel_id": channel_id})
        if existing and existing.get("status") == "active":
            await message.reply("⚠️ Channel already verified!")
            return
        chat = await client.get_chat(channel_id)
        bot_member = await client.get_chat_member(channel_id, "me")
        if bot_member.status not in [enums.ChatMemberStatus.ADMINISTRATOR, enums.ChatMemberStatus.OWNER]:
            await message.reply("❌ Bot must be admin in the channel.")
            return
        is_private = chat.username is None
        bio = chat.description or ""
        await channels_col.update_one(
            {"channel_id": channel_id},
            {"$set": {
                "channel_id": channel_id,
                "title": chat.title,
                "username": chat.username,
                "bio": bio,
                "is_private": is_private,
                "status": "active",
                "verified_by": message.from_user.id,
                "verified_at": datetime.now(timezone.utc)
            }},
            upsert=True
        )
        clear_all_cache()
        await message.reply(f"✅ Channel {chat.title} verified!")
        await log_action("channel_verified", message.from_user.id, f"{chat.title} ({channel_id})")
    except Exception as e:
        logger.error(f"Verify error: {e}")
        await message.reply(f"❌ Error: {e}")

# --- Unverify ---
@app.on_message(filters.command("unverify") & filters.private)
async def unverify_channel(client: Client, message: Message):
    try:
        if not await is_admin(message.from_user.id):
            await message.reply("❌ Only admins can use this.")
            return
        if len(message.command) < 2:
            await message.reply("❌ Usage: /unverify channel_id")
            return
        channel_id = int(message.command[1])
        channel = await channels_col.find_one({"channel_id": channel_id})
        if not channel:
            await message.reply("❌ Channel not found.")
            return
        await channels_col.delete_one({"channel_id": channel_id})
        clear_all_cache()
        await message.reply(f"✅ Channel {channel.get('title')} removed.")
        await log_action("channel_removed", message.from_user.id, f"{channel_id}")
    except Exception as e:
        logger.error(f"Unverify error: {e}")
        await message.reply(f"❌ Error: {e}")

# --- Channels List ---
@app.on_message(filters.command("channels") & filters.private)
async def list_channels_cmd(client: Client, message: Message):
    try:
        if not await is_admin(message.from_user.id):
            await message.reply("❌ Only admins can use this.")
            return
        channels = await channels_col.find({"status": "active"}).to_list(length=None)
        if not channels:
            await message.reply("No verified channels.")
            return
        text = "📋 Verified Channels:\n\n"
        for i, ch in enumerate(channels, 1):
            privacy = "🔒" if ch.get("is_private") else "🌐"
            text += f"{i}. {privacy} {ch.get('title', 'Unknown')}\n   🆔 `{ch['channel_id']}`\n\n"
        text += f"Total: {len(channels)}"
        for part in split_message(text):
            await message.reply(part)
    except Exception as e:
        logger.error(f"Channels error: {e}")
        await message.reply(f"❌ Error: {e}")

# --- Detailed List ---
@app.on_message(filters.command("list") & filters.private)
async def list_detailed_cmd(client: Client, message: Message):
    try:
        if not await is_admin(message.from_user.id):
            await message.reply("❌ Only admins can use this.")
            return
        channels = await channels_col.find({"status": "active"}).to_list(length=None)
        if not channels:
            await message.reply("No verified channels.")
            return
        total = len(channels)
        total_pages = (total + LIST_PAGE_SIZE - 1) // LIST_PAGE_SIZE
        for page in range(total_pages):
            start = page * LIST_PAGE_SIZE
            end = min(start + LIST_PAGE_SIZE, total)
            page_channels = channels[start:end]
            text = f"📋 Verified Channels (Page {page+1}/{total_pages})\n\n"
            for i, ch in enumerate(page_channels, start+1):
                privacy = "🔒 Private" if ch.get("is_private") else "🌐 Public"
                bio = ch.get("bio", "") or "No bio"
                bio_preview = bio[:80] + "..." if len(bio) > 80 else bio
                verified_at = ch.get("verified_at", "N/A")
                if isinstance(verified_at, datetime):
                    verified_at = verified_at.strftime("%d %b %Y")
                text += f"━━━ {i} ━━━━━━━━━━━━━━━━━━\n📢 {ch.get('title', 'Unknown')}\n📝 {bio_preview}\n🔗 {'@'+ch['username'] if ch.get('username') else 'Private'}\n🆔 `{ch['channel_id']}`\n📅 Verified: {verified_at}\n\n"
            text += f"Total: {total} channels | Page {page+1}/{total_pages}"
            for part in split_message(text):
                await message.reply(part)
    except Exception as e:
        logger.error(f"List error: {e}")
        await message.reply(f"❌ Error: {e}")

# --- Broadcast ---
@app.on_message(filters.command("broadcast") & filters.private)
async def broadcast_command(client: Client, message: Message):
    try:
        if not await is_admin(message.from_user.id):
            await message.reply("❌ Only admins can use this.")
            return
        if not message.reply_to_message:
            await message.reply("❌ Reply to a message to broadcast.")
            return
        reply_msg = message.reply_to_message
        total_users = await users_col.count_documents({})
        if total_users == 0:
            await message.reply("❌ No users in database.")
            return
        preview = reply_msg.text[:200] if reply_msg.text else (reply_msg.caption[:200] if reply_msg.caption else f"[{reply_msg.media or 'Media'}]")
        await message.reply(
            f"📤 Broadcast Confirmation\n\nPreview:\n{preview}\n\nTotal Users: {total_users}\n\nProceed?",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Yes", callback_data=f"bc_yes_{reply_msg.id}"),
                 InlineKeyboardButton("❌ Cancel", callback_data="bc_cancel")]
            ])
        )
        broadcast_state[f"bc_{reply_msg.id}"] = {
            "reply_msg_id": reply_msg.id,
            "chat_id": message.chat.id,
            "admin_id": message.from_user.id
        }
    except Exception as e:
        logger.error(f"Broadcast error: {e}")
        await message.reply(f"❌ Error: {e}")

@app.on_callback_query(filters.regex("^bc_yes_"))
async def broadcast_confirm(client: Client, callback: CallbackQuery):
    try:
        if not await is_admin(callback.from_user.id):
            await callback.answer("❌ Access denied!", show_alert=True)
            return
        reply_msg_id = int(callback.data.split("_")[2])
        state = broadcast_state.get(f"bc_{reply_msg_id}")
        if not state:
            await callback.answer("❌ Session expired!", show_alert=True)
            return
        broadcast_msg = await client.get_messages(state["chat_id"], reply_msg_id)
        start_time = time.time()
        total_users = await users_col.count_documents({})
        success = failed = blocked = 0
        progress_msg = await callback.message.edit_text(
            f"📤 Broadcasting...\n\nProgress: 0/{total_users}\n✅ Sent: 0\n⏱️ Time: 0s\n❌ Failed: 0\n🚫 Blocked: 0"
        )
        processed = 0
        async for user_doc in users_col.find({}, {"user_id": 1}):
            uid = user_doc["user_id"]
            processed += 1
            try:
                await broadcast_msg.copy(uid)
                success += 1
            except (UserIsBlocked, InputUserDeactivated):
                blocked += 1
            except FloodWait as fw:
                await asyncio.sleep(fw.value)
                try:
                    await broadcast_msg.copy(uid)
                    success += 1
                except Exception:
                    failed += 1
            except Exception:
                failed += 1
            if processed % BROADCAST_SPEED == 0:
                elapsed = format_time_delta(time.time() - start_time)
                try:
                    await progress_msg.edit_text(
                        f"📤 Broadcasting...\n\nProgress: {processed}/{total_users}\n✅ Sent: {success}\n⏱️ Time: {elapsed}\n❌ Failed: {failed}\n🚫 Blocked: {blocked}"
                    )
                except Exception:
                    pass
            await asyncio.sleep(0.04)
        elapsed = format_time_delta(time.time() - start_time)
        await progress_msg.edit_text(
            f"✅ Broadcast Complete!\n\nTotal: {total_users}\n✅ Sent: {success}\n⏱️ Time: {elapsed}\n❌ Failed: {failed}\n🚫 Blocked: {blocked}"
        )
        broadcast_state.pop(f"bc_{reply_msg_id}", None)
        await log_action("broadcast", callback.from_user.id, f"Sent:{success} Failed:{failed} Blocked:{blocked}")
    except Exception as e:
        logger.error(f"Broadcast confirm error: {e}")
        await callback.message.edit_text(f"❌ Broadcast Error: {e}")

@app.on_callback_query(filters.regex("^bc_cancel$"))
async def broadcast_cancel(client: Client, callback: CallbackQuery):
    try:
        if not await is_admin(callback.from_user.id):
            await callback.answer("❌ Access denied!", show_alert=True)
            return
        await callback.message.edit_text("❌ Broadcast cancelled.")
    except Exception as e:
        logger.error(f"BC cancel error: {e}")

# --- Ads ---
@app.on_message(filters.command("addad") & filters.private)
async def add_advertisement(client: Client, message: Message):
    try:
        if not await is_admin(message.from_user.id):
            await message.reply("❌ Only admins can use this.")
            return
        args = message.text.split(None, 4)
        if len(args) < 5:
            await message.reply("❌ Usage: /addad slot type url button_text")
            return
        slot = int(args[1])
        if slot < 1 or slot > 6:
            await message.reply("❌ Slot must be 1-6.")
            return
        link_type = args[2].lower()
        if link_type not in ["request", "normal", "external"]:
            await message.reply("❌ Type must be request, normal, or external.")
            return
        url = args[3]
        button_text = args[4]
        await ads_col.update_one(
            {"slot": slot},
            {"$set": {"slot": slot, "type": link_type, "url": url, "button_text": button_text, "active": True, "clicks": 0, "impressions": 0, "created_at": datetime.now(timezone.utc), "created_by": message.from_user.id}},
            upsert=True
        )
        await message.reply(f"✅ Ad added to slot {slot}.")
        await log_action("ad_added", message.from_user.id, f"Slot {slot}")
    except Exception as e:
        logger.error(f"Add ad error: {e}")
        await message.reply(f"❌ Error: {e}")

@app.on_message(filters.command("removead") & filters.private)
async def remove_advertisement(client: Client, message: Message):
    try:
        if not await is_admin(message.from_user.id):
            await message.reply("❌ Only admins can use this.")
            return
        if len(message.command) < 2:
            await message.reply("❌ Usage: /removead slot")
            return
        slot = int(message.command[1])
        result = await ads_col.delete_one({"slot": slot})
        if result.deleted_count > 0:
            await message.reply(f"✅ Ad removed from slot {slot}.")
        else:
            await message.reply(f"❌ No ad found in slot {slot}.")
        await log_action("ad_removed", message.from_user.id, f"Slot {slot}")
    except Exception as e:
        logger.error(f"Remove ad error: {e}")
        await message.reply(f"❌ Error: {e}")

@app.on_message(filters.command("listads") & filters.private)
async def list_ads(client: Client, message: Message):
    try:
        if not await is_admin(message.from_user.id):
            await message.reply("❌ Only admins can use this.")
            return
        ads = await ads_col.find().sort("slot", 1).to_list(length=None)
        if not ads:
            await message.reply("No ads found.")
            return
        text = "📢 Advertisements:\n\n"
        for ad in ads:
            text += f"Slot {ad['slot']}: {ad.get('button_text')} - {ad.get('url')}\n   Impressions: {ad.get('impressions', 0)}\n\n"
        await message.reply(text)
        await log_action("list_ads", message.from_user.id)
    except Exception as e:
        logger.error(f"List ads error: {e}")
        await message.reply(f"❌ Error: {e}")

@app.on_message(filters.command("adstats") & filters.private)
async def ad_stats(client: Client, message: Message):
    try:
        if not await is_admin(message.from_user.id):
            await message.reply("❌ Only admins can use this.")
            return
        total = 0
        text = "📊 Ad Statistics:\n\n"
        async for ad in ads_col.find().sort("slot", 1):
            imp = ad.get("impressions", 0)
            total += imp
            text += f"Slot {ad['slot']}: {ad.get('button_text')} - 👁️ {imp}\n"
        text += f"\nTotal Impressions: {total}"
        await message.reply(text)
        await log_action("ad_stats", message.from_user.id)
    except Exception as e:
        logger.error(f"Ad stats error: {e}")
        await message.reply(f"❌ Error: {e}")

# --- Ban/Unban/Userinfo ---
@app.on_message(filters.command("ban") & filters.private)
async def ban_user(client: Client, message: Message):
    try:
        if not await is_admin(message.from_user.id):
            await message.reply("❌ Only admins can use this.")
            return
        if len(message.command) < 2:
            await message.reply("❌ Usage: /ban user_id")
            return
        target_id = int(message.command[1])
        if target_id == OWNER_ID:
            await message.reply("❌ Cannot ban owner.")
            return
        await users_col.update_one({"user_id": target_id}, {"$set": {"is_banned": True}}, upsert=True)
        await message.reply(f"✅ User {target_id} banned.")
        await log_action("user_banned", message.from_user.id, f"Target: {target_id}")
    except Exception as e:
        logger.error(f"Ban error: {e}")
        await message.reply(f"❌ Error: {e}")

@app.on_message(filters.command("unban") & filters.private)
async def unban_user(client: Client, message: Message):
    try:
        if not await is_admin(message.from_user.id):
            await message.reply("❌ Only admins can use this.")
            return
        if len(message.command) < 2:
            await message.reply("❌ Usage: /unban user_id")
            return
        target_id = int(message.command[1])
        await users_col.update_one({"user_id": target_id}, {"$set": {"is_banned": False}})
        await message.reply(f"✅ User {target_id} unbanned.")
        await log_action("user_unbanned", message.from_user.id, f"Target: {target_id}")
    except Exception as e:
        logger.error(f"Unban error: {e}")
        await message.reply(f"❌ Error: {e}")

@app.on_message(filters.command("userinfo") & filters.private)
async def user_info(client: Client, message: Message):
    try:
        if not await is_admin(message.from_user.id):
            await message.reply("❌ Only admins can use this.")
            return
        if len(message.command) < 2:
            await message.reply("❌ Usage: /userinfo user_id")
            return
        target_id = int(message.command[1])
        user_doc = await users_col.find_one({"user_id": target_id})
        if not user_doc:
            await message.reply("❌ User not found.")
            return
        ban_status = "🚫 Banned" if user_doc.get("is_banned") else "✅ Active"
        await message.reply(
            f"👤 User Info:\n\n🆔 {target_id}\n👤 {user_doc.get('first_name', 'N/A')}\n📛 @{user_doc.get('username', 'N/A')}\n📅 Joined: {user_doc.get('joined_at', 'N/A')}\n🔍 Searches: {user_doc.get('searches_count', 0)}\n📊 Status: {ban_status}"
        )
        await log_action("userinfo", message.from_user.id, f"Target: {target_id}")
    except Exception as e:
        logger.error(f"User info error: {e}")
        await message.reply(f"❌ Error: {e}")

# --- Force Join ---
@app.on_message(filters.command("addfj") & filters.private)
async def add_force_join(client: Client, message: Message):
    try:
        if not await is_admin(message.from_user.id):
            await message.reply("❌ Only admins can use this.")
            return
        if len(message.command) < 2:
            await message.reply("❌ Usage: /addfj channel_id")
            return
        channel_id = int(message.command[1])
        chat = await client.get_chat(channel_id)
        is_private = chat.username is None
        if not is_private:
            invite_url = f"https://t.me/{chat.username}"
            channel_data = {
                "channel_id": channel_id,
                "title": chat.title,
                "username": chat.username,
                "invite_url": invite_url,
                "type": "join",
                "is_private": False
            }
            await settings_col.update_one(
                {"_id": "force_join"},
                {"$addToSet": {"channels": channel_data}},
                upsert=True
            )
            await message.reply(f"✅ Force join added for {chat.title}")
        else:
            fj_pending[f"fj_{channel_id}"] = {"channel_id": channel_id, "title": chat.title, "username": chat.username}
            await message.reply(
                "🔒 Private channel detected.\nChoose type:",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔐 Request", callback_data=f"fj_request_{channel_id}"),
                     [InlineKeyboardButton("📥 Join", callback_data=f"fj_join_{channel_id}")]]
                ])
            )
        await log_action("add_fj", message.from_user.id, f"Channel: {channel_id}")
    except Exception as e:
        logger.error(f"Add FJ error: {e}")
        await message.reply(f"❌ Error: {e}")

@app.on_callback_query(filters.regex(r"^fj_(request|join)_"))
async def fj_type_callback(client: Client, callback: CallbackQuery):
    try:
        if not await is_admin(callback.from_user.id):
            await callback.answer("❌ Access denied!", show_alert=True)
            return
        parts = callback.data.split("_", 2)
        fj_type = parts[1]
        channel_id = int(parts[2])
        pending = fj_pending.get(f"fj_{channel_id}")
        if not pending:
            await callback.answer("❌ Expired! Use /addfj again.", show_alert=True)
            return
        invite = await client.create_chat_invite_link(
            chat_id=channel_id,
            creates_join_request=(fj_type == "request"),
            name=f"force_{fj_type}_{channel_id}"
        )
        channel_data = {
            "channel_id": channel_id,
            "title": pending["title"],
            "username": pending["username"],
            "invite_url": invite.invite_link,
            "type": fj_type,
            "is_private": True
        }
        await settings_col.update_one(
            {"_id": "force_join"},
            {"$addToSet": {"channels": channel_data}},
            upsert=True
        )
        fj_pending.pop(f"fj_{channel_id}", None)
        type_text = "Request" if fj_type == "request" else "Direct Join"
        await callback.message.edit_text(f"✅ Force join added with {type_text} for {pending['title']}")
        await log_action("fj_added", callback.from_user.id, f"Channel: {channel_id}")
    except Exception as e:
        logger.error(f"FJ callback error: {e}")
        await callback.message.edit_text(f"❌ Error: {e}")

@app.on_message(filters.command("removefj") & filters.private)
async def remove_force_join(client: Client, message: Message):
    try:
        if not await is_admin(message.from_user.id):
            await message.reply("❌ Only admins can use this.")
            return
        if len(message.command) < 2:
            await message.reply("❌ Usage: /removefj channel_id")
            return
        channel_id = int(message.command[1])
        await settings_col.update_one(
            {"_id": "force_join"},
            {"$pull": {"channels": {"channel_id": channel_id}}}
        )
        await message.reply(f"✅ Force join removed for channel {channel_id}.")
        await log_action("remove_fj", message.from_user.id, f"Channel: {channel_id}")
    except Exception as e:
        logger.error(f"Remove FJ error: {e}")
        await message.reply(f"❌ Error: {e}")

@app.on_message(filters.command("listfj") & filters.private)
async def list_force_join(client: Client, message: Message):
    try:
        if not await is_admin(message.from_user.id):
            await message.reply("❌ Only admins can use this.")
            return
        channels = await get_force_join_channels()
        if not channels:
            await message.reply("No force join channels.")
            return
        text = "📋 Force Join Channels:\n\n"
        for i, ch in enumerate(channels, 1):
            type_text = "Request" if ch.get("type") == "request" else "Join"
            privacy = "🔒 Private" if ch.get("is_private") else "🌐 Public"
            text += f"{i}. {ch.get('title')} | {privacy} | {type_text}\n   🆔 `{ch['channel_id']}`\n\n"
        await message.reply(text)
        await log_action("list_fj", message.from_user.id)
    except Exception as e:
        logger.error(f"List FJ error: {e}")
        await message.reply(f"❌ Error: {e}")

# --- Stats ---
@app.on_message(filters.command("stats") & filters.private)
async def bot_stats(client: Client, message: Message):
    try:
        if not await is_admin(message.from_user.id):
            await message.reply("❌ Only admins can use this.")
            return
        total_users = await users_col.count_documents({})
        banned = await users_col.count_documents({"is_banned": True})
        channels = await channels_col.count_documents({"status": "active"})
        admins = await admins_col.count_documents({})
        active_ads = await ads_col.count_documents({"active": True})
        today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        today_users = await users_col.count_documents({"joined_at": {"$gte": today}})
        today_searches = await logs_col.count_documents({"action": "search", "timestamp": {"$gte": today}})
        await message.reply(
            f"📊 Bot Statistics:\n\n👥 Users: {total_users}\n🚫 Banned: {banned}\n📢 Channels: {channels}\n🛡️ Admins: {admins+1}\n📢 Ads: {active_ads}\n\n📅 Today:\n👤 New: {today_users}\n🔍 Searches: {today_searches}"
        )
        await log_action("stats", message.from_user.id)
    except Exception as e:
        logger.error(f"Stats error: {e}")
        await message.reply(f"❌ Error: {e}")

@app.on_message(filters.command("channelstats") & filters.private)
async def channel_stats(client: Client, message: Message):
    try:
        if not await is_admin(message.from_user.id):
            await message.reply("❌ Only admins can use this.")
            return
        if len(message.command) < 2:
            await message.reply("❌ Usage: /channelstats channel_id")
            return
        channel_id = int(message.command[1])
        ch = await channels_col.find_one({"channel_id": channel_id})
        if not ch:
            await message.reply("❌ Channel not found.")
            return
        links = await invite_links_col.count_documents({"channel_id": channel_id})
        privacy = "🔒 Private" if ch.get("is_private") else "🌐 Public"
        await message.reply(
            f"📊 Channel Stats:\n\n📢 {ch.get('title')}\n🆔 {channel_id}\n🔐 {privacy}\n🔗 Links Generated: {links}\n📅 Verified: {ch.get('verified_at', 'N/A')}"
        )
        await log_action("channelstats", message.from_user.id, f"Channel: {channel_id}")
    except Exception as e:
        logger.error(f"Channel stats error: {e}")
        await message.reply(f"❌ Error: {e}")

@app.on_message(filters.command("searchstats") & filters.private)
async def search_stats(client: Client, message: Message):
    try:
        if not await is_admin(message.from_user.id):
            await message.reply("❌ Only admins can use this.")
            return
        total = await logs_col.count_documents({"action": "search"})
        today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        today_s = await logs_col.count_documents({"action": "search", "timestamp": {"$gte": today}})
        week = datetime.now(timezone.utc) - timedelta(days=7)
        week_s = await logs_col.count_documents({"action": "search", "timestamp": {"$gte": week}})
        await message.reply(
            f"📊 Search Statistics:\n\n🔍 Total: {total}\n📅 Today: {today_s}\n📆 This Week: {week_s}"
        )
        await log_action("searchstats", message.from_user.id)
    except Exception as e:
        logger.error(f"Search stats error: {e}")
        await message.reply(f"❌ Error: {e}")

# --- Backup ---
async def create_backup_data():
    backup = {}
    for col_name, col in [("users", users_col), ("channels", channels_col), ("advertisements", ads_col), ("settings", settings_col), ("admins", admins_col)]:
        docs = []
        async for doc in col.find():
            doc["_id"] = str(doc["_id"])
            for key, val in doc.items():
                if isinstance(val, datetime):
                    doc[key] = val.isoformat()
            docs.append(doc)
        backup[col_name] = docs
    return backup

async def create_backup_zip(backup_data):
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
        for name, data in backup_data.items():
            zf.writestr(f"{name}.json", json.dumps(data, indent=2, ensure_ascii=False))
    zip_buffer.seek(0)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return zip_buffer, f"backup_{timestamp}.zip"

@app.on_message(filters.command("backup") & filters.private)
async def backup_command(client: Client, message: Message):
    try:
        if not await is_admin(message.from_user.id):
            await message.reply("❌ Only admins can use this.")
            return
        status_msg = await message.reply("📦 Creating backup...")
        start_time = time.time()
        backup_data = await create_backup_data()
        zip_buffer, filename = await create_backup_zip(backup_data)
        elapsed = format_time_delta(time.time() - start_time)
        caption = f"📦 Backup\n\n📅 {datetime.now(timezone.utc).strftime('%d %b %Y %H:%M UTC')}\n👥 Users: {len(backup_data.get('users', []))}\n📢 Channels: {len(backup_data.get('channels', []))}\n⏱️ Time: {elapsed}"
        try:
            sent = await client.send_document(BACKUP_CHANNEL_ID, zip_buffer, file_name=filename, caption=caption)
            await backup_records_col.insert_one({
                "message_id": sent.id,
                "channel_id": BACKUP_CHANNEL_ID,
                "filename": filename,
                "created_at": datetime.now(timezone.utc),
                "type": "manual"
            })
        except Exception as e:
            logger.warning(f"Backup channel send failed: {e}")
        zip_buffer.seek(0)
        await client.send_document(message.from_user.id, zip_buffer, file_name=filename, caption=caption)
        await status_msg.delete()
        await log_action("backup", message.from_user.id, filename)
    except Exception as e:
        logger.error(f"Backup error: {e}")
        await message.reply(f"❌ Error: {e}")

# --- Restore ---
@app.on_message(filters.command("restore") & filters.private)
async def restore_command(client: Client, message: Message):
    try:
        if not await is_admin(message.from_user.id):
            await message.reply("❌ Only admins can use this.")
            return
        restore_state[message.from_user.id] = {"step": "waiting_file"}
        await message.reply("📦 Send the backup ZIP file.\n\nCancel: /start")
    except Exception as e:
        logger.error(f"Restore error: {e}")
        await message.reply(f"❌ Error: {e}")

@app.on_message(filters.document & filters.private, group=5)
async def handle_restore_document(client: Client, message: Message):
    try:
        user_id = message.from_user.id
        if user_id not in restore_state:
            return
        if restore_state[user_id].get("step") != "waiting_file":
            return
        if not message.document.file_name.endswith(".zip"):
            await message.reply("❌ Only ZIP files are allowed.")
            return
        status_msg = await message.reply("📥 Downloading...")
        file_path = await message.download()
        try:
            with zipfile.ZipFile(file_path, 'r') as zf:
                file_list = zf.namelist()
                backup_data = {}
                for fname in file_list:
                    if fname.endswith(".json"):
                        col_name = fname.replace(".json", "")
                        data = json.loads(zf.read(fname))
                        backup_data[col_name] = data
        except Exception as e:
            await status_msg.edit_text(f"❌ Invalid ZIP file: {e}")
            restore_state.pop(user_id, None)
            try: os.remove(file_path)
            except: pass
            return
        preview = "📦 Backup File Info:\n\n"
        for col_name, data in backup_data.items():
            preview += f"📁 {col_name}: {len(data)} records\n"
        preview += "\n⚠️ WARNING: Current data will be replaced!\n\nProceed?"
        restore_state[user_id] = {"step": "waiting_confirm", "file_path": file_path, "backup_data": backup_data}
        await status_msg.edit_text(
            preview,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Yes", callback_data="restore_yes"),
                 [InlineKeyboardButton("❌ Cancel", callback_data="restore_cancel")]]
            ])
        )
    except Exception as e:
        logger.error(f"Restore doc error: {e}")
        restore_state.pop(message.from_user.id, None)

@app.on_callback_query(filters.regex("^restore_yes$"))
async def restore_confirm(client: Client, callback: CallbackQuery):
    try:
        user_id = callback.from_user.id
        if user_id not in restore_state:
            await callback.answer("❌ Session expired!", show_alert=True)
            return
        state = restore_state[user_id]
        if state.get("step") != "waiting_confirm":
            await callback.answer("❌ Invalid state!", show_alert=True)
            return
        backup_data = state["backup_data"]
        file_path = state.get("file_path")
        await callback.message.edit_text("🔄 Restoring...")
        col_map = {
            "users": users_col,
            "channels": channels_col,
            "advertisements": ads_col,
            "settings": settings_col,
            "admins": admins_col
        }
        restore_report = "📊 Restore Report:\n\n"
        for col_name, data in backup_data.items():
            if col_name in col_map:
                col = col_map[col_name]
                await col.delete_many({})
                if data:
                    for doc in data:
                        if "_id" in doc and col_name != "settings":
                            del doc["_id"]
                        for key, val in doc.items():
                            if isinstance(val, str):
                                try:
                                    doc[key] = datetime.fromisoformat(val)
                                except:
                                    pass
                    try:
                        await col.insert_many(data)
                    except Exception as e:
                        for doc in data:
                            try:
                                await col.insert_one(doc)
                            except:
                                pass
                restore_report += f"✅ {col_name}: {len(data)} restored\n"
        clear_all_cache()
        restore_state.pop(user_id, None)
        try:
            if file_path:
                os.remove(file_path)
        except:
            pass
        await callback.message.edit_text("✅ Restore Complete!")
        await log_action("restore", user_id, "Database restored")
    except Exception as e:
        logger.error(f"Restore confirm error: {e}")
        await callback.message.edit_text(f"❌ Restore Error: {e}")
        restore_state.pop(callback.from_user.id, None)

@app.on_callback_query(filters.regex("^restore_cancel$"))
async def restore_cancel(client: Client, callback: CallbackQuery):
    try:
        user_id = callback.from_user.id
        state = restore_state.pop(user_id, None)
        if state and state.get("file_path"):
            try:
                os.remove(state["file_path"])
            except:
                pass
        await callback.message.edit_text("❌ Restore cancelled.")
    except Exception as e:
        logger.error(f"Restore cancel error: {e}")

# --- Maintenance ---
@app.on_message(filters.command("maintenance") & filters.private)
async def maintenance_mode(client: Client, message: Message):
    try:
        if not await is_admin(message.from_user.id):
            await message.reply("❌ Only admins can use this.")
            return
        if len(message.command) < 2:
            await message.reply("❌ Usage: /maintenance on/off")
            return
        mode = message.command[1].lower()
        if mode == "on":
            await settings_col.update_one({"_id": "maintenance"}, {"$set": {"enabled": True}}, upsert=True)
            await message.reply("🔧 Maintenance mode ON")
        elif mode == "off":
            await settings_col.update_one({"_id": "maintenance"}, {"$set": {"enabled": False}}, upsert=True)
            await message.reply("✅ Maintenance mode OFF")
        else:
            await message.reply("❌ Use 'on' or 'off'.")
        await log_action("maintenance", message.from_user.id, f"Mode: {mode}")
    except Exception as e:
        logger.error(f"Maintenance error: {e}")
        await message.reply(f"❌ Error: {e}")

# --- Logs ---
@app.on_message(filters.command("logs") & filters.private)
async def view_logs(client: Client, message: Message):
    try:
        if not await is_admin(message.from_user.id):
            await message.reply("❌ Only admins can use this.")
            return
        logs = await logs_col.find().sort("timestamp", -1).limit(20).to_list(length=20)
        if not logs:
            await message.reply("No logs found.")
            return
        text = "📋 Recent Logs (Last 20):\n\n"
        for log in logs:
            ts = log.get("timestamp", "N/A")
            if isinstance(ts, datetime):
                ts = ts.strftime("%d/%m %H:%M")
            text += f"⏱️ {ts}\n📌 {log.get('action', 'N/A')}\n👤 {log.get('user_id', 'N/A')}\n📝 {log.get('details', '')}\n{'─'*25}\n"
        for part in split_message(text):
            await message.reply(part)
        await log_action("logs", message.from_user.id)
    except Exception as e:
        logger.error(f"Logs error: {e}")
        await message.reply(f"❌ Error: {e}")

# --- Admin Management ---
@app.on_message(filters.command("addmin") & filters.private)
async def add_admin(client: Client, message: Message):
    try:
        if not await is_owner(message.from_user.id):
            await message.reply("❌ Only owner can use this.")
            return
        if len(message.command) < 2:
            await message.reply("❌ Usage: /addmin user_id or @username")
            return
        target = message.command[1]
        try:
            if target.startswith("@"):
                user_obj = await client.get_users(target)
            else:
                user_obj = await client.get_users(int(target))
            target_id = user_obj.id
            target_name = user_obj.first_name
            target_username = user_obj.username
        except Exception as e:
            await message.reply(f"❌ User not found: {e}")
            return
        if target_id == OWNER_ID:
            await message.reply("ℹ️ Owner already has full access.")
            return
        existing = await admins_col.find_one({"user_id": target_id})
        if existing:
            await message.reply("⚠️ User is already an admin.")
            return
        await admins_col.insert_one({
            "user_id": target_id,
            "name": target_name,
            "username": target_username,
            "added_by": message.from_user.id,
            "added_at": datetime.now(timezone.utc)
        })
        await message.reply(f"✅ Admin added: {target_name} (@{target_username})")
        await log_action("admin_added", message.from_user.id, f"Target: {target_id}")
    except Exception as e:
        logger.error(f"Add admin error: {e}")
        await message.reply(f"❌ Error: {e}")

@app.on_message(filters.command("radmin") & filters.private)
async def remove_admin(client: Client, message: Message):
    try:
        if not await is_owner(message.from_user.id):
            await message.reply("❌ Only owner can use this.")
            return
        if len(message.command) < 2:
            await message.reply("❌ Usage: /radmin user_id or @username")
            return
        target = message.command[1]
        try:
            if target.startswith("@"):
                user_obj = await client.get_users(target)
                target_id = user_obj.id
            else:
                target_id = int(target)
        except Exception as e:
            await message.reply(f"❌ Error: {e}")
            return
        result = await admins_col.delete_one({"user_id": target_id})
        if result.deleted_count > 0:
            await message.reply(f"✅ Admin {target_id} removed.")
        else:
            await message.reply(f"❌ User {target_id} is not an admin.")
        await log_action("admin_removed", message.from_user.id, f"Target: {target_id}")
    except Exception as e:
        logger.error(f"Remove admin error: {e}")
        await message.reply(f"❌ Error: {e}")

@app.on_message(filters.command("ladmin") & filters.private)
async def list_admins(client: Client, message: Message):
    try:
        if not await is_owner(message.from_user.id):
            await message.reply("❌ Only owner can use this.")
            return
        admins = await admins_col.find().to_list(length=None)
        text = f"👑 Admin List:\n\n1. 👑 Owner\n   🆔 {OWNER_ID}\n\n"
        for i, admin in enumerate(admins, 2):
            added_at = admin.get("added_at", "N/A")
            if isinstance(added_at, datetime):
                added_at = added_at.strftime("%d %b %Y %H:%M")
            text += f"{i}. 🛡️ {admin.get('name', 'Unknown')}\n   📛 @{admin.get('username', 'N/A')}\n   🆔 {admin['user_id']}\n   📅 Added: {added_at}\n\n"
        text += f"Total: {len(admins)+1} (including owner)"
        await message.reply(text)
        await log_action("list_admins", message.from_user.id)
    except Exception as e:
        logger.error(f"List admins error: {e}")
        await message.reply(f"❌ Error: {e}")

# --- Help ---
@app.on_message(filters.command("help") & (filters.private | filters.group))
async def help_command(client: Client, message: Message):
    try:
        user_id = message.from_user.id
        if await is_owner(user_id):
            text = "👑 Owner Commands:\n/addmin, /radmin, /ladmin\n\n🛡️ Admin Commands:\n/verify, /unverify, /channels, /list, /broadcast, /addad, /removead, /listads, /adstats, /ban, /unban, /userinfo, /addfj, /removefj, /listfj, /stats, /channelstats, /searchstats, /backup, /restore, /maintenance, /logs\n\nSearch:\n/search, /anime, /a, /s\n\nFilters:\n/addfilter, /editfilter, /delfilter, /listfilters, /filterstats, /addreply, /delreply, /filtergroup, /exportfilters, /importfilters\n\nAnti-Link:\n/antilink, /addwhitelist, /delwhitelist, /whitelist, /linkstats"
        elif await is_admin(user_id):
            text = "🛡️ Admin Commands:\n/verify, /unverify, /channels, /list, /broadcast, /addad, /removead, /listads, /adstats, /ban, /unban, /userinfo, /addfj, /removefj, /listfj, /stats, /channelstats, /searchstats, /backup, /restore, /maintenance, /logs\n\nSearch:\n/search, /anime, /a, /s\n\nFilters:\n/addfilter, /editfilter, /delfilter, /listfilters, /filterstats, /addreply, /delreply, /filtergroup, /exportfilters, /importfilters\n\nAnti-Link:\n/antilink, /addwhitelist, /delwhitelist, /whitelist, /linkstats"
        else:
            text = "🔍 Search Bot Help:\n\nDM: Just type any keyword.\n\nGroup:\n/search anime\n/anime Fairy Tail\n/a Dragon Ball\n/s One Piece\n\nFilters: /listfilters, /filterstats"
        await message.reply(text)
        await log_action("help", user_id)
    except Exception as e:
        logger.error(f"Help error: {e}")
        await message.reply(f"❌ Error: {e}")

# --- Search Commands (Bio & Name) ---
def fuzzy_match(search_query, target_text, threshold=FUZZY_THRESHOLD):
    if not search_query or not target_text:
        return False
    search_lower = search_query.lower().strip()
    target_lower = target_text.lower().strip()
    # Simple check: word boundary or prefix
    if search_lower in target_lower:
        return True
    search_words = search_lower.split()
    target_words = target_lower.split()
    for sw in search_words:
        for tw in target_words:
            if tw.startswith(sw):
                return True
    return False

@app.on_message(filters.text & filters.private & ~filters.command(ALL_COMMANDS))
async def search_handler(client: Client, message: Message):
    try:
        keyword = message.text.strip()
        if len(keyword) < 2:
            await message.reply("❌ Minimum 2 characters.")
            return
        await perform_search(client, message, keyword)
    except Exception as e:
        logger.error(f"Search handler error: {e}")

@app.on_message(filters.command(["search", "anime", "a", "s"]) & (filters.private | filters.group))
async def group_search_handler(client: Client, message: Message):
    try:
        if len(message.command) < 2:
            await message.reply("❌ Provide a keyword.\nExample: /search Naruto")
            return
        keyword = message.text.split(None, 1)[1].strip()
        if len(keyword) < 2:
            await message.reply("❌ Minimum 2 characters.")
            return
        await perform_search(client, message, keyword)
    except Exception as e:
        logger.error(f"Group search error: {e}")
        await message.reply("❌ Error.")

async def perform_search(client: Client, message: Message, keyword: str):
    try:
        user_id = message.from_user.id
        await save_user(user_id, message.from_user.username, message.from_user.first_name)
        # Check force join
        is_joined, not_joined = await check_force_join(client, user_id)
        if not is_joined:
            buttons = []
            for ch in not_joined:
                buttons.append([InlineKeyboardButton(f"📢 {ch.get('title')}", url=ch.get("invite_url"))])
            buttons.append([InlineKeyboardButton("✅ I Joined", callback_data="check_join")])
            await message.reply("⚠️ Please join required channels first.", reply_markup=InlineKeyboardMarkup(buttons))
            return
        user_doc = await users_col.find_one({"user_id": user_id})
        if user_doc and user_doc.get("is_banned", False):
            await message.reply("🚫 You are banned.")
            return
        if not check_rate_limit(user_id):
            await message.reply("⚠️ Rate limit exceeded. Try again later.")
            return
        searching_msg = await message.reply("🔍 Searching...")
        cached = get_cached_results(keyword)
        if cached is not None:
            matched_channels = cached
        else:
            all_channels = await channels_col.find({"status": "active"}).to_list(length=None)
            matched_channels = []
            for ch in all_channels:
                bio = ch.get("bio", "") or ""
                title = ch.get("title", "") or ""
                if fuzzy_match(keyword, bio) or fuzzy_match(keyword, title):
                    matched_channels.append(ch)
            set_cache(keyword, matched_channels)
        if not matched_channels:
            await searching_msg.edit_text(f"❌ No channels found for: {keyword}")
            return
        display_channels = matched_channels[:SEARCH_RESULT_LIMIT]
        response_text = f"🔍 Results for: {keyword}\n\n"
        buttons = []
        for ad in await get_active_ads():
            buttons.append([InlineKeyboardButton(ad.get("button_text"), url=ad.get("url"))])
            await ads_col.update_one({"_id": ad["_id"]}, {"$inc": {"impressions": 1}})
        success_channels = []
        for ch in display_channels:
            try:
                link = await get_or_create_invite_link(client, ch["channel_id"], ch.get("is_private", True))
                response_text += f"📌 {ch['title']}\n"
                buttons.append([InlineKeyboardButton(f"🔗 {ch['title']}", url=link)])
                success_channels.append(ch)
            except Exception as e:
                logger.error(f"Link fail for {ch.get('title')}: {e}")
                await notify_link_fail(client, ch["channel_id"], ch.get("title", "Unknown"), str(e))
                continue
        if not success_channels:
            await searching_msg.edit_text("⚠️ Link generation failed. Try again later.")
            return
        response_text += f"\nFound {len(success_channels)} channel(s)"
        await searching_msg.delete()
        sent_msg = await message.reply(response_text, reply_markup=InlineKeyboardMarkup(buttons))
        await search_results_col.insert_one({
            "user_id": user_id,
            "chat_id": message.chat.id,
            "message_id": sent_msg.id,
            "expires_at": datetime.now(timezone.utc) + timedelta(seconds=LINK_EXPIRY_SECONDS)
        })
        await users_col.update_one({"user_id": user_id}, {"$inc": {"searches_count": 1}})
        await log_action("search", user_id, f"Query: {keyword}, Found: {len(success_channels)}")
    except Exception as e:
        logger.error(f"Search error: {e}")
        await message.reply("❌ Error in search.")

# --- Invite Link Helpers (used in search) ---
async def get_or_create_invite_link(client, channel_id, is_private):
    now = datetime.now(timezone.utc)
    existing = await invite_links_col.find_one({
        "channel_id": channel_id,
        "status": "active",
        "reuse_until": {"$gt": now}
    })
    if existing:
        return existing["invite_link"]
    if is_private:
        invite = await client.create_chat_invite_link(
            chat_id=channel_id,
            creates_join_request=True,
            expire_date=now + timedelta(seconds=LINK_EXPIRY_SECONDS),
            name=f"search_{int(time.time())}"
        )
    else:
        invite = await client.create_chat_invite_link(
            chat_id=channel_id,
            expire_date=now + timedelta(seconds=LINK_EXPIRY_SECONDS),
            name=f"search_{int(time.time())}"
        )
    await invite_links_col.insert_one({
        "channel_id": channel_id,
        "invite_link": invite.invite_link,
        "is_private": is_private,
        "created_at": now,
        "expires_at": now + timedelta(seconds=LINK_EXPIRY_SECONDS),
        "reuse_until": now + timedelta(seconds=LINK_REUSE_WINDOW),
        "status": "active"
    })
    return invite.invite_link

async def notify_link_fail(client, channel_id, title, error):
    msg = f"⚠️ Link Generate Failed!\n\n📢 Channel: {title}\n🆔 {channel_id}\n❌ Error: {error}"
    try:
        await client.send_message(OWNER_ID, msg)
    except:
        pass
    async for admin in admins_col.find():
        try:
            await client.send_message(admin["user_id"], msg)
        except:
            pass

def get_cached_results(keyword):
    key = keyword.lower().strip()
    if key in search_cache and time.time() - search_cache[key]["time"] < SEARCH_CACHE_TTL:
        return search_cache[key]["results"]
    return None

def set_cache(keyword, results):
    key = keyword.lower().strip()
    search_cache[key] = {"results": results, "time": time.time()}

def clear_all_cache():
    search_cache.clear()

# ============================================================
# SCHEDULED TASKS
# ============================================================

async def cleanup_expired_links():
    try:
        now = datetime.now(timezone.utc)
        expired_links = await invite_links_col.find({"status": "active", "expires_at": {"$lte": now}}).to_list(length=100)
        for link in expired_links:
            try:
                await app.revoke_chat_invite_link(link["channel_id"], link["invite_link"])
            except:
                pass
            await invite_links_col.update_one({"_id": link["_id"]}, {"$set": {"status": "expired"}})
        expired_results = await search_results_col.find({"expires_at": {"$lte": now}}).to_list(length=100)
        for result in expired_results:
            try:
                await app.delete_messages(result["chat_id"], result["message_id"])
            except:
                pass
            await search_results_col.delete_one({"_id": result["_id"]})
    except Exception as e:
        logger.error(f"Cleanup error: {e}")

async def reset_daily_violations():
    try:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
        result = await antilink_violations_col.delete_many({"date": {"$lt": cutoff}})
        if result.deleted_count > 0:
            logger.info(f"Reset {result.deleted_count} violations")
    except Exception as e:
        logger.error(f"Reset violations error: {e}")

# ============================================================
# DUMMY WEB SERVER FOR RENDER
# ============================================================

async def health(request):
    return web.Response(text="OK", status=200)

async def start_web_server():
    port = int(os.environ.get("PORT", 8000))
    app_web = web.Application()
    app_web.router.add_get('/', health)
    app_web.router.add_get('/health', health)
    runner = web.AppRunner(app_web)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    logger.info(f"Web server running on port {port}")
    await asyncio.Event().wait()

# ============================================================
# STARTUP & MAIN
# ============================================================

async def on_startup():
    logger.info("Bot starting up...")
    await setup_indexes()
    scheduler.add_job(cleanup_expired_links, IntervalTrigger(minutes=2), id="cleanup_links", replace_existing=True)
    scheduler.add_job(reset_daily_violations, CronTrigger(hour=0, minute=0), id="reset_violations", replace_existing=True)
    scheduler.start()
    logger.info("Bot is ready!")

if __name__ == "__main__":
    print("=" * 50)
    print("  Alya Filter Bot v2.0 (Full: Media+Buttons, Group Filters, All Commands)")
    print("  Running on Render as Web Service")
    print("=" * 50)

    try:
        app.start()
        loop.run_until_complete(on_startup())
        print("Bot is running. Starting web server...")
        logger.info("Bot is running!")
        web_task = loop.create_task(start_web_server())
        loop.run_forever()
    except KeyboardInterrupt:
        print("\nStopping bot...")
        logger.info("Bot stopping...")
    finally:
        web_task.cancel()
        scheduler.shutdown()
        app.stop()
        mongo_client.close()
        print("Bot stopped.")
        logger.info("Bot stopped.")