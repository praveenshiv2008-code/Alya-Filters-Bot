# bot.py - Alya Filter Bot v2.0 (Complete)
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
from datetime import datetime, timedelta
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
                  "first_name": first_name, "last_active": datetime.utcnow()},
         "$setOnInsert": {"joined_at": datetime.utcnow(),
                          "searches_count": 0, "is_banned": False,
                          "warn_count": 0, "commands_used": 0}},
        upsert=True
    )

async def log_action(action: str, user_id: int = None, details: str = None):
    await logs_col.insert_one({
        "action": action, "user_id": user_id,
        "details": details, "timestamp": datetime.utcnow()
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
    await antilink_settings_col.insert_one(default_settings)
    return default_settings

async def update_antilink_settings(updates: dict):
    await antilink_settings_col.update_one(
        {"_id": "settings"},
        {"$set": updates},
        upsert=True
    )

async def get_user_violations(user_id: int, chat_id: int) -> dict:
    today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    
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
    today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    
    await antilink_violations_col.update_one(
        {"user_id": user_id, "chat_id": chat_id, "date": today},
        {
            "$inc": {"count": 1},
            "$set": {"username": username, "last_violation": datetime.utcnow()},
            "$setOnInsert": {"date": today, "created_at": datetime.utcnow()}
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
    today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    banned_until = datetime.utcnow() + timedelta(seconds=duration)
    
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
        "welcome_text": "🌸 <b>✦ ᴡᴇʟᴄᴏᴍᴇ {name}! ✦</b> 🌸\n\n<blockquote>🎀 ᴇɴᴊᴏʏ ʏᴏᴜʀ ꜱᴛᴀʏ ɪɴ ᴛʜᴇ ɢʀᴏᴜᴘ!</blockquote>",
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
    """Get start menu buttons with Alya name"""
    return [
        [
            InlineKeyboardButton("🇦", callback_data="alya_a"),
            InlineKeyboardButton("🇱", callback_data="alya_l"),
            InlineKeyboardButton("🇾", callback_data="alya_y"),
            InlineKeyboardButton("🇦", callback_data="alya_a2")
        ],
        [
            InlineKeyboardButton("📢 ᴀᴅᴅ ᴍᴇ ᴛᴏ ʏᴏᴜʀ ɢʀᴏᴜᴘ", url="https://t.me/Alya_Filter_Bot?startgroup=true")
        ],
        [
            InlineKeyboardButton("📜 ᴄᴏᴍᴍᴀɴᴅꜱ", callback_data="show_commands"),
            InlineKeyboardButton("📖 ᴀʙᴏᴜᴛ", callback_data="show_about")
        ],
        [
            InlineKeyboardButton("🔍 ꜱᴇᴀʀᴄʜ", switch_inline_query_current_chat=""),
            InlineKeyboardButton("ℹ️ ʜᴇʟᴘ", callback_data="show_help")
        ]
    ]

# ============================================================
# ANIME STYLE MESSAGES
# ============================================================

ACCESS_DENIED = """
🌸 <b>✦ ᴀᴄᴄᴇꜱꜱ ᴅᴇɴɪᴇᴅ, {name}-ᴄʜᴀɴ! ✦</b> 🌸

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
# /start COMMAND
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
                "ʏᴏᴜ ʜᴀᴠᴇ ʙᴇᴇɴ ʙᴀɴɴᴇᴅ ꜰʀᴏᴍ ᴜꜱɪɴɢ ᴛʜɪꜱ ʙᴏᴛ.\n\n"
                "💫 <i>ᴄᴏɴᴛᴀᴄᴛ ᴛʜᴇ ᴀᴅᴍɪɴ ꜰᴏʀ ᴍᴏʀᴇ ɪɴꜰᴏ.</i>"
            )
            return

        is_group = message.chat.type in [enums.ChatType.GROUP, enums.ChatType.SUPERGROUP]

        if is_group:
            await message.reply(
                f"🌸 <b>✦ ᴡᴇʟᴄᴏᴍᴇ ᴛᴏ ᴛʜᴇ ɢʀᴏᴜᴘ, {message.from_user.first_name}-ꜱᴇɴᴘᴀɪ! ✦</b> 🌸\n\n"
                f"<blockquote>🎴 <b>ᴀʟʏᴀ ʙᴏᴛ ɪꜱ ʜᴇʀᴇ ᴛᴏ ʜᴇʟᴘ!</b>\n\n"
                f"📌 ᴜꜱᴇ /help ᴛᴏ ꜱᴇᴇ ᴄᴏᴍᴍᴀɴᴅꜱ</blockquote>\n"
                f"💫 <b>ᴇɴᴊᴏʏ ᴛʜᴇ ɢʀᴏᴜᴘ, ꜱᴇɴᴘᴀɪ!</b>",
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
                buttons.append([InlineKeyboardButton("✅ I Joined", callback_data="check_join")])
                
                await message.reply(
                    f"🌸 <b>✦ ɴᴏᴛɪᴄᴇ, {message.from_user.first_name}-ꜱᴇɴᴘᴀɪ! ✦</b> 🌸\n\n"
                    f"<blockquote>🔒 <b>ᴊᴏɪɴ ʀᴇQᴜɪʀᴇᴅ</b>\n\n"
                    f"ᴘʟᴇᴀꜱᴇ ᴊᴏɪɴ ᴛʜᴇꜱᴇ ᴄʜᴀɴɴᴇʟꜱ ꜰɪʀꜱᴛ:\n"
                    f"{channels_text}</blockquote>",
                    reply_markup=InlineKeyboardMarkup(buttons)
                )
                return

        current_hour = datetime.utcnow().hour + 5.5
        current_hour = current_hour % 24
        
        if 5 <= current_hour < 12:
            greeting = "🌅 Good Morning"
        elif 12 <= current_hour < 17:
            greeting = "☀️ Good Afternoon"
        elif 17 <= current_hour < 21:
            greeting = "🌅 Good Evening"
        else:
            greeting = "🌙 Good Night"

        welcome_text = (
            f"🌸 <b>✦ ʜᴇʏ {message.from_user.first_name}, {greeting}! ✦</b> 🌸\n\n"
            f"<blockquote>⚡ <b>ɪ ᴀᴍ ᴛʜᴇ ᴍᴏꜱᴛ ᴘᴏᴡᴇʀꜰᴜʟ ᴀᴜᴛᴏ ꜰɪʟᴛᴇʀ ʙᴏᴛ ᴡɪᴛʜ ᴘʀᴇᴍɪᴜᴍ ꜰᴇᴀᴛᴜʀᴇꜱ,</b>\n"
            f"<b>ᴊᴜꜱᴛ ᴀᴅᴅ ᴍᴇ ᴛᴏ ʏᴏᴜʀ ɢʀᴏᴜᴘ ᴀɴᴅ ᴇɴᴊᴏʏ!</b></blockquote>\n\n"
            f"<blockquote>▶ <b>ᴍᴀɪɴᴛᴀɪɴᴇᴅ ʙʏ : <a href='https://t.me/PrimeCoreHQ'>ᴘʀɪᴍᴇ ᴄᴏʀᴇ</a></b></blockquote>\n\n"
            f"💫 <b>ᴅᴇᴠᴇʟᴏᴘᴇᴅ ᴡɪᴛʜ ❤️ ʙʏ <a href='https://t.me/PrimeCoreHQ'>ᴘʀɪᴍᴇ ᴄᴏʀᴇ</a></b>\n\n"
            f"～(^▽^)～ <b>ᴜꜱᴇ ᴛʜᴇ ʙᴜᴛᴛᴏɴꜱ ʙᴇʟᴏᴡ!</b> ～(^▽^)～"
        )
        
        await message.reply(
            welcome_text,
            reply_markup=InlineKeyboardMarkup(get_start_menu_buttons())
        )

    except Exception as e:
        logger.error(f"Start error: {e}")
        await message.reply("❌ <b>ᴇʀʀᴏʀ!</b>\n\nꜱᴏᴍᴇᴛʜɪɴɢ ᴡᴇɴᴛ ᴡʀᴏɴɢ, ꜱᴇɴᴘᴀɪ!")
# ============================================================
# ALYA BUTTON CALLBACKS
# ============================================================

@app.on_callback_query(filters.regex("^alya_"))
async def alya_callback(client: Client, callback: CallbackQuery):
    try:
        data = callback.data
        
        if data == "alya_a":
            await callback.answer("🇦 - Aʟʏᴀ ꜱᴛᴀʀᴛꜱ ᴡɪᴛʜ 'A' ꜰᴏʀ ᴀᴡᴇꜱᴏᴍᴇ!", show_alert=True)
        elif data == "alya_l":
            await callback.answer("🇱 - 'L' ꜰᴏʀ ʟᴏᴠᴇ ᴀɴᴅ ʟᴏʏᴀʟᴛʏ!", show_alert=True)
        elif data == "alya_y":
            await callback.answer("🇾 - 'Y' ꜰᴏʀ ʏᴇʟʟᴏᴡ ᴀɴᴅ ʏᴏᴜᴛʜ!", show_alert=True)
        elif data == "alya_a2":
            await callback.answer("🇦 - Aʟʏᴀ ᴇɴᴅꜱ ᴡɪᴛʜ 'A' ꜰᴏʀ ᴀᴍᴀᴢɪɴɢ!", show_alert=True)
    
    except Exception as e:
        logger.error(f"Alya callback error: {e}")

# ============================================================
# COMMANDS, ABOUT, HELP CALLBACKS
# ============================================================

@app.on_callback_query(filters.regex("^show_commands$"))
async def show_commands_callback(client: Client, callback: CallbackQuery):
    try:
        await callback.answer()
        
        commands_text = (
            "🌸 <b>✦ ᴄᴏᴍᴍᴀɴᴅꜱ ʟɪꜱᴛ ✦</b> 🌸\n\n"
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
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 ʙᴀᴄᴋ", callback_data="back_to_start")]
            ])
        )
    
    except Exception as e:
        logger.error(f"Show commands error: {e}")

@app.on_callback_query(filters.regex("^show_about$"))
async def show_about_callback(client: Client, callback: CallbackQuery):
    try:
        await callback.answer()
        
        about_text = (
            "🌸 <b>✦ ᴀʙᴏᴜᴛ ᴀʟʏᴀ ʙᴏᴛ ✦</b> 🌸\n\n"
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
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 ʙᴀᴄᴋ", callback_data="back_to_start")]
            ])
        )
    
    except Exception as e:
        logger.error(f"Show about error: {e}")

@app.on_callback_query(filters.regex("^show_help$"))
async def show_help_callback(client: Client, callback: CallbackQuery):
    try:
        await callback.answer()
        
        help_text = (
            "🌸 <b>✦ ʜᴇʟᴘ ɢᴜɪᴅᴇ ✦</b> 🌸\n\n"
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
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 ʙᴀᴄᴋ", callback_data="back_to_start")]
            ])
        )
    
    except Exception as e:
        logger.error(f"Show help error: {e}")

@app.on_callback_query(filters.regex("^back_to_start$"))
async def back_to_start_callback(client: Client, callback: CallbackQuery):
    try:
        await callback.answer()
        
        current_hour = datetime.utcnow().hour + 5.5
        current_hour = current_hour % 24
        
        if 5 <= current_hour < 12:
            greeting = "🌅 Good Morning"
        elif 12 <= current_hour < 17:
            greeting = "☀️ Good Afternoon"
        elif 17 <= current_hour < 21:
            greeting = "🌅 Good Evening"
        else:
            greeting = "🌙 Good Night"
        
        welcome_text = (
            f"🌸 <b>✦ ʜᴇʏ {callback.from_user.first_name}, {greeting}! ✦</b> 🌸\n\n"
            f"<blockquote>⚡ <b>ɪ ᴀᴍ ᴛʜᴇ ᴍᴏꜱᴛ ᴘᴏᴡᴇʀꜰᴜʟ ᴀᴜᴛᴏ ꜰɪʟᴛᴇʀ ʙᴏᴛ ᴡɪᴛʜ ᴘʀᴇᴍɪᴜᴍ ꜰᴇᴀᴛᴜʀᴇꜱ,</b>\n"
            f"<b>ᴊᴜꜱᴛ ᴀᴅᴅ ᴍᴇ ᴛᴏ ʏᴏᴜʀ ɢʀᴏᴜᴘ ᴀɴᴅ ᴇɴᴊᴏʏ!</b></blockquote>\n\n"
            f"<blockquote>▶ <b>ᴍᴀɪɴᴛᴀɪɴᴇᴅ ʙʏ : <a href='https://t.me/PrimeCoreHQ'>ᴘʀɪᴍᴇ ᴄᴏʀᴇ</a></b></blockquote>\n\n"
            f"💫 <b>ᴅᴇᴠᴇʟᴏᴘᴇᴅ ᴡɪᴛʜ ❤️ ʙʏ <a href='https://t.me/PrimeCoreHQ'>ᴘʀɪᴍᴇ ᴄᴏʀᴇ</a></b>\n\n"
            f"～(^▽^)～ <b>ᴜꜱᴇ ᴛʜᴇ ʙᴜᴛᴛᴏɴꜱ ʙᴇʟᴏᴡ!</b> ～(^▽^)～"
        )
        
        await callback.message.edit_text(
            welcome_text,
            reply_markup=InlineKeyboardMarkup(get_start_menu_buttons())
        )
    
    except Exception as e:
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
                f"<blockquote>🎴 <b>ʏᴏᴜ ᴄᴀɴ ɴᴏᴡ ᴜꜱᴇ ᴛʜᴇ ʙᴏᴛ!</b></blockquote>\n\n"
                f"💫 <b>ᴇɴᴊᴏʏ, ꜱᴇɴᴘᴀɪ!</b>",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("📜 ᴄᴏᴍᴍᴀɴᴅꜱ", callback_data="show_commands")],
                    [InlineKeyboardButton("📖 ᴀʙᴏᴜᴛ", callback_data="show_about")]
                ])
            )
        else:
            names = ", ".join([ch.get("title", "Channel") for ch in not_joined])
            await callback.answer(
                f"❌ ꜱᴛɪʟʟ ɴᴏᴛ ᴊᴏɪɴᴇᴅ, ꜱᴇɴᴘᴀɪ! ᴘʟᴇᴀꜱᴇ ᴊᴏɪɴ: {names}", 
                show_alert=True
            )

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
                
                welcome_text = settings.get("welcome_text", "🌸 <b>✦ ᴡᴇʟᴄᴏᴍᴇ {name}! ✦</b> 🌸")
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
# ANTI-LINK HANDLER (Continued)
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
            if datetime.utcnow() < banned_until:
                remaining = (banned_until - datetime.utcnow()).seconds
                
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
# ADD FILTER COMMAND
# ============================================================

@app.on_message(filters.command("addfilter") & filters.private)
async def add_filter_command(client: Client, message: Message):
    try:
        if not await check_admin(client, message):
            return

        if message.reply_to_message:
            replied = message.reply_to_message
            
            if len(message.command) < 2:
                await message.reply(
                    "🌸 <b>✦ ᴜꜱᴀɢᴇ, ꜱᴇɴᴘᴀɪ! ✦</b> 🌸\n\n"
                    "<blockquote>📁 <b>ʀᴇᴘʟʏ ᴛᴏ ᴀ ᴍᴇᴅɪᴀ ᴛᴏ ᴀᴅᴅ ꜰɪʟᴛᴇʀ:</b>\n\n"
                    "/addfilter [ᴋᴇʏᴡᴏʀᴅ]\n\n"
                    "ᴇxᴀᴍᴘʟᴇ: /addfilter ᴡᴇʟᴄᴏᴍᴇ</blockquote>"
                )
                return

            keyword = message.command[1].lower().strip()

            existing = await filters_col.find_one({"keyword": keyword})
            if existing:
                await message.reply(
                    f"⚠️ <b>ꜰɪʟᴛᴇʀ `{keyword}` ᴀʟʀᴇᴀᴅʏ ᴇxɪꜱᴛꜱ!</b>\n\n"
                    "ᴜꜱᴇ /editfilter ᴛᴏ ᴍᴏᴅɪꜰʏ ᴏʀ /delfilter ᴛᴏ ᴅᴇʟᴇᴛᴇ."
                )
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
                await message.reply("❌ <b>ᴜɴꜱᴜᴘᴘᴏʀᴛᴇᴅ ᴍᴇᴅɪᴀ ᴛʏᴘᴇ!</b>")
                return

            button_text = None
            button_url = None
            
            if len(message.command) >= 3:
                button_url = message.command[2]
                if len(message.command) >= 4:
                    button_text = " ".join(message.command[3:])
                else:
                    button_text = "🔗 ᴄʟɪᴄᴋ ʜᴇʀᴇ"

            filter_data = {
                "keyword": keyword,
                "media_type": media_type,
                "media_file_id": media_file_id,
                "caption": caption,
                "button_text": button_text,
                "button_url": button_url,
                "created_by": message.from_user.id,
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
                "usage_count": 0,
                "group": "general",
                "is_active": True
            }

            await filters_col.insert_one(filter_data)

            media_preview = "📷 ᴘʜᴏᴛᴏ" if media_type == "photo" else \
                           "🎬 ᴠɪᴅᴇᴏ" if media_type == "video" else \
                           "📄 ᴅᴏᴄᴜᴍᴇɴᴛ" if media_type == "document" else \
                           "🎨 ꜱᴛɪᴄᴋᴇʀ" if media_type == "sticker" else \
                           "🎥 ᴀɴɪᴍᴀᴛɪᴏɴ" if media_type == "animation" else \
                           "🎵 ᴀᴜᴅɪᴏ" if media_type == "audio" else media_type

            reply_text = (
                f"✅ <b>ꜰɪʟᴛᴇʀ ᴀᴅᴅᴇᴅ!</b>\n\n"
                f"🔑 <b>ᴋᴇʏᴡᴏʀᴅ:</b> `{keyword}`\n"
                f"📁 <b>ᴛʏᴘᴇ:</b> {media_preview}\n"
                f"📝 <b>ᴄᴀᴘᴛɪᴏɴ:</b> {caption[:50]}{'...' if len(caption) > 50 else ''}\n"
            )

            if button_text and button_url:
                reply_text += f"🔗 <b>ʙᴜᴛᴛᴏɴ:</b> {button_text} → {button_url}\n"

            reply_text += f"\n📅 <b>ᴄʀᴇᴀᴛᴇᴅ:</b> {datetime.utcnow().strftime('%d %b %Y %H:%M')}"

            await message.reply(reply_text)
            await log_action("filter_added_media", message.from_user.id, f"Keyword: {keyword}, Type: {media_type}")

        else:
            if len(message.command) < 3:
                await message.reply(
                    "🌸 <b>✦ ᴜꜱᴀɢᴇ, ꜱᴇɴᴘᴀɪ! ✦</b> 🌸\n\n"
                    "<blockquote>📁 <b>ᴛᴇxᴛ ꜰɪʟᴛᴇʀ:</b>\n"
                    "/addfilter [ᴋᴇʏᴡᴏʀᴅ] [ʀᴇᴘʟʏ]\n\n"
                    "<b>ᴍᴇᴅɪᴀ ꜰɪʟᴛᴇʀ:</b>\n"
                    "ʀᴇᴘʟʏ ᴛᴏ ᴍᴇᴅɪᴀ ᴡɪᴛʜ /addfilter [ᴋᴇʏᴡᴏʀᴅ]</blockquote>\n\n"
                    "ᴇxᴀᴍᴘʟᴇ: /addfilter ʜᴇʟʟᴏ ʜɪ!"
                )
                return

            parts = message.text.split(None, 2)
            keyword = parts[1].lower().strip()
            reply_text = parts[2]

            filter_count = await filters_col.count_documents({"created_by": message.from_user.id})
            if filter_count >= MAX_FILTERS_PER_USER:
                await message.reply(f"❌ Max filters limit reached ({MAX_FILTERS_PER_USER}).")
                return

            existing = await filters_col.find_one({"keyword": keyword})
            if existing:
                await message.reply(
                    f"⚠️ Filter `{keyword}` already exists!\n\n"
                    "Use `/editfilter` to modify or `/delfilter` to delete."
                )
                return

            await filters_col.insert_one({
                "keyword": keyword,
                "reply_text": reply_text,
                "replies": reply_text.split("|") if "|" in reply_text else [reply_text],
                "created_by": message.from_user.id,
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
                "usage_count": 0,
                "group": "general",
                "media_type": "text",
                "media_file_id": None,
                "is_active": True,
                "reply_mode": "random"
            })

            reply_preview = reply_text[:100] + "..." if len(reply_text) > 100 else reply_text
            reply_count = len(reply_text.split("|")) if "|" in reply_text else 1

            await message.reply(
                f"✅ <b>Filter Added!</b>\n\n"
                f"🔑 <b>Keyword:</b> `{keyword}`\n"
                f"📝 <b>Reply:</b> {reply_preview}\n"
                f"📊 <b>Replies:</b> {reply_count}\n"
                f"📁 <b>Group:</b> general\n\n"
                f"Use `/filterinfo {keyword}` for details."
            )
            await log_action("filter_added", message.from_user.id, f"Keyword: {keyword}")

    except Exception as e:
        logger.error(f"Add filter error: {e}")
        await message.reply(f"❌ Error: {e}")

# ============================================================
# EDIT FILTER COMMAND
# ============================================================

@app.on_message(filters.command("editfilter") & filters.private)
async def edit_filter_command(client: Client, message: Message):
    try:
        if not await check_admin(client, message):
            return

        if len(message.command) < 3:
            await message.reply("❌ <b>Usage:</b> /editfilter [keyword] [new_reply]")
            return

        parts = message.text.split(None, 2)
        keyword = parts[1].lower().strip()
        new_reply = parts[2]

        filter_doc = await filters_col.find_one({"keyword": keyword})
        if not filter_doc:
            await message.reply(f"❌ Filter `{keyword}` not found.")
            return

        if filter_doc["created_by"] != message.from_user.id and not await is_owner(message.from_user.id):
            await message.reply("❌ You can only edit your own filters.")
            return

        await filters_col.update_one(
            {"keyword": keyword},
            {"$set": {
                "reply_text": new_reply,
                "replies": new_reply.split("|") if "|" in new_reply else [new_reply],
                "updated_at": datetime.utcnow()
            }}
        )

        reply_count = len(new_reply.split("|")) if "|" in new_reply else 1

        await message.reply(
            f"✅ <b>Filter Updated!</b>\n\n"
            f"🔑 <b>Keyword:</b> `{keyword}`\n"
            f"📊 <b>Replies:</b> {reply_count}\n"
            f"📅 <b>Updated:</b> {datetime.utcnow().strftime('%d %b %Y %H:%M')}"
        )
        await log_action("filter_edited", message.from_user.id, f"Keyword: {keyword}")

    except Exception as e:
        logger.error(f"Edit filter error: {e}")
        await message.reply(f"❌ Error: {e}")

# ============================================================
# DELETE FILTER COMMAND
# ============================================================

@app.on_message(filters.command("delfilter") & filters.private)
async def delete_filter_command(client: Client, message: Message):
    try:
        if not await check_admin(client, message):
            return

        if len(message.command) < 2:
            await message.reply("❌ <b>Usage:</b> /delfilter [keyword]")
            return

        keyword = message.command[1].lower().strip()

        filter_doc = await filters_col.find_one({"keyword": keyword})
        if not filter_doc:
            await message.reply(f"❌ Filter `{keyword}` not found.")
            return

        if filter_doc["created_by"] != message.from_user.id and not await is_owner(message.from_user.id):
            await message.reply("❌ You can only delete your own filters.")
            return

        await filters_col.delete_one({"keyword": keyword})

        await message.reply(f"✅ Filter `{keyword}` deleted!")
        await log_action("filter_deleted", message.from_user.id, f"Keyword: {keyword}")

    except Exception as e:
        logger.error(f"Delete filter error: {e}")
        await message.reply(f"❌ Error: {e}")

# ============================================================
# LIST FILTERS COMMAND
# ============================================================

@app.on_message(filters.command("listfilters") & (filters.private | filters.group))
async def list_filters_command(client: Client, message: Message):
    try:
        filters_list = await filters_col.find({"is_active": True}).sort("keyword", 1).to_list(length=None)

        if not filters_list:
            await message.reply("📭 No filters found.")
            return

        groups = defaultdict(list)
        for f in filters_list:
            group = f.get("group", "general")
            groups[group].append(f)

        text = "📋 **All Filters:**\n\n"
        total_filters = 0

        for group_name, filters_in_group in groups.items():
            text += f"📁 **{group_name.upper()}** ({len(filters_in_group)})\n"
            for f in filters_in_group[:10]:
                usage = f.get("usage_count", 0)
                replies = len(f.get("replies", [f.get("reply_text", "")]))
                media_type = f.get("media_type", "text")
                media_icon = "📷" if media_type == "photo" else "🎬" if media_type == "video" else "📄" if media_type == "document" else "🎨" if media_type == "sticker" else "📝"
                text += f"   {media_icon} `{f['keyword']}` 👁️{usage} 💬{replies}\n"
            if len(filters_in_group) > 10:
                text += f"   ... and {len(filters_in_group) - 10} more\n"
            text += "\n"
            total_filters += len(filters_in_group)

        text += f"📊 **Total: {total_filters} filters**"

        for part in split_message(text):
            await message.reply(part)

    except Exception as e:
        logger.error(f"List filters error: {e}")
        await message.reply(f"❌ Error: {e}")

# ============================================================
# FILTER STATS COMMAND
# ============================================================

@app.on_message(filters.command("filterstats") & filters.private)
async def filter_stats_command(client: Client, message: Message):
    try:
        if not await check_admin(client, message):
            return

        total_filters = await filters_col.count_documents({"is_active": True})
        total_usage = await filter_logs_col.count_documents({})
        today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        today_usage = await filter_logs_col.count_documents({"used_at": {"$gte": today}})
        week = datetime.utcnow() - timedelta(days=7)
        week_usage = await filter_logs_col.count_documents({"used_at": {"$gte": week}})

        top_filters = await filters_col.find({"is_active": True}).sort("usage_count", -1).limit(10).to_list(length=10)
        top_text = ""
        if top_filters:
            top_text = "\n\n🏆 **Top 10 Most Used:**\n"
            for i, f in enumerate(top_filters, 1):
                top_text += f"   {i}. `{f['keyword']}` - {f.get('usage_count', 0)} uses\n"

        groups_dist = await filters_col.distinct("group")
        groups_text = "\n\n📁 **Group Distribution:**\n"
        for group in groups_dist:
            count = await filters_col.count_documents({"group": group, "is_active": True})
            groups_text += f"   • {group}: {count} filters\n"

        await message.reply(
            f"📊 <b>Filter Statistics:</b>\n\n"
            f"📌 Total Filters: {total_filters}\n"
            f"👁️ Total Uses: {total_usage}\n"
            f"📅 Today: {today_usage}\n"
            f"📆 This Week: {week_usage}"
            f"{top_text}{groups_text}"
        )

    except Exception as e:
        logger.error(f"Filter stats error: {e}")
        await message.reply(f"❌ Error: {e}")
# ============================================================
# ADD REPLY COMMAND
# ============================================================

@app.on_message(filters.command("addreply") & filters.private)
async def add_reply_command(client: Client, message: Message):
    try:
        if not await check_admin(client, message):
            return

        if len(message.command) < 3:
            await message.reply("❌ <b>Usage:</b> /addreply [keyword] [new_reply]")
            return

        parts = message.text.split(None, 2)
        keyword = parts[1].lower().strip()
        new_reply = parts[2]

        filter_doc = await filters_col.find_one({"keyword": keyword})
        if not filter_doc:
            await message.reply(f"❌ Filter `{keyword}` not found.")
            return

        if filter_doc["created_by"] != message.from_user.id and not await is_owner(message.from_user.id):
            await message.reply("❌ You can only edit your own filters.")
            return

        current_replies = filter_doc.get("replies", [filter_doc.get("reply_text", "")])
        
        if len(current_replies) >= MAX_REPLIES_PER_FILTER:
            await message.reply(f"❌ Max replies per filter ({MAX_REPLIES_PER_FILTER}) reached.")
            return

        current_replies.append(new_reply)

        await filters_col.update_one(
            {"keyword": keyword},
            {"$set": {
                "replies": current_replies,
                "reply_text": " | ".join(current_replies),
                "updated_at": datetime.utcnow()
            }}
        )

        await message.reply(
            f"✅ <b>Reply Added!</b>\n\n"
            f"🔑 <b>Keyword:</b> `{keyword}`\n"
            f"📝 <b>New Reply:</b> {new_reply[:100]}{'...' if len(new_reply) > 100 else ''}\n"
            f"📊 <b>Total Replies:</b> {len(current_replies)}"
        )
        await log_action("reply_added", message.from_user.id, f"Keyword: {keyword}")

    except Exception as e:
        logger.error(f"Add reply error: {e}")
        await message.reply(f"❌ Error: {e}")

# ============================================================
# DELETE REPLY COMMAND
# ============================================================

@app.on_message(filters.command("delreply") & filters.private)
async def delete_reply_command(client: Client, message: Message):
    try:
        if not await check_admin(client, message):
            return

        if len(message.command) < 3:
            await message.reply("❌ <b>Usage:</b> /delreply [keyword] [reply_number]")
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

        if filter_doc["created_by"] != message.from_user.id and not await is_owner(message.from_user.id):
            await message.reply("❌ You can only edit your own filters.")
            return

        current_replies = filter_doc.get("replies", [filter_doc.get("reply_text", "")])
        
        if reply_num < 0 or reply_num >= len(current_replies):
            await message.reply(f"❌ Reply number {reply_num + 1} not found. Total replies: {len(current_replies)}")
            return

        deleted_reply = current_replies.pop(reply_num)

        await filters_col.update_one(
            {"keyword": keyword},
            {"$set": {
                "replies": current_replies,
                "reply_text": " | ".join(current_replies) if current_replies else "No replies",
                "updated_at": datetime.utcnow()
            }}
        )

        await message.reply(
            f"✅ <b>Reply Deleted!</b>\n\n"
            f"🔑 <b>Keyword:</b> `{keyword}`\n"
            f"📝 <b>Deleted:</b> {deleted_reply[:100]}{'...' if len(deleted_reply) > 100 else ''}\n"
            f"📊 <b>Total Replies:</b> {len(current_replies)}"
        )
        await log_action("reply_deleted", message.from_user.id, f"Keyword: {keyword}")

    except Exception as e:
        logger.error(f"Delete reply error: {e}")
        await message.reply(f"❌ Error: {e}")

# ============================================================
# FILTER GROUP COMMAND
# ============================================================

@app.on_message(filters.command("filtergroup") & filters.private)
async def filter_group_command(client: Client, message: Message):
    try:
        if not await check_admin(client, message):
            return

        if len(message.command) < 3:
            await message.reply("❌ <b>Usage:</b> /filtergroup [keyword] [group_name]")
            return

        keyword = message.command[1].lower().strip()
        group_name = message.command[2].lower().strip()

        filter_doc = await filters_col.find_one({"keyword": keyword})
        if not filter_doc:
            await message.reply(f"❌ Filter `{keyword}` not found.")
            return

        if filter_doc["created_by"] != message.from_user.id and not await is_owner(message.from_user.id):
            await message.reply("❌ You can only modify your own filters.")
            return

        await filters_col.update_one(
            {"keyword": keyword},
            {"$set": {"group": group_name, "updated_at": datetime.utcnow()}}
        )

        await message.reply(
            f"✅ <b>Filter Group Updated!</b>\n\n"
            f"🔑 <b>Keyword:</b> `{keyword}`\n"
            f"📁 <b>New Group:</b> {group_name}"
        )
        await log_action("filter_group", message.from_user.id, f"Keyword: {keyword} -> {group_name}")

    except Exception as e:
        logger.error(f"Filter group error: {e}")
        await message.reply(f"❌ Error: {e}")

# ============================================================
# EXPORT FILTERS COMMAND
# ============================================================

@app.on_message(filters.command("exportfilters") & filters.private)
async def export_filters_command(client: Client, message: Message):
    try:
        if not await check_admin(client, message):
            return

        status_msg = await message.reply("📦 **Exporting filters...**")

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
        
        filename = f"filters_export_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(json_data)

        await client.send_document(
            message.from_user.id,
            filename,
            caption=f"📦 **Filters Export**\n\n📊 Total Filters: {len(export_data)}\n📅 Exported: {datetime.utcnow().strftime('%d %b %Y %H:%M')}"
        )

        os.remove(filename)
        await status_msg.delete()
        await log_action("filters_exported", message.from_user.id, f"Count: {len(export_data)}")

    except Exception as e:
        logger.error(f"Export filters error: {e}")
        await message.reply(f"❌ Error: {e}")

# ============================================================
# IMPORT FILTERS COMMAND
# ============================================================

@app.on_message(filters.command("importfilters") & filters.private)
async def import_filters_command(client: Client, message: Message):
    try:
        if not await check_admin(client, message):
            return

        await message.reply(
            "📤 **Import Filters**\n\n"
            "Send me the JSON file exported from this bot.\n\n"
            "⚠️ Existing filters with same keywords will be **overwritten**."
        )

    except Exception as e:
        logger.error(f"Import filters error: {e}")
        await message.reply(f"❌ Error: {e}")

# ============================================================
# IMPORT FILTERS FILE HANDLER
# ============================================================

@app.on_message(filters.document & filters.private)
async def import_filters_file_handler(client: Client, message: Message):
    try:
        if not message.document.file_name.endswith('.json'):
            return

        if not await check_admin(client, message):
            return

        status_msg = await message.reply("📥 **Downloading file...**")

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
        skipped = 0
        errors = 0

        for filter_data in import_data:
            try:
                keyword = filter_data.get("keyword", "").lower().strip()
                replies = filter_data.get("replies", [])
                group = filter_data.get("group", "general")
                media_type = filter_data.get("media_type", "text")
                media_file_id = filter_data.get("media_file_id")
                caption = filter_data.get("caption", "")
                button_text = filter_data.get("button_text")
                button_url = filter_data.get("button_url")

                if not keyword or not replies:
                    skipped += 1
                    continue

                existing = await filters_col.find_one({"keyword": keyword})
                
                filter_doc = {
                    "keyword": keyword,
                    "reply_text": " | ".join(replies),
                    "replies": replies,
                    "group": group,
                    "media_type": media_type,
                    "media_file_id": media_file_id,
                    "caption": caption,
                    "button_text": button_text,
                    "button_url": button_url,
                    "created_by": message.from_user.id,
                    "created_at": datetime.utcnow(),
                    "updated_at": datetime.utcnow(),
                    "usage_count": 0,
                    "is_active": True
                }

                if existing:
                    await filters_col.update_one(
                        {"keyword": keyword},
                        {"$set": {
                            "replies": replies,
                            "reply_text": " | ".join(replies),
                            "group": group,
                            "media_type": media_type,
                            "media_file_id": media_file_id,
                            "caption": caption,
                            "button_text": button_text,
                            "button_url": button_url,
                            "updated_at": datetime.utcnow()
                        }}
                    )
                    imported += 1
                else:
                    await filters_col.insert_one(filter_doc)
                    imported += 1

            except Exception as e:
                errors += 1
                logger.error(f"Import error: {e}")

        os.remove(file_path)

        await status_msg.edit_text(
            f"✅ <b>Import Complete!</b>\n\n"
            f"✅ Imported: {imported}\n"
            f"⏭️ Skipped: {skipped}\n"
            f"❌ Errors: {errors}"
        )
        await log_action("filters_imported", message.from_user.id, f"Imported: {imported}")

    except Exception as e:
        logger.error(f"Import filters file error: {e}")
        await message.reply(f"❌ Error: {e}")
# ============================================================
# ANTI-LINK COMMANDS
# ============================================================

@app.on_message(filters.command("antilink") & filters.private)
async def antilink_settings_command(client: Client, message: Message):
    try:
        if not await check_admin(client, message):
            return

        settings = await get_antilink_settings()
        
        if len(message.command) < 2:
            status = "🟢 Enabled" if settings.get("enabled", True) else "🔴 Disabled"
            await message.reply(
                f"🛡️ <b>✦ Anti-Link Settings ✦</b> 🛡️\n\n"
                f"<blockquote>📊 <b>Status:</b> {status}\n"
                f"⚠️ <b>Warn Limit:</b> {settings.get('warn_limit', 3)}\n"
                f"⏱️ <b>Ban Duration:</b> {settings.get('ban_duration', 10)}s\n"
                f"⏰ <b>Reset Time:</b> {settings.get('reset_time', 24)}h\n"
                f"⚡ <b>Action:</b> {settings.get('action', 'warn')}\n"
                f"📢 <b>Notify Admins:</b> {'✅' if settings.get('notify_admins', True) else '❌'}</blockquote>\n"
                f"<b>Usage:</b>\n"
                f"/antilink on/off - Toggle\n"
                f"/antilink limit [num] - Set warn limit\n"
                f"/antilink bantime [seconds] - Set ban duration\n"
                f"/antilink action [warn/delete/ban] - Set action\n"
                f"/antilink reset [user_id] - Reset user violations"
            )
            return

        command = message.command[1].lower()

        if command == "on":
            await update_antilink_settings({"enabled": True})
            await message.reply("✅ <b>Anti-Link Enabled!</b>")

        elif command == "off":
            await update_antilink_settings({"enabled": False})
            await message.reply("❌ <b>Anti-Link Disabled!</b>")

        elif command == "limit":
            if len(message.command) < 3:
                await message.reply("❌ <b>Usage:</b> /antilink limit [num]")
                return
            try:
                limit = int(message.command[2])
                if limit < 1 or limit > 10:
                    await message.reply("❌ <b>Limit must be between 1 and 10</b>")
                    return
                await update_antilink_settings({"warn_limit": limit})
                await message.reply(f"✅ <b>Warn limit set to:</b> {limit}")
            except ValueError:
                await message.reply("❌ <b>Invalid number!</b>")

        elif command == "bantime":
            if len(message.command) < 3:
                await message.reply("❌ <b>Usage:</b> /antilink bantime [seconds]")
                return
            try:
                duration = int(message.command[2])
                if duration < 5 or duration > 3600:
                    await message.reply("❌ <b>Duration must be between 5 and 3600 seconds</b>")
                    return
                await update_antilink_settings({"ban_duration": duration})
                await message.reply(f"✅ <b>Ban duration set to:</b> {duration}s")
            except ValueError:
                await message.reply("❌ <b>Invalid number!</b>")

        elif command == "action":
            if len(message.command) < 3:
                await message.reply("❌ <b>Usage:</b> /antilink action [warn/delete/ban]")
                return
            action = message.command[2].lower()
            if action not in ["warn", "delete", "ban"]:
                await message.reply("❌ <b>Invalid action! Use:</b> warn, delete, or ban")
                return
            await update_antilink_settings({"action": action})
            await message.reply(f"✅ <b>Action set to:</b> {action}")

        elif command == "reset":
            if len(message.command) < 3:
                await message.reply("❌ <b>Usage:</b> /antilink reset [user_id]")
                return
            try:
                user_id = int(message.command[2])
                await reset_user_violations(user_id, message.chat.id)
                await message.reply(f"✅ <b>Violations reset for user:</b> `{user_id}`")
                await log_action("antilink_reset", message.from_user.id, f"User: {user_id}")
            except ValueError:
                await message.reply("❌ <b>Invalid user ID!</b>")

        else:
            await message.reply("❌ <b>Invalid command!</b> Use /antilink for help")

    except Exception as e:
        logger.error(f"Anti-link settings error: {e}")
        await message.reply(f"❌ <b>Error:</b> {e}")

# ============================================================
# AUTO-REPLY HANDLER (Filter Trigger)
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
            replies = f.get("replies", [f.get("reply_text", "")])
            
            if keyword in text:
                pattern = r'\b' + re.escape(keyword) + r'\b'
                if re.search(pattern, text):
                    reply = random.choice(replies)
                    
                    await message.reply(reply)
                    
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
                        "used_at": datetime.utcnow()
                    })
                    
                    await users_col.update_one(
                        {"user_id": message.from_user.id},
                        {"$inc": {"commands_used": 1}}
                    )
                    
                    break

    except Exception as e:
        logger.error(f"Filter reply error: {e}")

# ============================================================
# PING COMMAND
# ============================================================

@app.on_message(filters.command("ping"))
async def ping_command(client: Client, message: Message):
    try:
        start = time.time()
        msg = await message.reply("🏓 Pinging...")
        end = time.time()
        
        latency = (end - start) * 1000
        await msg.edit_text(
            f"🏓 <b>Pong!</b>\n\n"
            f"📊 Latency: `{latency:.0f}ms`\n"
            f"⏱️ Response: `{(end - start):.2f}s`"
        )
    except Exception as e:
        logger.error(f"Ping error: {e}")

# ============================================================
# SCHEDULED TASKS
# ============================================================

async def cleanup_expired_links():
    try:
        now = datetime.utcnow()
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
        cutoff = datetime.utcnow() - timedelta(hours=24)
        result = await antilink_violations_col.delete_many({"date": {"$lt": cutoff}})
        if result.deleted_count > 0:
            logger.info(f"Reset {result.deleted_count} violations")
    except Exception as e:
        logger.error(f"Reset violations error: {e}")

# ============================================================
# STARTUP
# ============================================================

async def on_startup():
    logger.info("Bot starting up...")
    await setup_indexes()
    
    scheduler.add_job(cleanup_expired_links, IntervalTrigger(minutes=2), id="cleanup_links", replace_existing=True)
    scheduler.add_job(reset_daily_violations, CronTrigger(hour=0, minute=0), id="reset_violations", replace_existing=True)
    
    scheduler.start()
    logger.info("Bot is ready!")

# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    print("=" * 50)
    print("  Alya Filter Bot v2.0")
    print("  Running on Render.com" if os.environ.get("RENDER") else "  Running locally")
    print("=" * 50)

    try:
        app.start()
        loop.run_until_complete(on_startup())
        print("Bot is running! Press Ctrl+C to stop.")
        logger.info("Bot is running!")
        loop.run_forever()
    except KeyboardInterrupt:
        print("\nStopping bot...")
        logger.info("Bot stopping...")
    finally:
        scheduler.shutdown()
        app.stop()
        mongo_client.close()
        print("Bot stopped.")
        logger.info("Bot stopped.")