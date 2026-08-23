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

5 line