# bot.py - Telegram Search Bot v2.0 (Complete Rewrite)
# Search by Channel Bio & Name | URL Reuse | Fuzzy Match

import os
import sys
import json
import time
import asyncio
import logging
import zipfile
import io
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Optional, List, Dict, Any, Tuple
from difflib import SequenceMatcher

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

OWNER_ID = 6454751048
BACKUP_CHANNEL_ID = -1002932260531

LINK_EXPIRY_SECONDS = 1800     # 30 minutes
LINK_REUSE_WINDOW = 1500       # 25 minutes
BIO_UPDATE_INTERVAL = 14       # hours
SEARCH_CACHE_TTL = 300         # 5 minutes (cache)
SEARCH_RESULT_LIMIT = 10
RATE_LIMIT_SEARCHES = 5
RATE_LIMIT_WINDOW = 60         # seconds
BROADCAST_SPEED = 25
TG_MAX_LENGTH = 4096
FUZZY_THRESHOLD = 0.75
LIST_PAGE_SIZE = 25
BACKUP_RETAIN_COUNT = 4

# All bot commands (for search filter exclusion)
ALL_COMMANDS = [
    "start", "verify", "unverify", "channels", "list",
    "broadcast", "addad", "removead", "listads", "adstats",
    "ban", "unban", "userinfo", "addfj", "removefj", "listfj",
    "stats", "channelstats", "searchstats", "backup", "restore",
    "maintenance", "logs", "addmin", "radmin", "ladmin", "help",
    "search", "anime", "a", "s"
]

# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %levelname)s - %(message)s',
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
        raise RuntimeError(
            "Missing required environment variables: " + ", ".join(missing)
        )
    if API_ID <= 0:
        raise RuntimeError("API_ID must be a positive integer.")


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

# Scheduler
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
        logger.info("Database indexes created")
    except Exception as e:
        logger.error(f"Index error: {e}")
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
                          "searches_count": 0, "is_banned": False}},
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
        fj_type = ch.get("type", "join")  # "request" or "join"
        
        # For "request" type - check if user has sent request OR is member
        if fj_type == "request":
            # First check if request exists in our database
            request_doc = await force_join_requests_col.find_one({
                "user_id": user_id,
                "channel_id": channel_id
            })
            
            if request_doc:
                # User has sent request - ALLOWED ✅
                continue
            
            # Also check if somehow user is already member
            try:
                member = await client.get_chat_member(channel_id, user_id)
                if member.status not in [enums.ChatMemberStatus.BANNED, enums.ChatMemberStatus.LEFT]:
                    # User is member - ALLOWED ✅
                    continue
            except UserNotParticipant:
                pass
            except Exception:
                pass
            
            # Neither request sent nor member
            not_joined.append(ch)
        
        # For "join" type - must be actual member
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


# ============================================================
# FUZZY MATCH (3-Level Matching)
# ============================================================

def fuzzy_match(search_query: str, target_text: str, threshold: float = FUZZY_THRESHOLD) -> bool:
    if not search_query or not target_text:
        return False

    search_lower = search_query.lower().strip()
    target_lower = target_text.lower().strip()

    if not search_lower or not target_lower:
        return False

    # Level 1: Full search query at word boundary in target
    idx = target_lower.find(search_lower)
    while idx != -1:
        if idx == 0 or target_lower[idx - 1] == ' ':
            return True
        idx = target_lower.find(search_lower, idx + 1)

    # Level 2: Each search word is prefix of some target word
    search_words = search_lower.split()
    target_words = target_lower.split()

    if search_words:
        prefix_matched = 0
        for s_word in search_words:
            for t_word in target_words:
                if t_word.startswith(s_word):
                    prefix_matched += 1
                    break
        if prefix_matched == len(search_words):
            return True

    # Level 3: Fuzzy word matching (75% threshold)
    if search_words:
        fuzzy_matched = 0
        for s_word in search_words:
            best_ratio = 0
            for t_word in target_words:
                ratio = SequenceMatcher(None, s_word, t_word).ratio()
                if ratio > best_ratio:
                    best_ratio = ratio
            if best_ratio >= threshold:
                fuzzy_matched += 1
        if len(search_words) > 0:
            match_ratio = fuzzy_matched / len(search_words)
            if match_ratio >= threshold:
                return True

    return False


# ============================================================
# SEARCH CACHE
# ============================================================

def get_cached_results(keyword: str) -> Optional[list]:
    key = keyword.lower().strip()
    if key in search_cache:
        entry = search_cache[key]
        if time.time() - entry["time"] < SEARCH_CACHE_TTL:
            return entry["results"]
        else:
            del search_cache[key]
    return None


def set_cache(keyword: str, results: list):
    key = keyword.lower().strip()
    search_cache[key] = {"results": results, "time": time.time()}


def clear_all_cache():
    search_cache.clear()


# ============================================================
# GET OR CREATE INVITE LINK (Reuse System)
# ============================================================

async def get_or_create_invite_link(client: Client, channel_id: int, is_private: bool) -> str:
    now = datetime.utcnow()

    # Check existing reusable link
    existing = await invite_links_col.find_one({
        "channel_id": channel_id,
        "status": "active",
        "reuse_until": {"$gt": now}
    })

    if existing:
        return existing["invite_link"]

    # Create new link
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


# ============================================================
# LINK FAIL NOTIFICATION
# ============================================================

async def notify_link_fail(client: Client, channel_id: int, title: str, error: str):
    msg = (
        f"⚠️ **Link Generate Failed!**\n\n"
        f"📢 **Channel:** {title}\n"
        f"🆔 **ID:** `{channel_id}`\n"
        f"❌ **Error:** {error}\n"
        f"📅 **Time:** {datetime.utcnow().strftime('%d %b %Y %H:%M UTC')}\n\n"
        f"Please check channel admin status."
    )
    try:
        await client.send_message(OWNER_ID, msg)
    except Exception:
        pass
    async for admin in admins_col.find():
        try:
            await client.send_message(admin["user_id"], msg)
        except Exception:
            pass


# ============================================================
# MAINTENANCE CHECK MIDDLEWARE
# ============================================================

@app.on_message(filters.private, group=-2)
async def maintenance_check(client: Client, message: Message):
    try:
        user_id = message.from_user.id
        if await is_admin(user_id):
            return
        setting = await settings_col.find_one({"_id": "maintenance"})
        if setting and setting.get("enabled", False):
            await message.reply("🔧 **Bot is under maintenance.**\nPlease try again later.")
            message.stop_propagation()
    except Exception:
        pass
# ============================================================
# JOIN REQUEST HANDLER
# ============================================================
@app.on_chat_join_request()
async def handle_join_request(client: Client, join_request: ChatJoinRequest):
    try:
        user_id = join_request.from_user.id
        chat_id = join_request.chat.id
        invite_link = join_request.invite_link

        if not invite_link:
            return

        link_name = invite_link.name or ""

        # SEARCH links → Auto approve
        if link_name.startswith("search_"):
            link_doc = await invite_links_col.find_one({
                "invite_link": invite_link.invite_link,
                "status": "active"
            })
            if link_doc and datetime.utcnow() < link_doc["expires_at"]:
                await client.approve_chat_join_request(chat_id, user_id)
                await log_action("join_approved", user_id, f"Channel: {chat_id}")
            else:
                try:
                    await client.decline_chat_join_request(chat_id, user_id)
                except Exception:
                    pass

        # FORCE JOIN request links → Save request (don't approve)
        elif link_name.startswith("force_request_"):
            # Save that user has sent request for this channel
            await force_join_requests_col.update_one(
                {"user_id": user_id, "channel_id": chat_id},
                {
                    "$set": {
                        "user_id": user_id,
                        "channel_id": chat_id,
                        "requested_at": datetime.utcnow(),
                        "status": "pending"
                    }
                },
                upsert=True
            )
            await log_action("force_join_request", user_id, f"Channel: {chat_id}")
            logger.info(f"Force join request saved: user {user_id} for channel {chat_id}")

        # force_join_ links (direct join type) → Do nothing, user joins directly

    except Exception as e:
        logger.error(f"Join request error: {e}")

# ============================================================
# /start COMMAND
# ============================================================

@app.on_message(filters.command("start") & (filters.private | filters.group))
async def start_command(client: Client, message: Message):
    try:
        user_id = message.from_user.id
        await save_user(user_id, message.from_user.username, message.from_user.first_name)

        # Check if group or private
        is_group = message.chat.type in [enums.ChatType.GROUP, enums.ChatType.SUPERGROUP]

        if is_group:
            # Short message for groups
            await message.reply(
            f"👋 **Welcome {message.from_user.first_name}!**\n\n"
            f"🔍 **Anime Search Bot by @SBANIME**\n\n"
            f"📌 **/help**\n"
            f"🔎 **/search anime name**\n"
            f"🔍 **/s anime name**\n"
            f"༺═━━━ {{ ⚜ }} ━━━═༻\n"
            f"    **👑 Developed by RJ**\n"
            f"༺═━━━ {{ ⚜ }} ━━━═༻"
        )
            return

        # Private chat - full flow with force join check
        is_joined, not_joined = await check_force_join(client, user_id)
        if not is_joined:
            buttons = []
            for ch in not_joined:
                btn_text = ch.get("title", "Join Channel")
                btn_url = ch.get("invite_url", "https://t.me")
                buttons.append([InlineKeyboardButton(f"📢 {btn_text}", url=btn_url)])
            buttons.append([InlineKeyboardButton("✅ I Joined", callback_data="check_join")])
            await message.reply(
                "⚠️ **Bot use karne ke liye pehle ye channels join karo:**",
                reply_markup=InlineKeyboardMarkup(buttons)
            )
            return

        await message.reply(
            f"👋 **Welcome {message.from_user.first_name}!**\n\n"
            f"🔍 **Anime Search Bot by @SBANIME**\n\n"
            f"📌 **/help**\n"
            f"🔎 **/search anime name**\n"
            f"🔍 **/s anime name**\n"
            f"༺═━━━ {{ ⚜ }} ━━━═༻\n"
            f"    **👑 Developed by RJ**\n"
            f"༺═━━━ {{ ⚜ }} ━━━═༻"
        )
    except Exception as e:
        logger.error(f"Start error: {e}")


# ============================================================
# FORCE JOIN CHECK CALLBACK
# ============================================================

@app.on_callback_query(filters.regex("^check_join$"))
async def check_join_callback(client: Client, callback: CallbackQuery):
    try:
        user_id = callback.from_user.id
        is_joined, not_joined = await check_force_join(client, user_id)
        if is_joined:
            await callback.message.edit_text(
                "✅ **Verified!** Ab tum bot use kar sakte ho.\n\n"
                "🔍 Koi bhi keyword bhejo search karne ke liye!"
            )
        else:
            names = ", ".join([ch.get("title", "Channel") for ch in not_joined])
            await callback.answer(f"❌ Abhi join nahi kiya: {names}", show_alert=True)
    except Exception as e:
        logger.error(f"Check join error: {e}")

# ============================================================
# CORE SEARCH FUNCTION (Used by both DM and Group)
# ============================================================

async def perform_search(client: Client, message: Message, keyword: str):
    """Core search logic - works for both private and group"""
    try:
        user_id = message.from_user.id

        await save_user(user_id, message.from_user.username, message.from_user.first_name)

        # Force join check
        is_joined, not_joined = await check_force_join(client, user_id)
        if not is_joined:
            buttons = []
            for ch in not_joined:
                buttons.append([InlineKeyboardButton(
                    f"📢 {ch.get('title', 'Join')}", url=ch.get("invite_url", "https://t.me")
                )])
            buttons.append([InlineKeyboardButton("✅ I Joined", callback_data="check_join")])
            await message.reply("⚠️ **Pehle ye channels join karo:**",
                                reply_markup=InlineKeyboardMarkup(buttons))
            return

        # Ban check
        user_doc = await users_col.find_one({"user_id": user_id})
        if user_doc and user_doc.get("is_banned", False):
            await message.reply("🚫 Tum banned ho. Admin se contact karo.")
            return

        # Rate limit
        if not check_rate_limit(user_id):
            await message.reply("⚠️ Bohot zyada search! 1 minute mein max 5 search.")
            return

        searching_msg = await message.reply("🔍 **Searching...**")

        # Check cache
        cached = get_cached_results(keyword)
        if cached is not None:
            matched_channels = cached
        else:
            # Get all verified channels
            all_channels = await channels_col.find({"status": "active"}).to_list(length=None)

            matched_channels = []
            for ch in all_channels:
                bio = ch.get("bio", "") or ""
                title = ch.get("title", "") or ""

                if fuzzy_match(keyword, bio) or fuzzy_match(keyword, title):
                    matched_channels.append(ch)

            set_cache(keyword, matched_channels)

        if not matched_channels:
            await searching_msg.edit_text(
                f"❌ **No channels found for:** `{keyword}`\n\nKoi aur keyword try karo."
            )
            return

        display_channels = matched_channels[:SEARCH_RESULT_LIMIT]

        # Build response
        response_text = f"🔍 **Search Results for:** `{keyword}`\n\n"
        buttons = []

        # ADS FIRST
        active_ads = await get_active_ads()
        for ad in active_ads:
            buttons.append([InlineKeyboardButton(
                ad.get("button_text", "Sponsored"), url=ad.get("url", "https://t.me")
            )])
            await ads_col.update_one({"_id": ad["_id"]}, {"$inc": {"impressions": 1}})

        # Channel links
        success_channels = []
        for ch in display_channels:
            try:
                link = await get_or_create_invite_link(
                    client, ch["channel_id"], ch.get("is_private", True)
                )
                response_text += f"📌 {ch['title']}\n"
                buttons.append([InlineKeyboardButton(
                    f"🔗 {ch['title']}", url=link
                )])
                success_channels.append(ch)
            except Exception as e:
                logger.error(f"Link fail for {ch.get('title')}: {e}")
                await notify_link_fail(
                    client, ch["channel_id"], ch.get("title", "Unknown"), str(e)
                )
                continue

        if not success_channels:
            await searching_msg.edit_text(
                "⚠️ **Link Generation Failed**\n\n"
                "Matching channels mile lekin links generate nahi ho pa rahe.\n"
                "Please kuch time baad try karo."
            )
            return

        response_text += (
            f"\n━━━━━━━━━━━━━━━━━━━\n"
            f"📊 Found {len(success_channels)} channel(s)"
        )

        await searching_msg.delete()

        sent_msg = await message.reply(
            response_text,
            reply_markup=InlineKeyboardMarkup(buttons)
        )

        # Store for auto-delete
        await search_results_col.insert_one({
            "user_id": user_id,
            "chat_id": message.chat.id,
            "message_id": sent_msg.id,
            "expires_at": datetime.utcnow() + timedelta(seconds=LINK_EXPIRY_SECONDS)
        })

        await users_col.update_one({"user_id": user_id}, {"$inc": {"searches_count": 1}})
        await log_action("search", user_id, f"Query: {keyword}, Found: {len(success_channels)}")

    except Exception as e:
        logger.error(f"Search error: {e}")
        try:
            await message.reply("❌ There was an error in the search. Please try again.")
        except Exception:
            pass

# ============================================================
# SEARCH HANDLER (Main Search - Bio & Name Based)
# ============================================================

@app.on_message(filters.text & filters.private & ~filters.command(ALL_COMMANDS))
async def search_handler(client: Client, message: Message):
    """Handle direct text search in DM only"""
    try:
        keyword = message.text.strip()

        if not keyword or len(keyword) < 2:
            await message.reply("❌ Minimum 2 characters ka keyword bhejo.")
            return
        if len(keyword) > 200:
            await message.reply("❌ Keyword bohot lamba hai. Max 200 characters.")
            return

        await perform_search(client, message, keyword)

    except Exception as e:
        logger.error(f"Search handler error: {e}")

# ============================================================
# GROUP SEARCH HANDLER (/search, /anime, /a, /s)
# ============================================================

@app.on_message(filters.command(["search", "anime", "a", "s"]) & (filters.private | filters.group))
async def group_search_handler(client: Client, message: Message):
    """Handle /search, /anime, /a, /s commands in both DM and Groups"""
    try:
        # Extract keyword from command
        if len(message.command) < 2:
            await message.reply(
                "❌ **name bhi likho!**\n\n"
                "**Example:**\n"
                "/search **carnitrix**\n"
                "/anime Fairy Tail\n"
                "/a Dragon Ball\n"
                "/s One Piece"
            )
            return

        # Get everything after command as keyword
        keyword = message.text.split(None, 1)[1].strip()

        if not keyword or len(keyword) < 2:
            await message.reply("❌ Minimum 2 characters ka keyword bhejo.")
            return
        if len(keyword) > 250:
            await message.reply("❌ Keyword bohot lamba hai. Max 250 characters.")
            return

        await perform_search(client, message, keyword)

    except Exception as e:
        logger.error(f"Group search error: {e}")
        try:
            await message.reply("❌ There was an error in the search. Please try again.")
        except Exception:
            pass
# ============================================================
# /verify COMMAND (Saves Bio)
# ============================================================

@app.on_message(filters.command("verify") & filters.private)
async def verify_channel(client: Client, message: Message):
    try:
        if not await is_admin(message.from_user.id):
            await message.reply("❌ Sirf owner/admin ye command use kar sakta hai.")
            return

        if len(message.command) < 2:
            await message.reply("❌ **Usage:** `/verify channel_id`\n**Example:** `/verify -1001234567890`")
            return

        try:
            channel_id = int(message.command[1])
        except ValueError:
            await message.reply("❌ Invalid channel ID.")
            return

        existing = await channels_col.find_one({"channel_id": channel_id})
        if existing and existing.get("status") == "active":
            await message.reply("⚠️ Ye channel pehle se verified hai!")
            return

        try:
            chat = await client.get_chat(channel_id)
        except ChannelPrivate:
            await message.reply("❌ Bot ko is channel mein admin access nahi hai.")
            return
        except PeerIdInvalid:
            await message.reply("❌ Invalid channel ID.")
            return
        except Exception as e:
            await message.reply(f"❌ Channel access error: {e}")
            return

        try:
            bot_member = await client.get_chat_member(channel_id, "me")
            if bot_member.status not in [enums.ChatMemberStatus.ADMINISTRATOR, enums.ChatMemberStatus.OWNER]:
                await message.reply("❌ Bot ko is channel mein admin banao pehle.")
                return
        except Exception:
            await message.reply("❌ Bot ki admin status check nahi ho paayi.")
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
                "verified_at": datetime.utcnow()
            }},
            upsert=True
        )

        clear_all_cache()

        privacy = "🔒 Private" if is_private else "🌐 Public"
        bio_preview = bio[:100] + "..." if len(bio) > 100 else (bio or "No bio")

        await message.reply(
            f"✅ **Channel Verified!**\n\n"
            f"📢 **Title:** {chat.title}\n"
            f"🆔 **ID:** `{channel_id}`\n"
            f"🔐 **Type:** {privacy}\n"
            f"📝 **Bio:** {bio_preview}"
        )
        await log_action("channel_verified", message.from_user.id, f"{chat.title} ({channel_id})")

    except Exception as e:
        logger.error(f"Verify error: {e}")
        await message.reply(f"❌ Error: {e}")


# ============================================================
# /unverify COMMAND
# ============================================================

@app.on_message(filters.command("unverify") & filters.private)
async def unverify_channel(client: Client, message: Message):
    try:
        if not await is_admin(message.from_user.id):
            await message.reply("❌ Sirf owner/admin ye command use kar sakta hai.")
            return

        if len(message.command) < 2:
            await message.reply("❌ **Usage:** `/unverify channel_id`")
            return

        try:
            channel_id = int(message.command[1])
        except ValueError:
            await message.reply("❌ Invalid channel ID.")
            return

        channel = await channels_col.find_one({"channel_id": channel_id})
        if not channel:
            await message.reply("❌ Ye channel verified nahi hai.")
            return

        await channels_col.delete_one({"channel_id": channel_id})
        clear_all_cache()

        await message.reply(
            f"✅ **Channel Removed!**\n\n"
            f"📢 **Title:** {channel.get('title', 'Unknown')}\n"
            f"🆔 **ID:** `{channel_id}`"
        )
        await log_action("channel_removed", message.from_user.id, f"{channel_id}")

    except Exception as e:
        logger.error(f"Unverify error: {e}")
        await message.reply(f"❌ Error: {e}")


# ============================================================
# /channels COMMAND
# ============================================================

@app.on_message(filters.command("channels") & filters.private)
async def list_channels_cmd(client: Client, message: Message):
    try:
        if not await is_admin(message.from_user.id):
            await message.reply("❌ Sirf owner/admin ye command use kar sakta hai.")
            return

        channels = await channels_col.find({"status": "active"}).to_list(length=None)
        if not channels:
            await message.reply("📭 Koi verified channel nahi hai.")
            return

        text = "📋 **Verified Channels:**\n\n"
        for i, ch in enumerate(channels, 1):
            privacy = "🔒" if ch.get("is_private", True) else "🌐"
            text += (
                f"{i}. {privacy} **{ch.get('title', 'Unknown')}**\n"
                f"   🆔 `{ch['channel_id']}`\n\n"
            )
        text += f"**Total: {len(channels)} channels**"

        for part in split_message(text):
            await message.reply(part)

    except Exception as e:
        logger.error(f"Channels error: {e}")
        await message.reply(f"❌ Error: {e}")


# ============================================================
# /list COMMAND (Detailed Paginated List)
# ============================================================

@app.on_message(filters.command("list") & filters.private)
async def list_detailed_cmd(client: Client, message: Message):
    try:
        if not await is_admin(message.from_user.id):
            await message.reply("❌ Sirf owner/admin ye command use kar sakta hai.")
            return

        channels = await channels_col.find({"status": "active"}).to_list(length=None)
        if not channels:
            await message.reply("📭 Koi verified channel nahi hai.")
            return

        total = len(channels)
        total_pages = (total + LIST_PAGE_SIZE - 1) // LIST_PAGE_SIZE

        for page in range(total_pages):
            start = page * LIST_PAGE_SIZE
            end = min(start + LIST_PAGE_SIZE, total)
            page_channels = channels[start:end]

            text = f"📋 **Verified Channels** (Page {page + 1}/{total_pages})\n\n"

            for i, ch in enumerate(page_channels, start + 1):
                privacy = "🔒 Private" if ch.get("is_private", True) else "🌐 Public"
                bio = ch.get("bio", "") or "No bio"
                bio_preview = bio[:80] + "..." if len(bio) > 80 else bio

                if ch.get("username"):
                    url_text = f"@{ch['username']}"
                else:
                    url_text = "Private Channel"

                verified_at = ch.get("verified_at", "N/A")
                if isinstance(verified_at, datetime):
                    verified_at = verified_at.strftime("%d %b %Y")

                text += (
                    f"━━━ {i} ━━━━━━━━━━━━━━━━━━\n"
                    f"📢 **{ch.get('title', 'Unknown')}**\n"
                    f"📝 {bio_preview}\n"
                    f"🔗 {url_text} | {privacy}\n"
                    f"🆔 `{ch['channel_id']}`\n"
                    f"📅 Verified: {verified_at}\n\n"
                )

            text += f"📊 **Total:** {total} channels | Page {page + 1}/{total_pages}"

            for part in split_message(text):
                await message.reply(part)

    except Exception as e:
        logger.error(f"List error: {e}")
        await message.reply(f"❌ Error: {e}")


# ============================================================
# /broadcast COMMAND
# ============================================================

@app.on_message(filters.command("broadcast") & filters.private)
async def broadcast_command(client: Client, message: Message):
    try:
        if not await is_admin(message.from_user.id):
            await message.reply("❌ Sirf owner/admin ye command use kar sakta hai.")
            return

        if not message.reply_to_message:
            await message.reply(
                "❌ **Kisi message ko reply karke /broadcast use karo.**\n\n"
                "**Steps:**\n1. Message bhejo\n2. Reply karo `/broadcast`"
            )
            return

        reply_msg = message.reply_to_message
        total_users = await users_col.count_documents({})

        if total_users == 0:
            await message.reply("❌ Koi user nahi hai database mein.")
            return

        preview = ""
        if reply_msg.text:
            preview = reply_msg.text[:200]
        elif reply_msg.caption:
            preview = reply_msg.caption[:200]
        else:
            preview = f"[{reply_msg.media or 'Media'}]"

        confirm_msg = await message.reply(
            f"📤 **Broadcast Confirmation**\n\n"
            f"📝 **Preview:**\n{preview}\n\n"
            f"👥 **Total Users:** {total_users}\n\n"
            f"Kya broadcast karna hai?",
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

        try:
            broadcast_msg = await client.get_messages(state["chat_id"], reply_msg_id)
        except Exception as e:
            await callback.message.edit_text(f"❌ Original message nahi mila: {e}")
            return

        start_time = time.time()
        total_users = await users_col.count_documents({})
        success = failed = blocked = 0

        progress_msg = await callback.message.edit_text(
            f"📤 **Broadcasting...**\n\nProgress: 0/{total_users}\n"
            f"✅ Sent: 0\n⏱️ Time: 0s\n❌ Failed: 0\n🚫 Blocked: 0"
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
                        f"📤 **Broadcasting...**\n\nProgress: {processed}/{total_users}\n"
                        f"✅ Sent: {success}\n⏱️ Time: {elapsed}\n"
                        f"❌ Failed: {failed}\n🚫 Blocked: {blocked}"
                    )
                except Exception:
                    pass
            await asyncio.sleep(0.04)

        elapsed = format_time_delta(time.time() - start_time)
        await progress_msg.edit_text(
            f"✅ **Broadcast Complete!**\n\n👥 Total: {total_users}\n"
            f"✅ Sent: {success}\n⏱️ Time: {elapsed}\n"
            f"❌ Failed: {failed}\n🚫 Blocked: {blocked}"
        )

        broadcast_state.pop(f"bc_{reply_msg_id}", None)
        await log_action("broadcast", callback.from_user.id,
                          f"Sent:{success} Failed:{failed} Blocked:{blocked}")

    except Exception as e:
        logger.error(f"Broadcast confirm error: {e}")
        await callback.message.edit_text(f"❌ Broadcast Error: {e}")


@app.on_callback_query(filters.regex("^bc_cancel$"))
async def broadcast_cancel(client: Client, callback: CallbackQuery):
    try:
        if not await is_admin(callback.from_user.id):
            await callback.answer("❌ Access denied!", show_alert=True)
            return
        await callback.message.edit_text("❌ **Broadcast cancelled.**")
    except Exception as e:
        logger.error(f"BC cancel error: {e}")


# ============================================================
# ADVERTISEMENT COMMANDS
# ============================================================

@app.on_message(filters.command("addad") & filters.private)
async def add_advertisement(client: Client, message: Message):
    try:
        if not await is_admin(message.from_user.id):
            await message.reply("❌ Sirf owner/admin.")
            return

        args = message.text.split(None, 4)
        if len(args) < 5:
            await message.reply(
                "❌ **Usage:** `/addad slot type url button_text`\n\n"
                "**Slots:** 1-6 | **Types:** `request`, `normal`, `external`\n\n"
                "**Example:** `/addad 1 external https://google.com Google`"
            )
            return

        try:
            slot = int(args[1])
        except ValueError:
            await message.reply("❌ Slot 1-6 hona chahiye.")
            return

        if slot < 1 or slot > 6:
            await message.reply("❌ Slot 1-6 ke beech hona chahiye.")
            return

        link_type = args[2].lower()
        if link_type not in ["request", "normal", "external"]:
            await message.reply("❌ Type: `request`, `normal`, ya `external`")
            return

        url = args[3]
        button_text = args[4]

        await ads_col.update_one(
            {"slot": slot},
            {"$set": {"slot": slot, "type": link_type, "url": url,
                      "button_text": button_text, "active": True,
                      "clicks": 0, "impressions": 0,
                      "created_at": datetime.utcnow(), "created_by": message.from_user.id}},
            upsert=True
        )

        await message.reply(
            f"✅ **Ad Added!**\n\n📍 Slot: {slot}\n🔗 Type: {link_type}\n📝 Button: {button_text}"
        )
    except Exception as e:
        logger.error(f"Add ad error: {e}")
        await message.reply(f"❌ Error: {e}")


@app.on_message(filters.command("removead") & filters.private)
async def remove_advertisement(client: Client, message: Message):
    try:
        if not await is_admin(message.from_user.id):
            await message.reply("❌ Sirf owner/admin.")
            return
        if len(message.command) < 2:
            await message.reply("❌ **Usage:** `/removead slot_number`")
            return
        try:
            slot = int(message.command[1])
        except ValueError:
            await message.reply("❌ Invalid slot.")
            return
        result = await ads_col.delete_one({"slot": slot})
        if result.deleted_count > 0:
            await message.reply(f"✅ Slot {slot} se ad remove ho gaya.")
        else:
            await message.reply(f"❌ Slot {slot} mein koi ad nahi.")
    except Exception as e:
        await message.reply(f"❌ Error: {e}")


@app.on_message(filters.command("listads") & filters.private)
async def list_ads(client: Client, message: Message):
    try:
        if not await is_admin(message.from_user.id):
            await message.reply("❌ Sirf owner/admin.")
            return
        ads = await ads_col.find().sort("slot", 1).to_list(length=None)
        if not ads:
            await message.reply("📭 Koi advertisement nahi hai.")
            return
        text = "📢 **Advertisements:**\n\n"
        for ad in ads:
            status = "✅" if ad.get("active") else "❌"
            text += (
                f"**Slot {ad['slot']}:** {status}\n"
                f"  📝 {ad.get('button_text', 'N/A')}\n"
                f"  🔗 {ad.get('url', 'N/A')}\n"
                f"  👁️ Impressions: {ad.get('impressions', 0)}\n\n"
            )
        await message.reply(text)
    except Exception as e:
        await message.reply(f"❌ Error: {e}")


@app.on_message(filters.command("adstats") & filters.private)
async def ad_stats(client: Client, message: Message):
    try:
        if not await is_admin(message.from_user.id):
            await message.reply("❌ Sirf owner/admin.")
            return
        text = "📊 **Ad Statistics:**\n\n"
        total = 0
        async for ad in ads_col.find().sort("slot", 1):
            imp = ad.get("impressions", 0)
            total += imp
            text += f"Slot {ad['slot']}: {ad.get('button_text', 'N/A')} - 👁️ {imp}\n"
        text += f"\n**Total Impressions:** {total}"
        await message.reply(text)
    except Exception as e:
        await message.reply(f"❌ Error: {e}")
# ============================================================
# FORCE JOIN MANAGEMENT (Private/Public Detection)
# ============================================================

@app.on_message(filters.command("addfj") & filters.private)
async def add_force_join(client: Client, message: Message):
    try:
        if not await is_admin(message.from_user.id):
            await message.reply("❌ Sirf owner/admin.")
            return

        if len(message.command) < 2:
            await message.reply("❌ **Usage:** `/addfj channel_id`")
            return

        try:
            channel_id = int(message.command[1])
        except ValueError:
            await message.reply("❌ Invalid channel ID.")
            return

        try:
            chat = await client.get_chat(channel_id)
        except Exception as e:
            await message.reply(f"❌ Channel access error: {e}")
            return

        is_private = chat.username is None

        if not is_private:
            # Public channel → auto set type "join"
            invite_url = f"https://t.me/{chat.username}"

            channel_data = {
                "channel_id": channel_id, "title": chat.title,
                "username": chat.username, "invite_url": invite_url,
                "type": "join", "is_private": False
            }

            await settings_col.update_one(
                {"_id": "force_join"},
                {"$addToSet": {"channels": channel_data}},
                upsert=True
            )

            await message.reply(
                f"✅ **Force Join Added!**\n\n"
                f"📢 {chat.title}\n🌐 Public | 📥 Type: Join\n"
                f"🔗 {invite_url}"
            )
        else:
            # Private channel → ask Request or Join
            fj_pending[f"fj_{channel_id}"] = {
                "channel_id": channel_id,
                "title": chat.title,
                "username": chat.username
            }

            await message.reply(
                f"🔒 **Private Channel Detected**\n\n"
                f"📢 {chat.title}\n\n"
                f"Force join ka type choose karo:",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔐 Request", callback_data=f"fj_request_{channel_id}"),
                     InlineKeyboardButton("📥 Join", callback_data=f"fj_join_{channel_id}")]
                ])
            )

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
        fj_type = parts[1]  # "request" or "join"
        channel_id = int(parts[2])

        pending = fj_pending.get(f"fj_{channel_id}")
        if not pending:
            await callback.answer("❌ Expired! /addfj dubara use karo.", show_alert=True)
            return

        # Create invite link based on type
        try:
            if fj_type == "request":
                invite = await client.create_chat_invite_link(
                    chat_id=channel_id,
                    creates_join_request=True,
                    name=f"force_request_{channel_id}"
                )
            else:
                invite = await client.create_chat_invite_link(
                    chat_id=channel_id,
                    creates_join_request=False,
                    name=f"force_join_{channel_id}"
                )

            invite_url = invite.invite_link
        except Exception as e:
            await callback.message.edit_text(f"❌ Invite link create fail: {e}")
            return

        channel_data = {
            "channel_id": channel_id,
            "title": pending["title"],
            "username": pending["username"],
            "invite_url": invite_url,
            "type": fj_type,
            "is_private": True
        }

        await settings_col.update_one(
            {"_id": "force_join"},
            {"$addToSet": {"channels": channel_data}},
            upsert=True
        )

        fj_pending.pop(f"fj_{channel_id}", None)

        type_text = "🔐 Request (Bot auto-approve NAHI karega)" if fj_type == "request" else "📥 Direct Join"

        await callback.message.edit_text(
            f"✅ **Force Join Added!**\n\n"
            f"📢 {pending['title']}\n"
            f"🔒 Private | {type_text}"
        )

    except Exception as e:
        logger.error(f"FJ callback error: {e}")
        await callback.message.edit_text(f"❌ Error: {e}")


@app.on_message(filters.command("removefj") & filters.private)
async def remove_force_join(client: Client, message: Message):
    try:
        if not await is_admin(message.from_user.id):
            await message.reply("❌ Sirf owner/admin.")
            return
        if len(message.command) < 2:
            await message.reply("❌ **Usage:** `/removefj channel_id`")
            return
        try:
            channel_id = int(message.command[1])
        except ValueError:
            await message.reply("❌ Invalid channel ID.")
            return

        await settings_col.update_one(
            {"_id": "force_join"},
            {"$pull": {"channels": {"channel_id": channel_id}}}
        )
        await message.reply(f"✅ Force join channel `{channel_id}` removed!")
    except Exception as e:
        await message.reply(f"❌ Error: {e}")


@app.on_message(filters.command("listfj") & filters.private)
async def list_force_join(client: Client, message: Message):
    try:
        if not await is_admin(message.from_user.id):
            await message.reply("❌ Sirf owner/admin.")
            return
        channels = await get_force_join_channels()
        if not channels:
            await message.reply("📭 Koi force join channel nahi hai.")
            return
        text = "📋 **Force Join Channels:**\n\n"
        for i, ch in enumerate(channels, 1):
            fj_type = ch.get("type", "join")
            privacy = "🔒 Private" if ch.get("is_private") else "🌐 Public"
            type_text = "🔐 Request" if fj_type == "request" else "📥 Join"
            text += (
                f"{i}. **{ch.get('title', 'Unknown')}**\n"
                f"   {privacy} | {type_text}\n"
                f"   🆔 `{ch['channel_id']}`\n\n"
            )
        await message.reply(text)
    except Exception as e:
        await message.reply(f"❌ Error: {e}")


# ============================================================
# USER MANAGEMENT
# ============================================================

@app.on_message(filters.command("ban") & filters.private)
async def ban_user(client: Client, message: Message):
    try:
        if not await is_admin(message.from_user.id):
            await message.reply("❌ Sirf owner/admin.")
            return
        if len(message.command) < 2:
            await message.reply("❌ **Usage:** `/ban user_id`")
            return
        target_id = int(message.command[1])
        if target_id == OWNER_ID:
            await message.reply("❌ Owner ko ban nahi kar sakte!")
            return
        await users_col.update_one({"user_id": target_id}, {"$set": {"is_banned": True}}, upsert=True)
        await message.reply(f"✅ User `{target_id}` banned!")
        await log_action("user_banned", message.from_user.id, f"Target: {target_id}")
    except ValueError:
        await message.reply("❌ Invalid user ID.")
    except Exception as e:
        await message.reply(f"❌ Error: {e}")


@app.on_message(filters.command("unban") & filters.private)
async def unban_user(client: Client, message: Message):
    try:
        if not await is_admin(message.from_user.id):
            await message.reply("❌ Sirf owner/admin.")
            return
        if len(message.command) < 2:
            await message.reply("❌ **Usage:** `/unban user_id`")
            return
        target_id = int(message.command[1])
        await users_col.update_one({"user_id": target_id}, {"$set": {"is_banned": False}})
        await message.reply(f"✅ User `{target_id}` unbanned!")
    except ValueError:
        await message.reply("❌ Invalid user ID.")
    except Exception as e:
        await message.reply(f"❌ Error: {e}")


@app.on_message(filters.command("userinfo") & filters.private)
async def user_info(client: Client, message: Message):
    try:
        if not await is_admin(message.from_user.id):
            await message.reply("❌ Sirf owner/admin.")
            return
        if len(message.command) < 2:
            await message.reply("❌ **Usage:** `/userinfo user_id`")
            return
        target_id = int(message.command[1])
        user_doc = await users_col.find_one({"user_id": target_id})
        if not user_doc:
            await message.reply("❌ User not found.")
            return
        ban_status = "🚫 Banned" if user_doc.get("is_banned") else "✅ Active"
        await message.reply(
            f"👤 **User Info:**\n\n"
            f"🆔 `{target_id}`\n"
            f"👤 {user_doc.get('first_name', 'N/A')}\n"
            f"📛 @{user_doc.get('username', 'N/A')}\n"
            f"📅 Joined: {user_doc.get('joined_at', 'N/A')}\n"
            f"🔍 Searches: {user_doc.get('searches_count', 0)}\n"
            f"📊 Status: {ban_status}"
        )
    except ValueError:
        await message.reply("❌ Invalid user ID.")
    except Exception as e:
        await message.reply(f"❌ Error: {e}")

# ============================================================
# ADMIN MANAGEMENT (Owner Only)
# ============================================================

@app.on_message(filters.command("addmin") & filters.private)
async def add_admin(client: Client, message: Message):
    try:
        if not await is_owner(message.from_user.id):
            await message.reply("❌ Sirf owner ye command use kar sakta hai.")
            return
        if len(message.command) < 2:
            await message.reply("❌ **Usage:** `/addmin user_id` ya `/addmin @username`")
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
            await message.reply(f"❌ User resolve error: {e}")
            return

        if target_id == OWNER_ID:
            await message.reply("ℹ️ Owner already has full access.")
            return

        existing = await admins_col.find_one({"user_id": target_id})
        if existing:
            await message.reply("⚠️ Ye user pehle se admin hai!")
            return

        await admins_col.insert_one({
            "user_id": target_id, "name": target_name,
            "username": target_username, "added_by": message.from_user.id,
            "added_at": datetime.utcnow()
        })

        await message.reply(
            f"✅ **Admin Added!**\n\n👤 {target_name}\n"
            f"📛 @{target_username or 'N/A'}\n🆔 `{target_id}`"
        )
        await log_action("admin_added", message.from_user.id, f"{target_id}")
    except Exception as e:
        await message.reply(f"❌ Error: {e}")


@app.on_message(filters.command("radmin") & filters.private)
async def remove_admin(client: Client, message: Message):
    try:
        if not await is_owner(message.from_user.id):
            await message.reply("❌ Sirf owner.")
            return
        if len(message.command) < 2:
            await message.reply("❌ **Usage:** `/radmin user_id` ya `/radmin @username`")
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
            await message.reply(f"✅ Admin `{target_id}` removed!")
        else:
            await message.reply(f"❌ User `{target_id}` admin nahi hai.")
    except Exception as e:
        await message.reply(f"❌ Error: {e}")


@app.on_message(filters.command("ladmin") & filters.private)
async def list_admins(client: Client, message: Message):
    try:
        if not await is_owner(message.from_user.id):
            await message.reply("❌ Sirf owner.")
            return

        admins = await admins_col.find().to_list(length=None)
        text = f"👑 **Admin List:**\n\n1. 👑 **Owner**\n   🆔 `{OWNER_ID}`\n\n"

        for i, admin in enumerate(admins, 2):
            added_at = admin.get("added_at", "N/A")
            if isinstance(added_at, datetime):
                added_at = added_at.strftime("%d %b %Y %H:%M")
            text += (
                f"{i}. 🛡️ **{admin.get('name', 'Unknown')}**\n"
                f"   📛 @{admin.get('username', 'N/A')}\n"
                f"   🆔 `{admin['user_id']}`\n"
                f"   📅 Added: {added_at}\n\n"
            )

        text += f"**Total: {len(admins) + 1}** (including owner)"
        await message.reply(text)
    except Exception as e:
        await message.reply(f"❌ Error: {e}")


# ============================================================
# STATISTICS
# ============================================================

@app.on_message(filters.command("stats") & filters.private)
async def bot_stats(client: Client, message: Message):
    try:
        if not await is_admin(message.from_user.id):
            await message.reply("❌ Sirf owner/admin.")
            return

        total_users = await users_col.count_documents({})
        banned = await users_col.count_documents({"is_banned": True})
        channels = await channels_col.count_documents({"status": "active"})
        admins = await admins_col.count_documents({})
        active_ads = await ads_col.count_documents({"active": True})

        today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        today_users = await users_col.count_documents({"joined_at": {"$gte": today}})
        today_searches = await logs_col.count_documents({"action": "search", "timestamp": {"$gte": today}})

        await message.reply(
            f"📊 **Bot Statistics:**\n\n"
            f"👥 Users: {total_users}\n🚫 Banned: {banned}\n"
            f"📢 Channels: {channels}\n🛡️ Admins: {admins + 1}\n"
            f"📢 Ads: {active_ads}\n\n"
            f"📅 **Today:**\n👤 New: {today_users}\n🔍 Searches: {today_searches}"
        )
    except Exception as e:
        await message.reply(f"❌ Error: {e}")


@app.on_message(filters.command("channelstats") & filters.private)
async def channel_stats(client: Client, message: Message):
    try:
        if not await is_admin(message.from_user.id):
            await message.reply("❌ Sirf owner/admin.")
            return
        if len(message.command) < 2:
            await message.reply("❌ **Usage:** `/channelstats channel_id`")
            return
        channel_id = int(message.command[1])
        ch = await channels_col.find_one({"channel_id": channel_id})
        if not ch:
            await message.reply("❌ Channel not found.")
            return
        links = await invite_links_col.count_documents({"channel_id": channel_id})
        privacy = "🔒 Private" if ch.get("is_private") else "🌐 Public"
        await message.reply(
            f"📊 **Channel Stats:**\n\n📢 {ch.get('title')}\n"
            f"🆔 `{channel_id}`\n🔐 {privacy}\n"
            f"🔗 Links Generated: {links}\n"
            f"📅 Verified: {ch.get('verified_at', 'N/A')}"
        )
    except ValueError:
        await message.reply("❌ Invalid ID.")
    except Exception as e:
        await message.reply(f"❌ Error: {e}")


@app.on_message(filters.command("searchstats") & filters.private)
async def search_stats(client: Client, message: Message):
    try:
        if not await is_admin(message.from_user.id):
            await message.reply("❌ Sirf owner/admin.")
            return

        total = await logs_col.count_documents({"action": "search"})
        today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        today_s = await logs_col.count_documents({"action": "search", "timestamp": {"$gte": today}})
        week = datetime.utcnow() - timedelta(days=7)
        week_s = await logs_col.count_documents({"action": "search", "timestamp": {"$gte": week}})

        await message.reply(
            f"📊 **Search Statistics:**\n\n"
            f"🔍 Total: {total}\n📅 Today: {today_s}\n📆 This Week: {week_s}"
        )
    except Exception as e:
        await message.reply(f"❌ Error: {e}")


# ============================================================
# BACKUP SYSTEM
# ============================================================

async def create_backup_data() -> dict:
    """Collect all data for backup"""
    backup = {}

    for col_name, col in [("users", users_col), ("channels", channels_col),
                            ("advertisements", ads_col), ("settings", settings_col),
                            ("admins", admins_col)]:
        docs = []
        async for doc in col.find():
            doc["_id"] = str(doc["_id"])
            for key, val in doc.items():
                if isinstance(val, datetime):
                    doc[key] = val.isoformat()
            docs.append(doc)
        backup[col_name] = docs

    return backup


async def create_backup_zip(backup_data: dict) -> Tuple[io.BytesIO, str]:
    """Create ZIP file from backup data"""
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
        for name, data in backup_data.items():
            zf.writestr(f"{name}.json", json.dumps(data, indent=2, ensure_ascii=False))
    zip_buffer.seek(0)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"backup_{timestamp}.zip"
    return zip_buffer, filename


@app.on_message(filters.command("backup") & filters.private)
async def backup_command(client: Client, message: Message):
    try:
        if not await is_admin(message.from_user.id):
            await message.reply("❌ Sirf owner/admin.")
            return

        status_msg = await message.reply("📦 **Creating backup...**")
        start_time = time.time()

        backup_data = await create_backup_data()
        zip_buffer, filename = await create_backup_zip(backup_data)

        elapsed = format_time_delta(time.time() - start_time)
        caption = (
            f"📦 **Manual Backup**\n\n"
            f"📅 {datetime.utcnow().strftime('%d %b %Y %H:%M UTC')}\n"
            f"👥 Users: {len(backup_data.get('users', []))}\n"
            f"📢 Channels: {len(backup_data.get('channels', []))}\n"
            f"⏱️ Time: {elapsed}"
        )

        # Send to backup channel
        try:
            sent = await client.send_document(BACKUP_CHANNEL_ID, zip_buffer,
                                               file_name=filename, caption=caption)
            await backup_records_col.insert_one({
                "message_id": sent.id, "channel_id": BACKUP_CHANNEL_ID,
                "filename": filename, "created_at": datetime.utcnow(), "type": "manual"
            })
        except Exception as e:
            logger.warning(f"Backup channel send failed: {e}")
    except Exception as e:
        logger.error(f"Backup error: {e}")
        await message.reply(f"❌ Error: {e}")


# ============================================================
# RESTORE SYSTEM
# ============================================================

@app.on_message(filters.command("restore") & filters.private)
async def restore_command(client: Client, message: Message):
    try:
        if not await is_admin(message.from_user.id):
            await message.reply("❌ Sirf owner/admin.")
            return

        restore_state[message.from_user.id] = {"step": "waiting_file"}
        await message.reply("📦 **Backup ZIP file bhejo...**\n\nCancel: /start")

    except Exception as e:
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
            await message.reply("❌ Sirf ZIP file bhejo.")
            return

        status_msg = await message.reply("📥 **Downloading...**")

        file_path = await message.download()

        # Validate ZIP
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
            try:
                os.remove(file_path)
            except Exception:
                pass
            return

        # Show preview
        preview = "📦 **Backup File Info:**\n\n"
        for col_name, data in backup_data.items():
            preview += f"📁 {col_name}: {len(data)} records\n"

        preview += "\n⚠️ **WARNING:** Current data replace ho jayega!\n\nRestore karna hai?"

        restore_state[user_id] = {
            "step": "waiting_confirm",
            "file_path": file_path,
            "backup_data": backup_data
        }

        await status_msg.edit_text(
            preview,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Yes, Restore", callback_data="restore_yes"),
                 InlineKeyboardButton("❌ Cancel", callback_data="restore_cancel")]
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

        await callback.message.edit_text("🔄 **Restoring...**")

        # Collection mapping
        col_map = {
            "users": users_col,
            "channels": channels_col,
            "advertisements": ads_col,
            "settings": settings_col,
            "admins": admins_col
        }

        restore_report = "📊 **Restore Report:**\n\n"

        for col_name, data in backup_data.items():
            if col_name in col_map:
                col = col_map[col_name]
                await col.delete_many({})
                if data:
                    # Convert _id back
                    for doc in data:
                        if "_id" in doc and col_name != "settings":
                            del doc["_id"]
                        elif "_id" in doc and col_name == "settings":
                            pass  # Keep settings _id
                        # Convert ISO dates back
                        for key, val in doc.items():
                            if isinstance(val, str):
                                try:
                                    doc[key] = datetime.fromisoformat(val)
                                except (ValueError, TypeError):
                                    pass
                    try:
                        await col.insert_many(data)
                    except Exception as e:
                        logger.error(f"Insert error for {col_name}: {e}")
                        # Try one by one
                        for doc in data:
                            try:
                                await col.insert_one(doc)
                            except Exception:
                                pass

                restore_report += f"✅ {col_name}: {len(data)} restored\n"

        clear_all_cache()

        # Channel access check
        restore_report += "\n🔍 **Channel Access Check:**\n\n"
        channels = await channels_col.find({"status": "active"}).to_list(length=None)

        accessible = 0
        not_accessible = 0
        problem_channels = []

        for ch in channels:
            try:
                chat = await client.get_chat(ch["channel_id"])
                # Update bio and title
                await channels_col.update_one(
                    {"channel_id": ch["channel_id"]},
                    {"$set": {"title": chat.title, "bio": chat.description or "",
                              "is_private": chat.username is None,
                              "username": chat.username}}
                )

                try:
                    member = await client.get_chat_member(ch["channel_id"], "me")
                    is_admin_status = member.status in [
                        enums.ChatMemberStatus.ADMINISTRATOR,
                        enums.ChatMemberStatus.OWNER
                    ]
                except Exception:
                    is_admin_status = False

                if is_admin_status:
                    accessible += 1
                else:
                    not_accessible += 1
                    problem_channels.append(f"⚠️ {chat.title} (`{ch['channel_id']}`) - Not admin")

                await asyncio.sleep(0.5)

            except ChannelPrivate:
                not_accessible += 1
                problem_channels.append(
                    f"❌ {ch.get('title', 'Unknown')} (`{ch['channel_id']}`) - No access"
                )
            except Exception as e:
                not_accessible += 1
                problem_channels.append(
                    f"❌ {ch.get('title', 'Unknown')} (`{ch['channel_id']}`) - {str(e)[:50]}"
                )
                await asyncio.sleep(0.5)

        total_ch = accessible + not_accessible
        restore_report += f"✅ Accessible: {accessible}/{total_ch}\n"
        restore_report += f"❌ Not Accessible: {not_accessible}/{total_ch}\n"

        if problem_channels:
            restore_report += "\n❌ **Problem Channels:**\n"
            for p in problem_channels:
                restore_report += f"{p}\n"

        # Send report to admin DM
        for part in split_message(restore_report):
            await client.send_message(user_id, part)

        await callback.message.edit_text("✅ **Restore Complete!** Report DM mein bhej diya.")

        # Notify owner if admin did restore
        if user_id != OWNER_ID:
            try:
                await client.send_message(
                    OWNER_ID,
                    f"⚠️ **Admin Restore Alert!**\n\n"
                    f"👤 Admin `{user_id}` ne database restore kiya.\n"
                    f"📅 {datetime.utcnow().strftime('%d %b %Y %H:%M UTC')}"
                )
            except Exception:
                pass

        # Cleanup
        restore_state.pop(user_id, None)
        try:
            if file_path:
                os.remove(file_path)
        except Exception:
            pass

        await log_action("restore", user_id, f"Channels: {total_ch}, Accessible: {accessible}")

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
            except Exception:
                pass
        await callback.message.edit_text("❌ **Restore cancelled.**")
    except Exception as e:
        logger.error(f"Restore cancel error: {e}")


# ============================================================
# MAINTENANCE COMMAND
# ============================================================

@app.on_message(filters.command("maintenance") & filters.private)
async def maintenance_mode(client: Client, message: Message):
    try:
        if not await is_admin(message.from_user.id):
            await message.reply("❌ Sirf owner/admin.")
            return
        if len(message.command) < 2:
            await message.reply("❌ **Usage:** `/maintenance on` or `/maintenance off`")
            return

        mode = message.command[1].lower()
        if mode == "on":
            await settings_col.update_one(
                {"_id": "maintenance"},
                {"$set": {"enabled": True, "updated_at": datetime.utcnow(),
                          "enabled_by": message.from_user.id}},
                upsert=True
            )
            await message.reply("🔧 **Maintenance mode ON**\n\nSirf admins bot use kar sakte hain.")
        elif mode == "off":
            await settings_col.update_one(
                {"_id": "maintenance"},
                {"$set": {"enabled": False, "updated_at": datetime.utcnow()}},
                upsert=True
            )
            await message.reply("✅ **Maintenance mode OFF**\n\nBot sab users ke liye available hai.")
        else:
            await message.reply("❌ `on` ya `off` use karo.")
    except Exception as e:
        await message.reply(f"❌ Error: {e}")


# ============================================================
# LOGS COMMAND
# ============================================================

@app.on_message(filters.command("logs") & filters.private)
async def view_logs(client: Client, message: Message):
    try:
        if not await is_admin(message.from_user.id):
            await message.reply("❌ Sirf owner/admin.")
            return

        logs = await logs_col.find().sort("timestamp", -1).limit(20).to_list(length=20)
        if not logs:
            await message.reply("📭 No logs found.")
            return

        text = "📋 **Recent Logs (Last 20):**\n\n"
        for log in logs:
            ts = log.get("timestamp", "N/A")
            if isinstance(ts, datetime):
                ts = ts.strftime("%d/%m %H:%M")
            text += (
                f"⏱️ {ts}\n"
                f"📌 {log.get('action', 'N/A')}\n"
                f"👤 {log.get('user_id', 'N/A')}\n"
                f"📝 {log.get('details', '')}\n"
                f"{'─' * 25}\n"
            )

        for part in split_message(text):
            await message.reply(part)
    except Exception as e:
        await message.reply(f"❌ Error: {e}")


# ============================================================
# HELP COMMAND
# ============================================================

@app.on_message(filters.command("help") & (filters.private | filters.group))
async def help_command(client: Client, message: Message):
    try:
        user_id = message.from_user.id

        if await is_owner(user_id):
            text = (
                "👑 **Owner Commands:**\n\n"
                "**Admin Management:**\n"
                "`/addmin user_id/@username` - Add admin\n"
                "`/radmin user_id/@username` - Remove admin\n"
                "`/ladmin` - List admins\n\n"
                "🛡️ **Admin Commands:**\n\n"
                "**Channel:**\n"
                "`/verify channel_id` - Verify channel\n"
                "`/unverify channel_id` - Remove channel\n"
                "`/channels` - Quick list\n"
                "`/list` - Detailed list\n\n"
                "**Force Join:**\n"
                "`/addfj channel_id` - Add force join\n"
                "`/removefj channel_id` - Remove\n"
                "`/listfj` - List force join\n\n"
                "**Broadcast:**\n"
                "`/broadcast` - Reply to msg to broadcast\n\n"
                "**Ads:**\n"
                "`/addad slot type url text` - Add ad\n"
                "`/removead slot` - Remove ad\n"
                "`/listads` - List ads\n"
                "`/adstats` - Ad stats\n\n"
                "**Users:**\n"
                "`/ban user_id` - Ban\n"
                "`/unban user_id` - Unban\n"
                "`/userinfo user_id` - Info\n\n"
                "**Stats:**\n"
                "`/stats` - Bot stats\n"
                "`/channelstats id` - Channel stats\n"
                "`/searchstats` - Search stats\n\n"
                "**System:**\n"
                "`/backup` - Create backup\n"
                "`/restore` - Restore backup\n"
                "`/maintenance on/off` - Maintenance\n"
                "`/logs` - View logs\n"
                "**Search (DM + Group):**\n"
                "`/search keyword` - Search\n"
                "`/anime keyword` - Search\n"
                "`/a keyword` - Search\n"
                "`/s keyword` - Search\n\n"
                "༺═━━━ { ⚜ } ━━━═༻\n"
                "    👑 Developed by RJ\n"
                "༺═━━━ { ⚜ } ━━━═༻"
            )
        elif await is_admin(user_id):
            text = (
                "🛡️ **Admin Commands:**\n\n"
                "**Channel:**\n"
                "`/verify channel_id` - Verify channel\n"
                "`/unverify channel_id` - Remove channel\n"
                "`/channels` - Quick list\n"
                "`/list` - Detailed list\n\n"
                "**Force Join:**\n"
                "`/addfj channel_id` - Add force join\n"
                "`/removefj channel_id` - Remove\n"
                "`/listfj` - List force join\n\n"
                "**Broadcast:**\n"
                "`/broadcast` - Reply to msg to broadcast\n\n"
                "**Ads:**\n"
                "`/addad slot type url text` - Add ad\n"
                "`/removead slot` - Remove ad\n"
                "`/listads` - List ads\n"
                "`/adstats` - Ad stats\n\n"
                "**Users:**\n"
                "`/ban user_id` - Ban\n"
                "`/unban user_id` - Unban\n"
                "`/userinfo user_id` - Info\n\n"
                "**Stats:**\n"
                "`/stats` - Bot stats\n"
                "`/channelstats id` - Channel stats\n"
                "`/searchstats` - Search stats\n\n"
                "**System:**\n"
                "`/backup` - Create backup\n"
                "`/restore` - Restore backup\n"
                "`/maintenance on/off` - Maintenance\n"
                "`/logs` - View logs\n"
                "**Search (DM + Group):**\n"
                "`/search keyword` - Search\n"
                "`/anime keyword` - Search\n"
                "`/a keyword` - Search\n"
                "`/s keyword` - Search\n\n"
                "༺═━━━ { ⚜ } ━━━═༻\n"
                "    👑 Developed by RJ\n"
                "༺═━━━ { ⚜ } ━━━═༻"
            )
        else:
            text = (
                "🔍 **Search Bot Help:**\n\n"
                "**📌 DM mein:**\n"
                "Koi bhi keyword type karo\n"
                "Example: `Naruto`\n\n"
                "**📌 Group mein:**\n"
                "/search **carnitrix**\n"
                "/anime Fairy Tail\n"
                "/a Dragon Ball\n"
                "/s One Piece\n\n"
                "༺═━━━ { ⚜ } ━━━═༻\n"
                "    👑 Developed by RJ\n"
                "༺═━━━ { ⚜ } ━━━═༻"
            )

        await message.reply(text)
    except Exception as e:
        logger.error(f"Help error: {e}")


# ============================================================
# SCHEDULED TASKS
# ============================================================

async def cleanup_expired_links():
    """Revoke expired invite links and delete search result messages"""
    try:
        now = datetime.utcnow()

        # 1. Revoke expired invite links (30 min passed)
        expired_links = await invite_links_col.find({
            "status": "active",
            "expires_at": {"$lte": now}
        }).to_list(length=100)

        for link in expired_links:
            try:
                await app.revoke_chat_invite_link(
                    link["channel_id"], link["invite_link"]
                )
            except Exception as e:
                logger.debug(f"Revoke error: {e}")

            await invite_links_col.update_one(
                {"_id": link["_id"]},
                {"$set": {"status": "expired"}}
            )

        # 2. Delete expired search result messages
        expired_results = await search_results_col.find({
            "expires_at": {"$lte": now}
        }).to_list(length=100)

        for result in expired_results:
            try:
                await app.delete_messages(
                    result["chat_id"], result["message_id"]
                )
            except MessageDeleteForbidden:
                pass
            except Exception as e:
                logger.debug(f"Delete msg error: {e}")

            await search_results_col.delete_one({"_id": result["_id"]})

        if expired_links or expired_results:
            logger.info(
                f"Cleanup: {len(expired_links)} links revoked, "
                f"{len(expired_results)} messages deleted"
            )

    except Exception as e:
        logger.error(f"Cleanup error: {e}")


async def cleanup_old_data():
    """Delete old logs and expired records"""
    try:
        # Delete logs older than 30 days
        cutoff = datetime.utcnow() - timedelta(days=30)
        result = await logs_col.delete_many({"timestamp": {"$lt": cutoff}})
        if result.deleted_count > 0:
            logger.info(f"Cleaned {result.deleted_count} old logs")

        # Delete expired invite records older than 7 days
        link_cutoff = datetime.utcnow() - timedelta(days=7)
        result = await invite_links_col.delete_many({
            "status": "expired",
            "expires_at": {"$lt": link_cutoff}
        })
        if result.deleted_count > 0:
            logger.info(f"Cleaned {result.deleted_count} old invite records")

    except Exception as e:
        logger.error(f"Old data cleanup error: {e}")


async def update_channel_bios():
    """Update all channel bios and titles (runs every 14 hours)"""
    try:
        channels = await channels_col.find({"status": "active"}).to_list(length=None)
        if not channels:
            return

        updated = 0
        errors = 0
        deactivated = 0

        logger.info(f"Bio update started for {len(channels)} channels")

        for ch in channels:
            try:
                chat = await app.get_chat(ch["channel_id"])

                new_bio = chat.description or ""
                new_title = chat.title or ""
                new_username = chat.username
                new_is_private = chat.username is None

                # Check if anything changed
                changes = {}
                if n
        # Send to admin
        zip_buffer.seek(0)
        await client.send_document(message.from_user.id, zip_buffer,
                                    file_name=filename, caption=caption)
        await status_msg.delete()
        await log_action("backup", message.from_user.id, filename)
    except Exception as e:
        logger.error(f"Backup error: {e}")
        await message.reply(f"❌ Error: {e}")


# ============================================================
# RESTORE SYSTEM
# ============================================================

@app.on_message(filters.command("restore") & filters.private)
async def restore_command(client: Client, message: Message):
    try:
        if not await is_admin(message.from_user.id):
            await message.reply("❌ Sirf owner/admin.")
            return

        restore_state[message.from_user.id] = {"step": "waiting_file"}
        await message.reply("📦 **Backup ZIP file bhejo...**\n\nCancel: /start")

    except Exception as e:
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
            await message.reply("❌ Sirf ZIP file bhejo.")
            return

        status_msg = await message.reply("📥 **Downloading...**")

        file_path = await message.download()

        # Validate ZIP
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
            try:
                os.remove(file_path)
            except Exception:
                pass
            return

        # Show preview
        preview = "📦 **Backup File Info:**\n\n"
        for col_name, data in backup_data.items():
            preview += f"📁 {col_name}: {len(data)} records\n"

        preview += "\n⚠️ **WARNING:** Current data replace ho jayega!\n\nRestore karna hai?"

        restore_state[user_id] = {
            "step": "waiting_confirm",
            "file_path": file_path,
            "backup_data": backup_data
        }

        await status_msg.edit_text(
            preview,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Yes, Restore", callback_data="restore_yes"),
                 InlineKeyboardButton("❌ Cancel", callback_data="restore_cancel")]
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

        await callback.message.edit_text("🔄 **Restoring...**")

        # Collection mapping
        col_map = {
            "users": users_col,
            "channels": channels_col,
            "advertisements": ads_col,
            "settings": settings_col,
            "admins": admins_col
        }

        restore_report = "📊 **Restore Report:**\n\n"

        for col_name, data in backup_data.items():
            if col_name in col_map:
                col = col_map[col_name]
                await col.delete_many({})
                if data:
                    # Convert _id back
                    for doc in data:
                        if "_id" in doc and col_name != "settings":
                            del doc["_id"]
                        elif "_id" in doc and col_name == "settings":
                            pass  # Keep settings _id
                        # Convert ISO dates back
                        for key, val in doc.items():
                            if isinstance(val, str):
                                try:
                                    doc[key] = datetime.fromisoformat(val)
                                except (ValueError, TypeError):
                                    pass
                    try:
                        await col.insert_many(data)
                    except Exception as e:
                        logger.error(f"Insert error for {col_name}: {e}")
                        # Try one by one
                        for doc in data:
                            try:
                                await col.insert_one(doc)
                            except Exception:
                                pass

                restore_report += f"✅ {col_name}: {len(data)} restored\n"

        clear_all_cache()

        # Channel access check
        restore_report += "\n🔍 **Channel Access Check:**\n\n"
        channels = await channels_col.find({"status": "active"}).to_list(length=None)

        accessible = 0
        not_accessible = 0
        problem_channels = []

        for ch in channels:
            try:
                chat = await client.get_chat(ch["channel_id"])
                # Update bio and title
                await channels_col.update_one(
                    {"channel_id": ch["channel_id"]},
                    {"$set": {"title": chat.title, "bio": chat.description or "",
                              "is_private": chat.username is None,
                              "username": chat.username}}
                )

                try:
                    member = await client.get_chat_member(ch["channel_id"], "me")
                    is_admin_status = member.status in [
                        enums.ChatMemberStatus.ADMINISTRATOR,
                        enums.ChatMemberStatus.OWNER
                    ]
                except Exception:
                    is_admin_status = False

                if is_admin_status:
                    accessible += 1
                else:
                    not_accessible += 1
                    problem_channels.append(f"⚠️ {chat.title} (`{ch['channel_id']}`) - Not admin")

                await asyncio.sleep(0.5)

            except ChannelPrivate:
                not_accessible += 1
                problem_channels.append(
                    f"❌ {ch.get('title', 'Unknown')} (`{ch['channel_id']}`) - No access"
                )
            except Exception as e:
                not_accessible += 1
                problem_channels.append(
                    f"❌ {ch.get('title', 'Unknown')} (`{ch['channel_id']}`) - {str(e)[:50]}"
                )
                await asyncio.sleep(0.5)

        total_ch = accessible + not_accessible
        restore_report += f"✅ Accessible: {accessible}/{total_ch}\n"
        restore_report += f"❌ Not Accessible: {not_accessible}/{total_ch}\n"

        if problem_channels:
            restore_report += "\n❌ **Problem Channels:**\n"
            for p in problem_channels:
                restore_report += f"{p}\n"

        # Send report to admin DM
        for part in split_message(restore_report):
            await client.send_message(user_id, part)

        await callback.message.edit_text("✅ **Restore Complete!** Report DM mein bhej diya.")

        # Notify owner if admin did restore
        if user_id != OWNER_ID:
            try:
                await client.send_message(
                    OWNER_ID,
                    f"⚠️ **Admin Restore Alert!**\n\n"
                    f"👤 Admin `{user_id}` ne database restore kiya.\n"
                    f"📅 {datetime.utcnow().strftime('%d %b %Y %H:%M UTC')}"
                )
            except Exception:
                pass

        # Cleanup
        restore_state.pop(user_id, None)
        try:
            if file_path:
                os.remove(file_path)
        except Exception:
            pass

        await log_action("restore", user_id, f"Channels: {total_ch}, Accessible: {accessible}")

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
            except Exception:
                pass
        await callback.message.edit_text("❌ **Restore cancelled.**")
    except Exception as e:
        logger.error(f"Restore cancel error: {e}")


# ============================================================
# MAINTENANCE COMMAND
# ============================================================

@app.on_message(filters.command("maintenance") & filters.private)
async def maintenance_mode(client: Client, message: Message):
    try:
        if not await is_admin(message.from_user.id):
            await message.reply("❌ Sirf owner/admin.")
            return
        if len(message.command) < 2:
            await message.reply("❌ **Usage:** `/maintenance on` or `/maintenance off`")
            return

        mode = message.command[1].lower()
        if mode == "on":
            await settings_col.update_one(
                {"_id": "maintenance"},
                {"$set": {"enabled": True, "updated_at": datetime.utcnow(),
                          "enabled_by": message.from_user.id}},
                upsert=True
            )
            await message.reply("🔧 **Maintenance mode ON**\n\nSirf admins bot use kar sakte hain.")
        elif mode == "off":
            await settings_col.update_one(
                {"_id": "maintenance"},
                {"$set": {"enabled": False, "updated_at": datetime.utcnow()}},
                upsert=True
            )
            await message.reply("✅ **Maintenance mode OFF**\n\nBot sab users ke liye available hai.")
        else:
            await message.reply("❌ `on` ya `off` use karo.")
    except Exception as e:
        await message.reply(f"❌ Error: {e}")


# ============================================================
# LOGS COMMAND
# ============================================================

@app.on_message(filters.command("logs") & filters.private)
async def view_logs(client: Client, message: Message):
    try:
        if not await is_admin(message.from_user.id):
            await message.reply("❌ Sirf owner/admin.")
            return

        logs = await logs_col.find().sort("timestamp", -1).limit(20).to_list(length=20)
        if not logs:
            await message.reply("📭 No logs found.")
            return

        text = "📋 **Recent Logs (Last 20):**\n\n"
        for log in logs:
            ts = log.get("timestamp", "N/A")
            if isinstance(ts, datetime):
                ts = ts.strftime("%d/%m %H:%M")
            text += (
                f"⏱️ {ts}\n"
                f"📌 {log.get('action', 'N/A')}\n"
                f"👤 {log.get('user_id', 'N/A')}\n"
                f"📝 {log.get('details', '')}\n"
                f"{'─' * 25}\n"
            )

        for part in split_message(text):
            await message.reply(part)
    except Exception as e:
        await message.reply(f"❌ Error: {e}")


# ============================================================
# HELP COMMAND
# ============================================================

@app.on_message(filters.command("help") & (filters.private | filters.group))
async def help_command(client: Client, message: Message):
    try:
        user_id = message.from_user.id

        if await is_owner(user_id):
            text = (
                "👑 **Owner Commands:**\n\n"
                "**Admin Management:**\n"
                "`/addmin user_id/@username` - Add admin\n"
                "`/radmin user_id/@username` - Remove admin\n"
                "`/ladmin` - List admins\n\n"
                "🛡️ **Admin Commands:**\n\n"
                "**Channel:**\n"
                "`/verify channel_id` - Verify channel\n"
                "`/unverify channel_id` - Remove channel\n"
                "`/channels` - Quick list\n"
                "`/list` - Detailed list\n\n"
                "**Force Join:**\n"
                "`/addfj channel_id` - Add force join\n"
                "`/removefj channel_id` - Remove\n"
                "`/listfj` - List force join\n\n"
                "**Broadcast:**\n"
                "`/broadcast` - Reply to msg to broadcast\n\n"
                "**Ads:**\n"
                "`/addad slot type url text` - Add ad\n"
                "`/removead slot` - Remove ad\n"
                "`/listads` - List ads\n"
                "`/adstats` - Ad stats\n\n"
                "**Users:**\n"
                "`/ban user_id` - Ban\n"
                "`/unban user_id` - Unban\n"
                "`/userinfo user_id` - Info\n\n"
                "**Stats:**\n"
                "`/stats` - Bot stats\n"
                "`/channelstats id` - Channel stats\n"
                "`/searchstats` - Search stats\n\n"
                "**System:**\n"
                "`/backup` - Create backup\n"
                "`/restore` - Restore backup\n"
                "`/maintenance on/off` - Maintenance\n"
                "`/logs` - View logs\n"
                "**Search (DM + Group):**\n"
                "`/search keyword` - Search\n"
                "`/anime keyword` - Search\n"
                "`/a keyword` - Search\n"
                "`/s keyword` - Search\n\n"
                "༺═━━━ { ⚜ } ━━━═༻\n"
                "    👑 Developed by RJ\n"
                "༺═━━━ { ⚜ } ━━━═༻"
            )
        elif await is_admin(user_id):
            text = (
                "🛡️ **Admin Commands:**\n\n"
                "**Channel:**\n"
                "`/verify channel_id` - Verify channel\n"
                "`/unverify channel_id` - Remove channel\n"
                "`/channels` - Quick list\n"
                "`/list` - Detailed list\n\n"
                "**Force Join:**\n"
                "`/addfj channel_id` - Add force join\n"
                "`/removefj channel_id` - Remove\n"
                "`/listfj` - List force join\n\n"
                "**Broadcast:**\n"
                "`/broadcast` - Reply to msg to broadcast\n\n"
                "**Ads:**\n"
                "`/addad slot type url text` - Add ad\n"
                "`/removead slot` - Remove ad\n"
                "`/listads` - List ads\n"
                "`/adstats` - Ad stats\n\n"
                "**Users:**\n"
                "`/ban user_id` - Ban\n"
                "`/unban user_id` - Unban\n"
                "`/userinfo user_id` - Info\n\n"
                "**Stats:**\n"
                "`/stats` - Bot stats\n"
                "`/channelstats id` - Channel stats\n"
                "`/searchstats` - Search stats\n\n"
                "**System:**\n"
                "`/backup` - Create backup\n"
                "`/restore` - Restore backup\n"
                "`/maintenance on/off` - Maintenance\n"
                "`/logs` - View logs\n"
                "**Search (DM + Group):**\n"
                "`/search keyword` - Search\n"
                "`/anime keyword` - Search\n"
                "`/a keyword` - Search\n"
                "`/s keyword` - Search\n\n"
                "༺═━━━ { ⚜ } ━━━═༻\n"
                "    👑 Developed by RJ\n"
                "༺═━━━ { ⚜ } ━━━═༻"
            )
        else:
            text = (
                "🔍 **Search Bot Help:**\n\n"
                "**📌 DM mein:**\n"
                "Koi bhi keyword type karo\n"
                "Example: `Naruto`\n\n"
                "**📌 Group mein:**\n"
                "/search **carnitrix**\n"
                "/anime Fairy Tail\n"
                "/a Dragon Ball\n"
                "/s One Piece\n\n"
                "༺═━━━ { ⚜ } ━━━═༻\n"
                "    👑 Developed by RJ\n"
                "༺═━━━ { ⚜ } ━━━═༻"
            )

        await message.reply(text)
    except Exception as e:
        logger.error(f"Help error: {e}")


# ============================================================
# SCHEDULED TASKS
# ============================================================

async def cleanup_expired_links():
    """Revoke expired invite links and delete search result messages"""
    try:
        now = datetime.utcnow()

        # 1. Revoke expired invite links (30 min passed)
        expired_links = await invite_links_col.find({
            "status": "active",
            "expires_at": {"$lte": now}
        }).to_list(length=100)

        for link in expired_links:
            try:
                await app.revoke_chat_invite_link(
                    link["channel_id"], link["invite_link"]
                )
            except Exception as e:
                logger.debug(f"Revoke error: {e}")

            await invite_links_col.update_one(
                {"_id": link["_id"]},
                {"$set": {"status": "expired"}}
            )

        # 2. Delete expired search result messages
        expired_results = await search_results_col.find({
            "expires_at": {"$lte": now}
        }).to_list(length=100)

        for result in expired_results:
            try:
                await app.delete_messages(
                    result["chat_id"], result["message_id"]
                )
            except MessageDeleteForbidden:
                pass
            except Exception as e:
                logger.debug(f"Delete msg error: {e}")

            await search_results_col.delete_one({"_id": result["_id"]})

        if expired_links or expired_results:
            logger.info(
                f"Cleanup: {len(expired_links)} links revoked, "
                f"{len(expired_results)} messages deleted"
            )

    except Exception as e:
        logger.error(f"Cleanup error: {e}")


async def cleanup_old_data():
    """Delete old logs and expired records"""
    try:
        # Delete logs older than 30 days
        cutoff = datetime.utcnow() - timedelta(days=30)
        result = await logs_col.delete_many({"timestamp": {"$lt": cutoff}})
        if result.deleted_count > 0:
            logger.info(f"Cleaned {result.deleted_count} old logs")

        # Delete expired invite records older than 7 days
        link_cutoff = datetime.utcnow() - timedelta(days=7)
        result = await invite_links_col.delete_many({
            "status": "expired",
            "expires_at": {"$lt": link_cutoff}
        })
        if result.deleted_count > 0:
            logger.info(f"Cleaned {result.deleted_count} old invite records")

    except Exception as e:
        logger.error(f"Old data cleanup error: {e}")


async def update_channel_bios():
    """Update all channel bios and titles (runs every 14 hours)"""
    try:
        channels = await channels_col.find({"status": "active"}).to_list(length=None)
        if not channels:
            return

        updated = 0
        errors = 0
        deactivated = 0

        logger.info(f"Bio update started for {len(channels)} channels")

        for ch in channels:
            try:
                chat = await app.get_chat(ch["channel_id"])

                                new_bio = chat.description or ""
                new_title = chat.title or ""
                new_username = chat.username
                new_is_private = chat.username is None

                # Check if anything changed
                changes = {}
                if new_bio != ch.get("bio", ""):
                    changes["bio"] = new_bio
                if new_title != ch.get("title", ""):
                    changes["title"] = new_title
                if new_username != ch.get("username"):
                    changes["username"] = new_username
                if new_is_private != ch.get("is_private", True):
                    changes["is_private"] = new_is_private

                if changes:
                    changes["bio_updated_at"] = datetime.utcnow()
                    await channels_col.update_one(
                        {"channel_id": ch["channel_id"]},
                        {"$set": changes}
                    )
                    updated += 1
                    # Clear cache since data changed
                    clear_all_cache()

                await asyncio.sleep(0.5)  # Anti-flood

            except (ChannelPrivate, PeerIdInvalid):
                # Bot removed or channel deleted
                await channels_col.update_one(
                    {"channel_id": ch["channel_id"]},
                    {"$set": {"status": "inactive", "deactivated_at": datetime.utcnow()}}
                )
                deactivated += 1

                # Notify owner
                try:
                    await app.send_message(
                        OWNER_ID,
                        f"⚠️ **Channel Access Lost!**\n\n"
                        f"📢 {ch.get('title', 'Unknown')}\n"
                        f"🆔 `{ch['channel_id']}`\n\n"
                        f"Channel deactivated. Re-verify with `/verify {ch['channel_id']}`"
                    )
                except Exception:
                    pass

            except FloodWait as fw:
                logger.warning(f"FloodWait {fw.value}s in bio update")
                await asyncio.sleep(fw.value + 1)

            except Exception as e:
                errors += 1
                logger.debug(f"Bio update error for {ch.get('channel_id')}: {e}")
                await asyncio.sleep(1)

        logger.info(
            f"Bio update done: {updated} updated, "
            f"{errors} errors, {deactivated} deactivated"
        )

    except Exception as e:
        logger.error(f"Bio update scheduler error: {e}")


async def auto_backup_scheduled():
    """Automatic backup at scheduled times (7:15 AM & 7:30 PM IST)"""
    try:
        logger.info("Auto backup started...")

        backup_data = await create_backup_data()
        zip_buffer, filename = await create_backup_zip(backup_data)

        # Replace 'backup_' with 'auto_backup_' in filename
        filename = filename.replace("backup_", "auto_backup_")

        caption = (
            f"📦 **Auto Backup**\n\n"
            f"📅 {datetime.utcnow().strftime('%d %b %Y %H:%M UTC')}\n"
            f"👥 Users: {len(backup_data.get('users', []))}\n"
            f"📢 Channels: {len(backup_data.get('channels', []))}\n"
            f"🛡️ Admins: {len(backup_data.get('admins', []))}\n"
            f"📢 Ads: {len(backup_data.get('advertisements', []))}"
        )

        # Send to backup channel
        try:
            sent = await app.send_document(
                BACKUP_CHANNEL_ID, zip_buffer,
                file_name=filename, caption=caption
            )

            # Save backup record
            await backup_records_col.insert_one({
                "message_id": sent.id,
                "channel_id": BACKUP_CHANNEL_ID,
                "filename": filename,
                "created_at": datetime.utcnow(),
                "type": "auto"
            })

            logger.info(f"Auto backup sent: {filename}")

        except Exception as e:
            logger.error(f"Auto backup channel send failed: {e}")
            return

        # Retention cleanup: keep only last 4 auto backups
        await backup_retention_cleanup()

    except Exception as e:
        logger.error(f"Auto backup error: {e}")


async def backup_retention_cleanup():
    """Keep only last BACKUP_RETAIN_COUNT auto backups, delete older ones"""
    try:
        # Get all auto backup records sorted by date (newest first)
        all_backups = await backup_records_col.find(
            {"type": "auto"}
        ).sort("created_at", -1).to_list(length=None)

        if len(all_backups) <= BACKUP_RETAIN_COUNT:
            return  # Nothing to delete

        # Backups to delete (older than last 4)
        to_delete = all_backups[BACKUP_RETAIN_COUNT:]

        deleted_count = 0
        for backup in to_delete:
            # Delete message from backup channel
            try:
                await app.delete_messages(
                    backup["channel_id"], backup["message_id"]
                )
                deleted_count += 1
            except MessageDeleteForbidden:
                logger.debug(f"Cannot delete backup msg {backup['message_id']}")
            except Exception as e:
                logger.debug(f"Backup msg delete error: {e}")

            # Delete record from database
            await backup_records_col.delete_one({"_id": backup["_id"]})

        if deleted_count > 0:
            logger.info(f"Backup retention: deleted {deleted_count} old backups")

    except Exception as e:
        logger.error(f"Backup retention error: {e}")


async def cleanup_rate_limiter():
    """Clean rate limiter memory"""
    try:
        now = time.time()
        keys_to_delete = []
        for user_id, timestamps in rate_limiter.items():
            rate_limiter[user_id] = [
                t for t in timestamps if now - t < RATE_LIMIT_WINDOW
            ]
            if not rate_limiter[user_id]:
                keys_to_delete.append(user_id)
        for key in keys_to_delete:
            del rate_limiter[key]
    except Exception as e:
        logger.error(f"Rate limiter cleanup error: {e}")


async def cleanup_search_cache():
    """Clean expired search cache entries"""
    try:
        now = time.time()
        expired_keys = [
            key for key, val in search_cache.items()
            if now - val["time"] >= SEARCH_CACHE_TTL
        ]
        for key in expired_keys:
            del search_cache[key]
        if expired_keys:
            logger.debug(f"Cache cleanup: {len(expired_keys)} entries removed")
    except Exception as e:
        logger.error(f"Cache cleanup error: {e}")


async def check_force_join_channels():
    """Auto-detect private→public changes in force join channels"""
    try:
        force_channels = await get_force_join_channels()
        if not force_channels:
            return

        updated = False
        for ch in force_channels:
            try:
                chat = await app.get_chat(ch["channel_id"])
                current_is_private = chat.username is None

                # If was private with "request" type but now public
                if ch.get("is_private", True) and not current_is_private:
                    # Channel became public → change type to "join"
                    await settings_col.update_one(
                        {"_id": "force_join", "channels.channel_id": ch["channel_id"]},
                        {"$set": {
                            "channels.$.is_private": False,
                            "channels.$.type": "join",
                            "channels.$.username": chat.username,
                            "channels.$.invite_url": f"https://t.me/{chat.username}"
                        }}
                    )
                    updated = True
                    logger.info(
                        f"Force join auto-updated: {ch['channel_id']} "
                        f"private→public, type→join"
                    )

                    # Notify owner
                    try:
                        await app.send_message(
                            OWNER_ID,
                            f"ℹ️ **Force Join Auto-Updated**\n\n"
                            f"📢 {chat.title}\n"
                            f"🔄 Private → Public\n"
                            f"📥 Type changed to: Join"
                        )
                    except Exception:
                        pass

                # If was public but now private
                elif not ch.get("is_private", True) and current_is_private:
                    await settings_col.update_one(
                        {"_id": "force_join", "channels.channel_id": ch["channel_id"]},
                        {"$set": {
                            "channels.$.is_private": True,
                            "channels.$.type": "join",
                            "channels.$.username": None
                        }}
                    )
                    updated = True
                    logger.info(
                        f"Force join auto-updated: {ch['channel_id']} "
                        f"public→private, type→join (default)"
                    )

                    try:
                        await app.send_message(
                            OWNER_ID,
                            f"⚠️ **Force Join Auto-Updated**\n\n"
                            f"📢 {chat.title}\n"
                            f"🔄 Public → Private\n"
                            f"📥 Type set to: Join (default)\n\n"
                            f"Use `/removefj {ch['channel_id']}` and "
                            f"`/addfj {ch['channel_id']}` to change type."
                        )
                    except Exception:
                        pass

                await asyncio.sleep(0.5)

            except Exception as e:
                logger.debug(f"FJ check error for {ch.get('channel_id')}: {e}")

    except Exception as e:
        logger.error(f"Force join check error: {e}")


# ============================================================
# STARTUP
# ============================================================

async def on_startup():
    """Run on bot startup"""
    logger.info("Bot starting up...")

    # Setup database indexes
    await setup_indexes()

    # Cleanup expired links every 2 minutes
    scheduler.add_job(
        cleanup_expired_links,
        IntervalTrigger(minutes=2),
        id="cleanup_links",
        replace_existing=True
    )

    # Clean old data daily
    scheduler.add_job(
        cleanup_old_data,
        IntervalTrigger(hours=24),
        id="cleanup_old_data",
        replace_existing=True
    )

    # Update channel bios every 14 hours
    scheduler.add_job(
        update_channel_bios,
        IntervalTrigger(hours=BIO_UPDATE_INTERVAL),
        id="update_bios",
        replace_existing=True
    )

    # Auto backup: 7:15 AM IST = 01:45 UTC
    scheduler.add_job(
        auto_backup_scheduled,
        CronTrigger(hour=1, minute=45),
        id="backup_morning",
        replace_existing=True
    )

    # Auto backup: 7:30 PM IST = 14:00 UTC
    scheduler.add_job(
        auto_backup_scheduled,
        CronTrigger(hour=14, minute=0),
        id="backup_evening",
        replace_existing=True
    )

    # Clean rate limiter every 5 minutes
    scheduler.add_job(
        cleanup_rate_limiter,
        IntervalTrigger(minutes=5),
        id="cleanup_rate_limiter",
        replace_existing=True
    )

    # Clean search cache every 5 minutes
    scheduler.add_job(
        cleanup_search_cache,
        IntervalTrigger(minutes=5),
        id="cleanup_cache",
        replace_existing=True
    )

    # Check force join channels every 6 hours (private/public changes)
    scheduler.add_job(
        check_force_join_channels,
        IntervalTrigger(hours=6),
        id="check_fj_channels",
        replace_existing=True
    )

    scheduler.start()
    logger.info("All schedulers started")
    logger.info("Bot is ready!")


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    print("=" * 50)
    print("  Telegram Search Bot v2.0")
    print("  Bio & Name Based Search")
    print("  Starting...")
    print("=" * 50)

    loop = asyncio.get_event_loop()

    app.start()
    loop.run_until_complete(on_startup())

    print("Bot is running! Press Ctrl+C to stop.")
    logger.info("Bot is running!")

    try:
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

    