# bot.py - Telegram Search Bot v2.0 (Render Compatible - Fixed)
import os
import sys
import asyncio

# ============================================================
# CRITICAL FIX: Create event loop BEFORE importing pyrogram
# ============================================================
try:
    loop = asyncio.get_event_loop()
except RuntimeError:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

# Now import everything else
import json
import time
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

OWNER_ID = int(os.environ.get("OWNER_ID", "6454751048"))
BACKUP_CHANNEL_ID = int(os.environ.get("BACKUP_CHANNEL_ID", "-1002932260531"))

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

ALL_COMMANDS = [
    "start", "verify", "unverify", "channels", "list",
    "broadcast", "addad", "removead", "listads", "adstats",
    "ban", "unban", "userinfo", "addfj", "removefj", "listfj",
    "stats", "channelstats", "searchstats", "backup", "restore",
    "maintenance", "logs", "addmin", "radmin", "ladmin", "help",
    "search", "anime", "a", "s"
]

# ============================================================
# LOGGING (Render Compatible)
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
        raise RuntimeError("Missing required environment variables: " + ", ".join(missing))
    if API_ID <= 0:
        raise RuntimeError("API_ID must be a positive integer.")

validate_config()

# Create app with proper event loop
app = Client(
    "search_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)

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
# ALL BOT FUNCTIONS (Compressed)
# ============================================================

# Add all your bot functions here...
# (Same as your original code)

# ============================================================
# STARTUP
# ============================================================

async def on_startup():
    logger.info("Bot starting up...")
    await setup_indexes()
    
    scheduler.add_job(cleanup_expired_links, IntervalTrigger(minutes=2), id="cleanup_links", replace_existing=True)
    scheduler.add_job(update_channel_bios, IntervalTrigger(hours=BIO_UPDATE_INTERVAL), id="update_bios", replace_existing=True)
    scheduler.add_job(auto_backup_scheduled, CronTrigger(hour=1, minute=45), id="backup_morning", replace_existing=True)
    scheduler.add_job(auto_backup_scheduled, CronTrigger(hour=14, minute=0), id="backup_evening", replace_existing=True)
    
    scheduler.start()
    logger.info("Bot is ready!")

# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    print("=" * 50)
    print("  Telegram Search Bot v2.0")
    print("  Running on Render.com" if os.environ.get("RENDER") else "  Running locally")
    print("=" * 50)
    
    # Use the existing event loop
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