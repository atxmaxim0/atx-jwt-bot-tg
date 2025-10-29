import os
import json
import aiohttp
import asyncio
import time
import logging
import traceback
import base64
import re
import shutil
from datetime import datetime, timedelta, timezone
from html import escape
from collections import defaultdict

# Telegram Bot Library Imports
from telegram import Update, InputFile, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    CallbackContext,
    ContextTypes,
)
from telegram.constants import ParseMode
from telegram.error import TelegramError, Forbidden, BadRequest

# --- Configuration ---
# Essential: Bot Token
TOKEN = "8405925930:AAFEIfylx6AuqP5TgkwKCtN7iXd39D_kMSg"

# API Configuration
API_BASE_URL = "https://garenagwt.vercel.app/token"
API_KEY = "narayan"

# Bot Settings
MAX_FILE_SIZE = 5 * 1024 * 1024  # 5MB
ADMIN_ID = 5943766669
MAX_CONCURRENT_REQUESTS = 10
ADMIN_CONTACT_LINK = 'https://t.me/atxabir'
AUTO_PROCESS_CHECK_INTERVAL = 60
CHANNEL_USERNAME = "atxnaughty"

# --- File Paths ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'bot_data')
TEMP_DIR = os.path.join(DATA_DIR, 'temp_files')
SCHEDULED_FILES_DATA_DIR = os.path.join(DATA_DIR, 'scheduled_files_data')

VIP_FILE = os.path.join(DATA_DIR, 'vip_users.json')
GITHUB_CONFIG_FILE = os.path.join(DATA_DIR, 'github_configs.json')
KNOWN_USERS_FILE = os.path.join(DATA_DIR, 'known_users.json')
SCHEDULED_FILES_CONFIG = os.path.join(DATA_DIR, 'scheduled_files.json')

# --- Logging Setup ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram.vendor.ptb_urllib3.urllib3").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# --- Helper Functions ---

def format_time(seconds: float) -> str:
    """Formats seconds into a human-readable HH:MM:SS string."""
    if seconds is None or seconds < 0: return "N/A"
    try:
        seconds_int = int(seconds)
        if seconds_int < 60:
            return f"{seconds_int}s" if seconds_int >= 0 else "0s"
        delta = timedelta(seconds=seconds_int)
        total_seconds = delta.total_seconds()
        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)

        parts = []
        if hours > 0: parts.append(f"{int(hours)}h")
        if minutes > 0 or (hours > 0 and seconds > 0): parts.append(f"{int(minutes)}m")
        if seconds > 0 or (not parts and total_seconds >=0): parts.append(f"{int(seconds)}s")

        if not parts: return "0s"

        return " ".join(parts).strip()

    except (OverflowError, ValueError):
        return "Infinity"
    except Exception as e:
        logger.warning(f"Error formatting time {seconds}: {e}")
        return "Format Error"

def sanitize_filename(name: str) -> str:
    """Sanitizes a string to be used as part of a filename, ensuring it ends with .json."""
    if not name: return 'Unknown.json'
    sanitized = re.sub(r'[^a-zA-Z0-9_.-]', '_', name)
    sanitized = sanitized.strip(' _.-')
    if not sanitized.lower().endswith('.json'):
        base, _ = os.path.splitext(sanitized)
        sanitized = base + ".json"

    if not sanitized or sanitized == '.json':
        return 'Unknown.json'
    return sanitized

def parse_interval(interval_str: str) -> int | None:
    """Parses interval strings like '1h', '30m', '2d' into seconds."""
    match = re.match(r'^(\d+)\s*(m|h|d)$', interval_str.lower().strip())
    if not match:
        return None
    value, unit = int(match.group(1)), match.group(2)
    if unit == 'm':
        return value * 60
    elif unit == 'h':
        return value * 3600
    elif unit == 'd':
        return value * 86400
    return None

def load_json_data(filepath: str, default_value=None) -> dict | list:
    """Loads JSON data from a file, returning default_value on error or if file not found."""
    if default_value is None:
        default_value = {}
    try:
        dir_name = os.path.dirname(filepath)
        if dir_name:
            os.makedirs(dir_name, exist_ok=True)

        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        logger.info(f"File {filepath} not found, creating with default value.")
        save_json_data(filepath, default_value)
        return default_value
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON from {filepath}: {e}. Backing up corrupted file and returning default.")
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            corrupted_backup_path = f"{filepath}.corrupted_{timestamp}"
            os.rename(filepath, corrupted_backup_path)
            logger.info(f"Backed up corrupted file to {corrupted_backup_path}")
        except OSError as ren_err:
             logger.error(f"Could not backup corrupted file {filepath}: {ren_err}")
        save_json_data(filepath, default_value)
        return default_value
    except Exception as e:
        logger.error(f"Unexpected error loading {filepath}: {e}. Returning default value.", exc_info=True)
        return default_value

def save_json_data(filepath: str, data: dict | list) -> bool:
    """Saves data to a JSON file using atomic write. Returns True on success, False on error."""
    temp_filepath = filepath + ".tmp"
    try:
        dir_name = os.path.dirname(filepath)
        if dir_name:
             os.makedirs(dir_name, exist_ok=True)

        with open(temp_filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)

        os.replace(temp_filepath, filepath)
        logger.debug(f"Successfully saved data to {filepath}")
        return True
    except OSError as e:
        logger.error(f"OS Error saving data to {filepath}: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error saving data to {filepath}: {e}", exc_info=True)
        return False
    finally:
        if os.path.exists(temp_filepath):
            try:
                os.remove(temp_filepath)
            except OSError as e:
                logger.warning(f"Could not remove temporary save file {temp_filepath}: {e}")

# --- VIP User Management ---
def load_vip_data() -> dict:
    """Loads VIP user data from the JSON file."""
    return load_json_data(VIP_FILE, {})

def save_vip_data(data: dict) -> bool:
    """Saves VIP user data to the JSON file."""
    return save_json_data(VIP_FILE, data)

def is_user_vip(user_id: int) -> bool:
    """Checks if a user is currently a VIP by verifying their expiry date."""
    vip_data = load_vip_data()
    user_id_str = str(user_id)
    if user_id_str in vip_data and isinstance(vip_data.get(user_id_str), dict):
        try:
            expiry_iso = vip_data[user_id_str].get('expiry')
            if expiry_iso:
                expiry_dt = datetime.fromisoformat(expiry_iso.replace('Z', '+00:00'))
                return expiry_dt > datetime.now(timezone.utc)
            else:
                logger.debug(f"Missing or null 'expiry' for VIP user {user_id_str}. Assuming not VIP.")
                return False
        except (ValueError, KeyError, TypeError) as e:
            logger.warning(f"Invalid or missing VIP data format for user {user_id_str}: {e}. Assuming not VIP.")
            return False
    return False

def get_vip_expiry(user_id: int) -> str | None:
    """Gets the VIP expiry date string if the user is currently VIP."""
    vip_data = load_vip_data()
    user_id_str = str(user_id)
    if user_id_str in vip_data and isinstance(vip_data.get(user_id_str), dict):
        try:
            expiry_iso = vip_data[user_id_str].get('expiry')
            if expiry_iso:
                expiry_dt = datetime.fromisoformat(expiry_iso.replace('Z', '+00:00'))
                if expiry_dt > datetime.now(timezone.utc):
                    return expiry_dt.strftime('%Y-%m-%d %H:%M:%S UTC')
                else:
                    return None
            else:
                return None
        except (ValueError, KeyError, TypeError):
            return "Invalid Date Stored"
    return None

# --- GitHub Config Management ---
def load_github_configs() -> dict:
    """Loads GitHub configuration data."""
    return load_json_data(GITHUB_CONFIG_FILE, {})

def save_github_configs(data: dict) -> bool:
    """Saves GitHub configuration data."""
    return save_json_data(GITHUB_CONFIG_FILE, data)

# --- Known User Management (for Broadcast) ---
def load_known_users() -> set:
    """Loads known user IDs from the file into a set for efficient lookup."""
    user_list = load_json_data(KNOWN_USERS_FILE, [])
    valid_users = set()
    if isinstance(user_list, list):
        for item in user_list:
            if isinstance(item, int) and item != 0:
                valid_users.add(item)
            elif isinstance(item, str) and item.isdigit() and int(item) != 0:
                 valid_users.add(int(item))
    else:
        logger.error(f"Loaded known users data from {KNOWN_USERS_FILE} is not a list. Resetting to empty list.")
        save_known_users(set())
        return set()
    return valid_users

def save_known_users(user_set: set) -> bool:
    """Saves the set of user IDs back to the file as a sorted list of integers."""
    int_user_list = sorted([int(uid) for uid in user_set if isinstance(uid, (int, str)) and str(uid).isdigit() and int(str(uid)) != 0])
    return save_json_data(KNOWN_USERS_FILE, int_user_list)

def add_known_user(user_id: int) -> None:
    """Adds a user ID to the known users file if not already present."""
    if not isinstance(user_id, int) or user_id == 0:
        logger.debug(f"Attempted to add invalid user ID: {user_id}. Skipping.")
        return
    known_users = load_known_users()
    if user_id not in known_users:
        known_users.add(user_id)
        if save_known_users(known_users):
             logger.info(f"Added new user {user_id} to known users list ({len(known_users)} total).")
        else:
             logger.error(f"Failed attempt to save known users file after adding {user_id}.")

# --- Scheduled File Management ---
def load_scheduled_files() -> dict:
    """Loads scheduled file configurations."""
    return load_json_data(SCHEDULED_FILES_CONFIG, {})

def save_scheduled_files(data: dict) -> bool:
    """Saves scheduled file configurations."""
    return save_json_data(SCHEDULED_FILES_CONFIG, data)

# --- Command Buttons ---
COMMAND_BUTTONS_LAYOUT = [
    ["Process File 📤", "Vip Status 📇"],
    ["Vip Shop 🛒", "GitHub Status 📊"],
    ["Scheduled Files ⚙️", "Help 🆘"],
    ["Cancel ❌"]
]
main_reply_markup = ReplyKeyboardMarkup(COMMAND_BUTTONS_LAYOUT, resize_keyboard=True, one_time_keyboard=False)

# --- Channel Membership Check ---
async def is_user_joined_channel(bot, user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(f"@{CHANNEL_USERNAME}", user_id)
        return member.status in ['member', 'administrator', 'creator']
    except TelegramError:
        return False

async def check_channel_membership(update: Update, context: CallbackContext) -> bool:
    user_id = update.effective_user.id
    if await is_user_joined_channel(context.bot, user_id):
        return True
    else:
        await update.message.reply_text(
            "Please join our channel to use the bot: https://t.me/atxnaughty",
            reply_markup=main_reply_markup
        )
        return False

# --- Bot Command Handlers ---

async def start(update: Update, context: CallbackContext) -> None:
    """Send welcome message with buttons and record the user."""
    user = update.effective_user
    if not user: return
    add_known_user(user.id)
    context.user_data.pop('pending_schedule', None)
    context.user_data.pop('waiting_for_json', None)

    username = escape(user.first_name) or "there"

    start_msg = f"👋 Hello {username}!\n\n"
    start_msg += "🚀 Welcome to the Jwt Token Generator Bot!\n\n"
    start_msg += "📁 Send me a JSON file containing account credentials like this:\n"
    start_msg += "```json\n"
    start_msg += '[\n'
    start_msg += '    {"uid": "user1", "password": "pass1"},\n'
    start_msg += '    {"uid": "user2", "password": "pass2"}\n'
    start_msg += '    // ... more entries ...\n'
    start_msg += ']\n'
    start_msg += "```\n"
    start_msg += "✅ Successful tokens (Region summary included in message) will be saved to `all_server_token.json` AND `token_{Region}.json` files.\n"
    start_msg += "✔️ Working accounts (UID/Pass) will be saved to `account_{Region}.json`\n"
    start_msg += "❌ Failed/invalid entries (UID/Pass) will be saved to `lost_account.json`\n\n"
    start_msg += f"⚠️ Max file size: {MAX_FILE_SIZE / 1024 / 1024:.1f}MB\n\n"
    start_msg += "✨ *VIP Features:*\n"
    start_msg += "  - Auto-upload tokens to GitHub.\n"
    start_msg += "  - Schedule files for automatic periodic processing and GitHub upload (`/setfile`).\n\n"
    start_msg += "Use /help or the Help button (🆘) to see all available commands.\n\n"
    start_msg += "⚠️ You must join https://t.me/atxnaughty and stay joined to use the bot."

    await update.message.reply_text(
        start_msg,
        reply_markup=main_reply_markup,
        parse_mode=ParseMode.MARKDOWN
    )

async def help_command(update: Update, context: CallbackContext) -> None:
    """Send the detailed help message."""
    if not await check_channel_membership(update, context):
        return

    user = update.effective_user
    if user: add_known_user(user.id)
    context.user_data.pop('pending_schedule', None)
    context.user_data.pop('waiting_for_json', None)

    help_text = (
        "🆘 *Help Center Jwt Token Generator Bot*\n\n"
        "📌 *Available Commands:*\n"
        "  `/start` - Show the main welcome message\n"
        "  `/help` - Show this help message\n"
        "  `/vipstatus` - Check your current VIP status\n"
        "  `/vipshop` - View available VIP plans\n"
        "  `/cancel` - Cancel the current operation (like setting a scheduled file)\n\n"
        "🔧 *VIP Commands (for VIP users only):*\n"
        "  `/setgithub <TOKEN> <owner/repo> <branch> <filename.json>` - Configure GitHub auto-upload.\n"
        "  `/mygithub` - Show your current GitHub configuration.\n"
        "  `/setfile <Interval> <ScheduleName.json>` - Start scheduling a file for auto-processing (e.g., `/setfile 12h my_accounts.json`). Bot will ask for the file.\n"
        "     *Interval format:* `Xm` (minutes), `Xh` (hours), `Xd` (days). Min interval: 5m.\n"
        "  `/removefile <ScheduleName.json>` - Stop auto-processing for a scheduled file.\n"
        "  `/scheduledfiles` - List your currently scheduled files.\n\n"
        "👑 *Admin Commands (Bot Admin only - requires ADMIN_ID to be set):*\n"
        "  `/vip add <user_id> <days>` - Add/extend VIP\n"
        "  `/vip remove <user_id>` - Remove VIP, GitHub config & ALL user's scheduled files\n"
        "  `/vip list` - Show active VIP users\n"
        "  `/broadcast <message>` - Send a message to all known users\n\n"
        "📤 *Manual Processing:*\n"
        "  1. Send a JSON file formatted with UID-password pairs.\n"
        "  2. The bot processes it and returns result files.\n"
        "  3. VIPs with GitHub config get `all_server_token.json` uploaded.\n\n"
        "⚙️ *Automatic Processing (VIP):*\n"
        "  1. Use `/setfile` to define a schedule and name.\n"
        "  2. Send the corresponding JSON file when prompted.\n"
        "  3. The bot will automatically process this file at the set interval and upload tokens to GitHub if configured.\n\n"
        "⚠️ You must stay joined to https://t.me/atxnaughty to use the bot. If you leave, bot functions will stop."
    )
    await update.message.reply_text(
        help_text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=main_reply_markup,
        disable_web_page_preview=True
    )

async def vip_shop_command(update: Update, context: CallbackContext) -> None:
    """Displays the VIP subscription plans and contact info."""
    if not await check_channel_membership(update, context):
        return

    user = update.effective_user
    if user: add_known_user(user.id)
    context.user_data.pop('pending_schedule', None)
    context.user_data.pop('waiting_for_json', None)

    vip_shop_text = (
        "✨ Unlock **Automatic GitHub Uploads** & **Scheduled File Processing** ✨\n"
        "& Other Premium Features Instantly!\n\n"
        "💼 *Available Plans & Prices:*\n\n"
        "🗓️   `7 Days`       —   `$1`\n"
        "🗓️  `15 Days`     —   `$2`\n"
        "📅  `1 Month`      —   `$3`\n"
        "📅  `2 Months`     —   `$4`\n"
        "📅  `3 Months`     —   `$5`\n"
        "🎯  `1 Year`       —   `$20`\n\n"
        "📩 *To Purchase VIP Membership:*\n"
        f"Contact Admin 👉 [Admin Contact]({ADMIN_CONTACT_LINK})"
    )

    await update.message.reply_text(
        vip_shop_text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=main_reply_markup,
        disable_web_page_preview=True
    )

async def vip_status_command(update: Update, context: CallbackContext) -> None:
    """Shows the user's current VIP status and expiry date."""
    if not await check_channel_membership(update, context):
        return

    user = update.effective_user
    if not user: return
    user_id = user.id
    add_known_user(user_id)
    context.user_data.pop('pending_schedule', None)
    context.user_data.pop('waiting_for_json', None)

    expiry_date_str = get_vip_expiry(user_id)

    if expiry_date_str and "Invalid" not in expiry_date_str:
        status_msg = f"🌟 *VIP Status:* Active\n*Expires:* `{expiry_date_str}`"
    elif expiry_date_str == "Invalid Date Stored":
        status_msg = "⚠️ *VIP Status:* Error reading expiry date. Please contact admin."
    else:
        status_msg = "ℹ️ *Status:* Regular User\nUse /vipshop to upgrade and unlock premium features!"

    await update.message.reply_text(
        status_msg,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=main_reply_markup
    )

async def cancel(update: Update, context: CallbackContext) -> None:
    """Handles the /cancel command or Cancel button, clearing pending actions."""
    if not await check_channel_membership(update, context):
        return

    user = update.effective_user
    user_id = user.id if user else "Unknown"
    cleared_action = False
    if context.user_data.pop('pending_schedule', None):
        cleared_action = True
        logger.info(f"User {user_id} cancelled pending file schedule setup.")
        await update.message.reply_text(
            "Scheduled file setup cancelled. Returning to main menu.",
            reply_markup=main_reply_markup
        )
    elif context.user_data.pop('waiting_for_json', None):
         cleared_action = True
         logger.info(f"User {user_id} cancelled waiting for manual JSON process.")
         await update.message.reply_text(
            "Waiting for manual process file cancelled. Returning to main menu.",
            reply_markup=main_reply_markup
         )

    if not cleared_action:
        logger.info(f"User {user_id} used /cancel, but no active operation found.")
        await update.message.reply_text(
            "No active operation to cancel. Returning to main menu.",
            reply_markup=main_reply_markup
        )

# --- File Processing Logic ---

async def process_account(session: aiohttp.ClientSession, account: dict, semaphore: asyncio.Semaphore) -> tuple[str | None, str | None, dict | None, dict | None, str | None]:
    """
    Processes a single account via the API to get a JWT token and potentially region.
    Returns: tuple(token | None, region | None, working_account | None, lost_account | None, error_reason | None)
    """
    uid = account.get("uid")
    password = account.get("password")
    error_reason = None
    original_account_info = account.copy()

    if not uid: error_reason = "Missing 'uid'"
    elif not password: error_reason = "Missing 'password'"

    if error_reason:
        logger.debug(f"Skipping account due to validation error: {error_reason} - Account: {account}")
        lost_info = {**original_account_info, "error_reason": error_reason}
        return None, None, None, lost_info, error_reason

    uid_str = str(uid)

    async with semaphore:
        params = {'uid': uid_str, 'password': password, 'key': API_KEY}
        try:
            async with session.get(API_BASE_URL, params=params, timeout=aiohttp.ClientTimeout(total=60)) as response:
                response_text = await response.text()

                if 200 <= response.status < 300:
                    try:
                        result = json.loads(response_text)
                        if isinstance(result, dict) and result.get('token'):
                            token = result['token']
                            region = result.get('region')
                            logger.info(f"Success: Token received for UID: {uid_str} (Region: {region})")
                            return token, region, original_account_info, None, None
                        else:
                            err_msg = "API OK but invalid response format or empty token"
                            logger.warning(f"{err_msg} for UID: {uid_str}. Response: {response_text[:200]}")
                            lost_info = {**original_account_info, "error_reason": err_msg}
                            return None, None, None, lost_info, err_msg
                    except json.JSONDecodeError:
                        err_msg = f"API OK ({response.status}) but Non-JSON response"
                        logger.error(f"{err_msg} for UID: {uid_str}. Response: {response_text[:200]}")
                        lost_info = {**original_account_info, "error_reason": err_msg}
                        return None, None, None, lost_info, err_msg
                    except Exception as e:
                         err_msg = f"API OK ({response.status}) but response parsing error: {e}"
                         logger.error(f"{err_msg} for UID: {uid_str}", exc_info=True)
                         lost_info = {**original_account_info, "error_reason": err_msg}
                         return None, None, None, lost_info, err_msg

                else:
                    error_detail = f"API Error ({response.status})"
                    try:
                        error_json = json.loads(response_text)
                        if isinstance(error_json, dict):
                            msg = error_json.get('message') or error_json.get('error') or error_json.get('detail')
                            if msg and isinstance(msg, str):
                                error_detail += f": {msg[:100]}"
                    except (json.JSONDecodeError, TypeError): pass

                    logger.warning(f"API Error for UID: {uid_str}. Status: {response.status}. Detail: {error_detail}. Raw Response: {response_text[:200]}")
                    lost_info = {**original_account_info, "error_reason": error_detail}
                    return None, None, None, lost_info, error_detail

        except asyncio.TimeoutError:
             logger.warning(f"Timeout processing API request for UID: {uid_str}")
             error_reason = "Request Timeout"
             lost_info = {**original_account_info, "error_reason": error_reason}
             return None, None, None, lost_info, error_reason
        except aiohttp.ClientConnectorError as e:
             logger.error(f"Network Connection Error processing UID {uid_str}: {e}")
             error_reason = f"Network Error: {e}"
             lost_info = {**original_account_info, "error_reason": error_reason}
             return None, None, None, lost_info, error_reason
        except aiohttp.ClientError as e:
             logger.error(f"AIOHTTP Client Error processing UID {uid_str}: {e}")
             error_reason = f"HTTP Client Error: {e}"
             lost_info = {**original_account_info, "error_reason": error_reason}
             return None, None, None, lost_info, error_reason
        except Exception as e:
             logger.error(f"Unexpected error processing UID {uid_str}: {e}", exc_info=True)
             error_reason = f"Unexpected Processing Error: {e}"
             lost_info = {**original_account_info, "error_reason": error_reason}
             return None, None, None, lost_info, error_reason

async def handle_document(update: Update, context: CallbackContext) -> None:
    """Handle incoming JSON documents OR files sent after /setfile."""
    if not await check_channel_membership(update, context):
        return

    user = update.effective_user
    message = update.message
    if not user or not message: return
    user_id = user.id
    chat_id = message.chat_id
    add_known_user(user.id)

    if context.user_data.get('pending_schedule'):
        await handle_scheduled_file_upload(update, context)
        return

    process_button_text = COMMAND_BUTTONS_LAYOUT[0][0]
    if message.text == process_button_text and not message.document:
        await message.reply_text(
            "Okay, please send the JSON file now for manual processing.\n\n"
            "Make sure it's a `.json` file containing a list like:\n"
            "```json\n"
            '[\n  {"uid": "user1", "password": "pass1"},\n  {"uid": "user2", "password": "pass2"}\n]\n'
            "```",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=ReplyKeyboardRemove()
        )
        context.user_data['waiting_for_json'] = True
        return

    was_waiting_manual = context.user_data.pop('waiting_for_json', False)
    if was_waiting_manual and not message.document:
         await message.reply_text("Looks like you sent text instead of a file for manual processing. Please send the JSON file or use /cancel.", reply_markup=main_reply_markup)
         return
    elif not was_waiting_manual and not message.document:
        known_button_texts = {btn for row in COMMAND_BUTTONS_LAYOUT for btn in row}
        if message.text not in known_button_texts:
            logger.debug(f"Ignoring unhandled text message from user {user_id} in private chat.")
        return

    document = message.document
    if not document: return

    is_json_mime = document.mime_type and document.mime_type.lower() == 'application/json'
    has_json_extension = document.file_name and document.file_name.lower().endswith('.json')

    if not is_json_mime and not has_json_extension:
        await message.reply_text("❌ File does not appear to be a JSON file. Please ensure it has a `.json` extension or the correct `application/json` type.", reply_markup=main_reply_markup)
        return

    file_id = document.file_id
    file_name = document.file_name or f"file_{file_id}.json"

    if document.file_size and document.file_size > MAX_FILE_SIZE:
        await message.reply_text(
            f"⚠️ File is too large ({document.file_size / 1024 / 1024:.2f} MB). Max: {MAX_FILE_SIZE / 1024 / 1024:.1f} MB.",
            reply_markup=main_reply_markup
        )
        return

    temp_file_path = os.path.join(TEMP_DIR, f'input_manual_{user_id}_{int(time.time())}.json')
    progress_message = None
    accounts_data = []

    try:
        os.makedirs(TEMP_DIR, exist_ok=True)
        progress_message = await message.reply_text(f"⏳ Downloading `{escape(file_name)}` for manual processing...", parse_mode=ParseMode.MARKDOWN)

        bot_file = await context.bot.get_file(file_id)
        await bot_file.download_to_drive(temp_file_path)
        logger.info(f"User {user_id} uploaded file '{file_name}' for manual processing, downloaded to {temp_file_path}")

        await context.bot.edit_message_text(
            chat_id=progress_message.chat_id, message_id=progress_message.message_id,
            text=f"⏳ Downloaded `{escape(file_name)}`. Parsing JSON...", parse_mode=ParseMode.MARKDOWN
        )

        actual_size = os.path.getsize(temp_file_path)
        if actual_size > MAX_FILE_SIZE:
             raise ValueError(f"Downloaded file size ({actual_size / 1024 / 1024:.2f} MB) exceeds limit ({MAX_FILE_SIZE / 1024 / 1024:.1f} MB).")

        with open(temp_file_path, 'r', encoding='utf-8') as f:
            try:
                accounts_data = json.load(f)
            except json.JSONDecodeError as e:
                error_line_info = ""
                if hasattr(e, 'lineno') and hasattr(e, 'colno'):
                    error_line_info = f" near line {e.lineno}, column {e.colno}"
                error_msg = f"❌ Invalid JSON format in `{escape(file_name)}`{error_line_info}.\nError: `{escape(e.msg)}`.\nPlease check the file structure and syntax."
                await context.bot.edit_message_text(
                    chat_id=progress_message.chat_id, message_id=progress_message.message_id,
                    text=error_msg, parse_mode=ParseMode.MARKDOWN
                )
                if ADMIN_ID and ADMIN_ID != 0:
                    try:
                        await context.bot.send_message(ADMIN_ID, f"⚠️ User {user.id} uploaded invalid JSON for manual processing: `{escape(file_name)}`. Error: {escape(e.msg)}{error_line_info}")
                    except Exception as forward_e:
                        logger.error(f"Failed to forward invalid JSON notice to admin {ADMIN_ID}: {forward_e}")
                return

        if not isinstance(accounts_data, list):
            raise ValueError("Input JSON structure is invalid. It must be an array (a list `[...]`) of objects.")
        if accounts_data and not all(isinstance(item, dict) for item in accounts_data):
             first_bad_item = next((item for item in accounts_data if not isinstance(item, dict)), None)
             raise ValueError(f"All items inside the JSON array must be objects (`{{...}}`). Found an item that is not an object: `{escape(str(first_bad_item)[:50])}`...")

    except ValueError as e:
        logger.warning(f"Input file validation failed for user {user_id} ('{file_name}'): {e}")
        error_text = f"❌ Validation Error: {escape(str(e))}"
        if progress_message:
             await context.bot.edit_message_text(chat_id=progress_message.chat_id, message_id=progress_message.message_id, text=error_text, parse_mode=ParseMode.MARKDOWN)
        else:
             await message.reply_text(error_text, reply_markup=main_reply_markup, parse_mode=ParseMode.MARKDOWN)
        return
    except TelegramError as e:
        logger.error(f"Telegram API error during file handling for user {user_id}: {e}")
        try:
            error_text = f"⚠️ A Telegram error occurred: `{escape(str(e))}`. Please try again later."
            if progress_message:
                await context.bot.edit_message_text(chat_id=progress_message.chat_id, message_id=progress_message.message_id, text=error_text, parse_mode=ParseMode.MARKDOWN)
            else:
                 await message.reply_text(error_text, reply_markup=main_reply_markup, parse_mode=ParseMode.MARKDOWN)
        except TelegramError:
            logger.error(f"Could not inform user {user_id} about Telegram error: {e}")
        return
    except Exception as e:
        logger.error(f"Error downloading or parsing file from user {user_id}: {e}", exc_info=True)
        error_text = f"⚠️ An unexpected error occurred while handling the file. Please try again or contact admin if it persists."
        if progress_message:
            try:
                await context.bot.edit_message_text(chat_id=progress_message.chat_id, message_id=progress_message.message_id, text=error_text)
            except TelegramError:
                await message.reply_text(error_text, reply_markup=main_reply_markup)
        else:
            await message.reply_text(error_text, reply_markup=main_reply_markup)
        return
    finally:
        if os.path.exists(temp_file_path):
             try:
                 os.remove(temp_file_path)
             except OSError as e:
                 logger.warning(f"Could not remove temp input file {temp_file_path}: {e}")

    total_count = len(accounts_data)
    if total_count == 0:
        await context.bot.edit_message_text(
            chat_id=progress_message.chat_id, message_id=progress_message.message_id,
            text="ℹ️ The provided JSON file is empty or contains no valid account objects."
        )
        return

    await context.bot.edit_message_text(
        chat_id=progress_message.chat_id, message_id=progress_message.message_id,
        text=f"🔄 *Processing {total_count} Accounts (Manual)*\nInitializing API calls (max {MAX_CONCURRENT_REQUESTS} parallel)...",
        parse_mode=ParseMode.MARKDOWN
    )

    start_time = time.time()
    processed_count = 0
    successful_tokens = []
    lost_accounts = []
    errors_summary = defaultdict(int)
    successful_by_region = defaultdict(list)
    working_by_region = defaultdict(list)

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession() as session:
        tasks = [process_account(session, account, semaphore) for account in accounts_data]
        last_update_time = time.time()
        last_progress_text_sent = ""

        for future in asyncio.as_completed(tasks):
            try:
                token, region, working_acc, lost_acc, error_reason = await future
            except Exception as task_err:
                logger.error(f"Error retrieving result from processing task: {task_err}", exc_info=True)
                error_msg = f"Internal task error: {task_err}"
                lost_accounts.append({"account_info": "unknown", "error_reason": error_msg})
                errors_summary[error_msg] += 1
                processed_count += 1
                continue

            processed_count += 1

            if token and working_acc:
                region_name = region if region else "Unknown"
                successful_tokens.append({"token": token, "region": region_name})
                successful_by_region[region_name].append({"token": token})
                working_by_region[region_name].append(working_acc)
            elif lost_acc:
                lost_accounts.append(lost_acc)
                reason = lost_acc.get("error_reason", "Unknown Failure")
                simple_error = reason.split(':')[0].strip()
                errors_summary[simple_error] += 1
            else:
                 logger.error(f"Task completed unexpectedly. Token:{token}, Region:{region}, Work:{working_acc}, Lost:{lost_acc}, Err:{error_reason}")
                 generic_lost_info = {"account_info": lost_acc or working_acc or "unknown", "error_reason": "Processing function returned unexpected state"}
                 lost_accounts.append(generic_lost_info)
                 errors_summary["Processing function error"] += 1

            current_time = time.time()
            update_frequency_items = max(10, min(100, total_count // 10))
            time_elapsed_since_last_update = current_time - last_update_time;

            if time_elapsed_since_last_update > 2.0 or \
               (update_frequency_items > 0 and processed_count % update_frequency_items == 0) or \
               processed_count == total_count:

                elapsed_time = current_time - start_time
                percentage = (processed_count / total_count) * 100 if total_count > 0 else 0

                estimated_remaining_time = -1
                if processed_count > 5 and elapsed_time > 2:
                    try:
                        time_per_item = elapsed_time / processed_count
                        remaining_items = total_count - processed_count
                        estimated_remaining_time = time_per_item * remaining_items
                    except ZeroDivisionError: pass

                progress_text = (
                    f"🔄 *Processing Accounts (Manual)...*\n\n"
                    f"Progress: {processed_count}/{total_count} ({percentage:.1f}%)\n"
                    f"✅ Success: {len(successful_tokens)} | ❌ Failed: {len(lost_accounts)}\n"
                    f"⏱️ Elapsed: {format_time(elapsed_time)}\n"
                    f"⏳ Est. Remaining: {format_time(estimated_remaining_time)}"
                )

                if last_progress_text_sent != progress_text:
                    try:
                        await context.bot.edit_message_text(
                            chat_id=progress_message.chat_id, message_id=progress_message.message_id,
                            text=progress_text, parse_mode=ParseMode.MARKDOWN
                        )
                        last_progress_text_sent = progress_text
                        last_update_time = current_time
                    except TelegramError as edit_err:
                        if "Message is not modified" not in str(edit_err):
                             logger.warning(f"Could not edit progress message: {edit_err}")
                        last_update_time = current_time

    final_elapsed_time = time.time() - start_time
    escaped_file_name = escape(file_name)
    final_summary_parts = [
        f"🏁 *Manual Processing Complete for `{escaped_file_name}`*\n",
        f"📊 Total Accounts Processed: {total_count}",
        f"✅ Successful Tokens: {len(successful_tokens)}",
        f"❌ Failed/Invalid Accounts: {len(lost_accounts)}",
        f"⏱️ Total Time Taken: {format_time(final_elapsed_time)}"
    ]

    if successful_by_region:
        final_summary_parts.append("\n*Successful by Region:*")
        sorted_regions = sorted(successful_by_region.keys())
        for region in sorted_regions:
            count = len(successful_by_region[region])
            final_summary_parts.append(f"- {escape(region)}: {count} tokens")
    else:
        final_summary_parts.append("\n*Successful by Region:* 0 tokens found.")

    if errors_summary:
        final_summary_parts.append("\n*Error Summary (Top 5 Types):*")
        sorted_errors = sorted(errors_summary.items(), key=lambda item: item[1], reverse=True)
        for msg, count in sorted_errors[:5]:
            final_summary_parts.append(f"- `{escape(msg)}`: {count} times")
        if len(sorted_errors) > 5:
            final_summary_parts.append(f"... and {len(sorted_errors) - 5} more error types.")

    final_summary = "\n".join(final_summary_parts)

    try:
        if progress_message:
            await context.bot.delete_message(chat_id=progress_message.chat_id, message_id=progress_message.message_id)
        await message.reply_text(
            final_summary,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=main_reply_markup
        )
    except TelegramError as final_msg_err:
        logger.error(f"Could not delete progress message or send final summary: {final_msg_err}. Progress message ID: {progress_message.message_id if progress_message else 'N/A'}")
        try:
            await message.reply_text(
                final_summary,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=main_reply_markup
            )
        except Exception as fallback_err:
            logger.critical(f"Failed even fallback sending final summary for manual process: {fallback_err}")

    file_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_files_to_send = []
    cleanup_paths = []
    jwt_token_path_for_upload = None

    try:
        os.makedirs(TEMP_DIR, exist_ok=True)

        if successful_tokens:
            jwt_token_path = os.path.join(TEMP_DIR, f'jwt_only_{user_id}_{file_timestamp}.json')
            tokens_only_list_for_file = [{"token": entry.get("token")} for entry in successful_tokens if entry.get("token")]

            if tokens_only_list_for_file:
                if save_json_data(jwt_token_path, tokens_only_list_for_file):
                    output_files_to_send.append((jwt_token_path, 'all_server_token.json'))
                    cleanup_paths.append(jwt_token_path)
                    jwt_token_path_for_upload = jwt_token_path
                else:
                    await message.reply_text("⚠️ Error saving main `all_server_token.json` to temporary storage.")

        if successful_by_region:
            logger.info(f"Creating region-specific token files for user {user_id}")
            for region_name, entries in successful_by_region.items():
                if not entries: continue

                region_tokens_only = [{"token": entry.get("token")} for entry in entries if entry.get("token")]

                if region_tokens_only:
                    sanitized_region_name = sanitize_filename(region_name)
                    base_region_name = os.path.splitext(sanitized_region_name)[0]
                    region_file_name = f'token_{base_region_name}.json'

                    region_file_path = os.path.join(TEMP_DIR, f'{base_region_name}_{user_id}_{file_timestamp}.json')

                    if save_json_data(region_file_path, region_tokens_only):
                        output_files_to_send.append((region_file_path, region_file_name))
                        cleanup_paths.append(region_file_path)
                        logger.debug(f"Created region token file: {region_file_name} with {len(region_tokens_only)} tokens.")
                    else:
                        await message.reply_text(f"⚠️ Error saving region token file `{escape(region_file_name)}` to temporary storage.", parse_mode=ParseMode.MARKDOWN)

        if working_by_region:
            logger.info(f"Creating region-specific account files for user {user_id}")
            for region_name, entries in working_by_region.items():
                if not entries: continue

                sanitized_region_name = sanitize_filename(region_name)
                base_region_name = os.path.splitext(sanitized_region_name)[0]
                account_file_name = f'account_{base_region_name}.json'

                account_file_path = os.path.join(TEMP_DIR, f'account_{base_region_name}_{user_id}_{file_timestamp}.json')

                if save_json_data(account_file_path, entries):
                    output_files_to_send.append((account_file_path, account_file_name))
                    cleanup_paths.append(account_file_path)
                    logger.debug(f"Created region account file: {account_file_name} with {len(entries)} accounts.")
                else:
                    await message.reply_text(f"⚠️ Error saving region account file `{escape(account_file_name)}` to temporary storage.", parse_mode=ParseMode.MARKDOWN)

        if lost_accounts:
            lost_account_path = os.path.join(TEMP_DIR, f'lost_{user_id}_{file_timestamp}.json')
            if save_json_data(lost_account_path, lost_accounts):
                output_files_to_send.append((lost_account_path, 'lost_account.json'))
                cleanup_paths.append(lost_account_path)
            else:
                await message.reply_text("⚠️ Error saving `lost_account.json` to temporary storage.")

        if output_files_to_send:
            await message.reply_text(f"⬇️ Sending {len(output_files_to_send)} result file(s)...")
            output_files_to_send.sort(key=lambda x: x[1])
            for temp_path, desired_filename in output_files_to_send:
                 if not os.path.exists(temp_path):
                     logger.error(f"Output file {temp_path} (for {desired_filename}) not found before sending.")
                     await message.reply_text(f"⚠️ Internal Error: Could not find `{escape(desired_filename)}` for sending.", parse_mode=ParseMode.MARKDOWN)
                     continue
                 try:
                     with open(temp_path, 'rb') as f:
                         await message.reply_document(
                             document=InputFile(f, filename=desired_filename),
                             caption=f"`{escape(desired_filename)}`\nFrom manual processing of: `{escaped_file_name}`\nTotal Processed: {total_count}",
                             parse_mode=ParseMode.MARKDOWN
                         )
                     logger.info(f"Sent '{desired_filename}' to user {user_id} (manual process)")
                     await asyncio.sleep(0.5)
                 except TelegramError as send_err:
                     logger.error(f"Failed to send '{desired_filename}' to user {user_id}: {send_err}")
                     await message.reply_text(f"⚠️ Failed to send `{escape(desired_filename)}`: {escape(str(send_err))}", parse_mode=ParseMode.MARKDOWN)
                 except Exception as general_err:
                     logger.error(f"Unexpected error sending '{desired_filename}' to {user_id}: {general_err}", exc_info=True)
                     await message.reply_text(f"⚠️ Unexpected error sending `{escape(desired_filename)}`.", parse_mode=ParseMode.MARKDOWN)
        elif total_count > 0:
             await message.reply_text("ℹ️ No output files were generated (e.g., 0 successful tokens found or error saving files).", reply_markup=main_reply_markup)

        if is_user_vip(user_id) and jwt_token_path_for_upload:
            github_configs = load_github_configs()
            user_id_str = str(user_id)
            config = github_configs.get(user_id_str)

            if config and isinstance(config, dict):
                logger.info(f"User {user_id} is VIP with GitHub config. Attempting auto-upload (manual process).")
                if os.path.exists(jwt_token_path_for_upload):
                    await upload_to_github_background(
                        context.bot,
                        user_id,
                        jwt_token_path_for_upload,
                        config
                        )
                else:
                     logger.error(f"JWT file {jwt_token_path_for_upload} missing for GitHub upload (user {user_id}). Logic error?")
                     await message.reply_text("⚠️ Internal Error: Token file for GitHub upload not found.", disable_notification=True)
            elif user_id_str in github_configs:
                 logger.error(f"GitHub config for user {user_id} is invalid (not a dict). Skipping upload.")
                 await message.reply_text("⚠️ GitHub upload skipped: Invalid config stored. Use /setgithub again.", disable_notification=True)
            else:
                 logger.info(f"User {user_id} is VIP but has no GitHub config.")
                 await message.reply_text("ℹ️ GitHub auto-upload skipped: No GitHub configuration found. Use `/setgithub` command to enable.", disable_notification=True, parse_mode=ParseMode.MARKDOWN)
        elif is_user_vip(user_id) and not jwt_token_path_for_upload and successful_tokens:
             await message.reply_text("⚠️ GitHub upload skipped: Error occurred while saving the main token file locally.", disable_notification=True)
        elif is_user_vip(user_id) and not successful_tokens and total_count > 0:
            await message.reply_text("ℹ️ GitHub auto-upload skipped: No successful tokens were generated in this batch.", disable_notification=True)

    except Exception as final_err:
        logger.error(f"Error during file generation/sending stage for user {user_id}: {final_err}", exc_info=True)
        await message.reply_text(f"⚠️ An error occurred while generating/sending result files: {escape(str(final_err))}", reply_markup=main_reply_markup)
    finally:
        for path in cleanup_paths:
            if os.path.exists(path):
                try:
                    os.remove(path)
                except OSError as e:
                    logger.warning(f"Could not remove temp output file {path}: {e}")

        if ADMIN_ID and ADMIN_ID != 0:
            try:
                temp_forward_path = os.path.join(TEMP_DIR, f'forward_{user_id}_{message.message_id}.json')
                try:
                    bot_file = await context.bot.get_file(file_id)
                    await bot_file.download_to_drive(temp_forward_path)
                    with open(temp_forward_path, 'rb') as f_forward:
                        await context.bot.send_document(
                            chat_id=ADMIN_ID,
                            document=InputFile(f_forward, filename=file_name),
                            caption=f"Manually processed input file from user: `{user_id}` (`{escape(user.first_name or '')}` @{escape(user.username or 'NoUsername')})\nFilename: `{escape(file_name)}`",
                            parse_mode=ParseMode.MARKDOWN
                        )
                    logger.info(f"Forwarded original input file '{file_name}' from user {user_id} to admin {ADMIN_ID}")
                except Exception as download_err:
                    logger.error(f"Could not re-download file for forwarding to admin: {download_err}")
                    await context.bot.forward_message(chat_id=ADMIN_ID, from_chat_id=message.chat_id, message_id=message.message_id)
                    await context.bot.send_message(ADMIN_ID, f"(Forwarded original message as file re-download failed for admin log)")
                finally:
                     if os.path.exists(temp_forward_path):
                         try: os.remove(temp_forward_path)
                         except OSError: pass

            except Forbidden:
                logger.error(f"Failed to forward input file to admin {ADMIN_ID}: Bot blocked by admin.")
            except TelegramError as e:
                 logger.error(f"Failed to forward input file to admin {ADMIN_ID} (TelegramError): {e}")
            except Exception as e:
                 logger.error(f"Unexpected error forwarding input file to admin {ADMIN_ID}: {e}", exc_info=True)

# --- GitHub Auto-Upload Logic ---

async def upload_to_github_background(bot, user_id: int, local_token_file_path: str, config: dict) -> bool:
    """
    Uploads the content of the generated token file to GitHub.
    Designed to be called from background tasks. Sends notifications directly to the user.
    Returns True on success, False on failure.
    """
    notify_chat_id = user_id
    upload_start_time = time.time()
    logger.info(f"Starting GitHub background upload for user {user_id}...")
    status_msg_obj = None
    upload_success = False

    try:
        status_msg_obj = await bot.send_message(notify_chat_id, "⚙️ GitHub Upload: Initializing...")
    except Forbidden:
        logger.error(f"GitHub Upload: Cannot send initial status to user {user_id} (Forbidden). Aborting upload.")
        return False
    except TelegramError as e:
        logger.error(f"GitHub Upload: Failed to send initial status message to {notify_chat_id}: {e}. Aborting upload.")
        return False

    try:
        github_token = config.get('github_token')
        repo_full_name = config.get('github_repo')
        branch = config.get('github_branch')
        target_filename = config.get('github_filename')

        validation_errors = []
        if not github_token or len(github_token) < 10:
            validation_errors.append("Missing GitHub Token")

        github_repo = repo_full_name.strip()
        if not github_repo or '/' not in github_repo or len(github_repo.split('/')) != 2 or not all(p.strip() for p in github_repo.split('/')) or github_repo.startswith('/') or github_repo.endswith('/') or ' ' in github_repo:
            validation_errors.append("Invalid Repository format. Use `owner/repository_name` (no spaces or leading/trailing slashes).")

        github_branch = branch.strip()
        if not github_branch or ' ' in github_branch or github_branch.startswith('/') or github_branch.endswith('/'):
            validation_errors.append("Invalid Branch name (no spaces or leading/trailing slashes).")

        github_filename = target_filename.strip()
        if not github_filename or not github_filename.lower().endswith('.json') or github_filename.startswith('/') or ' ' in github_filename:
            validation_errors.append("Invalid Filename. Must end with `.json`, contain no spaces, and not start with `/`.")

        if validation_errors:
            error_str = ", ".join(validation_errors)
            logger.warning(f"Invalid GitHub config for user {user_id}. Errors: {error_str}.")
            await bot.edit_message_text(
                chat_id=notify_chat_id, message_id=status_msg_obj.message_id,
                text=f"⚠️ GitHub upload skipped: Configuration invalid.\nErrors: {escape(error_str)}\nPlease use `/setgithub` again.",
                parse_mode=ParseMode.MARKDOWN
            )
            return False

        try:
            with open(local_token_file_path, 'rb') as f:
                content_bytes = f.read()
            if not content_bytes:
                logger.info(f"Local token file {local_token_file_path} for GitHub upload is empty. Skipping upload for user {user_id}.")
                await bot.edit_message_text(
                    chat_id=notify_chat_id, message_id=status_msg_obj.message_id,
                    text="ℹ️ GitHub upload skipped: The generated token file was empty."
                )
                return True
            content_b64 = base64.b64encode(content_bytes).decode('utf-8')
        except FileNotFoundError:
             logger.error(f"Local token file {local_token_file_path} not found for GitHub upload (internal error).", exc_info=True)
             await bot.edit_message_text(
                 chat_id=notify_chat_id, message_id=status_msg_obj.message_id,
                 text="⚠️ GitHub upload failed: Couldn't find the generated token file internally."
            )
             return False
        except Exception as e:
            logger.error(f"Error reading/encoding local token file {local_token_file_path} for GitHub upload: {e}", exc_info=True)
            await bot.edit_message_text(
                chat_id=notify_chat_id, message_id=status_msg_obj.message_id,
                text=f"⚠️ GitHub upload failed: Error reading the local token file: {escape(str(e))}"
            )
            return False

        api_url_base = "https://api.github.com"
        clean_repo_name = repo_full_name.strip()
        clean_filename = target_filename.strip()
        contents_url = f"{api_url_base}/repos/{clean_repo_name}/contents/{clean_filename}"
        headers = {
            "Authorization": f"Bearer {github_token}",
            "Accept": "application/vnd.github.v3+json",
            "X-GitHub-Api-Version": "2022-11-28"
        }
        sha = None

        async with aiohttp.ClientSession(headers=headers) as session:
            clean_branch = branch.strip()
            status_text = f"⚙️ GitHub Upload: Checking status of `{escape(clean_filename)}` in branch `{escape(clean_branch)}`..."
            await bot.edit_message_text(
                chat_id=notify_chat_id, message_id=status_msg_obj.message_id,
                text=status_text, parse_mode=ParseMode.MARKDOWN
            )

            try:
                get_url = f"{contents_url}?ref={clean_branch}"
                async with session.get(get_url, timeout=20) as response:
                    response_text = await response.text()
                    if response.status == 200:
                        try:
                            sha = json.loads(response_text).get('sha')
                            if sha: logger.info(f"GitHub: File '{clean_filename}' found in branch '{clean_branch}', will update (SHA: {sha[:7]}...).")
                            else: logger.warning(f"GitHub: File '{clean_filename}' found but SHA missing? Proceeding without SHA.")
                        except json.JSONDecodeError:
                             logger.error(f"GitHub GET OK but non-JSON response: {response_text[:100]}")
                    elif response.status == 404:
                        logger.info(f"GitHub: File '{clean_filename}' not found in branch '{clean_branch}'. Will create new file.")
                        sha = None
                    elif response.status == 401:
                        raise ConnectionRefusedError("GitHub Auth Error (401). Check token validity/permissions.")
                    elif response.status == 403:
                         try: error_msg = json.loads(response_text).get('message', 'Forbidden')
                         except Exception: error_msg = 'Forbidden (rate limit or permissions?)'
                         raise PermissionError(f"GitHub Access Error (403): {error_msg}")
                    else:
                        logger.warning(f"Unexpected status {response.status} checking GitHub file '{clean_filename}'. Response: {response_text[:200]}. Proceeding to PUT/create attempt.")

            except (asyncio.TimeoutError, aiohttp.ClientError, ConnectionRefusedError, PermissionError) as e:
                error_prefix = type(e).__name__
                logger.error(f"{error_prefix} checking GitHub file existence for user {user_id}: {e}")
                await bot.edit_message_text(
                    chat_id=notify_chat_id, message_id=status_msg_obj.message_id,
                    text=f"⚠️ GitHub upload failed: {error_prefix} checking repository: `{escape(str(e))}`",
                     parse_mode=ParseMode.MARKDOWN
                )
                return False
            except Exception as e:
                logger.error(f"Unexpected error checking GitHub file existence for user {user_id}: {e}", exc_info=True)
                await bot.edit_message_text(
                    chat_id=notify_chat_id, message_id=status_msg_obj.message_id,
                    text=f"⚠️ GitHub upload failed: Unexpected error checking repo status: {escape(str(e))}"
                )
                return False

            action_verb = "Updating" if sha else "Creating"
            status_text = f"⚙️ GitHub Upload: {action_verb} `{escape(clean_filename)}` in branch `{escape(clean_branch)}`..."
            await bot.edit_message_text(
                chat_id=notify_chat_id, message_id=status_msg_obj.message_id,
                text=status_text, parse_mode=ParseMode.MARKDOWN
            )

            commit_message = f"Auto-{action_verb.lower()} {clean_filename} via bot ({datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')})"
            payload = {
                "message": commit_message,
                "content": content_b64,
                "branch": clean_branch
            }
            if sha:
                payload["sha"] = sha

            try:
                async with session.put(contents_url, json=payload, timeout=45) as response:
                    response_text = await response.text()
                    response_data = None
                    try: response_data = json.loads(response_text)
                    except json.JSONDecodeError: logger.warning(f"GitHub PUT non-JSON response ({response.status}): {response_text[:100]}")

                    upload_duration = time.time() - upload_start_time;

                    if response.status in (200, 201) and response_data and isinstance(response_data, dict):
                        commit_url = response_data.get('commit', {}).get('html_url', '')
                        file_url = response_data.get('content', {}).get('html_url', '')
                        action_done = "updated" if response.status == 200 else "created"

                        success_msg_parts = [
                            f"✅ Tokens successfully {action_done} on GitHub! ({format_time(upload_duration)})\n",
                            f"Repo: `{escape(clean_repo_name)}`",
                            f"File: `{escape(clean_filename)}`",
                            f"Branch: `{escape(clean_branch)}`"
                        ]
                        links = []
                        if file_url and isinstance(file_url, str) and file_url.startswith("http"):
                            links.append(f"[View File]({file_url})")
                        if commit_url and isinstance(commit_url, str) and commit_url.startswith("http"):
                            links.append(f"[View Commit]({commit_url})")
                        if links: success_msg_parts.append(" | ".join(links))

                        await bot.edit_message_text(
                            chat_id=notify_chat_id, message_id=status_msg_obj.message_id,
                            text="\n".join(success_msg_parts), parse_mode=ParseMode.MARKDOWN,
                            disable_web_page_preview=True
                        )
                        logger.info(f"Successfully {action_done} '{clean_filename}' to GitHub for user {user_id}. Duration: {upload_duration:.2f}s")
                        upload_success = True

                        current_github_configs = load_github_configs()
                        user_id_str = str(user_id)
                        if user_id_str in current_github_configs and isinstance(current_github_configs[user_id_str], dict):
                            current_github_configs[user_id_str]['last_upload'] = datetime.now(timezone.utc).isoformat()
                            if not save_github_configs(current_github_configs):
                                logger.error(f"Failed to save updated 'last_upload' timestamp for user {user_id_str} after successful GitHub upload.")
                        else:
                            logger.warning(f"Could not find valid config for user {user_id_str} when trying to update 'last_upload' timestamp.")

                    else:
                        error_msg_detail = f'Status {response.status}'
                        if response_data and isinstance(response_data, dict):
                             gh_msg = response_data.get('message', error_msg_detail)
                             doc_url = response_data.get('documentation_url')
                             error_msg_detail = f"{gh_msg}" + (f" (Docs: {doc_url})" if doc_url else "")
                        elif response_text:
                             error_msg_detail = response_text[:150]

                        final_error_message = f"⚠️ GitHub upload failed for `{escape(clean_repo_name)}`.\nStatus: {response.status}\nError: `{escape(error_msg_detail)}`"
                        logger.error(f"Failed GitHub upload for user {user_id}. Status: {response.status}. Error: {error_msg_detail}. Raw Response: {response_text[:200]}")
                        await bot.edit_message_text(
                            chat_id=notify_chat_id, message_id=status_msg_obj.message_id,
                            text=final_error_message, parse_mode=ParseMode.MARKDOWN
                        )
                        upload_success = False

            except (asyncio.TimeoutError, aiohttp.ClientError) as e:
                 error_prefix = type(e).__name__
                 logger.error(f"{error_prefix} during GitHub PUT for user {user_id}: {e}")
                 await bot.edit_message_text(
                     chat_id=notify_chat_id, message_id=status_msg_obj.message_id,
                     text=f"⚠️ GitHub upload failed: {error_prefix} during upload: {escape(str(e))}"
                 )
                 upload_success = False
            except Exception as e:
                logger.error(f"Unexpected error during GitHub PUT for user {user_id}: {e}", exc_info=True)
                await bot.edit_message_text(
                    chat_id=notify_chat_id, message_id=status_msg_obj.message_id,
                    text=f"⚠️ Unexpected error during GitHub upload: {escape(str(e))}"
                )
                upload_success = False

    except Exception as e:
        logger.error(f"General GitHub background upload error for user {user_id}: {e}", exc_info=True)
        if status_msg_obj:
            try:
                await bot.edit_message_text(
                    chat_id=notify_chat_id, message_id=status_msg_obj.message_id,
                    text=f"⚠️ GitHub upload failed: An internal bot error occurred: {escape(str(e))}"
                )
            except TelegramError:
                 logger.error(f"Could not edit final error status for GitHub upload user {user_id}. Sending new.")
                 try:
                     await bot.send_message(notify_chat_id, f"⚠️ GitHub upload failed due to an internal error: {escape(str(e))}")
                 except Exception:
                      logger.critical(f"Failed even to send a final error message for GitHub upload user {user_id} after status edit failure.")
        else:
            logger.critical(f"Cannot update GitHub status_msg as it failed initially. General error: {e}")
            try:
                await bot.send_message(notify_chat_id, f"⚠️ GitHub upload failed due to an internal error: {escape(str(e))}")
            except Exception:
                logger.error("Failed even to send a final error message for GitHub upload after initial status failure.")
        upload_success = False

    return upload_success

# --- Direct GitHub Configuration Command ---
async def set_github_direct(update: Update, context: CallbackContext) -> None:
    """Handles the /setgithub command where users provide all arguments directly."""
    if not await check_channel_membership(update, context):
        return

    user = update.effective_user
    message = update.message
    if not user or not message: return
    user_id = user.id
    add_known_user(user.id)
    context.user_data.pop('pending_schedule', None)
    context.user_data.pop('waiting_for_json', None)

    if not is_user_vip(user.id):
        await message.reply_text(
            "❌ GitHub configuration is only available for VIP users. Use /vipshop to upgrade.",
            reply_markup=main_reply_markup
        )
        return

    args = context.args
    usage_text = (
        "⚙️ *GitHub Configuration Usage:*\n\n"
        "Provide all details in *one* command message:\n"
        "`/setgithub <TOKEN> <owner/repo> <branch> <filename.json>`\n\n"
        "*Example:*\n"
        "`/setgithub ghp_YourToken123 YourGitHubUser/MyRepo main my_tokens.json`\n\n"
        "⚠️ *Security Warning:*\nYour GitHub token will be visible in your command message. "
        "The bot will attempt to delete this message after saving, but *please manually delete it immediately* if the bot fails to do so, to protect your token."
    )

    if len(args) != 4:
        await message.reply_text(
            f"❌ Incorrect number of arguments. Expected 4, got {len(args)}.\n\n{usage_text}",
            parse_mode=ParseMode.MARKDOWN,
            disable_web_page_preview=True,
            reply_markup=main_reply_markup
        )
        return

    github_token, github_repo_raw, github_branch_raw, github_filename_raw = args
    user_id_str = str(user.id)

    validation_errors = []
    if not github_token or len(github_token) < 10:
        validation_errors.append("GitHub Token seems missing or too short.")

    github_repo = github_repo_raw.strip()
    if not github_repo or '/' not in github_repo or len(github_repo.split('/')) != 2 or not all(p.strip() for p in github_repo.split('/')) or github_repo.startswith('/') or github_repo.endswith('/') or ' ' in github_repo:
        validation_errors.append("Invalid Repository format. Use `owner/repository_name` (no spaces or leading/trailing slashes).")

    github_branch = github_branch_raw.strip()
    if not github_branch or ' ' in github_branch or github_branch.startswith('/') or github_branch.endswith('/'):
        validation_errors.append("Invalid Branch name (no spaces or leading/trailing slashes).")

    github_filename = github_filename_raw.strip()
    if not github_filename or not github_filename.lower().endswith('.json') or github_filename.startswith('/') or ' ' in github_filename:
        validation_errors.append("Invalid Filename. Must end with `.json`, contain no spaces, and not start with `/`.")

    if validation_errors:
        safe_errors = [escape(e) for e in validation_errors]
        error_message = "❌ Configuration validation failed:\n" + "\n".join(f"- {e}" for e in safe_errors)
        error_message += f"\n\n{usage_text}"
        await message.reply_text(
            error_message,
            parse_mode=ParseMode.MARKDOWN,
            disable_web_page_preview=True,
            reply_markup=main_reply_markup
        )
        return

    config_data = {
        'github_token': github_token,
        'github_repo': github_repo,
        'github_branch': github_branch,
        'github_filename': github_filename,
        'last_upload': None,
        'config_set_on': datetime.now(timezone.utc).isoformat()
    }
    logger.info(f"Received valid GitHub config via /setgithub from VIP user {user_id_str}. Saving...")

    github_configs = load_github_configs()
    github_configs[user_id_str] = config_data

    if save_github_configs(github_configs):
        logger.info(f"Successfully saved GitHub config for user {user_id_str}")

        masked_token = "****"
        if len(github_token) > 8:
            masked_token = github_token[:4] + "****" + github_token[-4:]
        elif github_token:
             masked_token = "****"

        safe_repo = escape(config_data['github_repo'])
        safe_branch = escape(config_data['github_branch'])
        safe_filename = escape(config_data['github_filename'])
        safe_masked_token = escape(masked_token)

        confirmation_message = (
            "✅ *GitHub Configuration Saved Successfully!*\n\n"
            f"• Repo: `{safe_repo}`\n"
            f"• Branch: `{safe_branch}`\n"
            f"• Filename: `{safe_filename}`\n"
            f"• Token: `{safe_masked_token}` (Masked)\n\n"
            "Auto-upload is now configured for future token generation results.\n\n"
            "⏳ *Attempting to delete your command message containing the token for security...*"
        )
        confirm_msg_obj = await message.reply_text(
            confirmation_message,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=main_reply_markup
        )

        delete_successful = False
        try:
            await message.delete()
            delete_successful = True
            logger.info(f"Successfully deleted user's /setgithub command message for user {user_id_str}")
            await context.bot.edit_message_text(
                chat_id=confirm_msg_obj.chat_id, message_id=confirm_msg_obj.message_id,
                text=confirmation_message.replace("⏳ *Attempting to delete your command message containing the token for security...*",
                                                   "✅ Your command message containing the token has been deleted."),
                parse_mode=ParseMode.MARKDOWN,
            )
        except TelegramError as e:
            logger.warning(f"Could not delete user's /setgithub command message for {user_id_str}: {e}. User needs to delete manually.")
            try:
                await context.bot.edit_message_text(
                    chat_id=confirm_msg_obj.chat_id, message_id=confirm_msg_obj.message_id,
                    text=confirmation_message.replace("⏳ *Attempting to delete your command message containing the token for security...*",
                                                       "⚠️ *Could not automatically delete your command message! Please delete it manually NOW to protect your token.*"),
                    parse_mode=ParseMode.MARKDOWN,
                )
            except TelegramError as edit_err:
                 logger.error(f"Failed to edit confirmation message to warn about manual deletion: {edit_err}")
                 await message.reply_text("⚠️ *IMPORTANT: Could not automatically delete your command message! Please delete the message containing your `/setgithub` command manually NOW to protect your token.*", parse_mode=ParseMode.MARKDOWN, reply_markup=main_reply_markup)

    else:
        logger.error(f"Failed to save GitHub configuration file for user {user_id_str}")
        await message.reply_text(
            "❌ **Error:** Could not save the GitHub configuration due to a file system error. Please try again later or contact the admin.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=main_reply_markup
        )

async def my_github_config(update: Update, context: CallbackContext) -> None:
    """Shows the VIP user's current GitHub configuration (with masked token)."""
    if not await check_channel_membership(update, context):
        return

    user = update.effective_user
    message = update.message
    if not user or not message: return
    user_id = user.id
    add_known_user(user.id)
    context.user_data.pop('pending_schedule', None)
    context.user_data.pop('waiting_for_json', None)

    if not is_user_vip(user.id):
        await message.reply_text(
            "ℹ️ GitHub auto-upload configuration is a VIP feature. Use /vipshop to upgrade.",
            reply_markup=main_reply_markup
        )
        return

    github_configs = load_github_configs()
    user_id_str = str(user.id)
    config = github_configs.get(user_id_str)

    if not config or not isinstance(config, dict):
        await message.reply_text(
            "ℹ️ GitHub auto-upload is not configured yet, or the stored configuration is invalid.\n\n"
            "Use the `/setgithub <TOKEN> <owner/repo> <branch> <filename.json>` command to set it up.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=main_reply_markup
        )
        return

    token = config.get('github_token', 'Not Set')
    masked_token = "Not Set"
    if isinstance(token, str) and token != 'Not Set':
        if len(token) > 8:
            masked_token = token[:4] + "****" + token[-4:]
        elif token:
            masked_token = "****"

    safe_repo = escape(config.get('github_repo', 'Not Set'))
    safe_branch = escape(config.get('github_branch', 'Not Set'))
    safe_filename = escape(config.get('github_filename', 'Not Set'))
    safe_masked_token = escape(masked_token)

    message_parts = [
        f"🔧 *Your Current GitHub Auto-Upload Config:*\n",
        f"• Repo: `{safe_repo}`",
        f"• Branch: `{safe_branch}`",
        f"• Filename: `{safe_filename}`",
        f"• Token: `{safe_masked_token}` (Masked)"
    ]

    last_upload_iso = config.get('last_upload')
    if last_upload_iso:
        try:
            last_upload_dt = datetime.fromisoformat(last_upload_iso.replace('Z', '+00:00'))
            last_upload_str = last_upload_dt.strftime('%Y-%m-%d %H:%M:%S UTC')
            message_parts.append(f"• Last Successful Upload: `{last_upload_str}`")
        except (ValueError, TypeError):
            safe_iso_snippet = escape(str(last_upload_iso)[:19])
            message_parts.append(f"• Last Successful Upload: `Invalid Date Stored ({safe_iso_snippet}...)`")
    else:
        message_parts.append("• Last Successful Upload: `Never`")

    config_set_on_iso = config.get('config_set_on')
    if config_set_on_iso:
         try:
             config_set_dt = datetime.fromisoformat(config_set_on_iso.replace('Z', '+00:00'))
             config_set_str = config_set_dt.strftime('%Y-%m-%d %H:%M:%S UTC')
             message_parts.append(f"• Config Set/Updated: `{config_set_str}`")
         except (ValueError, TypeError):
             pass

    message_parts.append("\nUse `/setgithub <TOKEN> ...` to update your configuration.")

    await message.reply_text(
        "\n".join(message_parts),
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=main_reply_markup,
        disable_web_page_preview=True
    )

# --- Scheduled File Commands ---

async def set_scheduled_file_start(update: Update, context: CallbackContext) -> None:
    """Starts the process of scheduling a file for automatic processing."""
    if not await check_channel_membership(update, context):
        return

    user = update.effective_user
    message = update.message
    if not user or not message: return
    user_id = user.id
    add_known_user(user.id)
    context.user_data.pop('waiting_for_json', None)

    if not is_user_vip(user.id):
        await message.reply_text(
            "❌ File scheduling is a VIP feature. Use /vipshop to upgrade.",
            reply_markup=main_reply_markup
        )
        return

    args = context.args
    usage_text = (
        "⚙️ *Schedule File for Auto-Processing*\n\n"
        "*Usage:* `/setfile <Interval> <ScheduleName.json>`\n"
        "*Interval:* Number followed by `m` (minutes), `h` (hours), or `d` (days). Min interval: 5m.\n"
        "*ScheduleName:* A name for this schedule, ending in `.json`.\n\n"
        "*Example:* `/setfile 12h my_main_accounts.json`\n\n"
        "After using the command, send the corresponding JSON file."
    )

    if len(args) != 2:
        await message.reply_text(
            f"❌ Incorrect number of arguments. Expected 2, got {len(args)}.\n\n{usage_text}",
            parse_mode=ParseMode.MARKDOWN, reply_markup=main_reply_markup
        )
        return

    interval_str, user_filename = args[0], args[1]

    interval_seconds = parse_interval(interval_str)
    min_interval_seconds = 5 * 60
    if interval_seconds is None:
        await message.reply_text(
            f"❌ Invalid interval format: `{escape(interval_str)}`. Use formats like `30m`, `6h`, `1d`.\n\n{usage_text}",
            parse_mode=ParseMode.MARKDOWN, reply_markup=main_reply_markup
        )
        return
    if interval_seconds < min_interval_seconds:
         await message.reply_text(
            f"❌ Interval is too short. Minimum interval is {format_time(min_interval_seconds)} (`{min_interval_seconds // 60}m`).\n\n{usage_text}",
            parse_mode=ParseMode.MARKDOWN, reply_markup=main_reply_markup
         )
         return

    if not user_filename.lower().endswith('.json'):
         await message.reply_text(
             f"❌ Schedule name must end with `.json`. You provided: `{escape(user_filename)}`.\n\n{usage_text}",
             parse_mode=ParseMode.MARKDOWN, reply_markup=main_reply_markup
         )
         return

    sanitized_name = sanitize_filename(user_filename)
    if not sanitized_name or sanitized_name == '.json':
         await message.reply_text(
             f"❌ Invalid schedule name after sanitization: `{escape(user_filename)}` became `{escape(sanitized_name)}`.\nChoose a more descriptive name.",
             parse_mode=ParseMode.MARKDOWN, reply_markup=main_reply_markup
         )
         return

    context.user_data['pending_schedule'] = {
        'interval_seconds': interval_seconds,
        'schedule_name': sanitized_name,
        'user_filename': user_filename
    }

    logger.info(f"User {user_id} initiated scheduling for '{sanitized_name}' with interval {interval_seconds}s. Waiting for file.")
    await message.reply_text(
        f"✅ Okay, schedule details accepted for `'{escape(user_filename)}'` "
        f"(Interval: {escape(interval_str)} = {format_time(interval_seconds)}).\n\n"
        f"📎 **Now, please send the JSON file** you want to associate with this schedule.\n\n"
        f"Use /cancel to abort.",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=ReplyKeyboardRemove()
    )

async def handle_scheduled_file_upload(update: Update, context: CallbackContext) -> None:
    """Handles the JSON file upload specifically for a pending schedule."""
    if not await check_channel_membership(update, context):
        return

    user = update.effective_user
    message = update.message
    if not user or not message or not message.document:
        logger.warning(f"handle_scheduled_file_upload called without user/message/document for user {user.id if user else 'Unknown'}")
        if context.user_data.get('pending_schedule'):
             await message.reply_text("Please send the JSON *file* to schedule, not text. Or use /cancel.", reply_markup=main_reply_markup)
        return

    user_id = user.id
    pending_schedule = context.user_data['pending_schedule']
    schedule_name = pending_schedule['schedule_name']
    user_filename = pending_schedule['user_filename']
    interval_seconds = pending_schedule['interval_seconds']

    document = message.document
    original_telegram_filename = document.file_name or f"file_{document.file_id}.json"

    is_json_mime = document.mime_type and document.mime_type.lower() == 'application/json'
    has_json_extension = original_telegram_filename and original_telegram_filename.lower().endswith('.json')
    if not is_json_mime and not has_json_extension:
        await message.reply_text(f"❌ The file you sent (`{escape(original_telegram_filename)}`) doesn't seem to be a JSON file (.json). Schedule cancelled.", reply_markup=main_reply_markup)
        context.user_data.pop('pending_schedule', None)
        return

    if document.file_size and document.file_size > MAX_FILE_SIZE:
        await message.reply_text(
            f"⚠️ File is too large ({document.file_size / 1024 / 1024:.2f} MB). Max: {MAX_FILE_SIZE / 1024 / 1024:.1f} MB. Schedule cancelled.",
            reply_markup=main_reply_markup
        )
        context.user_data.pop('pending_schedule', None)
        return

    temp_download_path = os.path.join(TEMP_DIR, f'schedule_down_{user_id}_{schedule_name}_{int(time.time())}.json')
    persistent_file_path = os.path.join(SCHEDULED_FILES_DATA_DIR, f"{user_id}_{schedule_name}")
    progress_msg = None

    try:
        os.makedirs(TEMP_DIR, exist_ok=True)
        os.makedirs(SCHEDULED_FILES_DATA_DIR, exist_ok=True)

        progress_msg = await message.reply_text(f"⏳ Downloading `{escape(original_telegram_filename)}` for schedule `'{escape(user_filename)}'`...", parse_mode=ParseMode.MARKDOWN)

        bot_file = await context.bot.get_file(document.file_id)
        await bot_file.download_to_drive(temp_download_path)
        logger.info(f"Downloaded file for schedule '{schedule_name}' (user {user_id}) to temp path: {temp_download_path}")

        actual_size = os.path.getsize(temp_download_path)
        if actual_size > MAX_FILE_SIZE:
             raise ValueError(f"Downloaded file size ({actual_size / 1024 / 1024:.2f} MB) exceeds limit.")

        try:
            with open(temp_download_path, 'r', encoding='utf-8') as f_check:
                content = json.load(f_check)
                if not isinstance(content, list):
                     raise ValueError("JSON content must be an array (list).")
                if content and not all(isinstance(item, dict) for item in content):
                     first_bad = next((x for x in content if not isinstance(x, dict)), None)
                     raise ValueError(f"All items in the list must be JSON objects (`{{...}}`). Found: {type(first_bad)}")
                logger.info(f"Scheduler: JSON syntax and structure validation passed for scheduled file '{schedule_name}' (user {user_id}).")
        except json.JSONDecodeError as json_err:
             error_line_info = ""
             if hasattr(json_err, 'lineno') and hasattr(json_err, 'colno'):
                 error_line_info = f" near line {json_err.lineno}, column {json_err.colno}"
             raise ValueError(f"Invalid JSON format in the uploaded file{error_line_info}. Error: {json_err.msg}")
        except ValueError as val_err:
             raise val_err
        except Exception as read_err:
             raise ValueError(f"Could not read or validate the downloaded file: {read_err}")

        shutil.move(temp_download_path, persistent_file_path)
        logger.info(f"Stored file for schedule '{schedule_name}' (user {user_id}) persistently at: {persistent_file_path}")

        schedules = load_scheduled_files()
        user_id_str = str(user_id)
        now_utc = datetime.now(timezone.utc)
        next_run_time = now_utc + timedelta(seconds=interval_seconds)

        if user_id_str not in schedules:
            schedules[user_id_str] = {}

        schedules[user_id_str][schedule_name] = {
            'interval_seconds': interval_seconds,
            'telegram_file_id': document.file_id,
            'stored_file_path': persistent_file_path,
            'last_run_time_iso': None,
            'next_run_time_iso': next_run_time.isoformat(),
            'added_on_iso': now_utc.isoformat(),
            'original_telegram_filename': original_telegram_filename,
            'user_schedule_name': user_filename
        }

        if save_scheduled_files(schedules):
            logger.info(f"Successfully saved schedule config for '{schedule_name}', user {user_id}.")
            confirmation_text = (
                f"✅ **File Schedule Set Successfully!**\n\n"
                f"🏷️ **Schedule Name:** `{escape(user_filename)}`\n"
                f"📄 **Associated File:** `{escape(original_telegram_filename)}`\n"
                f"🔄 **Interval:** {format_time(interval_seconds)}\n"
                f"⏰ **Next Run:** `{next_run_time.strftime('%Y-%m-%d %H:%M:%S UTC')}` (approximately)\n\n"
                f"The bot will now automatically process this file and upload tokens to GitHub (if configured) every {format_time(interval_seconds)}.\n"
                f"Use /scheduledfiles to view or /removefile to stop."
            )
            await context.bot.edit_message_text(
                chat_id=progress_msg.chat_id, message_id=progress_msg.message_id,
                text=confirmation_text, parse_mode=ParseMode.MARKDOWN
            )
            context.user_data.pop('pending_schedule', None)
        else:
             logger.error(f"Failed to save schedule config file after setting '{schedule_name}' for user {user_id}.")
             if os.path.exists(persistent_file_path):
                 try: os.remove(persistent_file_path)
                 except OSError as del_err: logger.error(f"Failed cleanup: Could not delete stored file {persistent_file_path} after config save error: {del_err}")
             raise IOError("Failed to save the updated schedule configuration file.")

    except (ValueError, IOError, OSError, TelegramError) as e:
        logger.error(f"Error setting up schedule '{schedule_name}' for user {user_id}: {e}", exc_info=False)
        error_text = f"❌ Error setting up schedule `'{escape(user_filename)}'`:\n`{escape(str(e))}`\n\nPlease try again or use /cancel."
        if progress_msg:
            await context.bot.edit_message_text(
                chat_id=progress_msg.chat_id, message_id=progress_msg.message_id,
                text=error_text, parse_mode=ParseMode.MARKDOWN
            )
        else:
            await message.reply_text(error_text, reply_markup=main_reply_markup, parse_mode=ParseMode.MARKDOWN)
        if os.path.exists(persistent_file_path):
            try: os.remove(persistent_file_path)
            except OSError: logger.warning(f"Could not clean up stored schedule file after error: {persistent_file_path}")
        context.user_data.pop('pending_schedule', None)
    except Exception as e:
        logger.error(f"Unexpected error setting up schedule '{schedule_name}' for user {user_id}: {e}", exc_info=True)
        error_text = f"❌ An unexpected error occurred while setting up the schedule `'{escape(user_filename)}'`. Schedule cancelled."
        if progress_msg:
             try:
                 await context.bot.edit_message_text(
                      chat_id=progress_msg.chat_id, message_id=progress_msg.message_id,
                      text=error_text, parse_mode=ParseMode.MARKDOWN
                  )
             except TelegramError as edit_err:
                 logger.error(f"Failed to edit progress message in general exception block: {edit_err}")
                 await message.reply_text(error_text, reply_markup=main_reply_markup, parse_mode=ParseMode.MARKDOWN)
        else:
             await message.reply_text(error_text, reply_markup=main_reply_markup, parse_mode=ParseMode.MARKDOWN)
        if os.path.exists(persistent_file_path):
            try: os.remove(persistent_file_path)
            except OSError: pass
        context.user_data.pop('pending_schedule', None)
    finally:
        if os.path.exists(temp_download_path):
            try: os.remove(temp_download_path)
            except OSError as e: logger.warning(f"Could not remove temp schedule download file {temp_download_path}: {e}")

async def remove_scheduled_file(update: Update, context: CallbackContext) -> None:
    """Removes a specific scheduled file configuration and its stored data."""
    if not await check_channel_membership(update, context):
        return

    user = update.effective_user
    message = update.message
    if not user or not message: return
    user_id = user.id
    add_known_user(user.id)
    context.user_data.pop('pending_schedule', None)
    context.user_data.pop('waiting_for_json', None)

    if not is_user_vip(user.id):
        await message.reply_text(
            "❌ File scheduling management is a VIP feature.",
            reply_markup=main_reply_markup
        )
        return

    args = context.args
    usage_text = "Usage: `/removefile <ScheduleName.json>` (Use the name you provided during `/setfile`)"

    if len(args) != 1:
        await message.reply_text(
            f"❌ Incorrect number of arguments.\n\n{usage_text}",
            parse_mode=ParseMode.MARKDOWN, reply_markup=main_reply_markup
        )
        return

    user_filename_to_remove = args[0]
    sanitized_name_to_remove = sanitize_filename(user_filename_to_remove)

    schedules = load_scheduled_files()
    user_id_str = str(user_id)

    if user_id_str not in schedules or sanitized_name_to_remove not in schedules[user_id_str]:
        await message.reply_text(
            f"ℹ️ No schedule found with the name `'{escape(user_filename_to_remove)}'`. "
            f"Use /scheduledfiles to see your active schedules.",
            parse_mode=ParseMode.MARKDOWN, reply_markup=main_reply_markup
        )
        return

    schedule_info = schedules[user_id_str][sanitized_name_to_remove]
    stored_file_path = schedule_info.get('stored_file_path')
    display_name = schedule_info.get('user_schedule_name', sanitized_name_to_remove)

    del schedules[user_id_str][sanitized_name_to_remove]
    if not schedules[user_id_str]:
        del schedules[user_id_str]

    config_save_success = save_scheduled_files(schedules)
    file_delete_success = False
    file_delete_error = None

    if stored_file_path and os.path.exists(stored_file_path):
        try:
            os.remove(stored_file_path)
            file_delete_success = True
            logger.info(f"Deleted stored file for schedule '{sanitized_name_to_remove}' user {user_id}: {stored_file_path}")
        except OSError as e:
            file_delete_error = str(e)
            logger.error(f"Failed to delete stored file {stored_file_path} for schedule '{sanitized_name_to_remove}' user {user_id}: {e}")

    response_parts = []
    if config_save_success:
        response_parts.append(f"✅ Schedule `'{escape(display_name)}'` removed successfully.")
        logger.info(f"Removed schedule '{sanitized_name_to_remove}' for user {user_id}.")
    else:
        response_parts.append(f"⚠️ Failed to save the configuration after removing schedule `'{escape(display_name)}'`. It might reappear temporarily.")

    if stored_file_path:
        if file_delete_success:
            response_parts.append("✅ Associated stored file deleted.")
        elif file_delete_error:
            response_parts.append(f"⚠️ Could not delete the associated stored file: {escape(file_delete_error)}")
        elif not os.path.exists(stored_file_path):
             response_parts.append("ℹ️ Associated stored file was already missing or path was invalid.")
    else:
        response_parts.append("ℹ️ No stored file path found in config for this schedule.")

    await message.reply_text("\n".join(response_parts), parse_mode=ParseMode.MARKDOWN, reply_markup=main_reply_markup)

async def list_scheduled_files(update: Update, context: CallbackContext) -> None:
    """Lists the user's currently active scheduled files."""
    if not await check_channel_membership(update, context):
        return

    user = update.effective_user
    message = update.message
    if not user or not message: return
    user_id = user.id
    add_known_user(user.id)
    context.user_data.pop('pending_schedule', None)
    context.user_data.pop('waiting_for_json', None)

    if not is_user_vip(user.id):
        await message.reply_text(
            "ℹ️ File scheduling is a VIP feature. Use /vipshop to upgrade.",
            reply_markup=main_reply_markup
        )
        return

    schedules = load_scheduled_files()
    user_id_str = str(user.id)
    user_schedules = schedules.get(user_id_str, {})

    if not user_schedules:
        await message.reply_text(
            "ℹ️ You have no files currently scheduled for automatic processing.\n\n"
            "Use `/setfile <Interval> <ScheduleName.json>` to set one up.",
            parse_mode=ParseMode.MARKDOWN, reply_markup=main_reply_markup
        )
        return

    message_parts = ["⚙️ *Your Scheduled Files for Auto-Processing:*\n"]
    now_utc = datetime.now(timezone.utc)

    sorted_schedule_items = sorted(
        user_schedules.items(),
        key=lambda item: item[1].get('user_schedule_name', item[0])
    )

    for schedule_name, details in sorted_schedule_items:
        if not isinstance(details, dict): continue

        user_display_name = details.get('user_schedule_name', schedule_name)
        interval_s = details.get('interval_seconds')
        next_run_iso = details.get('next_run_time_iso')
        last_run_iso = details.get('last_run_time_iso')
        original_tg_file = details.get('original_telegram_filename', 'N/A')

        message_parts.append(f"\n🏷️ **Name:** `{escape(user_display_name)}`")
        message_parts.append(f"   📄 *Source File:* `{escape(original_tg_file)}`")
        if interval_s and isinstance(interval_s, int):
            message_parts.append(f"   🔄 *Interval:* {format_time(interval_s)}")
        else:
            message_parts.append(f"   🔄 *Interval:* `Error: Invalid/Not Set`")

        if next_run_iso:
            try:
                next_run_dt = datetime.fromisoformat(next_run_iso.replace('Z', '+00:00'))
                next_run_formatted = next_run_dt.strftime('%Y-%m-%d %H:%M UTC')
                time_until_next = next_run_dt - now_utc
                if time_until_next.total_seconds() > 0:
                    remaining_str = format_time(time_until_next.total_seconds())
                    message_parts.append(f"   ⏰ *Next Run:* {next_run_formatted} (`{remaining_str}`)")
                else:
                    message_parts.append(f"   ⏰ *Next Run:* {next_run_formatted} (`Due now or overdue`)")

            except (ValueError, TypeError):
                message_parts.append(f"   ⏰ *Next Run:* `Error: Invalid Date ({escape(str(next_run_iso)[:19])})`")
        else:
             message_parts.append(f"   ⏰ *Next Run:* `Not Scheduled Yet / Error`")

        if last_run_iso:
             try:
                 last_run_dt = datetime.fromisoformat(last_run_iso.replace('Z', '+00:00'))
                 last_run_formatted = last_run_dt.strftime('%Y-%m-%d %H:%M UTC')
                 message_parts.append(f"   ⏱️ *Last Run:* {last_run_formatted}")
             except (ValueError, TypeError):
                 message_parts.append(f"   ⏱️ *Last Run:* `Invalid Date`")
        else:
             message_parts.append(f"   ⏱️ *Last Run:* `Never`")

    message_parts.append("\nUse `/removefile <ScheduleName.json>` to stop a schedule.")

    final_message = "\n".join(message_parts)
    if len(final_message) > 4096:
        await message.reply_text("Your list of scheduled files is too long to display fully. Showing the first part:")
        safe_truncate_point = final_message[:4050].rfind('\n')
        if safe_truncate_point == -1: safe_truncate_point = 4050
        await message.reply_text(final_message[:safe_truncate_point]+"...", parse_mode=ParseMode.MARKDOWN, reply_markup=main_reply_markup)
    else:
        await message.reply_text(final_message, parse_mode=ParseMode.MARKDOWN, reply_markup=main_reply_markup)

# --- Admin Commands ---

async def vip_management(update: Update, context: CallbackContext) -> None:
    """Manage VIP users: add, remove, list (admin only)."""
    user = update.effective_user
    message = update.message
    if not user or not message or not ADMIN_ID or user.id != ADMIN_ID:
        logger.warning(f"Unauthorized access attempt to /vip by user {user.id if user else 'Unknown'}")
        if message: await message.reply_text("You are not authorized to use this command.", reply_markup=main_reply_markup)
        return
    if message.chat.type != 'private':
         await message.reply_text("Admin commands must be used in a private chat with the bot.")
         return

    args = context.args
    command_usage = (
        "👑 *Admin VIP Management*\n\n"
        "*Usage:*\n"
        "`/vip add <user_id> <days>` - Add/extend VIP\n"
        "`/vip remove <user_id>` - Remove VIP, GitHub config & *ALL* user's scheduled files\n"
        "`/vip list` - List VIPs (active & expired)\n\n"
        "*Example:* `/vip add 123456789 30`"
    )

    if not args:
        await message.reply_text(command_usage, parse_mode=ParseMode.MARKDOWN)
        return

    action = args[0].lower()
    vip_data = load_vip_data()

    if action == 'add':
        if len(args) != 3:
            return await message.reply_text(f"⚠️ Incorrect arguments for 'add'.\n\n{command_usage}", parse_mode=ParseMode.MARKDOWN)

        try:
            target_user_id_str, days_str = args[1], args[2]
            if not target_user_id_str.isdigit() or not days_str.isdigit():
                return await message.reply_text("⚠️ Invalid User ID or Days. Both must be numbers.")

            target_user_id = int(target_user_id_str)
            days_to_add = int(days_str)

            if days_to_add <= 0:
                 return await message.reply_text("⚠️ Number of days must be positive.")

            now_utc = datetime.now(timezone.utc)
            start_date_for_calc = now_utc

            user_vip_info = vip_data.get(target_user_id_str, {})
            if not isinstance(user_vip_info, dict): user_vip_info = {}

            is_extending = False
            if target_user_id_str in vip_data:
                try:
                    current_expiry_iso = user_vip_info.get('expiry')
                    if current_expiry_iso:
                        current_expiry_dt = datetime.fromisoformat(current_expiry_iso.replace('Z', '+00:00'))
                        if current_expiry_dt > now_utc:
                            start_date_for_calc = current_expiry_dt
                            is_extending = True
                            logger.info(f"Extending existing VIP for {target_user_id} from {current_expiry_dt.isoformat()}")
                except (ValueError, TypeError, KeyError) as e:
                    logger.warning(f"Invalid expiry format ('{user_vip_info.get('expiry')}') for user {target_user_id_str} in VIP data: {e}. Starting new period from now.")
                    user_vip_info = {}

            new_expiry_date = start_date_for_calc + timedelta(days=days_to_add)

            user_vip_info.update({
                'expiry': new_expiry_date.isoformat(),
                'added_by': user.id,
                'added_on': user_vip_info.get('added_on', now_utc.isoformat()),
                'last_update': now_utc.isoformat()
            })
            vip_data[target_user_id_str] = user_vip_info

            if save_vip_data(vip_data):
                expiry_formatted_display = new_expiry_date.strftime('%Y-%m-%d %H:%M:%S UTC')
                action_word = "Extended" if is_extending else "Added"
                response_msg = f"✅ VIP {action_word} for User ID `{target_user_id}`.\nDuration Added: {days_to_add} days\nNew Expiry: `{expiry_formatted_display}`"
                logger.info(f"Admin {user.id} {action_word.lower()} VIP for {target_user_id} to {expiry_formatted_display}")
                await message.reply_text(response_msg, parse_mode=ParseMode.MARKDOWN)

                try:
                    target_user_info_str = f"User ID `{target_user_id}`"
                    try:
                        chat_info = await context.bot.get_chat(target_user_id)
                        name_parts = []
                        if chat_info.username: name_parts.append(f"@{escape(chat_info.username)}")
                        elif chat_info.first_name: name_parts.append(escape(chat_info.first_name))
                        if name_parts:
                            target_user_info_str = f"{' '.join(name_parts)} (`{target_user_id}`)"
                    except TelegramError as chat_err:
                        logger.warning(f"Could not get chat info for {target_user_id} during VIP notification: {chat_err}")

                    admin_name = escape(user.first_name) or f"Admin (`{user.id}`)"

                    added_on_str = "Unknown"
                    added_on_iso = user_vip_info.get('added_on')
                    if added_on_iso:
                        try:
                            added_on_dt = datetime.fromisoformat(added_on_iso.replace('Z', '+00:00'))
                            added_on_str = added_on_dt.strftime('%Y-%m-%d %H:%M:%S UTC')
                        except (ValueError, TypeError): added_on_str = f"Invalid ({escape(added_on_iso[:19])})"

                    vip_dm_message = (
                        f"🎉 Congratulations! Your VIP status has been {'updated' if is_extending else 'activated'}!\n\n"
                        f"📊 *Status:* Active VIP ✔️\n"
                        f"🆔 *User:* {target_user_info_str}\n"
                        f"📅 *Subscription Start:* `{added_on_str}`\n"
                        f"📅 *Expires:* `{expiry_formatted_display}`\n"
                        f"👤 *Updated by:* {admin_name}\n\n"
                        "Enjoy your premium features, including GitHub auto-upload and file scheduling!"
                    )

                    await context.bot.send_message(target_user_id, vip_dm_message, parse_mode=ParseMode.MARKDOWN)
                    await message.reply_text(f"✅ User `{target_user_id}` notified of the update.", parse_mode=ParseMode.MARKDOWN, disable_notification=True)

                except Forbidden:
                    logger.warning(f"Could not notify user {target_user_id} about VIP update (Forbidden: Bot blocked or user deactivated).")
                    await message.reply_text(f"⚠️ Could not notify user `{target_user_id}` (Bot blocked or user left).", parse_mode=ParseMode.MARKDOWN, disable_notification=True)
                except BadRequest as notify_err:
                    logger.warning(f"Could not notify user {target_user_id} about VIP update (BadRequest): {notify_err}")
                    await message.reply_text(f"⚠️ Could not notify user `{target_user_id}`: {escape(str(notify_err))}", parse_mode=ParseMode.MARKDOWN, disable_notification=True)
                except TelegramError as notify_err:
                    logger.warning(f"Could not notify user {target_user_id} about VIP update (TelegramError): {notify_err}")
                    await message.reply_text(f"⚠️ Could not notify user `{target_user_id}`: {escape(str(notify_err))}", parse_mode=ParseMode.MARKDOWN, disable_notification=True)
                except Exception as notify_err:
                     logger.error(f"Unexpected error notifying user {target_user_id} about VIP update: {notify_err}", exc_info=True)
                     await message.reply_text(f"⚠️ Unexpected error creating/sending notification to user `{target_user_id}`.", parse_mode=ParseMode.MARKDOWN, disable_notification=True)

            else:
                logger.error(f"Failed to save VIP data file after attempting to add/extend for user {target_user_id_str}")
                await message.reply_text("❌ **Error:** Could not save updated VIP data to file. The change was not applied.")

        except ValueError:
            await message.reply_text("⚠️ Invalid number format for User ID or Days.")
        except Exception as e:
            logger.error(f"Error processing '/vip add' command: {e}", exc_info=True)
            await message.reply_text(f"An unexpected error occurred during VIP addition: {escape(str(e))}")

    elif action == 'remove':
        if len(args) != 2:
            return await message.reply_text(f"⚠️ Incorrect arguments for 'remove'.\n\n{command_usage}", parse_mode=ParseMode.MARKDOWN)

        target_user_id_str = args[1]
        if not target_user_id_str.isdigit():
            return await message.reply_text("⚠️ Invalid User ID format. Must be a number.")

        target_user_id = int(target_user_id_str)
        removed_vip, removed_github, removed_schedules = False, False, False
        schedule_files_deleted_count = 0
        vip_save_error, github_save_error, schedule_save_error = False, False, False
        file_delete_errors = []
        response_parts = []

        was_vip = target_user_id_str in vip_data
        if was_vip:
            del vip_data[target_user_id_str]
            if save_vip_data(vip_data):
                removed_vip = True
                response_parts.append(f"✅ Successfully removed VIP status for `{target_user_id_str}`.")
                logger.info(f"Admin {user.id} removed VIP for {target_user_id_str}.")
            else:
                vip_save_error = True
                response_parts.append(f"❌ Error saving VIP data after attempting removal for `{target_user_id_str}`.")
                logger.error(f"Failed to save VIP data after removing {target_user_id_str}.")
                vip_data = load_vip_data()
        else:
            response_parts.append(f"ℹ️ User `{target_user_id_str}` was not found in the VIP list.")

        github_configs = load_github_configs()
        was_github_config = target_user_id_str in github_configs
        if was_github_config:
            del github_configs[target_user_id_str]
            if save_github_configs(github_configs):
                removed_github = True
                response_parts.append(f"✅ Successfully removed associated GitHub config for `{target_user_id_str}`.")
                logger.info(f"Removed GitHub config for {target_user_id_str} during VIP removal.")
            else:
                github_save_error = True
                response_parts.append(f"❌ Error saving GitHub config data after attempting removal for `{target_user_id_str}`.")
                logger.error(f"Failed to save GitHub config data after removing for {target_user_id_str}.")

        schedules_data = load_scheduled_files()
        user_schedules = schedules_data.get(target_user_id_str, {})
        if user_schedules:
            schedule_names_to_remove = list(user_schedules.keys())
            paths_to_delete = [info.get('stored_file_path') for info in user_schedules.values() if info.get('stored_file_path')]

            del schedules_data[target_user_id_str]
            if save_scheduled_files(schedules_data):
                removed_schedules = True
                response_parts.append(f"✅ Successfully removed {len(schedule_names_to_remove)} scheduled file configuration(s) for `{target_user_id_str}`.")
                logger.info(f"Removed {len(schedule_names_to_remove)} schedule configs for {target_user_id_str} during VIP removal.")

                for file_path in paths_to_delete:
                    if file_path and os.path.exists(file_path):
                        try:
                            os.remove(file_path)
                            schedule_files_deleted_count += 1
                            logger.info(f"Deleted stored schedule file: {file_path}")
                        except OSError as e:
                            file_delete_errors.append(os.path.basename(file_path))
                            logger.error(f"Error deleting stored schedule file {file_path}: {e}")
                if paths_to_delete:
                     if not file_delete_errors:
                         response_parts.append(f"✅ Deleted {schedule_files_deleted_count} associated stored file(s).")
                     else:
                          safe_failed_files = escape(", ".join(file_delete_errors))
                          response_parts.append(f"⚠️ Deleted {schedule_files_deleted_count} stored file(s), but failed to delete: `{safe_failed_files}`")

            else:
                 schedule_save_error = True
                 response_parts.append(f"❌ Error saving schedule config data after attempting removal for `{target_user_id_str}`.")
                 logger.error(f"Failed to save schedule config data after removing for {target_user_id_str}.")

        await message.reply_text("\n".join(response_parts) if response_parts else "No action taken or user not found.", parse_mode=ParseMode.MARKDOWN)

        if removed_vip:
            try:
                notify_message = "ℹ️ Your VIP status has been removed by an admin."
                removed_features = []
                if removed_github: removed_features.append("GitHub upload config")
                if removed_schedules: removed_features.append("scheduled file processing")
                if removed_features:
                    notify_message += f"\nAssociated features removed: {', '.join(removed_features)}."

                await context.bot.send_message(chat_id=target_user_id, text=notify_message)
                await message.reply_text(f"✅ User `{target_user_id}` notified of removal.", parse_mode=ParseMode.MARKDOWN, disable_notification=True)
            except Forbidden:
                 logger.warning(f"Could not notify user {target_user_id} about VIP removal (Forbidden).")
                 await message.reply_text(f"⚠️ Could not notify user `{target_user_id}` of removal (Bot blocked or user left).", parse_mode=ParseMode.MARKDOWN, disable_notification=True)
            except BadRequest as notify_err:
                 logger.warning(f"Could not notify user {target_user_id} about VIP removal (BadRequest): {notify_err}")
                 await message.reply_text(f"⚠️ Could not notify user `{target_user_id}` of removal: {escape(str(notify_err))}", parse_mode=ParseMode.MARKDOWN, disable_notification=True)
            except TelegramError as notify_err:
                 logger.warning(f"Could not notify user {target_user_id} about VIP removal (TelegramError): {notify_err}")
                 await message.reply_text(f"⚠️ Could not notify user `{target_user_id}` of removal: {escape(str(notify_err))}", parse_mode=ParseMode.MARKDOWN, disable_notification=True)
            except Exception as notify_err:
                 logger.error(f"Unexpected error notifying user {target_user_id} of removal: {notify_err}", exc_info=True)
                 await message.reply_text(f"⚠️ Unexpected error notifying user `{target_user_id}` of removal.", parse_mode=ParseMode.MARKDOWN, disable_notification=True)

    elif action == 'list':
        active_vips, inactive_vips, invalid_entries = [], [], []
        now_utc = datetime.now(timezone.utc)

        for uid_str, data in vip_data.items():
            safe_uid_str = escape(uid_str)
            if not isinstance(data, dict):
                invalid_entries.append(f"ID: `{safe_uid_str}` | Invalid data format (not a dictionary)")
                continue

            try:
                expiry_iso = data.get('expiry')
                if not expiry_iso:
                    invalid_entries.append(f"ID: `{safe_uid_str}` | Missing 'expiry' date field")
                    continue

                expiry_dt = datetime.fromisoformat(expiry_iso.replace('Z', '+00:00'))
                expiry_fmt = expiry_dt.strftime('%Y-%m-%d %H:%M UTC')

                if expiry_dt > now_utc:
                    rem_delta = expiry_dt - now_utc
                    days = rem_delta.days
                    hours, remainder = divmod(rem_delta.seconds, 3600)
                    minutes, _ = divmod(remainder, 60)

                    rem_str = ""
                    if days > 0: rem_str += f"{days}d "
                    if hours > 0: rem_str += f"{hours}h "
                    if days <= 0 and hours <= 0 and minutes > 0 : rem_str += f"{minutes}m"
                    if not rem_str.strip(): rem_str = "< 1m"
                    rem_str = rem_str.strip()

                    active_vips.append(f"✅ ID: `{safe_uid_str}` | Expires: {expiry_fmt} | Rem: `{escape(rem_str)}`")
                else:
                    inactive_vips.append(f"❌ ID: `{safe_uid_str}` | Expired: {expiry_fmt}")

            except (ValueError, TypeError):
                safe_iso_snippet = escape(str(expiry_iso)[:25])
                invalid_entries.append(f"ID: `{safe_uid_str}` | Invalid date format stored ('{safe_iso_snippet}...')")
            except Exception as e:
                invalid_entries.append(f"ID: `{safe_uid_str}` | Error processing entry: {escape(str(e))}")

        message_parts = [f"🌟 *VIP User List* ({len(active_vips)} Active)\n"]

        if active_vips:
            active_vips.sort(key=lambda x: datetime.strptime(re.search(r'Expires: (\d{4}-\d{2}-\d{2} \d{2}:\d{2}) UTC', x).group(1), '%Y-%m-%d %H:%M'))
            message_parts.append("*Active VIPs:*")
            message_parts.extend(active_vips)
        else:
            message_parts.append("No active VIP users found.")

        if inactive_vips:
             inactive_vips.sort(key=lambda x: int(re.search(r'ID: `(\d+)`', x).group(1)) if re.search(r'ID: `(\d+)`', x) else 0)
             message_parts.append(f"\n*Expired VIPs ({len(inactive_vips)}):*")
             message_parts.extend(inactive_vips)

        if invalid_entries:
            invalid_entries.sort(key=lambda x: int(re.search(r'ID: `(\d+)`', x).group(1)) if re.search(r'ID: `(\d+)`', x) else 0)
            message_parts.append(f"\n*Invalid/Error Entries ({len(invalid_entries)}):*")
            message_parts.extend(invalid_entries)

        message_parts.append(f"\nTotal entries in VIP file: {len(vip_data)}")

        final_message = "\n".join(message_parts)

        if len(final_message) > 4096:
            logger.warning("VIP list message exceeds 4096 characters. Sending truncated message.")
            split_point = final_message.rfind('\n', 0, 4080)
            if split_point == -1: split_point = 4080
            await message.reply_text(final_message[:split_point] + "\n\n...(list truncated due to length)", parse_mode=ParseMode.MARKDOWN)
        else:
            await message.reply_text(final_message, parse_mode=ParseMode.MARKDOWN)

    else:
        await message.reply_text(f"⚠️ Invalid action '{escape(action)}'.\n\n" + command_usage, parse_mode=ParseMode.MARKDOWN)

async def broadcast(update: Update, context: CallbackContext) -> None:
    """Sends a message to all known users (admin only). Supports Markdown/HTML."""
    user = update.effective_user
    message = update.message;

    if not user or not message or not ADMIN_ID or user.id != ADMIN_ID:
        logger.warning(f"Unauthorized access attempt to /broadcast by user {user.id if user else 'Unknown'}")
        if message: await message.reply_text("You are not authorized to use this command.")
        return
    if message.chat.type != 'private':
         await message.reply_text("Broadcast must be initiated from a private chat with the bot.")
         return

    message_to_send = ""
    parse_mode_to_use = None

    replied_message = message.reply_to_message
    if replied_message:
        message_to_send = replied_message.text or replied_message.caption or ""
        if replied_message.text_html or replied_message.caption_html:
            parse_mode_to_use = ParseMode.HTML
            message_to_send = replied_message.text_html or replied_message.caption_html
            logger.debug("Using HTML parse mode for broadcast from replied message.")
        elif replied_message.text_markdown_v2 or replied_message.caption_markdown_v2:
             parse_mode_to_use = ParseMode.MARKDOWN_V2
             message_to_send = replied_message.text_markdown_v2 or replied_message.caption_markdown_v2
             logger.debug("Using MarkdownV2 parse mode for broadcast from replied message.")
    else:
        if message.text:
            text_content = message.text
            command_pattern = rf"^\s*/broadcast(?:@{context.bot.username})?\s+"
            message_to_send = re.sub(command_pattern, '', text_content, count=1, flags=re.IGNORECASE | re.DOTALL).strip()

            if message.entities:
                 if any(e.type in ['bold', 'italic', 'code', 'pre', 'text_link', 'strikethrough', 'underline', 'spoiler'] for e in message.entities):
                     parse_mode_to_use = ParseMode.MARKDOWN_V2
                     message_to_send = message.text_markdown_v2
                     logger.debug(f"Detected MarkdownV2 entities, using MarkdownV2 for broadcast.")

    if not message_to_send:
         return await message.reply_text(
             "Usage: `/broadcast <Your message here>`\n"
             "Or reply to the message you want to broadcast with just `/broadcast`.\n"
             "(You should use MarkdownV2 or HTML formatting in your message).",
             parse_mode=ParseMode.MARKDOWN
         )

    known_users = load_known_users()
    if not known_users:
         return await message.reply_text("ℹ️ No known users found in the database to broadcast to.")

    total_users = len(known_users)

    logger.info(f"Admin {user.id} initiated broadcast to {total_users} users. ParseMode: {parse_mode_to_use}. Message (start): '{message_to_send[:50]}...'")
    status_message_obj = None
    try:
        status_message_obj = await message.reply_text(f"📣 Broadcasting to {total_users} users... Starting now.")
    except TelegramError as e:
        logger.error(f"Failed to send initial broadcast status message: {e}")
        await message.reply_text(f"⚠️ Failed to start broadcast status tracking. Attempting to send anyway...")
        status_message_obj = None

    success, fail, blocked, other_fail = 0, 0, 0, 0
    start_time = time.time()
    update_interval_count = max(10, min(100, total_users // 20 if total_users >= 200 else 10))
    last_status_update_time = time.time()

    user_list = list(known_users)
    users_to_remove = set()

    for i, user_id_to_send in enumerate(user_list):
        if user_id_to_send == ADMIN_ID:
            logger.debug("Skipping broadcast to admin self.")
            continue

        sent = False
        try:
            await context.bot.send_message(
                chat_id=user_id_to_send,
                text=message_to_send,
                parse_mode=parse_mode_to_use,
                disable_web_page_preview=True
            )
            success += 1
            sent = True
            logger.debug(f"Broadcast: Sent to {user_id_to_send}")
        except Forbidden:
            blocked += 1
            users_to_remove.add(user_id_to_send)
            logger.debug(f"Broadcast: User {user_id_to_send} blocked the bot or is deactivated.")
        except BadRequest as e:
            error_str = str(e).lower()
            if any(sub in error_str for sub in ["chat not found", "user is deactivated", "bot was kicked", "user not found", "peer_id_invalid", "bot_blocked_by_user", "group chat was deactivated"]):
                 blocked += 1
                 users_to_remove.add(user_id_to_send)
                 logger.debug(f"Broadcast: User {user_id_to_send} unreachable ({error_str}). Marking for removal.")
            elif "can't parse entities" in error_str:
                 other_fail += 1
                 logger.warning(f"Broadcast parse error for {user_id_to_send}: {e}. Message may need fixing.")
            else:
                 other_fail += 1
                 logger.warning(f"Broadcast BadRequest for {user_id_to_send}: {e}")
        except TelegramError as e:
            logger.warning(f"Broadcast TelegramError for {user_id_to_send}: {e}")
            other_fail += 1
        except Exception as e:
            logger.error(f"Broadcast unexpected error for {user_id_to_send}: {e}", exc_info=True)
            other_fail += 1

        if not sent:
            fail += 1

        current_time = time.time()
        processed_users = i + 1
        processed_users = i + 1
        should_update_count = (update_interval_count > 0 and processed_users % update_interval_count == 0)
        should_update_time = (current_time - last_status_update_time > 10)
        is_last_user = (processed_users == total_users)

        if status_message_obj and (should_update_count or should_update_time or is_last_user):
             try:
                status_text = (f"📣 Broadcasting... {processed_users}/{total_users}\n"
                               f"✅ Sent: {success} | ❌ Failed: {fail} "
                               f"(🚫Blocked/Gone: {blocked}, ❓Other: {other_fail})")
                await context.bot.edit_message_text(
                    chat_id=status_message_obj.chat_id, message_id=status_message_obj.message_id,
                    text=status_text
                )
                last_status_update_time = current_time
             except TelegramError as edit_err:
                  if "Message is not modified" not in str(edit_err):
                       logger.warning(f"Broadcast status update failed: {edit_err}")
                  last_status_update_time = current_time

        await asyncio.sleep(0.05)

    end_time = time.time()
    duration = format_time(end_time - start_time)
    removed_count = len(users_to_remove)
    save_status = "N/A"

    if users_to_remove:
        logger.info(f"Broadcast complete. Attempting to remove {removed_count} blocked/unreachable users from known list.")
        current_known_users = load_known_users()
        cleaned_users = current_known_users - users_to_remove
        if save_known_users(cleaned_users):
            final_user_count = len(cleaned_users)
            save_status = f"✅ Removed {removed_count} inactive users ({final_user_count} remain)"
            logger.info(f"Saved cleaned known_users file after broadcast. {final_user_count} users remain.")
        else:
            save_status = f"❌ Save FAILED! ({removed_count} were marked for removal)"
            logger.error("Failed saving cleaned known_users file after broadcast.")
    else:
         save_status = f"✅ 0 users marked for removal ({total_users} remain)."

    logger.info(f"Broadcast finished. Sent: {success}, Failed: {fail} (Blocked:{blocked}, Other:{other_fail}). Cleanup: {save_status}. Duration: {duration}")

    final_text = (
        f"🏁 Broadcast Complete!\n\n"
        f"✅ Messages Sent: {success}\n"
        f"❌ Send Failures: {fail}\n"
        f"   - 🚫 Blocked/Gone: {blocked}\n"
        f"   - ❓ Other Errors: {other_fail}\n"
        f"👥 Total Users Attempted: {total_users}\n"
        f"🧹 User List Cleanup: {save_status}\n"
        f"⏱️ Duration: {duration}"
    )

    if status_message_obj:
        try:
            await context.bot.edit_message_text(
                chat_id=status_message_obj.chat_id, message_id=status_message_obj.message_id,
                text=final_text
            )
        except TelegramError as e:
            logger.warning(f"Failed to edit final broadcast status: {e}")
            await message.reply_text(final_text)
    else:
        await message.reply_text(final_text)

# --- Message Forwarding (Handle non-command, non-button messages) ---
async def forward_to_admin(update: Update, context: CallbackContext) -> None:
    """Forwards unhandled messages from non-admins in private chat to the admin."""
    user = update.effective_user
    message = update.message

    if not ADMIN_ID or ADMIN_ID == 0: return
    if not user or not message: return
    if message.chat.type != 'private': return
    if user.id == ADMIN_ID: return

    if context.user_data.get('pending_schedule'):
        await message.reply_text("I'm currently waiting for you to send the JSON *file* for your schedule. Please send the file or use /cancel.", reply_markup=main_reply_markup)
        return
    if context.user_data.get('waiting_for_json'):
        await message.reply_text("I'm currently waiting for you to send the JSON *file* for manual processing. Please send the file or use /cancel.", reply_markup=main_reply_markup)
        return

    add_known_user(user.id)

    try:
        user_info = f"Forwarded message from: ID `{user.id}`"
        details = []
        if user.username: details.append(f"@{escape(user.username)}")
        if user.first_name: details.append(escape(user.first_name))
        if user.last_name: details.append(escape(user.last_name))
        if details: user_info += f" ({' '.join(details)})"

        await context.bot.send_message(ADMIN_ID, user_info, parse_mode=ParseMode.MARKDOWN)

        await context.bot.forward_message(
            chat_id=ADMIN_ID,
            from_chat_id=message.chat_id,
            message_id=message.message_id
        )
        logger.info(f"Forwarded message ID {message.message_id} from user {user.id} to admin {ADMIN_ID}")

    except Forbidden:
        logger.error(f"Failed to forward message to admin {ADMIN_ID}: Bot might be blocked by admin.")
    except TelegramError as e:
         logger.error(f"Failed to forward message from user {user.id} to admin {ADMIN_ID} (TelegramError): {e}")
    except Exception as e:
         logger.error(f"Unexpected error forwarding message from user {user.id} to admin {ADMIN_ID}: {e}", exc_info=True)

# --- Channel Leave Handler ---
async def handle_member_left(update: Update, context: CallbackContext) -> None:
    """Handles when a user leaves the required channel."""
    if update.chat_member:
        chat = update.chat_member.chat
        if chat.username == CHANNEL_USERNAME:
            new_member = update.chat_member.new_chat_member
            if new_member.status == 'left':
                left_user = new_member.user
                user_id = left_user.id
                logger.info(f"User {user_id} left the channel {chat.username}.")
                try:
                    await context.bot.send_message(
                        user_id,
                        "You have left the required channel. Bot functions are now disabled. Rejoin https://t.me/atxnaughty to resume."
                    )
                except Exception as e:
                    logger.error(f"Failed to notify user {user_id} about leaving channel: {e}")

# --- Global Error Handler ---
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Log Errors caused by Updates and notify Admin."""
    logger.error(f"Exception while handling an update: {context.error}", exc_info=context.error)

    user_notified = False
    if isinstance(update, Update) and update.effective_chat:
        chat_id_for_notify = update.effective_chat.id
        cleaned = False
        if context.user_data.pop('pending_schedule', None):
            logger.info(f"Cleared 'pending_schedule' state for chat {chat_id_for_notify} due to error.")
            cleaned = True
        if context.user_data.pop('waiting_for_json', None):
             logger.info(f"Cleared 'waiting_for_json' state for chat {chat_id_for_notify} due to error.")
             cleaned = True

        if cleaned:
             try:
                 await context.bot.send_message(
                     chat_id=chat_id_for_notify,
                     text="⚠️ An internal error occurred. Any pending action (like file scheduling or waiting for a file) has been cancelled. Please try again.",
                     reply_markup=main_reply_markup
                 )
                 user_notified = True
             except Exception as notify_err:
                  logger.error(f"Failed to notify user {chat_id_for_notify} about state cleanup after error: {notify_err}")

    if not ADMIN_ID or ADMIN_ID == 0:
        if not user_notified:
            logger.warning("Admin ID not set or invalid, cannot send error notification.")
        return

    tb_list = traceback.format_exception(None, context.error, context.error.__traceback__)
    tb_string = "".join(tb_list)

    try:
        update_str = "Update data unavailable."
        if isinstance(update, Update):
             try:
                 update_data = update.to_dict()
                 if 'message' in update_data and update_data['message']:
                     msg = update_data['message']
                     user_id_from_update = msg.get('from_user', {}).get('id', 'unknown')
                     update_data_simple = {'update_id': update_data.get('update_id'), 'message': {'message_id': msg.get('message_id'), 'chat_id': msg.get('chat',{}).get('id'), 'from_user_id': user_id_from_update, 'type': msg.get('document',{}).get('mime_type') or ('text' if 'text' in msg else 'other') }}
                     update_str = json.dumps(update_data_simple, indent=1, ensure_ascii=False, default=str)
                 else:
                     update_str = json.dumps(update_data, indent=1, ensure_ascii=False, default=str, skipkeys=True)

             except Exception as json_err:
                 logger.error(f"Could not serialize update object to JSON: {json_err}")
                 update_str = str(update)
        elif update: update_str = str(update)

        context_str = "Context data unavailable or complex."
        try:
            from pprint import pformat
            chat_data_str = pformat(context.chat_data, width=80, depth=2)[:300] + ('...' if len(pformat(context.chat_data)) > 300 else '')
            user_data_str = pformat(context.user_data, width=80, depth=2)[:300] + ('...' if len(pformat(context.user_data)) > 300 else '')
            bot_data_str = pformat(context.bot_data, width=80, depth=2)[:200] + ('...' if len(pformat(context.bot_data)) > 200 else '')

            context_info = {
                "args": str(context.args) if hasattr(context, 'args') else 'N/A',
                "user_data": user_data_str,
                "chat_data": chat_data_str,
                "bot_data": bot_data_str,
            }
            context_str = json.dumps(context_info, indent=1, default=str)
        except Exception as ctx_err:
            context_str = f"Error getting context data: {ctx_err}"

        max_len_tb = 3000
        max_len_update = 500
        max_len_context = 300

        error_escaped = escape(str(context.error))
        error_type_escaped = escape(type(context.error).__name__)
        update_short = escape(update_str[:max_len_update] + ('...' if len(update_str) > max_len_update else ''))
        context_short = escape(context_str[:max_len_context] + ('...' if len(context_str) > max_len_context else ''))
        tb_short = escape(tb_string[-max_len_tb:])

        error_message = (
            f"⚠️ <b>Bot Error Encountered</b> ⚠️\n\n"
            f"<b>Error:</b>\n<pre>{error_escaped}</pre>\n"
            f"<b>Type:</b> <pre>{error_type_escaped}</pre>\n\n"
            f"<b>Update (limited):</b>\n<pre>{update_short}</pre>\n\n"
            f"<b>Context (limited):</b>\n<pre>{context_short}</pre>\n\n"
            f"<b>Traceback (end, limited):</b>\n<pre>{tb_short}</pre>"
        )

        max_msg_len = 4096
        if len(error_message) > max_msg_len:
             logger.warning(f"Error message length ({len(error_message)}) exceeds limit. Sending in parts.")
             for i in range(0, len(error_message), max_msg_len):
                 chunk = error_message[i:i + max_msg_len]
                 await context.bot.send_message(ADMIN_ID, chunk, parse_mode=ParseMode.HTML)
                 await asyncio.sleep(0.5)
        else:
            await context.bot.send_message(ADMIN_ID, error_message, parse_mode=ParseMode.HTML)

    except Forbidden:
        logger.critical(f"CRITICAL: Cannot send error notification to admin {ADMIN_ID}. Bot might be blocked by the admin.")
    except Exception as e:
        logger.critical(f"CRITICAL: Failed to send detailed error notification to admin: {e}", exc_info=True)
        try:
            fallback_msg = f"Bot encountered a critical error. Check logs immediately!\nError type: {type(context.error).__name__}\nError: {context.error}"
            await context.bot.send_message(ADMIN_ID, fallback_msg[:4090])
        except Exception as fallback_err:
            logger.critical(f"CRITICAL: Failed even the simplest error notification to admin. Check logs manually. Fallback error: {fallback_err}")

# --- Background Task for Scheduled Processing ---

async def run_scheduled_file_processor(application: Application) -> None:
    """Periodically checks for scheduled files and processes them."""
    bot = application.bot
    logger.info(f"Background scheduler started. Check interval: {AUTO_PROCESS_CHECK_INTERVAL}s")
    await asyncio.sleep(15)

    while True:
        try:
            now_utc = datetime.now(timezone.utc)
            logger.debug(f"Scheduler check running at {now_utc.isoformat()}")
            schedules = load_scheduled_files()
            if not schedules:
                logger.debug("Scheduler: No schedules found.")
                await asyncio.sleep(AUTO_PROCESS_CHECK_INTERVAL)
                continue

            tasks_to_run = []

            github_configs = load_github_configs()

            for user_id_str in list(schedules.keys()):
                user_schedules = schedules.get(user_id_str)
                if not isinstance(user_schedules, dict):
                    logger.warning(f"Scheduler: Invalid schedule data format for user {user_id_str}. Skipping.")
                    continue

                try:
                    user_id = int(user_id_str)
                except ValueError:
                    logger.warning(f"Scheduler: Invalid user ID key '{user_id_str}' in schedules. Skipping.")
                    continue

                if not is_user_vip(user_id):
                     logger.info(f"Scheduler: User {user_id} is no longer VIP. Skipping their schedules.")
                     continue

                if not await is_user_joined_channel(bot, user_id):
                    logger.info(f"Scheduler: User {user_id} not joined to channel. Skipping schedules.")
                    try:
                        await bot.send_message(user_id, "⚠️ Your scheduled processings are skipped because you are not joined to the channel. Please rejoin https://t.me/atxnaughty to resume.")
                    except Exception as e:
                        logger.error(f"Failed to notify user {user_id} about skipped schedules: {e}")
                    continue

                user_github_config = github_configs.get(user_id_str)

                for schedule_name in list(user_schedules.keys()):
                    schedule_info = user_schedules.get(schedule_name)
                    if not isinstance(schedule_info, dict):
                        logger.warning(f"Scheduler: Invalid schedule entry '{schedule_name}' for user {user_id}. Skipping.")
                        continue

                    next_run_iso = schedule_info.get('next_run_time_iso')
                    stored_file_path = schedule_info.get('stored_file_path')
                    interval_seconds = schedule_info.get('interval_seconds')

                    if not next_run_iso or not stored_file_path or not interval_seconds:
                        logger.warning(f"Scheduler: Skipping invalid schedule '{schedule_name}' for user {user_id} (missing essential info).")
                        continue

                    try:
                        next_run_dt = datetime.fromisoformat(next_run_iso.replace('Z', '+00:00'))
                    except (ValueError, TypeError):
                        logger.warning(f"Scheduler: Skipping schedule '{schedule_name}' for user {user_id} due to invalid next_run_time_iso: {next_run_iso}")
                        continue

                    if next_run_dt <= now_utc:
                        logger.info(f"Scheduler: Schedule '{schedule_name}' for user {user_id} is due. Preparing task.")
                        if os.path.exists(stored_file_path):
                            tasks_to_run.append(
                                process_single_schedule(
                                    bot, user_id, schedule_name, schedule_info, user_github_config
                                )
                            )
                        else:
                            logger.error(f"Scheduler: Stored file missing for due schedule '{schedule_name}' user {user_id} at path {stored_file_path}. Skipping run and notifying user.")
                            try:
                                await bot.send_message(
                                    user_id,
                                    f"⚠️ Error: Could not run scheduled task `'{escape(schedule_info.get('user_schedule_name', schedule_name))}'`. The associated data file seems to be missing. Please use `/setfile` again for this schedule.",
                                    parse_mode=ParseMode.MARKDOWN
                                )
                            except Exception as notify_err:
                                logger.error(f"Scheduler: Failed to notify user {user_id} about missing schedule file: {notify_err}")

            if tasks_to_run:
                logger.info(f"Scheduler: Running {len(tasks_to_run)} due schedule(s).")
                results = await asyncio.gather(*tasks_to_run, return_exceptions=True)

                current_schedules = load_scheduled_files()
                made_changes = False
                for result in results:
                     if isinstance(result, tuple) and len(result) == 3:
                        res_user_id_str, res_schedule_name, _ = result
                        try:
                            if res_user_id_str in current_schedules and res_schedule_name in current_schedules[res_user_id_str]:
                                info = current_schedules[res_user_id_str][res_schedule_name]
                                interval_s = info.get('interval_seconds')
                                if interval_s and isinstance(interval_s, int) and interval_s > 0:
                                    last_run_time = datetime.now(timezone.utc)
                                    next_run_time = last_run_time + timedelta(seconds=interval_s)
                                    current_schedules[res_user_id_str][res_schedule_name]['last_run_time_iso'] = last_run_time.isoformat()
                                    current_schedules[res_user_id_str][res_schedule_name]['next_run_time_iso'] = next_run_time.isoformat()
                                    made_changes = True
                                    logger.info(f"Scheduler: Updated next run time for '{res_schedule_name}' (User {res_user_id_str}) to {next_run_time.isoformat()}")
                                else:
                                     logger.error(f"Scheduler: Cannot update next run for '{res_schedule_name}' (User {res_user_id_str}) - missing or invalid interval.")
                            else:
                                 logger.info(f"Scheduler: Schedule '{res_schedule_name}' for user {res_user_id_str} was removed before run time update.")
                        except Exception as update_err:
                            logger.error(f"Scheduler: Error updating schedule info for User {res_user_id_str}, Schedule {res_schedule_name}: {update_err}", exc_info=True)

                     elif isinstance(result, Exception):
                         logger.error(f"Scheduler: Error result returned from scheduled task processing: {result}", exc_info=result)

                if made_changes:
                    if not save_scheduled_files(current_schedules):
                        logger.error("Scheduler: CRITICAL - Failed to save updated schedule run times!")

            else:
                logger.debug("Scheduler: No schedules were due this cycle.")

        except Exception as loop_err:
            logger.critical(f"Scheduler: Unhandled exception in main processing loop: {loop_err}", exc_info=True)
            await asyncio.sleep(60)

        await asyncio.sleep(AUTO_PROCESS_CHECK_INTERVAL)

async def process_single_schedule(bot, user_id: int, schedule_name: str, schedule_info: dict, github_config: dict | None) -> tuple[str, str, bool]:
    """
    Processes a single scheduled file: reads data, calls API, uploads to GitHub.
    Returns (user_id_str, schedule_name, github_upload_success_or_skipped)
    """
    user_id_str = str(user_id)
    stored_file_path = schedule_info.get('stored_file_path')
    user_display_name = schedule_info.get('user_schedule_name', schedule_name)
    github_upload_status = False

    log_prefix = f"AutoProcess User {user_id} Schedule '{schedule_name}':"
    logger.info(f"{log_prefix} Starting.")

    notify_parts = [f"⚙️ Auto-processing started for schedule `'{escape(user_display_name)}'`..."]
    status_msg_obj = None
    try:
        status_msg_obj = await bot.send_message(user_id, notify_parts[0], parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        logger.error(f"{log_prefix} Failed to send initial status DM: {e}")

    accounts_data = []
    run_timestamp = int(time.time())
    temp_results_dir = os.path.join(TEMP_DIR, f"auto_{user_id}_{schedule_name}_{run_timestamp}")
    cleanup_paths_auto = [temp_results_dir]
    jwt_token_path_for_upload = None

    try:
        if not stored_file_path or not os.path.exists(stored_file_path):
            raise FileNotFoundError(f"Stored file path missing or file not found: {stored_file_path}")

        await update_schedule_status(bot, status_msg_obj, notify_parts, "Reading stored file...")

        with open(stored_file_path, 'r', encoding='utf-8') as f:
            try:
                accounts_data = json.load(f)
                if not isinstance(accounts_data, list):
                    raise ValueError("JSON content must be an array (list).")
                if accounts_data and not all(isinstance(item, dict) for item in accounts_data):
                     first_bad = next((x for x in accounts_data if not isinstance(x, dict)), None)
                     raise ValueError(f"All items in the list must be JSON objects (`{{...}}`). Found: {type(first_bad)}")
                logger.info(f"{log_prefix} Read {len(accounts_data)} accounts from {stored_file_path}")
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON format in stored file: {e.msg} (Line: {e.lineno}, Col: {e.colno})")
            except ValueError as ve:
                 raise ve
            except Exception as read_err:
                 raise IOError(f"Could not read or validate the downloaded file: {read_err}")

        total_count = len(accounts_data)
        if total_count == 0:
            logger.info(f"{log_prefix} Stored file is empty. No processing needed.")
            await update_schedule_status(bot, status_msg_obj, notify_parts, "✅ Finished: Stored file was empty.")
            return user_id_str, schedule_name, True

        await update_schedule_status(bot, status_msg_obj, notify_parts, f"Processing {total_count} accounts via API...")

        start_time = time.time()
        successful_tokens = []
        lost_accounts = []
        errors_summary = defaultdict(int)
        successful_by_region = defaultdict(list)
        working_by_region = defaultdict(list)
        processed_count = 0

        semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        async with aiohttp.ClientSession() as session:
            tasks = [process_account(session, account, semaphore) for account in accounts_data]

            for i, future in enumerate(asyncio.as_completed(tasks)):
                update_freq_auto = max(5, min(50, total_count // 10))
                if status_msg_obj and (i + 1) % update_freq_auto == 0:
                     progress_pct = ((i + 1) / total_count) * 100
                     await update_schedule_status(bot, status_msg_obj, notify_parts, f"Processing API... {i+1}/{total_count} ({progress_pct:.0f}%)", keep_last=False)

                try:
                    token, region, working_acc, lost_acc, error_reason = await future
                    processed_count += 1
                    if token and working_acc:
                        region_name = region if region else "Unknown"
                        successful_tokens.append({"token": token, "region": region_name})
                        successful_by_region[region_name].append({"token": token})
                        working_by_region[region_name].append(working_acc)
                    elif lost_acc:
                        lost_accounts.append(lost_acc)
                        reason = lost_acc.get("error_reason", "Unknown")
                        errors_summary[reason.split(':')[0].strip()] += 1
                    else:
                         lost_accounts.append({"account_info": "unknown", "error_reason": "Unexpected process_account result"})
                         errors_summary["Processing function error"] += 1
                except Exception as task_err:
                    processed_count += 1
                    logger.error(f"{log_prefix} Error retrieving result from API task: {task_err}", exc_info=True)
                    lost_accounts.append({"account_info": "unknown", "error_reason": f"Task Error: {task_err}"})
                    errors_summary["Internal task error"] += 1

        processing_time = time.time() - start_time
        logger.info(f"{log_prefix} API processing finished in {processing_time:.2f}s. Success: {len(successful_tokens)}, Failed: {len(lost_accounts)}")
        notify_parts.append(f"📊 API Results: {len(successful_tokens)} tokens generated, {len(lost_accounts)} failures.")
        if errors_summary:
             top_errors = sorted(errors_summary.items(), key=lambda item: item[1], reverse=True)
             error_snippets = []
             for err_msg, count in top_errors[:2]:
                 error_snippets.append(f"`{escape(err_msg)}` ({count})")
             notify_parts.append(f"   (Top errors: {'; '.join(error_snippets)})")

        if successful_tokens:
            await update_schedule_status(bot, status_msg_obj, notify_parts, "Preparing token file for upload...")
            os.makedirs(temp_results_dir, exist_ok=True)
            jwt_token_path_for_upload = os.path.join(temp_results_dir, 'all_server_token_auto.json')
            tokens_only_list = [{"token": entry["token"]} for entry in successful_tokens if entry.get("token")]
            if tokens_only_list:
                if not save_json_data(jwt_token_path_for_upload, tokens_only_list):
                    jwt_token_path_for_upload = None
                    raise IOError("Failed to save temporary token file for upload.")
                logger.info(f"{log_prefix} Saved {len(tokens_only_list)} tokens to {jwt_token_path_for_upload}")
            else:
                jwt_token_path_for_upload = None
                logger.info(f"{log_prefix} No valid tokens found found to save for upload, although processing reported successes.")
                notify_parts.append("ℹ️ No valid tokens found, skipping GitHub upload.")
                github_upload_status = True
        else:
            logger.info(f"{log_prefix} No successful tokens generated. Skipping GitHub upload prep.")
            notify_parts.append("ℹ️ No successful tokens, skipping GitHub upload.")
            github_upload_status = True

        if jwt_token_path_for_upload and github_config and isinstance(github_config, dict):
            await update_schedule_status(bot, status_msg_obj, notify_parts, "Attempting GitHub upload...")
            upload_success = await upload_to_github_background(
                bot, user_id, jwt_token_path_for_upload, github_config
            )
            github_upload_status = upload_success
            logger.info(f"{log_prefix} GitHub upload finished. Success: {upload_success}")
        elif jwt_token_path_for_upload:
            logger.info(f"{log_prefix} Tokens generated but GitHub not configured or config invalid. Skipping upload.")
            notify_parts.append("ℹ️ GitHub upload skipped (not configured or config invalid). Use /mygithub & /setgithub.")
            await update_schedule_status(bot, status_msg_obj, notify_parts, "Skipped GitHub upload (no config).")
            github_upload_status = True

        final_status_line = "✅ Auto-processing completed."
        if jwt_token_path_for_upload and github_config and not github_upload_status:
             final_status_line = "⚠️ Auto-processing completed, but GitHub upload failed (see details above)."

        await update_schedule_status(bot, status_msg_obj, notify_parts, final_status_line, is_final=True)
        logger.info(f"{log_prefix} Finished. Overall Success (for scheduler): {github_upload_status or (not jwt_token_path_for_upload)}")

        final_success_state = github_upload_status or (not jwt_token_path_for_upload)

    except Exception as e:
        logger.error(f"{log_prefix} FAILED: {e}", exc_info=True)
        final_success_state = False
        try:
            error_msg = f"❌ **FAILED:** Auto-processing for schedule `'{escape(user_display_name)}'` encountered an error:\n`{escape(str(e))}`"
            if status_msg_obj:
                 await update_schedule_status(bot, status_msg_obj, notify_parts, error_msg, is_final=True)
            else:
                await bot.send_message(user_id, error_msg, parse_mode=ParseMode.MARKDOWN)
        except Exception as notify_err:
            logger.error(f"{log_prefix} Could not notify user about processing failure: {notify_err}")

    finally:
        for path in cleanup_paths_auto:
            if os.path.exists(path):
                try:
                    if os.path.isdir(path):
                        shutil.rmtree(path)
                    else:
                        os.remove(path)
                    logger.debug(f"{log_prefix} Cleaned up temp path: {path}")
                except OSError as e:
                    logger.warning(f"{log_prefix} Could not clean up temp path {path}: {e}")

    return user_id_str, schedule_name, final_success_state

async def update_schedule_status(bot, status_msg_obj, notify_parts: list, new_status: str, keep_last=True, is_final=False):
    """Helper to update the status message sent to the user during auto-processing."""
    if not status_msg_obj: return

    if keep_last and len(notify_parts) > 1:
        notify_parts[-1] = new_status
    else:
        notify_parts.append(new_status)

    max_lines = 7
    if len(notify_parts) > max_lines:
        notify_parts = [notify_parts[0]] + notify_parts[-(max_lines-1):]
        if notify_parts[1] != "...": notify_parts.insert(1, "...")

    message_text = "\n".join(notify_parts)

    try:
        await bot.edit_message_text(
            chat_id=status_msg_obj.chat_id,
            message_id=status_msg_obj.message_id,
            text=message_text[:4096],
            parse_mode=ParseMode.MARKDOWN,
        )
    except TelegramError as e:
        if "Message is not modified" not in str(e) and "message to edit not found" not in str(e).lower():
            logger.warning(f"AutoProcess: Failed to edit status message {status_msg_obj.message_id}: {e}")
        if "message to edit not found" in str(e).lower():
            logger.warning(f"AutoProcess: Status message {status_msg_obj.message_id} was deleted by user? Stopping updates.")
            status_msg_obj = None

# --- Main Application Setup ---

async def main() -> None:
    """Initialize data, set up handlers, start scheduler, and run the bot."""
    global ADMIN_ID, TOKEN

    print("\n--- Initializing Bot ---")

    if not TOKEN or TOKEN == "YOUR_FALLBACK_BOT_TOKEN":
        print("\n" + "="*60)
        print(" FATAL ERROR: TELEGRAM_BOT_TOKEN is missing or invalid.")
        print(" Please set the TELEGRAM_BOT_TOKEN environment variable or")
        print(" update the TOKEN variable directly in the script.")
        print(" -> Exiting.")
        print("="*60 + "\n")
        exit(1)
    elif len(TOKEN.split(':')) != 2:
        print("\n" + "="*60)
        print(f" FATAL ERROR: TELEGRAM_BOT_TOKEN format looks incorrect ('{TOKEN[:10]}...'). Should be 'ID:SECRET'.")
        print(" -> Exiting.")
        print("="*60 + "\n")
        exit(1)

    try:
        if isinstance(ADMIN_ID, int) and ADMIN_ID != 0: print(f" WARNING: ADMIN_ID not set or invalid in environment. Using script default: {ADMIN_ID}")
        else:
            ADMIN_ID = 0
            print(" WARNING: ADMIN_ID not set/invalid/0. Setting to 0. Admin features disabled.")
    except Exception as e:
         ADMIN_ID = 0
         print(f" WARNING: Error processing ADMIN_ID from environment ({e}). Setting to 0. Admin features disabled.")

    if not API_BASE_URL: logger.warning("JWT_API_URL not set, using default.")
    else: logger.info(f"Using API Base URL: {API_BASE_URL}")
    if not API_KEY or API_KEY == 'atxdev': logger.warning("JWT_API_KEY not set or using default.")

    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        os.makedirs(TEMP_DIR, exist_ok=True)
        os.makedirs(SCHEDULED_FILES_DATA_DIR, exist_ok=True)
        logger.info(f"Data Directory: {DATA_DIR}")
        logger.info(f"Temp Directory: {TEMP_DIR}")
        logger.info(f"Scheduled Files Storage: {SCHEDULED_FILES_DATA_DIR}")
    except OSError as e:
        print(f"\nFATAL ERROR: Cannot create required directories: {e}\n-> Exiting.")
        exit(1)

    app_builder = Application.builder().token(TOKEN)\
        .concurrent_updates(True) \
        .read_timeout(30) \
        .write_timeout(30) \
        .connect_timeout(30) \
        .pool_timeout(60) \
        .get_updates_read_timeout(40) \
        .get_updates_pool_timeout(70)

    application = app_builder.build()

    private_chat_filter = filters.ChatType.PRIVATE

    application.add_handler(CommandHandler("start", start, filters=private_chat_filter))
    application.add_handler(CommandHandler("help", help_command, filters=private_chat_filter))
    application.add_handler(MessageHandler(filters.Regex(f"^{re.escape(COMMAND_BUTTONS_LAYOUT[2][1])}$") & private_chat_filter, help_command))

    application.add_handler(CommandHandler("vipstatus", vip_status_command, filters=private_chat_filter))
    application.add_handler(MessageHandler(filters.Regex(f"^{re.escape(COMMAND_BUTTONS_LAYOUT[0][1])}$") & private_chat_filter, vip_status_command))
    application.add_handler(CommandHandler("vipshop", vip_shop_command, filters=private_chat_filter))
    application.add_handler(MessageHandler(filters.Regex(f"^{re.escape(COMMAND_BUTTONS_LAYOUT[1][0])}$") & private_chat_filter, vip_shop_command))

    application.add_handler(CommandHandler("setgithub", set_github_direct, filters=private_chat_filter))
    application.add_handler(CommandHandler("mygithub", my_github_config, filters=private_chat_filter))
    application.add_handler(MessageHandler(filters.Regex(f"^{re.escape(COMMAND_BUTTONS_LAYOUT[1][1])}$") & private_chat_filter, my_github_config))

    application.add_handler(CommandHandler("setfile", set_scheduled_file_start, filters=private_chat_filter))
    application.add_handler(CommandHandler("removefile", remove_scheduled_file, filters=private_chat_filter))
    application.add_handler(CommandHandler("scheduledfiles", list_scheduled_files, filters=private_chat_filter))
    application.add_handler(MessageHandler(filters.Regex(f"^{re.escape(COMMAND_BUTTONS_LAYOUT[2][0])}$") & private_chat_filter, list_scheduled_files))

    application.add_handler(MessageHandler(filters.Text(COMMAND_BUTTONS_LAYOUT[0][0]) & private_chat_filter, handle_document))
    application.add_handler(MessageHandler(
        (filters.Document.MimeType('application/json') | filters.Document.FileExtension('json')) & private_chat_filter,
        handle_document
    ))

    application.add_handler(CommandHandler("cancel", cancel, filters=private_chat_filter))
    application.add_handler(MessageHandler(filters.Regex(f"^{re.escape(COMMAND_BUTTONS_LAYOUT[3][0])}$") & private_chat_filter, cancel))

    if ADMIN_ID and ADMIN_ID != 0:
        admin_filter = filters.User(user_id=ADMIN_ID) & private_chat_filter
        application.add_handler(CommandHandler("vip", vip_management, filters=admin_filter))
        application.add_handler(CommandHandler("broadcast", broadcast, filters=admin_filter))
        logger.info(f"Admin commands (/vip, /broadcast) enabled for ADMIN_ID: {ADMIN_ID}.")
    else:
         logger.warning("Admin commands are disabled as ADMIN_ID is not set, 0, or invalid.")

    if ADMIN_ID and ADMIN_ID != 0:
        known_button_texts_set = {btn for row in COMMAND_BUTTONS_LAYOUT for btn in row}
        forwarding_filters = (
            private_chat_filter &
            ~filters.User(user_id=ADMIN_ID) &
            ~filters.COMMAND &
            ~filters.Text(known_button_texts_set) &
            ~(filters.Document.MimeType('application/json') | filters.Document.FileExtension('json')) &
            (filters.TEXT | filters.PHOTO | filters.Sticker.ALL | filters.VIDEO | filters.VIDEO_NOTE | filters.VOICE | filters.AUDIO | filters.Document.ALL) &
            ~filters.UpdateType.EDITED_MESSAGE
        )
        application.add_handler(MessageHandler(forwarding_filters, forward_to_admin))
        logger.info("Message forwarding to admin enabled.")
    else:
        logger.warning("Message forwarding to admin is disabled as ADMIN_ID is not set or invalid.")

    # Add handler for channel leave
    application.add_handler(MessageHandler(filters.StatusUpdate.LEFT_CHAT_MEMBER, handle_member_left))

    application.add_error_handler(error_handler)

    logger.info("🤖 Bot is initializing and connecting to Telegram...")
    print("\n" + "="*60)
    print(" 🚀 Advanced JWT Token Bot with File Scheduling is starting...")

    try:
        await application.initialize()

        bot_info = await application.bot.get_me()
        print(f" ✔️ Bot Username: @{bot_info.username} (ID: {bot_info.id})")
        print(f" ✔️ Admin ID: {ADMIN_ID if (ADMIN_ID and ADMIN_ID != 0) else 'Not Set (Admin Features Disabled)'}")
        print(f" ✔️ Data Directory: {DATA_DIR}")
        print(f" ✔️ Scheduled File Check Interval: {AUTO_PROCESS_CHECK_INTERVAL}s")

        scheduler_task = asyncio.create_task(run_scheduled_file_processor(application))
        logger.info("Background scheduler task created.")

        await application.start()
        await application.updater.start_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)
        print("\n Bot is now polling for updates. Press Ctrl+C to stop.")
        print("="*60 + "\n")

        await scheduler_task

    except (TelegramError, ConnectionError) as e:
         print("\n" + "="*60)
         print(f" FATAL ERROR: Could not connect to Telegram or initialize bot.")
         print(f" Error: {e}")
         print(" Please check your network connection and bot token.")
         print(" -> Exiting.")
         print("="*60 + "\n")
         logger.critical(f"Failed to initialize or start polling: {e}", exc_info=True)
         exit(1)
    except asyncio.CancelledError:
        logger.info("Main task or scheduler was cancelled.")
    except Exception as e:
        print("\n" + "="*60)
        print(f" FATAL ERROR: An unexpected error occurred during bot startup or main loop.")
        print(f" Error: {e}")
        print(" -> Exiting.")
        print("="*60 + "\n")
        logger.critical(f"Unhandled exception during startup/runtime: {e}", exc_info=True)
        exit(1)
    finally:
         if 'application' in locals() and application.running:
              logger.info("Attempting graceful shutdown...")
              await application.stop()
              await application.shutdown()
              logger.info("Application stopped.")
         if 'scheduler_task' in locals() and not scheduler_task.done():
              logger.info("Cancelling scheduler task...")
              scheduler_task.cancel()
              try:
                   await scheduler_task
              except asyncio.CancelledError:
                   logger.info("Scheduler task cancelled successfully.")
              except Exception as task_err:
                  logger.error(f"Error during scheduler task cancellation/await: {task_err}")
         logger.info("Shutdown complete.")

if __name__ == '__main__':
    try:
        if not TOKEN or TOKEN == "YOUR_FALLBACK_BOT_TOKEN":
             print("FATAL: TELEGRAM_BOT_TOKEN is missing. Please configure it before running.")
        else:
             asyncio.run(main())
    except KeyboardInterrupt:
        print("\n-- Bot stopping due to Ctrl+C --")
        logger.info("Bot stopped manually via KeyboardInterrupt.")
    except Exception as e:
        print(f"\n💥 A critical unhandled exception occurred outside the main asyncio loop: {e}")
        logger.critical(f"Critical unhandled exception in __main__: {e}", exc_info=True)
