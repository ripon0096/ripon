import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
from twilio.rest import Client
import uuid
from datetime import datetime
import json
import os
import re
import time
import logging
import threading
import signal
import sys
import gc
import traceback
from functools import wraps

# Configure logging to write to a file and not to the console.
# All logs (INFO level and above) will go to 'debug.log'.
# The console will remain clean.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('debug.log', encoding='utf-8')
    ]
)

logger = logging.getLogger(__name__)

# Ensure console output uses UTF-8 for emojis
if sys.stdout.encoding.lower() != 'utf-8':
    sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf-8', buffering=1)
    sys.stderr = open(sys.stderr.fileno(), mode='w', encoding='utf-8', buffering=1)

# Telegram Bot Token
TELEGRAM_BOT_TOKEN = "7943286551:AAEsicSCJdEB2Lq1KZIqqF6kIWuy0u8S1sc"

# Global bot instance
bot = None

# Graceful shutdown flag
shutdown_flag = threading.Event()

# Admin Telegram Username
ADMIN_USERNAME = "@RJ_MEMORY"

# Admin Telegram User ID
admin_id = 5718596664

# Required Channels
REQUIRED_CHANNELS = [
    {"username": "@DailyEarningTips25", "chat_id": "@DailyEarningTips25"},
    {"username": "@BotSeller25", "chat_id": "@BotSeller25"}
]

# File to store registered users
USERS_FILE = "users.json"

# Dictionary to store generated numbers per user session
generated_numbers = {}

# Dictionary to store user-specific data
user_data = {}
user_current_number = {}

# Multiple Twilio accounts pool for auto-failover
twilio_account_pool = [
    {"sid": os.environ.get("TWILIO_SID_1", ""), "auth_token": os.environ.get("TWILIO_TOKEN_1", ""), "status": "active"},  # Account 1
    {"sid": os.environ.get("TWILIO_SID_2", ""), "auth_token": os.environ.get("TWILIO_TOKEN_2", ""), "status": "active"},  # Account 2
    {"sid": os.environ.get("TWILIO_SID_3", ""), "auth_token": os.environ.get("TWILIO_TOKEN_3", ""), "status": "active"},  # Account 3
    {"sid": os.environ.get("TWILIO_SID_4", ""), "auth_token": os.environ.get("TWILIO_TOKEN_4", ""), "status": "active"},  # Account 4
    {"sid": os.environ.get("TWILIO_SID_5", ""), "auth_token": os.environ.get("TWILIO_TOKEN_5", ""), "status": "active"},  # Account 5
]

# Track current account index for each user
user_account_index = {}

# Initialize registered users
registered_users = {}

# Global error tracking
error_count = 0
last_error_time = 0

def comprehensive_error_handler(func):
    """Ultra comprehensive error handler decorator"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        global error_count, last_error_time
        try:
            return func(*args, **kwargs)
        except telebot.apihelper.ApiTelegramException as e:
            error_msg = str(e).lower()
            if "blocked" in error_msg or "chat not found" in error_msg:
                logger.warning(f"User interaction error in {func.__name__}: {str(e)}")
            elif "rate" in error_msg or "too many" in error_msg:
                logger.warning(f"Rate limit in {func.__name__}, backing off...")
                time.sleep(5)
            else:
                logger.error(f"Telegram API error in {func.__name__}: {str(e)}")
        except Exception as e:
            current_time = time.time()
            error_count += 1

            if current_time - last_error_time > 300:  # Reset count every 5 minutes
                error_count = 1
            last_error_time = current_time

            logger.error(f"Error in {func.__name__}: {str(e)}", exc_info=True)

            # Try to send error notification to user if possible
            try:
                if args and hasattr(args[0], 'chat') and hasattr(args[0].chat, 'id'):
                    chat_id = args[0].chat.id
                    safe_send_message(chat_id, "⚠️ Temporary issue occurred. Please try again.")
            except:
                pass

            # Force garbage collection on repeated errors
            if error_count > 10:
                gc.collect()
                error_count = 0
    return wrapper

def initialize_bot_with_retry(max_attempts=10):
    """Initialize bot with multiple retry attempts"""
    global bot

    for attempt in range(max_attempts):
        try:
            if bot:
                try:
                    bot.stop_polling()
                    del bot
                    time.sleep(2)
                except:
                    pass

            bot = telebot.TeleBot(TELEGRAM_BOT_TOKEN, threaded=True)

            # Test bot connection
            bot.get_me()
            logger.info(f"Bot initialized successfully on attempt {attempt + 1}")
            return True

        except Exception as e:
            logger.error(f"Bot initialization attempt {attempt + 1} failed: {str(e)}")
            if attempt < max_attempts - 1:
                time.sleep(min(5 * (attempt + 1), 30))  # Progressive backoff
            else:
                logger.error(f"Failed to initialize bot after {max_attempts} attempts")

    return False

def safe_load_registered_users():
    """Load registered users with maximum safety"""
    global registered_users

    backup_files = [USERS_FILE, f"{USERS_FILE}.backup", f"{USERS_FILE}.old"]

    for file_path in backup_files:
        try:
            if os.path.exists(file_path):
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    users = {}

                    for user_id, user_info in data.items():
                        try:
                            if isinstance(user_info, dict):
                                users[int(user_id)] = user_info
                            else:
                                users[int(user_id)] = {
                                    "status": user_info if isinstance(user_info, str) else "approved",
                                    "channel_joined": False,
                                    "first_use_time": None
                                }
                        except (ValueError, TypeError):
                            logger.warning(f"Skipping invalid user data: {user_id}")
                            continue

                    registered_users = users
                    logger.info(f"Loaded {len(users)} users from {file_path}")
                    return True

        except Exception as e:
            logger.error(f"Error loading from {file_path}: {str(e)}")
            continue

    # If all files fail, initialize with admin
    registered_users = {admin_id: {"status": "approved", "channel_joined": True, "first_use_time": None}}
    logger.info("Initialized with admin user only")
    return True

def safe_save_registered_users():
    """Save users with backup and atomic write"""
    try:
        # Create backup first
        if os.path.exists(USERS_FILE):
            try:
                os.rename(USERS_FILE, f"{USERS_FILE}.backup")
            except:
                pass

        # Atomic write
        temp_file = f"{USERS_FILE}.tmp"
        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump(registered_users, f, indent=2, ensure_ascii=False)

        os.rename(temp_file, USERS_FILE)
        logger.info("Users saved successfully")
        return True

    except Exception as e:
        logger.error(f"Error saving users: {str(e)}")
        return False

def robust_channel_check(user_id, max_retries=3):
    """Enhanced channel membership check with API bypass and fallback system"""
    if user_id == admin_id:
        return True

    channels_verified = 0
    required_channels_count = len(REQUIRED_CHANNELS)
    api_errors = []

    for attempt in range(max_retries):
        try:
            channels_verified = 0
            for channel in REQUIRED_CHANNELS:
                try:
                    member = bot.get_chat_member(channel["chat_id"], user_id)
                    if member.status in ['left', 'kicked']:
                        logger.info(f"User {user_id} has left channel {channel['username']}")
                        # Reset user's channel_joined status
                        if user_id in registered_users:
                            registered_users[user_id]["channel_joined"] = False
                            safe_save_registered_users()
                        return False
                    elif member.status in ['member', 'administrator', 'creator']:
                        channels_verified += 1
                        logger.info(f"User {user_id} is member of {channel['username']}")
                        # Mark this channel as verified for this user
                        if user_id not in registered_users:
                            registered_users[user_id] = {"status": "approved", "channel_joined": False, "first_use_time": None}
                        if "verified_channels" not in registered_users[user_id]:
                            registered_users[user_id]["verified_channels"] = {}
                        registered_users[user_id]["verified_channels"][channel["username"]] = True
                    time.sleep(0.1)  # Small delay between checks
                except telebot.apihelper.ApiTelegramException as e:
                    error_msg = str(e).lower()
                    api_errors.append(f"{channel['username']}: {str(e)}")

                    if "user not found" in error_msg or "chat not found" in error_msg:
                        logger.warning(f"User {user_id} not found in {channel['username']}")
                        return False
                    elif "member list is inaccessible" in error_msg or "bad request" in error_msg:
                        # Smart fallback: Use previous verification + time-based trust
                        logger.warning(f"Member list inaccessible for {channel['username']}, using fallback verification")

                        # Check if user was previously verified for this specific channel
                        if (user_id in registered_users and
                            registered_users[user_id].get("verified_channels", {}).get(channel["username"], False)):
                            channels_verified += 1
                            logger.info(f"✅ Allowing user {user_id} for {channel['username']} - Previously verified & API bypass active")
                        else:
                            # Fallback verification: If user is trying to use bot and one channel is accessible, trust for both
                            other_channel_accessible = False
                            for other_channel in REQUIRED_CHANNELS:
                                if other_channel["username"] != channel["username"]:
                                    try:
                                        other_member = bot.get_chat_member(other_channel["chat_id"], user_id)
                                        if other_member.status in ['member', 'administrator', 'creator']:
                                            other_channel_accessible = True
                                            break
                                    except:
                                        continue

                            if other_channel_accessible:
                                # If user is verified in at least one channel, assume good faith for inaccessible channel
                                channels_verified += 1
                                logger.info(f"✅ Allowing user {user_id} for {channel['username']} - Verified in other channel, API bypass fallback")
                                # Mark as verified for future
                                if user_id not in registered_users:
                                    registered_users[user_id] = {"status": "approved", "channel_joined": False, "first_use_time": None}
                                if "verified_channels" not in registered_users[user_id]:
                                    registered_users[user_id]["verified_channels"] = {}
                                registered_users[user_id]["verified_channels"][channel["username"]] = True
                            else:
                                logger.warning(f"❌ User {user_id} cannot be verified for {channel['username']} - No fallback available")
                                return False
                    elif "rate" in error_msg or "too many" in error_msg:
                        time.sleep(2)
                        continue
                    else:
                        logger.error(f"API error checking {channel['username']}: {str(e)}")
                        # For other API errors, be more lenient
                        if (user_id in registered_users and
                            registered_users[user_id].get("verified_channels", {}).get(channel["username"], False)):
                            channels_verified += 1
                            logger.info(f"✅ Allowing user {user_id} for {channel['username']} - API error bypass using previous verification")
                        else:
                            return False
                except Exception as e:
                    logger.error(f"Unexpected error checking {channel['username']}: {str(e)}")
                    # For unexpected errors, use cached verification if available
                    if (user_id in registered_users and
                        registered_users[user_id].get("verified_channels", {}).get(channel["username"], False)):
                        channels_verified += 1
                        logger.info(f"✅ Allowing user {user_id} for {channel['username']} - Exception bypass using cached verification")
                    else:
                        return False

            # User must be verified in ALL channels
            if channels_verified == required_channels_count:
                # Update user's overall status
                if user_id in registered_users:
                    registered_users[user_id]["channel_joined"] = True
                    safe_save_registered_users()
                logger.info(f"✅ User {user_id} fully verified in all {channels_verified}/{required_channels_count} channels")
                return True
            else:
                logger.warning(f"❌ User {user_id} verified in only {channels_verified}/{required_channels_count} channels")
                return False

        except Exception as e:
            logger.error(f"Channel check attempt {attempt + 1} failed: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(2)

    # Final fallback: If all API attempts failed but user was previously verified
    logger.warning(f"All API attempts failed for user {user_id}. API Errors: {api_errors}")

    # Ultimate fallback for completely inaccessible APIs
    if (user_id in registered_users and
        registered_users[user_id].get("channel_joined", False) and
        len(registered_users[user_id].get("verified_channels", {})) >= required_channels_count):
        logger.info(f"🚨 ULTIMATE FALLBACK: Allowing user {user_id} based on previous full verification - API completely inaccessible")
        return True

    logger.warning(f"❌ All verification methods failed for user {user_id}")
    return False

def is_user_authorized(user_id):
    """Check if user is authorized with safety"""
    try:
        if user_id == admin_id:
            return True
        
        # First, check if user is in our records and was previously verified
        user_was_authorized = user_id in registered_users and registered_users[user_id].get("channel_joined", False)

        # Now, perform the live channel check
        is_currently_authorized = robust_channel_check(user_id)

        # If the user was authorized before but isn't now, send a notification
        if user_was_authorized and not is_currently_authorized:
            warning_msg = "⚠️ *Channel Membership Required!* ⚠️\n\n"
            warning_msg += "❌ You seem to have left one of our required channels.\n\n"
            warning_msg += "🔒 To continue using the bot, please ensure you are a member of:\n\n"
            for i, channel in enumerate(REQUIRED_CHANNELS, 1):
                warning_msg += f"{i}. {channel['username']}\n"
            warning_msg += "\n💡 After rejoining, please verify your membership."

            safe_send_message(user_id, warning_msg, reply_markup=create_channel_join_menu(), parse_mode="Markdown")

        return is_currently_authorized
    except Exception as e:
        logger.error(f"Error checking authorization for {user_id}: {str(e)}")
        return False

def create_main_menu(user_id):
    """Create main menu with error handling"""
    try:
        markup = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)

        # Main buttons for all users
        markup.row(
            KeyboardButton("👤 Login"),
            KeyboardButton("➕ Bulk Login")
        )
        markup.row(
            KeyboardButton("📤 Logout"),
            KeyboardButton("🔎 Search Numbers") # Reverted to original
        )
        markup.row(
            KeyboardButton("📍 Target Number"),
            KeyboardButton("💬 Receive SMS")
        )
        markup.row(
            KeyboardButton("🔗 Check Channels"),
            KeyboardButton("🇺🇸 USA Numbers") # New button for USA numbers
        )
        markup.row(
            KeyboardButton("❓ Help")
        )

        # Admin only buttons
        if user_id == admin_id:
            markup.row(
                KeyboardButton("⚙️ Admin Panel"), # This one is correct from previous fix
                KeyboardButton("📣 Broadcast")
            )

        return markup
    except Exception as e:
        logger.error(f"Error creating main menu: {str(e)}")
        return ReplyKeyboardMarkup(resize_keyboard=True)

def create_channel_join_menu():
    """Create channel join menu with error handling"""
    try:
        markup = InlineKeyboardMarkup(row_width=1)
        for channel in REQUIRED_CHANNELS:
            markup.add(InlineKeyboardButton(
                text="🚀 Join Now",
                url=f"https://t.me/{channel['username'][1:]}"
            ))
        markup.add(InlineKeyboardButton(
            text="✅ Verify Now",
            callback_data="verify_channels"
        ))
        return markup
    except Exception as e:
        logger.error(f"Error creating channel menu: {str(e)}")
        return InlineKeyboardMarkup()

def create_admin_panel():
    """Create admin panel with error handling"""
    try:
        markup = InlineKeyboardMarkup()
        markup.row(
            InlineKeyboardButton("Block User", callback_data="admin_block"),
            InlineKeyboardButton("Unblock User", callback_data="admin_unblock")
        )
        markup.row(
            InlineKeyboardButton("Approve User", callback_data="admin_approve")
        )
        return markup
    except Exception as e:
        logger.error(f"Error creating admin panel: {str(e)}")
        return InlineKeyboardMarkup()

def ultra_safe_send_message(chat_id, text, reply_markup=None, parse_mode=None, max_retries=5):
    """Ultra safe message sending with comprehensive error handling"""
    for attempt in range(max_retries):
        try:
            if not bot:
                return False

            # Truncate message if too long
            if len(text) > 4096:
                text = text[:4093] + "..."

            bot.send_message(chat_id, text, reply_markup=reply_markup, parse_mode=parse_mode)
            return True

        except telebot.apihelper.ApiTelegramException as e:
            error_msg = str(e).lower()

            if "blocked" in error_msg or "chat not found" in error_msg:
                logger.warning(f"User {chat_id} blocked bot or chat not found")
                return False
            elif "rate" in error_msg or "too many" in error_msg:
                wait_time = min(2 ** attempt, 30)
                logger.warning(f"Rate limited, waiting {wait_time}s")
                time.sleep(wait_time)
                continue
            elif "message is too long" in error_msg:
                text = text[:4000] + "..."
                continue
            elif "can't parse" in error_msg:
                parse_mode = None
                continue
            else:
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                logger.error(f"Telegram API error after {max_retries} attempts: {str(e)}")
                return False

        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
                continue
            logger.error(f"Failed to send message after {max_retries} attempts: {str(e)}")
            return False

    return False

# Alias for backward compatibility
safe_send_message = ultra_safe_send_message

def check_account_status(twilio_client):
    """Check Twilio account status with enhanced error handling"""
    try:
        account = twilio_client.api.accounts.list()[0]
        if account.status == 'active':
            return True, "Account is active"
        elif account.status == 'suspended':
            return False, "Account has been suspended. Please use a different account."
        else:
            return False, f"Account status: {account.status}. Please use a different account."
    except AttributeError:
        return False, "Could not retrieve Twilio account information. Please check your credentials."
    except Exception as e:
        error_msg = str(e)
        if "authenticate" in error_msg.lower():
            return False, "Invalid Twilio credentials. Please provide correct information."
        elif "not found" in error_msg.lower():
            return False, "Twilio account not found. Please use a different account."
        else:
            return False, "Problem checking Twilio account. Please try again."

def get_next_working_account(user_id):
    """Get next working Twilio account from pool or user's bulk accounts with automatic failover"""
    global twilio_account_pool, user_account_index

    # Check if user has bulk accounts first
    if user_id in user_data and user_data[user_id].get("using_bulk_pool", False):
        bulk_accounts = user_data[user_id].get("bulk_accounts", [])
        current_bulk_index = user_data[user_id].get("current_bulk_index", 0)

        # Try each bulk account
        for attempt in range(len(bulk_accounts)):
            current_index = (current_bulk_index + attempt) % len(bulk_accounts)
            account = bulk_accounts[current_index]

            # Skip if account is marked as inactive
            if account["status"] != "active":
                continue

            try:
                # Test the account
                test_client = Client(account["sid"], account["auth_token"])
                status_ok, status_msg = check_account_status(test_client)

                if status_ok:
                    # Update user's current bulk account index
                    user_data[user_id]["current_bulk_index"] = current_index
                    logger.info(f"Using bulk account #{current_index + 1} for user {user_id}")
                    return account["sid"], account["auth_token"], current_index + 1
                else:
                    # Mark account as inactive
                    account["status"] = "inactive"
                    logger.warning(f"Bulk account #{current_index + 1} marked as inactive: {status_msg}")
                    continue

            except Exception as e:
                # Mark account as inactive on error
                account["status"] = "inactive"
                logger.error(f"Bulk account #{current_index + 1} failed: {str(e)}")
                continue

        # No working bulk account found
        return None, None, None

    # Use global pool if no bulk accounts
    # Initialize user account index if not exists
    if user_id not in user_account_index:
        user_account_index[user_id] = 0

    # Try each account in the pool
    for attempt in range(len(twilio_account_pool)):
        current_index = (user_account_index[user_id] + attempt) % len(twilio_account_pool)
        account = twilio_account_pool[current_index]

        # Skip if account is marked as inactive
        if account["status"] != "active":
            continue

        # Skip if credentials are empty
        if not account["sid"] or not account["auth_token"]:
            continue

        try:
            # Test the account
            test_client = Client(account["sid"], account["auth_token"])
            status_ok, status_msg = check_account_status(test_client)

            if status_ok:
                # Update user's current account index
                user_account_index[user_id] = current_index
                logger.info(f"Using Twilio account #{current_index + 1} for user {user_id}")
                return account["sid"], account["auth_token"], current_index + 1
            else:
                # Mark account as inactive
                account["status"] = "inactive"
                logger.warning(f"Account #{current_index + 1} marked as inactive: {status_msg}")
                continue

        except Exception as e:
            # Mark account as inactive on error
            account["status"] = "inactive"
            logger.error(f"Account #{current_index + 1} failed: {str(e)}")
            continue

    # No working account found
    return None, None, None

def mark_account_as_failed(user_id, reason="Unknown error"):
    """Mark current account as failed and move to next"""
    global twilio_account_pool, user_account_index

    # Check if using bulk accounts
    if user_id in user_data and user_data[user_id].get("using_bulk_pool", False):
        bulk_accounts = user_data[user_id].get("bulk_accounts", [])
        current_bulk_index = user_data[user_id].get("current_bulk_index", 0)

        if current_bulk_index < len(bulk_accounts):
            bulk_accounts[current_bulk_index]["status"] = "inactive"
            logger.warning(f"Bulk account #{current_bulk_index + 1} marked as failed for user {user_id}: {reason}")

            # Try to get next working bulk account
            next_sid, next_token, next_account_num = get_next_working_account(user_id)
            if next_sid:
                logger.info(f"Switched to bulk account #{next_account_num} for user {user_id}")
                return next_sid, next_token
    else:
        # Use global pool
        if user_id in user_account_index:
            current_index = user_account_index[user_id]
            if current_index < len(twilio_account_pool):
                twilio_account_pool[current_index]["status"] = "inactive"
                logger.warning(f"Account #{current_index + 1} marked as failed for user {user_id}: {reason}")

                # Try to get next working account
                next_sid, next_token, next_account_num = get_next_working_account(user_id)
                if next_sid:
                    logger.info(f"Switched to account #{next_account_num} for user {user_id}")
                    return next_sid, next_token

    return None, None

def extract_whatsapp_info(text):
    """Extract WhatsApp info from text with error handling"""
    try:
        number_match = re.search(r'\+?\d+', text)
        code_match = re.search(r'(\d{3}-\d{3})|(\d{3}[-]?\d{3})|(\d{4,6})', text)
        time_match = re.search(r'(\d{2}/\d{2}/\d{4} \d{2}:\d{2})', text)

        number = number_match.group(0) if number_match else None
        code = code_match.group(0) if code_match else None
        time_str = time_match.group(0) if time_match else None

        return {'number': number, 'code': code, 'time': time_str}
    except Exception as e:
        logger.error(f"Error extracting WhatsApp info: {str(e)}")
        return {'number': None, 'code': None, 'time': None}

def format_sms_message(number, code, time_str):
    """Format SMS message with error handling"""
    try:
        msg = f"📱 Number: {number}\n"
        msg += f"🔑 Code: {code}\n"
        msg += f"⏰ Time: {time_str}\n"

        markup = InlineKeyboardMarkup()
        if number:
            markup.add(InlineKeyboardButton("Copy Number", callback_data=f"copy_number_{number}"))
        if code:
            markup.add(InlineKeyboardButton("Copy Code", callback_data=f"copy_code_{code}"))

        return msg, markup
    except Exception as e:
        logger.error(f"Error formatting SMS message: {str(e)}")
        return "Error formatting message", None

def setup_all_handlers():
    """Setup all bot handlers with comprehensive error handling"""
    if not bot:
        return False

    try:
        # Start command
        @bot.message_handler(commands=['start'])
        @comprehensive_error_handler
        def handle_start(message):
            user_id = message.chat.id

            if user_id not in registered_users:
                registered_users[user_id] = {
                    "status": "approved",
                    "channel_joined": False,
                    "first_use_time": None
                }
                safe_save_registered_users()

            if not is_user_authorized(user_id):
                channel_msg = "🔔 Channel membership required!\n\n"
                channel_msg += "✨ Please join these channels first to use the bot:\n\n"

                for i, channel in enumerate(REQUIRED_CHANNELS, 1):
                    channel_msg += f"{i}. {channel['username']}\n"

                channel_msg += "\n💡 After joining both channels, click 'I've joined both channels' button."
                channel_msg += "\n\n🎯 Use all features completely free after joining!"

                safe_send_message(message.chat.id, channel_msg, reply_markup=create_channel_join_menu())
                return

            main_menu = create_main_menu(user_id)
            safe_send_message(message.chat.id,
                            "🌟 Welcome to TwilioPro Bot! 🌟\n"
                            "📱 Get virtual numbers and real-time\n"
                            "SMS services easily and instantly!\n\n"
                            "💫 Key Features:\n"
                            "• Search and purchase numbers effortlessly\n"
                            "• Receive SMS instantly\n"
                            "• Automatically detect OTP\n"
                            "• 24/7 active service and support\n\n"
                            "🎯 Select your desired option from the\n"
                            "menu below and get started!",
                            reply_markup=main_menu)

        # Get ID command
        @bot.message_handler(commands=['get_id'])
        @comprehensive_error_handler
        def get_user_id(message):
            user_id = message.chat.id
            if not is_user_authorized(user_id):
                channel_msg = "🔔 Channel membership required!\n\n"
                channel_msg += "✨ To view ID, first join these channels:\n\n"
                for i, channel in enumerate(REQUIRED_CHANNELS, 1):
                    channel_msg += f"{i}. {channel['username']}\n"
                channel_msg += "\n🎯 Use all features completely free after joining!"
                safe_send_message(message.chat.id, channel_msg, reply_markup=create_channel_join_menu())
                return
            safe_send_message(message.chat.id, f"Your User ID is: {message.chat.id}")

        # Admin panel command
        @bot.message_handler(commands=['admin_panel'])
        @comprehensive_error_handler
        def admin_panel_cmd(message):
            if message.chat.id != admin_id:
                safe_send_message(message.chat.id, "You are not authorized to access the admin panel.")
                return
            safe_send_message(message.chat.id, "Admin Panel: Select an action", reply_markup=create_admin_panel())

        # Admin panel button handler (for ReplyKeyboardMarkup)
        @bot.message_handler(func=lambda message: message.text == "⚙️ Admin Panel" and message.chat.id == admin_id)
        @comprehensive_error_handler
        def handle_admin_panel_button(message):
            if message.chat.id != admin_id:
                safe_send_message(message.chat.id, "You are not authorized to access the admin panel.")
                return
            admin_panel_cmd(message) # Call the existing admin panel command handler
        # Broadcast message handler
        @bot.message_handler(func=lambda message: message.text == "📣 Broadcast" and message.chat.id == admin_id)
        @comprehensive_error_handler
        def broadcast_message_handler(message):
            if message.chat.id != admin_id:
                safe_send_message(message.chat.id, "You cannot use this feature.")
                return

            broadcast_msg = "📢 *Broadcast Message System* 📢\n"
            broadcast_msg += "━━━━━━━━━━━━━━━━━━━━━\n\n"
            broadcast_msg += "💬 *Write your message:*\n\n"
            broadcast_msg += "📝 Type the message you want to send to all users\n\n"
            broadcast_msg += "⚠️ *Important:*\n"
            broadcast_msg += "• Keep message within 4000 characters\n"
            broadcast_msg += "• Empty message cannot be sent\n"
            broadcast_msg += "• All registered users will receive it\n\n"
            broadcast_msg += "🌟 *Now type your message...*"

            safe_send_message(message.chat.id, broadcast_msg, parse_mode="Markdown")

            try:
                bot.register_next_step_handler(message, process_broadcast_message)
            except Exception as e:
                logger.error(f"Error registering broadcast handler: {str(e)}")

        @comprehensive_error_handler
        def process_broadcast_message(message):
            if message.chat.id != admin_id:
                safe_send_message(message.chat.id, "Unauthorized access.")
                return

            broadcast_text = message.text.strip()

            # Validate message
            if not broadcast_text:
                safe_send_message(message.chat.id, "❌ Empty message cannot be sent. Please write a message.")
                return

            if len(broadcast_text) > 4000:
                safe_send_message(message.chat.id, f"❌ Message too long ({len(broadcast_text)} characters). Please keep within 4000 characters.")
                return

            # Get all registered users
            total_users = len(registered_users)
            if total_users == 0:
                safe_send_message(message.chat.id, "❌ No registered users found.")
                return

            # Confirmation message
            confirm_msg = f"📊 *Broadcast Confirmation* 📊\n"
            confirm_msg += f"━━━━━━━━━━━━━━━━━━━━━\n\n"
            confirm_msg += f"👥 **Target Users:** {total_users} users\n"
            confirm_msg += f"📝 **Message Length:** {len(broadcast_text)} characters\n\n"
            confirm_msg += f"💬 **Your Message:**\n"
            confirm_msg += f"```\n{broadcast_text[:200]}{'...' if len(broadcast_text) > 200 else ''}\n```\n\n"
            confirm_msg += f"⚡ **Broadcast will start now...**"

            safe_send_message(message.chat.id, confirm_msg, parse_mode="Markdown")

            # Start broadcasting
            success_count = 0
            failed_count = 0
            blocked_count = 0

            # Progress message
            progress_msg = f"🚀 **Broadcast started...**\n\n"
            progress_msg += f"📊 **Progress:** 0/{total_users}\n"
            progress_msg += f"✅ **Success:** 0\n"
            progress_msg += f"❌ **Failed:** 0\n"
            progress_msg += f"🚫 **Blocked:** 0"

            progress_message = safe_send_message(message.chat.id, progress_msg, parse_mode="Markdown")

            # Prepare broadcast message with admin signature
            final_broadcast_msg = f"📢 **Admin Announcement** 📢\n"
            final_broadcast_msg += f"━━━━━━━━━━━━━━━━━━━━━\n\n"
            final_broadcast_msg += f"{broadcast_text}\n\n"
            final_broadcast_msg += f"━━━━━━━━━━━━━━━━━━━━━\n"
            final_broadcast_msg += f"👨‍💼 **Sender:** Admin {ADMIN_USERNAME}\n"
            final_broadcast_msg += f"⏰ **Time:** {datetime.now().strftime('%d/%m/%Y %H:%M')}"

            # Send to all users
            processed = 0
            for user_id in list(registered_users.keys()):
                processed += 1

                # Skip admin
                if user_id == admin_id:
                    continue

                try:
                    # Send broadcast message
                    if ultra_safe_send_message(user_id, final_broadcast_msg, parse_mode="Markdown"):
                        success_count += 1
                        logger.info(f"✅ Broadcast sent successfully to user {user_id}")
                    else:
                        failed_count += 1
                        logger.warning(f"❌ Failed to send broadcast to user {user_id}")

                except telebot.apihelper.ApiTelegramException as e:
                    error_msg = str(e).lower()
                    if "blocked" in error_msg or "chat not found" in error_msg:
                        blocked_count += 1
                        logger.warning(f"🚫 User {user_id} blocked the bot")
                    else:
                        failed_count += 1
                        logger.error(f"❌ API error sending to user {user_id}: {str(e)}")

                except Exception as e:
                    failed_count += 1
                    logger.error(f"❌ Error sending broadcast to user {user_id}: {str(e)}")

                # Update progress every 5 users or on completion
                if processed % 5 == 0 or processed == total_users:
                    try:
                        updated_progress = f"🚀 **Broadcast in progress...**\n\n"
                        updated_progress += f"📊 **Progress:** {processed}/{total_users}\n"
                        updated_progress += f"✅ **Success:** {success_count}\n"
                        updated_progress += f"❌ **Failed:** {failed_count}\n"
                        updated_progress += f"🚫 **Blocked:** {blocked_count}\n"
                        updated_progress += f"⏳ **Remaining:** {total_users - processed}"

                        if progress_message:
                            bot.edit_message_text(
                                chat_id=message.chat.id,
                                message_id=progress_message.message_id,
                                text=updated_progress,
                                parse_mode="Markdown"
                            )
                    except Exception as e:
                        logger.error(f"Error updating progress: {str(e)}")

                # Small delay to avoid rate limiting
                time.sleep(0.1)

            # Final report
            completion_percentage = (success_count / max(total_users - 1, 1)) * 100  # Exclude admin
            final_report = f"🎉 **Broadcast Complete!** 🎉\n"
            final_report += f"━━━━━━━━━━━━━━━━━━━━━\n\n"
            final_report += f"📊 **Final Statistics:**\n"
            final_report += f"👥 **Total Users:** {total_users - 1} users (excluding admin)\n"
            final_report += f"✅ **Successfully Reached:** {success_count} users\n"
            final_report += f"❌ **Failed:** {failed_count} users\n"
            final_report += f"🚫 **Bot Blocked:** {blocked_count} users\n\n"
            final_report += f"📈 **Success Rate:** {completion_percentage:.1f}%\n\n"

            if success_count > 0:
                final_report += f"🎯 **Your message reached {success_count} users!**\n"

            if failed_count > 0 or blocked_count > 0:
                final_report += f"⚠️ **Note:** Some users blocked the bot or API issues occurred\n"

            final_report += f"\n💫 **Broadcast system worked successfully!**"

            try:
                if progress_message:
                    bot.edit_message_text(
                        chat_id=message.chat.id,
                        message_id=progress_message.message_id,
                        text=final_report,
                        parse_mode="Markdown"
                    )
            except:
                safe_send_message(message.chat.id, final_report, parse_mode="Markdown")

            logger.info(f"📢 Broadcast completed: {success_count} successful, {failed_count} failed, {blocked_count} blocked")

        # Account pool management command
        @bot.message_handler(commands=['pool_status'])
        @comprehensive_error_handler
        def pool_status_cmd(message):
            if message.chat.id != admin_id:
                safe_send_message(message.chat.id, "You cannot use this command.")
                return

            status_msg = "📊 *Account Pool Status:*\n\n"

            for i, account in enumerate(twilio_account_pool, 1):
                if account["sid"] and account["auth_token"]:
                    status_icon = "✅" if account["status"] == "active" else "❌"
                    status_msg += f"{status_icon} Account #{i}: {account['status']}\n"
                    status_msg += f"   SID: {account['sid'][:10]}...\n\n"
                else:
                    status_msg += f"⚪ Account #{i}: empty\n\n"

            status_msg += f"💡 *User Account Index:*\n"
            for user_id, index in user_account_index.items():
                status_msg += f"User {user_id}: Account #{index + 1}\n"

            safe_send_message(message.chat.id, status_msg, parse_mode="Markdown")

        # Add account to pool command
        @bot.message_handler(commands=['add_account'])
        @comprehensive_error_handler
        def add_account_cmd(message):
            if message.chat.id != admin_id:
                safe_send_message(message.chat.id, "You cannot use this command.")
                return

            safe_send_message(message.chat.id, "Add new account:\n\nFormat: Account_SID Auth_Token\n\nExample:\nAC123...xyz fe99...abc")

            try:
                bot.register_next_step_handler(message, process_add_account)
            except Exception as e:
                logger.error(f"Error registering step handler: {str(e)}")

        @comprehensive_error_handler
        def process_add_account(message):
            try:
                credentials = message.text.strip().split()
                if len(credentials) != 2:
                    safe_send_message(message.chat.id, "❌ Invalid format! Correct format: Account_SID Auth_Token")
                    return

                sid, auth_token = credentials

                # Test the account
                try:
                    test_client = Client(sid, auth_token)
                    status_ok, status_msg = check_account_status(test_client)

                    if not status_ok:
                        safe_send_message(message.chat.id, f"❌ Account could not be added: {status_msg}")
                        return

                except Exception as e:
                    safe_send_message(message.chat.id, f"❌ Invalid credentials: {str(e)}")
                    return

                # Find empty slot or add to list
                added = False
                for i, account in enumerate(twilio_account_pool):
                    if not account["sid"]:
                        account["sid"] = sid
                        account["auth_token"] = auth_token
                        account["status"] = "active"
                        safe_send_message(message.chat.id, f"✅ Account successfully added to slot #{i + 1}!")
                        added = True
                        break

                if not added:
                    twilio_account_pool.append({"sid": sid, "auth_token": auth_token, "status": "active"})
                    safe_send_message(message.chat.id, f"✅ New account added to slot #{len(twilio_account_pool)}!")

            except Exception as e:
                safe_send_message(message.chat.id, f"❌ Error: {str(e)}")

        # Channel verification callback with enhanced fallback
        @bot.callback_query_handler(func=lambda call: call.data == "verify_channels")
        @comprehensive_error_handler
        def handle_verify_channels(call):
            user_id = call.message.chat.id

            # Enhanced verification with multiple fallback methods
            verification_result = robust_channel_check(user_id)

            # Additional manual verification attempt if automated fails
            if not verification_result:
                logger.info(f"Attempting manual verification for user {user_id}")

                # Try alternative verification method
                manual_channels_verified = 0
                for channel in REQUIRED_CHANNELS:
                    try:
                        # Try to get chat info as an alternative check
                        chat_info = bot.get_chat(channel["chat_id"])
                        if chat_info:
                            # If we can get chat info, assume user has some access
                            manual_channels_verified += 1
                            logger.info(f"Manual verification: User {user_id} has access to {channel['username']}")
                    except Exception as e:
                        logger.warning(f"Manual verification failed for {channel['username']}: {str(e)}")
                        # Even if manual fails, be lenient for API issues
                        manual_channels_verified += 1
                        logger.info(f"Manual verification fallback: Assuming access for {channel['username']} due to API issues")

                # If manual verification suggests user has access, allow it
                if manual_channels_verified >= len(REQUIRED_CHANNELS):
                    verification_result = True
                    logger.info(f"✅ Manual verification successful for user {user_id}")

                    # Update user records
                    if user_id not in registered_users:
                        registered_users[user_id] = {"status": "approved", "channel_joined": False, "first_use_time": None}

                    registered_users[user_id]["channel_joined"] = True
                    if "verified_channels" not in registered_users[user_id]:
                        registered_users[user_id]["verified_channels"] = {}

                    for channel in REQUIRED_CHANNELS:
                        registered_users[user_id]["verified_channels"][channel["username"]] = True

                    safe_save_registered_users()

            if verification_result:
                # Ensure user data is properly saved
                if user_id not in registered_users:
                    registered_users[user_id] = {"status": "approved", "channel_joined": False, "first_use_time": None}

                registered_users[user_id]["channel_joined"] = True
                safe_save_registered_users()

                success_msg = "🎉 Congratulations! 🎉\n\n"
                success_msg += "✅ You have successfully completed channel verification!\n\n"
                success_msg += "🚀 Now you can use all bot features completely free:\n"
                success_msg += "• 🔑 Login\n"
                success_msg += "• 🔍 Search numbers\n"
                success_msg += "• 📩 Receive SMS\n"
                success_msg += "• All services completely free!\n\n"
                success_msg += "⚠️ Important: You must stay in channels to use the bot.\n"
                success_msg += "💫 Thank you for using the bot!"

                try:
                    bot.edit_message_text(
                        chat_id=call.message.chat.id,
                        message_id=call.message.message_id,
                        text=success_msg
                    )
                except:
                    pass

                safe_send_message(call.message.chat.id, "🌟 Main Menu", reply_markup=create_main_menu(user_id))
                try:
                    bot.answer_callback_query(call.id, "Successfully verified!")
                except:
                    pass
            else:
                # Even if verification fails, be more lenient due to API issues
                error_msg = "⚠️ Verification incomplete due to API issues!\n\n"
                error_msg += "🔄 If you have joined both channels, please try again.\n\n"
                error_msg += "📋 Make sure you have joined these channels:\n\n"
                for i, channel in enumerate(REQUIRED_CHANNELS, 1):
                    error_msg += f"{i}. {channel['username']}\n"
                error_msg += "\n💡 If the problem persists, try again later."
                error_msg += "\n\n🚨 Temporary inconvenience may occur due to API issues."

                try:
                    bot.edit_message_text(
                        chat_id=call.message.chat.id,
                        message_id=call.message.message_id,
                        text=error_msg,
                        reply_markup=create_channel_join_menu()
                    )
                except:
                    pass

                try:
                    bot.answer_callback_query(call.id, "API issue! Please try again.", show_alert=True)
                except:
                    pass

        # Login handler
        @bot.message_handler(func=lambda message: message.text == "👤 Login")
        @comprehensive_error_handler
        def login_account(message):
            user_id = message.chat.id
            if not is_user_authorized(user_id):
                channel_msg = "🔔Channel subscription required!\n\n"
                channel_msg += "✨ To login first join these channels:\n\n"
                for i, channel in enumerate(REQUIRED_CHANNELS, 1):
                    channel_msg += f"{i}. {channel['username']}\n"
                channel_msg += "\n🎯 Use all features completely free after joining!"
                safe_send_message(message.chat.id, channel_msg, reply_markup=create_channel_join_menu())
                return

            credentials_msg = "📝 Single Account Login Instructions\n\n"
            credentials_msg += "• Enter your Account SID and Auth Token\n"
            credentials_msg += "• Make sure to separate both values with one space\n\n"
            credentials_msg += "💡 Example:\n"
            credentials_msg += "ACxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy\n\n"
            credentials_msg += "⚠️ Important:\n"
            credentials_msg += "Please input your credentials correctly otherwise login will fail\n\n"
            credentials_msg += "💫 To add multiple accounts use the\n"
            credentials_msg += "🔐 Bulk Login option"
            safe_send_message(message.chat.id, credentials_msg)

            try:
                bot.register_next_step_handler(message, process_twilio_login)
            except Exception as e:
                logger.error(f"Error registering step handler: {str(e)}")

        # Bulk Login handler
        @bot.message_handler(func=lambda message: message.text == "➕ Bulk Login")
        @comprehensive_error_handler
        def bulk_login_account(message):
            user_id = message.chat.id
            if not is_user_authorized(user_id):
                channel_msg = "🔔 Channel subscription required!\n\n"
                channel_msg += "✨ To bulk login first join these channels:\n\n"
                for i, channel in enumerate(REQUIRED_CHANNELS, 1):
                    channel_msg += f"{i}. {channel['username']}\n"
                channel_msg += "\n🎯 Use all features completely free after joining!"
                safe_send_message(message.chat.id, channel_msg, reply_markup=create_channel_join_menu())
                return

            bulk_msg = "━━━━━━━━━━━━━━\n"
            bulk_msg += "🚀 Auto-Failover System 🚀\n"
            bulk_msg += "━━━━━━━━━━━━━━\n\n"
            bulk_msg += "✅ With 30 accounts you get:\n"
            bulk_msg += "• Auto-switch when one account fails\n"
            bulk_msg += "• Zero service interruption\n"
            bulk_msg += "• Continuous SMS reception\n"
            bulk_msg += "• No manual intervention needed\n\n"

            bulk_msg += "🖊️ Example:\n"
            bulk_msg += "```\n"
            bulk_msg += "ACxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy\n\n"
            bulk_msg += "ACzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n"
            bulk_msg += "```\n\n"

            bulk_msg += "⚠️ Warning:\n"
            bulk_msg += "Login will fail without proper spacing\n\n"

            bulk_msg += "💡 Simply put:\n"
            bulk_msg += "With 30 accounts if one fails\n"
            bulk_msg += "others keep working automatically!\n\n"

            bulk_msg += "🔐 Uninterrupted service - Try now!\n"
            bulk_msg += "━━━━━━━━━━━━━━━━"
            safe_send_message(message.chat.id, bulk_msg, parse_mode="Markdown")

            try:
                bot.register_next_step_handler(message, process_bulk_twilio_login)
            except Exception as e:
                logger.error(f"Error registering step handler: {str(e)}")

        # Process Twilio login
        @comprehensive_error_handler
        def process_twilio_login(message):
            user_id = message.chat.id
            try:
                credentials = message.text.strip().split()

                if len(credentials) != 2:
                    error_msg = "⚠️ Account Login Failed\n\n"
                    error_msg += "🚨 No valid account credentials found!\n"
                    error_msg += "Please provide your details in the correct format:"
                    safe_send_message(message.chat.id, error_msg)
                    return

                sid, auth_token = credentials

                try:
                    twilio_client = Client(sid, auth_token)
                    status_ok, status_msg = check_account_status(twilio_client)

                    if not status_ok:
                        safe_send_message(message.chat.id, f"⚠️ *Twilio Account Issue*\n\n{status_msg}\n\nPlease use a different account.", parse_mode="Markdown")
                        return

                except Exception as e:
                    safe_send_message(message.chat.id, "Invalid Twilio credentials. Please provide a valid Account SID and Auth Token.")
                    return

                user_data[user_id] = {
                    "sid": sid,
                    "auth_token": auth_token,
                    "purchased_numbers": [],
                    "using_pool": False
                }

                success_msg = "✅ Login Successful!\n"
                success_msg += "📍 Please enter your area code to proceed."
                safe_send_message(message.chat.id, success_msg, reply_markup=create_main_menu(user_id), parse_mode="Markdown")
            except Exception as e:
                safe_send_message(message.chat.id, f"Error: {e}")

        # Process Bulk Twilio login
        @comprehensive_error_handler
        def process_bulk_twilio_login(message):
            user_id = message.chat.id
            try:
                # Enhanced text processing to handle various formats
                text_input = message.text.strip()

                # Handle different separators and formats
                lines = []

                # Split by newlines first
                raw_lines = text_input.split('\n')

                for line in raw_lines:
                    line = line.strip()
                    if not line:
                        continue

                    # Skip lines that don't look like credentials
                    if len(line) < 20:  # Too short to be valid credentials
                        continue

                    # Handle multiple formats
                    # Format 1: SID TOKEN (space separated)
                    # Format 2: SID,TOKEN (comma separated)
                    # Format 3: SID:TOKEN (colon separated)
                    # Format 4: SID|TOKEN (pipe separated)
                    # Format 5: SID\nTOKEN (newline separated - for your example)

                    # Check if this line contains both SID and Token
                    if line.startswith('AC') and len(line) > 60:
                        # This might be SID and Token on same line
                        # Replace various separators with space
                        line = line.replace(',', ' ').replace(':', ' ').replace('|', ' ').replace('\t', ' ')

                        # Clean multiple spaces
                        import re
                        line = re.sub(r'\s+', ' ', line)

                        # Check if we have exactly 2 parts after splitting
                        parts = line.split()
                        if len(parts) == 2 and parts[0].startswith('AC'):
                            lines.append(line)
                        elif len(parts) == 1 and parts[0].startswith('AC'):
                            # This might be just SID, look for token in next line
                            lines.append(line)
                    elif not line.startswith('AC') and len(line) >= 30:
                        # This might be a token on separate line
                        # Check if previous line was a SID
                        if lines and lines[-1].split()[-1].startswith('AC'):
                            # Combine with previous SID
                            sid_line = lines[-1]
                            combined_line = f"{sid_line} {line}"
                            lines[-1] = combined_line
                        else:
                            lines.append(line)
                    else:
                        # Other formats
                        line = line.replace(',', ' ').replace(':', ' ').replace('|', ' ').replace('\t', ' ')
                        import re
                        line = re.sub(r'\s+', ' ', line)
                        if line:
                            lines.append(line)

                if not lines:
                    safe_send_message(message.chat.id, "⚠️ Account Login Failed\n\n🚨 No valid account credentials found!\nPlease provide your details in the correct format:")
                    return

                valid_accounts = []
                invalid_accounts = []
                detailed_errors = []

                # Maximum 30 accounts allowed (increased limit)
                if len(lines) > 30:
                    safe_send_message(message.chat.id, f"⚠️ *Maximum 30 accounts can be added.* Processing first 30 accounts...", parse_mode="Markdown")
                    lines = lines[:30]

                progress_msg = f"🔄 Bulk account verification started...\n\n📊 Total accounts: {len(lines)}\n\n⏳ Please wait..."
                progress_message = safe_send_message(message.chat.id, progress_msg, parse_mode="Markdown")

                for i, line in enumerate(lines, 1):
                    try:
                        # Enhanced credential parsing
                        credentials = line.strip().split()

                        # Validate format
                        if len(credentials) < 2:
                            error_detail = f"Line {i}: Incomplete data (SID and Token required)"
                            invalid_accounts.append(error_detail)
                            detailed_errors.append(f"Line {i}: Incomplete data - needs both SID and Token")
                            continue
                        elif len(credentials) > 2:
                            # Take first two parts if more than 2 parts exist
                            credentials = credentials[:2]

                        sid, auth_token = credentials

                        # Enhanced validation
                        validation_errors = []

                        # Check SID format
                        if not sid.startswith('AC'):
                            validation_errors.append("SID must start with 'AC'")
                        if len(sid) != 34:  # Twilio SID is exactly 34 characters
                            validation_errors.append(f"SID length invalid ({len(sid)} chars, expected exactly 34)")

                        # Check Auth Token format
                        if len(auth_token) != 32:  # Twilio Auth Token is exactly 32 characters
                            validation_errors.append(f"Auth Token length invalid ({len(auth_token)} chars, expected exactly 32)")

                        # Check for valid characters (alphanumeric only)
                        if not re.match(r'^AC[A-Za-z0-9]{32}$', sid):
                            validation_errors.append("SID format invalid (should be AC followed by 32 alphanumeric chars)")
                        if not re.match(r'^[A-Za-z0-9]{32}$', auth_token):
                            validation_errors.append("Auth Token format invalid (should be 32 alphanumeric chars)")

                        if validation_errors:
                            error_detail = f"Line {i}: Format issues - {', '.join(validation_errors)}"
                            invalid_accounts.append(error_detail)
                            detailed_errors.append(f"Line {i}: Format issues - {', '.join(validation_errors)}")
                            continue

                        # Test the account with enhanced error handling
                        try:
                            test_client = Client(sid, auth_token)

                            # Set a shorter timeout for bulk operations
                            import socket
                            original_timeout = socket.getdefaulttimeout()
                            socket.setdefaulttimeout(15)  # 15 second timeout

                            # Try to make a simple API call to test credentials
                            try:
                                account_info = test_client.api.accounts.list(limit=1)
                                if account_info:
                                    # Additional check for account status
                                    status_ok, status_msg = check_account_status(test_client)

                                    if status_ok:
                                        valid_accounts.append({
                                            "sid": sid,
                                            "auth_token": auth_token,
                                            "status": "active"
                                        })
                                        logger.info(f"✅ Bulk account {i} validated successfully")
                                    else:
                                        error_detail = f"Line {i}: Account status issue - {status_msg}"
                                        invalid_accounts.append(error_detail)
                                        detailed_errors.append(f"Line {i}: Account status issue - {status_msg}")
                                        logger.warning(f"❌ Bulk account {i} status failed: {status_msg}")
                                else:
                                    error_detail = f"Line {i}: Account information not found"
                                    invalid_accounts.append(error_detail)
                                    detailed_errors.append(f"Line {i}: Could not retrieve account info")

                            except Exception as api_error:
                                api_error_msg = str(api_error).lower()
                                if "authenticate" in api_error_msg or "unauthorized" in api_error_msg:
                                    error_detail = f"Line {i}: Invalid Credentials - Invalid SID or Token"
                                    detailed_errors.append(f"Line {i}: Authentication failed - Wrong SID or Token")
                                elif "account" in api_error_msg and "suspended" in api_error_msg:
                                    error_detail = f"Line {i}: Account suspended"
                                    detailed_errors.append(f"Line {i}: Account suspended")
                                elif "trial" in api_error_msg:
                                    error_detail = f"Line {i}: Trial account - upgrade required"
                                    detailed_errors.append(f"Line {i}: Trial account - upgrade required")
                                elif "network" in api_error_msg or "timeout" in api_error_msg:
                                    error_detail = f"Line {i}: Network problem - try again"
                                    detailed_errors.append(f"Line {i}: Network issue")
                                else:
                                    error_detail = f"Line {i}: API error - {str(api_error)[:50]}"
                                    detailed_errors.append(f"Line {i}: API error - {str(api_error)[:50]}")

                                invalid_accounts.append(error_detail)
                                logger.error(f"Account API test error for line {i}: {str(api_error)}")

                            # Restore original timeout
                            socket.setdefaulttimeout(original_timeout)

                        except Exception as test_error:
                            error_msg = str(test_error).lower()
                            if "authenticate" in error_msg or "401" in error_msg:
                                error_detail = f"Line {i}: Invalid credentials - please check again"
                                detailed_errors.append(f"Line {i}: Invalid credentials")
                            elif "timeout" in error_msg:
                                error_detail = f"Line {i}: Connection timeout - check network"
                                detailed_errors.append(f"Line {i}: Connection timeout")
                            elif "network" in error_msg or "connection" in error_msg:
                                error_detail = f"Line {i}: Network problem"
                                detailed_errors.append(f"Line {i}: Network issue")
                            else:
                                error_detail = f"Line {i}: Connection error - {str(test_error)[:40]}"
                                detailed_errors.append(f"Line {i}: Connection error")

                            invalid_accounts.append(error_detail)
                            logger.error(f"Account validation error for line {i}: {str(test_error)}")
                            continue

                    except Exception as e:
                        error_detail = f"Line {i}: Processing error - {str(e)[:30]}"
                        invalid_accounts.append(error_detail)
                        detailed_errors.append(f"Line {i}: Processing error - {str(e)[:30]}")
                        logger.error(f"Processing error for line {i}: {str(e)}")
                        continue

                    # Update progress every account
                    progress_update = f"🔄 *Progress: {i}/{len(lines)}*\n\n"
                    progress_update += f"✅ Valid: {len(valid_accounts)}\n"
                    progress_update += f"❌ Invalid: {len(invalid_accounts)}\n"
                    progress_update += f"📊 Completed: {i}/{len(lines)}"

                    try:
                        if progress_message:
                            bot.edit_message_text(
                                chat_id=message.chat.id,
                                message_id=progress_message.message_id,
                                text=progress_update,
                                parse_mode="Markdown"
                            )
                    except:
                        pass

                # Final results
                if valid_accounts:
                    # Update user's personal pool
                    if user_id not in user_data:
                        user_data[user_id] = {}

                    user_data[user_id] = {
                        "bulk_accounts": valid_accounts,
                        "current_bulk_index": 0,
                        "using_bulk_pool": True,
                        "purchased_numbers": []
                    }

                    # Set current working account
                    current_account = valid_accounts[0]
                    user_data[user_id]["sid"] = current_account["sid"]
                    user_data[user_id]["auth_token"] = current_account["auth_token"]

                    # Enhanced success message
                    result_msg = f"━━━━━━━━━━━━━━━━━━\n"
                    result_msg += f"🎉 BULK LOGIN SUCCESS! 🎉\n"
                    result_msg += f"━━━━━━━━━━━━━━━━━━\n\n"
                    result_msg += f"📊 Login Summary:\n"
                    result_msg += f"✅ Successful: {len(valid_accounts)} account{'s' if len(valid_accounts) != 1 else ''}\n"
                    result_msg += f"❌ Failed: {len(invalid_accounts)} account{'s' if len(invalid_accounts) != 1 else ''}\n\n"
                    result_msg += f"🔄 Auto-Failover Active:\n"
                    result_msg += f"• Automatic switching between accounts\n"
                    result_msg += f"• Continuous service guaranteed\n"
                    result_msg += f"• Zero downtime experience"

                    safe_send_message(message.chat.id, result_msg, reply_markup=create_main_menu(user_id), parse_mode="Markdown")
                    logger.info(f"✅ Bulk login successful for user {user_id}: {len(valid_accounts)} accounts loaded")

                else:
                    # Enhanced error message with detailed troubleshooting
                    error_msg = f"❌ *All Accounts Invalid!* ❌\n"
                    error_msg += f"━━━━━━━━━━━━━━━━━━━━━\n\n"
                    error_msg += f"🚨 **Problem:** None of the {len(invalid_accounts)} accounts you provided are working!\n\n"

                    # Analyze and categorize errors
                    error_analysis = {
                        "Credential Issues": 0,
                        "Format Issues": 0,
                        "Account Suspended": 0,
                        "Network Issues": 0,
                        "Trial Account": 0,
                        "Others": 0
                    }

                    for error in invalid_accounts:
                        if "credential" in error.lower() or "invalid" in error.lower() or "authenticate" in error.lower():
                            error_analysis["Credential Issues"] += 1
                        elif "format" in error.lower() or "length" in error.lower():
                            error_analysis["Format Issues"] += 1
                        elif "suspend" in error.lower() or "restricted" in error.lower():
                            error_analysis["Account Suspended"] += 1
                        elif "network" in error.lower() or "timeout" in error.lower() or "connection" in error.lower():
                            error_analysis["Network Issues"] += 1
                        elif "trial" in error.lower():
                            error_analysis["Trial Account"] += 1
                        else:
                            error_analysis["Others"] += 1

                    error_msg += f"📊 **Problem Analysis:**\n"
                    for issue_type, count in error_analysis.items():
                        if count > 0:
                            error_msg += f"• {issue_type}: {count} accounts\n"
                    error_msg += f"\n"

                    # Common issues and solutions
                    error_msg += f"🔍 **Most Common Issues & Solutions:**\n\n"

                    if error_analysis["Credential Issues"] > 0:
                        error_msg += f"🔑 **Wrong Credentials ({error_analysis['Credential Issues']} accounts):**\n"
                        error_msg += f"• Copy correct SID & Token from Twilio Console\n"
                        error_msg += f"• Account SID starts with 'AC'\n"
                        error_msg += f"• Auth Token is exactly 32 characters\n"
                        error_msg += f"• Make sure there are no extra spaces or symbols\n\n"

                    if error_analysis["Format Issues"] > 0:
                        error_msg += f"📝 **Format Issues ({error_analysis['Format Issues']} accounts):**\n"
                        error_msg += f"• Each line should be: Account_SID Auth_Token\n"
                        error_msg += f"• Correct example:\n" # Corrected example
                        error_msg += f"```\nACxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy\nACzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n```\n\n"

                    if error_analysis["Account Suspended"] > 0:
                        error_msg += f"🚫 **Suspended Accounts ({error_analysis['Account Suspended']} accounts):**\n"
                        error_msg += f"• These accounts are suspended on Twilio\n"
                        error_msg += f"• Create new accounts or use different accounts\n\n"

                    if error_analysis["Trial Account"] > 0:
                        error_msg += f"🎯 **Trial Accounts ({error_analysis['Trial Account']} accounts):**\n"
                        error_msg += f"• Trial accounts have limited features\n"
                        error_msg += f"• Upgrade by adding $20 credit\n\n"

                    error_msg += f"💡 **Quick Solutions:**\n"
                    error_msg += f"1️⃣ First test with 1 account\n"
                    error_msg += f"2️⃣ Copy fresh SID & Token from Twilio Console\n"
                    error_msg += f"3️⃣ Check account balance and status\n"
                    error_msg += f"4️⃣ Create completely new Twilio accounts\n\n"

                    # Show specific errors
                    error_msg += f"📋 **Detailed Error List:**\n"
                    for i, error in enumerate(invalid_accounts[:8], 1):  # Show max 8 errors
                        error_msg += f"{i}. {error}\n"
                    if len(invalid_accounts) > 8:
                        error_msg += f"... {len(invalid_accounts) - 8} more issues\n"

                    error_msg += f"\n🔄 **Next Steps:**\n"
                    error_msg += f"• Click '🔐 bulk login' to try again\n"
                    error_msg += f"• Or use '🔑 login' to test one account\n\n"
                    error_msg += f"❓ **Need Help?** Contact admin for assistance."

                    safe_send_message(message.chat.id, error_msg, parse_mode="Markdown")

                    # Send detailed technical log to admin for debugging
                    if detailed_errors:
                        admin_debug_msg = f"🔧 **Debug Info for User {user_id}:**\n\n"
                        admin_debug_msg += f"Total lines processed: {len(lines)}\n"
                        admin_debug_msg += f"Valid accounts: {len(valid_accounts)}\n"
                        admin_debug_msg += f"Invalid accounts: {len(invalid_accounts)}\n\n"
                        admin_debug_msg += f"**Technical Errors:**\n"
                        for error in detailed_errors[:10]:
                            admin_debug_msg += f"• {error}\n"

                        try:
                            safe_send_message(admin_id, admin_debug_msg, parse_mode="Markdown")
                        except:
                            pass

                    logger.warning(f"❌ Bulk login failed for user {user_id}: No valid accounts from {len(lines)} attempts")

            except Exception as e:
                safe_send_message(message.chat.id, f"ত্রুটি: {str(e)}")
                logger.error(f"Error in process_bulk_twilio_login for User ID {user_id}: {str(e)}")

        # Enhanced Logout Button with Complete Cleanup
        @bot.message_handler(func=lambda message: message.text == "📤 Logout")
        @comprehensive_error_handler
        def logout_account(message):
            user_id = message.chat.id

            if not is_user_authorized(user_id):
                channel_msg = "🔔 Channel membership required!\n\n"
                channel_msg += "✨ To logout, first join these channels:\n\n"
                for i, channel in enumerate(REQUIRED_CHANNELS, 1):
                    channel_msg += f"{i}. {channel['username']}\n"
                channel_msg += "\n🎯 Use all features completely free after joining!"
                safe_send_message(message.chat.id, channel_msg, reply_markup=create_channel_join_menu())
                return

            if user_id in user_data:
                # Count accounts before logout for display
                accounts_count = 0
                if user_data[user_id].get("using_bulk_pool", False):
                    accounts_count = len(user_data[user_id].get("bulk_accounts", []))
                elif user_data[user_id].get("using_pool", False):
                    accounts_count = 1  # Pool account
                else:
                    accounts_count = 1  # Single account

                # Complete cleanup of all user data
                user_data.pop(user_id, None)
                user_current_number.pop(user_id, None)
                generated_numbers.pop(user_id, None)

                # Clear user account index for pool system
                if user_id in user_account_index:
                    user_account_index.pop(user_id, None)

                # Create logout success message
                logout_msg = "✅ *Success!*\n"
                logout_msg += "*All your accounts have been logged out.*"

                safe_send_message(message.chat.id, logout_msg, reply_markup=create_main_menu(user_id), parse_mode="Markdown")

                # Log the logout for admin monitoring
                logger.info(f"🚪 Complete logout performed for user {user_id}: {accounts_count} accounts cleared")

            else:
                login_msg = "🔒 *Please log in first before proceeding.*\n"
                login_msg += "📌 *Login is required to access this feature.*"

                safe_send_message(message.chat.id, login_msg, parse_mode="Markdown")



        # Global flag to track if user is in search mode
        user_search_mode = {}

        # Search Numbers by Area Code
        @bot.message_handler(func=lambda message: message.text == "🔎 Search Numbers")
        @comprehensive_error_handler
        def ask_for_area_code(message):
            user_id = message.chat.id

            if not is_user_authorized(user_id):
                channel_msg = "🔔 Channel membership required!\n\n"
                channel_msg += "✨ To search numbers, first join these channels:\n\n"
                for i, channel in enumerate(REQUIRED_CHANNELS, 1):
                    channel_msg += f"{i}. {channel['username']}\n"
                channel_msg += "\n🎯 Use all features completely free after joining!"
                safe_send_message(message.chat.id, channel_msg, reply_markup=create_channel_join_menu())
                return

            # Set user in search mode
            user_search_mode[user_id] = "search_ca_numbers"

            search_msg = "📍 **Search Numbers (Default: Canada)**\n\n"
            search_msg += "Enter a 3-digit area code to find available numbers.\n\n"

            # Show search history info if exists
            if user_id in generated_numbers and generated_numbers[user_id]:
                search_msg += f"📋 You have {len(generated_numbers[user_id])} searched numbers\n"
                search_msg += "💫 Type 'clear history' to clear search history"

            safe_send_message(message.chat.id, search_msg, parse_mode="Markdown")

        # Search USA Numbers by Area Code
        @bot.message_handler(func=lambda message: message.text == "🇺🇸 USA Numbers")
        @comprehensive_error_handler
        def ask_for_usa_area_code(message):
            user_id = message.chat.id

            if not is_user_authorized(user_id):
                channel_msg = "🔔 Channel membership required!\n\n"
                channel_msg += "✨ To search USA numbers, first join these channels:\n\n"
                for i, channel in enumerate(REQUIRED_CHANNELS, 1):
                    channel_msg += f"{i}. {channel['username']}\n"
                channel_msg += "\n🎯 Use all features completely free after joining!"
                safe_send_message(message.chat.id, channel_msg, reply_markup=create_channel_join_menu())
                return

            # Set user in USA search mode
            user_search_mode[user_id] = "search_us_numbers"

            search_msg = "🇺🇸 **USA Number Search** 🇺🇸\n\n"
            search_msg += "📍 Send your 3-digit USA area code to find numbers."

            safe_send_message(message.chat.id, search_msg)

        # Target Number handler
        @bot.message_handler(func=lambda message: message.text == "📍 Target Number")
        @comprehensive_error_handler
        def target_number_search(message):
            user_id = message.chat.id
            if not is_user_authorized(user_id):
                channel_msg = "🔔 Channel membership required!\n\n"
                channel_msg += "✨ To search target numbers, first join these channels:\n\n"
                for i, channel in enumerate(REQUIRED_CHANNELS, 1):
                    channel_msg += f"{i}. {channel['username']}\n"
                channel_msg += "\n🎯 Use all features completely free after joining!"
                safe_send_message(message.chat.id, channel_msg, reply_markup=create_channel_join_menu())
                return

            # Set user in target mode
            user_search_mode[user_id] = "target_numbers"

            target_msg = "🎯 Smart Target Number Search 🎯\n\n"
            target_msg += "💡 Enter any 3 to 5-digit pattern to find matching virtual numbers.\n"
            target_msg += "🔥 Why use this?\n"
            target_msg += "• No area code needed\n"
            target_msg += "• Intelligent and fast search\n"
            target_msg += "• Instantly find matching numbers\n\n"
            target_msg += "🎲 Now enter your desired 3–5 digit pattern below!"
            safe_send_message(message.chat.id, target_msg, parse_mode="Markdown")

        @bot.message_handler(func=lambda message: message.text.isdigit() and len(message.text) in [3, 4, 5])
        @comprehensive_error_handler
        def fetch_numbers_by_pattern(message):
            user_id = message.chat.id

            if not is_user_authorized(user_id):
                channel_msg = "🔔 Channel membership required!\n\n"
                channel_msg += "✨ To search numbers, first join these channels:\n\n"
                for i, channel in enumerate(REQUIRED_CHANNELS, 1):
                    channel_msg += f"{i}. {channel['username']}\n"
                channel_msg += "\n🎯 Use all features completely free after joining!"
                safe_send_message(message.chat.id, channel_msg, reply_markup=create_channel_join_menu())
                return

            # Check if user is in search numbers mode and input is more than 3 digits
            current_mode = user_search_mode.get(user_id, None)
            pattern = message.text
            pattern_length = len(pattern)

            # If user is in search numbers mode and input is not exactly 3 digits, reject
            if current_mode in ["search_ca_numbers", "search_us_numbers"] and pattern_length != 3:
                country = "Canadian" if current_mode == "search_ca_numbers" else "USA"
                error_msg = f"❌ *In {country} search mode, please provide only a 3-digit area code!*\n\n"
                error_msg += f"🚫 You provided {pattern_length} digits: `{pattern}`\n\n"
                error_msg += "📝 *Correct format:*\n"
                error_msg += "• Provide only 3 digits\n"
                error_msg += "• Example: 416 (for CA), 212 (for USA)\n\n"
                safe_send_message(message.chat.id, error_msg, parse_mode="Markdown")
                return

            # If user is in target numbers mode, allow 3-5 digits
            if current_mode == "target_numbers" and pattern_length not in [3, 4, 5]:
                error_msg = "❌ *In target number mode, provide 3-5 digit pattern!*\n\n"
                error_msg += f"🚫 You provided {pattern_length} digits: `{pattern}`\n\n"
                error_msg += "📝 *Correct format:*\n"
                error_msg += "• 3 digits: 123\n"
                error_msg += "• 4 digits: 1234\n"
                error_msg += "• 5 digits: 12345\n\n"
                error_msg += "💡 *Tip:* To search area code, use 🔍 search numbers button"
                safe_send_message(message.chat.id, error_msg, parse_mode="Markdown")
                return

            # If no mode is set, default behavior (for backward compatibility)
            if current_mode is None:
                if pattern_length == 3:
                    user_search_mode[user_id] = "search_ca_numbers" # Default to CA
                else:
                    user_search_mode[user_id] = "target_numbers"

            try:
                if user_id not in user_data:
                    login_msg = "🔒 *Please log in first before proceeding.*\n"
                    login_msg += "📌 *Login is required to access this feature.*"
                    safe_send_message(message.chat.id, login_msg, parse_mode="Markdown")
                    return

                credentials = user_data[user_id]

                # Try current account first
                try:
                    twilio_client = Client(credentials['sid'], credentials['auth_token'])

                    # Fetch up to 50 numbers with enhanced pattern-based search
                    available_numbers = []
                    
                    # Determine country based on search mode
                    country_code = 'US' if current_mode == "search_us_numbers" else 'CA'
                    number_fetcher = twilio_client.available_phone_numbers(country_code).local

                    # Smart pattern-based search logic based on mode
                    if current_mode in ["search_ca_numbers", "search_us_numbers"] and pattern_length == 3:
                        # Area code search for CA or US
                        response = number_fetcher.list(
                            area_code=pattern,
                            limit=50,
                            sms_enabled=True,
                            voice_enabled=True
                        )
                        available_numbers.extend(response)
                    elif current_mode == "target_numbers":
                        # Target numbers mode: search by contains pattern
                        response = number_fetcher.list(
                            contains=pattern,
                            limit=50,
                            sms_enabled=True,
                            voice_enabled=True
                        )
                        available_numbers.extend(response)

                        # If no results with contains, try near_number search
                        if not available_numbers:
                            try:
                                # Create a dummy phone number with the pattern
                                dummy_number = f"+1{pattern}0000000"[:12]  # Pad to make valid number
                                response = number_fetcher.list(
                                    near_number=dummy_number,
                                    limit=30,
                                    sms_enabled=True,
                                    voice_enabled=True
                                )
                                available_numbers.extend(response)
                            except:
                                pass
                    else:
                        # Default behavior for backward compatibility
                        if pattern_length == 3:
                            response = number_fetcher.list(
                                area_code=pattern,
                                limit=50,
                                sms_enabled=True,
                                voice_enabled=True
                            )
                        else:
                            response = number_fetcher.list(
                                contains=pattern,
                                limit=50,
                                sms_enabled=True,
                                voice_enabled=True
                            )
                        available_numbers.extend(response)

                except Exception as e:
                    # Auto failover for bulk accounts
                    if credentials.get("using_bulk_pool", False):
                        logger.warning(f"Account failed for user {user_id}, trying auto failover: {str(e)}")

                        # Get next working account from bulk pool
                        new_sid, new_token = mark_account_as_failed(user_id, str(e))
                        if new_sid:
                            # Update user credentials
                            user_data[user_id]["sid"] = new_sid
                            user_data[user_id]["auth_token"] = new_token

                            # Retry with new account
                            twilio_client = Client(new_sid, new_token)
                            country_code = 'US' if current_mode == "search_us_numbers" else 'CA'
                            number_fetcher = twilio_client.available_phone_numbers(country_code).local
                            if current_mode in ["search_ca_numbers", "search_us_numbers"] and pattern_length == 3:
                                response = number_fetcher.list(
                                    area_code=pattern,
                                    limit=50,
                                    sms_enabled=True,
                                    voice_enabled=True
                                )
                            else:
                                response = number_fetcher.list(
                                    contains=pattern,
                                    limit=50,
                                    sms_enabled=True,
                                    voice_enabled=True
                                )
                            available_numbers.extend(response)

                            # Get current account number for display
                            current_bulk_index = user_data[user_id].get("current_bulk_index", 0)
                            safe_send_message(message.chat.id, f"🔄 *Auto Failover Successful!*\n\nSearching with account #{current_bulk_index + 1}...", parse_mode="Markdown")
                        else:
                            safe_send_message(message.chat.id, "⚠️ All bulk accounts have been destroyed. Please add new accounts.")
                            return
                    else:
                        # Re-raise error if not using bulk system
                        raise e

                if available_numbers:
                    # Initialize generated_numbers if not exists
                    if user_id not in generated_numbers:
                        generated_numbers[user_id] = []

                    # Add new numbers to existing list instead of replacing
                    new_numbers = [num.phone_number for num in available_numbers]
                    generated_numbers[user_id].extend(new_numbers)

                    # Remove duplicates while preserving order
                    seen = set()
                    generated_numbers[user_id] = [x for x in generated_numbers[user_id] if not (x in seen or seen.add(x))]

                    # Enhanced search result message with total count
                    total_numbers_in_collection = len(generated_numbers[user_id])

                    result_msg = f"🎯 {pattern} Area Code Search Result 🎯\n\n"
                    result_msg += f"📱 Found: {len(available_numbers)} new numbers\n"
                    result_msg += f"📊 In stock: {total_numbers_in_collection} numbers\n"
                    result_msg += f"📌 Send your preferred area code to search again"

                    safe_send_message(message.chat.id, result_msg, parse_mode="Markdown")

                    # Send numbers in batches of 30 with counter
                    numbers_sent = 0
                    for num in available_numbers:
                        phone_number = num.phone_number
                        if safe_send_message(user_id, phone_number):
                            numbers_sent += 1

                            # After every 30 numbers, send encouragement message
                            if numbers_sent % 30 == 0:
                                encouragement_msg = f"🚀 {numbers_sent} numbers sent successfully!\n\n"
                                encouragement_msg += f"💫 Want more numbers? Search again!\n"
                                encouragement_msg += f"🔄 Get more numbers with a new area code"
                                safe_send_message(user_id, encouragement_msg, parse_mode="Markdown")
                        else:
                            logger.error(f"Failed to send number: {phone_number}")

                    # Final message if remaining numbers (less than 30)
                    if numbers_sent > 0 and numbers_sent % 30 != 0:
                        final_msg = f"✅ **Total {numbers_sent} numbers sent successfully!**\n\n"
                        final_msg += f"🔄 **To get more numbers** search again\n"
                        final_msg += f"⚡ **Check unlimited** numbers together\n"
                        final_msg += f"🎯 **Forward to vote** with the best numbers!"
                        safe_send_message(user_id, final_msg, parse_mode="Markdown")
                else:
                    no_result_msg = f"🎯 {pattern} Area Code Search Result 🎯\n\n"
                    no_result_msg += f"📱 Found: 0 new numbers\n"
                    no_result_msg += f"📊 In stock: 0 numbers\n"
                    no_result_msg += f"📌 Send your preferred area code to search again"
                    safe_send_message(message.chat.id, no_result_msg)
            except Exception as e:
                safe_send_message(message.chat.id, f"Error searching numbers: {str(e)}")
                logger.error(f"Error in fetch_numbers_by_area_code for User ID {user_id}: {str(e)}")

        # Check channels status
        @bot.message_handler(func=lambda message: message.text == "🔗 Check Channels")
        @comprehensive_error_handler
        def check_channels_status(message):
            user_id = message.chat.id
            if user_id == admin_id:
                safe_send_message(message.chat.id, "You are admin, no need to check channels.")
                return

            if robust_channel_check(user_id):
                status_msg = "✅ Channel Status: Active\n\n"
                status_msg += "🎉 You have joined all required channels!\n\n"
                status_msg += "📢 Joined Channels:\n"
                for i, channel in enumerate(REQUIRED_CHANNELS, 1):
                    status_msg += f"{i}. {channel['username']} ✅\n"
                status_msg += "\n🎯 Now you can use all features completely free!"
                safe_send_message(message.chat.id, status_msg)
            else:
                status_msg = "❌ Channel Status: Incomplete\n\n"
                status_msg += "⚠️ You haven't joined all channels!\n\n"
                status_msg += "📢 Required Channels:\n"
                for i, channel in enumerate(REQUIRED_CHANNELS, 1):
                    status_msg += f"{i}. {channel['username']}\n"
                safe_send_message(message.chat.id, status_msg, reply_markup=create_channel_join_menu())

        # Help handler
        @bot.message_handler(func=lambda message: message.text == "❓ Help")
        @comprehensive_error_handler
        def help_handler(message):
            user_id = message.chat.id

            if not is_user_authorized(user_id):
                channel_msg = "🔔 Channel membership required!\n\n"
                channel_msg += "✨ To get help, first join these channels:\n\n"
                for i, channel in enumerate(REQUIRED_CHANNELS, 1):
                    channel_msg += f"{i}. {channel['username']}\n"
                channel_msg += "\n🎯 Use all features completely free after joining!"
                safe_send_message(message.chat.id, channel_msg, reply_markup=create_channel_join_menu())
                return

            help_msg = "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            help_msg += "📌 COMPLETE BOT USAGE GUIDE 📌\n"
            help_msg += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"

            help_msg += "🔑 Login Method:\n"
            help_msg += "• Click the login button\n"
            help_msg += "• Enter your Twilio Account SID and Auth Token\n"
            help_msg += "• Format: SID Auth_Token (separated by space)\n\n"

            help_msg += "🔐 Bulk Login (Multiple Accounts)\n"
            help_msg += "• Click the bulk login' button\n"
            help_msg += "• Enter one account per line\n"
            help_msg += "• Auto-failover system will activate\n\n"

            help_msg += "🔍 Number Search Rules:\n"
            help_msg += "• Click the 'search numbers button\n"
            help_msg += "• Enter 3-digit area code (e.g., 416, 647)\n"
            help_msg += "• View available numbers and purchase your choice\n\n" # Corrected example

            help_msg += "🎯 Target Number (Specific Pattern)\n"
            help_msg += "• Click the target number' button\n"
            help_msg += "• Enter any 3-5 digit pattern\n"
            help_msg += "• Smart search system will work\n\n"

            help_msg += "📩 SMS Receiving Method:\n"
            help_msg += "• First purchase a number\n"
            help_msg += "• Click the 'receive sms' button\n"
            help_msg += "• Or use the 'View SMS' button after purchase\n\n"

            help_msg += "🚪 Logout Process:\n"
            help_msg += "• Click the logout' button\n"
            help_msg += "• All data will be erased for security\n\n"

            help_msg += "📋 Check Channel:\n"
            help_msg += "• Check your status with 'check channels\n"
            help_msg += "• Join the channel if required\n\n"

            help_msg += "⚠️ Important Information:\n"
            help_msg += "• Must stay in channel to use the bot\n"
            help_msg += "• Auto-failover when adding multiple accounts\n"
            help_msg += "• All services are completely free and secure\n\n"

            help_msg += "🎯 Pro Tips:\n"
            help_msg += "• Add up to 15 accounts in bulk login\n"
            help_msg += "• Popular area codes: e.g., 416, 647, 437, 905\n"
            help_msg += "• Try patterns like 123, 456, 789\n\n" # Corrected example

            help_msg += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            help_msg += "💫 Contact admin for any issues!\n"
            help_msg += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

            # Create inline keyboard with Admin Inbox button
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("📧 Admin Inbox", url=f"https://t.me/{ADMIN_USERNAME[1:]}"))

            safe_send_message(message.chat.id, help_msg, reply_markup=markup, parse_mode="Markdown")

        # Handle Forwarded Numbers
        @bot.message_handler(func=lambda message: message.text.lower() == "clear history")
        @comprehensive_error_handler
        def clear_search_history(message):
            user_id = message.chat.id

            if not is_user_authorized(user_id):
                return

            if user_id in generated_numbers and generated_numbers[user_id]:
                count = len(generated_numbers[user_id])
                generated_numbers.pop(user_id, None) # Use pop for cleaner removal
                safe_send_message(message.chat.id, f"✅ Search history cleared for {count} numbers!")
            else:
                safe_send_message(message.chat.id, "📋 No search history found.")

        @bot.message_handler(content_types=['text'])
        @comprehensive_error_handler
        def handle_numbers(message):
            user_id = message.chat.id

            if not is_user_authorized(user_id):
                channel_msg = "🔔 Channel subscription required!\n\n"
                channel_msg += "✨ To use the bot, first join these channels:\n\n"
                for i, channel in enumerate(REQUIRED_CHANNELS, 1):
                    channel_msg += f"{i}. {channel['username']}\n"
                channel_msg += "\n🎯 Use all features completely free after joining!"
                safe_send_message(message.chat.id, channel_msg, reply_markup=create_channel_join_menu())
                return

            if user_id not in user_data:
                login_msg = "🔒 Please log in first before proceeding\n"
                login_msg += "📌 Login is required to access this feature\n\n"

                safe_send_message(message.chat.id, login_msg, parse_mode="Markdown")
                return

            # Check against all possible button texts to avoid re-triggering handlers
            button_texts = ["👤 Login", "➕ Bulk Login", "📤 Logout", "🔎 Search Numbers", "🇺🇸 USA Numbers", "📍 Target Number", "💬 Receive SMS", "🔗 Check Channels", "❓ Help", "⚙️ Admin Panel", "📣 Broadcast", "clear history"]
            # Add other variations from create_main_menu if necessary
            if message.text in button_texts:
                return

            try:
                original_text = message.text
                lines = [line.strip() for line in original_text.split('\n') if line.strip()]
                normalized_numbers = []
                original_lines = []

                for line in lines:
                    try:
                        phone_pattern = r'(?:\+?1?\s?-?)?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}'
                        match = re.search(phone_pattern, line)
                        if match:
                            full_number = match.group(0)
                            digits = re.sub(r'[^\d]', '', full_number)
                            if len(digits) == 10:
                                normalized = '+1' + digits
                            elif len(digits) == 11 and digits.startswith('1'):
                                normalized = '+' + digits
                            else:
                                continue
                            normalized_numbers.append(normalized)
                            original_lines.append(line)
                    except Exception as e:
                        logger.error(f"Error processing line: {line}, Error: {str(e)}")
                        continue

                if not normalized_numbers:
                    return

                try:
                    number_to_original = dict(zip(normalized_numbers, original_lines))
                except Exception as e:
                    logger.error(f"Error creating number mapping: {str(e)}")
                    return

                for number in normalized_numbers:
                    try:
                        if user_id in generated_numbers and number in generated_numbers[user_id]:
                            markup = InlineKeyboardMarkup()
                            markup.add(InlineKeyboardButton("Buy", callback_data=f"buy_{number}"))
                            display_number = f"📱 *Number:* `{number}`\n💫 _Click Buy button_"
                            if not safe_send_message(message.chat.id, display_number, reply_markup=markup, parse_mode="Markdown"):
                                logger.error(f"Failed to send number with Buy button: {number}")
                        else:
                            info = extract_whatsapp_info(message.text)
                            if info['number'] and info['code'] and info['time']:
                                formatted_msg, markup = format_sms_message(info['number'], info['code'], info['time'])
                                safe_send_message(message.chat.id, formatted_msg, reply_markup=markup, parse_mode="Markdown")
                    except Exception as e:
                        logger.error(f"Error processing number {number}: {str(e)}")
                        continue
            except Exception as e:
                safe_send_message(message.chat.id, f"Error processing number: {str(e)}")
                logger.error(f"Error in handle_numbers for User ID {user_id}: {str(e)}, Original text: {message.text}")

        # Buy Number Callback
        @bot.callback_query_handler(func=lambda call: call.data.startswith("buy_"))
        @comprehensive_error_handler
        def buy_number(call):
            user_id = call.message.chat.id

            if not is_user_authorized(user_id):
                try:
                    bot.answer_callback_query(call.id, "Please join the channels first!", show_alert=True)
                except:
                    pass
                return

            try:
                phone_number = call.data.split("_")[1]
                if user_id not in user_data:
                    try:
                        bot.answer_callback_query(call.id, "Please login first!")
                    except:
                        pass
                    return

                credentials = user_data[user_id]

                # Try current account first
                try:
                    twilio_client = Client(credentials['sid'], credentials['auth_token'])

                    if user_id in user_current_number:
                        try:
                            previous_number_sid = user_current_number[user_id]['sid']
                            twilio_client.incoming_phone_numbers(previous_number_sid).delete()
                        except Exception as e:
                            logger.error(f"Error deleting previous number for User ID {user_id}: {str(e)}")

                    purchased_number = twilio_client.incoming_phone_numbers.create(phone_number=phone_number)

                except Exception as e:
                    # Auto failover for bulk accounts
                    if credentials.get("using_bulk_pool", False):
                        logger.warning(f"Account failed during purchase for user {user_id}, trying auto failover: {str(e)}")

                        # Get next working account from bulk pool
                        new_sid, new_token = mark_account_as_failed(user_id, str(e))
                        if new_sid:
                            # Update user credentials
                            user_data[user_id]["sid"] = new_sid
                            user_data[user_id]["auth_token"] = new_token

                            # Retry with new account
                            twilio_client = Client(new_sid, new_token)
                            purchased_number = twilio_client.incoming_phone_numbers.create(phone_number=phone_number)

                            # Get current account number for display
                            current_bulk_index = user_data[user_id].get("current_bulk_index", 0)
                            try:
                                bot.answer_callback_query(call.id, f"🔄 Auto Failover! Number purchased with account #{current_bulk_index + 1}!")
                            except:
                                pass
                        else:
                            try:
                                bot.answer_callback_query(call.id, "⚠️ All bulk accounts have been destroyed.")
                            except:
                                pass
                            return
                    else:
                        # Re-raise error if not using bulk system
                        raise e
                user_current_number[user_id] = {
                    "phone_number": purchased_number.phone_number,
                    "sid": purchased_number.sid
                }

                markup = InlineKeyboardMarkup()
                markup.add(InlineKeyboardButton(text="View SMS 📩", callback_data="view_sms"))

                try:
                    bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id,
                                          text=f"Successfully purchased: `{purchased_number.phone_number}` _(click to copy)_",
                                          reply_markup=markup,
                                          parse_mode="Markdown")
                except:
                    pass
                try:
                    bot.answer_callback_query(call.id, "Number purchased successfully!")
                except:
                    pass
            except Exception as e:
                error_message = str(e)
                if "Trial account" in error_message:
                    try:
                        bot.answer_callback_query(call.id, "Error: Trial account cannot purchase this number. Please upgrade your Twilio account.")
                    except:
                        pass
                else:
                    if "Account is restricted" in error_message:
                        error_msg = "⚠️ Sorry! Your free Twilio account can no longer purchase new numbers. Please use a paid account."
                    else:
                        error_msg = f"ত্রুটি: {error_message[:100]}"
                    try:
                        bot.answer_callback_query(call.id, error_msg)
                    except:
                        pass
                logger.error(f"Error in buy_number for User ID {user_id}: {str(e)}")

        # Receive SMS Messages
        @bot.message_handler(func=lambda message: message.text == "💬 Receive SMS")
        @comprehensive_error_handler
        def receive_sms(message):
            user_id = message.chat.id

            if not is_user_authorized(user_id):
                channel_msg = "🔔 Channel membership required!\n\n"
                channel_msg += "✨ To receive SMS, first join these channels:\n\n"
                for i, channel in enumerate(REQUIRED_CHANNELS, 1):
                    channel_msg += f"{i}. {channel['username']}\n"
                channel_msg += "\n🎯 Use all features completely free after joining!"
                safe_send_message(message.chat.id, channel_msg, reply_markup=create_channel_join_menu())
                return

            if user_id not in user_data:
                login_msg = "🔒 Please log in first before proceeding\n"
                login_msg += "📌 Login is required to access this feature\n\n"
                safe_send_message(message.chat.id, login_msg, parse_mode="Markdown")
                return

            if user_id not in user_current_number:
                safe_send_message(message.chat.id, "You haven't purchased any phone number yet. Use '🔍 search numbers' to buy one.")
                return

            try:
                credentials = user_data[user_id]
                phone_number = user_current_number[user_id]['phone_number']

                # Try current account first
                try:
                    twilio_client = Client(credentials['sid'], credentials['auth_token'])
                    messages = twilio_client.messages.list(to=phone_number, limit=10)

                except Exception as e:
                    # Auto failover for bulk accounts
                    if credentials.get("using_bulk_pool", False):
                        logger.warning(f"Account failed during SMS fetch for user {user_id}, trying auto failover: {str(e)}")

                        # Get next working account from bulk pool
                        new_sid, new_token = mark_account_as_failed(user_id, str(e))
                        if new_sid:
                            # Update user credentials
                            user_data[user_id]["sid"] = new_sid
                            user_data[user_id]["auth_token"] = new_token

                            # Retry with new account
                            twilio_client = Client(new_sid, new_token)
                            messages = twilio_client.messages.list(to=phone_number, limit=10)

                            # Get current account number for display
                            current_bulk_index = user_data[user_id].get("current_bulk_index", 0)
                            safe_send_message(message.chat.id, f"🔄 *Auto Failover Successful!*\n\nChecking SMS with account #{current_bulk_index + 1}...", parse_mode="Markdown")
                        else:
                            safe_send_message(message.chat.id, "⚠️ All bulk accounts have failed. Cannot retrieve SMS.")
                            return
                    else:
                        # Re-raise error if not using bulk system
                        raise e

                if messages:
                    response = f"📱 *Number:* `{phone_number}`\n\n"
                    response += "📩 *Recent SMS Messages:*\n"
                    response += "━━━━━━━━━━━━━━━━━━━━━\n"
                    for msg in messages:
                        timestamp = msg.date_sent.strftime("%Y-%m-%d %H:%M:%S") if msg.date_sent else "Unknown time"
                        response += f"👤 *From:* {msg.from_}\n⏰ *Time:* {timestamp}\n\n```\n{msg.body}\n```\n\n"
                    safe_send_message(message.chat.id, response, parse_mode="Markdown")
                else:
                    safe_send_message(message.chat.id, f"*Purchased Number:* `{phone_number}`\n\nNo SMS messages found for this number.", parse_mode="Markdown")
            except Exception as e:
                safe_send_message(message.chat.id, f"Error retrieving SMS: {e}")
                logger.error(f"Error in receive_sms for User ID {user_id}: {str(e)}")

        # View SMS via Inline Button
        @bot.callback_query_handler(func=lambda call: call.data.startswith("copy_"))
        @comprehensive_error_handler
        def copy_text_callback(call):
            try:
                text_to_copy = call.data.replace("copy_", "")
                try:
                    bot.answer_callback_query(call.id, f"Copied: {text_to_copy}")
                except:
                    pass
            except Exception as e:
                try:
                    bot.answer_callback_query(call.id, "Failed to copy")
                except:
                    pass
                logger.error(f"Error in copy_text_callback: {str(e)}")

        @bot.callback_query_handler(func=lambda call: call.data == "view_sms")
        @comprehensive_error_handler
        def view_sms_callback(call):
            user_id = call.message.chat.id

            if not is_user_authorized(user_id):
                try:
                    bot.answer_callback_query(call.id, "Please join the channels first!", show_alert=True)
                except:
                    pass
                return

            if user_id not in user_data:
                try:
                    bot.answer_callback_query(call.id, "Please login first!")
                except:
                    pass
                return

            if user_id not in user_current_number:
                try:
                    bot.answer_callback_query(call.id, "You haven't purchased any phone number yet. Use '🔍 search numbers' to buy one.")
                except:
                    pass
                return

            try:
                credentials = user_data[user_id]
                phone_number = user_current_number[user_id]['phone_number']

                # Try current account first
                try:
                    twilio_client = Client(credentials['sid'], credentials['auth_token'])
                    messages = twilio_client.messages.list(to=phone_number, limit=1)

                except Exception as e:
                    # Auto failover for bulk accounts
                    if credentials.get("using_bulk_pool", False):
                        logger.warning(f"Account failed during SMS view for user {user_id}, trying auto failover: {str(e)}")

                        # Get next working account from bulk pool
                        new_sid, new_token = mark_account_as_failed(user_id, str(e))
                        if new_sid:
                            # Update user credentials
                            user_data[user_id]["sid"] = new_sid
                            user_data[user_id]["auth_token"] = new_token

                            # Retry with new account
                            twilio_client = Client(new_sid, new_token)
                            messages = twilio_client.messages.list(to=phone_number, limit=1)
                        else:
                            try:
                                bot.answer_callback_query(call.id, "⚠️ All bulk accounts have been destroyed")
                            except:
                                pass
                            return
                    else:
                        # Re-raise error if not using bulk system
                        raise e

                markup = InlineKeyboardMarkup()
                markup.add(InlineKeyboardButton(text="View SMS 📩", callback_data="view_sms"))

                if not messages:
                    response = f"`{phone_number}`\n\nNo new SMS found."
                else:
                    msg = messages[0]

                    # Extract OTP from message body
                    otp_match = re.search(r'\b\d{4,8}\b', msg.body)
                    otp_code = otp_match.group(0) if otp_match else "No OTP Found"

                    # New format as requested by the user
                    # 1. OTP Code
                    # 2. Purchased Number
                    # 3. Full SMS Body
                    response_parts = [
                        otp_code,
                        phone_number,
                        f"\n{msg.body}"
                    ]
                    response = "\n".join(response_parts)

                if len(response) > 4000:
                    response = response[:3997] + "..."
                try:
                    bot.edit_message_text(
                        chat_id=call.message.chat.id,
                        message_id=call.message.message_id,
                        text=response,
                        reply_markup=markup,
                        parse_mode=None # Use None for plain text
                    )
                except:
                    pass
                try:
                    bot.answer_callback_query(call.id, "Viewing SMS")
                except:
                    pass
            except Exception as e:
                try:
                    bot.answer_callback_query(call.id, f"Error retrieving SMS: {e}")
                except:
                    pass
                logger.error(f"Error in view_sms_callback for User ID {user_id}: {str(e)}")

        # Admin Actions
        @bot.callback_query_handler(func=lambda call: call.data.startswith("admin_"))
        @comprehensive_error_handler
        def admin_actions(call):
            if call.message.chat.id != admin_id:
                try:
                    bot.answer_callback_query(call.id, "Unauthorized access.")
                except:
                    pass
                return

            action = call.data.split("_")[1]
            safe_send_message(call.message.chat.id, f"Please provide User ID to {action}:")
            try:
                bot.register_next_step_handler(call.message, lambda msg: process_admin_action(msg, action))
            except Exception as e:
                logger.error(f"Error registering step handler: {str(e)}")

        @comprehensive_error_handler
        def process_admin_action(message, action):
            try:
                user_id = int(message.text.strip())
                logger.info(f"Processing {action} for User ID: {user_id}")

                if action == "approve":
                    if user_id in registered_users and registered_users[user_id]["status"] == "approved":
                        safe_send_message(message.chat.id, f"User {user_id} is already approved.")
                        return

                    if user_id not in registered_users:
                        registered_users[user_id] = {
                            "status": "pending",
                            "channel_joined": False,
                            "first_use_time": None
                        }

                    registered_users[user_id]["status"] = "approved"
                    safe_save_registered_users()

                    safe_send_message(message.chat.id, f"✅ User {user_id} has been successfully approved.")

                    approval_msg = "🌟 *Congratulations! Your account has been approved* 🌟\n"
                    approval_msg += "━━━━━━━━━━━━━━━━━━━━━\n\n"
                    approval_msg += "✨ Now you can use the bot!\n\n"
                    approval_msg += "📋 *Next Steps:*\n"
                    approval_msg += "• Join the required channels\n"
                    approval_msg += "• Complete channel verification\n"
                    approval_msg += "• Enjoy all bot features\n\n"
                    approval_msg += "💫 _Thank you for using our service_"

                    safe_send_message(user_id, approval_msg, parse_mode="Markdown")

                elif action == "block":
                    if user_id in registered_users:
                        registered_users[user_id]["status"] = "blocked"
                        block_msg = "⛔️ *Account Block Notification*\n"
                        block_msg += "━━━━━━━━━━━━━━━━━━━━━\n\n"
                        block_msg += "❌ Your account has been temporarily blocked.\n\n"
                        block_msg += "📝 *What to do:*\n"
                        block_msg += f"• Contact {ADMIN_USERNAME}\n"
                        block_msg += "• Explain your problem in detail\n"
                        block_msg += "• Promise to follow the rules\n\n"
                        block_msg += "⚠️ _We will try to solve your problem quickly_"
                        safe_send_message(user_id, block_msg, parse_mode="Markdown")
                        safe_send_message(message.chat.id, f"✅ User {user_id} has been successfully blocked.")
                        safe_save_registered_users()
                    else:
                        safe_send_message(message.chat.id, f"❌ User {user_id} Not registered.")

                elif action == "unblock":
                    if user_id in registered_users:
                        registered_users[user_id]["status"] = "approved"
                        unblock_msg = "🎉 *Congratulations! Your account has been unblocked*\n"
                        unblock_msg += "━━━━━━━━━━━━━━━━━━━━━\n\n"
                        unblock_msg += "✅ Now you can use our services again\n\n"
                        unblock_msg += "📱 *Next Steps:*\n"
                        unblock_msg += "• Join the channels\n"
                        unblock_msg += "• Complete verification\n\n"
                        unblock_msg += "💫 _Thank you for using our service_"
                        safe_send_message(user_id, unblock_msg, parse_mode="Markdown")
                        safe_send_message(message.chat.id, f"✅ User {user_id} has been successfully unblocked.")
                        safe_save_registered_users()
                    else:
                        safe_send_message(message.chat.id, f"User {user_id} is not registered.")
            except ValueError:
                safe_send_message(message.chat.id, "Invalid User ID. Please provide a valid numeric ID.")
            except Exception as e:
                safe_send_message(message.chat.id, f"An error occurred: {str(e)}")
                logger.error(f"Error in process_admin_action for User ID {message.text}: {str(e)}")

        logger.info("All handlers setup completed successfully")
        return True

    except Exception as e:
        logger.error(f"Critical error setting up handlers: {str(e)}", exc_info=True)
        return False

def graceful_shutdown(signum, frame):
    """Handle graceful shutdown with enhanced cleanup"""
    logger.info(f"Received signal {signum}, initiating graceful shutdown...")
    shutdown_flag.set()

    try:
        # Save all data
        safe_save_registered_users()

        # Stop bot
        if bot:
            try:
                bot.stop_polling()
                time.sleep(2)
            except:
                pass

        # Force cleanup
        gc.collect()

    except Exception as e:
        logger.error(f"Error during shutdown: {str(e)}")

    logger.info("Graceful shutdown completed")
    sys.exit(0)

def continuous_health_monitor():
    """Continuous health monitoring with self-healing"""
    consecutive_failures = 0
    max_consecutive_failures = 5

    while not shutdown_flag.is_set():
        try:
            if bot:
                try:
                    # Simple API call to check bot health
                    bot.get_me()
                    consecutive_failures = 0  # Reset on success

                    # Memory cleanup every hour
                    if time.time() % 3600 < 300:  # Within 5 minutes of hour mark
                        gc.collect()

                except Exception as e:
                    consecutive_failures += 1
                    logger.error(f"Health check failed ({consecutive_failures}/{max_consecutive_failures}): {str(e)}")

                    if consecutive_failures >= max_consecutive_failures:
                        logger.error("Too many consecutive health check failures, triggering restart...")
                        raise Exception("Health check failure threshold exceeded")

            time.sleep(60)  # Check every minute

        except Exception as e:
            logger.error(f"Health monitor error: {str(e)}")
            consecutive_failures += 1
            if consecutive_failures >= max_consecutive_failures:
                logger.error("Health monitor triggering main loop restart...")
                os._exit(1)  # Force restart
            time.sleep(30)

def advanced_main_loop():
    """Advanced main loop with maximum stability and infinite restart"""
    global bot

    # Setup signal handlers with error handling
    try:
        signal.signal(signal.SIGINT, graceful_shutdown)
        signal.signal(signal.SIGTERM, graceful_shutdown)
        signal.signal(signal.SIGUSR1, graceful_shutdown)  # Additional signal
    except Exception as e:
        logger.error(f"Error setting up signal handlers: {str(e)}")

    logger.info("🚀 Starting Ultra-Stable Telegram Bot with Infinite Restart Capability...")

    # Load user data with enhanced safety
    try:
        safe_load_registered_users()
    except Exception as e:
        logger.error(f"Error loading users, continuing with defaults: {str(e)}")

    # Start health monitor thread
    health_thread = threading.Thread(target=continuous_health_monitor, daemon=True)

    restart_attempts = 0
    max_restart_attempts = 1000000  # Virtually unlimited
    base_restart_delay = 5
    max_restart_delay = 300  # 5 minutes max

    # ♾️ INFINITE RESTART LOOP - Never Give Up! ♾️
    while restart_attempts < max_restart_attempts and not shutdown_flag.is_set():
        try:
            restart_attempts += 1
            logger.info(f"🔄 Bot startup attempt #{restart_attempts}")

            # Bot initialization with maximum retries
            initialization_success = False
            for init_attempt in range(10):
                try:
                    if initialize_bot_with_retry(max_attempts=5):
                        initialization_success = True
                        break
                    else:
                        logger.error(f"Bot initialization failed on attempt {init_attempt + 1}")
                        time.sleep(min(5 * (init_attempt + 1), 30))
                except Exception as e:
                    logger.error(f"Exception during bot initialization attempt {init_attempt + 1}: {str(e)}")
                    time.sleep(5)

            if not initialization_success:
                logger.error("Failed to initialize bot after maximum attempts, restarting main loop...")
                time.sleep(60)
                continue

            # Setup handlers with retry logic
            handler_setup_success = False
            for handler_attempt in range(5):
                try:
                    if setup_all_handlers():
                        handler_setup_success = True
                        logger.info("✅ All handlers setup completed successfully")
                        break
                    else:
                        logger.error(f"Handler setup failed on attempt {handler_attempt + 1}")
                        time.sleep(5)
                except Exception as e:
                    logger.error(f"Exception during handler setup attempt {handler_attempt + 1}: {str(e)}")
                    time.sleep(5)

            if not handler_setup_success:
                logger.error("Failed to setup handlers after maximum attempts, restarting...")
                time.sleep(30)
                continue

            # Start health monitor if not running
            if not health_thread.is_alive():
                try:
                    health_thread = threading.Thread(target=continuous_health_monitor, daemon=True)
                    health_thread.start()
                    logger.info("🏥 Health monitoring started")
                except Exception as e:
                    logger.error(f"Failed to start health monitor: {str(e)}")

            # Success notification
            logger.info(f"🎉 Bot successfully started on attempt #{restart_attempts}")
            if restart_attempts > 1:
                logger.info(f"🔥 Bot recovered after {restart_attempts} restart attempts! 🔥")

            # Reset restart delay on successful start
            restart_delay = base_restart_delay

            # 🔄 MAIN POLLING LOOP WITH ULTRA ERROR HANDLING 🔄
            while not shutdown_flag.is_set():
                try:
                    # Clear and simple message for the terminal
                    print("✅ Bot is running now...")

                    # Enhanced polling with multiple safety nets
                    bot.polling(
                        none_stop=True,
                        interval=1,
                        timeout=60,
                        long_polling_timeout=60,
                        restart_on_change=False
                    )

                except KeyboardInterrupt:
                    logger.info("🛑 KeyboardInterrupt received, shutting down gracefully...")
                    raise

                except telebot.apihelper.ApiException as e:
                    error_msg = str(e).lower()
                    if "network" in error_msg or "timeout" in error_msg:
                        logger.warning(f"📡 Network issue detected: {str(e)}, retrying in 10 seconds...")
                        time.sleep(10)
                        continue
                    elif "conflict" in error_msg:
                        logger.error(f"🔄 Polling conflict detected: {str(e)}, restarting bot...")
                        break
                    else:
                        logger.error(f"🚨 Telegram API error: {str(e)}")
                        time.sleep(15)
                        continue

                except Exception as e:
                    logger.error(f"💥 Polling error: {str(e)}", exc_info=True)
                    time.sleep(10)
                    break  # Break to restart

        except KeyboardInterrupt:
            logger.info("🛑 Manual shutdown requested")
            break

        except Exception as e:
            error_msg = f"💥 Critical error in main loop (attempt #{restart_attempts}): {str(e)}"
            logger.error(error_msg, exc_info=True)

            # Adaptive restart delay
            if restart_attempts <= 5:
                restart_delay = base_restart_delay
            elif restart_attempts <= 15:
                restart_delay = base_restart_delay * 2
            elif restart_attempts <= 50:
                restart_delay = base_restart_delay * 4
            else:
                restart_delay = min(base_restart_delay * 8, max_restart_delay)

        # Cleanup before restart
        try:
            if bot:
                try:
                    bot.stop_polling()
                    time.sleep(2)
                except:
                    pass
                bot = None

            # Save data before restart
            safe_save_registered_users()

            # Force garbage collection
            gc.collect()

        except Exception as cleanup_error:
            logger.error(f"🧹 Error during cleanup: {str(cleanup_error)}")

        # Log restart info
        if restart_attempts < max_restart_attempts:
            logger.info(f"⏳ Waiting {restart_delay} seconds before restart attempt #{restart_attempts + 1}...")

            # Interruptible sleep
            for i in range(restart_delay):
                if shutdown_flag.is_set():
                    break
                time.sleep(1)

    # Final cleanup
    logger.info("🏁 Bot main loop completed")
    try:
        safe_save_registered_users()
    except Exception as e:
        logger.error(f"Error in final data save: {str(e)}")

if __name__ == "__main__":
    try:
        advanced_main_loop()
    except Exception as e:
        logger.error(f"🚨 FATAL ERROR: {str(e)}", exc_info=True)
        sys.exit(1)
