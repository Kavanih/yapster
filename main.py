import logging
import asyncio
import json
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from telegram import Update
from telegram.ext import Application, CommandHandler, CallbackContext
import os

# Replace with your bot token
TELEGRAM_BOT_TOKEN = "7768583690:AAH9MRxkGj2r5lFMxwbml-c1yLycy_--sWI"



# File to store tokens
TOKEN_FILE = "tokens.json"

# Enable logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration Section for Yapster
BASE_URL = "https://hub.kaito.ai/api/v1/yapper/emerging-leaderboard?range=30D"
HEADERS = {
    "accept": "application/json, text/plain, */*",
    "authorization": "",
    "content-type": "application/json",
    "origin": "https://yaps.kaito.ai",
    "referer": "https://yaps.kaito.ai/",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
    "privy-id-token": ""
}

SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
CREDENTIALS_FILE = "kaito-sheet-208be79c4b81.json"  # Replace with your JSON key file name
GOOGLE_SHEET_NAME = "KAITO_Data"  # Replace with your Google Sheet name

# Telegram Bot Functions
async def start(update: Update, context: CallbackContext) -> None:
    await update.message.reply_text('Hi! Use /updateToken <authorization_token> <privy_id_token> to update tokens.')

async def update_token(update: Update, context: CallbackContext) -> None:
    if len(context.args) != 2:
        await update.message.reply_text("Usage: /updateToken <authorization_token> <privy_id_token>")
        return

    authorization_token, privy_id_token = context.args
    tokens = {
        "authorization": authorization_token,
        "privy-id": privy_id_token
    }
    with open(TOKEN_FILE, "w") as file:
        json.dump(tokens, file, indent=4)
    HEADERS["authorization"] = authorization_token
    HEADERS["privy-id-token"] = privy_id_token

    await update.message.reply_text("Tokens updated successfully!\nFetching data from Yapster...")
    # Fetch and update after updating tokens
    await fetch_and_update_data(update)

async def fetch_and_update_data(update: Update):
    print("Fetching data...")
    try:
        response = requests.get(BASE_URL, headers=HEADERS)
        if response.status_code == 200:
            try:
                data = response.json()
                items = data.get("items", [])  # Fetch all available items without limitation
                if items:
                    logger.info(f"Successfully fetched {len(items)} items from Yapster.")
                    update_google_sheet(items)
                    await update.message.reply_text(f"Successfully updated Google Sheet with {len(items)} records.")
                else:
                    logger.warning("No data received from Yapster API.")
                    await update.message.reply_text("No data received from Yapster API.")
            except ValueError:
                logger.error("Failed to parse response as JSON.")
                logger.error(response.text)
                await update.message.reply_text("Failed to parse response from Yapster.")
        else:
            logger.error(f"Failed to retrieve data: {response.status_code}")
            logger.error(response.text)
            await update.message.reply_text(f"Failed to retrieve data from Yapster. Status code: {response.status_code}")
    except Exception as e:
        logger.error(f"Error fetching data: {e}")
        await update.message.reply_text(f"Error fetching data: {e}")

def update_google_sheet(data):
    credentials = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, SCOPE)
    client = gspread.authorize(credentials)
    try:
        worksheet = client.open(GOOGLE_SHEET_NAME).sheet1
    except gspread.WorksheetNotFound:
        worksheet = client.open(GOOGLE_SHEET_NAME).add_worksheet(title="Emerging_Market", rows="1000", cols="20")

    # Fetch existing usernames to detect new entries
    existing_usernames = set(row[2] for row in worksheet.get_all_values()[1:] if len(row) > 2)  # Skip header row
    new_entries = []

    # Prepare data for the worksheet
    rows = [["Rank", "Name", "Username", "Twitter URL", "New Entry"]]
    for item in data:
        rank = item.get("rank", "N/A")
        name = item.get("user_name", "N/A")
        username = item.get("twitter_handle", "N/A")
        twitter_url = f"https://x.com/{username}" if username != "N/A" else "N/A"
        is_new = username not in existing_usernames if username != "N/A" else False

        if is_new:
            new_entries.append(username)
        rows.append([rank, name, username, twitter_url, "Yes" if is_new else "No"])

    # Update the worksheet
    if rows:
        worksheet.clear()
        worksheet.insert_rows(rows)
        logger.info(f"Inserted {len(rows) - 1} rows into Google Sheet.")
        if new_entries:
            logger.info(f"New entries detected: {new_entries}")

def start_bot():
    load_tokens()
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("updateToken", update_token))

    # Start bot in a separate thread
    asyncio.run(application.run_polling())

def load_tokens():
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, "r") as file:
            tokens = json.load(file)
            HEADERS["authorization"] = tokens.get("authorization", "")
            HEADERS["privy-id-token"] = tokens.get("privy-id", "")

if __name__ == "__main__":
    try:
        start_bot()
    except RuntimeError as e:
        logger.error(f"RuntimeError occurred: {e}")
