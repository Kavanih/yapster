import requests
import time
import schedule
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ====================
# Configuration Section
# ====================
BASE_URL = "https://hub.kaito.ai/api/v1/gateway/ai"
HEADERS = {
    "accept": "application/json, text/plain, */*",
    "authorization": "Bearer YOUR_BEARER_TOKEN",  # Replace with your actual token
    "content-type": "application/json",
    "origin": "https://yaps.kaito.ai",
    "referer": "https://yaps.kaito.ai/",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
}

PAYLOADS = [
    {
        "topic_id": "0G",
        "url_params": "?duration=30d&topic_id=0G&top_n=100",
        "payload": {
            "path": "/api/yapper/public_kol_mindshare_leaderboard",
            "method": "GET",
            "params": {"duration": "30d", "topic_id": "0G", "top_n": 100},
            "body": {}
        }
    },
    {
        "topic_id": "Pre-TGE",
        "url_params": "?duration=30d",
        "payload": {
            "path": "/api/yapper/public_kol_mindshare_pre_tge",
            "method": "GET",
            "params": {"duration": "30d"},
            "body": {}
        }
    },
    {
        "topic_id": "vcarena",
        "url_params": "?duration=30d&topic_id=vcarena&top_n=100",
        "payload": {
            "path": "/api/yapper/public_kol_mindshare_leaderboard",
            "method": "GET",
            "params": {"duration": "30d", "topic_id": "vcarena", "top_n": 100},
            "body": {}
        }
    },
    {
        "topic_id": "CORN",
        "url_params": "?duration=30d&topic_id=CORN&top_n=100",
        "payload": {
            "path": "/api/yapper/public_kol_mindshare_leaderboard",
            "method": "GET",
            "params": {"duration": "30d", "topic_id": "CORN", "top_n": 100},
            "body": {}
        }
    },
     {
        "topic_id": "GeneralCryptoAI",
        "url_params": "?duration=30d&topic_id=GeneralCryptoAI&top_n=100",
        "payload": {
            "path": "/api/yapper/public_kol_mindshare_leaderboard",
            "method": "GET",
            "params": {"duration": "30d", "topic_id": "GeneralCryptoAI", "top_n": 100},
            "body": {}
        }
    },
    {
        "topic_id": "ECLIPSE",
        "url_params": "?duration=30d&topic_id=ECLIPSE&top_n=100",
        "payload": {
            "path": "/api/yapper/public_kol_mindshare_leaderboard",
            "method": "GET",
            "params": {"duration": "30d", "topic_id": "ECLIPSE", "top_n": 100},
            "body": {}
        }
    },
    {
        "topic_id": "INITIA",
        "url_params": "?duration=30d&topic_id=INITIA&top_n=100",
        "payload": {
            "path": "/api/yapper/public_kol_mindshare_leaderboard",
            "method": "GET",
            "params": {"duration": "30d", "topic_id": "INITIA", "top_n": 100},
            "body": {}
        }
    },
    {
        "topic_id": "KAITO",
        "url_params": "?duration=30d&topic_id=KAITO&top_n=100",
        "payload": {
            "path": "/api/yapper/public_kol_mindshare_leaderboard",
            "method": "GET",
            "params": {"duration": "30d", "topic_id": "KAITO", "top_n": 100},
            "body": {}
        }
    },
     {
        "topic_id": "MONAD",
        "url_params": "?duration=30d&topic_id=MONAD&top_n=100",
        "payload": {
            "path": "/api/yapper/public_kol_mindshare_leaderboard",
            "method": "GET",
            "params": {"duration": "30d", "topic_id": "MONAD", "top_n": 100},
            "body": {}
        }
    },
    {
        "topic_id": "PARADEX",
        "url_params": "?duration=30d&topic_id=PARADEX&top_n=100",
        "payload": {
            "path": "/api/yapper/public_kol_mindshare_leaderboard",
            "method": "GET",
            "params": {"duration": "30d", "topic_id": "PARADEX", "top_n": 100},
            "body": {}
        }
    },
    {
        "topic_id": "QUAI",
        "url_params": "?duration=30d&topic_id=QUAI&top_n=100",
        "payload": {
            "path": "/api/yapper/public_kol_mindshare_leaderboard",
            "method": "GET",
            "params": {"duration": "30d", "topic_id": "QUAI", "top_n": 100},
            "body": {}
        }
    },
]



SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
CREDENTIALS_FILE = "kaito-sheet-208be79c4b81.json"  # Replace with your JSON key file name
GOOGLE_SHEET_NAME = "KAITO_Data"  # Replace with your Google Sheet name

# ====================
# Helper Functions
# ====================
def fetch_data(url, payload, topic_id, max_retries=3, retry_delay=2):
    """Fetch data from the API for a specific topic with retry logic."""
    print(f"Processing topic: {topic_id}")
    retries = 0
    while retries <= max_retries:
        try:
            response = requests.post(url, headers=HEADERS, json=payload)
            if response.status_code == 200:
                try:
                    data = response.json()
                    return data
                except ValueError:
                    print(f"Failed to parse JSON for topic {topic_id}.")
                    return {"error": f"Failed to parse JSON for topic {topic_id}", "status_code": response.status_code}
            else:
                print(f"Failed to retrieve data for topic {topic_id}: {response.status_code}")
                return {"error": f"Failed to retrieve data for topic {topic_id}", "status_code": response.status_code, "response": response.text}
        except Exception as e:
            print(f"Error fetching data for topic {topic_id}: {e}")
            if retries < max_retries:
                print(f"Retrying in {retry_delay} seconds... (Attempt {retries + 1}/{max_retries})")
                time.sleep(retry_delay)
                retries += 1
            else:
                print(f"Max retries reached for topic {topic_id}. Request Failed: {e}")
                return {"error": f"Request Failed for topic {topic_id}: {e}"}
    return {"error": f"Request failed after {max_retries} retries"}

def extract_user_data(data):
    """Extract required fields from the API response."""
    filtered_users = []
    for user in data:
        filtered_user = {
            "rank": user.get("rank", "N/A"),
            "name": user.get("name", "N/A"),
            "username": user.get("username", "N/A"),
            "twitter_user_url": user.get("twitter_user_url", "N/A") or f"https://x.com/{user.get('username', 'N/A')}"
        }
        filtered_users.append(filtered_user)
    return filtered_users

def update_worksheet(worksheet, rows, new_entries):
    """Update the worksheet with new data and highlight new entries."""
    # Clear the worksheet and write updated data
    worksheet.clear()
    worksheet.insert_rows(rows)
    # Highlight new entries in batches
    if new_entries:
        print(f"Highlighting {len(new_entries)} new entries...")
        highlight_new_entries(worksheet, new_entries)

def highlight_new_entries(worksheet, new_entries):
    """Highlight new entries in the worksheet in batches."""
    all_values = worksheet.get_all_values()
    ranges_to_format = []
    for idx, row in enumerate(all_values[1:], start=2):  # Skip header row
        if row[2] in new_entries:  # Check if the username is in new_entries
            ranges_to_format.append(f"A{idx}:E{idx}")
    # Batch format in chunks of 10 rows
    batch_size = 10
    for i in range(0, len(ranges_to_format), batch_size):
        batch_ranges = ranges_to_format[i:i + batch_size]
        print(f"Highlighting rows {i + 1} to {i + len(batch_ranges)}...")
        try:
            worksheet.batch_format([
                {
                    "range": range_str,
                    "format": {
                        "backgroundColor": {"red": 0.92, "green": 0.98, "blue": 0.85}
                    }
                }
                for range_str in batch_ranges
            ])
        except gspread.exceptions.APIError as e:
            if "Quota exceeded" in str(e):
                print("Rate limit hit. Retrying after a delay...")
                time.sleep(5)  # Wait before retrying
                worksheet.batch_format([
                    {
                        "range": range_str,
                        "format": {
                            "backgroundColor": {"red": 0.92, "green": 0.98, "blue": 0.85}
                        }
                    }
                    for range_str in batch_ranges
                ])
            else:
                raise

# ====================
# Main Functions
# ====================
def fetch_and_update_data():
    """Fetch data for all topics and update Google Sheets."""
    print("Fetching and updating data...")
    combined_data = {}
    # Fetch data for each topic
    for payload_info in PAYLOADS:
        topic_id = payload_info["topic_id"]
        url = f"{BASE_URL}{payload_info['url_params']}"
        payload = payload_info["payload"]
        data = fetch_data(url, payload, topic_id)
        if isinstance(data, list):  # Ensure data is a list before processing
            combined_data[topic_id] = extract_user_data(data)
        elif "error" in data:
            print(f"Error for topic {topic_id}: {data['error']}")
            combined_data[topic_id] = [{"error": data["error"]}]
        else:
            combined_data[topic_id] = [{"error": "Unknown error occurred"}]
        time.sleep(2)  # Optional: Add a small delay between requests
    # Update Google Sheets
    update_google_sheets(combined_data)

def update_google_sheets(data):
    """Update Google Sheets with the fetched data."""
    credentials = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, SCOPE)
    client = gspread.authorize(credentials)
    for topic_id, users in data.items():
        try:
            # Try to get the worksheet for the topic
            worksheet = client.open(GOOGLE_SHEET_NAME).worksheet(topic_id)
        except gspread.WorksheetNotFound:
            # If the worksheet doesn't exist, create it
            worksheet = client.open(GOOGLE_SHEET_NAME).add_worksheet(title=topic_id, rows="1000", cols="20")

        # Fetch existing usernames to detect new entries
        all_values = worksheet.get_all_values()
        existing_usernames = set(row[2] for row in all_values[1:]) if len(all_values) > 1 else set()  # Skip header row

        # Prepare data for the worksheet
        rows = [["Rank", "Name", "Username", "Twitter URL", "New Entry"]]
        new_entries = []
        for user in users:
            if "error" in user:
                # If there was an error, log it in the first row
                rows.append([f"Error: {user['error']}"])
                break  # No need to process further for this topic
            else:
                # Safely get the username, or default to "N/A" if missing
                username = user.get("username", "N/A")
                is_new = username not in existing_usernames if username != "N/A" else False
                if is_new:
                    new_entries.append(username)
                # Use default values for missing keys to prevent errors
                rank = user.get("rank", "N/A")
                name = user.get("name", "N/A")
                twitter_url = user.get("twitter_user_url", "N/A")
                rows.append([rank, name, username, twitter_url, "Yes" if is_new else "No"])

        # Update the worksheet
        update_worksheet(worksheet, rows, new_entries)        
def main():
    """Main function to run the script."""
    # Initial data fetch
    fetch_and_update_data()
    # Schedule the script to run every 24 hours
    schedule.every(24).hours.do(fetch_and_update_data)
    while True:
        schedule.run_pending()
        time.sleep(8)

if __name__ == "__main__":
    main()
