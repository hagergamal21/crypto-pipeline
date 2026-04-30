import requests
import json
import os
from dotenv import load_dotenv
from datetime import datetime, timezone

load_dotenv()

COINS = ["bitcoin", "ethereum", "solana", "cardano"]
BASE_URL = "https://api.coingecko.com/api/v3/coins/markets"

def fetch_crypto_data():
    params = {
        "vs_currency": "usd",
        "ids": ",".join(COINS),
        "order": "market_cap_desc",
        "sparkline": False
    }
    try:
        response = requests.get(BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        print(f"Fetched {len(data)} coins at {datetime.now(timezone.utc)}")
        return data
    except requests.exceptions.RequestException as e:
        print(f"API request failed: {e}")
        return None

def save_raw_data(data):
    os.makedirs("data/raw", exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    file_path = f"data/raw/crypto_{timestamp}.json"

    with open(file_path, "w") as f:
        json.dump(data, f, indent=2)

    print(f"Saved raw data to {file_path}")
    return file_path

if __name__ == "__main__":
    data = fetch_crypto_data()
    if data:
        save_raw_data(data)