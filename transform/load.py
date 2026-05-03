import json
import os
import glob
from datetime import datetime, timezone
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

def get_engine():
    url = (
        f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )
    return create_engine(url)

def get_latest_file():
    files = glob.glob("data/raw/crypto_*.json")
    if not files:
        print("✗ No raw data files found")
        return None
    latest = max(files, key=os.path.getctime)
    print(f"✓ Using file: {latest}")
    return latest

def insert_bronze(data, conn, fetched_at):
    try:
        conn.execute(text("""
            INSERT IGNORE INTO crypto_raw (data, fetched_at)
            VALUES (:data, :fetched_at)
        """), {"data": json.dumps(data), "fetched_at": fetched_at})
        print("✓ Bronze: raw JSON saved")
    except Exception as e:
        print(f"✗ Bronze failed: {e}")

def insert_silver(data, conn, fetched_at):
    inserted = skipped = 0
    for coin in data:
        try:
            conn.execute(text("""
                INSERT IGNORE INTO crypto_prices (
                    coin_id, symbol, name,
                    current_price, market_cap, total_volume,
                    high_24h, low_24h,
                    price_change_24h, price_change_percentage_24h,
                    market_cap_rank, circulating_supply, max_supply,
                    ath, atl, last_updated, fetched_at
                ) VALUES (
                    :coin_id, :symbol, :name,
                    :current_price, :market_cap, :total_volume,
                    :high_24h, :low_24h,
                    :price_change_24h, :price_change_percentage_24h,
                    :market_cap_rank, :circulating_supply, :max_supply,
                    :ath, :atl, :last_updated, :fetched_at
                )
            """), {
                "coin_id":                    coin["id"],
                "symbol":                     coin["symbol"],
                "name":                       coin["name"],
                "current_price":              coin.get("current_price"),
                "market_cap":                 coin.get("market_cap"),
                "total_volume":               coin.get("total_volume"),
                "high_24h":                   coin.get("high_24h"),
                "low_24h":                    coin.get("low_24h"),
                "price_change_24h":           coin.get("price_change_24h"),
                "price_change_percentage_24h":coin.get("price_change_percentage_24h"),
                "market_cap_rank":            coin.get("market_cap_rank"),
                "circulating_supply":         coin.get("circulating_supply"),
                "max_supply":                 coin.get("max_supply"),
                "ath":                        coin.get("ath"),
                "atl":                        coin.get("atl"),
                "last_updated":               coin.get("last_updated"),
                "fetched_at":                 fetched_at
            })
            inserted += 1
        except Exception as e:
            print(f"Skipped {coin['id']}: {e}")
            skipped += 1
    print(f"Silver: {inserted} inserted | {skipped} skipped")

if __name__ == "__main__":
    file_path = get_latest_file()
    if file_path:
        with open(file_path) as f:
            data = json.load(f)
        engine = get_engine()
        fetched_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        with engine.connect() as conn:
            insert_bronze(data, conn, fetched_at)
            insert_silver(data, conn, fetched_at)
            conn.commit()
