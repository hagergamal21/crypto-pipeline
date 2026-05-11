from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.insert(0, "/Users/hager/work/dev/crypto-pipeline")

from extract.extract import fetch_crypto_data, save_raw_data
from transform.load import get_engine, get_latest_file
from transform.transform import read_silver, clean, aggregate, write_gold

from datetime import timezone
from dotenv import load_dotenv

load_dotenv()

default_args = {
    'owner': 'hager',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

def task_extract():
    data = fetch_crypto_data()
    if data:
        save_raw_data(data)
    else:
        raise ValueError("Extract failed - no data returned")


def task_load():
    import json
    from datetime import datetime, timezone
    from sqlalchemy import text

    filepath = get_latest_file()
    if not filepath:
        raise ValueError("✗ No raw file found")

    with open(filepath) as f:
        data = json.load(f)

    engine = get_engine()
    fetched_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    with engine.begin() as conn:
        # Bronze
        try:
            conn.execute(text("""
                INSERT IGNORE INTO crypto_raw (data, fetched_at)
                VALUES (:data, :fetched_at)
            """), {"data": json.dumps(data), "fetched_at": fetched_at})
            print("✓ Bronze: raw JSON saved")
        except Exception as e:
            print(f"✗ Bronze failed: {e}")

        # Silver
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
                    "coin_id": coin["id"],
                    "symbol": coin["symbol"],
                    "name": coin["name"],
                    "current_price": coin.get("current_price"),
                    "market_cap": coin.get("market_cap"),
                    "total_volume": coin.get("total_volume"),
                    "high_24h": coin.get("high_24h"),
                    "low_24h": coin.get("low_24h"),
                    "price_change_24h": coin.get("price_change_24h"),
                    "price_change_percentage_24h": coin.get("price_change_percentage_24h"),
                    "market_cap_rank": coin.get("market_cap_rank"),
                    "circulating_supply": coin.get("circulating_supply"),
                    "max_supply": coin.get("max_supply"),
                    "ath": coin.get("ath"),
                    "atl": coin.get("atl"),
                    "last_updated": coin.get("last_updated"),
                    "fetched_at": fetched_at
                })
                inserted += 1
            except Exception as e:
                print(f"✗ Skipped {coin['id']}: {e}")
                skipped += 1

        print(f"✓ Silver: {inserted} inserted | {skipped} skipped")

def task_transform():
    engine = get_engine()
    df = read_silver(engine)
    df = clean(df)
    gold = aggregate(df)
    write_gold(gold, engine)

with DAG(
    dag_id = 'crypto_pipeline',
    default_args = default_args,
    description = 'Daily crypto data pipeline',
    schedule = '0 8 * * *', # runs every day at 8am
    start_date=datetime(2026, 4, 1),
    catchup = False,
    tags = ['crypto', 'pipeline']
) as dag:

    extract = PythonOperator(
        task_id = "extract_from_coingecko",
        python_callable = task_extract
    )

    load = PythonOperator(
        task_id = "load_to_mysql",
        python_callable = task_load
    )

    transform = PythonOperator(
        task_id = "transform_to_gold",
        python_callable = task_transform
    )

    # pipeline order
    extract >> load >> transform

