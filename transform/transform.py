import os
import pandas as pd
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

def read_silver(engine):
    df = pd.read_sql("SELECT * FROM crypto_prices", engine)
    print(f"Read {len(df)} rows from silver table")
    return df

def clean(df):
    df = df.dropna(subset=["current_price"])
    df["fetched_at"] = pd.to_datetime(df["fetched_at"])
    df["summary_date"] = df["fetched_at"].dt.date
    df["max_supply"] = df["max_supply"].fillna(df["circulating_supply"])

    print(f"Cleaned: {len(df)} rows remaining")
    return df

def aggregate(df):
    gold = df.groupby(["coin_id", "name", "summary_date"]).agg(
        avg_price = ("current_price", "mean"),
        min_price = ("current_price", "min"),
        max_price = ("current_price", "max"),
        avg_volume = ("total_volume", "mean"),
        avg_market_cap = ("market_cap", "mean"),
        avg_price_change = ("price_change_percentage_24h", "mean"),
    ).reset_index()

    gold["price_range"] = gold["max_price"] - gold["min_price"]

    gold = gold.round({
        "avg_price":        8,
        "min_price":        8,
        "max_price":        8,
        "price_range":      8,
        "avg_volume":       2,
        "avg_market_cap":   2,
        "avg_price_change": 5
    })

    print(f"Aggregated into {len(gold)} summary rows")
    return gold

def write_gold(gold, engine):
    with engine.connect() as conn:
        for _, row in gold.iterrows():
            conn.execute(text("""
                INSERT INTO crypto_daily_summary (
                    coin_id, name, summary_date,
                    avg_price, min_price, max_price, price_range,
                    avg_volume, avg_market_cap, avg_price_change
                ) VALUES (
                    :coin_id, :name, :summary_date,
                    :avg_price, :min_price, :max_price, :price_range,
                    :avg_volume, :avg_market_cap, :avg_price_change
                )
                ON DUPLICATE KEY UPDATE
                    avg_price        = VALUES(avg_price),
                    min_price        = VALUES(min_price),
                    max_price        = VALUES(max_price),
                    price_range      = VALUES(price_range),
                    avg_volume       = VALUES(avg_volume),
                    avg_market_cap   = VALUES(avg_market_cap),
                    avg_price_change = VALUES(avg_price_change)
            """), {
                "coin_id":        row["coin_id"],
                "name":           row["name"],
                "summary_date":   str(row["summary_date"]),
                "avg_price":      row["avg_price"],
                "min_price":      row["min_price"],
                "max_price":      row["max_price"],
                "price_range":    row["price_range"],
                "avg_volume":     row["avg_volume"],
                "avg_market_cap": row["avg_market_cap"],
                "avg_price_change": row["avg_price_change"]
            })
        conn.commit()
    print(f"✓ Gold: {len(gold)} rows written to crypto_daily_summary")

if __name__ == "__main__":
    engine = get_engine()
    df = read_silver(engine)
    df = clean(df)
    gold = aggregate(df)
    write_gold(gold, engine)
