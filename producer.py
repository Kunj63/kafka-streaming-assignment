<<<<<<< HEAD
# -*- coding: utf-8 -*-
"""
producer.py - Kafka Producer
==============================
Reads the Bike Sharing dataset and sends each row to Kafka at ~1 row/sec.
Run with: python producer.py
"""

=======
>>>>>>> 261a17ce0c37605a85c3b091963ceb4046e788c8
import json
import time
import numpy as np
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError
from config import DATA_FILE, PRODUCER_DELAY, RAW_TOPIC, get_kafka_config

<<<<<<< HEAD
FEATURE_COLS = [
    "season",
    "yr",
    "mnth",
    "hr",
    "holiday",
    "weekday",
    "workingday",
    "weathersit",
    "temp",
    "atemp",
    "hum",
    "windspeed",
]

=======

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC             = "raw-data"
DELAY_SECONDS     = 1.0          # ~1 row/second for demo


SASL_USERNAME = os.getenv("KAFKA_SASL_USERNAME", "")
SASL_PASSWORD = os.getenv("KAFKA_SASL_PASSWORD", "")
>>>>>>> 261a17ce0c37605a85c3b091963ceb4046e788c8

def load_data(path: str) -> pd.DataFrame:
    """Load and clean the Bike Sharing dataset."""
    df = pd.read_csv(path)
    df.drop(columns=["instant", "dteday", "casual", "registered"], inplace=True, errors="ignore")
    df.dropna(subset=["cnt"], inplace=True)
    df = df[df["cnt"] > 0]
    df.fillna(df.median(numeric_only=True), inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


def row_to_event(row: pd.Series, row_num: int) -> dict:
    """Convert a DataFrame row to a Kafka event dict."""
    event = {"row_id": row_num}

    # Include target for consumer display
    for col in ["cnt"]:
        if col in row.index:
            val = row[col]
            event[col] = None if pd.isna(val) else int(val)

    # Include all feature columns
    for col in FEATURE_COLS:
        if col in row.index:
            val = row[col]
            event[col] = None if (pd.isna(val) or val != val) else float(val)

    return event


def main():
    print("=" * 55)
    print("  Bike Sharing Kafka Producer")
    print("=" * 55)

    df = load_data(DATA_FILE)
    print(f"Loaded {len(df)} rows from {DATA_FILE}")
    print(f"Broker : {get_kafka_config()['bootstrap_servers']}")
    print(f"Topic  : {RAW_TOPIC}")
    print(f"Delay  : {PRODUCER_DELAY}s per row")
    print("-" * 55)

    kafka_cfg = get_kafka_config()
    producer = KafkaProducer(
        bootstrap_servers=kafka_cfg["bootstrap_servers"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        **{k: v for k, v in kafka_cfg.items() if k != "bootstrap_servers"},
    )

    print("Connected to Kafka broker. Starting stream...\n")

    sent = 0
    try:
        for idx, row in df.iterrows():
            event  = row_to_event(row, idx)
            future = producer.send(RAW_TOPIC, value=event)
            try:
                future.get(timeout=10)
            except KafkaError as e:
                print(f"Failed to send row {idx}: {e}")
                continue

            sent    += 1
            cnt_val  = event.get("cnt", 0)
            temp_val = event.get("temp", 0.0)
            season   = int(event.get("season", 0))
            hr       = int(event.get("hr", 0))

            print(
                f"Sent row {idx:>5} | "
                f"Hr: {hr:>2}  Season: {season} | "
                f"Actual Count: {cnt_val:>4} | "
                f"Temp: {temp_val:.2f}"
            )
            time.sleep(PRODUCER_DELAY)

    except KeyboardInterrupt:
        print(f"\nProducer stopped by user. Sent {sent} messages.")
    finally:
        producer.flush()
        producer.close()
        print(f"Producer closed. Total sent: {sent}")


if __name__ == "__main__":
    main()
