"""
producer.py
Reads rows from data_sample.csv and publishes each one as a JSON message
to the raw-data Kafka topic at ~1 row/second.

Usage:
    python producer.py

Set KAFKA_BOOTSTRAP_SERVERS and (if using Confluent Cloud) auth env vars below.
"""

import json
import time
import os
import pandas as pd
from kafka import KafkaProducer

# ── Config ────────────────────────────────────────────────────────────────────
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC             = "raw-data"
DELAY_SECONDS     = 1.0          # ~1 row/second for demo

# Confluent Cloud / MSK / Aiven — set these env vars if needed:
SASL_USERNAME = os.getenv("KAFKA_SASL_USERNAME", "")
SASL_PASSWORD = os.getenv("KAFKA_SASL_PASSWORD", "")

def make_producer():
    kwargs = dict(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8"),
        acks="all",
        retries=3,
    )
    if SASL_USERNAME:
        kwargs.update(
            security_protocol="SASL_SSL",
            sasl_mechanism="PLAIN",
            sasl_plain_username=SASL_USERNAME,
            sasl_plain_password=SASL_PASSWORD,
        )
    return KafkaProducer(**kwargs)

def main():
    df = pd.read_csv("model/data_sample.csv")
    producer = make_producer()
    print(f"🚀 Producer started — sending to topic '{TOPIC}' on {BOOTSTRAP_SERVERS}")
    print(f"   Dataset rows: {len(df)}  |  Rate: 1 row/second\n")

    for idx, row in df.iterrows():
        msg = row.to_dict()
        msg["row_id"] = int(idx)
        producer.send(TOPIC, key=str(idx), value=msg)
        print(f"[{idx:>5}] Sent → {msg}")
        time.sleep(DELAY_SECONDS)

    producer.flush()
    producer.close()
    print("\n✅ Producer finished.")

if __name__ == "__main__":
    main()
