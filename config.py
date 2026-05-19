# -*- coding: utf-8 -*-
"""
config.py - Kafka & App Configuration
Confluent Cloud setup for ENGR 5785G Assignment 1.
"""

import os
import ssl
import faust

# --- Confluent Cloud Credentials ---
_BROKER     = "pkc-619z3.us-east1.gcp.confluent.cloud:9092"
_API_KEY    = "E4QILOEEYICJJRRD"
_API_SECRET = "cfltO47x6FfMqY4gTDoedypgFeFmQC9MK4ipYoMxHXxm4+dRJSVYo+n4QRxoUJeg"

# Allow overrides via environment variables
KAFKA_BROKER      = os.getenv("KAFKA_BROKER",      _BROKER)
KAFKA_API_KEY     = os.getenv("KAFKA_API_KEY",     _API_KEY)
KAFKA_API_SECRET  = os.getenv("KAFKA_API_SECRET",  _API_SECRET)

# --- Topic Names ---
RAW_TOPIC         = "bike-sharing-raw"
PREDICTIONS_TOPIC = "bike-sharing-predictions"

# --- Producer Settings ---
PRODUCER_DELAY = 1.0                    # seconds between each row sent
DATA_FILE      = "data/hour.csv"
MODEL_FILE     = "model.joblib"

# --- Build kafka-python producer/consumer config ---
def get_kafka_config() -> dict:
    """Returns kafka-python compatible config dict."""
    return {
        "bootstrap_servers":  KAFKA_BROKER,
        "security_protocol":  "SASL_SSL",
        "sasl_mechanism":     "PLAIN",
        "sasl_plain_username": KAFKA_API_KEY,
        "sasl_plain_password": KAFKA_API_SECRET,
    }

# --- Faust Broker URL & Credentials ---
FAUST_BROKER = f"kafka://{KAFKA_BROKER}"

def get_faust_credentials() -> faust.SASLCredentials:
    """Returns Faust SASLCredentials for Confluent Cloud SASL_SSL."""
    return faust.SASLCredentials(
        username=KAFKA_API_KEY,
        password=KAFKA_API_SECRET,
        ssl_context=ssl.create_default_context(),
        mechanism="PLAIN",
    )
