"""
streams_processor.py
Faust Streams Processor — consumes raw-data topic, runs ML model,
publishes predictions to the predictions topic.

Usage:
    faust -A streams_processor worker -l info

Environment variables (same as producer.py):
    KAFKA_BOOTSTRAP_SERVERS  (default: localhost:9092)
    KAFKA_SASL_USERNAME / KAFKA_SASL_PASSWORD  (Confluent Cloud / MSK)
"""

import faust
import joblib
import os
import numpy as np

# ── Config ────────────────────────────────────────────────────────────────────
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SASL_USERNAME     = os.getenv("KAFKA_SASL_USERNAME", "")
SASL_PASSWORD     = os.getenv("KAFKA_SASL_PASSWORD", "")

broker_credentials = None
if SASL_USERNAME:
    from faust.types.auth import SASLCredentials
    broker_credentials = SASLCredentials(
        username=SASL_USERNAME,
        password=SASL_PASSWORD,
        ssl=True,
    )

app = faust.App(
    "bike-streaming-app",
    broker=f"kafka://{BOOTSTRAP_SERVERS}",
    broker_credentials=broker_credentials,
    value_serializer="json",
)

# ── Kafka Topics ──────────────────────────────────────────────────────────────
raw_topic         = app.topic("raw-data",     value_type=dict)
predictions_topic = app.topic("predictions",  value_type=dict)

# ── Load pre-trained model ────────────────────────────────────────────────────
MODEL    = joblib.load("model/bike_model.joblib")
FEATURES = joblib.load("model/features.joblib")
print(f"✅ Model loaded — features: {FEATURES}")

# ── Faust Agent (the Streams processor) ──────────────────────────────────────
@app.agent(raw_topic)
async def process_stream(stream):
    """
    Consumes every message from raw-data, extracts the feature vector,
    runs the RandomForest model, and sends a prediction message to the
    predictions topic.
    """
    async for record in stream:
        try:
            # Build feature vector in the correct order
            feature_vector = np.array([[record[f] for f in FEATURES]])
            predicted_cnt  = float(MODEL.predict(feature_vector)[0])

            output = {
                "row_id":         record.get("row_id", -1),
                "actual_cnt":     record.get("cnt", None),
                "predicted_cnt":  round(predicted_cnt, 1),
                "input_features": {f: record[f] for f in FEATURES},
            }

            await predictions_topic.send(value=output)

        except KeyError as e:
            print(f"⚠  Missing feature in record: {e}")
        except Exception as e:
            print(f"⚠  Prediction error: {e}")
