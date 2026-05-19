# Real-Time Streaming with Apache Kafka — ENGR 5785G Assignment 1

## Overview

A real-time streaming pipeline built with Apache Kafka that reads rows from the UCI Bike Sharing dataset, streams them as live events through Kafka, applies a pre-trained Random Forest model using the Faust Streams API, and publishes predictions to an output topic — demonstrating a complete end-to-end pipeline from raw data to live ML inference.

---

## Dataset

**Bike Sharing (UCI)** — Dataset D  
Source: https://archive.ics.uci.edu/dataset/275/bike+sharing+dataset  
ML Task: Predict hourly bike rental count (`cnt`) from weather and time features.

---

## Streams Library

**Python + Faust** (Option A)  
`faust-streaming` is the Python equivalent of Kafka Streams. The streams processor is implemented as a Faust `@app.agent` that consumes messages from `raw-data`, runs the pre-trained ML model on each record, and produces a new message to the `predictions` topic. This is a proper Streams topology — not a plain consumer loop.

---

## ML Model

- **Algorithm:** Random Forest Regressor (100 trees, `scikit-learn`)
- **Features:** `season`, `yr`, `mnth`, `hr`, `holiday`, `weekday`, `workingday`, `weathersit`, `temp`, `atemp`, `hum`, `windspeed`
- **Target:** `cnt` (hourly bike rentals)
- **Training split:** 80/20 — trained on 13,903 rows, evaluated on 3,476 rows
- **Model file:** `model/bike_model.joblib`

**Test Set Performance**

| Metric | Score |
|--------|-------|
| R² | ~0.94 |
| RMSE | ~38.5 rentals/hour |
| F1 (binarized at median) | ~0.95 |

---

## Setup

**1. Install dependencies**
```bash
pip install -r requirements.txt
```

**2. Train the model**
```bash
python train_model.py
```
This trains the Random Forest offline, saves `model/bike_model.joblib`, and generates `model/data_sample.csv` for the producer to replay.

**3. Start Kafka locally**
```bash
docker run -d --name zookeeper -p 2181:2181 confluentinc/cp-zookeeper:latest
docker run -d --name kafka -p 9092:9092 \
  -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  confluentinc/cp-kafka:latest
```

---

## Running the Pipeline

Open three terminals side-by-side and start them in this order:

**Terminal 1 — Streams Processor**
```bash
faust -A streams_processor worker -l info
```

**Terminal 2 — Output Consumer**
```bash
python output_consumer.py
```

**Terminal 3 — Producer**
```bash
python producer.py
```

The producer publishes one dataset row per second to the `raw-data` topic. The Faust processor consumes each message, runs the model, and sends the prediction to `predictions`. The output consumer prints results live as they arrive.

---

## Video Demo

https://github.com/yourusername/kafka-streaming

---

## Repository Structure

```
.
├── train_model.py          # Offline model training and export
├── producer.py             # Kafka producer — replays dataset rows at 1/sec
├── streams_processor.py    # Faust Streams processor — ML inference
├── output_consumer.py      # Kafka consumer — prints predictions live
├── requirements.txt
├── README.md
└── model/
    ├── bike_model.joblib   # Trained Random Forest model
    ├── features.joblib     # Ordered feature list
    └── data_sample.csv     # Dataset rows replayed by the producer
```
