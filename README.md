# Bike Sharing Real-Time Streaming Pipeline
ENGR 5785G — Assignment 1

A real-time streaming application built with Apache Kafka and Faust (Python Streams API) that streams hourly bike sharing data, runs live ML inference, and outputs predicted rental count in real time.

---

## Overview

A real-time streaming pipeline built with Apache Kafka that reads rows from the UCI Bike Sharing dataset, streams them as live events through Kafka, applies a pre-trained Random Forest model using the Faust Streams API, and publishes predictions to an output topic — demonstrating a complete end-to-end pipeline from raw data to live ML inference.

---

## Dataset
<<<<<<< HEAD

**Bike Sharing (UCI)** — Dataset D

Source: https://archive.ics.uci.edu/dataset/275/bike+sharing+dataset  
Records: 17,379 hourly records (after cleaning: 17,379 valid rows)  
Description: Hourly bike rental counts from Washington D.C.'s Capital Bikeshare system (2011–2012), combined with corresponding weather and seasonal information extracted from weather.com and freemeteo.com.  
Citation: Fanaee-T, H. (2013). Bike Sharing Dataset [Dataset]. UCI Machine Learning Repository. https://doi.org/10.24432/C5W894

---

## Streams Library Used

**Option A: Python + Faust**

Library: `faust-streaming` (Python equivalent of Kafka Streams)  
The `streams_processor.py` uses a `@app.agent()` decorator to define a Faust agent that consumes raw bike sharing events, applies the ML model, and produces prediction messages — no plain consumer loop.
=======

**Bike Sharing (UCI)** — Dataset D  
Source: https://archive.ics.uci.edu/dataset/275/bike+sharing+dataset  
ML Task: Predict hourly bike rental count (`cnt`) from weather and time features.

---

## Streams Library

**Python + Faust** (Option A)  
`faust-streaming` is the Python equivalent of Kafka Streams. The streams processor is implemented as a Faust `@app.agent` that consumes messages from `raw-data`, runs the pre-trained ML model on each record, and produces a new message to the `predictions` topic. This is a proper Streams topology — not a plain consumer loop.
>>>>>>> 261a17ce0c37605a85c3b091963ceb4046e788c8

---

## ML Model

<<<<<<< HEAD
The machine learning task for predicting the continuous hourly bike rental count (`cnt`) is a **Regression task**. A Random Forest Regressor was trained offline, and its continuous predictions are evaluated using standard regression metrics.

To strictly satisfy the grading rubric's specific requirement to report Accuracy + F1 score, we also map the continuous predictions back into discrete rental demand bands (Low: <100, Medium: 100–400, High: >400 rentals/hour) and calculate the binned classification metrics. Both evaluations are reported below.

### 1. Regression Model Performance (Primary Task)
Used by the Streams Processor to predict the exact continuous rental count.

| Metric | Value | Interpretation |
|--------|-------|----------------|
| RMSE (Root Mean Squared Error) | ~34.2 rentals/hr | Average deviation from ground-truth count |
| MAE (Mean Absolute Error) | ~26.4 rentals/hr | Average magnitude of absolute errors |
| R² Score (Coefficient of Determination) | 0.941 | 94.1% of rental variance is explained by the model |

### 2. Classification Metrics (Rubric Requirement)
Obtained by grouping the continuous rental predictions into demand level bands.

| Metric | Value | Note |
|--------|-------|------|
| Accuracy | 93.2% | Percentage of binned predictions matching actual binned class |
| F1 Score (weighted) | 93.1% | Balanced precision and recall across all demand classes |

The regressor is trained offline on 80% of the dataset (13,903 rows) and tested on the remaining 20% (3,476 rows). The continuous prediction is included in every live output message.

---

## Project Structure

```
Project - 1/
├── data/
│   └── hour.csv                  # UCI Bike Sharing dataset (downloaded)
├── config.py                     # Kafka broker config (local or Confluent Cloud)
├── train_model.py                # Step 0: Offline ML training script
├── producer.py                   # Step 1: Kafka producer (streams dataset at ~1 row/sec)
├── streams_processor.py          # Step 2: Faust streams processor (ML inference)
├── consumer.py                   # Step 3: Output consumer (prints predictions)
├── faust_patch.py                # aiokafka compatibility patch for faust-streaming
├── model.joblib                  # Saved trained model
├── requirements.txt              # Python dependencies
└── README.md
```
=======
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
>>>>>>> 261a17ce0c37605a85c3b091963ceb4046e788c8

---

## Setup

<<<<<<< HEAD
### Prerequisites
- Python 3.9+
- Apache Kafka running locally **OR** a Confluent Cloud account

### Step 0 — Download the Dataset
Download the Bike Sharing dataset from https://archive.ics.uci.edu/dataset/275/bike+sharing+dataset and place the file `hour.csv` inside a `data/` folder in the project root. The expected path is `data/hour.csv`.

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure Kafka Broker

**Option A — Local Kafka (default, no config needed):** The app defaults to `localhost:9092`. Make sure Kafka is running locally.

Start Zookeeper:
```bash
bin/zookeeper-server-start.sh config/zookeeper.properties
```

Start Kafka broker:
```bash
bin/kafka-server-start.sh config/server.properties
```

**Option B — Confluent Cloud:** Set the following environment variables before running:
```bash
set KAFKA_BROKER=<your-bootstrap-server>
set KAFKA_API_KEY=<your-api-key>
set KAFKA_API_SECRET=<your-api-secret>
```

### 3. Create Kafka Topics (Local Kafka only)
```bash
kafka-topics.sh --create --topic bike-sharing-raw --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
kafka-topics.sh --create --topic bike-sharing-predictions --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```
*(Confluent Cloud: create topics via the web UI)*

### 4. Train the ML Model (Run Once)
```bash
python train_model.py
=======
**1. Install dependencies**
```bash
pip install -r requirements.txt
>>>>>>> 261a17ce0c37605a85c3b091963ceb4046e788c8
```
This trains the Random Forest Regressor on the Bike Sharing dataset and saves `model.joblib`.

<<<<<<< HEAD
---

## How to Run Each Component

Open three separate terminals side-by-side and run in order:

**Terminal 1 — Faust Streams Processor (start first)**
```bash
faust -A streams_processor worker -l info
```
Or alternatively:
=======
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
>>>>>>> 261a17ce0c37605a85c3b091963ceb4046e788c8
```bash
python streams_processor.py worker -l info
```

<<<<<<< HEAD
**Terminal 2 — Output Consumer**
```bash
python consumer.py
```

**Terminal 3 — Producer (start last)**
=======
**Terminal 3 — Producer**
>>>>>>> 261a17ce0c37605a85c3b091963ceb4046e788c8
```bash
python producer.py
```

<<<<<<< HEAD
The producer will begin sending one row per second. Predictions will appear in the consumer terminal in real time.
=======
The producer publishes one dataset row per second to the `raw-data` topic. The Faust processor consumes each message, runs the model, and sends the prediction to `predictions`. The output consumer prints results live as they arrive.
>>>>>>> 261a17ce0c37605a85c3b091963ceb4046e788c8

---

## Video Demo
<<<<<<< HEAD

[[Google Drive Video Demo Link](https://drive.google.com/drive/folders/1WOJwOFhvTbTVPtogT1sgHTM1sRLKbo-s?usp=sharing)]

---

## Topics Used

| Topic | Purpose |
|-------|---------|
| `bike-sharing-raw` | Raw hourly bike sharing events (JSON, 1 msg/sec) |
| `bike-sharing-predictions` | ML predictions output (JSON) |

---

## Sample Output

**Producer (Terminal 3):**
```
Sent row    0 | Hr:  0  Season: 1 | Actual Count:   16 | Temp: 0.24
Sent row    1 | Hr:  1  Season: 1 | Actual Count:   40 | Temp: 0.22
=======

https://drive.google.com/drive/folders/1WOJwOFhvTbTVPtogT1sgHTM1sRLKbo-s?usp=sharing

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
>>>>>>> 261a17ce0c37605a85c3b091963ceb4046e788c8
```

**Consumer (Terminal 2):**
```
------------------------------------------------------------
[   1] Row 0  Hour: 00:00
  Count Measured  : 16 rentals/hr
  Count Predicted : 18.3 rentals/hr
  Error           : 2.3 rentals/hr
  Conditions      : Temp 0.24  |  Humidity 81.0%  |  Spring / Clear
```

---

## Dependencies

See `requirements.txt` for the full list of dependencies.
