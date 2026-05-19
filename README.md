# Bike Sharing Real-Time Streaming Pipeline
ENGR 5785G — Assignment 1

A real-time streaming application built with Apache Kafka and Faust (Python Streams API) that streams hourly bike sharing data, runs live ML inference, and outputs predicted rental count in real time.

---

## Dataset

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

---

## ML Model

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

---

## Setup

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
```
This trains the Random Forest Regressor on the Bike Sharing dataset and saves `model.joblib`.

---

## How to Run Each Component

Open three separate terminals side-by-side and run in order:

**Terminal 1 — Faust Streams Processor (start first)**
```bash
faust -A streams_processor worker -l info
```
Or alternatively:
```bash
python streams_processor.py worker -l info
```

**Terminal 2 — Output Consumer**
```bash
python consumer.py
```

**Terminal 3 — Producer (start last)**
```bash
python producer.py
```

The producer will begin sending one row per second. Predictions will appear in the consumer terminal in real time.

---

## Video Demo

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
