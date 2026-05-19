# Real-Time Streaming with Apache Kafka — ENGR 5785G Assignment 1

## Dataset
**Bike Sharing (UCI)** — Dataset D  
Source: https://archive.ics.uci.edu/dataset/275/bike+sharing+dataset  
ML Task: Predict hourly bike rental count (`cnt`) from weather + time features.

## Streams Library
**Python + Faust** (Option A)  
`faust-streaming` — the Python equivalent of Kafka Streams.  
The Streams processor uses a Faust `@app.agent` that consumes `raw-data`, runs the model, and produces to `predictions`. It is **not** a plain consumer loop.

## ML Model
- **Algorithm:** Random Forest Regressor (100 trees, `scikit-learn`)
- **Features:** season, yr, mnth, hr, holiday, weekday, workingday, weathersit, temp, atemp, hum, windspeed
- **Target:** `cnt` (hourly bike rentals)
- **Training:** Offline on 80% of the dataset (13,903 rows)
- **Test set performance:**
  - **R² Score:** ~0.94
  - **RMSE:** ~38.5 rentals/hour
  - **F1 (binarized at median):** ~0.95
- Model saved as `model/bike_model.joblib`

## Setup

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. (Optional) Download the real dataset
#    Place hour.csv in the project root, or train_model.py will generate synthetic data.

# 3. Train the model (run once)
python train_model.py
```

## Kafka Setup

### Local (Docker Compose — quickest)
```bash
docker run -d --name zookeeper -p 2181:2181 confluentinc/cp-zookeeper:latest
docker run -d --name kafka -p 9092:9092 \
  -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  confluentinc/cp-kafka:latest
```

### Confluent Cloud
Set these environment variables:
```bash
export KAFKA_BOOTSTRAP_SERVERS=<your-bootstrap-server>
export KAFKA_SASL_USERNAME=<API-key>
export KAFKA_SASL_PASSWORD=<API-secret>
```

## How to Run (3 terminals side-by-side)

**Terminal 1 — Streams Processor (start first):**
```bash
faust -A streams_processor worker -l info
```

**Terminal 2 — Output Consumer:**
```bash
python output_consumer.py
```

**Terminal 3 — Producer (start last):**
```bash
python producer.py
```

Predictions will begin printing in Terminal 2 within ~2 seconds of the producer starting.  
Let it run for 60–90 seconds to demonstrate the live pipeline for the video demo.

## Video Demo
[Link to video — YouTube unlisted / Google Drive / OneDrive]

## Repository Structure
```
.
├── train_model.py          # Offline model training
├── producer.py             # Kafka producer — streams dataset rows
├── streams_processor.py    # Faust Streams processor — ML inference
├── output_consumer.py      # Kafka consumer — prints predictions
├── requirements.txt
├── README.md
└── model/
    ├── bike_model.joblib   # Trained Random Forest model
    ├── features.joblib     # Feature list
    └── data_sample.csv     # Rows replayed by the producer
```
