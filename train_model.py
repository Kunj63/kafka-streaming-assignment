"""
train_model.py
Run this ONCE before starting the pipeline to train and save the model.
Uses the Bike Sharing (UCI) dataset - predicts hourly rental count (cnt).
Dataset: https://archive.ics.uci.edu/dataset/275/bike+sharing+dataset
Download hour.csv from there and place it alongside this script.
"""

import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score
import joblib
import numpy as np
import os

CSV_PATH = "hour.csv"   # path to the UCI hour.csv

# ── Fallback: generate synthetic data if CSV not downloaded yet ──────────────
if not os.path.exists(CSV_PATH):
    print("⚠  hour.csv not found – generating synthetic data for demo purposes.")
    np.random.seed(42)
    n = 17379
    df = pd.DataFrame({
        "season":   np.random.randint(1, 5, n),
        "yr":       np.random.randint(0, 2, n),
        "mnth":     np.random.randint(1, 13, n),
        "hr":       np.random.randint(0, 24, n),
        "holiday":  np.random.randint(0, 2, n),
        "weekday":  np.random.randint(0, 7, n),
        "workingday": np.random.randint(0, 2, n),
        "weathersit": np.random.randint(1, 5, n),
        "temp":     np.random.uniform(0, 1, n).round(2),
        "atemp":    np.random.uniform(0, 1, n).round(2),
        "hum":      np.random.uniform(0, 1, n).round(2),
        "windspeed": np.random.uniform(0, 1, n).round(2),
        "cnt":      np.random.randint(1, 977, n),
    })
else:
    df = pd.read_csv(CSV_PATH)

FEATURES = ["season","yr","mnth","hr","holiday","weekday",
            "workingday","weathersit","temp","atemp","hum","windspeed"]
TARGET = "cnt"

X = df[FEATURES]
y = df[TARGET]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

model = RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1)
model.fit(X_train, y_train)

y_pred = model.predict(X_test)
rmse = np.sqrt(mean_squared_error(y_test, y_pred))
r2   = r2_score(y_test, y_pred)

print(f"✅ Model trained — RMSE: {rmse:.2f}  |  R²: {r2:.4f}")

joblib.dump(model, "model/bike_model.joblib")
joblib.dump(FEATURES, "model/features.joblib")

# Save a sample CSV for the producer to replay
df[FEATURES + [TARGET]].to_csv("model/data_sample.csv", index=False)
print("✅ Saved model/bike_model.joblib and model/data_sample.csv")
print(f"   Accuracy (R²): {r2:.4f}   RMSE: {rmse:.2f}")
