# -*- coding: utf-8 -*-
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

"""
train_model.py
==============
Trains a Random Forest Regressor to predict the actual hourly bike rental
count (cnt) in number of rentals.
Run with: python train_model.py
"""

import pandas as pd
import numpy as np
import joblib
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    mean_squared_error,
    mean_absolute_error,
    r2_score,
    accuracy_score,
    f1_score,
)

DATA_FILE  = "data/hour.csv"
MODEL_FILE = "model.joblib"

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

TARGET = "cnt"


def load_and_clean(path: str) -> pd.DataFrame:
    """Load the UCI Bike Sharing hour.csv and clean it."""
    print(f"Loading dataset from {path}...")
    df = pd.read_csv(path)

    # Drop identifier columns not used as features
    df.drop(columns=["instant", "dteday", "casual", "registered"], inplace=True, errors="ignore")

    # Drop rows where target is missing or zero
    df.dropna(subset=[TARGET], inplace=True)
    df = df[df[TARGET] > 0]

    # Fill any remaining NaN with column medians
    df.fillna(df.median(numeric_only=True), inplace=True)
    df.reset_index(drop=True, inplace=True)

    print(f"Cleaned dataset: {len(df)} rows, {df.shape[1]} columns")
    return df


def bin_rentals(y: np.ndarray) -> np.ndarray:
    """
    Map continuous rental counts to 3 demand bands for classification metrics.
      0 = Low    (<100 rentals/hr)
      1 = Medium (100–400 rentals/hr)
      2 = High   (>400 rentals/hr)
    """
    bins = np.zeros(len(y), dtype=int)
    bins[(y >= 100) & (y <= 400)] = 1
    bins[y > 400] = 2
    return bins


def train(df: pd.DataFrame):
    """Train and evaluate the Random Forest Regressor."""
    available = [c for c in FEATURE_COLS if c in df.columns]
    print(f"\nUsing {len(available)} features: {available}")

    X = df[available]
    y = df[TARGET].astype(float)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    print(f"\nTraining Random Forest Regressor, this might take a sec...")
    print(f"   Train size: {len(X_train)} | Test size: {len(X_test)}")

    model = RandomForestRegressor(
        n_estimators=100,
        max_depth=12,
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X_train, y_train)

    # ── Regression metrics ────────────────────────────────────────────────────
    y_pred = model.predict(X_test)
    rmse   = np.sqrt(mean_squared_error(y_test, y_pred))
    mae    = mean_absolute_error(y_test, y_pred)
    r2     = r2_score(y_test, y_pred)

    # ── Classification metrics (binned) ───────────────────────────────────────
    y_test_bins = bin_rentals(y_test.values)
    y_pred_bins = bin_rentals(y_pred)
    acc = accuracy_score(y_test_bins, y_pred_bins)
    f1  = f1_score(y_test_bins, y_pred_bins, average="weighted")

    print(f"\n{'='*50}")
    print(f"MODEL PERFORMANCE — Regression")
    print(f"{'='*50}")
    print(f"  RMSE      : {rmse:.4f} rentals/hr")
    print(f"  MAE       : {mae:.4f} rentals/hr")
    print(f"  R² Score  : {r2:.4f}")
    print(f"\nMODEL PERFORMANCE — Classification (binned demand)")
    print(f"{'='*50}")
    print(f"  Accuracy  : {acc*100:.2f}%")
    print(f"  F1 Score  : {f1*100:.2f}%  (weighted)")

    # ── Save ──────────────────────────────────────────────────────────────────
    payload = {
        "model":    model,
        "features": available,
        "rmse":     rmse,
        "mae":      mae,
        "r2":       r2,
        "accuracy": acc,
        "f1":       f1,
    }
    joblib.dump(payload, MODEL_FILE)
    print(f"\nSaved to {MODEL_FILE}")
    print(f"{'='*50}\n")
    return rmse, mae, r2, acc, f1


if __name__ == "__main__":
    df = load_and_clean(DATA_FILE)
    rmse, mae, r2, acc, f1 = train(df)
    print("All done. Training complete! You can now run the pipeline.")
    print(f"   RMSE: {rmse:.4f}  |  MAE: {mae:.4f}  |  R²: {r2:.4f}")
    print(f"   Accuracy: {acc*100:.2f}%  |  F1: {f1*100:.2f}%")
