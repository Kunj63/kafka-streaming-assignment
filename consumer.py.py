# -*- coding: utf-8 -*-
"""
consumer.py - Output Consumer
==============================
Reads prediction results from the 'bike-sharing-predictions' topic
and prints each result to the console in a readable, colour-coded format.

Usage:
    python consumer.py
"""

import json
import sys
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from config import PREDICTIONS_TOPIC, get_kafka_config

# ANSI colour codes for terminal output
RESET  = "\033[0m"
BOLD   = "\033[1m"
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
CYAN   = "\033[96m"
GREY   = "\033[90m"

SEASON_LABELS = {1: "Spring", 2: "Summer", 3: "Fall", 4: "Winter"}
WEATHER_LABELS = {
    1: "Clear",
    2: "Cloudy",
    3: "Light Rain/Snow",
    4: "Heavy Rain/Snow",
}


def error_colour(error: float) -> str:
    """Colour the error value based on magnitude."""
    if error is None:
        return GREY
    if error < 30:
        return GREEN
    if error < 80:
        return YELLOW
    return RED


def format_prediction(msg: dict, count: int) -> str:
    """Format a prediction message for human-readable console output."""
    row_id    = msg.get("row_id", "?")
    cnt_meas  = msg.get("cnt_measured")
    cnt_pred  = msg.get("cnt_predicted")
    error     = msg.get("error")
    hour      = msg.get("hour")
    season    = msg.get("season")
    temp      = msg.get("temp")
    humidity  = msg.get("humidity_pct")
    weather   = msg.get("weathersit")

    cnt_meas_str = f"{cnt_meas} rentals/hr"   if cnt_meas  is not None else "N/A"
    cnt_pred_str = f"{cnt_pred:.1f} rentals/hr" if cnt_pred is not None else "N/A"
    error_str    = f"{error:.1f} rentals/hr"  if error     is not None else "N/A"
    hour_str     = f"{int(hour):02d}:00"       if hour      is not None else "N/A"
    temp_str     = f"{temp:.2f}"               if temp      is not None else "N/A"
    hum_str      = f"{humidity*100:.1f}%"      if humidity  is not None else "N/A"
    season_str   = SEASON_LABELS.get(int(season), "N/A") if season is not None else "N/A"
    weather_str  = WEATHER_LABELS.get(int(weather), "N/A") if weather is not None else "N/A"

    ec = error_colour(error)

    lines = [
        f"{CYAN}{'-'*60}{RESET}",
        f"{BOLD}[{count:>4}] Row {row_id}  {GREY}Hour: {hour_str}{RESET}",
        f"  Count Measured  : {cnt_meas_str}",
        f"  Count Predicted : {BOLD}{cnt_pred_str}{RESET}",
        f"  Error           : {ec}{error_str}{RESET}",
        f"  Conditions      : Temp {temp_str}  |  Humidity {hum_str}  |  {season_str} / {weather_str}",
    ]
    return "\n".join(lines)


def main():
    print(f"{BOLD}{CYAN}{'='*60}")
    print("  Bike Sharing Live Rental Count Predictions Consumer")
    print(f"{'='*60}{RESET}")
    print(f"  Listening on topic: {BOLD}{PREDICTIONS_TOPIC}{RESET}")
    print(f"  Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  {GREY}(Press Ctrl+C to stop){RESET}\n")

    kafka_cfg = get_kafka_config()
    consumer = KafkaConsumer(
        PREDICTIONS_TOPIC,
        bootstrap_servers=kafka_cfg["bootstrap_servers"],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="bike-sharing-consumer-group",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        **{k: v for k, v in kafka_cfg.items() if k != "bootstrap_servers"},
    )

    count       = 0
    total_error = 0.0

    try:
        for message in consumer:
            data = message.value
            if isinstance(data, (bytes, bytearray)):
                data = json.loads(data)

            count += 1
            err = data.get("error")
            if err is not None:
                total_error += err

            print(format_prediction(data, count))

            # Print running MAE every 10 messages
            if count % 10 == 0:
                running_mae = total_error / count
                print(
                    f"\n{BOLD}  Running MAE: {running_mae:.2f} rentals/hr "
                    f"(over {count} predictions){RESET}\n"
                )

    except KeyboardInterrupt:
        final_mae = (total_error / count) if count > 0 else 0.0
        print(f"\n{CYAN}{'='*60}{RESET}")
        print(f"{BOLD}  Consumer stopped.{RESET}")
        print(f"  Total predictions received : {count}")
        print(f"  Final MAE                  : {final_mae:.2f} rentals/hr")
        print(f"{CYAN}{'='*60}{RESET}")
        sys.exit(0)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
