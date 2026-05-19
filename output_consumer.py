import json
import os
from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC             = "predictions"
SASL_USERNAME     = os.getenv("KAFKA_SASL_USERNAME", "")
SASL_PASSWORD     = os.getenv("KAFKA_SASL_PASSWORD", "")

def make_consumer():
    kwargs = dict(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="output-consumer-group",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    if SASL_USERNAME:
        kwargs.update(
            security_protocol="SASL_SSL",
            sasl_mechanism="PLAIN",
            sasl_plain_username=SASL_USERNAME,
            sasl_plain_password=SASL_PASSWORD,
        )
    return KafkaConsumer(TOPIC, **kwargs)

def main():
    consumer = make_consumer()
    print(f"📥 Output Consumer started — listening on '{TOPIC}' ...\n")
    print(f"{'Row':>6}  {'Actual':>8}  {'Predicted':>10}  {'Error':>8}")
    print("-" * 40)

    for msg in consumer:
        rec   = msg.value
        row   = rec.get("row_id", "?")
        actual = rec.get("actual_cnt", "N/A")
        pred  = rec.get("predicted_cnt", "N/A")

        if actual != "N/A" and pred != "N/A":
            err = abs(float(actual) - float(pred))
            print(f"{row:>6}  {actual:>8}  {pred:>10.1f}  {err:>8.1f}")
        else:
            print(f"{row:>6}  {'N/A':>8}  {str(pred):>10}  {'N/A':>8}")

if __name__ == "__main__":
    main()
