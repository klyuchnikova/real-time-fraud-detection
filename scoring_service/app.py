"""Read transactions, calculate scores, and publish them to Kafka."""

import json
import logging
import os

from confluent_kafka import Consumer, KafkaException, Producer

from scoring_service.scorer import Scorer


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def publish(producer: Producer, topic: str, result: dict) -> None:
    errors = []

    def on_delivery(error, _message):
        if error is not None:
            errors.append(error)

    producer.produce(
        topic,
        key=result["transaction_id"].encode("utf-8"),
        value=json.dumps(result, allow_nan=False).encode("utf-8"),
        on_delivery=on_delivery,
    )
    if producer.flush(30) != 0 or errors:
        raise RuntimeError(f"Could not publish score: {errors}")


def main() -> None:
    scorer = Scorer(
        os.getenv("MODEL_PATH", "/app/artifacts/my_catboost.cbm"),
        os.getenv("PREPROCESSOR_PATH", "/app/artifacts/preprocessor.json"),
        float(os.getenv("FRAUD_THRESHOLD", "0.98")),
    )
    broker = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
    input_topic = os.getenv("KAFKA_TRANSACTIONS_TOPIC", "transactions")
    output_topic = os.getenv("KAFKA_SCORES_TOPIC", "scores")
    consumer = Consumer({
        "bootstrap.servers": broker,
        "group.id": "fraud-scorer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    producer = Producer({"bootstrap.servers": broker, "acks": "all"})
    consumer.subscribe([input_topic])
    logger.info("Scorer is listening to %s", input_topic)
    try:
        while True:
            message = consumer.poll(1)
            if message is None:
                continue
            if message.error():
                raise KafkaException(message.error())
            try:
                transaction = json.loads(message.value())
                result = scorer.score(transaction)
            except (ValueError, TypeError, KeyError, UnicodeError) as error:
                logger.warning("Skipping invalid transaction at offset %s: %s", message.offset(), error)
                consumer.commit(message=message, asynchronous=False)
                continue
            publish(producer, output_topic, result)
            consumer.commit(message=message, asynchronous=False)
            logger.info("Scored transaction %s", result["transaction_id"])
    finally:
        producer.flush(10)
        consumer.close()


if __name__ == "__main__":
    main()
