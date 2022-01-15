import json
import time
from datetime import datetime

from kafka import KafkaProducer

from utils.serializers import KafkaDefaultEncoder


HOST: str = "localhost"
PORT: int = 9092
TOPIC: str = "some_dummy_topic"


def main() -> None:
    kafka_producer = KafkaProducer(bootstrap_servers=f"{HOST}:{PORT}")
    while True:
        kafka_producer.send(
            TOPIC,
            json.dumps(
                {
                    "time": datetime.now(),
                    "message": "some random string",
                },
                cls=KafkaDefaultEncoder,
            ).encode("utf-8"),
        )
        time.sleep(1)


if __name__ == "__main__":
    main()
