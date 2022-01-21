import typing as t

import argparse
import logging
import requests
import threading
import time

from kafka import KafkaProducer

from kafka_producer.twitter.utils import SAMPLE_STREAM_URL
from kafka_producer.twitter.utils import create_twitter_payload
from kafka_producer.twitter.utils import get_bearer_oauth_from_token
from kafka_producer.utils.rate_limiting import TokenBucket
from kafka_producer.utils.rate_limiting import RateLimiterKillswitch


# -----------------------------------------------------------------------------
#   Constants
# -----------------------------------------------------------------------------


logging.basicConfig(level=logging.INFO)

RATE_LIMITER_RECORDS_PER_MINUTE: int = 1_000
TOKEN_BUCKET_LOCK: threading.Condition = threading.Condition()
SLEEP_TIME_IN_SECONDS: int = 60


# -----------------------------------------------------------------------------
#   Parse CLI args
# -----------------------------------------------------------------------------


def get_cli_args() -> t.Any:
    parser = argparse.ArgumentParser(
        description="Arguments for the Twitter sample stream script"
    )

    parser.add_argument(
        "--bearer-token",
        dest="bearer_token",
        default=None,
        help="The bearer token for the Twitter streaming API",
    )

    parser.add_argument(
        "--bootstrap-server",
        dest="bootstrap_server",
        default="localhost:9092",
        help="The bootstrap server",
    )

    parser.add_argument(
        "--topic",
        dest="topic",
        default=None,
        help="The Kafka topic to which we will publish messages",
    )

    return parser.parse_args()


# -----------------------------------------------------------------------------
#   Rate limiter daemon
# -----------------------------------------------------------------------------


def refill_token_bucket(
    token_bucket: TokenBucket, rate_limiter_killswitch: RateLimiterKillswitch
) -> None:
    while True:
        if rate_limiter_killswitch.should_kill:
            break
        with TOKEN_BUCKET_LOCK:
            logging.info(
                f"Refilling tokens in bucket; previously {token_bucket.num_tokens}"
            )
            token_bucket.num_tokens = RATE_LIMITER_RECORDS_PER_MINUTE
            TOKEN_BUCKET_LOCK.notify()
        time.sleep(SLEEP_TIME_IN_SECONDS)


def start_rate_limiter_daemon(
    token_bucket: TokenBucket, rate_limiter_killswitch: RateLimiterKillswitch
) -> None:
    logging.info("Starting rate limiter daemon")
    t = threading.Thread(
        target=refill_token_bucket,
        args=(
            token_bucket,
            rate_limiter_killswitch,
        ),
    )
    t.start()
    return t


# -----------------------------------------------------------------------------
#   Producer daemon
# -----------------------------------------------------------------------------


def stream_connect(
    auth: t.Any, kafka_producer: t.Any, topic: str, token_bucket: TokenBucket
) -> None:
    response = requests.get(SAMPLE_STREAM_URL, auth=auth, stream=True)
    for response_line in response.iter_lines():
        if response_line:
            with TOKEN_BUCKET_LOCK:
                if not token_bucket.num_tokens:
                    logging.info("Not enough tokens in the bucket, waiting...")
                    TOKEN_BUCKET_LOCK.wait()
                token_bucket.num_tokens -= 1
                payload = create_twitter_payload(response_line)
                if payload:
                    kafka_producer.send(topic, payload)


def start_producer(token_bucket: TokenBucket) -> None:
    args = get_cli_args()
    bearer_oauth_callable = get_bearer_oauth_from_token(args.bearer_token)
    kafka_producer = KafkaProducer(bootstrap_servers=args.bootstrap_server)
    timeout = 0
    while True:
        stream_connect(bearer_oauth_callable, kafka_producer, args.topic, token_bucket)
        time.sleep(2 ** timeout)
        timeout += 1


# -----------------------------------------------------------------------------
#   Entrypoint
# -----------------------------------------------------------------------------


def main() -> None:
    token_bucket = TokenBucket(RATE_LIMITER_RECORDS_PER_MINUTE)
    rate_limiter_killswitch = RateLimiterKillswitch(False)
    thread = start_rate_limiter_daemon(token_bucket, rate_limiter_killswitch)
    try:
        start_producer(token_bucket)
    except Exception as e:
        logging.error(
            f"Ran into some exception {e} during production; stopping "
            "rate limiting daemon"
        )
        try:
            rate_limiter_killswitch.should_kill = True
            thread.join()
        except Exception as te:
            logging.error(
                f"Ran into some exception {te} while joining to rate "
                "limiting daemon thread; oh wells"
            )
            pass


if __name__ == "__main__":
    main()
