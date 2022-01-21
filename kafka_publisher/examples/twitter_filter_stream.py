import typing as t

import argparse
import logging
import requests
import threading
import time
import traceback

from kafka import KafkaProducer

from kafka_publisher.twitter.utils import FILTER_STREAM_URL
from kafka_publisher.twitter.utils import create_twitter_payload
from kafka_publisher.twitter.utils import get_bearer_oauth_from_token
from kafka_publisher.twitter.utils import setup_rules
from kafka_publisher.utils.rate_limiting import TokenBucket
from kafka_publisher.utils.rate_limiting import RateLimiterKillswitch


# -----------------------------------------------------------------------------
#   Constants
# -----------------------------------------------------------------------------


logging.basicConfig(level=logging.INFO)

# See documentation for what is allowed:
# https://developer.twitter.com/en/docs/twitter-api/tweets/filtered-stream/integrate/build-a-rule#list
SAMPLE_RULES: t.List[t.Dict[str, str]] = [
    {
        "value": "feel lang:en sample:10",
        "tag": "Sampled tweets about feelings, in English",
    },
]

KILL_RATE_LIMITER_DAEMON = False
RATE_LIMITER_RECORDS_PER_MINUTE: int = 1_000
TOKEN_BUCKET_LOCK: threading.Condition = threading.Condition()
SLEEP_TIME_IN_SECONDS: int = 30


# -----------------------------------------------------------------------------
#   Parse CLI args
# -----------------------------------------------------------------------------


def get_cli_args() -> t.Any:
    parser = argparse.ArgumentParser(
        description="Arguments for the Twitter filter stream script"
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
    response = requests.get(FILTER_STREAM_URL, auth=auth, stream=True)
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
    bearer_oauth_callable = get_bearer_oauth_from_token(
        args.bearer_token, "filter_stream"
    )
    setup_rules(
        bearer_oauth_callable, SAMPLE_RULES
    )  # NOTE: Comment this line if you already setup rules and want to keep them

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
            f"Ran into some exception {str(e)} with traceback {traceback.format_exc()} "
            "during production; stopping rate limiting daemon"
        )
        try:
            rate_limiter_killswitch.should_kill = True
            thread.join()
        except Exception as te:
            logging.error(
                f"Ran into some exception {str(te)} while joining to rate "
                "limiting daemon thread; oh wells"
            )
            pass


if __name__ == "__main__":
    main()
