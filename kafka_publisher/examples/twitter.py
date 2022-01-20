import typing as t

import argparse
import logging
import requests
import threading
import time

from dataclasses import dataclass

from kafka import KafkaProducer

# -----------------------------------------------------------------------------
#   Constants
# -----------------------------------------------------------------------------


logging.basicConfig(level=logging.INFO)

TWEET_FIELDS: str = ",".join(
    [
        "author_id",
        "context_annotations",
        "conversation_id",
        "created_at",
        "entities",
        "geo",
        "lang",
        "possibly_sensitive",
        "public_metrics",
        "source",
        "withheld",
    ]
)
USER_FIELDS: str = ",".join(
    [
        "created_at",
        "description",
        "entities",
        "location",
        "pinned_tweet_id",
        "profile_image_url",
        "protected",
        "public_metrics",
        "url",
        "verified",
        "withheld",
    ]
)
STREAM_URL: str = (
    "https://api.twitter.com/2/tweets/search/stream"
    f"?tweet.fields={TWEET_FIELDS}&expansions=author_id"
    f"&user.fields={USER_FIELDS}"
)
RULES_URL: str = "https://api.twitter.com/2/tweets/search/stream/rules"

# See documentation for what is allowed:
# https://developer.twitter.com/en/docs/twitter-api/tweets/filtered-stream/integrate/build-a-rule#list
SAMPLE_RULES: t.List[t.Dict[str, str]] = [
    {
        "value": "feel lang:en sample:10",
        "tag": "Sampled tweets about feelings, in English",
    },
]

RATE_LIMITER_RECORDS_PER_MINUTE: int = 1_000
TOKEN_BUCKET_LOCK: threading.Condition = threading.Condition()
SLEEP_TIME_IN_SECONDS: int = 60


@dataclass
class TokenBucket:
    num_tokens: int


# -----------------------------------------------------------------------------
#   Utility methods; pretty much copied from https://bit.ly/3GBnHaU
# -----------------------------------------------------------------------------


def get_bearer_oauth_from_token(bearer_token: str) -> t.Callable:
    def bearer_oauth(r: t.Any) -> t.Any:
        r.headers["Authorization"] = f"Bearer {bearer_token}"
        r.headers["User-Agent"] = "v2FilteredStreamPython"
        return r

    return bearer_oauth


def get_all_rules(auth: t.Any) -> t.Any:
    response = requests.get(RULES_URL, auth=auth)
    if response.status_code is not 200:
        raise Exception(
            f"Cannot get rules (HTTP %d): %s" % (response.status_code, response.text)
        )
    return response.json()


def delete_all_rules(rules: t.Any, auth: t.Any) -> None:
    if rules is None or "data" not in rules:
        return None

    ids = list(map(lambda rule: rule["id"], rules["data"]))
    payload = {"delete": {"ids": ids}}
    response = requests.post(RULES_URL, auth=auth, json=payload)
    if response.status_code is not 200:
        raise Exception(
            f"Cannot delete rules (HTTP %d): %s" % (response.status_code, response.text)
        )


def set_rules(rules: t.Any, auth: t.Any) -> None:
    if rules is None:
        return

    payload = {"add": rules}
    response = requests.post(RULES_URL, auth=auth, json=payload)
    if response.status_code is not 201:
        raise Exception(
            f"Cannot create rules (HTTP %d): %s" % (response.status_code, response.text)
        )


def setup_rules(auth: t.Any) -> None:
    current_rules = get_all_rules(auth)
    delete_all_rules(current_rules, auth)
    set_rules(SAMPLE_RULES, auth)


# -----------------------------------------------------------------------------
#   Parse CLI args
# -----------------------------------------------------------------------------


def get_cli_args() -> t.Any:
    parser = argparse.ArgumentParser(description="Process some integers.")

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


def refill_token_bucket(token_bucket: TokenBucket) -> None:
    while True:
        with TOKEN_BUCKET_LOCK:
            logging.info(
                f"Refilling tokens in bucket; previously {token_bucket.num_tokens}"
            )
            token_bucket.num_tokens = RATE_LIMITER_RECORDS_PER_MINUTE
            TOKEN_BUCKET_LOCK.notify()
        time.sleep(SLEEP_TIME_IN_SECONDS)


def start_rate_limiter_daemon(token_bucket: TokenBucket) -> None:
    logging.info("Starting rate limiter daemon")
    t = threading.Thread(
        target=refill_token_bucket,
        args=(token_bucket,),
    )
    t.start()
    return t  # though we don't actually do anything with it atm


# -----------------------------------------------------------------------------
#   Producer daemon
# -----------------------------------------------------------------------------


def stream_connect(
    auth: t.Any, kafka_producer: t.Any, topic: str, token_bucket: TokenBucket
) -> None:
    response = requests.get(STREAM_URL, auth=auth, stream=True)
    for response_line in response.iter_lines():
        if response_line:
            with TOKEN_BUCKET_LOCK:
                if not token_bucket.num_tokens:
                    logging.info("Not enough tokens in the bucket, waiting...")
                    TOKEN_BUCKET_LOCK.wait()
                token_bucket.num_tokens -= 1
                kafka_producer.send(topic, response_line)


def start_producer(token_bucket: TokenBucket) -> None:
    args = get_cli_args()
    bearer_oauth_callable = get_bearer_oauth_from_token(args.bearer_token)
    setup_rules(
        bearer_oauth_callable
    )  # NOTE: Comment this line if you already setup rules and want to keep them

    kafka_producer = KafkaProducer(bootstrap_servers=args.bootstrap_server)

    # NOTE: Listen to the stream. This reconnection logic will attempt to
    # reconnect when a disconnection is detected. To avoid rate limits,
    # this logic implements exponential backoff, so the wait time will
    # increase if the client cannot reconnect to the stream.
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
    start_rate_limiter_daemon(token_bucket)
    start_producer(token_bucket)


if __name__ == "__main__":
    main()
