import typing as t

import argparse
import requests
import time

from kafka import KafkaProducer

# -----------------------------------------------------------------------------
#   Constants
# -----------------------------------------------------------------------------


STREAM_URL: str = "https://api.twitter.com/2/tweets/search/stream"
RULES_URL: str = "https://api.twitter.com/2/tweets/search/stream/rules"

# See documentation for what is allowed:
# https://developer.twitter.com/en/docs/twitter-api/tweets/filtered-stream/integrate/build-a-rule#list
SAMPLE_RULES: t.List[t.Dict[str, str]] = [
    {
        "value": "feel lang:en sample:10",
        "tag": "Sampled tweets about feelings, in English",
    },
]


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


def stream_connect(auth: t.Any, kafka_producer: t.Any, topic: str) -> None:
    response = requests.get(STREAM_URL, auth=auth, stream=True)
    for response_line in response.iter_lines():
        if response_line:
            kafka_producer.send(topic, response_line)


def setup_rules(auth: t.Any) -> None:
    current_rules = get_all_rules(auth)
    delete_all_rules(current_rules, auth)
    set_rules(SAMPLE_RULES, auth)


# -----------------------------------------------------------------------------
#   Execute script
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


def main() -> None:
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
        stream_connect(bearer_oauth_callable, kafka_producer, args.topic)
        time.sleep(2 ** timeout)
        timeout += 1


if __name__ == "__main__":
    main()
