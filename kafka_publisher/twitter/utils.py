import typing as t

import json
import logging
import requests


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
RULES_URL: str = "https://api.twitter.com/2/tweets/search/stream/rules"
FILTER_STREAM_URL: str = (
    "https://api.twitter.com/2/tweets/search/stream"
    f"?tweet.fields={TWEET_FIELDS}&expansions=author_id"
    f"&user.fields={USER_FIELDS}"
)
SAMPLE_STREAM_URL: str = (
    "https://api.twitter.com/2/tweets/sample/stream"
    f"?tweet.fields={TWEET_FIELDS}&expansions=author_id"
    f"&user.fields={USER_FIELDS}"
)


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


def setup_rules(auth: t.Any, rules: t.List[t.Dict[str, str]]) -> None:
    current_rules = get_all_rules(auth)
    delete_all_rules(current_rules, auth)
    set_rules(rules, auth)


def create_twitter_payload(message: t.Any) -> t.Optional[str]:
    message = json.loads(message)
    data = message.get("data", {})
    payload = None
    if not data:
        payload = None
    users = message.get("includes", {}).get("users", [])
    if not users:
        payload = data
    elif len(users) == 1:
        payload = {**data, "user": users[0]}
    else:
        payload = {**data, "users": users}
    return json.dumps(payload) if payload else None
