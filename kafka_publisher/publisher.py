"""
This module should contain general use method for producing
data to Kafka.

Currently we anticipate these will be standalone library methods;
but we could use them as route handlers in a web application.
"""
import typing as t
from enum import Enum


# -----------------------------------------------------------------------------
#   Types
# -----------------------------------------------------------------------------


class KafkaPolicy(Enum):
    FIRE_AND_FORGET = 1
    PARTIAL_QUORUM = 2
    FULL_QUORUM = 3


class Result(Enum):
    OK = 1
    ERROR = 2


# -----------------------------------------------------------------------------
#   Core utils
# -----------------------------------------------------------------------------


def publish_datum_to_kafka(
    datum: t.Any, policy: KafkaPolicy, topic: str, host: t.Optional[str]
) -> Result:
    pass
