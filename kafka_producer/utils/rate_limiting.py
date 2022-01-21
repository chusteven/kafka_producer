from dataclasses import dataclass


@dataclass
class TokenBucket:
    num_tokens: int
