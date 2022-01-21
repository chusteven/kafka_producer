from dataclasses import dataclass


@dataclass
class TokenBucket:
    num_tokens: int


@dataclass
class RateLimiterKillswitch:
    should_kill: bool
