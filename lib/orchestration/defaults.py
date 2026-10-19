"""Task settings shared by every DAG of the platform, passed as the DAG's default_args."""

from __future__ import annotations

from datetime import timedelta

DEFAULT_ARGS = {
    # Transient failures (a database restart, a network blip) get two more attempts.
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    # A multiplier, not an on/off switch (True would mean 1, i.e. no backoff): each
    # retry waits about twice as long as the previous one, with some random spread.
    "retry_exponential_backoff": 2.0,
    "max_retry_delay": timedelta(minutes=10),
    # A hung task fails instead of blocking the DAG, whose runs do not overlap.
    "execution_timeout": timedelta(minutes=30),
}
