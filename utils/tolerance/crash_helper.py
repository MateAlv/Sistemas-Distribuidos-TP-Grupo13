import os
import signal
import sys
from collections import defaultdict

_counters = defaultdict(int)


def _crash_now():
    try:
        os.kill(os.getpid(), signal.SIGKILL)
    except Exception:
        sys.exit(1)


def crash_after_two_chunks(key: str, env_var: str = "CRASH_AFTER_TWO_CHUNKS"):
    """
    If the env var is set (any value), crash after processing two chunks
    for the given key (worker type).
    """
    if not os.getenv(env_var):
        return
    _counters[(env_var, key)] += 1
    if _counters[(env_var, key)] >= 2:
        _crash_now()


def crash_after_end_processed(key: str, env_var: str = "CRASH_AFTER_END"):
    """
    If the env var is set (any value), crash right after END is processed
    for the given key (worker type).
    """
    if not os.getenv(env_var):
        return
    _crash_now()
