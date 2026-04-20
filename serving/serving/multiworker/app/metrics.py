from __future__ import annotations

import os

from prometheus_client import (
    Counter,
    Histogram,
    CollectorRegistry,
    generate_latest,
    CONTENT_TYPE_LATEST,
)

try:
    from prometheus_client import multiprocess as _mp_module
except ImportError:
    _mp_module = None

_MULTIPROC_DIR = os.getenv("PROMETHEUS_MULTIPROC_DIR", "")

REQUESTS_TOTAL = Counter(
    "recommend_requests_total",
    "Total recommend endpoint requests",
    ["mode", "status"],
)

FALLBACK_TOTAL = Counter(
    "fallback_used_total",
    "Fallback invocations by reason",
    ["reason"],
)

RECOMMENDATION_SCORE = Histogram(
    "recommend_score_distribution",
    "Distribution of recommendation scores",
    buckets=[0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0],
)

REQUEST_LATENCY = Histogram(
    "recommend_request_latency_seconds",
    "End-to-end latency per request",
    ["mode"],
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
)

USER_CLICKS = Counter(
    "user_click_total",
    "User clicks on recommendations",
    ["rank"],
)

CTR = Histogram(
    "recommendation_ctr",
    "Click-through rate by rank position",
    buckets=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
)

CANDIDATE_COUNT = Histogram(
    "recommend_candidate_count",
    "Number of candidates per request",
    buckets=[1, 10, 50, 100, 200, 500, 1000],
)

RETURN_COUNT = Histogram(
    "recommend_return_count",
    "Number of recommendations returned per request",
    buckets=[1, 5, 10, 20, 50, 100, 500],
)


def collect_metrics() -> tuple[bytes, str]:
    if _MULTIPROC_DIR and _mp_module is not None:
        registry = CollectorRegistry()
        _mp_module.MultiProcessCollector(registry)
        return generate_latest(registry), CONTENT_TYPE_LATEST
    return generate_latest(), CONTENT_TYPE_LATEST
