import json
import os
import statistics
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests

BASE_URL = os.getenv("BASE_URL", "http://127.0.0.1:8000").rstrip("/")
ENDPOINT = f"{BASE_URL}/recommend"

TOTAL_REQUESTS = int(os.getenv("TOTAL_REQUESTS", "50"))
CONCURRENCY = int(os.getenv("CONCURRENCY", "1"))
TIMEOUT_SECONDS = float(os.getenv("TIMEOUT_SECONDS", "30"))
SLA_MS = float(os.getenv("SLA_MS", "0"))

REPO_ROOT = Path(__file__).resolve().parent.parent
SAMPLE_PATH = Path(
    os.getenv("SAMPLE_PATH", str(REPO_ROOT / "contracts" / "benchmarks_input_2000.json"))
)


def _load_payload() -> dict:
    with open(SAMPLE_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, dict):
        raise ValueError(f"{SAMPLE_PATH} must be a JSON object, got {type(data)}")

    if "user_embedding" not in data:
        raise ValueError(f"{SAMPLE_PATH} missing required field: user_embedding")

    if "candidates" not in data:
        raise ValueError(f"{SAMPLE_PATH} missing required field: candidates")

    candidate_count = len(data.get("candidates", []))
    print(f"[benchmark] Loaded payload from {SAMPLE_PATH}")
    print(f"[benchmark] Candidates: {candidate_count}")
    return data


BASE_PAYLOAD: dict = _load_payload()


def percentile(sorted_values: list[float], p: float) -> float:
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return sorted_values[0]

    k = (len(sorted_values) - 1) * p
    f = int(k)
    c = min(f + 1, len(sorted_values) - 1)
    return sorted_values[f] * (c - k) + sorted_values[c] * (k - f)


def build_payload(request_index: int) -> dict:
    payload = dict(BASE_PAYLOAD)
    payload["request_id"] = f"{BASE_PAYLOAD.get('request_id', 'bench')}-{request_index}"
    return payload


def send_one_request(request_index: int) -> dict:
    payload = build_payload(request_index)
    start = time.perf_counter()

    try:
        response = requests.post(
            ENDPOINT,
            json=payload,
            timeout=(TIMEOUT_SECONDS, TIMEOUT_SECONDS),
        )
        elapsed_ms = (time.perf_counter() - start) * 1000

        if response.status_code != 200:
            return {
                "ok": False,
                "status_code": response.status_code,
                "latency_ms": elapsed_ms,
                "error": response.text[:500],
                "error_type": "http_error",
            }

        if SLA_MS > 0 and elapsed_ms > SLA_MS:
            return {
                "ok": False,
                "status_code": response.status_code,
                "latency_ms": elapsed_ms,
                "error": f"SLA timeout: {elapsed_ms:.2f} ms > {SLA_MS:.2f} ms",
                "error_type": "sla_timeout",
            }

        return {
            "ok": True,
            "status_code": response.status_code,
            "latency_ms": elapsed_ms,
            "error": None,
            "error_type": None,
        }

    except requests.exceptions.Timeout:
        elapsed_ms = (time.perf_counter() - start) * 1000
        return {
            "ok": False,
            "status_code": None,
            "latency_ms": elapsed_ms,
            "error": f"Network timeout after {TIMEOUT_SECONDS}s",
            "error_type": "network_timeout",
        }
    except Exception as e:
        elapsed_ms = (time.perf_counter() - start) * 1000
        return {
            "ok": False,
            "status_code": None,
            "latency_ms": elapsed_ms,
            "error": str(e),
            "error_type": "exception",
        }


def warmup(n: int = 2) -> None:
    print(f"Warming up ({n} requests)...")
    for i in range(n):
        payload = build_payload(i)
        payload["request_id"] = f"warmup-{i}"
        try:
            requests.post(
                ENDPOINT,
                json=payload,
                timeout=(TIMEOUT_SECONDS, TIMEOUT_SECONDS),
            )
            print(f"  warmup {i+1}/{n} done")
        except Exception as e:
            print(f"  warmup {i+1}/{n} failed: {e}")


def summarize_failures(failed_results: list[dict]) -> None:
    if not failed_results:
        return

    error_type_counts = {}
    for item in failed_results:
        error_type = item.get("error_type", "unknown")
        error_type_counts[error_type] = error_type_counts.get(error_type, 0) + 1

    print("\n=== Failure breakdown ===")
    for error_type, count in sorted(error_type_counts.items(), key=lambda x: x[0]):
        print(f"{error_type:16s}: {count}")

    print("\n=== Sample failures ===")
    for idx, item in enumerate(failed_results[:5], start=1):
        print(
            f"[{idx}] type={item['error_type']} "
            f"status={item['status_code']} "
            f"latency_ms={item['latency_ms']:.2f} "
            f"error={item['error']}"
        )


def main() -> None:
    print("=== Benchmark configuration ===")
    print(f"Endpoint       : {ENDPOINT}")
    print(f"Total requests : {TOTAL_REQUESTS}")
    print(f"Concurrency    : {CONCURRENCY}")
    print(f"Network timeout: {TIMEOUT_SECONDS}s")
    print(f"SLA timeout    : {'disabled' if SLA_MS <= 0 else f'{SLA_MS} ms'}")
    print()

    warmup(2)

    overall_start = time.perf_counter()
    results: list[dict] = []

    with ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
        futures = [
            executor.submit(send_one_request, i)
            for i in range(TOTAL_REQUESTS)
        ]

        completed = 0
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            completed += 1

            if completed % 10 == 0 or completed == TOTAL_REQUESTS:
                ok_count = sum(1 for r in results if r["ok"])
                fail_count = len(results) - ok_count
                print(f"[progress] {completed}/{TOTAL_REQUESTS} done | ok={ok_count} fail={fail_count}")

    overall_elapsed = time.perf_counter() - overall_start

    success_results = [r for r in results if r["ok"]]
    failed_results = [r for r in results if not r["ok"]]
    success_latencies = sorted(r["latency_ms"] for r in success_results)

    total = len(results)
    success_count = len(success_results)
    fail_count = len(failed_results)
    error_rate = fail_count / total if total else 0.0
    success_rate = success_count / total if total else 0.0
    throughput = total / overall_elapsed if overall_elapsed > 0 else 0.0

    print("\n=== Benchmark results ===")
    print(f"Total requests   : {total}")
    print(f"Successful       : {success_count}")
    print(f"Failed           : {fail_count}")
    print(f"Success rate     : {success_rate:.2%}")
    print(f"Error rate       : {error_rate:.2%}")
    print(f"Throughput       : {throughput:.2f} req/s")
    print(f"Total wall time  : {overall_elapsed:.2f}s")

    if success_latencies:
        avg = statistics.mean(success_latencies)
        p50 = percentile(success_latencies, 0.50)
        p95 = percentile(success_latencies, 0.95)
        p99 = percentile(success_latencies, 0.99)

        print(f"Latency avg (ms) : {avg:.2f}")
        print(f"Latency p50 (ms) : {p50:.2f}")
        print(f"Latency p95 (ms) : {p95:.2f}")
        print(f"Latency p99 (ms) : {p99:.2f}")
    else:
        print("No successful requests — latency stats unavailable.")

    summarize_failures(failed_results)


if __name__ == "__main__":
    main()