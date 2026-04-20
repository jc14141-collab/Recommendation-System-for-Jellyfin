import json
import sys
from pathlib import Path

import requests


def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/test_old_input.py <input_json> [base_url]")
        sys.exit(1)

    input_path = Path(sys.argv[1])
    base_url = sys.argv[2] if len(sys.argv) >= 3 else "http://127.0.0.1:8000"
    endpoint = f"{base_url.rstrip('/')}/recommend"

    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list) or len(data) == 0:
        raise ValueError("Input JSON must be a non-empty list.")

    item = data[0]

    payload = {
        "request_id": "single-test",
        "user_id": str(item["user_id"]),
        "timestamp": "2026-04-06T00:00:00Z",
        "request_k": 1,
        "user_embedding": item["user_embedding"],
        "candidates": [
            {
                "movie_id": str(item["movie_id"]),
                "movie_embedding": item["movie_embedding"],
            }
        ],
    }

    resp = requests.post(endpoint, json=payload, timeout=30)
    resp.raise_for_status()
    result = resp.json()

    top = result["recommendations"][0]

    output = {
        "user_id": item["user_id"],
        "movie_id": item["movie_id"],
        "predicted_score": top["score"],
    }

    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()