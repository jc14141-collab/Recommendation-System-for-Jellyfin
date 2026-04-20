from __future__ import annotations

from typing import Any

import requests


def _normalize_candidate_items(raw_items: Any) -> list[dict[str, Any]]:
    if not isinstance(raw_items, list):
        return []

    normalized: list[dict[str, Any]] = []
    for item in raw_items:
        if isinstance(item, dict):
            normalized_item = dict(item)
            if "movie_id" not in normalized_item:
                if "item_id" in normalized_item:
                    normalized_item["movie_id"] = normalized_item["item_id"]
                elif "item" in normalized_item:
                    normalized_item["movie_id"] = normalized_item["item"]
            normalized.append(normalized_item)
            continue

        try:
            normalized.append({"movie_id": int(item)})
        except (TypeError, ValueError):
            continue

    return normalized


def _normalize_inference_payload(payload: dict[str, Any]) -> dict[str, Any]:
    raw_items = payload.get("recommendations")
    if raw_items is None:
        raw_items = payload.get("recommend")

    normalized_payload = dict(payload)
    normalized_payload.pop("recommend", None)
    normalized_payload.pop("recommendations", None)
    normalized_payload["items"] = _normalize_candidate_items(raw_items)
    return normalized_payload


def fetch_incremental_candidates(
    user_id: int,
    online_service_url: str,
    top_k: int = 20,
    timeout_seconds: float = 10.0,
    inference: bool = False,
    inference_service_url: str = "http://training-manager:8096",
    model_version: str = "latest",
) -> dict[str, Any] | None:
    """Request candidate items once for a user and return the parsed payload.

    The caller decides when to invoke this helper and whether to cache the
    result. This module does not manage loops, sleeps, or retry policies.
    """
    try:
        user_id_int = int(user_id)
        if inference:
            base_url = (inference_service_url or "http://training-manager:8096").rstrip("/")
            response = requests.get(
                f"{base_url}/api/recommend/{user_id_int}",
                params={"top_n": int(top_k), "model_version": str(model_version)},
                timeout=float(timeout_seconds),
            )
        else:
            response = requests.get(
                online_service_url,
                params={"user_id": user_id_int, "top_k": int(top_k)},
                timeout=float(timeout_seconds),
            )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError(f"unexpected candidate payload type: {type(payload)}")
        if inference:
            return _normalize_inference_payload(payload)
        return payload
    except Exception as exc:
        print(f"[api request failed] user_id={user_id} error={exc}")
        return None
