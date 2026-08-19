"""OpenLetterConnect (OLC) API client.

Docs: https://api-docs.openletterconnect.com/

Environments:
    demo: https://demoapi.openletterconnect.com/api/v1   (default)
    prod: https://api.openletterconnect.com/api/v1

Auth is an API key created in the OLC dashboard (profile icon -> Settings ->
API Keys -> Create), sent as `Authorization: Bearer <key>`.

Env vars:
    OLC_API_KEY   required for any call
    OLC_BASE_URL  optional override; set to the prod URL to go live
"""
from __future__ import annotations

import os
import uuid
import time

import requests

# OLC recommends a 120s client timeout as a safe buffer on order creation;
# inline contact objects (how we send) want more headroom than contacts by id.
ORDER_TIMEOUT = 600

DEMO_BASE_URL = "https://demoapi.openletterconnect.com/api/v1"
PROD_BASE_URL = "https://api.openletterconnect.com/api/v1"


class OLCNotConfigured(Exception):
    """Raised when OLC_API_KEY is missing — the account isn't set up yet."""


class OLCApiError(Exception):
    def __init__(self, status_code: int, message: str, data=None):
        self.status_code = status_code
        self.message = message
        self.data = data
        super().__init__(f"OLC API Error {status_code}: {message}")


def _unwrap(payload):
    """OLC wraps most responses as {"message": ..., "data": ...}."""
    if isinstance(payload, dict) and "data" in payload:
        return payload["data"]
    return payload


def _as_list(payload) -> list:
    """Normalize list endpoints: bare list, {"data": [...]}, or {"data": {"rows": [...]}}."""
    data = _unwrap(payload)
    if isinstance(data, dict):
        for key in ("rows", "records", "items", "results"):
            if isinstance(data.get(key), list):
                return data[key]
        return []
    return data if isinstance(data, list) else []


class OLCClient:
    def __init__(self, api_key: str, base_url: str = DEMO_BASE_URL, timeout: int = 60):
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    @classmethod
    def from_env(cls) -> "OLCClient":
        api_key = os.getenv("OLC_API_KEY")
        if not api_key:
            raise OLCNotConfigured(
                "OLC_API_KEY is not set. Create an API key in the OpenLetterConnect "
                "dashboard (Settings -> API Keys) and add it to .env."
            )
        return cls(api_key, os.getenv("OLC_BASE_URL", DEMO_BASE_URL))

    @property
    def is_live(self) -> bool:
        return "demoapi." not in self.base_url

    # ── HTTP core ────────────────────────────────────────────────────────

    def _request(self, method: str, path: str, *, params=None, json=None,
                 retries: int = 3, idempotency_key: str | None = None,
                 timeout: int | None = None):
        url = f"{self.base_url}/{path.lstrip('/')}"
        headers = {"Authorization": f"Bearer {self.api_key}"}
        # OLC dedupes writes carrying the same key, so a retry after a timeout
        # can never place the same order twice (OLC support, 2026-08-18).
        if idempotency_key:
            headers["idempotency-key"] = idempotency_key
        last_err = None

        for attempt in range(retries):
            try:
                resp = requests.request(
                    method, url, headers=headers, params=params, json=json,
                    timeout=timeout or self.timeout,
                )
            except requests.RequestException as e:
                last_err = OLCApiError(0, f"Network error: {e}")
                time.sleep(2 ** attempt)
                continue

            # Retry on rate limit / transient server errors.
            if resp.status_code == 429 or resp.status_code >= 500:
                last_err = OLCApiError(resp.status_code, resp.text[:500])
                time.sleep(2 ** attempt)
                continue

            try:
                payload = resp.json()
            except ValueError:
                payload = {"raw": resp.text[:1000]}

            if resp.status_code >= 400:
                msg = payload.get("message") if isinstance(payload, dict) else None
                raise OLCApiError(resp.status_code, msg or resp.reason, payload)
            return payload

        raise last_err or OLCApiError(0, f"Failed after {retries} retries")

    # ── Endpoints ────────────────────────────────────────────────────────

    def get_products(self) -> list:
        return _as_list(self._request("GET", "/products"))

    def get_templates(self, page: int = 1, page_size: int = 100) -> list:
        return _as_list(self._request(
            "GET", "/templates", params={"page": page, "pageSize": page_size},
        ))

    def create_order(self, payload: dict, idempotency_key: str | None = None) -> dict:
        """POST /orders — payload needs productId, templateId, and contacts[].

        There is no cap on contacts per order (OLC support, 2026-08-18): 13,000
        in one request is fine, and `numberOfBatches` / `daysBetweenEachBatch`
        in the payload spread a list over several mail dates. The response
        returns as soon as the order record exists (15-30s for contacts passed
        by id, longer for inline contact objects), so give it real headroom.

        ALWAYS pass an idempotency_key for a paid send: without one a retry
        after a timeout can place the order twice. `order_uuid` in the payload
        does the same job if you prefer it in the body.
        """
        if not idempotency_key:
            idempotency_key = payload.get("order_uuid") or str(uuid.uuid4())
        return self._request("POST", "/orders", json=payload,
                             idempotency_key=idempotency_key,
                             timeout=max(self.timeout, ORDER_TIMEOUT))

    def view_proof(self, payload: dict) -> dict:
        """POST /orders/view-proof — renders a PDF without placing an order.

        payload: {templateId, contact: {...}, returnContact: {...}} ->
        response data.base64 is the PDF.
        """
        return self._request("POST", "/orders/view-proof", json=payload)

    def get_orders(self, page: int = 1, page_size: int = 25, search: str | None = None) -> dict:
        params = {"page": page, "pageSize": page_size}
        if search:
            params["search"] = search
        return self._request("GET", "/orders", params=params)

    def get_order(self, order_id) -> dict:
        return self._request("GET", f"/orders/{order_id}")
