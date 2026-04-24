import time
import requests
from datetime import datetime, timezone
from flask import current_app


class PCMApiError(Exception):
    def __init__(self, status_code, message, data=None):
        self.status_code = status_code
        self.message = message
        self.data = data or []
        super().__init__(f"PCM API Error {status_code}: {message}")


class PCMClient:
    """PostcardMania DirectMail API v3 client with token caching and retries."""

    BASE_URL = "https://v3.pcmintegrations.com"

    def __init__(self, api_key=None, api_secret=None, child_ref_nbr=None):
        self.api_key = api_key
        self.api_secret = api_secret
        self.child_ref_nbr = child_ref_nbr
        self._token = None
        self._token_expires = None

    @classmethod
    def from_config(cls):
        """Create client from Flask app config."""
        return cls(
            api_key=current_app.config["PCM_API_KEY"],
            api_secret=current_app.config["PCM_API_SECRET"],
            child_ref_nbr=current_app.config["PCM_CHILD_REF_NBR"],
        )

    @property
    def token(self):
        """Get a valid token, refreshing if expired or missing."""
        if self._token and self._token_expires:
            # Refresh 60 seconds before expiry
            if datetime.now(timezone.utc) < self._token_expires:
                return self._token
        self.authenticate()
        return self._token

    def authenticate(self):
        """Authenticate and cache the token."""
        resp = requests.post(
            f"{self.BASE_URL}/auth/login",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={
                "apiKey": self.api_key,
                "apiSecret": self.api_secret,
                "childRefNbr": self.child_ref_nbr,
            },
        )
        if resp.status_code != 200:
            raise PCMApiError(resp.status_code, "Authentication failed")

        result = resp.json()
        self._token = result["token"]
        # Parse expiry: "2026-03-24T21:27:19.262Z"
        expires_str = result.get("expires", "")
        try:
            self._token_expires = datetime.fromisoformat(expires_str.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            # Default: token valid for 14 minutes
            from datetime import timedelta
            self._token_expires = datetime.now(timezone.utc) + timedelta(minutes=14)

    def _headers(self):
        return {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def _request(self, method, endpoint, retries=3, **kwargs):
        """Make an API request with retries and error handling."""
        url = f"{self.BASE_URL}{endpoint}"
        for attempt in range(retries):
            resp = requests.request(method, url, headers=self._headers(), **kwargs)

            if resp.status_code == 401:
                # Token expired, re-authenticate and retry
                self._token = None
                continue

            if resp.status_code == 429:
                # Rate limited, back off
                wait = 2 ** attempt
                time.sleep(wait)
                continue

            if resp.status_code >= 500:
                # Server error, retry with backoff
                wait = 2 ** attempt
                time.sleep(wait)
                continue

            if resp.status_code >= 400:
                error = resp.json().get("error", {})
                raise PCMApiError(
                    resp.status_code,
                    error.get("message", "Unknown error"),
                    error.get("data", []),
                )

            return resp.json()

        raise PCMApiError(0, f"Failed after {retries} retries")

    def get(self, endpoint, params=None):
        return self._request("GET", endpoint, params=params)

    def post(self, endpoint, data=None):
        return self._request("POST", endpoint, json=data)

    def put(self, endpoint, data=None):
        return self._request("PUT", endpoint, json=data)

    def delete(self, endpoint):
        return self._request("DELETE", endpoint)
