"""Auth helpers: inbound API keys, per-task callback tokens, HMAC signatures,
rate limiting and push-URL (SSRF) checks."""
from __future__ import annotations

import hashlib
import hmac
import ipaddress
import socket
import time
from urllib.parse import urlparse

from .config import ApiKey

SIG_HEADER = "X-A2A-Signature"


def match_api_key(keys: list[ApiKey], presented: str | None) -> ApiKey | None:
    if not presented:
        return None
    h = hashlib.sha256(presented.encode("utf-8")).hexdigest()
    found = None
    for k in keys:  # constant-time compare against every key
        if hmac.compare_digest(h, k.sha256):
            found = k
    return found


def bearer(auth_header: str | None) -> str | None:
    if not auth_header:
        return None
    parts = auth_header.split(None, 1)
    if len(parts) == 2 and parts[0].lower() == "bearer":
        return parts[1].strip()
    return None


def callback_token(secret: str, task_id: str) -> str:
    return "cbt_" + hmac.new(secret.encode(), f"callback:{task_id}".encode(), hashlib.sha256).hexdigest()


def sign(secret: str, body: bytes, ts: int | None = None) -> str:
    ts = int(time.time()) if ts is None else ts
    mac = hmac.new(secret.encode(), f"{ts}.".encode() + body, hashlib.sha256).hexdigest()
    return f"t={ts},v1={mac}"


def verify_signature(secret: str, body: bytes, header: str | None, max_skew: int) -> bool:
    if not header:
        return False
    try:
        kv = dict(x.strip().split("=", 1) for x in header.split(","))
        ts = int(kv["t"])
        mac = kv["v1"]
    except (ValueError, KeyError):
        return False
    if abs(time.time() - ts) > max_skew:
        return False
    expected = hmac.new(secret.encode(), f"{ts}.".encode() + body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, mac)


class RateLimiter:
    """In-memory token bucket per key. Good for a single-process deployment."""

    def __init__(self, per_min: int, burst: int):
        self.rate = per_min / 60.0
        self.burst = max(1, burst)
        self.buckets: dict[str, tuple[float, float]] = {}

    def allow(self, key: str) -> tuple[bool, float]:
        if self.rate <= 0:
            return True, 0.0
        now = time.monotonic()
        tokens, last = self.buckets.get(key, (float(self.burst), now))
        tokens = min(self.burst, tokens + (now - last) * self.rate)
        if tokens >= 1:
            self.buckets[key] = (tokens - 1, now)
            if len(self.buckets) > 50000:  # crude memory bound
                self.buckets.clear()
            return True, 0.0
        self.buckets[key] = (tokens, now)
        return False, (1 - tokens) / self.rate


def check_push_url(url: str, allow_http: bool, allow_private: bool) -> None:
    u = urlparse(url)
    if u.scheme not in ("https", "http") or not u.hostname:
        raise ValueError("push url must be an absolute http(s) URL")
    if u.scheme == "http" and not allow_http:
        raise ValueError("push url must use https")
    if allow_private:
        return
    try:
        infos = socket.getaddrinfo(u.hostname, u.port or (443 if u.scheme == "https" else 80))
    except socket.gaierror as e:
        raise ValueError("push url host does not resolve") from e
    for info in infos:
        ip = ipaddress.ip_address(info[4][0])
        if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved or ip.is_multicast \
                or ip.is_unspecified:
            raise ValueError("push url resolves to a non-public address")
