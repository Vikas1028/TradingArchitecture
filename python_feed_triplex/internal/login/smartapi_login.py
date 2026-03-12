"""SmartAPI login helpers."""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict

try:
    from SmartApi import SmartConnect  # type: ignore
except Exception:  # pylint: disable=broad-except
    SmartConnect = None

try:
    import pyotp  # type: ignore
except Exception:  # pylint: disable=broad-except
    pyotp = None


@dataclass
class AngelSession:
    """Simple container for SmartAPI session details."""

    api_key: str
    client_id: str
    refresh_token: str
    feed_token: str
    access_token: str = ""


_SESSION_LOCK = threading.Lock()
_SESSION_CACHE: Dict[str, tuple[AngelSession, float]] = {}


def clear_angel_session_cache(client_id: str | None = None) -> None:
    """Clear cached SmartAPI sessions globally or for one client."""
    with _SESSION_LOCK:
        if client_id:
            _SESSION_CACHE.pop(client_id, None)
        else:
            _SESSION_CACHE.clear()


def logout_angel_session(angel_cfg: Dict[str, Any], logger: logging.Logger) -> None:
    """Best-effort SmartAPI logout to invalidate current auth context."""
    client_id = str(angel_cfg.get("client_id") or "")
    refresh_token = str(angel_cfg.get("refresh_token") or "")
    api_key = str(angel_cfg.get("api_key") or "")

    # Always clear local cache first so next login path is forced fresh.
    clear_angel_session_cache(client_id or None)

    if not SmartConnect or not api_key or not client_id:
        logger.info("Skipping SmartAPI logout (sdk/api_key/client_id unavailable) client_id=%s", client_id or "unknown")
        return

    try:
        client = SmartConnect(api_key=api_key)
        terminated = False
        terminate_fn = getattr(client, "terminateSession", None)
        if callable(terminate_fn):
            terminate_fn(client_id)
            terminated = True

        # Some SDK variants expose a refresh-token based revoke.
        revoke_fn = getattr(client, "logout", None)
        if callable(revoke_fn) and refresh_token:
            revoke_fn(client_id, refresh_token)  # type: ignore[misc]
            terminated = True

        if terminated:
            logger.info("SmartAPI logout/terminate requested for client_id=%s", client_id)
        else:
            logger.info("SmartAPI logout method unavailable; cache cleared for client_id=%s", client_id)
    except Exception as exc:  # pylint: disable=broad-except
        logger.warning("SmartAPI logout failed for client_id=%s: %s", client_id, exc)


# create_angel_session logs into SmartAPI and returns a session object for WS use.
# Parameters:
# - angel_cfg: dictionary with api_key, client_id, refresh_token, feed_token, password, totp_secret.
# - logger: logger for diagnostics.
# Returns:
# - AngelSession instance (real if SmartAPI SDK available, otherwise config-supplied tokens).
# Flow: if SmartConnect + password + TOTP present, perform login to obtain fresh tokens; otherwise fall back to provided tokens.
def create_angel_session(angel_cfg: Dict[str, Any], logger: logging.Logger) -> AngelSession:
    logger.info("Creating Angel session for client_id=%s", angel_cfg.get("client_id"))
    required = ["api_key", "client_id"]
    for key in required:
        if not angel_cfg.get(key):
            raise ValueError(f"Missing angel config field: {key}")

    api_key = str(angel_cfg["api_key"])
    client_id = str(angel_cfg["client_id"])
    password = angel_cfg.get("password")
    totp_secret = angel_cfg.get("totp_secret")
    cache_ttl_sec = int(angel_cfg.get("session_cache_ttl_sec", 600))

    with _SESSION_LOCK:
        cached = _SESSION_CACHE.get(client_id)
        if cached and (time.time() - cached[1]) < cache_ttl_sec:
            logger.info("Reusing cached SmartAPI session for client_id=%s", client_id)
            return cached[0]

        # Attempt real login if SDK and TOTP available.
        if SmartConnect and password and totp_secret and pyotp:
            try:
                otp = pyotp.TOTP(str(totp_secret)).now()
                client = SmartConnect(api_key=api_key)
                data = client.generateSession(client_id, str(password), otp)
                # Known keys from SmartAPI response
                refresh_token = (
                    (data.get("data") or {}).get("refreshToken")
                    or data.get("refreshToken")
                    or ""
                )
                access_token = (
                    (data.get("data") or {}).get("jwtToken")
                    or data.get("jwtToken")
                    or ""
                )
                feed_token = getattr(client, "feed_token", None) or (data.get("data") or {}).get("feedToken") or data.get("feedToken") or ""
                logger.info("SmartAPI login succeeded for client_id=%s", client_id)
                session = AngelSession(
                    api_key=api_key,
                    client_id=client_id,
                    refresh_token=str(refresh_token),
                    feed_token=str(feed_token),
                    access_token=str(access_token),
                )
                _SESSION_CACHE[client_id] = (session, time.time())
                return session
            except Exception as exc:  # pylint: disable=broad-except
                logger.warning("SmartAPI login failed; falling back to provided tokens: %s", exc)
        elif not SmartConnect:
            logger.warning("SmartAPI SDK not installed; using provided feed/refresh tokens")
        elif not pyotp:
            logger.warning("pyotp not installed; cannot generate TOTP, using provided feed/refresh tokens")
        else:
            logger.warning("Missing password/totp_secret; using provided feed/refresh tokens")

        # Fallback to provided tokens.
        feed_token = angel_cfg.get("feed_token") or angel_cfg.get("refresh_token")
        refresh_token = angel_cfg.get("refresh_token") or ""
        access_token = angel_cfg.get("access_token") or refresh_token
        if not feed_token:
            cached = _SESSION_CACHE.get(client_id)
            if cached:
                logger.warning("Using last cached SmartAPI session after login/fallback failure for client_id=%s", client_id)
                return cached[0]
            raise ValueError("Angel feed_token/refresh_token not provided and login unavailable")
        logger.info("Using provided tokens for client_id=%s (feed_token present=%s)", client_id, bool(feed_token))
        session = AngelSession(
            api_key=api_key,
            client_id=client_id,
            refresh_token=str(refresh_token),
            feed_token=str(feed_token),
            access_token=str(access_token),
        )
        _SESSION_CACHE[client_id] = (session, time.time())
        return session


# create_angel_client authenticates and returns a SmartConnect client for REST lookups.
# Parameters:
# - angel_cfg: dictionary with api_key, client_id, password, totp_secret.
# - logger: logger for diagnostics.
# Returns:
# - authenticated SmartConnect client, or None if SDK/login is unavailable.
def create_angel_client(angel_cfg: Dict[str, Any], logger: logging.Logger) -> Any | None:
    if not SmartConnect:
        logger.warning("SmartAPI SDK not installed; token lookup unavailable")
        return None
    if not pyotp:
        logger.warning("pyotp not installed; token lookup unavailable")
        return None

    api_key = angel_cfg.get("api_key")
    client_id = angel_cfg.get("client_id")
    password = angel_cfg.get("password")
    totp_secret = angel_cfg.get("totp_secret")
    if not api_key or not client_id or not password or not totp_secret:
        logger.warning("Missing SmartAPI credentials for token lookup")
        return None

    try:
        otp = pyotp.TOTP(str(totp_secret)).now()
        client = SmartConnect(api_key=str(api_key))
        client.generateSession(str(client_id), str(password), otp)
        logger.info("SmartAPI client authenticated for token lookup")
        return client
    except Exception as exc:  # pylint: disable=broad-except
        logger.warning("SmartAPI token lookup login failed: %s", exc)
        return None
