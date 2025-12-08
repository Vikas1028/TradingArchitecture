"""SmartAPI login helpers."""

from __future__ import annotations

import logging
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


# create_angel_session logs into SmartAPI and returns a session object for WS use.
# Parameters:
# - angel_cfg: dictionary with api_key, client_id, refresh_token, feed_token, password, totp_secret.
# - logger: logger for diagnostics.
# Returns:
# - AngelSession instance (real if SmartAPI SDK available, otherwise config-supplied tokens).
# Flow: if SmartConnect + password + TOTP present, perform login to obtain fresh tokens; otherwise fall back to provided tokens.
def create_angel_session(angel_cfg: Dict[str, Any], logger: logging.Logger) -> AngelSession:
    required = ["api_key", "client_id"]
    for key in required:
        if not angel_cfg.get(key):
            raise ValueError(f"Missing angel config field: {key}")

    api_key = str(angel_cfg["api_key"])
    client_id = str(angel_cfg["client_id"])
    password = angel_cfg.get("password")
    totp_secret = angel_cfg.get("totp_secret")

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
            return AngelSession(
                api_key=api_key,
                client_id=client_id,
                refresh_token=str(refresh_token),
                feed_token=str(feed_token),
                access_token=str(access_token),
            )
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
        raise ValueError("Angel feed_token/refresh_token not provided and login unavailable")
    return AngelSession(
        api_key=api_key,
        client_id=client_id,
        refresh_token=str(refresh_token),
        feed_token=str(feed_token),
        access_token=str(access_token),
    )
