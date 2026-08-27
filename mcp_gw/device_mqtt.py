"""MQTT broker provisioning for devices (haos_app).

Primary: mDNS _mcp-gw TXT mqtt= / mqtt_user / mqtt_pass.
Fallback: HTTP POST /api/wifi action=mqtt.

Broker URI never uses the device IP. Prefer MQTT_ADVERTISE_HOST / ADVERTISE_IP.
When mqtt_broker exists but mqtt_connected=false (missing auth), re-push creds.
"""

from __future__ import annotations

import logging
from typing import Any

import httpx

from . import config
from .mdns_advertise import guess_lan_ip

log = logging.getLogger("mcp_gw.device_mqtt")

_pushed_fp: dict[str, str] = {}
_last_config_fp: str = ""


def _is_loopback(host: str) -> bool:
    h = (host or "").strip().lower()
    if h.startswith("mqtt://"):
        h = h[7:]
    elif h.startswith("mqtts://"):
        h = h[8:]
    h = h.split("/")[0]
    if h.startswith("["):
        h = h.split("]")[0].lstrip("[")
    else:
        h = h.split(":")[0]
    return (not h) or h in ("127.0.0.1", "localhost", "0.0.0.0", "::1") or h.startswith("127.")


def _advertise_host() -> str:
    for name in ("MQTT_ADVERTISE_HOST", "ADVERTISE_IP"):
        v = (getattr(config, name, "") or "").strip()
        if v and not _is_loopback(v):
            return v
    return (guess_lan_ip() or "").strip()


def broker_uri_for_device(*_args: Any, **_kwargs: Any) -> str:
    """LAN broker for devices. Extra args ignored (compat with old lan_ip signature)."""
    raw = (config.MQTT_HOST or "").strip()
    advertise = _advertise_host()
    if raw and not _is_loopback(raw):
        host = raw
        if host.startswith("mqtt://"):
            host = host[7:].split("/")[0].split(":")[0]
        elif host.startswith("mqtts://"):
            host = host[8:].split("/")[0].split(":")[0]
    else:
        host = advertise
    if not host or _is_loopback(host):
        return ""
    return f"mqtt://{host}:{int(config.MQTT_PORT or 1883)}"


def config_fingerprint() -> str:
    uri = broker_uri_for_device()
    user = (config.MQTT_USER or "").strip()
    password = (config.MQTT_PASSWORD or "").strip()
    return f"{uri}\n{user}\n{password}"


def invalidate_push_cache() -> None:
    global _last_config_fp
    _pushed_fp.clear()
    _last_config_fp = ""
    log.info("mqtt push cache cleared (will re-push on next cycle)")


def consume_config_change() -> bool:
    global _last_config_fp
    fp = config_fingerprint()
    if fp == _last_config_fp:
        return False
    if _last_config_fp:
        log.info("mqtt config changed -> force re-push to devices")
        _pushed_fp.clear()
    _last_config_fp = fp
    return True


async def push_if_missing(device_ip: str, *, force: bool = False) -> bool:
    uri = broker_uri_for_device()
    if not uri:
        log.warning("skip mqtt push: invalid broker host (set advertise_ip / MQTT_ADVERTISE_HOST)")
        return False
    user = (config.MQTT_USER or "").strip()
    password = (config.MQTT_PASSWORD or "").strip()
    fp = config_fingerprint()
    if not user:
        log.warning(
            "mqtt push without MQTT_USER: LAN devices often get Not authorized; "
            "set plugin mqtt_user/password"
        )
    wifi_url = f"http://{device_ip}/api/wifi"
    try:
        async with httpx.AsyncClient(timeout=4.0) as client:
            if not force and _pushed_fp.get(device_ip) == fp:
                r = await client.get(wifi_url)
                r.raise_for_status()
                info: Any = r.json()
                if isinstance(info, dict) and bool(info.get("mqtt_connected")):
                    return True
            elif not force:
                r = await client.get(wifi_url)
                r.raise_for_status()
                info = r.json() if r.content else {}
                if isinstance(info, dict):
                    existing = str(info.get("mqtt_broker") or "").strip()
                    connected = bool(info.get("mqtt_connected"))
                    dev_user = str(info.get("mqtt_user") or "").strip()
                    same_broker = existing == uri
                    same_user = (not user) or (not dev_user) or (dev_user == user)
                    if same_broker and connected and same_user:
                        _pushed_fp[device_ip] = fp
                        return True
                    if existing and not connected and not user:
                        return False

            body = {
                "action": "mqtt",
                "broker": uri,
                "user": user,
                "password": password,
            }
            w = await client.post(wifi_url, json=body)
            txt = (w.text or "")[:180]
            if w.status_code >= 400:
                log.warning("mqtt push failed %s status=%s body=%s", device_ip, w.status_code, txt)
                return False
            _pushed_fp[device_ip] = fp
            log.info(
                "mqtt pushed %s user=%s -> %s%s",
                uri,
                user or "(none)",
                device_ip,
                " (force)" if force else "",
            )
            return True
    except Exception as e:
        log.debug("mqtt push skip %s: %s", device_ip, e)
        return False


async def ensure_device_mqtt(device_ip: str, *, force: bool = False) -> bool:
    """Alias used by coap_poll."""
    return await push_if_missing(device_ip, force=force)


async def push_all(device_ips: list[str], *, force: bool = False) -> dict[str, bool]:
    out: dict[str, bool] = {}
    if force:
        invalidate_push_cache()
        global _last_config_fp
        _last_config_fp = config_fingerprint()
    for ip in device_ips:
        ip = (ip or "").strip()
        if not ip:
            continue
        out[ip] = await push_if_missing(ip, force=force)
    return out
