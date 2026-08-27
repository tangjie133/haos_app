"""MQTT + voice provisioning for devices (haos_app).

Primary: mDNS _mcp-gw TXT mqtt= / mqtt_user / mqtt_pass / ws=.
Fallback: HTTP POST /api/wifi
  - action=mqtt with broker/user/password + ws（语音地址，固件需识别 ws 字段）
  - 再试 action=voice（新固件）；旧固件会回 missing ssid，忽略即可

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


def voice_ws_for_device() -> str:
    """LAN voice WebSocket URI devices should dial (never 127.0.0.1)."""
    host = _advertise_host()
    if not host or _is_loopback(host):
        return ""
    path = config.VOICE_WS_PATH if config.VOICE_WS_PATH.startswith("/") else f"/{config.VOICE_WS_PATH}"
    return f"ws://{host}:{int(config.VOICE_WS_PORT)}{path}"


def config_fingerprint() -> str:
    uri = broker_uri_for_device()
    user = (config.MQTT_USER or "").strip()
    password = (config.MQTT_PASSWORD or "").strip()
    voice = voice_ws_for_device()
    return f"{uri}\n{user}\n{password}\n{voice}"


def invalidate_push_cache() -> None:
    global _last_config_fp
    _pushed_fp.clear()
    _last_config_fp = ""
    log.info("mqtt/voice push cache cleared (will re-push on next cycle)")


def consume_config_change() -> bool:
    global _last_config_fp
    fp = config_fingerprint()
    if fp == _last_config_fp:
        return False
    if _last_config_fp:
        log.info("mqtt/voice config changed -> force re-push to devices")
        _pushed_fp.clear()
    _last_config_fp = fp
    return True


def _voice_from_wifi(info: Any) -> str:
    if not isinstance(info, dict):
        return ""
    for key in ("voice_ws", "audio_ws", "ws", "voice_url", "voice"):
        v = str(info.get(key) or "").strip()
        if v.startswith("ws://") or v.startswith("wss://"):
            return v
    return ""


async def _post_voice(client: httpx.AsyncClient, wifi_url: str, voice_ws: str) -> bool:
    """Best-effort dedicated voice action (new firmware). Old firmware → missing ssid."""
    if not voice_ws:
        return False
    try:
        w = await client.post(
            wifi_url,
            json={"action": "voice", "ws": voice_ws, "url": voice_ws, "voice_ws": voice_ws},
        )
        txt = (w.text or "")[:160]
        if w.status_code < 400:
            log.info("voice pushed action=voice -> %s (%s)", wifi_url, txt[:80])
            return True
        if "missing ssid" in txt.lower():
            log.debug("device ignores action=voice (need firmware ws support): %s", txt)
        else:
            log.debug("voice action=voice status=%s body=%s", w.status_code, txt)
    except Exception as e:
        log.debug("voice action=voice skip: %s", e)
    return False


async def push_if_missing(device_ip: str, *, force: bool = False) -> bool:
    uri = broker_uri_for_device()
    if not uri:
        log.warning("skip mqtt push: invalid broker host (set advertise_ip / MQTT_ADVERTISE_HOST)")
        return False
    user = (config.MQTT_USER or "").strip()
    password = (config.MQTT_PASSWORD or "").strip()
    voice_ws = voice_ws_for_device()
    fp = config_fingerprint()
    if not user:
        log.warning(
            "mqtt push without MQTT_USER: LAN devices often get Not authorized; "
            "set plugin mqtt_user/password"
        )
    wifi_url = f"http://{device_ip}/api/wifi"
    try:
        async with httpx.AsyncClient(timeout=4.0) as client:
            info: Any = {}
            if not force and _pushed_fp.get(device_ip) == fp:
                r = await client.get(wifi_url)
                r.raise_for_status()
                info = r.json()
                if isinstance(info, dict) and bool(info.get("mqtt_connected")):
                    # 已推过同一指纹；旧固件无 voice 字段时仍周期性试 action=voice
                    if voice_ws and _voice_from_wifi(info) != voice_ws:
                        await _post_voice(client, wifi_url, voice_ws)
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
                    have_voice = _voice_from_wifi(info)
                    voice_ok = (not voice_ws) or (have_voice == voice_ws)
                    if same_broker and connected and same_user and voice_ok:
                        _pushed_fp[device_ip] = fp
                        return True
                    if same_broker and connected and same_user and voice_ws and not have_voice:
                        # MQTT 已好，仅补语音（旧固件可能仍无字段）
                        body = {
                            "action": "mqtt",
                            "broker": uri,
                            "user": user,
                            "password": password,
                            "ws": voice_ws,
                            "voice_ws": voice_ws,
                        }
                        w = await client.post(wifi_url, json=body)
                        if w.status_code < 400:
                            _pushed_fp[device_ip] = fp
                            log.info(
                                "mqtt+voice re-push %s ws=%s -> %s (mqtt already up)",
                                uri,
                                voice_ws,
                                device_ip,
                            )
                        await _post_voice(client, wifi_url, voice_ws)
                        return w.status_code < 400
                    if existing and not connected and not user:
                        return False

            body = {
                "action": "mqtt",
                "broker": uri,
                "user": user,
                "password": password,
            }
            if voice_ws:
                body["ws"] = voice_ws
                body["voice_ws"] = voice_ws
            w = await client.post(wifi_url, json=body)
            txt = (w.text or "")[:180]
            if w.status_code >= 400:
                log.warning("mqtt push failed %s status=%s body=%s", device_ip, w.status_code, txt)
                return False
            _pushed_fp[device_ip] = fp
            log.info(
                "mqtt%s pushed %s user=%s -> %s%s",
                f"+voice({voice_ws})" if voice_ws else "",
                uri,
                user or "(none)",
                device_ip,
                " (force)" if force else "",
            )
            if voice_ws:
                await _post_voice(client, wifi_url, voice_ws)
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
