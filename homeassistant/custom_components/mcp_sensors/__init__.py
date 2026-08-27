"""订 Mosquitto 上 mcp_sensors/<id>/event，在 HA 里建实体。不发 MQTT Discovery。"""

from __future__ import annotations

import json
import logging
from typing import Any

from homeassistant.components import mqtt
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.dispatcher import async_dispatcher_send

from .const import DEFAULT_PREFIX, DOMAIN, NUMERIC_KEYS, SIGNAL_UPDATE

_LOGGER = logging.getLogger(__name__)

PLATFORMS = ["sensor", "binary_sensor"]


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    prefix = (entry.data.get("prefix") or DEFAULT_PREFIX).strip()
    store: dict[str, dict[str, Any]] = {}
    hass.data.setdefault(DOMAIN, {})
    hass.data[DOMAIN][entry.entry_id] = {"prefix": prefix, "devices": store}

    @callback
    def _on_event(msg: Any) -> None:
        parts = str(msg.topic).split("/")
        if len(parts) < 3:
            return
        device_id = parts[1].strip().lower()
        if not device_id:
            return
        kind = parts[2]
        rec = store.setdefault(
            device_id,
            {"online": True, "payload": {}, "numeric": {}, "raw": ""},
        )
        payload = msg.payload
        raw = payload.decode("utf-8", "replace") if isinstance(payload, (bytes, bytearray)) else str(payload or "")
        if kind == "availability":
            rec["online"] = raw.strip().lower() not in ("offline", "0", "false")
            async_dispatcher_send(hass, f"{SIGNAL_UPDATE}_{device_id}")
            async_dispatcher_send(hass, SIGNAL_UPDATE)
            return
        if kind != "event":
            return
        try:
            data = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            data = {"raw": raw}
        if not isinstance(data, dict):
            data = {"value": data}
        rec["payload"] = data
        rec["raw"] = raw[:500]
        rec["online"] = True
        numeric: dict[str, float] = {}
        for key, meta in NUMERIC_KEYS.items():
            if key not in data:
                continue
            try:
                numeric[key] = float(data[key])
            except (TypeError, ValueError):
                continue
        rec["numeric"] = numeric
        async_dispatcher_send(hass, f"{SIGNAL_UPDATE}_{device_id}")
        async_dispatcher_send(hass, SIGNAL_UPDATE)

    await mqtt.async_subscribe(hass, f"{prefix}/+/event", _on_event)
    await mqtt.async_subscribe(hass, f"{prefix}/+/availability", _on_event)
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    _LOGGER.info("mcp_sensors subscribed %s/+/event", prefix)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unload_ok:
        hass.data[DOMAIN].pop(entry.entry_id, None)
    return unload_ok
