from __future__ import annotations

from typing import Any

from homeassistant.components.binary_sensor import (
    BinarySensorDeviceClass,
    BinarySensorEntity,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.dispatcher import async_dispatcher_connect
from homeassistant.helpers.entity import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN, SIGNAL_UPDATE


async def async_setup_entry(
    hass: HomeAssistant, entry: ConfigEntry, async_add_entities: AddEntitiesCallback
) -> None:
    known: set[str] = set()
    store: dict[str, dict[str, Any]] = hass.data[DOMAIN][entry.entry_id]["devices"]

    def _ensure() -> None:
        new = []
        for device_id in list(store):
            if device_id in known:
                continue
            known.add(device_id)
            new.append(AvailabilitySensor(hass, entry.entry_id, device_id))
        if new:
            async_add_entities(new)

    @callback
    def _updated() -> None:
        _ensure()

    entry.async_on_unload(async_dispatcher_connect(hass, SIGNAL_UPDATE, _updated))
    _ensure()


class AvailabilitySensor(BinarySensorEntity):
    _attr_should_poll = False
    _attr_device_class = BinarySensorDeviceClass.CONNECTIVITY

    def __init__(self, hass: HomeAssistant, entry_id: str, device_id: str) -> None:
        self.hass = hass
        self._entry_id = entry_id
        self._device_id = device_id
        self._attr_unique_id = f"{DOMAIN}_{device_id}_online"
        self._attr_name = f"{device_id} 在线"

    @property
    def device_info(self) -> DeviceInfo:
        return DeviceInfo(
            identifiers={(DOMAIN, self._device_id)},
            name=f"MCP {self._device_id}",
            manufacturer="mcp_sensors",
        )

    @property
    def is_on(self) -> bool:
        rec = self.hass.data[DOMAIN][self._entry_id]["devices"].get(self._device_id) or {}
        return bool(rec.get("online", True))

    async def async_added_to_hass(self) -> None:
        self.async_on_remove(
            async_dispatcher_connect(
                self.hass, f"{SIGNAL_UPDATE}_{self._device_id}", self.async_write_ha_state
            )
        )
