from __future__ import annotations

from typing import Any

from homeassistant.components.sensor import SensorDeviceClass, SensorEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.dispatcher import async_dispatcher_connect
from homeassistant.helpers.entity import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN, NUMERIC_KEYS, SIGNAL_UPDATE


async def async_setup_entry(
    hass: HomeAssistant, entry: ConfigEntry, async_add_entities: AddEntitiesCallback
) -> None:
    known: set[tuple[str, str]] = set()
    data = hass.data[DOMAIN][entry.entry_id]
    store: dict[str, dict[str, Any]] = data["devices"]

    def _ensure() -> None:
        new: list[SensorEntity] = []
        for device_id, rec in list(store.items()):
            last_key = (device_id, "_last")
            if last_key not in known:
                known.add(last_key)
                new.append(LastEventSensor(hass, entry.entry_id, device_id))
            for key in rec.get("numeric") or {}:
                nk = (device_id, key)
                if nk in known:
                    continue
                known.add(nk)
                new.append(NumericSensor(hass, entry.entry_id, device_id, key))
        if new:
            async_add_entities(new)

    @callback
    def _updated() -> None:
        _ensure()

    entry.async_on_unload(async_dispatcher_connect(hass, SIGNAL_UPDATE, _updated))
    _ensure()


class _Base:
    _attr_should_poll = False

    def __init__(self, hass: HomeAssistant, entry_id: str, device_id: str) -> None:
        self.hass = hass
        self._entry_id = entry_id
        self._device_id = device_id

    @property
    def _rec(self) -> dict[str, Any]:
        return self.hass.data[DOMAIN][self._entry_id]["devices"].get(self._device_id) or {}

    @property
    def device_info(self) -> DeviceInfo:
        return DeviceInfo(
            identifiers={(DOMAIN, self._device_id)},
            name=f"MCP {self._device_id}",
            manufacturer="mcp_sensors",
        )

    @property
    def available(self) -> bool:
        return bool(self._rec.get("online", True))


class NumericSensor(_Base, SensorEntity):
    def __init__(self, hass: HomeAssistant, entry_id: str, device_id: str, key: str) -> None:
        super().__init__(hass, entry_id, device_id)
        self._key = key
        klass, unit, name = NUMERIC_KEYS[key]
        self._attr_unique_id = f"{DOMAIN}_{device_id}_{key}"
        self._attr_name = f"{device_id} {name}"
        self._attr_native_unit_of_measurement = unit
        if klass == "temperature":
            self._attr_device_class = SensorDeviceClass.TEMPERATURE
        elif klass == "humidity":
            self._attr_device_class = SensorDeviceClass.HUMIDITY
        elif klass == "carbon_dioxide":
            self._attr_device_class = SensorDeviceClass.CO2
        elif klass == "illuminance":
            self._attr_device_class = SensorDeviceClass.ILLUMINANCE

    @property
    def native_value(self) -> float | None:
        num = self._rec.get("numeric") or {}
        return num.get(self._key)

    async def async_added_to_hass(self) -> None:
        self.async_on_remove(
            async_dispatcher_connect(
                self.hass, f"{SIGNAL_UPDATE}_{self._device_id}", self.async_write_ha_state
            )
        )


class LastEventSensor(_Base, SensorEntity):
    def __init__(self, hass: HomeAssistant, entry_id: str, device_id: str) -> None:
        super().__init__(hass, entry_id, device_id)
        self._attr_unique_id = f"{DOMAIN}_{device_id}_last_event"
        self._attr_name = f"{device_id} 最近事件"

    @property
    def native_value(self) -> str | None:
        payload = self._rec.get("payload") or {}
        for k in ("type", "event", "name", "op"):
            if payload.get(k):
                return str(payload[k])[:64]
        return "ok" if payload else None

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        return dict(self._rec.get("payload") or {})

    async def async_added_to_hass(self) -> None:
        self.async_on_remove(
            async_dispatcher_connect(
                self.hass, f"{SIGNAL_UPDATE}_{self._device_id}", self.async_write_ha_state
            )
        )
