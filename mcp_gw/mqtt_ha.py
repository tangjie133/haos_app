"""订设备 MQTT 事件，并由本 App 代发 HA MQTT Discovery（固件不用改、用户不用拷集成）。"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from typing import Any

from . import config
from .registry import DeviceRegistry
from .sessions import SessionManager

log = logging.getLogger("mcp_gw.mqtt")

# CoAP snapshot / 嵌套 JSON 字段 → HA 实体（每种物理量只建一个）
# (候选 key, device_class, 单位, 中文名, unique 后缀)
_NUMERIC_GROUPS: list[tuple[tuple[str, ...], str | None, str, str, str]] = [
    (("temp", "temperature", "temp_c"), "temperature", "°C", "温度", "temp"),
    (("humidity", "hum", "rh"), "humidity", "%", "湿度", "humidity"),
    (("co2", "co2_ppm", "carbon_dioxide", "eco2"), "carbon_dioxide", "ppm", "二氧化碳", "co2"),
    (("light", "lux", "illuminance", "als", "ambient_light"), "illuminance", "lx", "环境光", "light"),
    (("sound", "db", "noise", "spl", "ambient_sound"), None, "dB", "环境声音", "sound"),
]


def _numeric_fields(ev: dict[str, Any]) -> list[tuple[str, str, tuple[Any, ...]]]:
    """返回 (unique后缀, JSON路径, (device_class, unit, 中文名))。"""
    found: list[tuple[str, str, tuple[Any, ...]]] = []
    seen_group: set[str] = set()
    nests: list[tuple[str, dict[str, Any]]] = [("", ev)]
    for nest in ("env", "data", "sensors", "values", "payload"):
        inner = ev.get(nest)
        if isinstance(inner, dict):
            nests.append((nest, inner))
    for keys, dev_class, unit, name, suffix in _NUMERIC_GROUPS:
        if suffix in seen_group:
            continue
        for nest, obj in nests:
            hit = next((k for k in keys if k in obj), None)
            if not hit:
                continue
            try:
                float(obj[hit])
            except (TypeError, ValueError):
                continue
            path = f"{nest}.{hit}" if nest else hit
            found.append((suffix, path, (dev_class, unit, name)))
            seen_group.add(suffix)
            break
    return found


# HA MQTT Event 实体只认 payload.event_type，且必须在此列表里
_HA_EVENT_TYPES: tuple[str, ...] = (
    "event",
    "ok",
    "update",
    "area_enter",
    "area_leave",
    "range_enter",
    "range_exit",
    "person_static",
    "person_moving",
    "person_enter",
    "person_leave",
    "threshold",
    "alarm",
    "button",
    "motion",
    "pir",
    "mmwave",
    "touch",
    "voice",
    "trigger",
    "config",
    "sample",
    "sound",
)


def _event_fp(ev: dict[str, Any]) -> str:
    keys = ("id", "ts", "timestamp", "time", "type", "event", "event_type", "category", "message", "area", "person_id")
    slim = {k: ev.get(k) for k in keys if k in ev}
    if len(slim) < 2:
        slim = ev
    return json.dumps(slim, ensure_ascii=False, sort_keys=True, default=str)[:800]


def _normalize_event(ev: dict[str, Any]) -> dict[str, Any]:
    out = dict(ev)
    raw = out.get("event_type") or out.get("type") or out.get("event") or out.get("category") or "event"
    raw = str(raw).strip() or "event"
    out["event_type"] = raw if raw in _HA_EVENT_TYPES else "event"
    if not out.get("type"):
        out["type"] = raw
    return out


def _avail(device_id: str) -> dict[str, Any]:
    topic = f"{config.MQTT_PREFIX}/{device_id}/availability"
    return {
        "availability_topic": topic,
        "payload_available": "online",
        "payload_not_available": "offline",
    }


def _expire_s() -> int:
    return max(20, int(config.COAP_POLL_S * 4))


class HaMqttBridge:
    def __init__(self, registry: DeviceRegistry, sessions: SessionManager) -> None:
        self.registry = registry
        self.sessions = sessions
        self._task: asyncio.Task[Any] | None = None
        self._connected = False
        self.last_error = ""
        self._client: Any = None
        self._discovered: set[str] = set()
        self._seen: dict[str, dict[str, None]] = {}
        self._mqtt_event_n = 0
        self._was_online: dict[str, bool] = {}
        self._last_snap: dict[str, dict[str, Any]] = {}
        # HA 里手动删实体后不再自动 Discovery，直到设备重新上线
        self._suppressed: set[str] = set()

    def status(self) -> dict[str, Any]:
        return {
            "enabled": config.MQTT_ENABLE,
            "connected": self._connected,
            "discovery": config.MQTT_DISCOVERY,
            "prefix": config.MQTT_PREFIX,
            "events_received": self._mqtt_event_n,
            "last_error": self.last_error,
        }

    async def start(self) -> None:
        if not config.MQTT_ENABLE:
            log.info("MQTT disabled")
            return
        self._task = asyncio.create_task(self._run(), name="mqtt-events")

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._client = None

    async def _run(self) -> None:
        try:
            import aiomqtt
        except ImportError:
            self.last_error = "aiomqtt not installed"
            log.warning(self.last_error)
            return

        # 设备上报；App 再 Discovery 转给 HA（不要让 HA 直接订设备主题）
        filters = (
            f"{config.MQTT_PREFIX}/+/event",
            f"{config.MQTT_PREFIX}/+/events",
            "homeassistant/status",
            "homeassistant/+/+/config",
        )
        while True:
            try:
                async with aiomqtt.Client(
                    hostname=config.MQTT_HOST,
                    port=config.MQTT_PORT,
                    username=config.MQTT_USER or None,
                    password=config.MQTT_PASSWORD or None,
                    identifier=f"{config.MQTT_CLIENT_ID}-ev-{os.getpid()}",
                ) as client:
                    self._client = client
                    self._connected = True
                    self.last_error = ""
                    for flt in filters:
                        try:
                            await client.subscribe(flt, qos=1)
                        except TypeError:
                            await client.subscribe(flt)
                    log.info(
                        "MQTT 收设备事件 %s → Discovery 转 HA discovery=%s",
                        " ".join(filters),
                        "on" if config.MQTT_DISCOVERY else "off",
                    )
                    async for message in client.messages:
                        await self._on_message(client, str(message.topic), message.payload)
            except asyncio.CancelledError:
                self._connected = False
                self._client = None
                raise
            except Exception as e:
                self._connected = False
                self._client = None
                self.last_error = str(e)
                log.warning("MQTT reconnect in 5s: %s", e)
                await asyncio.sleep(5)

    async def _on_message(
        self, client: Any, topic: str, payload: bytes | bytearray | str | None
    ) -> None:
        raw = (
            payload.decode("utf-8", "replace")
            if isinstance(payload, (bytes, bytearray))
            else str(payload or "")
        )
        if topic == "homeassistant/status":
            if raw.strip().lower() == "online":
                log.info("HA MQTT 上线，重新 Discovery 全部设备")
                await self._rediscover_all(client)
            return
        if topic.startswith("homeassistant/") and topic.endswith("/config"):
            if not raw.strip():
                parts = topic.split("/")
                obj = parts[2] if len(parts) >= 4 else ""
                if obj.startswith("mcp_"):
                    self._on_ha_removed_config(topic)
            return
        parts = topic.split("/")
        if len(parts) < 3:
            return
        device_id = parts[1].strip().lower()
        kind = parts[2]
        rec = self.registry.get(device_id)
        if rec is None:
            rec = self.registry.ensure_mqtt_device(device_id)
        if kind not in ("event", "events"):
            return
        log.info("MQTT 入站 %s %s", topic, raw[:160].replace("\n", " "))
        try:
            parsed = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            parsed = {"raw": raw}
        if isinstance(parsed, list):
            for item in parsed:
                if isinstance(item, dict):
                    await self.ingest_event(device_id, item, source="mqtt", client=client)
            return
        ev = parsed if isinstance(parsed, dict) else {"value": parsed}
        items = ev.get("events") if isinstance(ev.get("events"), list) else None
        if items:
            for item in items:
                if isinstance(item, dict):
                    await self.ingest_event(device_id, item, source="mqtt", client=client)
            return
        await self.ingest_event(device_id, ev, source="mqtt", client=client)

    async def _ensure_discovery(
        self,
        client: Any,
        device_id: str,
        ev: dict[str, Any],
        numeric_topic: str | None = None,
        force: bool = False,
    ) -> None:
        if not config.MQTT_DISCOVERY:
            return
        device_id = (device_id or "").strip().lower()
        if device_id in self._suppressed and not force:
            return
        if not isinstance(ev, dict):
            ev = {}
        ha_topic = f"{config.MQTT_PREFIX}/{device_id}/ha_event"
        device = {
            "identifiers": [f"mcp_sensors_{device_id}"],
            "name": f"传感器 {device_id}",
            "manufacturer": "mcp_sensors",
            "model": "ESP32",
        }
        last_id = f"{device_id}_last"
        if force or last_id not in self._discovered:
            await self._pub_config(
                client,
                f"homeassistant/sensor/mcp_{device_id}_last/config",
                {
                    "name": f"{device_id} 最近事件",
                    "unique_id": f"mcp_sensors_{device_id}_last",
                    "state_topic": ha_topic,
                    "value_template": "{{ value_json.message | default(value_json.type) }} {{ value_json.received_at | default('') }}",
                    "json_attributes_topic": ha_topic,
                    "device": device,
                    **_avail(device_id),
                },
            )
            self._discovered.add(last_id)
            log.info("HA Discovery 已登记设备 %s（MQTT 列表会出现，无需拷集成）", device_id)

        evt_id = f"{device_id}_evt"
        if force or evt_id not in self._discovered:
            await self._pub_config(
                client,
                f"homeassistant/event/mcp_{device_id}_evt/config",
                {
                    "name": f"{device_id} 事件",
                    "unique_id": f"mcp_sensors_{device_id}_evt",
                    "state_topic": ha_topic,
                    "event_types": list(_HA_EVENT_TYPES),
                    "device": device,
                    **_avail(device_id),
                },
            )
            self._discovered.add(evt_id)

        avail_id = f"{device_id}_online"
        if force or avail_id not in self._discovered:
            await self._pub_config(
                client,
                f"homeassistant/binary_sensor/mcp_{device_id}_online/config",
                {
                    "name": f"{device_id} 在线",
                    "unique_id": f"mcp_sensors_{device_id}_online",
                    "state_topic": f"{config.MQTT_PREFIX}/{device_id}/availability",
                    "payload_on": "online",
                    "payload_off": "offline",
                    "device_class": "connectivity",
                    "device": device,
                },
            )
            self._discovered.add(avail_id)

        for key, path, (dev_class, unit, name) in _numeric_fields(ev):
            uid = f"{device_id}_{key}"
            if not force and uid in self._discovered:
                continue
            snap_topic = numeric_topic or f"{config.MQTT_PREFIX}/{device_id}/snapshot"
            body: dict[str, Any] = {
                "name": f"{device_id} {name}",
                "unique_id": f"mcp_sensors_{uid}",
                "state_topic": snap_topic,
                "value_template": "{{ value_json.%s }}" % path,
                "expire_after": _expire_s(),
                "device": device,
                **_avail(device_id),
            }
            if unit:
                body["unit_of_measurement"] = unit
            if dev_class:
                body["device_class"] = dev_class
            await self._pub_config(
                client, f"homeassistant/sensor/mcp_{uid}/config", body
            )
            self._discovered.add(uid)

    async def publish_liveness(self, device_id: str, online: bool) -> None:
        """在线由 HTTP/CoAP 快照成败决定，写入 availability（固件 MQTT 不上报在线）。"""
        client = self._client
        if not client:
            return
        device_id = (device_id or "").strip().lower()
        if not device_id:
            return
        topic = f"{config.MQTT_PREFIX}/{device_id}/availability"
        payload = "online" if online else "offline"
        try:
            await client.publish(topic, payload, retain=True)
        except Exception as e:
            log.warning("发布在线状态失败 %s: %s", topic, e)
            return
        prev = self._was_online.get(device_id)
        if online and prev is False:
            self._seen.pop(device_id, None)
            self.forget_device(device_id)
            self._suppressed.discard(device_id)
            self._was_online[device_id] = True
            log.info("设备重新在线 id=%s，重新 Discovery", device_id)
            snap = self._last_snap.get(device_id) or {}
            await self._ensure_discovery(client, device_id, snap, force=True)
            return
        elif (not online) and prev is not False:
            log.info("设备离线 id=%s → %s", device_id, topic)
        self._was_online[device_id] = online
        # 离线时不重发 Discovery，避免 HA 里删了实体又被加回来

    async def publish_snapshot(self, device_id: str, snap: dict[str, Any]) -> None:
        """CoAP 快照写入 MQTT，供 HA 温度/湿度实体使用（固件本身不 MQTT 上报温湿度）。"""
        client = self._client
        if not client or not isinstance(snap, dict):
            return
        device_id = (device_id or "").strip().lower()
        self._last_snap[device_id] = snap
        topic = f"{config.MQTT_PREFIX}/{device_id}/snapshot"
        try:
            await client.publish(topic, json.dumps(snap, ensure_ascii=False), retain=True)
        except Exception as e:
            log.warning("发布 snapshot 失败 %s: %s", topic, e)
            return
        await self._ensure_discovery(client, device_id, snap, numeric_topic=topic)

    def forget_device(self, device_id: str) -> None:
        """清内存登记。"""
        device_id = (device_id or "").strip().lower()
        if not device_id:
            return
        drop = [k for k in self._discovered if k == device_id or k.startswith(f"{device_id}_")]
        for k in drop:
            self._discovered.discard(k)
        self._seen.pop(device_id, None)

    def suppress_device(self, device_id: str) -> None:
        """HA 删除实体/设备：停止自动 Discovery，直到设备重新 online。"""
        device_id = (device_id or "").strip().lower()
        if not device_id:
            return
        self.forget_device(device_id)
        self._suppressed.add(device_id)
        log.info("已抑制自动 Discovery id=%s（设备重新上线后才会再登记）", device_id)

    def _on_ha_removed_config(self, topic: str) -> None:
        parts = topic.split("/")
        if len(parts) < 4:
            return
        obj = parts[2]
        if not obj.startswith("mcp_"):
            return
        rest = obj[4:]
        did = rest
        for suffix in ("_last", "_evt", "_online", "_temp", "_humidity", "_co2", "_light", "_sound"):
            if rest.endswith(suffix):
                did = rest[: -len(suffix)]
                break
        else:
            if "_" in rest:
                did = rest.rsplit("_", 1)[0]
        self.suppress_device(did)
        log.info("HA 已删除 MQTT 配置 %s → 抑制自动登记 id=%s", obj, did)

    async def _rediscover_all(self, client: Any) -> None:
        self._discovered.clear()
        for rec in self.registry.list_all():
            if rec.id in self._suppressed:
                continue
            snap = self._last_snap.get(rec.id) or {}
            await self._ensure_discovery(client, rec.id, snap, force=True)

    def _remember(self, device_id: str, fp: str) -> bool:
        bucket = self._seen.setdefault(device_id, {})
        if fp in bucket:
            return False
        bucket[fp] = None
        while len(bucket) > 80:
            bucket.pop(next(iter(bucket)))
        return True

    async def ingest_event(
        self, device_id: str, ev: dict[str, Any], source: str = "", client: Any = None
    ) -> bool:
        """把一条设备事件写进缓存并推到 HA。"""
        client = client or self._client
        if not client or not isinstance(ev, dict):
            if isinstance(ev, dict):
                log.warning("MQTT 事件到达但 App 尚未连上 broker，丢弃 id=%s", device_id)
            return False
        device_id = (device_id or "").strip().lower()
        if not device_id:
            return False
        if self._was_online.get(device_id) is False:
            self._seen.pop(device_id, None)
            self._was_online[device_id] = True
            log.info("MQTT 事件表明设备已重连 id=%s，清空去重", device_id)
        norm = _normalize_event(ev)
        fp_a = _event_fp(ev)
        fp_b = _event_fp(norm)
        if not self._remember(device_id, fp_a):
            log.debug("重复事件已忽略 id=%s fp=%s", device_id, fp_a[:80])
            return False
        if fp_b != fp_a:
            self._remember(device_id, fp_b)
        self.registry.append_event(device_id, norm)
        await self._ensure_discovery(client, device_id, norm)
        topic_ha = f"{config.MQTT_PREFIX}/{device_id}/ha_event"
        stamped = dict(norm)
        stamped["received_at"] = time.strftime("%H:%M:%S")
        stamped["ha_seq"] = self._mqtt_event_n + 1
        raw = json.dumps(stamped, ensure_ascii=False)
        try:
            await client.publish(topic_ha, raw, retain=True)
        except Exception as e:
            log.warning("Discovery 状态发布失败 %s: %s", topic_ha, e)
            return False
        self._mqtt_event_n += 1
        log.info(
            "设备事件 → HA Discovery via=%s id=%s n=%s type=%s %s",
            source or "mqtt",
            device_id,
            self._mqtt_event_n,
            norm.get("type") or norm.get("event_type"),
            (norm.get("message") or "")[:80],
        )
        public = f"{device_id}{config.TOOL_SEP}sensor://events"
        await self.sessions.publish_resource_updated(public)
        return True

    async def ingest_events(self, device_id: str, events: list[dict[str, Any]], source: str) -> int:
        n = 0
        for ev in events:
            if await self.ingest_event(device_id, ev, source=source):
                n += 1
        return n

    async def _pub_config(self, client: Any, topic: str, body: dict[str, Any]) -> None:
        try:
            await client.publish(topic, json.dumps(body, ensure_ascii=False), retain=True)
        except Exception as e:
            log.warning("Discovery 发布失败 %s: %s", topic, e)
