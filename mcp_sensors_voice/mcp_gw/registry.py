from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class DeviceRecord:
    id: str
    base_url: str
    name: str = ""
    fw: str = ""
    coap_host: str = ""
    coap_port: int = 5683
    last_seen: float = field(default_factory=time.time)
    online: bool = True
    last_error: str = ""
    event_cache: list[dict[str, Any]] = field(default_factory=list)

    def coap_endpoint(self) -> tuple[str, int]:
        if self.coap_host:
            return self.coap_host, int(self.coap_port or 5683)
        host = self.base_url.replace("http://", "").replace("https://", "").split("/")[0]
        host = host.split(":")[0]
        return host, int(self.coap_port or 5683)

    def has_coap(self) -> bool:
        host = (self.coap_host or "").strip()
        return bool(host) and host not in ("0.0.0.0", "::")

    def touch(self) -> None:
        self.last_seen = time.time()
        self.online = True


class DeviceRegistry:
    """线程安全的设备注册表。"""

    def __init__(self, stale_s: float = 90.0) -> None:
        self._stale_s = stale_s
        self._lock = threading.RLock()
        self._devices: dict[str, DeviceRecord] = {}

    def upsert(
        self,
        *,
        device_id: str,
        base_url: str,
        name: str = "",
        fw: str = "",
        coap_host: str = "",
        coap_port: int = 5683,
    ) -> DeviceRecord:
        device_id = (device_id or "").strip().lower()
        if not device_id:
            raise ValueError("id required")
        base_url = (base_url or "").strip().rstrip("/")
        if not base_url.startswith("http://") and not base_url.startswith("https://"):
            raise ValueError("base_url must be http(s) URL")
        host = (coap_host or "").strip()
        port = int(coap_port or 5683)

        with self._lock:
            rec = self._devices.get(device_id)
            if rec is None:
                rec = DeviceRecord(
                    id=device_id,
                    base_url=base_url,
                    name=(name or "").strip(),
                    fw=(fw or "").strip(),
                    coap_host=host,
                    coap_port=port,
                )
                self._devices[device_id] = rec
            else:
                rec.base_url = base_url
                if host:
                    rec.coap_host = host
                rec.coap_port = port
                if name:
                    rec.name = name.strip()
                if fw:
                    rec.fw = fw.strip()
            rec.touch()
            rec.last_error = ""
            return rec

    def ensure_mqtt_device(self, device_id: str) -> DeviceRecord:
        """叶子可无 mDNS：仅凭 MQTT topic 进表。无 CoAP 地址则不能 GET/PUT。"""
        rec = self.get(device_id)
        if rec is not None:
            rec.touch()
            return rec
        return self.upsert(
            device_id=device_id,
            base_url="http://0.0.0.0",
            name=device_id,
            coap_host="",
            coap_port=5683,
        )

    def append_event(self, device_id: str, event: dict[str, Any], max_keep: int = 64) -> None:
        with self._lock:
            rec = self._devices.get((device_id or "").strip().lower())
            if not rec:
                return
            rec.event_cache.append(event)
            if len(rec.event_cache) > max_keep:
                rec.event_cache = rec.event_cache[-max_keep:]
            rec.last_seen = time.time()
            if not rec.has_coap():
                rec.online = True

    def remove(self, device_id: str) -> bool:
        with self._lock:
            return self._devices.pop((device_id or "").strip().lower(), None) is not None

    def get(self, device_id: str) -> Optional[DeviceRecord]:
        with self._lock:
            return self._devices.get((device_id or "").strip().lower())

    def list_all(self) -> list[DeviceRecord]:
        with self._lock:
            return list(self._devices.values())

    def list_online(self) -> list[DeviceRecord]:
        now = time.time()
        with self._lock:
            out: list[DeviceRecord] = []
            for rec in self._devices.values():
                if rec.has_coap():
                    if rec.online and (now - rec.last_seen) <= self._stale_s:
                        out.append(rec)
                    elif (now - rec.last_seen) > self._stale_s:
                        rec.online = False
                elif rec.online:
                    out.append(rec)
            return out

    def mark_error(self, device_id: str, error: str) -> None:
        with self._lock:
            rec = self._devices.get((device_id or "").strip().lower())
            if not rec:
                return
            rec.last_error = error
            rec.online = False

    def to_public(self, rec: DeviceRecord) -> dict[str, Any]:
        now = time.time()
        stale = (now - rec.last_seen) > self._stale_s
        return {
            "id": rec.id,
            "name": rec.name or rec.id,
            "host": rec.coap_host or rec.base_url,
            "coap": f"{rec.coap_host}:{rec.coap_port}" if rec.coap_host else "",
            "fw": rec.fw,
            "online": rec.online if not rec.has_coap() else (rec.online and not stale),
            "last_seen": rec.last_seen,
            "last_error": rec.last_error,
            "has_coap": rec.has_coap(),
        }
