"""对有 CoAP 的枢纽定时读快照；CoAP 失败则试 HTTP /api/data。全自动，不填 IP。"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

from . import config
from .coap_client import DeviceCoapClient
from .lan_discover import bind_missing_hosts, scan_http_hubs
from .mqtt_ha import HaMqttBridge
from .registry import DeviceRegistry

log = logging.getLogger("mcp_gw.coap_poll")


class CoapSnapshotPoller:
    def __init__(
        self,
        registry: DeviceRegistry,
        client: DeviceCoapClient,
        mqtt: HaMqttBridge,
    ) -> None:
        self.registry = registry
        self.client = client
        self.mqtt = mqtt
        self._task: asyncio.Task[Any] | None = None
        self._ok: set[str] = set()
        self._empty_n = 0
        self._last_scan = 0.0
        self._last_rescan = 0.0
        self._last_bind = 0.0

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="coap-snapshot")

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _http_data(self, host: str) -> dict[str, Any] | None:
        url = f"http://{host}/api/data"
        try:
            async with httpx.AsyncClient(timeout=2.0) as http:
                r = await http.get(url)
                r.raise_for_status()
                data = r.json()
            if isinstance(data, dict) and any(
                k in data for k in ("temp", "temperature", "humidity", "co2", "light", "lux", "sound")
            ):
                return data
            log.warning("HTTP %s 返回无环境字段: %s", url, list(data)[:12] if isinstance(data, dict) else type(data))
        except Exception as e:
            log.warning("HTTP 快照失败 %s: %s", url, e)
        return None

    async def _ensure_device_mqtt(self, rec: Any, host: str) -> None:
        """mDNS TXT 之外：单播 POST /api/wifi action=mqtt（含 ws= 语音地址）。"""
        from .device_mqtt import ensure_device_mqtt
        ok = await ensure_device_mqtt(host)
        if not ok:
            log.warning(
                "id=%s %s MQTT/语音告知失败：固件需支持 action=mqtt（含 ws=）；"
                "advertise_ip 填 HA 局域网 IP",
                rec.id,
                host,
            )

    async def _run(self) -> None:
        interval = max(3.0, float(config.COAP_POLL_S))
        log.info("快照轮询 %.1fs：先 HTTP /api/data，多设备并行", interval)
        await asyncio.sleep(2)
        while True:
            now = asyncio.get_running_loop().time()
            missing = [r for r in self.registry.list_all() if not r.has_coap()]
            # 仅 MQTT 进表、无 IP 的设备：优先主机名补绑，否则温湿度永远出不来
            if missing and now - self._last_bind > 15:
                self._last_bind = now
                n = await bind_missing_hosts(self.registry)
                if n:
                    log.info("为 %s 台无 IP 设备补绑/扫描成功", n)

            hubs = [r for r in self.registry.list_all() if r.has_coap()]
            if not hubs:
                self._empty_n += 1
                if now - self._last_scan > 45:
                    self._last_scan = now
                    log.info("registry 仍空，自动扫描局域网 HTTP /api/data（mDNS 组播可能未到）")
                    n = await scan_http_hubs(self.registry)
                    if n:
                        log.info("自动扫描登记 %s 台", n)
                    hubs = [r for r in self.registry.list_all() if r.has_coap()]
                if not hubs and (self._empty_n == 1 or self._empty_n % 4 == 0):
                    log.info(
                        "尚未发现枢纽。已听 mDNS _mcp-sensors；组播不通时会自动扫网段。"
                    )
            else:
                self._empty_n = 0
                # 仍有设备缺 IP 时加快补扫（否则要等 180s，第二台只有事件没有温湿度）
                rescan_every = 30.0 if missing else 180.0
                if now - self._last_rescan > rescan_every:
                    self._last_rescan = now
                    n = await scan_http_hubs(self.registry)
                    if n:
                        log.info("补扫局域网，新登记/补绑 %s 台", n)
                    hubs = [r for r in self.registry.list_all() if r.has_coap()]
            if hubs:
                await asyncio.gather(*(self._poll_one(rec) for rec in hubs))
            await asyncio.sleep(interval)

    async def _poll_one(self, rec: Any) -> None:
        host, port = rec.coap_endpoint()
        snap = await self._http_data(host)
        src = "http"
        if snap is None:
            try:
                got = await asyncio.wait_for(
                    self.client.get_json(host, "/sensors/snapshot", port),
                    timeout=2.0,
                )
                if isinstance(got, dict):
                    snap = got
                    src = "coap"
            except Exception as e:
                log.debug("CoAP snapshot 失败 id=%s %s:%s %s", rec.id, host, port, e)
        if not isinstance(snap, dict):
            rec.online = False
            rec.last_error = "CoAP/HTTP 均无快照"
            await self.mqtt.publish_liveness(rec.id, False)
            log.warning("读快照失败 id=%s %s → 标离线", rec.id, rec.last_error)
            return
        rec.touch()
        rec.last_error = ""
        rec.online = True
        await self.mqtt.publish_liveness(rec.id, True)
        await self.mqtt.publish_snapshot(rec.id, snap)
        await self._ensure_device_mqtt(rec, host)
        if rec.id not in self._ok:
            self._ok.add(rec.id)
            log.info(
                "快照成功 via=%s id=%s %s keys=%s",
                src,
                rec.id,
                host,
                list(snap)[:10],
            )
