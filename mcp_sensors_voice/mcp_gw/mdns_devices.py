"""浏览设备 mDNS（全自动，不填 IP）。

zeroconf 回调在后台线程，必须用 start() 时保存的 loop + run_coroutine_threadsafe。
不能 asyncio.get_running_loop()，否则发现结果会被静默丢掉，
coap_poll 就会一直打「mDNS 尚未发现枢纽」。

除 `_mcp-sensors._udp/tcp` 外，也听 `_http._tcp`（名称含 mcp-sensors），
部分固件只挂了 HTTP 服务名 + hostname。
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from .device_mqtt import push_if_missing
from .mdns_advertise import make_async_zeroconf
from .registry import DeviceRegistry

log = logging.getLogger("mcp_gw.mdns_dev")

SERVICES = (
    "_mcp-sensors._udp.local.",
    "_mcp-sensors._tcp.local.",
    "_http._tcp.local.",
)


def _ipv4(addrs: list[str]) -> str:
    for a in addrs:
        if ":" not in a:
            return a
    return addrs[0] if addrs else ""


def _id_from_name(name: str) -> str:
    # mcp-sensors-f20e08._http._tcp.local. → f20e08
    base = (name.split(".")[0] or "").strip().lower()
    if base.startswith("mcp-sensors-"):
        return base[len("mcp-sensors-") :]
    if base.startswith("mcp-sensors"):
        return base.replace("mcp-sensors", "").strip("-_") or base
    return base


class MdnsDeviceBrowser:
    def __init__(self, registry: DeviceRegistry) -> None:
        self.registry = registry
        self._azc: Any = None
        self._browser: Any = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self.enabled = False
        self.last_error = ""
        self.discover_count = 0
        self.last_name = ""
        self.last_at = 0.0

    async def start(self) -> None:
        try:
            from zeroconf import ServiceStateChange
            from zeroconf.asyncio import AsyncServiceBrowser
        except ImportError:
            self.last_error = "zeroconf not installed"
            log.warning("%s; skip device browse", self.last_error)
            return

        self._loop = asyncio.get_running_loop()
        try:
            self._azc = make_async_zeroconf()
        except Exception as e:
            self.last_error = str(e)
            log.exception("mDNS browser AsyncZeroconf failed")
            return

        def on_change(*_args: Any, **kwargs: Any) -> None:
            state_change = kwargs.get("state_change")
            service_type = kwargs.get("service_type") or ""
            name = kwargs.get("name") or ""
            if len(_args) >= 4:
                state_change = _args[3]
                service_type = str(_args[1] or "")
                name = str(_args[2] or "")
            if state_change not in (ServiceStateChange.Added, ServiceStateChange.Updated):
                return
            if not service_type or not name or not self._loop:
                return
            # _http._tcp 噪声大：只收 mcp-sensors*
            if "_http._tcp" in service_type and "mcp-sensors" not in name.lower():
                return
            log.info("mDNS 发现 %s (%s)", name, service_type)
            asyncio.run_coroutine_threadsafe(
                self._resolve(str(service_type), str(name)), self._loop
            )

        self._browser = AsyncServiceBrowser(
            self._azc.zeroconf,
            list(SERVICES),
            handlers=[on_change],
        )
        self.enabled = True
        self.last_error = ""
        log.info("browsing %s (All/IPv4)", ", ".join(SERVICES))

    async def stop(self) -> None:
        if self._browser:
            try:
                await self._browser.async_cancel()
            except Exception:
                pass
            self._browser = None
        if self._azc:
            try:
                await self._azc.async_close()
            except Exception:
                pass
            self._azc = None
        self.enabled = False

    async def _resolve(self, service_type: str, name: str) -> None:
        if not self._azc:
            return
        try:
            from zeroconf.asyncio import AsyncServiceInfo
        except ImportError:
            return
        info = AsyncServiceInfo(service_type, name)
        await info.async_request(self._azc.zeroconf, 4000)
        addrs = list(info.parsed_addresses() or [])
        ip = _ipv4(addrs)
        if not ip:
            log.warning("mDNS %s 无 IPv4 地址", name)
            return
        props: dict[str, str] = {}
        for k, v in (info.properties or {}).items():
            key = k.decode() if isinstance(k, bytes) else str(k)
            val = v.decode() if isinstance(v, bytes) else ("" if v is None else str(v))
            props[key] = val
        did = str(props.get("id") or props.get("device_id") or "").strip().lower()
        if not did:
            did = _id_from_name(name)
        if not did:
            log.warning("mDNS %s 无法得到设备 id TXT=%s", name, props)
            return
        port = int(info.port or 5683)
        if "udp" not in service_type and port in (80, 443):
            port = 5683
        disp = str(props.get("name") or did)
        self.registry.upsert(
            device_id=did,
            base_url=f"http://{ip}",
            name=disp,
            fw=f"{ip}:{port}",
            coap_host=ip,
            coap_port=port,
        )
        self.discover_count += 1
        self.last_name = name
        self.last_at = time.time()
        log.info("mDNS hub id=%s coap=%s:%s http://%s/", did, ip, port, ip)
        asyncio.create_task(push_if_missing(ip), name=f"mqtt-push-{did}")

    def status(self) -> dict:
        return {
            "enabled": self.enabled,
            "services": [s.rstrip(".") for s in SERVICES],
            "discover_count": self.discover_count,
            "last_name": self.last_name,
            "last_at": self.last_at or None,
            "last_error": self.last_error,
        }
