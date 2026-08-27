"""网关自己的 mDNS 广播（不是设备 _mcp-sensors）。

_mcp-gw._tcp : MCP_GW_PORT
  TXT ws/mqtt/ver —— ws= 音频回退；mqtt= 事件 broker。不要塞传感器、不要 path/mcp。
_voice-ws._tcp : VOICE_WS_PORT
  设备优先 browse 此项拿 PCM 对端。仍是 WS 服务端在本机/HA，设备当客户端。

用 InterfaceChoice.All + IPv4，并周期性 update，避免交换机丢掉组播状态后设备再也听不见。
"""

from __future__ import annotations

import asyncio
import logging
import socket
from typing import Any

from . import config

log = logging.getLogger("mcp_gw.mdns")

SERVICE_TYPE = "_mcp-gw._tcp.local."
SERVICE_NAME = f"{config.SERVER_NAME}.{SERVICE_TYPE}"
VOICE_TYPE = "_voice-ws._tcp.local."
VOICE_NAME = f"mcp-sensors-voice.{VOICE_TYPE}"
REANNOUNCE_S = 45.0


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


def guess_lan_ip() -> str:
    forced = (config.ADVERTISE_IP or "").strip()
    if forced and not _is_loopback(forced):
        return forced
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        if ip and not _is_loopback(ip):
            return ip
    except Exception:
        pass
    finally:
        s.close()
    return ""


def make_async_zeroconf() -> Any:
    """Prefer all IPv4 interfaces — HAOS host_network 下默认接口常漏网卡。"""
    from zeroconf import IPVersion, InterfaceChoice
    from zeroconf.asyncio import AsyncZeroconf

    try:
        return AsyncZeroconf(interfaces=InterfaceChoice.All, ip_version=IPVersion.V4Only)
    except TypeError:
        try:
            return AsyncZeroconf(interfaces=InterfaceChoice.All)
        except TypeError:
            return AsyncZeroconf()


class MdnsAdvertiser:
    """asyncio 友好的 mDNS 广播（配合 uvicorn）。"""

    def __init__(self) -> None:
        self._azc: Any = None
        self._info: Any = None
        self._voice_info: Any = None
        self._reannounce_task: asyncio.Task[Any] | None = None
        self.ip = ""
        self.mqtt_uri = ""
        self.voice_ws = ""
        self.enabled = False
        self.last_error = ""
        self.reannounce_count = 0

    async def start(self) -> bool:
        if not config.MDNS_ENABLE:
            log.info("mDNS advertise disabled")
            return False
        try:
            from zeroconf import ServiceInfo
        except ImportError:
            self.last_error = "zeroconf not installed"
            log.warning("%s; skip mDNS advertise", self.last_error)
            return False

        self.ip = guess_lan_ip()
        if not self.ip:
            self.last_error = "no LAN IP for _mcp-gw (set MCP_GW_ADVERTISE_IP / advertise_ip)"
            log.error("%s", self.last_error)
            return False
        voice_ws = f"ws://{self.ip}:{config.VOICE_WS_PORT}{config.VOICE_WS_PATH}"
        self.voice_ws = voice_ws
        raw_mqtt = (config.MQTT_HOST or "").strip()
        mqtt_uri = ""
        if raw_mqtt or self.ip:
            host = self.ip if (not raw_mqtt or _is_loopback(raw_mqtt)) else raw_mqtt
            if host.startswith("mqtt://"):
                host = host[7:].split("/")[0].split(":")[0]
            if _is_loopback(host):
                log.error("refuse mqtt= 127.0.0.1; set advertise_ip to HA/网关局域网 IP")
            else:
                mqtt_uri = f"mqtt://{host}:{int(config.MQTT_PORT or 1883)}"
        self.mqtt_uri = mqtt_uri
        props = {
            b"ws": voice_ws.encode("utf-8"),
            b"ver": b"1",
        }
        if mqtt_uri:
            props[b"mqtt"] = mqtt_uri.encode("utf-8")
            user = (config.MQTT_USER or "").strip()
            pw = (config.MQTT_PASSWORD or "").strip()
            if user:
                props[b"mqtt_user"] = user.encode("utf-8")
            if pw:
                props[b"mqtt_pass"] = pw.encode("utf-8")
        try:
            info = ServiceInfo(
                SERVICE_TYPE,
                SERVICE_NAME,
                addresses=[socket.inet_aton(self.ip)],
                port=int(config.PORT),
                properties=props,
                server=f"{config.SERVER_NAME}.local.",
                host_ttl=120,
                other_ttl=120,
            )
            azc = make_async_zeroconf()
            await azc.async_register_service(info)
            # 设备优先 browse _voice-ws；TXT 可空，端口即 WS。禁止在此塞传感器 JSON。
            voice_info = ServiceInfo(
                VOICE_TYPE,
                VOICE_NAME,
                addresses=[socket.inet_aton(self.ip)],
                port=int(config.VOICE_WS_PORT),
                properties={
                    b"ws": voice_ws.encode("utf-8"),
                    b"path": config.VOICE_WS_PATH.encode("utf-8"),
                },
                server=f"{config.SERVER_NAME}-voice.local.",
                host_ttl=120,
                other_ttl=120,
            )
            await azc.async_register_service(voice_info)
            self._azc = azc
            self._info = info
            self._voice_info = voice_info
            self.enabled = True
            self.last_error = ""
            self._reannounce_task = asyncio.create_task(self._reannounce_loop(), name="mdns-reannounce")
            log.info(
                "mDNS advertised %s and _voice-ws at %s voice=%s mqtt=%s (All/IPv4 + reannounce %.0fs)",
                SERVICE_NAME,
                self.ip,
                voice_ws,
                mqtt_uri or "-",
                REANNOUNCE_S,
            )
            return True
        except Exception as e:
            self.last_error = str(e)
            log.exception("mDNS advertise failed")
            await self.stop()
            return False

    async def _reannounce_loop(self) -> None:
        while True:
            await asyncio.sleep(REANNOUNCE_S)
            if not self._azc or not self._info:
                continue
            try:
                await self._azc.async_update_service(self._info)
                if self._voice_info:
                    await self._azc.async_update_service(self._voice_info)
                self.reannounce_count += 1
                log.debug("mDNS reannounce #%s ip=%s", self.reannounce_count, self.ip)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("mDNS reannounce failed: %s", e)

    async def stop(self) -> None:
        if self._reannounce_task:
            self._reannounce_task.cancel()
            try:
                await self._reannounce_task
            except asyncio.CancelledError:
                pass
            self._reannounce_task = None
        if self._azc and self._info:
            try:
                await self._azc.async_unregister_service(self._info)
            except Exception:
                pass
        if self._azc and self._voice_info:
            try:
                await self._azc.async_unregister_service(self._voice_info)
            except Exception:
                pass
        self._voice_info = None
        if self._azc:
            try:
                await self._azc.async_close()
            except Exception:
                pass
        self._azc = None
        self._info = None
        self.enabled = False

    def status(self) -> dict:
        voice = self.voice_ws or (
            f"ws://{self.ip}:{config.VOICE_WS_PORT}{config.VOICE_WS_PATH}" if self.ip else ""
        )
        return {
            "enabled": self.enabled,
            "service": SERVICE_TYPE.rstrip("."),
            "voice_service": VOICE_TYPE.rstrip("."),
            "name": config.SERVER_NAME,
            "ip": self.ip,
            "port": config.PORT,
            "voice_ws": voice,
            "mqtt": self.mqtt_uri,
            "reannounce_count": self.reannounce_count,
            "last_error": self.last_error,
        }
