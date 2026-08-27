"""mDNS 收不到时，自动在本网段探测 HTTP /api/data（不填 IP）。

多设备注意：
- 必须用 JSON 里的 id（或 MAC 后 6 位），不能用 IP 尾号（会撞号）。
- ARP 找到一台不能停，还要把网段扫完。
- 已有枢纽时仍周期性补扫，否则后上电的设备进不来。
"""

from __future__ import annotations

import asyncio
import logging
import socket
from pathlib import Path
from typing import Any

import httpx

from . import config
from .registry import DeviceRegistry

log = logging.getLogger("mcp_gw.lan")


def _local_prefixes() -> list[str]:
    prefixes: list[str] = []
    adv = (config.ADVERTISE_IP or "").strip()
    if adv.count(".") == 3:
        prefixes.append(".".join(adv.split(".")[:3]))
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        if ip.count(".") == 3 and not ip.startswith("127."):
            p = ".".join(ip.split(".")[:3])
            if p not in prefixes:
                prefixes.append(p)
    except OSError:
        pass
    return prefixes or ["192.168.1"]


def _arp_ips() -> dict[str, str]:
    out: dict[str, str] = {}
    path = Path("/proc/net/arp")
    if not path.is_file():
        return out
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines()[1:]:
        parts = line.split()
        if len(parts) < 4:
            continue
        ip, mac = parts[0], parts[3]
        if mac == "00:00:00:00:00:00":
            continue
        out[ip] = mac
    return out


def _skip_ip(ip: str) -> bool:
    if ip in {config.ADVERTISE_IP.strip(), "127.0.0.1", "0.0.0.0"}:
        return True
    if ip.startswith(("172.17.", "172.18.", "172.19.", "172.30.", "192.168.122.", "10.8.")):
        return True
    return False


def _id_from_mac(mac: str) -> str:
    hexmac = mac.replace(":", "").replace("-", "").lower()
    if len(hexmac) >= 6:
        return hexmac[-6:]
    return ""


async def _wifi_id(ip: str) -> str:
    url = f"http://{ip}/api/wifi"
    try:
        async with httpx.AsyncClient(timeout=0.7) as client:
            r = await client.get(url)
            r.raise_for_status()
            data = r.json()
        if isinstance(data, dict):
            return str(data.get("id") or data.get("device_id") or "").strip().lower()
    except Exception:
        pass
    return ""


async def _probe(ip: str, mac: str) -> tuple[str, str, dict[str, Any]] | None:
    url = f"http://{ip}/api/data"
    try:
        async with httpx.AsyncClient(timeout=0.7) as client:
            r = await client.get(url)
            r.raise_for_status()
            data = r.json()
    except Exception:
        return None
    if not isinstance(data, dict):
        return None
    if not any(k in data for k in ("temp", "temperature", "humidity", "co2", "light", "lux", "sound")):
        return None
    did = str(data.get("id") or data.get("device_id") or "").strip().lower()
    if not did:
        did = await _wifi_id(ip)
    if not did:
        did = _id_from_mac(mac)
    if not did:
        log.warning("LAN %s 有环境数据但无设备 id，跳过（多设备不能用 IP 尾号）", ip)
        return None
    return did, ip, data


async def bind_missing_hosts(registry: DeviceRegistry) -> int:
    """给仅 MQTT 进表、尚无局域网 IP 的设备补上地址（否则拉不到温湿度快照）。

    优先解析 mcp-sensors-<id>.local，再快速扫网段里未登记的 IP。
    """
    missing = [r for r in registry.list_all() if not r.has_coap()]
    if not missing:
        return 0
    bound = 0
    for rec in missing:
        host = await _resolve_hostname(f"mcp-sensors-{rec.id}.local")
        if not host:
            host = await _resolve_hostname(f"{rec.id}.local")
        if not host or _skip_ip(host):
            continue
        hit = await _probe(host, "")
        if not hit:
            continue
        did, hip, _data = hit
        if did and did != rec.id:
            log.warning("主机名解析到 id=%s 但期望 %s，跳过 %s", did, rec.id, hip)
            continue
        registry.upsert(
            device_id=rec.id,
            base_url=f"http://{hip}",
            name=rec.name or rec.id,
            fw=hip,
            coap_host=hip,
            coap_port=5683,
        )
        bound += 1
        log.info("补绑局域网 IP id=%s ip=%s（此前仅有 MQTT 事件）", rec.id, hip)
        from .device_mqtt import push_if_missing

        asyncio.create_task(push_if_missing(hip), name=f"mqtt-push-bind-{rec.id}")

    still = [r for r in registry.list_all() if not r.has_coap()]
    if still:
        # 主机名失败时扫一段，专门回填缺 IP 的设备
        n = await scan_http_hubs(registry)
        bound += n
    return bound


async def _resolve_hostname(name: str) -> str:
    try:
        loop = asyncio.get_running_loop()
        infos = await loop.getaddrinfo(name, 80, type=socket.SOCK_STREAM)
    except Exception:
        return ""
    for info in infos:
        ip = info[4][0]
        if ip and ":" not in ip and not ip.startswith("127."):
            return ip
    return ""


async def scan_http_hubs(registry: DeviceRegistry) -> int:
    arp = _arp_ips()
    prefixes = _local_prefixes()
    skip = {config.ADVERTISE_IP.strip(), "127.0.0.1"}
    known_ip = {
        (r.coap_host or "").strip()
        for r in registry.list_all()
        if (r.coap_host or "").strip()
    }
    candidates: list[tuple[str, str]] = []
    seen: set[str] = set()
    for ip, mac in arp.items():
        if ip in skip or ip in seen or _skip_ip(ip):
            continue
        seen.add(ip)
        candidates.append((ip, mac))
    for pref in prefixes:
        for last in range(1, 255):
            ip = f"{pref}.{last}"
            if ip in skip or ip in seen or _skip_ip(ip):
                continue
            seen.add(ip)
            candidates.append((ip, ""))

    sem = asyncio.Semaphore(24)
    found = 0
    lock = asyncio.Lock()

    async def one(ip: str, mac: str) -> None:
        nonlocal found
        if ip in known_ip:
            return
        async with sem:
            hit = await _probe(ip, mac)
        if not hit:
            return
        did, hip, _data = hit
        existing = registry.get(did)
        if existing and (existing.coap_host or "").strip() == hip:
            return
        registry.upsert(
            device_id=did,
            base_url=f"http://{hip}",
            name=did,
            fw=hip,
            coap_host=hip,
            coap_port=5683,
        )
        async with lock:
            found += 1
        log.info("LAN 发现枢纽 id=%s ip=%s", did, hip)
        # 与 mDNS 发现对齐：登记后立刻推 MQTT + 语音 ws（不等快照轮询）
        from .device_mqtt import push_if_missing

        asyncio.create_task(push_if_missing(hip), name=f"mqtt-push-lan-{did}")

    arp_jobs = [one(ip, mac) for ip, mac in candidates if mac]
    if arp_jobs:
        await asyncio.gather(*arp_jobs)
    rest = [one(ip, mac) for ip, mac in candidates if not mac]
    if rest:
        log.info("LAN 扫描 %s.1-254 /api/data（多设备不会在 ARP 命中后停）", prefixes[0] if prefixes else "?")
        await asyncio.gather(*rest)
    return found




