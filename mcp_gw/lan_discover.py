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

    arp_jobs = [one(ip, mac) for ip, mac in candidates if mac]
    if arp_jobs:
        await asyncio.gather(*arp_jobs)
    rest = [one(ip, mac) for ip, mac in candidates if not mac]
    if rest:
        log.info("LAN 扫描 %s.1-254 /api/data（多设备不会在 ARP 命中后停）", prefixes[0] if prefixes else "?")
        await asyncio.gather(*rest)
    return found
