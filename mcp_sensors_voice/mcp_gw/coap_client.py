"""CoAP 客户端：脚本 → 设备 UDP 5683。与固件 coap_hub.h 资源表一致。

方向：本进程发 GET/PUT，等应答。禁止用 MQTT/WS 做快照或改配置。
常用 path：/id /sensors/snapshot /sensors/thresholds /sensors/config /sensors/events
"""

from __future__ import annotations

import asyncio
import json
import socket
import struct
from typing import Any

_GET = 1
_PUT = 3


def _encode_path(path: str) -> bytes:
    opts = b""
    first = True
    for seg in path.strip("/").split("/"):
        if not seg:
            continue
        raw = seg.encode("utf-8")
        if len(raw) > 12:
            raise ValueError("path segment too long")
        delta = 11 if first else 0
        first = False
        opts += bytes([(delta << 4) | len(raw)]) + raw
    return opts


def _build(code: int, path: str, payload: bytes | None, mid: int) -> bytes:
    token = b"\x01"
    hdr = bytes([(1 << 6) | (0 << 4) | 1, code]) + struct.pack("!H", mid) + token
    opts = _encode_path(path)
    if payload:
        # Content-Format 50 = application/json（option 12，相对 Uri-Path 11 的 delta=1）
        opts += bytes([(1 << 4) | 1, 50])
        return hdr + opts + b"\xff" + payload
    return hdr + opts


def _parse_payload(data: bytes) -> bytes:
    if len(data) < 4:
        return b""
    tkl = data[0] & 0x0F
    pos = 4 + tkl
    while pos < len(data):
        if data[pos] == 0xFF:
            return data[pos + 1 :]
        nibble = data[pos]
        pos += 1
        delta = (nibble >> 4) & 0x0F
        olen = nibble & 0x0F
        if delta == 13:
            pos += 1
        elif delta == 14:
            pos += 2
        if olen == 13:
            olen = 13 + data[pos]
            pos += 1
        elif olen == 14:
            olen = 269 + (data[pos] << 8) + data[pos + 1]
            pos += 2
        pos += olen
    return b""


class DeviceCoapClient:
    def __init__(self, timeout_s: float = 5.0) -> None:
        self._timeout = timeout_s
        self._mid = 1

    async def get_json(self, host: str, path: str, port: int = 5683) -> Any:
        raw = await self._rpc(host, port, _GET, path, None)
        if not raw:
            return None
        return json.loads(raw.decode("utf-8"))

    async def put_json(self, host: str, path: str, obj: Any, port: int = 5683) -> Any:
        blob = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        raw = await self._rpc(host, port, _PUT, path, blob)
        if not raw:
            return {"ok": True}
        try:
            return json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError:
            return {"raw": raw.decode("utf-8", "replace")}

    async def _rpc(self, host: str, port: int, code: int, path: str, payload: bytes | None) -> bytes:
        self._mid = (self._mid + 1) & 0xFFFF or 1
        pkt = _build(code, path, payload, self._mid)
        timeout = self._timeout

        def _sync() -> bytes:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.settimeout(timeout)
            try:
                s.sendto(pkt, (host, port))
                data, _addr = s.recvfrom(2048)
                return _parse_payload(data)
            finally:
                s.close()

        return await asyncio.to_thread(_sync)
