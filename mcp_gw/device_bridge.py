from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from typing import Any, Optional

import httpx

from . import config
from .device_client import delete_device_session
from .registry import DeviceRegistry
from .sessions import SessionManager

log = logging.getLogger("mcp_gw.bridge")


def split_public_uri(public_uri: str) -> tuple[str, str]:
    if config.TOOL_SEP not in public_uri:
        raise ValueError(f"bad public uri: {public_uri}")
    device_id, native = public_uri.split(config.TOOL_SEP, 1)
    return device_id.strip().lower(), native


class _DeviceWatcher:
    """维持与单台设备的 MCP 会话：subscribe + GET SSE（失败则轮询）。"""

    def __init__(
        self,
        *,
        device_id: str,
        registry: DeviceRegistry,
        sessions: SessionManager,
    ) -> None:
        self.device_id = device_id
        self.registry = registry
        self.sessions = sessions
        self.native_uris: set[str] = set()
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()
        self._device_session: Optional[str] = None
        self._wake = asyncio.Event()
        self._hashes: dict[str, str] = {}

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._stop.clear()
            self._task = asyncio.create_task(self._run(), name=f"dev-watch-{self.device_id}")

    async def stop(self) -> None:
        self._stop.set()
        self._wake.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    def add_uri(self, native_uri: str) -> None:
        if native_uri in self.native_uris:
            return
        self.native_uris.add(native_uri)
        self._wake.set()

    def remove_uri(self, native_uri: str) -> None:
        self.native_uris.discard(native_uri)
        self._hashes.pop(native_uri, None)
        self._wake.set()

    async def _run(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            rec = self.registry.get(self.device_id)
            if rec is None or not self.native_uris:
                try:
                    await asyncio.wait_for(self._wake.wait(), timeout=2.0)
                except asyncio.TimeoutError:
                    pass
                self._wake.clear()
                continue
            try:
                await self._sse_loop(rec.mcp_url)
                backoff = 1.0
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("device SSE failed id=%s err=%s; fallback poll", self.device_id, e)
                try:
                    await self._poll_loop(rec.mcp_url, duration_s=max(8.0, backoff * 2))
                except asyncio.CancelledError:
                    raise
                except Exception as pe:
                    log.warning("device poll failed id=%s err=%s", self.device_id, pe)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)

    async def _initialize(self, client: httpx.AsyncClient, mcp_url: str) -> Optional[str]:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
        }
        body = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": config.PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": {"name": f"{config.SERVER_NAME}-bridge", "version": config.SERVER_VERSION},
            },
        }
        r = await client.post(mcp_url, json=body, headers=headers)
        r.raise_for_status()
        sid = r.headers.get("mcp-session-id") or r.headers.get("MCP-Session-Id")
        note_headers = dict(headers)
        if sid:
            note_headers["MCP-Session-Id"] = sid
        try:
            await client.post(
                mcp_url,
                json={"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}},
                headers=note_headers,
            )
        except Exception:
            pass
        return sid

    async def _rpc(
        self,
        client: httpx.AsyncClient,
        mcp_url: str,
        method: str,
        params: dict[str, Any],
        *,
        session_id: Optional[str],
        req_id: int,
    ) -> Any:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
        }
        if session_id:
            headers["MCP-Session-Id"] = session_id
        r = await client.post(
            mcp_url,
            json={"jsonrpc": "2.0", "id": req_id, "method": method, "params": params},
            headers=headers,
        )
        r.raise_for_status()
        data = r.json() if r.content else {}
        if isinstance(data, dict) and data.get("error"):
            raise RuntimeError(f"{method} error: {data['error']}")
        return data.get("result") if isinstance(data, dict) else None

    async def _subscribe_all(self, client: httpx.AsyncClient, mcp_url: str, session_id: Optional[str]) -> None:
        req_id = 10
        for uri in list(self.native_uris):
            try:
                await self._rpc(
                    client,
                    mcp_url,
                    "resources/subscribe",
                    {"uri": uri},
                    session_id=session_id,
                    req_id=req_id,
                )
                req_id += 1
            except Exception as e:
                log.debug("subscribe %s on %s failed: %s", uri, self.device_id, e)

    async def _sse_loop(self, mcp_url: str) -> None:
        timeout = httpx.Timeout(connect=10.0, read=None, write=10.0, pool=10.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            sid: Optional[str] = None
            try:
                sid = await self._initialize(client, mcp_url)
                self._device_session = sid
                await self._subscribe_all(client, mcp_url, sid)

                headers = {"Accept": "text/event-stream"}
                if sid:
                    headers["MCP-Session-Id"] = sid
                log.info("device SSE open id=%s uris=%s", self.device_id, sorted(self.native_uris))
                async with client.stream("GET", mcp_url, headers=headers) as resp:
                    if resp.status_code == 405:
                        raise RuntimeError("device SSE 405")
                    resp.raise_for_status()
                    buf = ""
                    async for chunk in resp.aiter_text():
                        if self._stop.is_set():
                            return
                        if self._wake.is_set():
                            self._wake.clear()
                            await self._subscribe_all(client, mcp_url, sid)
                        buf += chunk
                        while "\n\n" in buf:
                            block, buf = buf.split("\n\n", 1)
                            await self._handle_sse_block(block)
            finally:
                await delete_device_session(mcp_url, sid, client)
                self._device_session = None

    async def _handle_sse_block(self, block: str) -> None:
        data_lines: list[str] = []
        for line in block.splitlines():
            if line.startswith("data:"):
                data_lines.append(line[5:].lstrip())
            elif line.startswith(":"):
                continue
        if not data_lines:
            return
        payload = "\n".join(data_lines).strip()
        if not payload or payload == "[DONE]":
            return
        try:
            msg = json.loads(payload)
        except json.JSONDecodeError:
            return
        if not isinstance(msg, dict):
            return
        method = msg.get("method")
        params = msg.get("params") or {}
        if method == "notifications/resources/updated":
            native = str(params.get("uri") or "")
            if not native:
                # 无 uri：通知该设备所有已订阅资源
                for nuri in list(self.native_uris):
                    public = f"{self.device_id}{config.TOOL_SEP}{nuri}"
                    await self.sessions.publish_resource_updated(public)
                return
            if native.startswith(f"{self.device_id}{config.TOOL_SEP}"):
                public = native
            else:
                public = f"{self.device_id}{config.TOOL_SEP}{native}"
            await self.sessions.publish_resource_updated(public)
        elif method == "notifications/resources/list_changed":
            await self.sessions.publish_resources_list_changed()

    async def _poll_loop(self, mcp_url: str, duration_s: float) -> None:
        """SSE 不可用时：定时 resources/read，内容变化则推送 updated。"""
        deadline = asyncio.get_event_loop().time() + duration_s
        timeout = httpx.Timeout(config.DEVICE_TIMEOUT_S)
        async with httpx.AsyncClient(timeout=timeout) as client:
            sid: Optional[str] = None
            try:
                sid = await self._initialize(client, mcp_url)
                self._device_session = sid
                while not self._stop.is_set() and asyncio.get_event_loop().time() < deadline:
                    for native in list(self.native_uris):
                        try:
                            result = await self._rpc(
                                client,
                                mcp_url,
                                "resources/read",
                                {"uri": native},
                                session_id=sid,
                                req_id=20,
                            )
                            text = json.dumps(result, ensure_ascii=False, sort_keys=True)
                            digest = hashlib.sha1(text.encode("utf-8")).hexdigest()
                            prev = self._hashes.get(native)
                            self._hashes[native] = digest
                            if prev is not None and prev != digest:
                                public = f"{self.device_id}{config.TOOL_SEP}{native}"
                                await self.sessions.publish_resource_updated(public)
                        except Exception as e:
                            log.debug("poll read %s/%s failed: %s", self.device_id, native, e)
                    try:
                        await asyncio.wait_for(self._stop.wait(), timeout=2.0)
                    except asyncio.TimeoutError:
                        pass
            finally:
                await delete_device_session(mcp_url, sid, client)
                self._device_session = None


class DeviceNotificationBridge:
    """按 Hermes 订阅按需拉起设备 watcher，并转发 notifications。"""

    def __init__(self, registry: DeviceRegistry, sessions: SessionManager) -> None:
        self.registry = registry
        self.sessions = sessions
        self._watchers: dict[str, _DeviceWatcher] = {}
        self._lock = asyncio.Lock()

    async def sync_from_sessions(self) -> None:
        needed: dict[str, set[str]] = {}
        for public in self.sessions.public_uris_needed():
            try:
                device_id, native = split_public_uri(public)
            except ValueError:
                continue
            needed.setdefault(device_id, set()).add(native)

        async with self._lock:
            for device_id, uris in needed.items():
                w = self._watchers.get(device_id)
                if w is None:
                    w = _DeviceWatcher(
                        device_id=device_id,
                        registry=self.registry,
                        sessions=self.sessions,
                    )
                    self._watchers[device_id] = w
                    w.start()
                for uri in uris:
                    w.add_uri(uri)
                # 去掉不再需要的
                for old in list(w.native_uris - uris):
                    w.remove_uri(old)

            for device_id in list(self._watchers.keys()):
                if device_id not in needed:
                    await self._watchers[device_id].stop()
                    del self._watchers[device_id]

    async def on_subscribe(self, public_uri: str) -> None:
        await self.sync_from_sessions()

    async def on_unsubscribe(self, public_uri: str) -> None:
        await self.sync_from_sessions()

    async def stop(self) -> None:
        async with self._lock:
            for w in list(self._watchers.values()):
                await w.stop()
            self._watchers.clear()
