from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any, Optional

import httpx

from . import config

log = logging.getLogger("mcp_gw.device")


async def delete_device_session(
    mcp_url: str,
    session_id: Optional[str],
    client: Optional[httpx.AsyncClient] = None,
) -> None:
    """按 Streamable HTTP 结束设备侧会话，否则 ESP 的 session 槽会占满。"""
    if not session_id:
        return
    headers = {"MCP-Session-Id": session_id}
    own = client is None
    http = client or httpx.AsyncClient(timeout=5.0)
    try:
        r = await http.request("DELETE", mcp_url, headers=headers)
        log.debug("DELETE MCP session %s -> %s", session_id, r.status_code)
    except Exception as e:
        log.debug("DELETE MCP session %s failed: %s", session_id, e)
    finally:
        if own:
            await http.aclose()


@dataclass
class _CachedSession:
    mcp_url: str
    client: httpx.AsyncClient
    session_id: Optional[str]
    last_used: float


class DeviceMcpClient:
    """对 ESP32 Streamable HTTP MCP 的轻量客户端。

    复用 initialize 会话，避免每次 resources/read 都重新握手导致间歇超时。
    """

    def __init__(self, timeout_s: float | None = None) -> None:
        self._timeout = timeout_s if timeout_s is not None else config.DEVICE_TIMEOUT_S
        self._cache: dict[str, _CachedSession] = {}
        self._lock = asyncio.Lock()
        # 0 = 一直复用，直到 RPC 失败。超时丢弃且不 DELETE 会把设备会话槽占满。
        self._session_ttl_s = 0.0

    async def list_tools(self, mcp_url: str) -> list[dict[str, Any]]:
        tools, _resources = await self.fetch_catalog(mcp_url)
        return tools

    async def list_resources(self, mcp_url: str) -> list[dict[str, Any]]:
        _tools, resources = await self.fetch_catalog(mcp_url)
        return resources

    async def fetch_catalog(self, mcp_url: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        async def _once() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
            sess = await self._session(mcp_url)
            tools_result = await self._rpc(sess, "tools/list", {}, req_id=2)
            resources: list[dict[str, Any]] = []
            try:
                res_result = await self._rpc(sess, "resources/list", {}, req_id=3)
                raw = (res_result or {}).get("resources") or []
                if isinstance(raw, list):
                    resources = [r for r in raw if isinstance(r, dict) and r.get("uri")]
            except Exception as e:
                log.debug("resources/list failed for %s: %s", mcp_url, e)
            tools_raw = (tools_result or {}).get("tools") or []
            tools = (
                [t for t in tools_raw if isinstance(t, dict) and t.get("name")]
                if isinstance(tools_raw, list)
                else []
            )
            return tools, resources

        try:
            return await _once()
        except Exception as e:
            if "session state alloc failed" in str(e):
                raise
            await self._invalidate(mcp_url)
            return await _once()

    async def call_tool(
        self,
        mcp_url: str,
        name: str,
        arguments: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        async def _once() -> dict[str, Any]:
            sess = await self._session(mcp_url)
            result = await self._rpc(
                sess,
                "tools/call",
                {"name": name, "arguments": arguments or {}},
                req_id=2,
            )
            if not isinstance(result, dict):
                return {"content": [{"type": "text", "text": str(result)}], "isError": True}
            return result

        try:
            return await _once()
        except Exception as e:
            if "session state alloc failed" in str(e):
                raise
            await self._invalidate(mcp_url)
            return await _once()

    async def read_resource(self, mcp_url: str, uri: str) -> dict[str, Any]:
        async def _once() -> dict[str, Any]:
            sess = await self._session(mcp_url)
            result = await self._rpc(sess, "resources/read", {"uri": uri}, req_id=2)
            if not isinstance(result, dict):
                return {"contents": [{"uri": uri, "mimeType": "text/plain", "text": str(result)}]}
            return result

        try:
            return await _once()
        except Exception as e:
            if "session state alloc failed" in str(e):
                raise
            await self._invalidate(mcp_url)
            return await _once()

    async def probe(self, mcp_url: str) -> tuple[bool, str]:
        try:
            tools, resources = await self.fetch_catalog(mcp_url)
            return True, f"ok tools={len(tools)} resources={len(resources)}"
        except Exception as e:
            return False, str(e)

    async def close(self) -> None:
        async with self._lock:
            sessions = list(self._cache.values())
            self._cache.clear()
        for sess in sessions:
            await self._drop(sess)

    async def _invalidate(self, mcp_url: str) -> None:
        async with self._lock:
            sess = self._cache.pop(mcp_url, None)
        if sess is not None:
            await self._drop(sess)
            log.info("invalidated MCP session for %s", mcp_url)

    async def _drop(self, sess: _CachedSession) -> None:
        await delete_device_session(sess.mcp_url, sess.session_id, sess.client)
        try:
            await sess.client.aclose()
        except Exception:
            pass

    def _cache_fresh(self, cached: _CachedSession, now: float) -> bool:
        if self._session_ttl_s <= 0:
            return True
        return (now - cached.last_used) < self._session_ttl_s

    async def _session(self, mcp_url: str) -> _CachedSession:
        async with self._lock:
            now = time.time()
            cached = self._cache.get(mcp_url)
            if cached is not None and self._cache_fresh(cached, now):
                cached.last_used = now
                return cached
            if cached is not None:
                self._cache.pop(mcp_url, None)
                await self._drop(cached)

            client = httpx.AsyncClient(timeout=self._timeout)
            try:
                session_id = await self._initialize(client, mcp_url)
            except Exception:
                await client.aclose()
                raise
            entry = _CachedSession(
                mcp_url=mcp_url, client=client, session_id=session_id, last_used=now
            )
            self._cache[mcp_url] = entry
            log.debug("opened MCP session for %s sid=%s", mcp_url, session_id)
            return entry

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
                "clientInfo": {"name": config.SERVER_NAME, "version": config.SERVER_VERSION},
            },
        }
        r = await client.post(mcp_url, json=body, headers=headers)
        r.raise_for_status()
        session_id = r.headers.get("mcp-session-id") or r.headers.get("MCP-Session-Id")
        data = self._parse_body(r)
        if isinstance(data, dict) and data.get("error"):
            await delete_device_session(mcp_url, session_id, client)
            raise RuntimeError(f"initialize error: {data['error']}")

        note_headers = dict(headers)
        if session_id:
            note_headers["MCP-Session-Id"] = session_id
        note = {
            "jsonrpc": "2.0",
            "method": "notifications/initialized",
            "params": {},
        }
        try:
            nr = await client.post(mcp_url, json=note, headers=note_headers)
            if nr.status_code >= 400:
                log.debug("initialized notify status=%s", nr.status_code)
        except Exception as e:
            log.debug("initialized notify failed: %s", e)
        return session_id

    async def _rpc(
        self,
        sess: _CachedSession,
        method: str,
        params: dict[str, Any],
        *,
        req_id: int,
    ) -> Any:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
        }
        if sess.session_id:
            headers["MCP-Session-Id"] = sess.session_id
        body = {"jsonrpc": "2.0", "id": req_id, "method": method, "params": params}
        r = await sess.client.post(sess.mcp_url, json=body, headers=headers)
        r.raise_for_status()
        data = self._parse_body(r)
        if not isinstance(data, dict):
            raise RuntimeError(f"bad RPC response for {method}")
        if data.get("error"):
            raise RuntimeError(f"{method} error: {data['error']}")
        sess.last_used = time.time()
        return data.get("result")

    @staticmethod
    def _parse_body(r: httpx.Response) -> Any:
        ctype = (r.headers.get("content-type") or "").lower()
        text = r.text or ""
        if not text.strip():
            return {}
        if "text/event-stream" in ctype or text.lstrip().startswith("event:"):
            import json

            last = None
            for line in text.splitlines():
                if line.startswith("data:"):
                    payload = line[5:].strip()
                    if payload and payload != "[DONE]":
                        try:
                            last = json.loads(payload)
                        except json.JSONDecodeError:
                            continue
            return last if last is not None else {}
        return r.json()
