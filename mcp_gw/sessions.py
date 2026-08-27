from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Optional

log = logging.getLogger("mcp_gw.sessions")


@dataclass
class ClientSession:
    id: str
    queue: asyncio.Queue[dict[str, Any]] = field(default_factory=lambda: asyncio.Queue(maxsize=64))
    subscriptions: set[str] = field(default_factory=set)
    created_at: float = field(default_factory=time.time)
    sse_attached: bool = False


class SessionManager:
    """Hermes 侧 MCP 会话：订阅表 + SSE 出站队列。"""

    def __init__(self) -> None:
        self._sessions: dict[str, ClientSession] = {}
        self._lock = asyncio.Lock()

    async def ensure(self, session_id: str) -> ClientSession:
        async with self._lock:
            sess = self._sessions.get(session_id)
            if sess is None:
                sess = ClientSession(id=session_id)
                self._sessions[session_id] = sess
                log.info("session created id=%s", session_id)
            return sess

    async def get(self, session_id: str) -> Optional[ClientSession]:
        async with self._lock:
            return self._sessions.get(session_id)

    async def drop(self, session_id: str) -> None:
        async with self._lock:
            self._sessions.pop(session_id, None)
            log.info("session dropped id=%s", session_id)

    async def subscribe(self, session_id: str, public_uri: str) -> None:
        sess = await self.ensure(session_id)
        sess.subscriptions.add(public_uri)
        log.info("subscribe session=%s uri=%s (n=%d)", session_id, public_uri, len(sess.subscriptions))

    async def unsubscribe(self, session_id: str, public_uri: str) -> None:
        sess = await self.get(session_id)
        if sess is None:
            return
        sess.subscriptions.discard(public_uri)
        log.info("unsubscribe session=%s uri=%s", session_id, public_uri)

    def public_uris_needed(self) -> set[str]:
        out: set[str] = set()
        for sess in list(self._sessions.values()):
            out |= set(sess.subscriptions)
        return out

    async def publish_resource_updated(self, public_uri: str) -> int:
        """向订阅了该 URI 的会话推送 notifications/resources/updated。"""
        note = {
            "jsonrpc": "2.0",
            "method": "notifications/resources/updated",
            "params": {"uri": public_uri},
        }
        return await self._fanout(note, only_uri=public_uri)

    async def publish_resources_list_changed(self) -> int:
        note = {
            "jsonrpc": "2.0",
            "method": "notifications/resources/list_changed",
            "params": {},
        }
        return await self._fanout(note, only_uri=None)

    async def _fanout(self, note: dict[str, Any], *, only_uri: Optional[str]) -> int:
        sent = 0
        for sess in list(self._sessions.values()):
            if only_uri is not None and only_uri not in sess.subscriptions:
                continue
            # list_changed：只推给已开 SSE 或已有订阅的会话
            if only_uri is None and not sess.sse_attached and not sess.subscriptions:
                continue
            try:
                sess.queue.put_nowait(note)
                sent += 1
            except asyncio.QueueFull:
                log.warning("SSE queue full session=%s drop note", sess.id)
        if sent:
            log.debug("fanout %s -> %d sessions", note.get("method"), sent)
        return sent
