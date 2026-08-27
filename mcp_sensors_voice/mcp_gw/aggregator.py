from __future__ import annotations

import asyncio
import json
import logging
import uuid
from typing import Any, Optional

from starlette.requests import Request
from starlette.responses import JSONResponse, Response, StreamingResponse

from . import config
from .coap_client import DeviceCoapClient
from .registry import DeviceRegistry
from .sessions import SessionManager

log = logging.getLogger("mcp_gw.agg")

GATEWAY_LIST_DEVICES = "gw.list_devices"


def split_public_uri(public_uri: str) -> tuple[str, str]:
    if config.TOOL_SEP not in public_uri:
        raise ValueError(f"bad public uri: {public_uri}")
    device_id, native = public_uri.split(config.TOOL_SEP, 1)
    return device_id.strip().lower(), native


class AggregatorMcp:
    """Hermes 只看见本进程 MCP。tools 内部 = CoAP；events 订阅 = MQTT。

    工具名 {id}__sensors.get_* / set_* 对应 coap_hub.h 的 GET/PUT。
    资源 {id}__sensor://events 对应 topic mcp_sensors/<id>/event。
    禁止把 PCM 或 Discovery 接到这里。
    """

    def __init__(
        self,
        registry: DeviceRegistry,
        client: DeviceCoapClient,
        sessions: SessionManager,
    ) -> None:
        self.registry = registry
        self.client = client
        self.sessions = sessions

    def public_tools(self) -> list[dict[str, Any]]:
        tools: list[dict[str, Any]] = [
            {
                "name": GATEWAY_LIST_DEVICES,
                "description": (
                    "列出已发现设备（mDNS 枢纽或 MQTT 叶子）。"
                    f"枢纽工具 {{id}}{config.TOOL_SEP}sensors.get_snapshot 等走 CoAP；"
                    f"事件 {{id}}{config.TOOL_SEP}sensor://events 走 MQTT。"
                ),
                "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
            }
        ]
        empty = {"type": "object", "properties": {}, "additionalProperties": True}
        hub_catalog = (
            ("sensors.get_snapshot", "CoAP GET /sensors/snapshot"),
            ("sensors.get_thresholds", "CoAP GET /sensors/thresholds"),
            ("sensors.set_thresholds", "CoAP PUT /sensors/thresholds"),
            ("sensors.get_config", "CoAP GET /sensors/config"),
            ("sensors.set_config", "CoAP PUT /sensors/config"),
            ("sensors.get_recent_events", "MQTT 缓存或 CoAP GET /sensors/events"),
        )
        leaf_catalog = (
            ("sensors.get_recent_events", "仅 MQTT 事件缓存（叶子无常驻 CoAP）"),
        )
        for rec in self.registry.list_online():
            label = rec.name or rec.id
            catalog = hub_catalog if rec.has_coap() else leaf_catalog
            for native, desc in catalog:
                tools.append(
                    {
                        "name": f"{rec.id}{config.TOOL_SEP}{native}",
                        "description": f"[{label}/{rec.id}] {desc}",
                        "inputSchema": empty,
                    }
                )
        return tools

    def public_resources(self) -> list[dict[str, Any]]:
        resources: list[dict[str, Any]] = []
        for rec in self.registry.list_online():
            label = rec.name or rec.id
            resources.append(
                {
                    "uri": f"{rec.id}{config.TOOL_SEP}sensor://events",
                    "name": f"{rec.id}{config.TOOL_SEP}events",
                    "mimeType": "application/json",
                    "description": (
                        f"[{label}/{rec.id}] MQTT {config.MQTT_PREFIX}/{rec.id}/event；"
                        "禁止用 MQTT 改配置"
                    ),
                }
            )
        return resources

    async def handle_post(self, request: Request) -> Response:
        try:
            body = await request.json()
        except Exception:
            return JSONResponse(
                {"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": "Parse error"}},
                status_code=400,
            )

        if isinstance(body, list):
            out = []
            session = request.headers.get("mcp-session-id")
            for item in body:
                out.append(await self._dispatch_one(item, session))
            resp = JSONResponse(out)
            if session:
                resp.headers["MCP-Session-Id"] = session
            return resp

        if not isinstance(body, dict):
            return JSONResponse(
                {"jsonrpc": "2.0", "id": None, "error": {"code": -32600, "message": "Invalid Request"}},
                status_code=400,
            )

        session = request.headers.get("mcp-session-id")
        if "id" not in body and str(body.get("method") or "").startswith("notifications/"):
            return Response(status_code=202)

        result_msg = await self._dispatch_one(body, session)
        if body.get("method") == "initialize":
            session = session or str(uuid.uuid4())
            await self.sessions.ensure(session)
        resp = JSONResponse(result_msg)
        if session:
            resp.headers["MCP-Session-Id"] = session
        return resp

    async def _dispatch_one(self, body: dict[str, Any], session: Optional[str]) -> dict[str, Any]:
        req_id = body.get("id")
        method = body.get("method")
        params = body.get("params") or {}

        try:
            if method == "initialize":
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "protocolVersion": config.PROTOCOL_VERSION,
                        "capabilities": {
                            "tools": {"listChanged": False},
                            "resources": {"subscribe": True, "listChanged": True},
                        },
                        "serverInfo": {
                            "name": config.SERVER_NAME,
                            "version": config.SERVER_VERSION,
                        },
                        "instructions": (
                            "设备通道：mDNS _mcp-sensors + CoAP 读写 + MQTT 仅事件。"
                            "快照/阈值/雷达：{id}__sensors.get_* / set_*（CoAP）。"
                            "事件：订 {id}__sensor://events 或 get_recent_events。"
                            "PCM 只走 WebSocket。"
                        ),
                    },
                }
            if method == "tools/list":
                return {"jsonrpc": "2.0", "id": req_id, "result": {"tools": self.public_tools()}}
            if method == "tools/call":
                name = str((params or {}).get("name") or "")
                args = (params or {}).get("arguments") or {}
                if not isinstance(args, dict):
                    args = {}
                result = await self._call_tool(name, args)
                return {"jsonrpc": "2.0", "id": req_id, "result": result}
            if method == "resources/list":
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {"resources": self.public_resources()},
                }
            if method == "resources/templates/list":
                return {"jsonrpc": "2.0", "id": req_id, "result": {"resourceTemplates": []}}
            if method == "resources/read":
                uri = str((params or {}).get("uri") or "")
                result = await self._read_resource(uri)
                return {"jsonrpc": "2.0", "id": req_id, "result": result}
            if method == "resources/subscribe":
                uri = str((params or {}).get("uri") or "")
                await self._subscribe(session, uri)
                return {"jsonrpc": "2.0", "id": req_id, "result": {}}
            if method == "resources/unsubscribe":
                uri = str((params or {}).get("uri") or "")
                await self._unsubscribe(session, uri)
                return {"jsonrpc": "2.0", "id": req_id, "result": {}}
            if method == "ping":
                return {"jsonrpc": "2.0", "id": req_id, "result": {}}
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "error": {"code": -32601, "message": f"Method not found: {method}"},
            }
        except Exception as e:
            log.exception("MCP method %s failed", method)
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "error": {"code": -32000, "message": str(e)},
            }

    async def _subscribe(self, session: Optional[str], public_uri: str) -> None:
        if not session:
            raise RuntimeError("resources/subscribe 需要 MCP-Session-Id（先 initialize）")
        if not public_uri:
            raise RuntimeError("uri required")
        # 校验格式与在线
        device_id, _native = split_public_uri(public_uri)
        rec = self.registry.get(device_id)
        if rec is None:
            raise RuntimeError(f"设备未注册: {device_id}")
        online_ids = {r.id for r in self.registry.list_online()}
        if rec.id not in online_ids:
            raise RuntimeError(f"设备离线或心跳超时: {device_id}")
        await self.sessions.subscribe(session, public_uri)

    async def _unsubscribe(self, session: Optional[str], public_uri: str) -> None:
        if not session:
            return
        await self.sessions.unsubscribe(session, public_uri)

    async def _call_tool(self, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        if name == GATEWAY_LIST_DEVICES:
            devices = [self.registry.to_public(r) for r in self.registry.list_online()]
            text = json.dumps(devices, ensure_ascii=False, indent=2)
            return {"content": [{"type": "text", "text": text}], "isError": False}

        if config.TOOL_SEP not in name:
            return {
                "content": [
                    {
                        "type": "text",
                        "text": (
                            f"未知工具 {name}。设备工具须为 id{config.TOOL_SEP}native；"
                            f"或使用 {GATEWAY_LIST_DEVICES}。"
                        ),
                    }
                ],
                "isError": True,
            }

        device_id, native = name.split(config.TOOL_SEP, 1)
        rec = self.registry.get(device_id)
        if rec is None:
            return {
                "content": [{"type": "text", "text": f"设备未注册: {device_id}"}],
                "isError": True,
            }
        online_ids = {r.id for r in self.registry.list_online()}
        if rec.id not in online_ids:
            return {
                "content": [{"type": "text", "text": f"设备离线或心跳超时: {device_id}"}],
                "isError": True,
            }

        try:
            data = await self._coap_tool(rec, native, arguments)
            text = json.dumps(data, ensure_ascii=False)
            return {"content": [{"type": "text", "text": text}], "isError": False}
        except Exception as e:
            self.registry.mark_error(device_id, str(e))
            return {
                "content": [{"type": "text", "text": f"调用失败: {e}"}],
                "isError": True,
            }

    async def _coap_tool(self, rec: Any, native: str, arguments: dict[str, Any]) -> Any:
        if native == "sensors.get_recent_events":
            events = list(rec.event_cache)
            limit = int(arguments.get("limit") or 20)
            cat = str(arguments.get("category") or "").strip()
            if cat:
                events = [e for e in events if str(e.get("category") or "") == cat]
            if events:
                return {"events": events[-limit:]}
            if rec.has_coap():
                host, port = rec.coap_endpoint()
                return await self.client.get_json(host, "/sensors/events", port)
            return {"events": []}
        if not rec.has_coap():
            raise RuntimeError("该设备无 CoAP（叶子仅 MQTT 事件）")
        host, port = rec.coap_endpoint()
        if native == "sensors.get_snapshot":
            return await self.client.get_json(host, "/sensors/snapshot", port)
        if native == "sensors.get_thresholds":
            return await self.client.get_json(host, "/sensors/thresholds", port)
        if native == "sensors.set_thresholds":
            return await self.client.put_json(host, "/sensors/thresholds", arguments, port)
        if native == "sensors.get_config":
            return await self.client.get_json(host, "/sensors/config", port)
        if native == "sensors.set_config":
            return await self.client.put_json(host, "/sensors/config", arguments, port)
        raise RuntimeError(f"未知工具 {native}")

    async def _read_resource(self, public_uri: str) -> dict[str, Any]:
        if config.TOOL_SEP not in public_uri:
            raise RuntimeError(
                f"资源 URI 须为 id{config.TOOL_SEP}native，例如 demo01{config.TOOL_SEP}sensor://events"
            )
        device_id, _native_uri = public_uri.split(config.TOOL_SEP, 1)
        rec = self.registry.get(device_id)
        if rec is None:
            raise RuntimeError(f"设备未注册: {device_id}")
        online_ids = {r.id for r in self.registry.list_online()}
        if rec.id not in online_ids:
            raise RuntimeError(f"设备离线或心跳超时: {device_id}")

        try:
            if rec.event_cache:
                payload = {"events": rec.event_cache[-32:]}
            elif rec.has_coap():
                host, port = rec.coap_endpoint()
                payload = await self.client.get_json(host, "/sensors/events", port) or {"events": []}
            else:
                payload = {"events": []}
        except Exception as e:
            self.registry.mark_error(device_id, str(e))
            raise RuntimeError(f"读取事件失败: {e}") from e
        text = json.dumps(payload, ensure_ascii=False)
        return {
            "contents": [
                {
                    "uri": public_uri,
                    "mimeType": "application/json",
                    "text": text,
                }
            ]
        }

    async def handle_get(self, request: Request) -> Response:
        accept = (request.headers.get("accept") or "").lower()
        if "text/event-stream" not in accept and "*/*" not in accept:
            return JSONResponse(
                {"error": "Accept must include text/event-stream for SSE"},
                status_code=406,
            )

        session_id = request.headers.get("mcp-session-id") or request.headers.get("MCP-Session-Id")
        if not session_id:
            return JSONResponse(
                {"error": "MCP-Session-Id required (initialize via POST first)"},
                status_code=400,
            )

        sess = await self.sessions.ensure(session_id)
        sess.sse_attached = True
        log.info("Hermes SSE open session=%s", session_id)

        async def event_gen():
            # 立即发注释，确认流已建立
            yield ": connected\n\n"
            try:
                while True:
                    try:
                        note = await asyncio.wait_for(sess.queue.get(), timeout=15.0)
                        payload = json.dumps(note, ensure_ascii=False)
                        yield f"event: message\ndata: {payload}\n\n"
                    except asyncio.TimeoutError:
                        yield ": ping\n\n"
            except asyncio.CancelledError:
                raise
            finally:
                sess.sse_attached = False
                log.info("Hermes SSE closed session=%s", session_id)

        return StreamingResponse(
            event_gen(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "MCP-Session-Id": session_id,
                "X-Accel-Buffering": "no",
            },
        )
