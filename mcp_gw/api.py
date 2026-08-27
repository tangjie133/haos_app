from __future__ import annotations

import asyncio
import logging

from typing import Any

from starlette.requests import Request
from starlette.responses import JSONResponse

from . import config
from .device_mqtt import broker_uri_for_device, config_fingerprint, push_all, push_if_missing
from .registry import DeviceRegistry

log = logging.getLogger("mcp_gw.api")


def _unauthorized() -> JSONResponse:
    return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)


def _check_token(request: Request) -> bool:
    if not config.TOKEN:
        return True
    got = request.headers.get("x-mcp-gw-token") or ""
    q = request.query_params.get("token") or ""
    return got == config.TOKEN or q == config.TOKEN


class RegisterApi:
    """Optional manual register; primary path is browse _mcp-sensors._udp."""

    def __init__(self, registry: DeviceRegistry, mqtt: Any = None) -> None:
        self.registry = registry
        self.mqtt = mqtt

    async def register(self, request: Request) -> JSONResponse:
        if not _check_token(request):
            return _unauthorized()
        try:
            body = await request.json()
        except Exception:
            return JSONResponse({"ok": False, "error": "invalid json"}, status_code=400)
        if not isinstance(body, dict):
            return JSONResponse({"ok": False, "error": "object required"}, status_code=400)

        device_id = str(body.get("id") or "").strip()
        name = str(body.get("name") or "").strip()
        fw = str(body.get("fw") or "").strip()
        base_url = str(body.get("base_url") or "").strip()
        ip = str(body.get("ip") or "").strip()
        port = int(body.get("port") or body.get("coap_port") or 5683)
        if not base_url:
            if not ip:
                return JSONResponse({"ok": False, "error": "ip or base_url required"}, status_code=400)
            base_url = f"http://{ip}"
        if not ip:
            ip = base_url.replace("http://", "").replace("https://", "").split("/")[0].split(":")[0]
        try:
            rec = self.registry.upsert(
                device_id=device_id,
                base_url=base_url,
                name=name,
                fw=fw,
                coap_host=ip,
                coap_port=port,
            )
        except ValueError as e:
            return JSONResponse({"ok": False, "error": str(e)}, status_code=400)
        log.info("registered device id=%s coap=%s", rec.id, f"{rec.coap_host}:{rec.coap_port}")
        if rec.coap_host:
            asyncio.create_task(push_if_missing(rec.coap_host), name=f"mqtt-push-reg-{rec.id}")
        return JSONResponse({"ok": True, "device": self.registry.to_public(rec)})

    async def list_devices(self, request: Request) -> JSONResponse:
        if not _check_token(request):
            return _unauthorized()
        return JSONResponse(
            {"ok": True, "devices": [self.registry.to_public(r) for r in self.registry.list_all()]}
        )

    async def delete_device(self, request: Request) -> JSONResponse:
        if not _check_token(request):
            return _unauthorized()
        device_id = (request.path_params.get("id") or "").strip()
        if self.mqtt is not None:
            self.mqtt.forget_device(device_id)
        ok = self.registry.remove(device_id)
        return JSONResponse({"ok": ok}, status_code=200 if ok else 404)

    async def push_mqtt(self, request: Request) -> JSONResponse:
        """Force-push broker+creds after user changed MQTT options.

        POST /api/mqtt/push  body: {"force": true, "id": "f20e08"}
        """
        if not _check_token(request):
            return _unauthorized()
        force = True
        only_id = ""
        try:
            body = await request.json()
            if isinstance(body, dict):
                if "force" in body:
                    force = bool(body.get("force"))
                only_id = str(body.get("id") or body.get("device_id") or "").strip().lower()
        except Exception:
            pass

        recs = self.registry.list_all()
        if only_id:
            recs = [r for r in recs if r.id == only_id]
            if not recs:
                return JSONResponse({"ok": False, "error": "device not found"}, status_code=404)

        ips = [(r.coap_host or "").strip() for r in recs if (r.coap_host or "").strip()]
        results = await push_all(ips, force=force)
        ok_n = sum(1 for v in results.values() if v)
        log.info("mqtt push api force=%s ok=%s/%s uri=%s", force, ok_n, len(results), broker_uri_for_device())
        return JSONResponse(
            {
                "ok": ok_n == len(results) and len(results) > 0,
                "broker": broker_uri_for_device(),
                "user": (config.MQTT_USER or "").strip() or None,
                "fingerprint": config_fingerprint().split("\n")[0],
                "pushed": results,
                "ok_count": ok_n,
                "total": len(results),
            }
        )
