from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from . import config
from .aggregator import AggregatorMcp
from .api import RegisterApi
from .coap_client import DeviceCoapClient
from .coap_poll import CoapSnapshotPoller
from .device_mqtt import broker_uri_for_device, consume_config_change, push_if_missing
from .mdns_advertise import MdnsAdvertiser
from .mdns_devices import MdnsDeviceBrowser
from .mqtt_ha import HaMqttBridge
from .registry import DeviceRegistry
from .sessions import SessionManager

log = logging.getLogger("mcp_gw")


async def _mqtt_push_loop() -> None:
    """Periodic HTTP MQTT fallback; force when MQTT options fingerprint changes."""
    while True:
        try:
            force = consume_config_change()
            for rec in STATE.registry.list_all():
                host = (rec.coap_host or "").strip()
                if host:
                    await push_if_missing(host, force=force)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.debug("mqtt push loop: %s", e)
        await asyncio.sleep(30)


class AppState:
    def __init__(self) -> None:
        self.registry = DeviceRegistry(stale_s=config.STALE_S)
        self.client = DeviceCoapClient(timeout_s=config.DEVICE_TIMEOUT_S)
        self.sessions = SessionManager()
        self.mqtt = HaMqttBridge(self.registry, self.sessions)
        self.api = RegisterApi(self.registry, mqtt=self.mqtt)
        self.agg = AggregatorMcp(self.registry, self.client, self.sessions)
        self.mdns = MdnsAdvertiser()
        self.mdns_dev = MdnsDeviceBrowser(self.registry)
        self.coap_poll = CoapSnapshotPoller(self.registry, self.client, self.mqtt)


STATE = AppState()


@asynccontextmanager
async def lifespan(app: Starlette):
    log.info(
        "mcp_gw starting as device adapter host=%s port=%s mdns=%s mqtt=%s "
        "(/mcp is legacy; agents should use Home Assistant /api/mcp)",
        config.HOST,
        config.PORT,
        "yes" if config.MDNS_ENABLE else "no",
        "on" if config.MQTT_ENABLE else "off",
    )
    await STATE.mdns.start()
    await STATE.mdns_dev.start()
    await STATE.mqtt.start()
    await STATE.coap_poll.start()
    push_task = asyncio.create_task(_mqtt_push_loop(), name="mqtt-push-loop")
    try:
        yield
    finally:
        push_task.cancel()
        try:
            await push_task
        except asyncio.CancelledError:
            pass
        await STATE.coap_poll.stop()
        await STATE.mqtt.stop()
        await STATE.mdns_dev.stop()
        await STATE.mdns.stop()


async def health(_: Request) -> JSONResponse:
    online = STATE.registry.list_online()
    return JSONResponse(
        {
            "ok": True,
            "service": config.SERVER_NAME,
            "version": config.SERVER_VERSION,
            "devices_total": len(STATE.registry.list_all()),
            "devices_online": len(online),
            "role": "device-adapter",
            "mcp": "/mcp",
            "mcp_note": "legacy debug; agents use Home Assistant /api/mcp",
            "device_protocol": "coap+mqtt-event",
            "sse": True,
            "mdns": STATE.mdns.status(),
            "mqtt": {
                **STATE.mqtt.status(),
                "device_broker": broker_uri_for_device(),
                "device_auth_configured": bool((config.MQTT_USER or "").strip()),
            },
        }
    )


async def mcp_post(request: Request) -> Response:
    return await STATE.agg.handle_post(request)


async def mcp_get(request: Request) -> Response:
    return await STATE.agg.handle_get(request)


def create_app() -> Starlette:
    routes = [
        Route("/health", health, methods=["GET"]),
        Route("/api/devices", STATE.api.list_devices, methods=["GET"]),
        Route("/api/devices/register", STATE.api.register, methods=["POST"]),
        Route("/api/devices/{id}", STATE.api.delete_device, methods=["DELETE"]),
        Route("/api/mqtt/push", STATE.api.push_mqtt, methods=["POST"]),
        Route("/mcp", mcp_post, methods=["POST"]),
        Route("/mcp", mcp_get, methods=["GET"]),
    ]
    return Starlette(routes=routes, lifespan=lifespan)


app = create_app()
