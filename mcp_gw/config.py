from __future__ import annotations

import os


def _f(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or not str(raw).strip():
        return default
    return float(raw)


def _i(name: str, default: int) -> int:
    return int(_f(name, float(default)))


def _truthy(name: str, default: str = "0") -> bool:
    return (os.environ.get(name) or default).strip().lower() in ("1", "true", "yes", "on")


HOST = os.environ.get("MCP_GW_HOST", "0.0.0.0").strip() or "0.0.0.0"
PORT = _i("MCP_GW_PORT", 9080)
TOKEN = (os.environ.get("MCP_GW_TOKEN") or "").strip()
STALE_S = _f("MCP_GW_STALE_S", 90.0)
DEVICE_TIMEOUT_S = _f("MCP_GW_DEVICE_TIMEOUT_S", 15.0)

# mDNS：广播 _voice-ws / _mcp-gw TXT mqtt=；浏览设备走 _mcp-sensors._udp
MDNS_ENABLE = (os.environ.get("MCP_GW_MDNS", "1").strip().lower() not in ("0", "false", "no", "off"))
ADVERTISE_IP = (os.environ.get("MCP_GW_ADVERTISE_IP") or "").strip()

VOICE_WS_PORT = _i("MCP_GW_VOICE_WS_PORT", 8765)
VOICE_WS_PATH = (os.environ.get("MCP_GW_VOICE_WS_PATH") or "/ws").strip() or "/ws"
if not VOICE_WS_PATH.startswith("/"):
    VOICE_WS_PATH = "/" + VOICE_WS_PATH

# 工具/资源名前缀：{device_id}__{native}
TOOL_SEP = "__"

PROTOCOL_VERSION = "2025-03-26"
SERVER_NAME = "mcp-sensors-gw"
SERVER_VERSION = "0.5.0"

# 订设备事件。由本 App 代发 homeassistant/.../config，小白不必拷自定义集成。
MQTT_HOST = (os.environ.get("MQTT_HOST") or "").strip()
MQTT_PORT = _i("MQTT_PORT", 1883)
MQTT_USER = (os.environ.get("MQTT_USER") or "").strip()
MQTT_PASSWORD = (os.environ.get("MQTT_PASSWORD") or "").strip()
MQTT_PREFIX = (os.environ.get("MQTT_PREFIX") or "mcp_sensors").strip() or "mcp_sensors"
MQTT_CLIENT_ID = (os.environ.get("MQTT_CLIENT_ID") or "mcp-sensors-gw").strip() or "mcp-sensors-gw"
MQTT_ENABLE = bool(MQTT_HOST) and _truthy("MQTT_ENABLE", "1")
MQTT_DISCOVERY = _truthy("MQTT_DISCOVERY", "1")
COAP_POLL_S = _f("COAP_POLL_S", 5.0)
# 给设备看的 broker 主机；禁止 127.0.0.1（设备连的是自己，不是 HA）
MQTT_ADVERTISE_HOST = (os.environ.get("MQTT_ADVERTISE_HOST") or "").strip()
