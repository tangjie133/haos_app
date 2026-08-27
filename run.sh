#!/usr/bin/with-contenv bash
# with-contenv：把 s6 里的 SUPERVISOR_TOKEN 注入本脚本。
# 若仍用 #!/bin/bash，语音进程读不到令牌，Assist 会播报「还不能访问」。
set -euo pipefail

if [[ -z "${SUPERVISOR_TOKEN:-}" ]]; then
  for f in \
    /var/run/s6/container_environment/SUPERVISOR_TOKEN \
    /run/s6/container_environment/SUPERVISOR_TOKEN
  do
    if [[ -f "$f" ]]; then
      SUPERVISOR_TOKEN="$(tr -d '\n\r' < "$f")"
      export SUPERVISOR_TOKEN
      break
    fi
  done
fi

python3 - <<'PY'
import json, os, shlex
from pathlib import Path

opts = {}
p = Path("/data/options.json")
if p.is_file():
    opts = json.loads(p.read_text(encoding="utf-8"))

def g(key, default=""):
    v = opts.get(key, default)
    return "" if v is None else str(v)

def from_s6(name: str) -> str:
    for base in (
        Path("/var/run/s6/container_environment"),
        Path("/run/s6/container_environment"),
    ):
        f = base / name
        if f.is_file():
            return f.read_text(encoding="utf-8").strip()
    return ""

def from_proc1(name: str) -> str:
    try:
        raw = Path("/proc/1/environ").read_bytes()
    except OSError:
        return ""
    for item in raw.split(b"\0"):
        if item.startswith(name.encode() + b"="):
            return item.split(b"=", 1)[1].decode("utf-8", "replace").strip()
    return ""

sup = (
    os.environ.get("SUPERVISOR_TOKEN")
    or os.environ.get("HASSIO_TOKEN")
    or from_s6("SUPERVISOR_TOKEN")
    or from_s6("HASSIO_TOKEN")
    or from_proc1("SUPERVISOR_TOKEN")
    or from_proc1("HASSIO_TOKEN")
    or ""
).strip()
manual = g("ha_token").strip()
# 长期令牌只用于 Core :8123；不要拿它去打 Supervisor 代理（会 401）
ha_url = g("ha_url").strip()
if not ha_url:
    adv = g("advertise_ip").strip()
    if manual and adv:
        # 与浏览器打开 HA 的地址一致；很多 HAOS 是 80 而不是 8123
        ha_url = f"http://{adv}"
    elif manual:
        ha_url = "http://127.0.0.1"
    else:
        ha_url = "http://supervisor/core"

mqtt_raw = g("mqtt_host").strip()
adv = g("advertise_ip").strip()
loopback = mqtt_raw.lower() in ("127.0.0.1", "localhost", "0.0.0.0", "::1", "")
# App 在 host 网络里可以连 127.0.0.1；设备绝对不能用这个地址
mqtt_connect = "127.0.0.1" if loopback else mqtt_raw
mqtt_advertise = adv if loopback else (mqtt_raw or adv)
if loopback:
    print(
        "[haos_app] MQTT 填了 127.0.0.1/留空：仅本 App 连 Mosquitto。"
        "设备将使用局域网 IP 广播 mqtt=；请确认 advertise_ip 已填（如 192.168.1.210）"
    )
    if not adv:
        print(
            "[haos_app] 未填局域网 IP：mDNS 可能广播 127.0.0.1，设备无法上报事件"
        )
if mqtt_advertise.lower() in ("127.0.0.1", "localhost"):
    print("[haos_app] 错误：给设备的 MQTT 地址仍是 127.0.0.1，事件上报会失败")

env = {
    "MCP_GW_HOST": "0.0.0.0",
    "MCP_GW_PORT": "9080",
    "MCP_GW_MDNS": "1",
    "MCP_GW_VOICE_WS_PORT": "8765",
    "MCP_GW_VOICE_WS_PATH": "/ws",
    "MQTT_ENABLE": "1",
    "MQTT_HOST": mqtt_connect,
    "MQTT_PORT": g("mqtt_port", "1883"),
    "MQTT_USER": g("mqtt_user"),
    "MQTT_PASSWORD": g("mqtt_password"),
    "MQTT_PREFIX": g("mqtt_prefix", "mcp_sensors"),
    "MQTT_CLIENT_ID": "mcp-sensors-gw",
    "MQTT_DISCOVERY": "1",
    "MQTT_ADVERTISE_HOST": mqtt_advertise,
    "MCP_GW_ADVERTISE_IP": adv,
    "DASHSCOPE_API_KEY": g("dashscope_api_key"),
    "HA_LANGUAGE": g("language", "zh-CN"),
    "HA_AGENT_ID": g("conversation_agent"),
    "HA_URL": ha_url,
    "HA_LLAT": manual,
    "TTS_VOICE": g("tts_voice", "longanyang"),
    "TTS_VOLUME": g("tts_volume", "100"),
    "USE_STREAM_TTS": "0",
}

print(
    f"[haos_app] HA_URL={ha_url} supervisor_len={len(sup)} llat_len={len(manual)}"
)
if not g("mqtt_user") and not g("mqtt_password"):
    print("[haos_app] MQTT 用户/密码为空：若 Mosquitto 要求登录会出现 Not authorized (135)")
lines = [f"{k}={shlex.quote(v)}" for k, v in env.items()]
Path("/app/.env").write_text("\n".join(lines) + "\n", encoding="utf-8")
print("[haos_app] wrote /app/.env")
PY

set -a
# shellcheck disable=SC1091
. /app/.env
set +a
# .env 不要覆盖 Supervisor 内部令牌
if [[ -z "${SUPERVISOR_TOKEN:-}" ]]; then
  for f in \
    /var/run/s6/container_environment/SUPERVISOR_TOKEN \
    /run/s6/container_environment/SUPERVISOR_TOKEN
  do
    if [[ -f "$f" ]]; then
      export SUPERVISOR_TOKEN="$(tr -d '\n\r' < "$f")"
      break
    fi
  done
fi

export PYTHONPATH="/app:${PYTHONPATH:-}"

echo "[haos_app] start mcp_gw :${MCP_GW_PORT:-9080}"
python3 -u -m mcp_gw >>/proc/1/fd/1 2>>/proc/1/fd/2 &
GW_PID=$!

VOICE_PID=""
if [[ -n "${DASHSCOPE_API_KEY:-}" ]]; then
  echo "[haos_app] start voice-bridge :8765 (Assist, no Hermes)"
  python3 -u /app/voice/bridge.py >>/proc/1/fd/1 2>>/proc/1/fd/2 &
  VOICE_PID=$!
else
  echo "[haos_app] skip voice-bridge: 未配置 dashscope_api_key（mcp_gw 仍运行）"
fi

term() {
  kill "$GW_PID" ${VOICE_PID:+$VOICE_PID} 2>/dev/null || true
  wait || true
}
trap term INT TERM

if [[ -n "$VOICE_PID" ]]; then
  wait -n "$GW_PID" "$VOICE_PID"
else
  wait "$GW_PID"
fi
status=$?
echo "[haos_app] a process exited status=${status}"
term
exit "${status}"
