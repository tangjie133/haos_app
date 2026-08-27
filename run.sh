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
import json, os, shlex, socket
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
SKIP = ("127.", "172.17.", "172.18.", "172.19.", "172.30.", "192.168.122.", "10.8.", "169.254.")


def _lan_ok(ip: str) -> bool:
    ip = (ip or "").strip().split("/")[0]
    if ip.count(".") != 3:
        return False
    return not ip.startswith(SKIP)


def guess_lan_ip(token: str) -> str:
    if token:
        try:
            import urllib.request

            req = urllib.request.Request(
                "http://supervisor/network/info",
                headers={"Authorization": f"Bearer {token}"},
            )
            with urllib.request.urlopen(req, timeout=3) as resp:
                payload = json.loads(resp.read().decode("utf-8", "replace"))
            data = payload.get("data") or payload
            ifaces = data.get("interfaces") or []
            primary = ""
            for iface in ifaces:
                if not isinstance(iface, dict):
                    continue
                ipv4 = iface.get("ipv4") or {}
                addrs = ipv4.get("address") or []
                if isinstance(addrs, str):
                    addrs = [addrs]
                for item in addrs:
                    ip = str(item).split("/")[0].strip()
                    if not _lan_ok(ip):
                        continue
                    if iface.get("primary"):
                        return ip
                    if not primary:
                        primary = ip
            if primary:
                return primary
        except Exception as e:
            print(f"[haos_app] 自动探测局域网 IP 失败: {e}")
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        if _lan_ok(ip):
            return ip
    except OSError:
        pass
    return ""


manual = g("ha_token").strip()
adv = g("advertise_ip").strip()
if not _lan_ok(adv):
    guessed = guess_lan_ip(sup)
    if guessed:
        adv = guessed
        print(f"[haos_app] 局域网 IP 自动探测为 {adv}")
    elif adv:
        print(f"[haos_app] 局域网 IP {adv} 不可用，请改成 HA 在 WiFi/有线网的地址")
    else:
        print("[haos_app] 未能自动探测局域网 IP。设备连 MQTT/语音需要填 advertise_ip")

# 语音默认走 Supervisor 代理，不必填 HA 网址和长期令牌
ha_url = g("ha_url").strip() or "http://supervisor/core"

mqtt_raw = g("mqtt_host").strip()
loopback = mqtt_raw.lower() in ("127.0.0.1", "localhost", "0.0.0.0", "::1", "")
mqtt_connect = "127.0.0.1" if loopback else mqtt_raw
mqtt_advertise = adv if loopback else (mqtt_raw if _lan_ok(mqtt_raw) else adv)
mqtt_port = g("mqtt_port").strip() or "1883"
mqtt_prefix = g("mqtt_prefix").strip() or "mcp_sensors"

if not _lan_ok(mqtt_advertise):
    print("[haos_app] 给设备的 MQTT 地址无效（不能是 127.0.0.1），事件可能发不出来")

env = {
    "MCP_GW_HOST": "0.0.0.0",
    "MCP_GW_PORT": "9080",
    "MCP_GW_MDNS": "1",
    "MCP_GW_VOICE_WS_PORT": "8765",
    "MCP_GW_VOICE_WS_PATH": "/ws",
    "MQTT_ENABLE": "1",
    "MQTT_HOST": mqtt_connect,
    "MQTT_PORT": mqtt_port,
    "MQTT_USER": g("mqtt_user"),
    "MQTT_PASSWORD": g("mqtt_password"),
    "MQTT_PREFIX": mqtt_prefix,
    "MQTT_CLIENT_ID": "mcp-sensors-gw",
    "MQTT_DISCOVERY": "1",
    "MQTT_ADVERTISE_HOST": mqtt_advertise,
    "MCP_GW_ADVERTISE_IP": adv,
    "DASHSCOPE_API_KEY": g("dashscope_api_key"),
    "HA_LANGUAGE": g("language").strip() or "zh-CN",
    "HA_AGENT_ID": g("conversation_agent"),
    "HA_URL": ha_url,
    "HA_LLAT": manual,
    "TTS_VOICE": g("tts_voice").strip() or "longanyang",
    "TTS_VOLUME": g("tts_volume").strip() or "100",
    "USE_STREAM_TTS": "0",
}

print(
    f"[haos_app] lan={adv or '-'} mqtt_dev={mqtt_advertise or '-'} "
    f"HA_URL={ha_url} supervisor_len={len(sup)} llat_len={len(manual)}"
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
  sup_len=${#SUPERVISOR_TOKEN}
  llat_len=${#HA_LLAT}
  echo "[haos_app] start voice-bridge :8765 dashscope_len=${#DASHSCOPE_API_KEY} supervisor_len=${sup_len} llat_len=${llat_len}"
  # 显式传入 Supervisor 令牌，避免子进程读不到 s6 环境
  env SUPERVISOR_TOKEN="${SUPERVISOR_TOKEN:-}" HASSIO_TOKEN="${HASSIO_TOKEN:-}" \
    python3 -u /app/voice/bridge.py >>/proc/1/fd/1 2>>/proc/1/fd/2 &
  VOICE_PID=$!
else
  echo "[haos_app] skip voice-bridge: 未配置 dashscope_api_key（mcp_gw 仍运行；App 配置里填 sk- 密钥后重启）"
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
