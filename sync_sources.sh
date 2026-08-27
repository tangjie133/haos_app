#!/usr/bin/env bash
# 从桌面 server_stack 同步源码到本 App 目录（不含 venv）
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
STACK="${HERE}/../server_stack"
mkdir -p "${HERE}/mcp_gw" "${HERE}/voice"
cp -a "${STACK}/mcp_gw/mcp_gw/"*.py "${HERE}/mcp_gw/"
cp -a "${STACK}/voice-bridge/bridge.py" "${HERE}/voice/bridge.py"
echo "已从 ${STACK} 复制 mcp_gw 与 bridge.py。"
echo "注意：voice/bridge.py 若被 HA Assist 补丁覆盖，请重新应用 ha_assist 对接，勿直接覆盖丢失 ask_ha。"
