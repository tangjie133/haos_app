# MCP Sensors Apps（Home Assistant 仓库）

本仓库可在 Home Assistant **App 商店**里用 **Git 链接**添加。

```text
repository.yaml          ← 仓库元数据（本文件同级）
mcp_sensors_voice/       ← App：设备网关 + 语音桥
  config.yaml
  Dockerfile
  ...
```

## 用 Git 安装

1. 把本仓库推到 GitHub / Gitee（公开或 HA 能访问的私有库）
2. HA：**设置 → App → ⋮ → 仓库**
3. 添加仓库 URL，例如：
   ```text
   https://github.com/<你的用户名>/haos_app
   ```
4. 关闭后刷新，安装 **MCP Sensors + Voice**
5. 配置 MQTT 用户/密码、可选语音 Key 后启动

本地开发仍可直接拷贝 `mcp_sensors_voice/` 到 `/addons/mcp_sensors_voice/`。

说明见 [`mcp_sensors_voice/docs/使用说明.md`](mcp_sensors_voice/docs/使用说明.md)。
