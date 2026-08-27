# MCP Sensors + Voice

HA App：设备适配网关（`:9080`）+ 可选语音桥（`:8765`）。  
装好 **Mosquitto** 和本 App 即可。传感器用 MQTT Discovery 进 HA。

配置通常只需 **MQTT 用户/密码**；局域网 IP 自动探测；语音可选填阿里云 Key。

**当前版本：0.4.3。** 说明见 **[docs/使用说明.md](docs/使用说明.md)**。

## 安装方式

- **推荐（Git）**：把上级仓库加到 HA App 商店仓库列表后安装（见仓库根目录 `README.md`）
- **本地**：拷贝本目录到 `/addons/mcp_sensors_voice/`，刷新后安装
