DOMAIN = "mcp_sensors"
DEFAULT_PREFIX = "mcp_sensors"
SIGNAL_UPDATE = "mcp_sensors_update"

# MQTT JSON 里常见字段 → (device_class, unit, 显示名)
NUMERIC_KEYS = {
    "temp": ("temperature", "°C", "温度"),
    "temperature": ("temperature", "°C", "温度"),
    "humidity": ("humidity", "%", "湿度"),
    "co2": ("carbon_dioxide", "ppm", "二氧化碳"),
    "light": ("illuminance", "lx", "光照"),
    "lux": ("illuminance", "lx", "光照"),
    "sound": (None, "dB", "声强"),
}
