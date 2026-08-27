"""一次添加整套主题前缀；多设备由 MQTT 自动建实体，无需逐台确认。"""

from __future__ import annotations

import voluptuous as vol
from homeassistant import config_entries
from homeassistant.data_entry_flow import FlowResult

from .const import DEFAULT_PREFIX, DOMAIN

STEP_USER = vol.Schema(
    {
        vol.Required("prefix", default=DEFAULT_PREFIX): str,
    }
)


class ConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    VERSION = 1

    async def async_step_user(self, user_input: dict | None = None) -> FlowResult:
        if self._async_current_entries():
            return self.async_abort(reason="already_configured")
        if user_input is None:
            return self.async_show_form(step_id="user", data_schema=STEP_USER)
        prefix = (user_input.get("prefix") or DEFAULT_PREFIX).strip() or DEFAULT_PREFIX
        await self.async_set_unique_id(DOMAIN)
        self._abort_if_unique_id_configured()
        return self.async_create_entry(title=f"MCP Sensors ({prefix})", data={"prefix": prefix})
