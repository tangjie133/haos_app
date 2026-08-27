"""Home Assistant Conversation / Assist — 替代爱马仕。"""

from __future__ import annotations

import logging
import os
from typing import Any, Optional

import httpx

log = logging.getLogger("bridge.ha")

_working: Optional[tuple[str, str]] = None  # (api_base, token)


def _env(name: str, default: str = "") -> str:
    return (os.environ.get(name) or default).strip()


def _file_token(name: str) -> str:
    from pathlib import Path

    for base in (
        Path("/var/run/s6/container_environment"),
        Path("/run/s6/container_environment"),
    ):
        f = base / name
        try:
            if f.is_file():
                v = f.read_text(encoding="utf-8").strip()
                if v:
                    return v
        except OSError:
            continue
    return ""


def _supervisor_token() -> str:
    return (
        _env("SUPERVISOR_TOKEN")
        or _env("HASSIO_TOKEN")
        or _file_token("SUPERVISOR_TOKEN")
        or _file_token("HASSIO_TOKEN")
    )


def _llat() -> str:
    """用户在 App 配置里填的长期访问令牌（只能打 Core :8123，不能打 Supervisor 代理）。"""
    return _env("HA_LLAT") or (
        _env("HA_TOKEN") if _env("HA_TOKEN") != _supervisor_token() else ""
    )


def _ha_token() -> str:
    return _llat() or _supervisor_token()


def _ha_url() -> str:
    return _env("HA_URL", "http://supervisor/core").rstrip("/")


def _api_base_from(url: str) -> str:
    base = (url or "").rstrip("/")
    if base.endswith("/core"):
        return f"{base}/api"
    if base.endswith("/api"):
        return base
    return f"{base}/api"


def _attempts() -> list[tuple[str, str, str]]:
    """(label, raw_url, token) 内部代理必须用 SUPERVISOR_TOKEN；:8123 用长期令牌。"""
    seen: set[tuple[str, str]] = set()
    out: list[tuple[str, str, str]] = []
    sup = _supervisor_token()
    llat = _llat()

    def add(label: str, url: str, token: str) -> None:
        url = (url or "").strip().rstrip("/")
        token = (token or "").strip()
        if not url or not token:
            return
        key = (url, token)
        if key in seen:
            return
        seen.add(key)
        out.append((label, url, token))

    cfg = _env("HA_URL")
    cfg_is_proxy = "supervisor" in cfg or cfg.endswith("/core") or "172.30.32.2" in cfg
    add("configured", cfg, sup if cfg_is_proxy else (llat or sup))

    add("supervisor-dns", "http://supervisor/core", sup)
    add("supervisor-ip", "http://172.30.32.2/core", sup)

    cores = [
        "http://172.30.32.1:8123",
        "http://127.0.0.1:8123",
        "http://127.0.0.1",
        "http://homeassistant:8123",
    ]
    adv = _env("MCP_GW_ADVERTISE_IP")
    if adv:
        cores[0:0] = [f"http://{adv}", f"http://{adv}:8123"]
    extra = _env("HA_CORE_URL")
    if extra:
        cores.insert(0, extra)
    for u in cores:
        add("core-llat", u, llat)
        add("core-sup", u, sup)
    return out


def _speech_from_response(data: dict[str, Any]) -> str:
    resp = data.get("response") or {}
    speech = resp.get("speech") or {}
    plain = speech.get("plain") or {}
    text = (plain.get("speech") or "").strip()
    if text:
        return text
    inner = resp.get("data") or {}
    if isinstance(inner, dict):
        t = str(inner.get("message") or inner.get("text") or "").strip()
        if t:
            return t
    return ""


def log_auth_status() -> None:
    sup, llat = _supervisor_token(), _llat()
    log.info(
        "HA 鉴权 supervisor_len=%s llat_len=%s HA_URL=%s advertise=%s",
        len(sup),
        len(llat),
        _ha_url(),
        _env("MCP_GW_ADVERTISE_IP") or "-",
    )
    if not llat:
        log.warning(
            "未配置长期访问令牌。若 Supervisor 返回 401，请：卸载并重装本 App "
            "（让 homeassistant_api 生效），或填写 ha_token + 局域网 IP:8123"
        )


def ask_ha(
    user_text: str, conversation_id: Optional[str] = None
) -> tuple[str, Optional[str], bool]:
    """POST /api/conversation/process。返回 (reply, conversation_id, exit_intent=False)。"""
    global _working

    text = (user_text or "").strip()
    if not text:
        return "我没有听清，请再说一次。", conversation_id, False

    if not _ha_token():
        log.warning("无 HA 令牌")
        return (
            "语音已接到网关，但还不能访问 Home Assistant Assist。"
            "请重建本 App，或在配置里填写长期访问令牌。",
            conversation_id,
            False,
        )

    language = _env("HA_LANGUAGE", "zh-CN") or "zh-CN"
    agent_id = _env("HA_AGENT_ID")
    timeout_s = float(_env("HA_TIMEOUT_S", "12") or "12")
    body: dict[str, Any] = {"text": text, "language": language}
    if conversation_id:
        body["conversation_id"] = conversation_id
    if agent_id:
        body["agent_id"] = agent_id

    last_err: Optional[str] = None
    saw_401 = False
    with httpx.Client(timeout=timeout_s) as client:
        if _working:
            api_base, token = _working
            url = f"{api_base}/conversation/process"
            try:
                data = _post(client, url, token, body)
                return _ok(data, conversation_id, api_base)
            except Exception as e:
                last_err = str(e)
                log.warning("缓存地址失效 url=%s: %s", url, e)
                _working = None

        for label, raw, token in _attempts():
            api_base = _api_base_from(raw)
            url = f"{api_base}/conversation/process"
            try:
                data = _post(client, url, token, body)
            except httpx.HTTPStatusError as e:
                last_err = str(e)
                if e.response is not None and e.response.status_code == 401:
                    saw_401 = True
                log.warning("调用 HA Assist 失败 [%s] url=%s: %s", label, url, e)
                continue
            except Exception as e:
                last_err = str(e)
                log.warning("调用 HA Assist 失败 [%s] url=%s: %s", label, url, e)
                continue
            _working = (api_base, token)
            return _ok(data, conversation_id, api_base)

    log.warning("HA Assist 全部地址失败 last=%s", last_err)
    if saw_401 and not _llat():
        return (
            "Assist 拒绝了内部令牌。请卸载后重新安装本 App，"
            "或在配置里填写长期访问令牌和局域网地址。",
            conversation_id,
            False,
        )
    if saw_401:
        return (
            "Assist 鉴权失败。请确认长期访问令牌有效，地址填 http://<HA局域网IP>:8123。",
            conversation_id,
            False,
        )
    return "暂时连不上 Home Assistant，请稍后再试。", conversation_id, False


def _post(
    client: httpx.Client, url: str, token: str, body: dict[str, Any]
) -> dict[str, Any]:
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "X-Supervisor-Token": token,
    }
    r = client.post(url, headers=headers, json=body)
    if r.status_code >= 400:
        snippet = (r.text or "")[:400]
        log.warning("HA Assist HTTP %s url=%s body=%s", r.status_code, url, snippet)
        r.raise_for_status()
    return r.json()


def _ok(
    data: dict[str, Any], conversation_id: Optional[str], api_base: str
) -> tuple[str, Optional[str], bool]:
    reply = _speech_from_response(data) or "Home Assistant 没有返回可播报的内容。"
    new_cid = (data.get("conversation_id") or conversation_id or "").strip() or None
    if new_cid and new_cid != conversation_id:
        log.info("HA conversation_id=%s", new_cid)
    log.info("HA Assist 成功 api=%s", api_base)
    return reply, new_cid, False
