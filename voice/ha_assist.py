"""Home Assistant Conversation / Assist — 替代爱马仕。

对话代理：优先读「首选 Assist 流水线」的 conversation_engine（与网页 Assist 一致），
不靠名称关键字猜。可选环境变量 HA_AGENT_ID 覆盖。
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Optional

import httpx

log = logging.getLogger("bridge.ha")

_working: Optional[tuple[str, str]] = None  # (api_base, token)
_resolved_agent: Optional[str] = None  # 缓存：首选流水线上的 conversation_engine


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


def _ws_url_from_api(api_base: str) -> str:
    base = (api_base or "").rstrip("/")
    if base.startswith("https://"):
        return "wss://" + base[len("https://") :] + "/websocket"
    if base.startswith("http://"):
        return "ws://" + base[len("http://") :] + "/websocket"
    return f"ws://{base}/websocket"


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


def _ws_command(api_base: str, token: str, msg_type: str, **extra: Any) -> Any:
    """同步调用 HA WebSocket 命令，返回 result 字段。"""
    try:
        from websocket import create_connection
    except ImportError as e:
        raise RuntimeError("websocket-client not installed") from e

    ws_url = _ws_url_from_api(api_base)
    ws = create_connection(
        ws_url,
        timeout=8,
        header=[f"Authorization: Bearer {token}"],
    )
    try:
        auth_req = json.loads(ws.recv())
        if auth_req.get("type") != "auth_required":
            raise RuntimeError(f"unexpected first ws msg: {auth_req.get('type')}")
        ws.send(json.dumps({"type": "auth", "access_token": token}))
        auth_ok = json.loads(ws.recv())
        if auth_ok.get("type") != "auth_ok":
            raise RuntimeError(f"ws auth failed: {auth_ok}")
        payload = {"id": 1, "type": msg_type, **extra}
        ws.send(json.dumps(payload))
        while True:
            raw = ws.recv()
            msg = json.loads(raw)
            if msg.get("id") != 1:
                continue
            if not msg.get("success", True) and msg.get("type") == "result":
                err = msg.get("error") or {}
                raise RuntimeError(err.get("message") or str(err) or "ws command failed")
            if msg.get("type") == "result":
                if msg.get("success") is False:
                    err = msg.get("error") or {}
                    raise RuntimeError(err.get("message") or str(err) or "ws command failed")
                return msg.get("result")
            raise RuntimeError(f"unexpected ws reply: {msg.get('type')}")
    finally:
        try:
            ws.close()
        except Exception:
            pass


def _agent_from_preferred_pipeline(api_base: str, token: str) -> str:
    """读首选 Assist 流水线的 conversation_engine（与网页默认语音助手一致）。"""
    # 1) get 不带 id → 首选流水线
    try:
        pipe = _ws_command(api_base, token, "assist_pipeline/pipeline/get")
        if isinstance(pipe, dict):
            engine = str(pipe.get("conversation_engine") or "").strip()
            if engine:
                log.info(
                    "首选流水线 conversation_engine=%s name=%s id=%s",
                    engine,
                    pipe.get("name") or "-",
                    pipe.get("id") or "-",
                )
                return engine
    except Exception as e:
        log.debug("assist_pipeline/pipeline/get: %s", e)

    # 2) list → preferred_pipeline + pipelines[].conversation_engine
    try:
        data = _ws_command(api_base, token, "assist_pipeline/pipeline/list")
        if not isinstance(data, dict):
            return ""
        preferred = str(data.get("preferred_pipeline") or "").strip()
        pipelines = data.get("pipelines") or []
        if preferred.startswith("conversation.") and preferred:
            log.info("首选流水线即 conversation 实体 agent_id=%s", preferred)
            return preferred
        if isinstance(pipelines, list):
            for p in pipelines:
                if not isinstance(p, dict):
                    continue
                if preferred and str(p.get("id") or "") != preferred:
                    continue
                engine = str(p.get("conversation_engine") or "").strip()
                if engine:
                    log.info(
                        "pipeline/list 首选 conversation_engine=%s preferred=%s name=%s",
                        engine,
                        preferred or "-",
                        p.get("name") or "-",
                    )
                    return engine
            # preferred 对不上时，仍取列表第一项的引擎不如静默失败
    except Exception as e:
        log.debug("assist_pipeline/pipeline/list: %s", e)

    # 3) conversation/agent/list 的 default_agent（旧版字段，有则用）
    try:
        data = _ws_command(api_base, token, "conversation/agent/list")
        if isinstance(data, dict):
            default = str(data.get("default_agent") or "").strip()
            if default:
                log.info("conversation/agent/list default_agent=%s", default)
                return default
    except Exception as e:
        log.debug("conversation/agent/list: %s", e)

    return ""


def _discover_agent_id(api_base: str, token: str) -> str:
    """自动获取与网页 Assist 相同的对话代理；不靠名称查表。"""
    global _resolved_agent
    configured = _env("HA_AGENT_ID")
    if configured:
        return configured
    if _resolved_agent:
        return _resolved_agent

    agent = _agent_from_preferred_pipeline(api_base, token)
    if agent:
        _resolved_agent = agent
        return agent
    log.warning(
        "未能从首选 Assist 流水线读取 conversation_engine；"
        "将不传 agent_id（HA 默认内置意图）。可在 App 填写 conversation_agent 覆盖"
    )
    return ""


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
    timeout_s = float(_env("HA_TIMEOUT_S", "45") or "45")

    last_err: Optional[str] = None
    saw_401 = False
    with httpx.Client(timeout=timeout_s) as client:

        def _call(api_base: str, token: str) -> dict[str, Any]:
            agent_id = _discover_agent_id(api_base, token)
            body: dict[str, Any] = {"text": text, "language": language}
            if conversation_id:
                body["conversation_id"] = conversation_id
            if agent_id:
                body["agent_id"] = agent_id
            else:
                log.warning("未指定 agent_id，HA 将用内置意图引擎（可能答非所问）")
            url = f"{api_base}/conversation/process"
            data = _post(client, url, token, body)
            log.info("HA Assist agent=%s", agent_id or "(default home_assistant)")
            return data

        if _working:
            api_base, token = _working
            try:
                data = _call(api_base, token)
                return _ok(data, conversation_id, api_base)
            except Exception as e:
                last_err = str(e)
                log.warning("缓存地址失效 api=%s: %s", api_base, e)
                _working = None

        for label, raw, token in _attempts():
            api_base = _api_base_from(raw)
            try:
                data = _call(api_base, token)
            except httpx.HTTPStatusError as e:
                last_err = str(e)
                if e.response is not None and e.response.status_code == 401:
                    saw_401 = True
                log.warning("调用 HA Assist 失败 [%s] api=%s: %s", label, api_base, e)
                continue
            except Exception as e:
                last_err = str(e)
                log.warning("调用 HA Assist 失败 [%s] api=%s: %s", label, api_base, e)
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
