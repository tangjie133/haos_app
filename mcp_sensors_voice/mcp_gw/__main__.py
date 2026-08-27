from __future__ import annotations

import logging
import os
from pathlib import Path


def _load_dotenv() -> None:
    """可选加载 mcp_gw/.env（不依赖 python-dotenv）。"""
    here = Path(__file__).resolve().parent.parent
    env_path = here / ".env"
    if not env_path.is_file():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        k, v = s.split("=", 1)
        k, v = k.strip(), v.strip().strip('"').strip("'")
        if k and k not in os.environ:
            os.environ[k] = v


def main() -> None:
    _load_dotenv()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    from . import config
    import uvicorn

    uvicorn.run(
        "mcp_gw.app:app",
        host=config.HOST,
        port=config.PORT,
        log_level="info",
    )


if __name__ == "__main__":
    main()
