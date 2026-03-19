"""Command-line entrypoint for the stock screener web workbench."""

from __future__ import annotations

import os
from pathlib import Path

from quant_exchange.config import AppSettings
from quant_exchange.platform import QuantTradingPlatform
from quant_exchange.webapp.app import run_dev_server


def main() -> None:
    """Start the local stock screener development server."""

    project_root = Path(__file__).resolve().parents[3]
    database_path = os.getenv("QUANT_DB_URL", str(project_root / "data" / "runtime" / "quant_exchange.sqlite3"))
    host = os.getenv("QUANT_WEB_HOST", "127.0.0.1")
    port = int(os.getenv("QUANT_WEB_PORT", "8080"))
    platform = QuantTradingPlatform(AppSettings.from_mapping({"database": {"url": database_path}}))
    try:
        run_dev_server(platform, host=host, port=port)
    finally:
        platform.close()


if __name__ == "__main__":
    main()
