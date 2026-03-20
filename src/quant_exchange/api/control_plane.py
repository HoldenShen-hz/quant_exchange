"""Control-plane service methods that mirror the documented API design."""

from __future__ import annotations

import hashlib
import importlib.util
import secrets
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from quant_exchange.adapters.registry import AdapterRegistry
from quant_exchange.core.models import Action, AllocationMethod, AuditEvent, Instrument, Kline, OrderRequest, Role
from quant_exchange.execution.oms import OrderManager
from quant_exchange.ingestion.background_downloader import HistoryDownloadJobConfig
from quant_exchange.persistence.database import SQLitePersistence
from quant_exchange.rules.engine import MarketRuleEngine
from quant_exchange.scheduler.service import JobScheduler, ScheduledJob


class ControlPlaneAPI:
    """Expose application services with endpoint-like methods and response envelopes."""

    def __init__(
        self,
        *,
        platform,
        persistence: SQLitePersistence,
        adapter_registry: AdapterRegistry,
        scheduler: JobScheduler,
        market_rules: MarketRuleEngine,
    ) -> None:
        self.platform = platform
        self.persistence = persistence
        self.adapters = adapter_registry
        self.scheduler = scheduler
        self.market_rules = market_rules
        self.sessions: dict[str, dict[str, Any]] = {}
        self._watchlist_groups: dict[str, dict] = {}

    def create_user(self, username: str, password: str, role: Role, display_name: str | None = None) -> dict:
        """Create a user and assign an initial role."""

        existing = self.persistence.fetch_one("sys_users", where="username = :username", params={"username": username})
        if existing is not None:
            return self._error("ALREADY_EXISTS", "Username already exists.")
        password_hash = hashlib.sha256(password.encode("utf-8")).hexdigest()
        payload = {
            "username": username,
            "display_name": display_name or username,
            "password_hash": password_hash,
            "status": "ACTIVE",
            "roles": [role.value],
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        self.persistence.upsert_record(
            "sys_users",
            "username",
            username,
            payload,
            extra_columns={
                "display_name": payload["display_name"],
                "password_hash": password_hash,
                "status": payload["status"],
            },
        )
        self.persistence.upsert_record(
            "sys_roles",
            "role_code",
            role.value,
            {"role_code": role.value, "role_name": role.value.title()},
            extra_columns={"role_name": role.value.title()},
        )
        self.persistence.upsert_record(
            "sys_user_roles",
            "username",
            username,
            {"username": username, "role_code": role.value},
            extra_columns={"role_code": role.value},
        )
        return self._ok(payload)

    def login(self, username: str, password: str) -> dict:
        """Authenticate a user and return a simple bearer token payload."""

        row = self.persistence.fetch_one("sys_users", where="username = :username", params={"username": username})
        if row is None:
            return self._error("AUTH_FAILED", "Unknown username.")
        if row["password_hash"] != hashlib.sha256(password.encode("utf-8")).hexdigest():
            return self._error("AUTH_FAILED", "Invalid password.")
        roles = [item["role_code"] for item in self.persistence.fetch_all("sys_user_roles", where="username = :u", params={"u": username})]
        token = secrets.token_hex(16)
        user_payload = row["payload"]
        self.sessions[token] = {
            "username": username,
            "roles": roles,
            "display_name": user_payload.get("display_name") or username,
            "issued_at": datetime.now(timezone.utc).isoformat(),
        }
        return self._ok(
            {
                "access_token": token,
                "expires_in": 7200,
                "user": {
                    "username": username,
                    "display_name": user_payload.get("display_name") or username,
                    "roles": roles,
                },
            }
        )

    def current_user(self, token: str) -> dict:
        """Return the current user for a previously issued token."""

        session = self.sessions.get(token)
        if session is None:
            return self._error("UNAUTHORIZED", "Invalid token.")
        return self._ok(session)

    def logout(self, token: str) -> dict:
        """Invalidate one previously issued token."""

        self.sessions.pop(token, None)
        return self._ok({"logged_out": True})

    def register_web_user(self, username: str, password: str, display_name: str | None = None) -> dict:
        """Create a standard web user account and issue a login token."""

        created = self.create_user(username, password, Role.RESEARCHER, display_name=display_name)
        if created["code"] != "OK":
            return created
        return self.login(username, password)

    def list_users(self) -> dict:
        """List users stored in the persistence layer."""

        return self._ok([row["payload"] for row in self.persistence.fetch_all("sys_users")])

    def create_exchange(self, exchange_code: str, exchange_name: str, market_type: str, status: str = "ACTIVE") -> dict:
        """Create or update an exchange definition."""

        payload = {
            "exchange_code": exchange_code,
            "exchange_name": exchange_name,
            "market_type": market_type,
            "status": status,
        }
        self.persistence.upsert_record(
            "ref_exchanges",
            "exchange_code",
            exchange_code,
            payload,
            extra_columns={"exchange_name": exchange_name, "market_type": market_type, "status": status},
        )
        return self._ok(payload)

    def list_exchanges(self) -> dict:
        """List configured exchanges."""

        return self._ok([row["payload"] for row in self.persistence.fetch_all("ref_exchanges")])

    def sync_instruments(self, exchange_code: str) -> dict:
        """Import instruments from a registered market data adapter."""

        adapter = self.adapters.get_market_data(exchange_code)
        imported = []
        for instrument in adapter.fetch_instruments():
            payload = self._serialize(instrument)
            self.persistence.upsert_record(
                "ref_instruments",
                "instrument_id",
                instrument.instrument_id,
                payload,
                extra_columns={
                    "symbol": instrument.symbol,
                    "market_type": instrument.market.value,
                    "market_region": instrument.market_region,
                    "instrument_type": instrument.instrument_type,
                    "status": "ACTIVE",
                },
            )
            self.platform.register_instrument(instrument)
            imported.append(payload)
        return self._ok(imported)

    def list_instruments(self) -> dict:
        """List normalized instruments."""

        return self._ok([row["payload"] for row in self.persistence.fetch_all("ref_instruments")])

    def stock_filter_options(self) -> dict:
        """Return stock screener filter options for the web workbench."""

        return self._ok(self.platform.stocks.available_filters())

    def list_stocks(self, **filters) -> dict:
        """Return stock screener results for the provided filter set."""

        return self._ok(self.platform.stocks.list_stocks(filters))

    def count_stocks(self, **filters) -> dict:
        """Return the number of stocks matching the provided screener filters."""

        return self._ok({"count": self.platform.stocks.count_stocks(filters)})

    def stock_universe_summary(self, featured_limit: int = 24) -> dict:
        """Return a compact summary of the full stock universe."""

        return self._ok(self.platform.stocks.universe_summary(featured_limit=featured_limit))

    def crypto_universe_summary(self, featured_limit: int = 12) -> dict:
        """Return a compact summary of the crypto universe."""

        return self._ok(self.platform.crypto.universe_summary(featured_limit=featured_limit))

    def list_crypto_assets(self) -> dict:
        """Return all supported crypto assets for the web page."""

        return self._ok(self.platform.crypto.list_assets())

    def get_crypto_detail(self, instrument_id: str) -> dict:
        """Return one crypto asset detail record."""

        try:
            return self._ok(self.platform.crypto.get_asset(instrument_id))
        except KeyError:
            return self._error("NOT_FOUND", "Crypto instrument not found.")

    def get_crypto_history(self, instrument_id: str, *, interval: str = "1d", limit: int = 120) -> dict:
        """Return OHLCV history for one crypto asset."""

        try:
            return self._ok(self.platform.crypto.get_asset_history(instrument_id, interval=interval, limit=limit))
        except KeyError:
            return self._error("NOT_FOUND", "Crypto instrument not found.")

    # ── Futures Workbench ──

    def list_futures_contracts(self) -> dict:
        """Return all futures contracts from the simulated exchange."""
        try:
            return self._ok({"contracts": self.platform.futures.list_contracts()})
        except Exception as exc:
            return self._error("TEMPORARILY_UNAVAILABLE", f"Futures service unavailable: {exc}")

    def futures_universe_summary(self) -> dict:
        """Return a summary overview of the futures market."""
        try:
            return self._ok(self.platform.futures.universe_summary())
        except Exception as exc:
            return self._error("TEMPORARILY_UNAVAILABLE", f"Futures service unavailable: {exc}")

    def get_futures_detail(self, instrument_id: str) -> dict:
        """Return detailed info for one futures contract."""
        try:
            return self._ok(self.platform.futures.get_contract(instrument_id))
        except KeyError:
            return self._error("NOT_FOUND", f"Futures contract {instrument_id} not found.")
        except Exception as exc:
            return self._error("TEMPORARILY_UNAVAILABLE", f"Futures service unavailable: {exc}")

    def get_futures_klines(self, instrument_id: str, *, interval: str = "1d", limit: int = 120) -> dict:
        """Return historical kline data for a futures contract."""
        try:
            return self._ok(self.platform.futures.get_contract_history(instrument_id, interval=interval, limit=limit))
        except KeyError:
            return self._error("NOT_FOUND", f"Futures contract {instrument_id} not found.")
        except Exception as exc:
            return self._error("TEMPORARILY_UNAVAILABLE", f"Futures service unavailable: {exc}")

    # ─── FT-08: Trading Calendar and Session Periods ────────────────────────────────

    def get_futures_trading_calendar(self) -> dict:
        """Return futures trading calendar with session periods (FT-08)."""
        try:
            return self._ok(self.platform.futures.trading_calendar())
        except Exception as exc:
            return self._error("TEMPORARILY_UNAVAILABLE", f"Futures service unavailable: {exc}")

    def get_futures_trading_sessions(self, instrument_id: str) -> dict:
        """Return trading sessions for a specific contract (FT-08)."""
        try:
            return self._ok(self.platform.futures.get_trading_sessions(instrument_id))
        except KeyError:
            return self._error("NOT_FOUND", f"Futures contract {instrument_id} not found.")
        except Exception as exc:
            return self._error("TEMPORARILY_UNAVAILABLE", f"Futures service unavailable: {exc}")

    # ─── FT-09: Main Contract and Continuous Contract Mapping ────────────────────────

    def get_main_contract(self, product_code: str) -> dict:
        """Return the main (front-month) contract for a product (FT-09)."""
        try:
            return self._ok(self.platform.futures.get_main_contract(product_code))
        except KeyError:
            return self._error("NOT_FOUND", f"No main contract for product: {product_code}")
        except Exception as exc:
            return self._error("TEMPORARILY_UNAVAILABLE", f"Futures service unavailable: {exc}")

    def get_continuous_contract(self, product_code: str) -> dict:
        """Return continuous contract chain for a product (FT-09)."""
        try:
            return self._ok(self.platform.futures.get_continuous_contract(product_code))
        except KeyError:
            return self._error("NOT_FOUND", f"No continuous contract chain for: {product_code}")
        except Exception as exc:
            return self._error("TEMPORARILY_UNAVAILABLE", f"Futures service unavailable: {exc}")

    def get_rollover_recommendation(self, product_code: str) -> dict:
        """Provide rollover recommendation based on position and expiry (FT-09)."""
        try:
            return self._ok(self.platform.futures.get_rollover_recommendation(product_code))
        except Exception as exc:
            return self._error("TEMPORARILY_UNAVAILABLE", f"Futures service unavailable: {exc}")

    # ─── FT-10: Futures Simulated Trading ─────────────────────────────────────────

    def get_futures_dashboard(self, account_code: str = "futures_main") -> dict:
        """Return futures trading dashboard (FT-10)."""
        try:
            return self._ok(self.platform.futures_trading.get_dashboard(account_code))
        except Exception as exc:
            return self._error("TEMPORARILY_UNAVAILABLE", f"Futures trading unavailable: {exc}")

    def submit_futures_order(
        self,
        account_code: str,
        instrument_id: str,
        direction: str,
        quantity: int,
        order_type: str = "market",
        limit_price: float | None = None,
        contract_multiplier: float = 1.0,
    ) -> dict:
        """Submit a futures order (FT-10)."""
        try:
            order = self.platform.futures_trading.submit_order(
                account_code=account_code,
                instrument_id=instrument_id,
                direction=direction,
                quantity=quantity,
                order_type=order_type,
                limit_price=limit_price,
                contract_multiplier=contract_multiplier,
            )
            return self._ok({"order": {
                "order_id": order.order_id,
                "instrument_id": order.instrument_id,
                "direction": order.direction,
                "quantity": order.quantity,
                "filled_quantity": order.filled_quantity,
                "avg_fill_price": order.avg_fill_price,
                "status": order.status,
            }})
        except Exception as exc:
            return self._error("TEMPORARILY_UNAVAILABLE", f"Futures order failed: {exc}")

    def get_futures_positions(self, account_code: str = "futures_main") -> dict:
        """Get all positions for a futures account (FT-10)."""
        try:
            return self._ok({"positions": self.platform.futures_trading.get_positions(account_code)})
        except Exception as exc:
            return self._error("TEMPORARILY_UNAVAILABLE", f"Futures positions unavailable: {exc}")

    def mark_futures_to_market(self, account_code: str, instrument_id: str, current_price: float) -> dict:
        """Mark positions to market price (FT-10)."""
        try:
            return self._ok(self.platform.futures_trading.mark_to_market(account_code, instrument_id, current_price))
        except Exception as exc:
            return self._error("TEMPORARILY_UNAVAILABLE", f"Mark to market failed: {exc}")

    # ─── FT-11: Unified Cross-Market Portfolio View ─────────────────────────────────

    def get_unified_portfolio_summary(
        self,
        stock_positions: list[dict[str, Any]] | None = None,
        crypto_positions: list[dict[str, Any]] | None = None,
        futures_positions: list[dict[str, Any]] | None = None,
    ) -> dict:
        """Return unified portfolio view across stocks, crypto, and futures (FT-11)."""
        try:
            return self._ok(self.platform.futures.unified_portfolio_summary(
                stock_positions=stock_positions,
                crypto_positions=crypto_positions,
                futures_positions=futures_positions,
            ))
        except Exception as exc:
            return self._error("TEMPORARILY_UNAVAILABLE", f"Unified portfolio unavailable: {exc}")

    def get_stock_detail(self, instrument_id: str) -> dict:
        """Return one stock detail record including F10-style fields."""

        return self._ok(self.platform.stocks.get_stock(instrument_id))

    def analyze_stock_financials(self, instrument_id: str) -> dict:
        """Return a financial analysis scorecard for one stock."""

        return self._ok(self.platform.stocks.analyze_financials(instrument_id))

    def get_stock_history(self, instrument_id: str, limit: int = 120) -> dict:
        """Return historical daily bars for one stock."""

        return self._ok(self.platform.stocks.get_stock_history(instrument_id, limit=limit))

    def get_stock_financial_history(self, instrument_id: str, limit: int = 8) -> dict:
        """Return historical financial snapshots for one stock."""

        return self._ok(self.platform.stocks.get_financial_history(instrument_id, limit=limit))

    def get_stock_minute_bars(self, instrument_id: str, limit: int = 240) -> dict:
        """Return persisted one-minute trading bars for one stock."""

        return self._ok(self.platform.stocks.get_minute_bars(instrument_id, limit=limit))

    def get_learning_hub(
        self,
        principal_id: str,
        *,
        principal_type: str = "client",
        client_id: str | None = None,
        username: str | None = None,
    ) -> dict:
        """Return the beginner-oriented learning hub payload plus current principal progress."""

        return self._ok(
            {
                "hub": self.platform.learning.hub_payload(),
                "progress": self.platform.web_workspace.load_learning_progress(
                    principal_id,
                    principal_type=principal_type,
                    client_id=client_id,
                    username=username,
                ),
            }
        )

    def submit_learning_quiz(
        self,
        answers: dict[str, str] | None = None,
        *,
        principal_id: str,
        principal_type: str = "client",
        client_id: str | None = None,
        username: str | None = None,
        current_lesson_id: str | None = None,
    ) -> dict:
        """Evaluate one submitted learning quiz answer set and persist user-specific progress."""

        # Ensure answers is a dict, not None or other type
        if not isinstance(answers, dict):
            answers = {}
        result = self.platform.learning.evaluate_quiz(answers)
        progress = self.platform.web_workspace.record_learning_attempt(
            principal_id,
            result,
            principal_type=principal_type,
            client_id=client_id,
            username=username,
            current_lesson_id=current_lesson_id,
        )
        return self._ok({"result": result, "progress": progress})

    def compare_stocks(self, left_instrument_id: str, right_instrument_id: str) -> dict:
        """Return side-by-side comparison data for two stocks."""

        return self._ok(self.platform.stocks.compare_stocks(left_instrument_id, right_instrument_id))

    def get_realtime_market_snapshot(self, instrument_ids: list[str] | None = None) -> dict:
        """Return the latest background-refreshed whole-market quote snapshot."""

        return self._ok(self.platform.realtime_market.snapshot(instrument_ids))

    def start_history_download_job(
        self,
        *,
        job_id: str,
        provider_code: str,
        output_dir: str,
        start_date: str = "2010-01-01",
        end_date: str | None = None,
        refresh_existing: bool = False,
        continuous: bool = False,
        max_retries: int = 3,
        rediscover_interval_seconds: int = 900,
        metadata: dict[str, Any] | None = None,
    ) -> dict:
        """Start or resume a resumable history-download job."""

        config = HistoryDownloadJobConfig(
            job_id=job_id,
            provider_code=provider_code,
            output_dir=output_dir,
            start_date=start_date,
            end_date=end_date,
            refresh_existing=refresh_existing,
            continuous=continuous,
            max_retries=max_retries,
            rediscover_interval_seconds=rediscover_interval_seconds,
            metadata=metadata or {},
        )
        return self._ok(self.platform.history_downloads.start_job(config))

    def pause_history_download_job(self, job_id: str) -> dict:
        """Request a graceful pause for one download job."""

        return self._ok(self.platform.history_downloads.pause_job(job_id))

    def get_history_download_job(self, job_id: str) -> dict:
        """Return the latest checkpoint state for one download job."""

        return self._ok(self.platform.history_downloads.job_status(job_id))

    def list_history_download_jobs(self) -> dict:
        """List all persisted history-download jobs."""

        return self._ok(self.platform.history_downloads.list_jobs())

    def stop_history_download_job(self, job_id: str) -> dict:
        """Cancel one download job."""

        return self._ok(self.platform.history_downloads.stop_job(job_id))

    def list_history_download_overview(self) -> dict:
        """Return a web-friendly list of download jobs including default templates."""

        templates = {item["job_id"]: item for item in self._history_download_templates()}
        runtime = {item["config"]["job_id"]: item for item in self.platform.history_downloads.list_jobs() if item.get("config")}
        items: list[dict[str, Any]] = []
        for job_id, template in templates.items():
            status = runtime.get(job_id)
            merged = {
                **template,
                "status": "not_started" if template["supported"] else "unsupported",
                "completed_count": 0,
                "pending_count": 0,
                "failed_count": 0,
                "downloaded_rows": 0,
                "total_discovered": 0,
                "last_result": None,
                "last_error": None,
                "updated_at": None,
                "finished_at": None,
            }
            if status:
                merged.update(
                    {
                        "status": status.get("status", merged["status"]),
                        "completed_count": status.get("completed_count", 0),
                        "pending_count": status.get("pending_count", 0),
                        "failed_count": status.get("failed_count", 0),
                        "downloaded_rows": status.get("downloaded_rows", 0),
                        "total_discovered": status.get("total_discovered", 0),
                        "last_result": status.get("last_result"),
                        "last_error": status.get("last_error"),
                        "updated_at": status.get("updated_at"),
                        "finished_at": status.get("finished_at"),
                    }
                )
            if not template["supported"]:
                merged["status"] = "unsupported"
            merged["available_actions"] = {
                "download": bool(template["supported"]),
                "pause": bool(template["supported"] and merged["status"] in {"running", "pause_requested"}),
                "stop": bool(template["supported"] and merged["status"] in {"running", "pause_requested", "paused", "cancel_requested"}),
            }
            items.append(merged)
        return self._ok(sorted(items, key=lambda item: item["sort_order"]))

    def start_default_history_download_job(self, job_id: str) -> dict:
        """Start or resume one predefined download job."""

        template = self._history_download_template(job_id)
        if template is None:
            return self._error("NOT_FOUND", "Unknown download job.")
        if not template["supported"]:
            return self._error("NOT_SUPPORTED", "This market download provider is not wired yet.")
        return self.start_history_download_job(
            job_id=template["job_id"],
            provider_code=template["provider_code"],
            output_dir=template["output_dir"],
            start_date=template["start_date"],
            metadata=template.get("metadata"),
        )

    def pause_default_history_download_job(self, job_id: str) -> dict:
        """Pause one predefined download job."""

        template = self._history_download_template(job_id)
        if template is None:
            return self._error("NOT_FOUND", "Unknown download job.")
        if not template["supported"]:
            return self._error("NOT_SUPPORTED", "This market download provider is not wired yet.")
        return self.pause_history_download_job(job_id)

    def stop_default_history_download_job(self, job_id: str) -> dict:
        """Cancel one predefined download job."""

        template = self._history_download_template(job_id)
        if template is None:
            return self._error("NOT_FOUND", "Unknown download job.")
        if not template["supported"]:
            return self._error("NOT_SUPPORTED", "This market download provider is not wired yet.")
        return self.stop_history_download_job(job_id)

    def get_web_workspace_state(
        self,
        principal_id: str,
        *,
        principal_type: str = "client",
        client_id: str | None = None,
        username: str | None = None,
    ) -> dict:
        """Return the persisted web workspace state for one browser client or authenticated user."""

        return self._ok(
            self.platform.web_workspace.load_state(
                principal_id,
                principal_type=principal_type,
                client_id=client_id,
                username=username,
            )
        )

    def save_web_workspace_state(
        self,
        principal_id: str,
        state: dict[str, Any],
        *,
        principal_type: str = "client",
        client_id: str | None = None,
        username: str | None = None,
    ) -> dict:
        """Persist the current web workspace state for one browser client or authenticated user."""

        return self._ok(
            self.platform.web_workspace.save_state(
                principal_id,
                state,
                principal_type=principal_type,
                client_id=client_id,
                username=username,
            )
        )

    def record_web_activity(
        self,
        principal_id: str,
        event_type: str,
        *,
        payload: dict[str, Any] | None = None,
        path: str | None = None,
        principal_type: str = "client",
        client_id: str | None = None,
        username: str | None = None,
    ) -> dict:
        """Record one web UI activity event."""

        return self._ok(
            self.platform.web_workspace.log_event(
                principal_id,
                event_type,
                payload=payload,
                path=path,
                principal_type=principal_type,
                client_id=client_id,
                username=username,
            )
        )

    def _history_download_template(self, job_id: str) -> dict[str, Any] | None:
        for item in self._history_download_templates():
            if item["job_id"] == job_id:
                return item
        return None

    def _history_download_templates(self) -> list[dict[str, Any]]:
        project_root = Path(__file__).resolve().parents[3]
        a_share_supported = self._python_module_available("baostock")
        return [
            {
                "job_id": "a_share_daily_history",
                "title": "A股历史日线",
                "market_region": "CN",
                "description": "使用 BaoStock 后台批量下载 A 股日线数据，支持断点续传。"
                if a_share_supported
                else "当前 Python 环境未安装 BaoStock，A 股真实历史下载暂不可用。",
                "provider_code": "a_share_baostock",
                "output_dir": str(project_root / "data" / "cn_equities" / "a_share" / "daily_raw"),
                "start_date": "2010-01-01",
                "source_name": "BaoStock",
                "granularity": "1d",
                "supported": a_share_supported,
                "sort_order": 10,
            },
            {
                "job_id": "hk_equity_daily_history",
                "title": "港股历史日线",
                "market_region": "HK",
                "description": "批量下载港股主要标的日线数据（模拟生成），覆盖恒生科技、蓝筹等 20 只标的，支持断点续传。",
                "provider_code": "hk_simulated",
                "output_dir": str(project_root / "data" / "hk_equities" / "daily_raw"),
                "start_date": "2020-01-01",
                "source_name": "SimulatedHK",
                "granularity": "1d",
                "supported": True,
                "sort_order": 20,
                "metadata": {"market": "HK"},
            },
            {
                "job_id": "us_equity_daily_history",
                "title": "美股历史日线",
                "market_region": "US",
                "description": "批量下载美股主要标的日线数据（模拟生成），覆盖科技、金融、消费等 20 只标的，支持断点续传。",
                "provider_code": "us_simulated",
                "output_dir": str(project_root / "data" / "us_equities" / "daily_raw"),
                "start_date": "2020-01-01",
                "source_name": "SimulatedUS",
                "granularity": "1d",
                "supported": True,
                "sort_order": 30,
                "metadata": {"market": "US"},
            },
        ]

    def _python_module_available(self, module_name: str) -> bool:
        """Return whether a runtime dependency is importable in the current Python environment."""

        return importlib.util.find_spec(module_name) is not None

    def list_web_activity(
        self,
        principal_id: str,
        *,
        principal_type: str = "client",
        client_id: str | None = None,
        limit: int = 20,
    ) -> dict:
        """Return recent web UI activity events for one browser client or authenticated user."""

        return self._ok(
            {
                "client_id": client_id or principal_id,
                "principal_type": principal_type,
                "principal_id": principal_id,
                "events": self.platform.web_workspace.list_events(
                    principal_id,
                    principal_type=principal_type,
                    limit=limit,
                ),
            }
        )

    def resolve_session(self, token: str | None) -> dict[str, Any] | None:
        """Return the active session payload for a bearer token when available."""

        if not token:
            return None
        return self.sessions.get(token)

    def get_paper_trading_dashboard(self, account_code: str = "paper_stock_main", instrument_id: str | None = None) -> dict:
        """Return the simulated-trading dashboard for one paper account."""

        try:
            return self._ok(self.platform.paper_trading.dashboard(account_code=account_code, instrument_id=instrument_id))
        except KeyError:
            return self._error("NOT_FOUND", "Instrument not found.")
        except Exception as exc:  # pragma: no cover - defensive runtime isolation
            return self._error("TEMPORARILY_UNAVAILABLE", f"Paper trading dashboard unavailable: {exc}")

    def submit_paper_order(
        self,
        *,
        instrument_id: str,
        side: str,
        quantity: float,
        account_code: str = "paper_stock_main",
        order_type: str = "market",
        limit_price: float | None = None,
    ) -> dict:
        """Submit one simulated order against the paper-trading account."""

        try:
            payload = self.platform.paper_trading.submit_order(
                instrument_id=instrument_id,
                side=side,
                quantity=quantity,
                account_code=account_code,
                order_type=order_type,
                limit_price=limit_price,
            )
        except KeyError:
            return self._error("NOT_FOUND", "Instrument not found.")
        except ValueError as exc:
            return self._error("BAD_REQUEST", str(exc))
        return self._ok(payload)

    def cancel_paper_order(self, order_id: str, *, account_code: str = "paper_stock_main") -> dict:
        """Cancel one simulated order."""

        try:
            return self._ok(self.platform.paper_trading.cancel_order(order_id, account_code=account_code))
        except KeyError:
            return self._error("NOT_FOUND", "Order not found.")

    def reset_paper_account(self, account_code: str = "paper_stock_main") -> dict:
        """Reset one paper-trading account to its initial state."""

        return self._ok(self.platform.paper_trading.reset_account(account_code=account_code))

    def list_strategy_templates(self) -> dict:
        """Return hosted-strategy templates inspired by strategy-bot platforms."""

        return self._ok(self.platform.bot_center.list_templates())

    def list_strategy_bots(self) -> dict:
        """Return all strategy bots with refreshed runtime metrics."""

        return self._ok(self.platform.bot_center.list_bots())

    def create_strategy_bot(
        self,
        *,
        template_code: str,
        instrument_id: str,
        bot_name: str | None = None,
        mode: str = "paper",
        params: dict | None = None,
    ) -> dict:
        """Create a strategy bot from a template."""

        try:
            payload = self.platform.bot_center.create_bot(
                template_code=template_code,
                instrument_id=instrument_id,
                bot_name=bot_name,
                mode=mode,
                params=params,
            )
        except KeyError as exc:
            return self._error("NOT_FOUND", f"Unknown template or instrument: {exc.args[0]}")
        except ValueError as exc:
            return self._error("BAD_REQUEST", str(exc))
        return self._ok(payload)

    def start_strategy_bot(self, bot_id: str) -> dict:
        """Start one strategy bot."""

        try:
            return self._ok(self.platform.bot_center.start_bot(bot_id))
        except KeyError:
            return self._error("NOT_FOUND", "Bot not found.")

    def pause_strategy_bot(self, bot_id: str) -> dict:
        """Pause one strategy bot."""

        try:
            return self._ok(self.platform.bot_center.pause_bot(bot_id))
        except KeyError:
            return self._error("NOT_FOUND", "Bot not found.")

    def stop_strategy_bot(self, bot_id: str) -> dict:
        """Stop one strategy bot."""

        try:
            return self._ok(self.platform.bot_center.stop_bot(bot_id))
        except KeyError:
            return self._error("NOT_FOUND", "Bot not found.")

    def interact_strategy_bot(self, bot_id: str, command: str, payload: dict | None = None) -> dict:
        """Execute one interactive command against a strategy bot."""

        try:
            return self._ok(self.platform.bot_center.interact(bot_id, command, payload))
        except KeyError:
            return self._error("NOT_FOUND", "Bot not found.")
        except ValueError as exc:
            return self._error("BAD_REQUEST", str(exc))

    def list_strategy_notifications(self, limit: int = 20) -> dict:
        """Return recent strategy-bot notifications."""

        return self._ok({"notifications": self.platform.bot_center.list_notifications(limit=limit)})

    def sync_klines(self, exchange_code: str, instrument_id: str, interval: str) -> dict:
        """Import bars from a registered market data adapter."""

        adapter = self.adapters.get_market_data(exchange_code)
        bars = adapter.fetch_klines(instrument_id, interval)
        self.platform.market_data.ingest_klines(bars)
        for bar in bars:
            self.persistence.upsert_record(
                "mkt_market_klines",
                "open_time",
                f"{bar.instrument_id}:{bar.timeframe}:{bar.open_time.isoformat()}",
                self._serialize(bar),
                extra_columns={
                    "instrument_id": bar.instrument_id,
                    "interval": bar.timeframe,
                    "close_time": bar.close_time.isoformat(),
                    "close_price": bar.close,
                },
            )
        return self._ok([self._serialize(bar) for bar in bars])

    def query_klines(self, instrument_id: str, interval: str) -> dict:
        """Return bar history from the in-memory market data store."""

        bars = self.platform.market_data.query_klines(instrument_id, interval)
        return self._ok([self._serialize(bar) for bar in bars])

    def create_account(self, account_code: str, account_name: str, market_type: str, environment: str = "PAPER") -> dict:
        """Create a trading account record."""

        payload = {
            "account_code": account_code,
            "account_name": account_name,
            "market_type": market_type,
            "environment": environment,
            "status": "ACTIVE",
        }
        self.persistence.upsert_record(
            "acct_trading_accounts",
            "account_code",
            account_code,
            payload,
            extra_columns={
                "account_name": account_name,
                "market_type": market_type,
                "environment": environment,
                "status": "ACTIVE",
            },
        )
        return self._ok(payload)

    def list_accounts(self) -> dict:
        """List trading accounts."""

        return self._ok([row["payload"] for row in self.persistence.fetch_all("acct_trading_accounts")])

    def create_strategy(self, strategy_code: str, strategy_name: str, category: str) -> dict:
        """Create a strategy metadata record."""

        payload = {
            "strategy_code": strategy_code,
            "strategy_name": strategy_name,
            "category": category,
            "status": "ACTIVE",
        }
        self.persistence.upsert_record(
            "strat_strategies",
            "strategy_code",
            strategy_code,
            payload,
            extra_columns={"strategy_name": strategy_name, "category": category, "status": "ACTIVE"},
        )
        return self._ok(payload)

    def run_backtest(self, strategy_code: str, instrument_id: str, interval: str, initial_cash: float = 100_000.0) -> dict:
        """Execute a backtest through the platform services and persist the summary."""

        instrument_row = self.persistence.fetch_one(
            "ref_instruments", where="instrument_id = :instrument_id", params={"instrument_id": instrument_id}
        )
        if instrument_row is None:
            return self._error("NOT_FOUND", "Instrument not found.")
        bars = self.platform.market_data.query_klines(instrument_id, interval)
        if not bars:
            return self._error("NO_DATA", "No bars available for backtest.")
        strategy = self.platform.strategy_registry.get(strategy_code)
        result = self.platform.backtest.run(
            instrument=self.platform.market_data.instruments[instrument_id],
            klines=bars,
            strategy=strategy,
            intelligence_engine=self.platform.intelligence,
            risk_engine=self.platform.risk,
            initial_cash=initial_cash,
        )
        payload = {
            "run_id": f"run:{strategy_code}:{instrument_id}:{len(result.equity_curve)}",
            "strategy_code": strategy_code,
            "mode": "BACKTEST",
            "status": "COMPLETED",
            "metrics": self._serialize(result.metrics),
            "order_count": len(result.orders),
            "fill_count": len(result.fills),
        }
        self.persistence.upsert_record(
            "strat_strategy_runs",
            "run_id",
            payload["run_id"],
            payload,
            extra_columns={"strategy_code": strategy_code, "mode": "BACKTEST", "status": "COMPLETED"},
        )
        return self._ok(payload)

    def create_intel_source(self, source_code: str, source_name: str, source_type: str) -> dict:
        """Register a market intelligence source."""

        payload = {
            "source_code": source_code,
            "source_name": source_name,
            "source_type": source_type,
            "status": "ACTIVE",
        }
        self.persistence.upsert_record(
            "intel_sources",
            "source_code",
            source_code,
            payload,
            extra_columns={"source_name": source_name, "source_type": source_type, "status": "ACTIVE"},
        )
        return self._ok(payload)

    def ingest_intel_documents(self, documents: list) -> dict:
        """Process documents through the intelligence engine and persist outputs."""

        results = self.platform.intelligence.ingest_documents(documents)
        for document in documents:
            self.persistence.upsert_record(
                "intel_documents",
                "document_uid",
                document.document_id,
                self._serialize(document),
                extra_columns={
                    "source_code": document.source,
                    "instrument_id": document.instrument_id,
                    "published_at": document.published_at.isoformat(),
                },
            )
        for result in results:
            self.persistence.upsert_record(
                "intel_sentiment_scores",
                "sentiment_key",
                result.document_id,
                self._serialize(result),
                extra_columns={
                    "document_uid": result.document_id,
                    "instrument_id": result.instrument_id,
                    "sentiment_label": result.label.value,
                },
            )
        return self._ok([self._serialize(result) for result in results])

    def list_documents(self) -> dict:
        """List persisted intelligence documents."""

        return self._ok([row["payload"] for row in self.persistence.fetch_all("intel_documents")])

    def list_sentiment_scores(self) -> dict:
        """List persisted sentiment scores."""

        return self._ok([row["payload"] for row in self.persistence.fetch_all("intel_sentiment_scores")])

    def list_directional_signals(self) -> dict:
        """List persisted directional signals."""

        return self._ok([row["payload"] for row in self.persistence.fetch_all("intel_directional_signals")])

    def create_risk_rule(self, rule_code: str, rule_type: str, action_type: str, parameters: dict) -> dict:
        """Persist a risk rule definition."""

        payload = {
            "rule_code": rule_code,
            "rule_type": rule_type,
            "action_type": action_type,
            "parameters": parameters,
        }
        self.persistence.upsert_record(
            "risk_risk_rules",
            "rule_code",
            rule_code,
            payload,
            extra_columns={"rule_type": rule_type, "action_type": action_type},
        )
        return self._ok(payload)

    def submit_order(self, exchange_code: str, request: OrderRequest, *, as_of: datetime, available_position_qty: float = 0.0) -> dict:
        """Validate and submit an order through market rules, risk, OMS, and adapter."""

        instrument = self.platform.market_data.instruments[request.instrument_id]
        market_decision = self.market_rules.validate_order(
            instrument,
            request,
            as_of=as_of,
            available_position_qty=available_position_qty,
        )
        if not market_decision.approved:
            return self._error("MARKET_RULE_REJECTED", ",".join(market_decision.reasons))
        snapshot = self.platform.portfolio.mark_to_market({request.instrument_id: self.platform.market_data.latest_price(request.instrument_id)})
        risk_decision = self.platform.risk.evaluate_order(
            request,
            price=self.platform.market_data.latest_price(request.instrument_id),
            current_position_qty=available_position_qty,
            snapshot=snapshot,
        )
        if not risk_decision.approved:
            return self._error("RISK_REJECTED", ",".join(risk_decision.reasons))
        order = self.platform.oms.submit_order(request)
        venue_response = self.adapters.get_execution(exchange_code).submit_order(request)
        self.persistence.upsert_record(
            "trade_orders",
            "order_id",
            order.order_id,
            self._serialize(order) | {"venue_response": venue_response},
            extra_columns={
                "client_order_id": request.client_order_id,
                "instrument_id": request.instrument_id,
                "status": order.status.value,
            },
        )
        return self._ok({"order_id": order.order_id, "venue_response": venue_response})

    def list_orders(self) -> dict:
        """List persisted orders."""

        return self._ok([row["payload"] for row in self.persistence.fetch_all("trade_orders")])

    def cancel_order(self, exchange_code: str, order_id: str) -> dict:
        """Cancel an existing order and forward the cancellation to the adapter."""

        order = self.platform.oms.cancel_order(order_id)
        venue_response = self.adapters.get_execution(exchange_code).cancel_order(order_id)
        self.persistence.upsert_record(
            "trade_orders",
            "order_id",
            order.order_id,
            self._serialize(order) | {"venue_response": venue_response},
            extra_columns={
                "client_order_id": order.request.client_order_id,
                "instrument_id": order.request.instrument_id,
                "status": order.status.value,
            },
        )
        return self._ok({"order_id": order.order_id, "venue_response": venue_response})

    def register_job(self, job_code: str, job_name: str, job_type: str, interval_seconds: int, callback) -> dict:
        """Register a scheduled job."""

        job = ScheduledJob(job_code=job_code, job_name=job_name, job_type=job_type, interval_seconds=interval_seconds, callback=callback)
        self.scheduler.register_job(job)
        return self._ok({"job_code": job_code, "job_name": job_name, "interval_seconds": interval_seconds})

    def run_jobs(self) -> dict:
        """Run all due jobs once."""

        return self._ok(self.scheduler.run_due_jobs())

    def report_daily_account(self) -> dict:
        """Return a compact account report."""

        snapshot = self.platform.portfolio.mark_to_market(
            {instrument_id: self.platform.market_data.latest_price(instrument_id) for instrument_id in self.platform.market_data.instruments}
            or {}
        )
        return self._ok(self.platform.reporting.daily_summary(snapshot=snapshot, alerts=self.platform.monitoring.alerts))

    def report_risk_summary(self) -> dict:
        """Return a risk event summary report."""

        return self._ok(self.platform.reporting.risk_summary(
            alerts=self.platform.monitoring.alerts,
            risk_rejections=len(self.platform.risk.evaluation_log),
        ))

    def report_cost_analysis(self) -> dict:
        """Return a cost analysis report from OMS fills."""

        return self._ok(self.platform.reporting.cost_analysis(fills=self.platform.oms.fills))

    def activate_kill_switch(self) -> dict:
        """Activate the system-wide kill switch to block all new orders."""

        self.platform.risk.activate_kill_switch()
        return self._ok({"kill_switch": True})

    def release_kill_switch(self) -> dict:
        """Release the system-wide kill switch to resume order flow."""

        self.platform.risk.release_kill_switch()
        return self._ok({"kill_switch": False})

    def get_monitoring_metrics(self) -> dict:
        """Return current monitoring metrics."""

        return self._ok(self.platform.monitoring.metrics)

    def get_monitoring_alerts(self, severity: str | None = None) -> dict:
        """Return alerts, optionally filtered by severity."""

        from quant_exchange.core.models import AlertSeverity
        alerts = self.platform.monitoring.alerts
        if severity:
            try:
                sev = AlertSeverity(severity)
                alerts = self.platform.monitoring.alerts_by_severity(sev)
            except ValueError:
                pass
        return self._ok([{
            "code": a.code, "severity": a.severity.value, "message": a.message,
            "timestamp": a.timestamp.isoformat(), "context": a.context,
        } for a in alerts])

    def get_directional_bias(self, instrument_id: str, window_hours: int = 24) -> dict:
        """Return the current directional bias for one instrument."""

        from datetime import timedelta
        bias = self.platform.intelligence.directional_bias(
            instrument_id,
            as_of=datetime.now(timezone.utc),
            window=timedelta(hours=window_hours),
        )
        return self._ok(self._serialize(bias))

    def get_aggregate_sentiment(self, instrument_id: str, window_hours: int = 1) -> dict:
        """Return aggregated sentiment statistics for one instrument."""

        from datetime import timedelta
        return self._ok(self.platform.intelligence.aggregate_sentiment(
            instrument_id,
            as_of=datetime.now(timezone.utc),
            window=timedelta(hours=window_hours),
        ))

    def compute_portfolio_allocation(
        self,
        method: str,
        instrument_ids: list[str],
        **kwargs,
    ) -> dict:
        """Compute portfolio allocation using the specified method."""

        try:
            alloc_method = AllocationMethod(method)
        except ValueError:
            return self._error("BAD_REQUEST", f"Unknown allocation method: {method}")
        weights = self.platform.portfolio.compute_allocation(alloc_method, instrument_ids, **kwargs)
        return self._ok(weights)

    def authorize_action(self, role: str, action: str, *, confirmed: bool = False) -> dict:
        """Check authorization for a role+action pair with optional confirmation."""

        try:
            role_enum = Role(role)
            action_enum = Action(action)
        except ValueError:
            return self._error("BAD_REQUEST", "Invalid role or action.")
        result = self.platform.security.authorize_with_confirmation(role_enum, action_enum, confirmed=confirmed)
        return self._ok(result)

    def log_audit(self, event: AuditEvent) -> dict:
        """Persist an audit event."""

        payload = self._serialize(event)
        self.persistence.upsert_record(
            "sys_audit_logs",
            "audit_id",
            f"{event.actor}:{event.timestamp.isoformat()}:{event.action.value}",
            payload,
            extra_columns={
                "actor": event.actor,
                "action": event.action.value,
                "resource": event.resource,
                "success": 1 if event.success else 0,
            },
        )
        return self._ok(payload)

    def intelligence_recent(self, limit: int = 20) -> dict:
        """Return recent intelligence documents with sentiment summary."""

        try:
            intel = self.platform.intelligence
            docs = intel.recent_documents(limit=limit)
            result = []
            for d in docs:
                sr = intel.sentiment_results.get(d.document_id)
                result.append({
                    "title": d.title,
                    "source": d.source,
                    "instrument_id": d.instrument_id,
                    "sentiment_label": sr.label.value if sr else "NEUTRAL",
                    "sentiment_score": round(sr.score, 4) if sr else 0,
                    "event_tag": d.event_tag or intel.event_classifications.get(d.document_id, ""),
                    "published_at": d.published_at.isoformat(),
                })
            return self._ok({"documents": result})
        except Exception as exc:
            return self._error("TEMPORARILY_UNAVAILABLE", f"Intelligence service unavailable: {exc}")

    def risk_dashboard(self) -> dict:
        """Return combined risk dashboard data including kill-switch status, alerts, and metrics."""

        try:
            risk = self.platform.risk
            monitoring = self.platform.monitoring
            kill_switch = getattr(risk, 'kill_switch_active', False)
            alerts = monitoring.recent_alerts() if hasattr(monitoring, 'recent_alerts') else []
            metrics: dict[str, Any] = {}
            if hasattr(monitoring, 'metrics'):
                metrics = dict(monitoring.metrics)
            return self._ok({
                "kill_switch_active": kill_switch,
                "alerts": [
                    {
                        "severity": str(getattr(a, 'severity', '')),
                        "message": getattr(a, 'message', ''),
                        "timestamp": str(getattr(a, 'timestamp', '')),
                    }
                    for a in alerts
                ],
                "metrics": metrics,
            })
        except Exception as exc:  # pragma: no cover - defensive runtime isolation
            return self._error("TEMPORARILY_UNAVAILABLE", f"Risk dashboard unavailable: {exc}")

    def quick_paper_trade(
        self,
        *,
        symbol: str,
        side: str = "buy",
        quantity: int = 100,
        account_code: str = "paper_stock_main",
    ) -> dict:
        """Submit a quick market order through the paper-trading service."""

        if not symbol:
            return self._error("BAD_REQUEST", "请先选择标的")
        return self.submit_paper_order(
            instrument_id=symbol,
            side=side,
            quantity=float(quantity),
            account_code=account_code,
            order_type="market",
        )

    # PF-01~PF-06: Portfolio Allocation & Risk Attribution API

    def create_portfolio_allocator(
        self,
        *,
        allocator_type: str,
        name: str,
        description: str = "",
        max_weight: float = 0.3,
        allow_short: bool = False,
    ) -> dict:
        """Create a portfolio allocator configuration (PF-01~PF-03)."""
        from quant_exchange.enhanced.portfolio_allocators import AllocatorType
        try:
            at = AllocatorType(allocator_type)
        except ValueError:
            return self._error("BAD_REQUEST", f"Invalid allocator type: {allocator_type}")
        config = self.platform.portfolio_allocator.create_allocator(
            user_id="default",
            allocator_type=at,
            name=name,
            description=description,
            max_weight=max_weight,
            allow_short=allow_short,
        )
        return self._ok(self._serialize(config))

    def calculate_portfolio_allocation(
        self,
        *,
        allocator_config_id: str,
        expected_returns: dict[str, float],
        volatilities: dict[str, float],
        correlations: dict[str, float] | None = None,
    ) -> dict:
        """Calculate portfolio allocation based on allocator type (PF-02~PF-03)."""
        config = self.platform.portfolio_allocator.get_allocator(allocator_config_id)
        if not config:
            return self._error("NOT_FOUND", f"Allocator config not found: {allocator_config_id}")
        # Convert correlation keys from "A:B" to ("A", "B")
        corr = {}
        if correlations:
            for k, v in correlations.items():
                if ":" in k:
                    parts = k.split(":")
                    corr[(parts[0], parts[1])] = v
                else:
                    corr[k] = v
        result = self.platform.portfolio_allocator.calculate_allocation(
            allocator_config=config,
            expected_returns=expected_returns,
            volatilities=volatilities,
            correlations=corr,
        )
        return self._ok(self._serialize(result))

    def calculate_rebalance_plan(
        self,
        *,
        target_weights: dict[str, float],
        current_weights: dict[str, float],
        current_prices: dict[str, float],
        notional: float = 100000.0,
    ) -> dict:
        """Calculate rebalancing trades (PF-03)."""
        plan = self.platform.portfolio_allocator.calculate_rebalance_plan(
            portfolio_id="default",
            target_weights=target_weights,
            current_weights=current_weights,
            current_prices=current_prices,
            notional=notional,
        )
        return self._ok(self._serialize(plan))

    def get_risk_exposure_summary(
        self,
        prices: dict[str, float],
        positions: dict[str, float],
    ) -> dict:
        """Get aggregated risk exposure across strategies (PF-04)."""
        # Record positions for aggregation
        self.platform.risk_exposure.record_strategy_position(
            strategy_id="default",
            positions=positions,
        )
        exposure = self.platform.risk_exposure.aggregate_exposures(prices=prices)
        return self._ok(self._serialize(exposure))

    def get_attribution_analysis(
        self,
        portfolio_weights: dict[str, float],
        benchmark_weights: dict[str, float],
        portfolio_returns: dict[str, float],
        benchmark_returns: dict[str, float],
    ) -> dict:
        """Get return attribution analysis (PF-05)."""
        result = self.platform.attribution.brinson_attribution(
            portfolio_weights=portfolio_weights,
            benchmark_weights=benchmark_weights,
            portfolio_returns=portfolio_returns,
            benchmark_returns=benchmark_returns,
        )
        return self._ok(self._serialize(result))

    def create_multi_account(
        self,
        *,
        user_id: str,
        account_type: str = "primary",
        initial_cash: float = 0.0,
    ) -> dict:
        """Create a trading account for multi-account management (PF-06)."""
        account = self.platform.multi_account.create_account(
            user_id=user_id,
            account_type=account_type,
            initial_cash=initial_cash,
        )
        return self._ok(self._serialize(account))

    def get_multi_account_summary(self, account_id: str) -> dict:
        """Get multi-account summary."""
        summary = self.platform.multi_account.get_account_summary(account_id)
        if not summary:
            return self._error("NOT_FOUND", f"Account not found: {account_id}")
        return self._ok(summary)

    def transfer_between_accounts(
        self,
        *,
        from_account_id: str,
        to_account_id: str,
        amount: float,
    ) -> dict:
        """Transfer funds between accounts (PF-06)."""
        transfer = self.platform.multi_account.transfer_funds(
            from_account_id=from_account_id,
            to_account_id=to_account_id,
            amount=amount,
        )
        if not transfer:
            return self._error("BAD_REQUEST", "Transfer failed - check account balances")
        return self._ok(self._serialize(transfer))

    # ── Market Data APIs ─────────────────────────────────────────────────────────

    def get_orderbook(self, instrument_id: str) -> dict:
        """Return the latest order book snapshot for an instrument (五档盘口)."""
        ob = self.platform.market_data.get_orderbook(instrument_id)
        if not ob:
            return self._ok({"instrument_id": instrument_id, "bids": [], "asks": [], "timestamp": None})
        return self._ok({
            "instrument_id": instrument_id,
            "bids": [[price, qty] for price, qty in ob.bids],
            "asks": [[price, qty] for price, qty in ob.asks],
            "timestamp": ob.timestamp.isoformat() if ob.timestamp else None,
        })

    def get_trade_ticks(self, instrument_id: str, limit: int = 50) -> dict:
        """Return recent trade ticks for an instrument (成交明细)."""
        ticks = self.platform.market_data.query_ticks(instrument_id)
        recent = ticks[-limit:] if len(ticks) > limit else ticks
        return self._ok([{
            "instrument_id": t.instrument_id,
            "price": t.price,
            "quantity": t.quantity,
            "side": t.side.value if hasattr(t.side, 'value') else str(t.side),
            "timestamp": t.timestamp.isoformat() if t.timestamp else None,
        } for t in recent])

    # ── Alert History APIs ──────────────────────────────────────────────────────

    def get_alert_history(self, window_hours: int = 24, severity: str | None = None) -> dict:
        """Return alert history for the specified time window (MO-06)."""
        from datetime import timedelta
        from quant_exchange.core.models import AlertSeverity
        cutoff = timedelta(hours=window_hours)
        now = datetime.now(timezone.utc)
        alerts = self.platform.monitoring.recent_alerts(window=cutoff)
        if severity:
            try:
                sev = AlertSeverity(severity)
                alerts = [a for a in alerts if a.severity == sev]
            except ValueError:
                pass
        return self._ok([{
            "code": a.code,
            "severity": a.severity.value,
            "message": a.message,
            "timestamp": a.timestamp.isoformat(),
            "context": a.context,
        } for a in alerts])

    # ── Report APIs ─────────────────────────────────────────────────────────────

    def get_daily_report(self, account_id: str = "paper_stock_main") -> dict:
        """Return daily report summary (RP-05)."""
        # Get paper trading account data via dashboard
        data = self.platform.paper_trading.dashboard(account_code=account_id)
        snapshot = data.get("snapshot", {})
        positions = data.get("positions", [])
        fills = data.get("fills", [])
        alerts = self.platform.monitoring.alerts
        # Convert positions list to dict format expected by reporting
        pos_dict = {p["instrument_id"]: p for p in positions}
        report = self.platform.reporting.daily_report(
            account_id=account_id,
            snapshot=snapshot,
            positions=pos_dict,
            fills=fills,
            alerts=alerts,
        )
        return self._ok(report)

    def get_weekly_report(self, account_id: str = "paper_stock_main") -> dict:
        """Return weekly report summary (RP-05)."""
        data = self.platform.paper_trading.dashboard(account_code=account_id)
        snapshot = data.get("snapshot", {})
        positions = data.get("positions", [])
        fills = data.get("fills", [])
        alerts = self.platform.monitoring.alerts
        pos_dict = {p["instrument_id"]: p for p in positions}
        report = self.platform.reporting.weekly_report(
            account_id=account_id,
            snapshots=[snapshot] if snapshot else [],
            positions=pos_dict,
            fills=fills,
            alerts=alerts,
        )
        return self._ok(report)

    def get_monthly_report(self, account_id: str = "paper_stock_main") -> dict:
        """Return monthly report summary (RP-05)."""
        data = self.platform.paper_trading.dashboard(account_code=account_id)
        snapshot = data.get("snapshot", {})
        positions = data.get("positions", [])
        fills = data.get("fills", [])
        alerts = self.platform.monitoring.alerts
        pos_dict = {p["instrument_id"]: p for p in positions}
        report = self.platform.reporting.monthly_report(
            account_id=account_id,
            snapshots=[snapshot] if snapshot else [],
            positions=pos_dict,
            fills=fills,
            alerts=alerts,
        )
        return self._ok(report)

    # ── Watchlist Grouping APIs ─────────────────────────────────────────────────

    def create_watchlist_group(self, user_id: str, group_name: str) -> dict:
        """Create a new watchlist group (自选股分组管理)."""
        key = f"{user_id}:{group_name}"
        if key in self._watchlist_groups:
            return self._ok(self._watchlist_groups[key])
        group = {
            "group_id": key,
            "user_id": user_id,
            "group_name": group_name,
            "instruments": [],
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        self._watchlist_groups[key] = group
        return self._ok(group)

    def get_watchlist_groups(self, user_id: str) -> dict:
        """Get all watchlist groups for a user."""
        groups = [g for k, g in self._watchlist_groups.items() if k.startswith(f"{user_id}:")]
        return self._ok(groups)

    def add_to_watchlist_group(self, user_id: str, group_name: str, instrument_id: str) -> dict:
        """Add an instrument to a watchlist group."""
        key = f"{user_id}:{group_name}"
        if key not in self._watchlist_groups:
            self.create_watchlist_group(user_id, group_name)
        group = self._watchlist_groups[key]
        if instrument_id not in group.get("instruments", []):
            group.setdefault("instruments", []).append(instrument_id)
        return self._ok(group)

    # ── Futures Paper Trading APIs ───────────────────────────────────────────────

    def submit_futures_order(
        self,
        instrument_id: str,
        side: str,
        quantity: int,
        order_type: str = "market",
        limit_price: float | None = None,
    ) -> dict:
        """Submit a futures paper order (FT-05)."""
        if self.platform.futures_trading is None:
            return self._error("NOT_IMPLEMENTED", "Futures trading not available")
        result = self.platform.futures_trading.submit_order(
            instrument_id=instrument_id,
            side=side,
            quantity=quantity,
            order_type=order_type,
            limit_price=limit_price,
        )
        return self._ok(result)

    def get_futures_positions(self) -> dict:
        """Get current futures positions."""
        if self.platform.futures_trading is None:
            return self._error("NOT_IMPLEMENTED", "Futures trading not available")
        positions = getattr(self.platform.futures_trading, "positions", {})
        return self._ok(self._serialize(positions))

    # ── Technical Indicator APIs ─────────────────────────────────────────────────

    def calculate_indicator(
        self,
        indicator: str,
        prices: list[float],
        **params,
    ) -> dict:
        """Calculate technical indicator (MACD/KDJ/BOLL etc.) (CHART-02)."""
        from quant_exchange.strategy import factors
        indicator_map = {
            "MACD": factors.macd,
            "KDJ": factors.stochastic_k,
            "BOLL": factors.bollinger_bands,
            "RSI": factors.relative_strength_index,
            "CCI": factors.commodity_channel_index,
            "ATR": factors.average_true_range,
        }
        if indicator not in indicator_map:
            return self._error("BAD_REQUEST", f"Unknown indicator: {indicator}")
        func = indicator_map[indicator]
        try:
            result = func(prices, **params)
            return self._ok({"indicator": indicator, "result": result})
        except Exception as e:
            return self._error("CALCULATION_ERROR", str(e))

    def _serialize(self, value: Any) -> Any:
        if is_dataclass(value):
            return asdict(value)
        if isinstance(value, list):
            return [self._serialize(item) for item in value]
        if isinstance(value, dict):
            return {key: self._serialize(item) for key, item in value.items()}
        if hasattr(value, "isoformat"):
            return value.isoformat()
        return value

    def _ok(self, data: Any) -> dict:
        return {"code": "OK", "data": data}

    def _error(self, code: str, message: str) -> dict:
        return {"code": code, "error": {"message": message}}
