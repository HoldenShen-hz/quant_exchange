"""Resumable background download jobs for large market-history ingestion tasks."""

from __future__ import annotations

import argparse
import csv
import json
import random
import threading
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Protocol

from quant_exchange.ingestion.a_share_baostock import BaoStockAShareHistoryDownloader, BaoStockAShareRef


def _now_iso() -> str:
    """Return the current UTC timestamp in ISO-8601 format."""

    return datetime.now(timezone.utc).isoformat()


@dataclass(slots=True, frozen=True)
class HistoryDownloadTarget:
    """Describe one symbol-level history download task."""

    target_id: str
    symbol: str
    name: str
    exchange_code: str
    payload: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Serialize the target into a JSON-friendly dictionary."""

        return asdict(self)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "HistoryDownloadTarget":
        """Re-create a target from serialized checkpoint state."""

        return cls(
            target_id=str(payload["target_id"]),
            symbol=str(payload["symbol"]),
            name=str(payload["name"]),
            exchange_code=str(payload["exchange_code"]),
            payload=dict(payload.get("payload", {})),
        )


@dataclass(slots=True, frozen=True)
class HistoryDownloadJobConfig:
    """Configuration for one resumable background history-download job."""

    job_id: str
    provider_code: str
    output_dir: str
    start_date: str = "2010-01-01"
    end_date: str | None = None
    refresh_existing: bool = False
    max_retries: int = 3
    backoff_seconds: float = 1.0
    continuous: bool = False
    rediscover_interval_seconds: int = 900
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Serialize the config into a JSON-friendly dictionary."""

        return asdict(self)


class HistoryDownloadProvider(Protocol):
    """Contract implemented by market-specific download providers."""

    provider_code: str

    def discover_targets(self) -> list[HistoryDownloadTarget]:
        """Return the current symbol universe that should be downloaded."""

    def download_target(self, target: HistoryDownloadTarget, *, skip_existing: bool) -> dict[str, Any]:
        """Download one symbol and return a normalized result payload."""


class BaoStockAShareDownloadProvider:
    """Market-history provider backed by the BaoStock A-share downloader."""

    provider_code = "a_share_baostock"

    def __init__(self, config: HistoryDownloadJobConfig) -> None:
        self.config = config
        self.downloader = BaoStockAShareHistoryDownloader(
            output_dir=config.output_dir,
            start_date=config.start_date,
            end_date=config.end_date,
            processes=1,
        )

    def discover_targets(self) -> list[HistoryDownloadTarget]:
        """Discover the latest A-share stock universe from BaoStock."""

        refs = self.downloader.fetch_stock_universe()
        return [
            HistoryDownloadTarget(
                target_id=ref.code,
                symbol=ref.symbol,
                name=ref.name,
                exchange_code=ref.exchange_code,
                payload={"code": ref.code},
            )
            for ref in refs
        ]

    def download_target(self, target: HistoryDownloadTarget, *, skip_existing: bool) -> dict[str, Any]:
        """Download one A-share symbol using the existing BaoStock downloader."""

        ref = BaoStockAShareRef(
            code=str(target.payload.get("code") or target.target_id),
            name=target.name,
            exchange_code=target.exchange_code,
        )
        return self.downloader.download_symbol_history(ref, skip_existing=skip_existing)


class SimulatedMarketDownloadProvider:
    """Simulated download provider that generates demo daily OHLCV data for any market.

    This lets the HK / US / or any other market download buttons work immediately
    without requiring a real data vendor.  The generated data is deterministic per
    symbol so that repeated runs produce identical files (skip_existing works).
    """

    provider_code: str = "simulated"

    _HK_UNIVERSE: list[tuple[str, str, str]] = [
        ("00700.HK", "00700", "腾讯控股"),
        ("09988.HK", "09988", "阿里巴巴-W"),
        ("09999.HK", "09999", "网易-S"),
        ("03690.HK", "03690", "美团-W"),
        ("01810.HK", "01810", "小米集团-W"),
        ("02318.HK", "02318", "中国平安"),
        ("00941.HK", "00941", "中国移动"),
        ("01024.HK", "01024", "快手-W"),
        ("09618.HK", "09618", "京东集团-SW"),
        ("09888.HK", "09888", "百度集团-SW"),
        ("00005.HK", "00005", "汇丰控股"),
        ("02020.HK", "02020", "安踏体育"),
        ("01211.HK", "01211", "比亚迪股份"),
        ("00388.HK", "00388", "香港交易所"),
        ("02269.HK", "02269", "药明生物"),
        ("00003.HK", "00003", "香港中华煤气"),
        ("01398.HK", "01398", "工商银行"),
        ("00883.HK", "00883", "中国海洋石油"),
        ("02688.HK", "02688", "新奥能源"),
        ("06098.HK", "06098", "碧桂园服务"),
    ]

    _US_UNIVERSE: list[tuple[str, str, str]] = [
        ("AAPL.US", "AAPL", "Apple"),
        ("MSFT.US", "MSFT", "Microsoft"),
        ("NVDA.US", "NVDA", "NVIDIA"),
        ("GOOGL.US", "GOOGL", "Alphabet"),
        ("AMZN.US", "AMZN", "Amazon"),
        ("META.US", "META", "Meta Platforms"),
        ("TSLA.US", "TSLA", "Tesla"),
        ("BRK-B.US", "BRK-B", "Berkshire Hathaway"),
        ("JPM.US", "JPM", "JPMorgan Chase"),
        ("V.US", "V", "Visa"),
        ("UNH.US", "UNH", "UnitedHealth"),
        ("XOM.US", "XOM", "Exxon Mobil"),
        ("JNJ.US", "JNJ", "Johnson & Johnson"),
        ("WMT.US", "WMT", "Walmart"),
        ("MA.US", "MA", "Mastercard"),
        ("PG.US", "PG", "Procter & Gamble"),
        ("HD.US", "HD", "Home Depot"),
        ("COST.US", "COST", "Costco"),
        ("ABBV.US", "ABBV", "AbbVie"),
        ("BAC.US", "BAC", "Bank of America"),
    ]

    _BASE_PRICES: dict[str, float] = {
        "00700.HK": 380.0, "09988.HK": 85.0, "09999.HK": 155.0,
        "03690.HK": 130.0, "01810.HK": 16.0, "02318.HK": 55.0,
        "00941.HK": 70.0, "01024.HK": 55.0, "09618.HK": 130.0,
        "09888.HK": 110.0, "00005.HK": 58.0, "02020.HK": 90.0,
        "01211.HK": 260.0, "00388.HK": 290.0, "02269.HK": 35.0,
        "00003.HK": 7.5, "01398.HK": 5.0, "00883.HK": 14.0,
        "02688.HK": 55.0, "06098.HK": 18.0,
        "AAPL.US": 195.0, "MSFT.US": 420.0, "NVDA.US": 850.0,
        "GOOGL.US": 155.0, "AMZN.US": 185.0, "META.US": 510.0,
        "TSLA.US": 245.0, "BRK-B.US": 410.0, "JPM.US": 195.0,
        "V.US": 280.0, "UNH.US": 520.0, "XOM.US": 110.0,
        "JNJ.US": 155.0, "WMT.US": 170.0, "MA.US": 460.0,
        "PG.US": 165.0, "HD.US": 370.0, "COST.US": 740.0,
        "ABBV.US": 175.0, "BAC.US": 35.0,
    }

    def __init__(self, config: HistoryDownloadJobConfig) -> None:
        self.config = config
        self.provider_code = config.provider_code
        self.output_dir = Path(config.output_dir)
        self.start_date = config.start_date or "2020-01-01"
        market = config.metadata.get("market", "HK")
        self._universe = self._HK_UNIVERSE if market == "HK" else self._US_UNIVERSE
        self._exchange = "HKEX" if market == "HK" else "NASDAQ"

    def discover_targets(self) -> list[HistoryDownloadTarget]:
        return [
            HistoryDownloadTarget(
                target_id=tid, symbol=sym, name=name,
                exchange_code=self._exchange, payload={"base_price": self._BASE_PRICES.get(tid, 100.0)},
            )
            for tid, sym, name in self._universe
        ]

    def download_target(self, target: HistoryDownloadTarget, *, skip_existing: bool) -> dict[str, Any]:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        safe_name = target.symbol.replace("/", "_")
        filepath = self.output_dir / f"{safe_name}.csv"

        if skip_existing and filepath.exists() and filepath.stat().st_size > 100:
            existing_rows = sum(1 for _ in open(filepath)) - 1
            return {
                "symbol": target.symbol,
                "status": "skipped",
                "rows": existing_rows,
                "path": str(filepath),
                "message": "File already exists, skipped.",
            }

        base_price = float(target.payload.get("base_price", 100.0))
        rng = random.Random(hash(target.target_id))
        start = datetime.strptime(self.start_date, "%Y-%m-%d")
        end = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        rows: list[list[str]] = []
        price = base_price * 0.6
        day = start
        while day <= end:
            if day.weekday() < 5:
                change_pct = rng.gauss(0.0003, 0.018)
                price *= 1 + change_pct
                o = round(price * (1 + rng.uniform(-0.005, 0.005)), 2)
                h = round(max(o, price) * (1 + rng.uniform(0, 0.015)), 2)
                l = round(min(o, price) * (1 - rng.uniform(0, 0.015)), 2)  # noqa: E741
                c = round(price, 2)
                vol = int(rng.uniform(500_000, 15_000_000))
                rows.append([day.strftime("%Y-%m-%d"), str(o), str(h), str(l), str(c), str(vol)])
            day += timedelta(days=1)

        with open(filepath, "w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(["date", "open", "high", "low", "close", "volume"])
            writer.writerows(rows)

        time.sleep(rng.uniform(0.02, 0.08))

        return {
            "symbol": target.symbol,
            "status": "downloaded",
            "rows": len(rows),
            "path": str(filepath),
            "message": f"Generated {len(rows)} simulated daily bars.",
        }


@dataclass(slots=True)
class HistoryDownloadJobState:
    """Mutable checkpoint state for one resumable history-download job."""

    config: dict[str, Any]
    status: str = "idle"
    started_at: str | None = None
    updated_at: str = field(default_factory=_now_iso)
    finished_at: str | None = None
    current_target: dict[str, Any] | None = None
    pending_targets: list[dict[str, Any]] = field(default_factory=list)
    completed_targets: list[str] = field(default_factory=list)
    attempts_by_target: dict[str, int] = field(default_factory=dict)
    failed_targets: dict[str, dict[str, Any]] = field(default_factory=dict)
    downloaded_rows: int = 0
    last_result: dict[str, Any] | None = None
    last_error: str | None = None

    def snapshot(self) -> dict[str, Any]:
        """Return the serialized state with derived counters."""

        completed_count = len(self.completed_targets)
        pending_count = len(self.pending_targets)
        failed_count = len(self.failed_targets)
        total_discovered = max(
            completed_count + pending_count + failed_count,
            int(self.config.get("metadata", {}).get("last_discovered_total", 0)),
        )
        return {
            "config": self.config,
            "status": self.status,
            "started_at": self.started_at,
            "updated_at": self.updated_at,
            "finished_at": self.finished_at,
            "current_target": self.current_target,
            "pending_targets": self.pending_targets,
            "completed_targets": self.completed_targets,
            "attempts_by_target": self.attempts_by_target,
            "failed_targets": self.failed_targets,
            "downloaded_rows": self.downloaded_rows,
            "last_result": self.last_result,
            "last_error": self.last_error,
            "completed_count": completed_count,
            "pending_count": pending_count,
            "failed_count": failed_count,
            "total_discovered": total_discovered,
        }

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "HistoryDownloadJobState":
        """Build job state from checkpoint JSON."""

        return cls(
            config=dict(payload.get("config", {})),
            status=str(payload.get("status", "idle")),
            started_at=payload.get("started_at"),
            updated_at=str(payload.get("updated_at") or _now_iso()),
            finished_at=payload.get("finished_at"),
            current_target=payload.get("current_target"),
            pending_targets=list(payload.get("pending_targets", [])),
            completed_targets=list(payload.get("completed_targets", [])),
            attempts_by_target={str(key): int(value) for key, value in dict(payload.get("attempts_by_target", {})).items()},
            failed_targets={str(key): dict(value) for key, value in dict(payload.get("failed_targets", {})).items()},
            downloaded_rows=int(payload.get("downloaded_rows", 0)),
            last_result=payload.get("last_result"),
            last_error=payload.get("last_error"),
        )


class HistoryDownloadStateStore:
    """Persist checkpoint and control files for resumable download jobs."""

    def __init__(self, state_dir: str | Path) -> None:
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)

    def state_path(self, job_id: str) -> Path:
        """Return the JSON checkpoint path for one job."""

        return self.state_dir / f"{job_id}.state.json"

    def control_path(self, job_id: str) -> Path:
        """Return the JSON control path for one job."""

        return self.state_dir / f"{job_id}.control.json"

    def load_state(self, job_id: str) -> dict[str, Any] | None:
        """Load the last persisted checkpoint, if it exists."""

        path = self.state_path(job_id)
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))

    def save_state(self, job_id: str, payload: dict[str, Any]) -> None:
        """Persist checkpoint state atomically."""

        path = self.state_path(job_id)
        temp_path = path.with_suffix(path.suffix + ".tmp")
        temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        temp_path.replace(path)

    def request_action(self, job_id: str, action: str) -> None:
        """Persist a control action so the next active worker loop can handle it cleanly."""

        payload = {"action": action, "updated_at": _now_iso()}
        path = self.control_path(job_id)
        temp_path = path.with_suffix(path.suffix + ".tmp")
        temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        temp_path.replace(path)

    def request_pause(self, job_id: str) -> None:
        """Persist a pause request for one job."""

        self.request_action(job_id, "pause")

    def request_cancel(self, job_id: str) -> None:
        """Persist a cancel request for one job."""

        self.request_action(job_id, "cancel")

    def request_stop(self, job_id: str) -> None:
        """Backward-compatible alias for a pause request."""

        self.request_pause(job_id)

    def clear_stop_request(self, job_id: str) -> None:
        """Remove a pending stop request."""

        path = self.control_path(job_id)
        if path.exists():
            path.unlink()

    def control_action(self, job_id: str) -> str | None:
        """Return the latest out-of-process control action, if one exists."""

        path = self.control_path(job_id)
        if not path.exists():
            return None
        payload = json.loads(path.read_text(encoding="utf-8"))
        action = payload.get("action")
        if action:
            return str(action)
        if payload.get("stop_requested"):
            return "pause"
        return None

    def list_job_ids(self) -> list[str]:
        """List all jobs that currently have checkpoint files on disk."""

        return sorted(path.name.removesuffix(".state.json") for path in self.state_dir.glob("*.state.json"))


class ResumableHistoryDownloadJob:
    """Execute one resumable symbol-universe download in a background thread or foreground loop."""

    def __init__(
        self,
        config: HistoryDownloadJobConfig,
        provider: HistoryDownloadProvider,
        state_store: HistoryDownloadStateStore,
    ) -> None:
        self.config = config
        self.provider = provider
        self.state_store = state_store
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._state = self._load_or_initialize_state()

    def start(self) -> dict[str, Any]:
        """Start the download job in a daemon thread."""

        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return self.status()
            self._stop_event.clear()
            self.state_store.clear_stop_request(self.config.job_id)
            self._thread = threading.Thread(target=self._run, name=f"history-download:{self.config.job_id}", daemon=True)
            self._thread.start()
            return self.status()

    def run_foreground(self) -> dict[str, Any]:
        """Run the download job in the current process."""

        with self._lock:
            self._stop_event.clear()
            self.state_store.clear_stop_request(self.config.job_id)
        self._run()
        return self.status()

    def stop(self) -> dict[str, Any]:
        """Backward-compatible alias that pauses the job."""

        return self.pause()

    def pause(self) -> dict[str, Any]:
        """Request a graceful pause and persist the intent for out-of-process control."""

        self._stop_event.set()
        self.state_store.request_pause(self.config.job_id)
        with self._lock:
            self._state.status = "pause_requested"
            self._state.updated_at = _now_iso()
            self._persist_locked()
        return self.status()

    def cancel(self) -> dict[str, Any]:
        """Cancel the active run and clear the current pending queue for this run."""

        self._stop_event.set()
        self.state_store.request_cancel(self.config.job_id)
        with self._lock:
            self._state.status = "cancel_requested"
            self._state.updated_at = _now_iso()
            self._persist_locked()
        return self.status()

    def join(self, timeout: float | None = None) -> None:
        """Wait for the background thread to finish if it is running."""

        thread = self._thread
        if thread is not None:
            thread.join(timeout)

    def is_alive(self) -> bool:
        """Return whether the job currently owns an active background thread."""

        return self._thread is not None and self._thread.is_alive()

    def status(self) -> dict[str, Any]:
        """Return the latest in-memory or persisted checkpoint snapshot."""

        with self._lock:
            return self._state.snapshot()

    def _load_or_initialize_state(self) -> HistoryDownloadJobState:
        payload = self.state_store.load_state(self.config.job_id)
        if payload is not None:
            state = HistoryDownloadJobState.from_payload(payload)
            state.config = self._merge_config(state.config)
            return state
        state = HistoryDownloadJobState(config=self.config.to_dict())
        self.state_store.save_state(self.config.job_id, state.snapshot())
        return state

    def _merge_config(self, existing: dict[str, Any]) -> dict[str, Any]:
        merged = dict(existing)
        latest = self.config.to_dict()
        merged.update(latest)
        merged.setdefault("metadata", {})
        merged["metadata"] = {**dict(existing.get("metadata", {})), **dict(latest.get("metadata", {}))}
        return merged

    def _persist_locked(self) -> None:
        self.state_store.save_state(self.config.job_id, self._state.snapshot())

    def _run(self) -> None:
        while True:
            with self._lock:
                if self._state.started_at is None:
                    self._state.started_at = _now_iso()
                self._state.status = "running"
                self._state.updated_at = _now_iso()
                self._persist_locked()
            action = self._control_action()
            if action == "pause":
                self._mark_paused()
                return
            if action == "cancel":
                self._mark_cancelled()
                return
            try:
                target = self._next_target()
            except Exception as exc:  # pragma: no cover - depends on runtime providers
                self._mark_failed(str(exc))
                return
            if target is None:
                if self.config.continuous and self._control_action() is None:
                    with self._lock:
                        self._state.status = "idle"
                        self._state.current_target = None
                        self._state.updated_at = _now_iso()
                        self._persist_locked()
                    time.sleep(max(self.config.rediscover_interval_seconds, 1))
                    continue
                self._mark_finished()
                return
            self._process_target(target)

    def _control_action(self) -> str | None:
        if self._stop_event.is_set():
            status = self._state.status
            if status in {"cancel_requested", "cancelled"}:
                return "cancel"
            if status in {"pause_requested", "paused", "stop_requested", "stopped"}:
                return "pause"
        return self.state_store.control_action(self.config.job_id)

    def _mark_paused(self) -> None:
        with self._lock:
            self._state.status = "paused"
            self._state.current_target = None
            self._state.updated_at = _now_iso()
            self._persist_locked()

    def _mark_cancelled(self) -> None:
        with self._lock:
            self._state.status = "cancelled"
            self._state.current_target = None
            self._state.pending_targets = []
            self._state.finished_at = _now_iso()
            self._state.updated_at = self._state.finished_at
            self._persist_locked()

    def _mark_finished(self) -> None:
        with self._lock:
            self._state.status = "completed_with_errors" if self._state.failed_targets else "completed"
            self._state.finished_at = _now_iso()
            self._state.current_target = None
            self._state.updated_at = self._state.finished_at
            self._persist_locked()

    def _mark_failed(self, error: str) -> None:
        with self._lock:
            self._state.status = "failed"
            self._state.finished_at = _now_iso()
            self._state.current_target = None
            self._state.last_error = error
            self._state.updated_at = self._state.finished_at
            self._persist_locked()

    def _next_target(self) -> HistoryDownloadTarget | None:
        with self._lock:
            if not self._state.pending_targets:
                self._rediscover_pending_locked()
            if not self._state.pending_targets:
                return None
            payload = self._state.pending_targets.pop(0)
            self._state.current_target = payload
            self._state.updated_at = _now_iso()
            self._persist_locked()
        return HistoryDownloadTarget.from_dict(payload)

    def _rediscover_pending_locked(self) -> None:
        discovered = self.provider.discover_targets()
        completed_ids = set(self._state.completed_targets)
        pending_ids = {item["target_id"] for item in self._state.pending_targets}
        self._state.config.setdefault("metadata", {})
        self._state.config["metadata"]["last_discovered_total"] = len(discovered)
        for target in discovered:
            if target.target_id in completed_ids or target.target_id in pending_ids:
                continue
            attempts = self._state.attempts_by_target.get(target.target_id, 0)
            if attempts >= self.config.max_retries and target.target_id in self._state.failed_targets:
                continue
            self._state.pending_targets.append(target.to_dict())
            pending_ids.add(target.target_id)
        self._state.updated_at = _now_iso()
        self._persist_locked()

    def _process_target(self, target: HistoryDownloadTarget) -> None:
        target_id = target.target_id
        with self._lock:
            attempt = self._state.attempts_by_target.get(target_id, 0) + 1
            self._state.attempts_by_target[target_id] = attempt
            self._state.updated_at = _now_iso()
            self._persist_locked()
        try:
            result = self.provider.download_target(target, skip_existing=not self.config.refresh_existing)
        except Exception as exc:  # pragma: no cover - exercised via tests with fake providers
            result = {
                "code": target.payload.get("code", target_id),
                "name": target.name,
                "path": "",
                "rows": 0,
                "status": "error",
                "error": str(exc),
            }
        self._apply_result(target, result)

    def _apply_result(self, target: HistoryDownloadTarget, result: dict[str, Any]) -> None:
        target_id = target.target_id
        status = str(result.get("status", "error"))
        rows = int(result.get("rows", 0) or 0)
        with self._lock:
            self._state.last_result = dict(result)
            self._state.updated_at = _now_iso()
            if status in {"downloaded", "skipped"}:
                if target_id not in self._state.completed_targets:
                    self._state.completed_targets.append(target_id)
                self._state.downloaded_rows += rows
                self._state.failed_targets.pop(target_id, None)
                self._state.last_error = None
            else:
                attempt = self._state.attempts_by_target.get(target_id, 0)
                failure = {
                    "target": target.to_dict(),
                    "status": status,
                    "error": str(result.get("error") or "unknown_error"),
                    "attempt": attempt,
                    "updated_at": _now_iso(),
                }
                if attempt < self.config.max_retries:
                    self._state.pending_targets.append(target.to_dict())
                else:
                    self._state.failed_targets[target_id] = failure
                self._state.last_error = failure["error"]
            self._state.current_target = None
            self._persist_locked()
        if status not in {"downloaded", "skipped"} and self.config.backoff_seconds > 0:
            time.sleep(self.config.backoff_seconds)


class HistoryDownloadSupervisor:
    """Manage multiple resumable history-download jobs inside one process."""

    def __init__(
        self,
        state_dir: str | Path,
        provider_factories: dict[str, Callable[[HistoryDownloadJobConfig], HistoryDownloadProvider]] | None = None,
    ) -> None:
        self.state_store = HistoryDownloadStateStore(state_dir)
        self.provider_factories = provider_factories or {
            "a_share_baostock": BaoStockAShareDownloadProvider,
            "hk_simulated": SimulatedMarketDownloadProvider,
            "us_simulated": SimulatedMarketDownloadProvider,
        }
        self._jobs: dict[str, ResumableHistoryDownloadJob] = {}
        self._lock = threading.RLock()

    def start_job(self, config: HistoryDownloadJobConfig) -> dict[str, Any]:
        """Create or resume a job and start it in the background."""

        with self._lock:
            job = self._jobs.get(config.job_id)
            if job is None:
                provider = self._build_provider(config)
                job = ResumableHistoryDownloadJob(config, provider, self.state_store)
                self._jobs[config.job_id] = job
            return job.start()

    def run_job_foreground(self, config: HistoryDownloadJobConfig) -> dict[str, Any]:
        """Create or resume a job and run it in the current process."""

        with self._lock:
            job = self._jobs.get(config.job_id)
            if job is None:
                provider = self._build_provider(config)
                job = ResumableHistoryDownloadJob(config, provider, self.state_store)
                self._jobs[config.job_id] = job
        return job.run_foreground()

    def stop_job(self, job_id: str) -> dict[str, Any]:
        """Cancel the active run for one job."""

        with self._lock:
            job = self._jobs.get(job_id)
            if job is not None:
                return job.cancel()
        self.state_store.request_cancel(job_id)
        payload = self.state_store.load_state(job_id)
        if payload is None:
            return {"job_id": job_id, "status": "cancel_requested"}
        state = HistoryDownloadJobState.from_payload(payload)
        state.status = "cancel_requested"
        state.updated_at = _now_iso()
        self.state_store.save_state(job_id, state.snapshot())
        return state.snapshot()

    def pause_job(self, job_id: str) -> dict[str, Any]:
        """Request a graceful pause for one job."""

        with self._lock:
            job = self._jobs.get(job_id)
            if job is not None:
                return job.pause()
        self.state_store.request_pause(job_id)
        payload = self.state_store.load_state(job_id)
        if payload is None:
            return {"job_id": job_id, "status": "pause_requested"}
        state = HistoryDownloadJobState.from_payload(payload)
        state.status = "pause_requested"
        state.updated_at = _now_iso()
        self.state_store.save_state(job_id, state.snapshot())
        return state.snapshot()

    def job_status(self, job_id: str) -> dict[str, Any]:
        """Return the latest known status for one job."""

        with self._lock:
            job = self._jobs.get(job_id)
            if job is not None:
                return job.status()
        payload = self.state_store.load_state(job_id)
        if payload is None:
            return {"job_id": job_id, "status": "not_found"}
        return HistoryDownloadJobState.from_payload(payload).snapshot()

    def list_jobs(self) -> list[dict[str, Any]]:
        """List all checkpointed jobs from disk."""

        return [self.job_status(job_id) for job_id in self.state_store.list_job_ids()]

    def close(self) -> None:
        """Stop all active jobs and wait briefly for them to exit."""

        with self._lock:
            jobs = list(self._jobs.values())
        for job in jobs:
            if job.is_alive():
                job.pause()
        for job in jobs:
            if job.is_alive():
                job.join(timeout=2.0)

    def _build_provider(self, config: HistoryDownloadJobConfig) -> HistoryDownloadProvider:
        factory = self.provider_factories.get(config.provider_code)
        if factory is None:
            raise ValueError(f"unknown_history_provider:{config.provider_code}")
        return factory(config)


def _default_state_dir() -> Path:
    """Return the default runtime directory for background download checkpoints."""

    return Path(__file__).resolve().parents[3] / "data" / "runtime" / "history_downloads"


def build_arg_parser() -> argparse.ArgumentParser:
    """Build the CLI parser for the resumable history-download daemon."""

    parser = argparse.ArgumentParser(description="Run resumable background market-history download jobs.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Start or resume a download job.")
    run_parser.add_argument("--job-id", default="a_share_daily_history", help="Stable job identifier used for resume.")
    run_parser.add_argument("--provider", default="a_share_baostock", help="History download provider code.")
    run_parser.add_argument(
        "--output-dir",
        default="/Users/holden/Project/finance_devepment/quant_exchange/data/cn_equities/a_share/daily_raw",
        help="Directory where downloaded files will be stored.",
    )
    run_parser.add_argument("--start-date", default="2010-01-01", help="Download start date in YYYY-MM-DD format.")
    run_parser.add_argument("--end-date", default=None, help="Optional download end date in YYYY-MM-DD format.")
    run_parser.add_argument("--refresh-existing", action="store_true", help="Re-download symbols even if local files exist.")
    run_parser.add_argument("--continuous", action="store_true", help="Keep re-discovering the universe after completion.")
    run_parser.add_argument("--max-retries", type=int, default=3, help="Maximum attempts per symbol before giving up.")
    run_parser.add_argument(
        "--rediscover-interval-seconds",
        type=int,
        default=900,
        help="How long the continuous runner waits before re-discovering symbols.",
    )
    run_parser.add_argument(
        "--state-dir",
        default=str(_default_state_dir()),
        help="Directory used to store resumable checkpoint files.",
    )

    status_parser = subparsers.add_parser("status", help="Read the latest saved status for one job.")
    status_parser.add_argument("--job-id", default="a_share_daily_history", help="Stable job identifier.")
    status_parser.add_argument("--state-dir", default=str(_default_state_dir()), help="Checkpoint directory.")

    pause_parser = subparsers.add_parser("pause", help="Request a graceful pause for one running job.")
    pause_parser.add_argument("--job-id", default="a_share_daily_history", help="Stable job identifier.")
    pause_parser.add_argument("--state-dir", default=str(_default_state_dir()), help="Checkpoint directory.")

    stop_parser = subparsers.add_parser("stop", help="Cancel one running job.")
    stop_parser.add_argument("--job-id", default="a_share_daily_history", help="Stable job identifier.")
    stop_parser.add_argument("--state-dir", default=str(_default_state_dir()), help="Checkpoint directory.")
    return parser


def main() -> None:
    """CLI entrypoint for resumable history-download jobs."""

    parser = build_arg_parser()
    args = parser.parse_args()
    state_store = HistoryDownloadStateStore(args.state_dir)
    if args.command == "status":
        payload = state_store.load_state(args.job_id)
        if payload is None:
            payload = {"job_id": args.job_id, "status": "not_found"}
        else:
            payload = HistoryDownloadJobState.from_payload(payload).snapshot()
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return
    if args.command == "pause":
        state_store.request_pause(args.job_id)
        payload = state_store.load_state(args.job_id)
        if payload is None:
            payload = {"job_id": args.job_id, "status": "pause_requested"}
        else:
            state = HistoryDownloadJobState.from_payload(payload)
            state.status = "pause_requested"
            state.updated_at = _now_iso()
            state_store.save_state(args.job_id, state.snapshot())
            payload = state.snapshot()
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return
    if args.command == "stop":
        state_store.request_cancel(args.job_id)
        payload = state_store.load_state(args.job_id)
        if payload is None:
            payload = {"job_id": args.job_id, "status": "cancel_requested"}
        else:
            state = HistoryDownloadJobState.from_payload(payload)
            state.status = "cancel_requested"
            state.updated_at = _now_iso()
            state_store.save_state(args.job_id, state.snapshot())
            payload = state.snapshot()
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return
    config = HistoryDownloadJobConfig(
        job_id=args.job_id,
        provider_code=args.provider,
        output_dir=args.output_dir,
        start_date=args.start_date,
        end_date=args.end_date,
        refresh_existing=args.refresh_existing,
        max_retries=args.max_retries,
        continuous=args.continuous,
        rediscover_interval_seconds=args.rediscover_interval_seconds,
    )
    supervisor = HistoryDownloadSupervisor(args.state_dir)
    try:
        payload = supervisor.run_job_foreground(config)
    finally:
        supervisor.close()
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
