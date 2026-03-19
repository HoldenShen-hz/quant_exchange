from __future__ import annotations

import tempfile
import time
import unittest
from pathlib import Path

from quant_exchange.config import AppSettings
from quant_exchange.ingestion.background_downloader import (
    HistoryDownloadJobConfig,
    HistoryDownloadStateStore,
    HistoryDownloadSupervisor,
    HistoryDownloadTarget,
)
from quant_exchange.platform import QuantTradingPlatform


class FakeHistoryProvider:
    """Small fake provider used to test resumable background downloads."""

    provider_code = "fake_provider"

    def __init__(
        self,
        targets: list[HistoryDownloadTarget],
        *,
        state_store: HistoryDownloadStateStore | None = None,
        job_id: str | None = None,
        stop_after_first: bool = False,
        cancel_after_first: bool = False,
        delay_seconds: float = 0.0,
    ) -> None:
        self.targets = list(targets)
        self.state_store = state_store
        self.job_id = job_id
        self.stop_after_first = stop_after_first
        self.cancel_after_first = cancel_after_first
        self.delay_seconds = delay_seconds
        self.calls: list[str] = []

    def discover_targets(self) -> list[HistoryDownloadTarget]:
        return list(self.targets)

    def download_target(self, target: HistoryDownloadTarget, *, skip_existing: bool) -> dict:
        self.calls.append(target.target_id)
        if self.delay_seconds > 0:
            time.sleep(self.delay_seconds)
        if self.stop_after_first and len(self.calls) == 1 and self.state_store is not None and self.job_id is not None:
            self.state_store.request_pause(self.job_id)
        if self.cancel_after_first and len(self.calls) == 1 and self.state_store is not None and self.job_id is not None:
            self.state_store.request_cancel(self.job_id)
        output = Path(tempfile.gettempdir()) / f"{target.symbol}.csv.gz"
        output.write_text("ok", encoding="utf-8")
        return {"code": target.target_id, "name": target.name, "path": str(output), "rows": 5, "status": "downloaded"}


class BackgroundDownloaderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.state_store = HistoryDownloadStateStore(Path(self.temp_dir.name) / "state")
        self.output_dir = Path(self.temp_dir.name) / "history"
        self.targets = [
            HistoryDownloadTarget(target_id="AAA", symbol="AAA", name="Alpha", exchange_code="TEST"),
            HistoryDownloadTarget(target_id="BBB", symbol="BBB", name="Beta", exchange_code="TEST"),
            HistoryDownloadTarget(target_id="CCC", symbol="CCC", name="Gamma", exchange_code="TEST"),
        ]

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_bg_01_foreground_job_completes_and_persists_checkpoint(self) -> None:
        provider = FakeHistoryProvider(self.targets)
        supervisor = HistoryDownloadSupervisor(
            self.state_store.state_dir,
            provider_factories={"fake_provider": lambda config: provider},
        )
        config = HistoryDownloadJobConfig(job_id="bg01", provider_code="fake_provider", output_dir=str(self.output_dir))
        result = supervisor.run_job_foreground(config)
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["completed_count"], 3)
        self.assertEqual(provider.calls, ["AAA", "BBB", "CCC"])
        checkpoint = self.state_store.load_state("bg01")
        self.assertIsNotNone(checkpoint)
        self.assertEqual(checkpoint["completed_count"], 3)

    def test_bg_02_resume_uses_saved_pending_targets(self) -> None:
        saved_state = {
            "config": HistoryDownloadJobConfig(job_id="bg02", provider_code="fake_provider", output_dir=str(self.output_dir)).to_dict(),
            "status": "stopped",
            "started_at": "2026-03-17T00:00:00+00:00",
            "updated_at": "2026-03-17T00:00:00+00:00",
            "finished_at": None,
            "current_target": None,
            "pending_targets": [target.to_dict() for target in self.targets[1:]],
            "completed_targets": ["AAA"],
            "attempts_by_target": {"AAA": 1},
            "failed_targets": {},
            "downloaded_rows": 5,
            "last_result": None,
            "last_error": None,
        }
        self.state_store.save_state("bg02", saved_state)
        provider = FakeHistoryProvider(self.targets)
        supervisor = HistoryDownloadSupervisor(
            self.state_store.state_dir,
            provider_factories={"fake_provider": lambda config: provider},
        )
        config = HistoryDownloadJobConfig(job_id="bg02", provider_code="fake_provider", output_dir=str(self.output_dir))
        result = supervisor.run_job_foreground(config)
        self.assertEqual(result["status"], "completed")
        self.assertEqual(provider.calls, ["BBB", "CCC"])
        self.assertEqual(result["completed_count"], 3)

    def test_bg_03_pause_request_allows_clean_resume(self) -> None:
        provider = FakeHistoryProvider(self.targets, state_store=self.state_store, job_id="bg03", stop_after_first=True)
        supervisor = HistoryDownloadSupervisor(
            self.state_store.state_dir,
            provider_factories={"fake_provider": lambda config: provider},
        )
        config = HistoryDownloadJobConfig(job_id="bg03", provider_code="fake_provider", output_dir=str(self.output_dir))
        first = supervisor.run_job_foreground(config)
        self.assertEqual(first["status"], "paused")
        self.assertEqual(first["completed_count"], 1)
        self.assertEqual(provider.calls, ["AAA"])

        provider.stop_after_first = False
        second = supervisor.run_job_foreground(config)
        self.assertEqual(second["status"], "completed")
        self.assertEqual(second["completed_count"], 3)
        self.assertEqual(provider.calls, ["AAA", "BBB", "CCC"])

    def test_bg_04_cancel_request_marks_job_cancelled(self) -> None:
        provider = FakeHistoryProvider(self.targets, state_store=self.state_store, job_id="bg04", cancel_after_first=True)
        supervisor = HistoryDownloadSupervisor(
            self.state_store.state_dir,
            provider_factories={"fake_provider": lambda config: provider},
        )
        config = HistoryDownloadJobConfig(job_id="bg04", provider_code="fake_provider", output_dir=str(self.output_dir))
        first = supervisor.run_job_foreground(config)
        self.assertEqual(first["status"], "cancelled")
        self.assertEqual(first["completed_count"], 1)
        self.assertEqual(first["pending_count"], 0)

        provider.cancel_after_first = False
        second = supervisor.run_job_foreground(config)
        self.assertEqual(second["status"], "completed")
        self.assertEqual(second["completed_count"], 3)

    def test_bg_05_api_can_start_and_query_background_job(self) -> None:
        db_path = Path(self.temp_dir.name) / "runtime.sqlite3"
        platform = QuantTradingPlatform(AppSettings.from_mapping({"database": {"url": str(db_path)}}))
        self.addCleanup(platform.close)
        provider = FakeHistoryProvider(self.targets, delay_seconds=0.01)
        platform.history_downloads.provider_factories["fake_provider"] = lambda config: provider

        started = platform.api.start_history_download_job(
            job_id="bg05",
            provider_code="fake_provider",
            output_dir=str(self.output_dir),
        )
        self.assertEqual(started["code"], "OK")
        platform.history_downloads._jobs["bg05"].join(timeout=2.0)
        status = platform.api.get_history_download_job("bg05")
        self.assertEqual(status["code"], "OK")
        self.assertEqual(status["data"]["status"], "completed")
        listed = platform.api.list_history_download_jobs()
        self.assertEqual(listed["code"], "OK")
        self.assertTrue(any(item["config"]["job_id"] == "bg05" for item in listed["data"]))


if __name__ == "__main__":
    unittest.main()
