"""Download A-share daily historical data with BaoStock."""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import re
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


A_SHARE_CODE_PATTERN = re.compile(r"^(sh\.(600|601|603|605|688)\d{3}|sz\.(000|001|002|003|300|301)\d{3})$")
CSV_COLUMNS = ["date", "code", "open", "high", "low", "close", "volume", "amount", "turn", "pctChg"]


@dataclass(slots=True, frozen=True)
class BaoStockAShareRef:
    """Normalized A-share stock reference from BaoStock."""

    code: str
    name: str
    exchange_code: str

    @property
    def symbol(self) -> str:
        """Return the code suffix without the market prefix."""

        return self.code.split(".", 1)[1]


class BaoStockAShareHistoryDownloader:
    """Download daily A-share bars from BaoStock into local gzip-compressed CSV files."""

    def __init__(
        self,
        output_dir: str | Path,
        *,
        start_date: str = "2010-01-01",
        end_date: str | None = None,
        processes: int = 1,
        chunk_size: int = 50,
    ) -> None:
        self.output_dir = Path(output_dir)
        self.start_date = start_date
        self.end_date = end_date or datetime.now().strftime("%Y-%m-%d")
        self.processes = processes
        self.chunk_size = chunk_size
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._bs = None

    def fetch_stock_universe(self, *, day: str | None = None) -> list[BaoStockAShareRef]:
        """Fetch the current A-share stock universe and filter out non-equity instruments."""

        bs = self._import_baostock()
        self._login(bs)
        query_day = day or self.end_date
        for attempt_day in self._candidate_query_days(query_day):
            result = bs.query_all_stock(day=attempt_day)
            refs: list[BaoStockAShareRef] = []
            while result.error_code == "0" and result.next():
                code, trade_status, name = result.get_row_data()
                if trade_status != "1":
                    continue
                if not self._is_a_share_stock(code):
                    continue
                exchange_code = "SSE" if code.startswith("sh.") else "SZSE"
                refs.append(BaoStockAShareRef(code=code, name=name, exchange_code=exchange_code))
            if refs:
                return refs
        return []

    def download_all(self, *, limit: int | None = None, skip_existing: bool = True) -> dict[str, Any]:
        """Download all current A-share daily bars within the configured date window."""

        bs = self._import_baostock()
        self._login(bs)
        refs = self.fetch_stock_universe()
        if limit is not None:
            refs = refs[:limit]
        started_at = datetime.now(timezone.utc).isoformat()
        results: list[dict[str, Any]]
        total_rows: int
        if self.processes <= 1:
            results = []
            total_rows = 0
            for idx, ref in enumerate(refs, start=1):
                result = self.download_symbol_history(ref, skip_existing=skip_existing)
                results.append(result)
                total_rows += result["rows"]
                self._write_progress(completed=idx, total=len(refs), total_rows=total_rows)
                print(f"[{idx}/{len(refs)}] {ref.code} -> {result['status']} ({result['rows']} rows)", flush=True)
        else:
            self._logout(bs)
            results, total_rows = self._download_parallel(refs, skip_existing=skip_existing)
        manifest = {
            "source": "baostock",
            "started_at": started_at,
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "start_date": self.start_date,
            "end_date": self.end_date,
            "symbols_requested": len(refs),
            "downloaded_rows": total_rows,
            "results": results,
        }
        manifest_path = self.output_dir / "manifest.json"
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        progress_path = self.output_dir / "progress.json"
        progress_path.write_text(
            json.dumps(
                {
                    "status": "completed",
                    "completed": len(refs),
                    "total": len(refs),
                    "downloaded_rows": total_rows,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        self._logout(bs)
        return manifest

    def download_symbol_history(self, ref: BaoStockAShareRef, *, skip_existing: bool = True) -> dict[str, Any]:
        """Download one stock's daily history and persist it as gzip-compressed CSV."""

        symbol_dir = self.output_dir / ref.exchange_code.lower()
        symbol_dir.mkdir(parents=True, exist_ok=True)
        target = symbol_dir / f"{ref.symbol}.csv.gz"
        if skip_existing and target.exists() and target.stat().st_size > 0:
            return {"code": ref.code, "name": ref.name, "path": str(target), "rows": self._count_rows(target), "status": "skipped"}
        bs = self._import_baostock()
        last_error: str | None = None
        for attempt in range(1, 4):
            try:
                result = bs.query_history_k_data_plus(
                    ref.code,
                    ",".join(CSV_COLUMNS),
                    start_date=self.start_date,
                    end_date=self.end_date,
                    frequency="d",
                    adjustflag="3",
                )
                rows: list[dict[str, str]] = []
                while result.error_code == "0" and result.next():
                    rows.append(dict(zip(result.fields, result.get_row_data(), strict=False)))
                self._write_csv_gz(target, rows)
                meta_target = symbol_dir / f"{ref.symbol}.meta.json"
                meta_target.write_text(
                    json.dumps(
                        {
                            "code": ref.code,
                            "name": ref.name,
                            "exchange_code": ref.exchange_code,
                            "rows": len(rows),
                            "downloaded_at": datetime.now(timezone.utc).isoformat(),
                            "start_date": self.start_date,
                            "end_date": self.end_date,
                            "attempt": attempt,
                        },
                        ensure_ascii=False,
                        indent=2,
                    ),
                    encoding="utf-8",
                )
                return {"code": ref.code, "name": ref.name, "path": str(target), "rows": len(rows), "status": "downloaded"}
            except Exception as exc:  # pragma: no cover - depends on remote service behavior
                last_error = str(exc)
                try:
                    self._logout(bs)
                finally:
                    self._login(bs)
                time.sleep(0.5 * attempt)
        return {"code": ref.code, "name": ref.name, "path": str(target), "rows": 0, "status": "error", "error": last_error}

    def _import_baostock(self):
        """Import BaoStock lazily so the core package stays usable without it."""

        try:
            import baostock as bs
        except ModuleNotFoundError as exc:  # pragma: no cover - exercised through runtime setup
            raise RuntimeError(
                "baostock is not installed. Use the project venv and install it before downloading A-share data."
            ) from exc
        return bs

    def _login(self, bs) -> None:
        """Establish a BaoStock session."""

        if self._bs is not None:
            return
        login = bs.login()
        if login.error_code != "0":
            raise RuntimeError(f"BaoStock login failed: {login.error_code} {login.error_msg}")
        self._bs = bs

    def _logout(self, bs) -> None:
        """Close the BaoStock session if it was opened."""

        if self._bs is None:
            return
        try:
            bs.logout()
        finally:
            self._bs = None

    def _is_a_share_stock(self, code: str) -> bool:
        """Return whether the BaoStock code matches the current A-share stock pattern."""

        return bool(A_SHARE_CODE_PATTERN.match(code))

    def _candidate_query_days(self, query_day: str) -> list[str]:
        """Return fallback query dates to handle non-trading or future calendar days."""

        base_day = datetime.strptime(query_day, "%Y-%m-%d").date()
        return [(base_day - timedelta(days=offset)).isoformat() for offset in range(0, 15)]

    def _write_csv_gz(self, path: Path, rows: list[dict[str, str]]) -> None:
        """Write history rows into a gzip-compressed CSV file."""

        with gzip.open(path, "wt", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=CSV_COLUMNS)
            writer.writeheader()
            writer.writerows(rows)

    def _count_rows(self, path: Path) -> int:
        """Count data rows inside a gzip-compressed CSV file."""

        with gzip.open(path, "rt", encoding="utf-8") as handle:
            return max(sum(1 for _ in handle) - 1, 0)

    def _download_parallel(self, refs: list[BaoStockAShareRef], *, skip_existing: bool) -> tuple[list[dict[str, Any]], int]:
        """Download bars in multiple worker processes."""

        tasks = [
            (str(self.output_dir), self.start_date, self.end_date, refs[idx : idx + self.chunk_size], skip_existing)
            for idx in range(0, len(refs), self.chunk_size)
        ]
        results: list[dict[str, Any]] = []
        total_rows = 0
        completed = 0
        with ProcessPoolExecutor(max_workers=self.processes) as executor:
            futures = [executor.submit(_download_chunk_worker, task) for task in tasks]
            for future in as_completed(futures):
                batch_results = future.result()
                results.extend(batch_results)
                total_rows += sum(item["rows"] for item in batch_results)
                completed += len(batch_results)
                self._write_progress(completed=completed, total=len(refs), total_rows=total_rows)
                print(f"[{completed}/{len(refs)}] batch finished", flush=True)
        return results, total_rows

    def _write_progress(self, *, completed: int, total: int, total_rows: int) -> None:
        """Persist the current download progress for external monitoring."""

        progress_path = self.output_dir / "progress.json"
        progress_path.write_text(
            json.dumps(
                {
                    "status": "running",
                    "completed": completed,
                    "total": total,
                    "downloaded_rows": total_rows,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )


def _download_chunk_worker(task: tuple[str, str, str, list[BaoStockAShareRef], bool]) -> list[dict[str, Any]]:
    """Download one chunk of symbols inside a worker process."""

    output_dir, start_date, end_date, refs, skip_existing = task
    downloader = BaoStockAShareHistoryDownloader(
        output_dir=output_dir,
        start_date=start_date,
        end_date=end_date,
        processes=1,
    )
    bs = downloader._import_baostock()
    downloader._login(bs)
    try:
        return [downloader.download_symbol_history(ref, skip_existing=skip_existing) for ref in refs]
    finally:
        downloader._logout(bs)


def main() -> None:
    """Command-line entrypoint for the BaoStock A-share downloader."""

    parser = argparse.ArgumentParser(description="Download A-share daily historical data with BaoStock.")
    parser.add_argument(
        "--output-dir",
        default="/Users/holden/Project/finance_devepment/quant_exchange/data/cn_equities/a_share/daily_raw",
        help="Directory where gzip-compressed CSV files will be stored.",
    )
    parser.add_argument("--start-date", default="2010-01-01", help="Download start date in YYYY-MM-DD format.")
    parser.add_argument(
        "--end-date",
        default=None,
        help="Download end date in YYYY-MM-DD format. Defaults to today.",
    )
    parser.add_argument("--limit", type=int, default=None, help="Optional symbol limit for partial downloads.")
    parser.add_argument(
        "--refresh-existing",
        action="store_true",
        help="Re-download symbols even if local files already exist.",
    )
    parser.add_argument("--processes", type=int, default=6, help="Worker process count for batch downloads.")
    parser.add_argument("--chunk-size", type=int, default=50, help="Symbols per batch task.")
    args = parser.parse_args()
    downloader = BaoStockAShareHistoryDownloader(
        output_dir=args.output_dir,
        start_date=args.start_date,
        end_date=args.end_date,
        processes=args.processes,
        chunk_size=args.chunk_size,
    )
    manifest = downloader.download_all(limit=args.limit, skip_existing=not args.refresh_existing)
    print(json.dumps({k: v for k, v in manifest.items() if k != "results"}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
