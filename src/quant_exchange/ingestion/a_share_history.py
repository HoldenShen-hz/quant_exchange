"""Download daily A-share historical data from Eastmoney into local files."""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import math
import ssl
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import certifi
except ModuleNotFoundError:  # pragma: no cover - depends on local environment
    certifi = None


LIST_FIELDS = "f12,f14,f13"
LIST_FS = "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23"
KLINE_FIELDS1 = "f1,f2,f3,f4,f5,f6"
KLINE_FIELDS2 = "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
CSV_COLUMNS = [
    "trade_date",
    "open",
    "close",
    "high",
    "low",
    "volume",
    "amount",
    "amplitude",
    "pct_change",
    "change",
    "turnover_rate",
]


@dataclass(slots=True, frozen=True)
class AShareInstrumentRef:
    """Minimal stock reference used by the A-share downloader."""

    symbol: str
    secid: str
    name: str
    exchange_code: str


@dataclass(slots=True, frozen=True)
class AShareDownloadResult:
    """One symbol download result."""

    symbol: str
    rows: int
    path: str
    status: str
    error: str | None = None


class EastmoneyAShareHistoryDownloader:
    """Download and persist daily A-share bars for the current stock universe."""

    def __init__(
        self,
        output_dir: str | Path,
        *,
        start_date: str = "19900101",
        end_date: str = "20500101",
        adjust: str = "raw",
        page_size: int = 5000,
        max_workers: int = 8,
        timeout: int = 30,
    ) -> None:
        self.output_dir = Path(output_dir)
        self.start_date = start_date
        self.end_date = end_date
        self.adjust = adjust
        self.page_size = page_size
        self.max_workers = max_workers
        self.timeout = timeout
        self.output_dir.mkdir(parents=True, exist_ok=True)
        if certifi is not None:
            self.ssl_context = ssl.create_default_context(cafile=certifi.where())
        else:
            self.ssl_context = ssl.create_default_context()

    def fetch_stock_universe(self) -> list[AShareInstrumentRef]:
        """Fetch the current A-share stock universe from the Eastmoney list endpoint."""

        first_page = self._fetch_json(self._build_list_url(page=1))
        data = first_page.get("data", {})
        total = int(data.get("total", 0))
        refs = self._parse_stock_list_payload(first_page)
        total_pages = max(math.ceil(total / self.page_size), 1)
        for page in range(2, total_pages + 1):
            refs.extend(self._parse_stock_list_payload(self._fetch_json(self._build_list_url(page=page))))
        return refs

    def download_all(self, *, limit: int | None = None, skip_existing: bool = True) -> dict[str, Any]:
        """Download the daily history for the full current A-share universe."""

        refs = self.fetch_stock_universe()
        if limit is not None:
            refs = refs[:limit]
        started_at = datetime.now(timezone.utc).isoformat()
        results: list[AShareDownloadResult] = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self.download_symbol_history, ref, skip_existing=skip_existing): ref.symbol for ref in refs
            }
            for idx, future in enumerate(as_completed(futures), start=1):
                result = future.result()
                results.append(result)
                print(
                    f"[{idx}/{len(futures)}] {result.symbol} -> {result.status} ({result.rows} rows)",
                    flush=True,
                )
        manifest = {
            "started_at": started_at,
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "start_date": self.start_date,
            "end_date": self.end_date,
            "adjust": self.adjust,
            "symbols_requested": len(refs),
            "symbols_downloaded": sum(1 for result in results if result.status in {"downloaded", "skipped"}),
            "downloaded_rows": sum(result.rows for result in results),
            "results": [result.__dict__ for result in sorted(results, key=lambda item: item.symbol)],
        }
        manifest_path = self.output_dir / "manifest.json"
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        return manifest

    def download_symbol_history(self, ref: AShareInstrumentRef, *, skip_existing: bool = True) -> AShareDownloadResult:
        """Download daily bars for one stock and store them as gzip-compressed CSV."""

        symbol_dir = self.output_dir / ref.exchange_code.lower()
        symbol_dir.mkdir(parents=True, exist_ok=True)
        target = symbol_dir / f"{ref.symbol}.csv.gz"
        if skip_existing and target.exists() and target.stat().st_size > 0:
            rows = self._count_rows(target)
            return AShareDownloadResult(symbol=ref.symbol, rows=rows, path=str(target), status="skipped")
        payload = self._fetch_json(self._build_kline_url(ref.secid))
        rows = self._parse_kline_payload(payload)
        self._write_csv_gz(target, rows)
        meta_target = symbol_dir / f"{ref.symbol}.meta.json"
        meta_target.write_text(
            json.dumps(
                {
                    "symbol": ref.symbol,
                    "secid": ref.secid,
                    "name": ref.name,
                    "exchange_code": ref.exchange_code,
                    "rows": len(rows),
                    "downloaded_at": datetime.now(timezone.utc).isoformat(),
                    "start_date": self.start_date,
                    "end_date": self.end_date,
                    "adjust": self.adjust,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        return AShareDownloadResult(symbol=ref.symbol, rows=len(rows), path=str(target), status="downloaded")

    def _build_list_url(self, *, page: int) -> str:
        """Build the Eastmoney A-share list URL."""

        params = {
            "pn": page,
            "pz": self.page_size,
            "po": 1,
            "np": 1,
            "fltt": 2,
            "invt": 2,
            "fid": "f3",
            "fs": LIST_FS,
            "fields": LIST_FIELDS,
        }
        return f"https://82.push2.eastmoney.com/api/qt/clist/get?{urllib.parse.urlencode(params)}"

    def _build_kline_url(self, secid: str) -> str:
        """Build the Eastmoney daily kline URL for one stock."""

        adjust_map = {"raw": 0, "qfq": 1, "hfq": 2}
        params = {
            "secid": secid,
            "fields1": KLINE_FIELDS1,
            "fields2": KLINE_FIELDS2,
            "klt": 101,
            "fqt": adjust_map[self.adjust],
            "beg": self.start_date,
            "end": self.end_date,
        }
        return f"https://push2his.eastmoney.com/api/qt/stock/kline/get?{urllib.parse.urlencode(params)}"

    def _fetch_json(self, url: str) -> dict[str, Any]:
        """Fetch one JSON payload from Eastmoney."""

        request = urllib.request.Request(
            url,
            headers={
                "User-Agent": "Mozilla/5.0",
                "Referer": "https://quote.eastmoney.com/",
                "Accept": "application/json,text/plain,*/*",
            },
        )
        with urllib.request.urlopen(request, timeout=self.timeout, context=self.ssl_context) as response:
            return json.load(response)

    def _parse_stock_list_payload(self, payload: dict[str, Any]) -> list[AShareInstrumentRef]:
        """Parse one stock-list payload into normalized references."""

        records = payload.get("data", {}).get("diff", []) or []
        refs: list[AShareInstrumentRef] = []
        for record in records:
            code = str(record["f12"])
            market_flag = str(record["f13"])
            exchange_code = "SZSE" if market_flag == "0" else "SSE"
            secid = f"{market_flag}.{code}"
            refs.append(AShareInstrumentRef(symbol=code, secid=secid, name=str(record["f14"]), exchange_code=exchange_code))
        return refs

    def _parse_kline_payload(self, payload: dict[str, Any]) -> list[dict[str, str]]:
        """Parse one Eastmoney daily kline payload into CSV-ready dictionaries."""

        klines = payload.get("data", {}).get("klines", []) or []
        rows: list[dict[str, str]] = []
        for line in klines:
            fields = line.split(",")
            if len(fields) < len(CSV_COLUMNS):
                continue
            rows.append(dict(zip(CSV_COLUMNS, fields, strict=False)))
        return rows

    def _write_csv_gz(self, path: Path, rows: list[dict[str, str]]) -> None:
        """Write normalized rows into a gzip-compressed CSV file."""

        with gzip.open(path, "wt", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=CSV_COLUMNS)
            writer.writeheader()
            writer.writerows(rows)

    def _count_rows(self, path: Path) -> int:
        """Count data rows inside a gzip-compressed CSV file."""

        with gzip.open(path, "rt", encoding="utf-8") as handle:
            return max(sum(1 for _ in handle) - 1, 0)


def main() -> None:
    """Command-line entrypoint for the A-share history downloader."""

    parser = argparse.ArgumentParser(description="Download A-share daily historical data from Eastmoney.")
    parser.add_argument(
        "--output-dir",
        default="/Users/holden/Project/finance_devepment/quant_exchange/data/cn_equities/a_share/daily_raw",
        help="Directory where gzip-compressed CSV files will be stored.",
    )
    parser.add_argument("--start-date", default="19900101", help="Download start date in YYYYMMDD format.")
    parser.add_argument("--end-date", default="20500101", help="Download end date in YYYYMMDD format.")
    parser.add_argument("--adjust", choices=["raw", "qfq", "hfq"], default="raw", help="Price adjustment mode.")
    parser.add_argument("--page-size", type=int, default=5000, help="Page size for the stock universe endpoint.")
    parser.add_argument("--max-workers", type=int, default=8, help="Concurrent worker count.")
    parser.add_argument("--timeout", type=int, default=30, help="Per-request timeout in seconds.")
    parser.add_argument("--limit", type=int, default=None, help="Optional symbol limit for partial downloads.")
    parser.add_argument(
        "--refresh-existing",
        action="store_true",
        help="Re-download symbols even if local files already exist.",
    )
    args = parser.parse_args()
    downloader = EastmoneyAShareHistoryDownloader(
        output_dir=args.output_dir,
        start_date=args.start_date,
        end_date=args.end_date,
        adjust=args.adjust,
        page_size=args.page_size,
        max_workers=args.max_workers,
        timeout=args.timeout,
    )
    manifest = downloader.download_all(limit=args.limit, skip_existing=not args.refresh_existing)
    print(json.dumps({k: v for k, v in manifest.items() if k != "results"}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
