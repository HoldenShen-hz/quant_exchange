"""Official master-data importers for China, Hong Kong, and US stock universes."""

from __future__ import annotations

import argparse
import csv
import io
import json
import re
import ssl
import urllib.request
import zipfile
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable

from quant_exchange.core.models import Instrument, MarketType
from quant_exchange.ingestion.a_share_baostock import BaoStockAShareHistoryDownloader, BaoStockAShareRef
from quant_exchange.persistence.database import SQLitePersistence
from quant_exchange.stocks.service import StockDirectoryService, StockProfile


def _now_iso() -> str:
    """Return the current UTC timestamp in ISO-8601 format."""

    return datetime.now(timezone.utc).isoformat()


def _project_root() -> Path:
    """Resolve the repository root for default cache and database paths."""

    return Path(__file__).resolve().parents[3]


@dataclass(slots=True, frozen=True)
class StockMasterRecord:
    """Normalized stock listing metadata before conversion into platform models."""

    instrument_id: str
    symbol: str
    company_name: str
    market_region: str
    exchange_code: str
    board: str
    lot_size: float
    currency: str
    settlement_cycle: str
    short_sellable: bool
    trading_sessions: tuple[tuple[str, str], ...]
    listing_date: date | None = None
    source: str = "official_master_data"


class StockMasterImportService:
    """Import official CN/HK/US stock master data into the stock directory service."""

    US_NASDAQ_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
    US_OTHER_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"
    HKEX_LIST_URL = "https://www.hkex.com.hk/eng/services/trading/securities/securitieslists/ListOfSecurities.xlsx"

    CN_SESSIONS = (("09:30", "11:30"), ("13:00", "15:00"))
    HK_SESSIONS = (("09:30", "12:00"), ("13:00", "16:00"))
    US_SESSIONS = (("09:30", "16:00"),)

    US_EXCHANGE_MAP = {
        "A": "NYSEAMERICAN",
        "N": "NYSE",
        "P": "NYSEARCA",
        "V": "IEX",
        "Z": "CBOEBZX",
    }

    def __init__(
        self,
        stock_directory: StockDirectoryService,
        *,
        cache_dir: str | Path | None = None,
        text_fetcher: Callable[[str], str] | None = None,
        bytes_fetcher: Callable[[str], bytes] | None = None,
        a_share_downloader_factory: Callable[[], BaoStockAShareHistoryDownloader] | None = None,
    ) -> None:
        self.stock_directory = stock_directory
        self.cache_dir = Path(cache_dir) if cache_dir is not None else _project_root() / "data" / "refdata" / "stock_master"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._fetch_text = text_fetcher or self._default_fetch_text
        self._fetch_bytes = bytes_fetcher or self._default_fetch_bytes
        self._a_share_downloader_factory = a_share_downloader_factory or self._default_a_share_downloader

    def import_all(self, *, refresh_cache: bool = False) -> dict[str, Any]:
        """Import the full A/H/US equity master data universe into persistence."""

        counts: dict[str, int] = {}
        total_imported = 0
        for region, loader in (
            ("CN", self.fetch_a_share_master),
            ("HK", self.fetch_hk_stock_master),
            ("US", self.fetch_us_stock_master),
        ):
            records = loader(refresh_cache=refresh_cache)
            counts[region] = len(records)
            total_imported += len(records)
            self._upsert_records(records)
        return {
            "status": "completed",
            "imported_at": _now_iso(),
            "regions": counts,
            "total_imported": total_imported,
        }

    def fetch_a_share_master(self, *, refresh_cache: bool = False) -> list[StockMasterRecord]:
        """Fetch the full current A-share stock universe."""

        cache_path = self.cache_dir / "a_share_universe.json"
        refs_payload: list[dict[str, Any]]
        if cache_path.exists() and not refresh_cache:
            refs_payload = json.loads(cache_path.read_text(encoding="utf-8"))
        else:
            downloader = self._a_share_downloader_factory()
            refs = downloader.fetch_stock_universe()
            refs_payload = [{"code": ref.code, "name": ref.name, "exchange_code": ref.exchange_code} for ref in refs]
            cache_path.write_text(json.dumps(refs_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        records = [
            self._record_from_a_share_ref(
                BaoStockAShareRef(
                    code=str(item["code"]),
                    name=str(item["name"]),
                    exchange_code=str(item["exchange_code"]),
                )
            )
            for item in refs_payload
        ]
        return self._deduplicate(records)

    def fetch_us_stock_master(self, *, refresh_cache: bool = False) -> list[StockMasterRecord]:
        """Fetch the full listed US equity universe from Nasdaq Trader symbol directories."""

        nasdaq_text = self._cached_text("nasdaqlisted.txt", self.US_NASDAQ_LISTED_URL, refresh_cache=refresh_cache)
        other_text = self._cached_text("otherlisted.txt", self.US_OTHER_LISTED_URL, refresh_cache=refresh_cache)
        records: list[StockMasterRecord] = []
        records.extend(self._parse_us_nasdaq_listed(nasdaq_text))
        records.extend(self._parse_us_other_listed(other_text))
        return self._deduplicate(records)

    def fetch_hk_stock_master(self, *, refresh_cache: bool = False) -> list[StockMasterRecord]:
        """Fetch the Hong Kong equity master data from the HKEX securities list."""

        workbook_bytes = self._cached_bytes("hkex_list.xlsx", self.HKEX_LIST_URL, refresh_cache=refresh_cache)
        return self._deduplicate(self._parse_hkex_workbook(workbook_bytes))

    def _upsert_records(self, records: list[StockMasterRecord]) -> None:
        """Persist and register imported records inside the stock directory."""

        for record in records:
            instrument, profile = self._to_platform_models(record)
            self.stock_directory.upsert_stock(instrument, profile)

    def _record_from_a_share_ref(self, ref: BaoStockAShareRef) -> StockMasterRecord:
        """Convert one BaoStock A-share reference into the normalized import shape."""

        symbol = f"{ref.symbol}.{'SH' if ref.exchange_code == 'SSE' else 'SZ'}"
        return StockMasterRecord(
            instrument_id=symbol,
            symbol=symbol,
            company_name=ref.name,
            market_region="CN",
            exchange_code=ref.exchange_code,
            board=self._infer_cn_board(ref.symbol),
            lot_size=100,
            currency="CNY",
            settlement_cycle="T+1",
            short_sellable=False,
            trading_sessions=self.CN_SESSIONS,
            source="BaoStock stock universe",
        )

    def _parse_us_nasdaq_listed(self, payload: str) -> list[StockMasterRecord]:
        """Parse Nasdaq-listed equities from the official Nasdaq Trader flat file."""

        reader = csv.DictReader(io.StringIO(payload), delimiter="|")
        records: list[StockMasterRecord] = []
        for row in reader:
            symbol = str(row.get("Symbol") or "").strip()
            security_name = str(row.get("Security Name") or "").strip()
            if not symbol or symbol == "File Creation Time":
                continue
            if str(row.get("Test Issue") or "").strip().upper() == "Y":
                continue
            if str(row.get("ETF") or "").strip().upper() == "Y":
                continue
            if not self._is_equity_security_name(security_name):
                continue
            records.append(
                StockMasterRecord(
                    instrument_id=f"{symbol}.US",
                    symbol=f"{symbol}.US",
                    company_name=self._clean_security_name(security_name),
                    market_region="US",
                    exchange_code="NASDAQ",
                    board="NASDAQ",
                    lot_size=self._parse_float(row.get("Round Lot Size"), default=100),
                    currency="USD",
                    settlement_cycle="T+1",
                    short_sellable=True,
                    trading_sessions=self.US_SESSIONS,
                    source="Nasdaq Trader nasdaqlisted.txt",
                )
            )
        return records

    def _parse_us_other_listed(self, payload: str) -> list[StockMasterRecord]:
        """Parse NYSE and other listed US equities from the Nasdaq Trader flat file."""

        reader = csv.DictReader(io.StringIO(payload), delimiter="|")
        records: list[StockMasterRecord] = []
        for row in reader:
            symbol = str(row.get("ACT Symbol") or "").strip()
            security_name = str(row.get("Security Name") or "").strip()
            exchange_code = str(row.get("Exchange") or "").strip().upper()
            if not symbol or symbol == "File Creation Time":
                continue
            if str(row.get("Test Issue") or "").strip().upper() == "Y":
                continue
            if str(row.get("ETF") or "").strip().upper() == "Y":
                continue
            if not self._is_equity_security_name(security_name):
                continue
            normalized_exchange = self.US_EXCHANGE_MAP.get(exchange_code, f"US-{exchange_code}")
            records.append(
                StockMasterRecord(
                    instrument_id=f"{symbol}.US",
                    symbol=f"{symbol}.US",
                    company_name=self._clean_security_name(security_name),
                    market_region="US",
                    exchange_code=normalized_exchange,
                    board=normalized_exchange,
                    lot_size=self._parse_float(row.get("Round Lot Size"), default=100),
                    currency="USD",
                    settlement_cycle="T+1",
                    short_sellable=True,
                    trading_sessions=self.US_SESSIONS,
                    source="Nasdaq Trader otherlisted.txt",
                )
            )
        return records

    def _parse_hkex_workbook(self, payload: bytes) -> list[StockMasterRecord]:
        """Parse Hong Kong listed equities from the HKEX securities workbook."""

        rows = self._xlsx_rows(payload)
        if not rows:
            return []
        header_index = next((idx for idx, row in enumerate(rows) if row and row[0] == "Stock Code"), None)
        if header_index is None:
            return []
        header = rows[header_index]
        columns = {name: idx for idx, name in enumerate(header)}
        records: list[StockMasterRecord] = []
        for row in rows[header_index + 1 :]:
            stock_code = self._cell(row, columns, "Stock Code").replace(" ", "")
            category = self._cell(row, columns, "Category")
            sub_category = self._cell(row, columns, "Sub-Category")
            if not stock_code or not stock_code.isdigit():
                continue
            if category != "Equity":
                continue
            if "Equity Securities" not in sub_category:
                continue
            symbol = f"{stock_code.zfill(5)}.HK"
            board = "GEM" if "GEM" in sub_category.upper() else "Main Board"
            records.append(
                StockMasterRecord(
                    instrument_id=symbol,
                    symbol=symbol,
                    company_name=self._clean_company_name(self._cell(row, columns, "Name of Securities")),
                    market_region="HK",
                    exchange_code="HKEX",
                    board=board,
                    lot_size=self._parse_float(self._cell(row, columns, "Board Lot"), default=100),
                    currency=self._cell(row, columns, "Trading Currency") or "HKD",
                    settlement_cycle="T+2",
                    short_sellable=self._cell(row, columns, "Shortsell Eligible") == "Y",
                    trading_sessions=self.HK_SESSIONS,
                    source="HKEX ListOfSecurities.xlsx",
                )
            )
        return records

    def _to_platform_models(self, record: StockMasterRecord) -> tuple[Instrument, StockProfile]:
        """Convert one imported master-data record into runtime instrument/profile models."""

        sector, industry = self._infer_sector_industry(record.company_name, record.market_region, record.board)
        concepts = self._concepts_for_record(record, sector, industry)
        f10_summary = (
            f"{record.company_name} 已从官方股票主数据清单导入，当前提供市场、交易所、板块和基础目录信息；"
            "更完整的财务、F10 和事件数据会随着后续数据源接入持续补齐。"
        )
        instrument = Instrument(
            instrument_id=record.instrument_id,
            symbol=record.symbol,
            market=MarketType.STOCK,
            instrument_type="equity",
            market_region=record.market_region,
            tick_size=0.01,
            lot_size=record.lot_size,
            quote_currency=record.currency,
            base_currency="",
            settlement_cycle=record.settlement_cycle,
            short_sellable=record.short_sellable,
            trading_sessions=record.trading_sessions,
            trading_rules={"board_lot": record.lot_size, "listing_source": record.source},
        )
        profile = StockProfile(
            instrument_id=record.instrument_id,
            symbol=record.symbol,
            company_name=record.company_name,
            market_region=record.market_region,
            exchange_code=record.exchange_code,
            board=record.board,
            sector=sector,
            industry=industry,
            concepts=concepts,
            f10_summary=f10_summary,
            main_business=f"{industry} 相关业务为主，当前优先接入的是官方挂牌主数据。",
            products_services=f"{sector} 相关证券研究入口已开通，详细产品与服务数据待后续财务源补齐。",
            competitive_advantages=f"{record.market_region} 市场挂牌公司，支持在统一选股页按市场、交易所、板块和文本条件筛选。",
            risks="当前为主数据层导入，详细基本面和事件研究结论需结合更深财务与公告数据。",
            listing_date=record.listing_date,
            currency=record.currency,
        )
        return instrument, profile

    def _concepts_for_record(self, record: StockMasterRecord, sector: str, industry: str) -> tuple[str, ...]:
        """Generate simple concept tags from master-data attributes."""

        region_tag = {"CN": "A股", "HK": "港股", "US": "美股"}.get(record.market_region, record.market_region)
        tags = [region_tag, record.board, sector, industry, "官方主数据"]
        return tuple(tag for tag in tags if tag)

    def _infer_cn_board(self, symbol: str) -> str:
        """Infer the A-share board from the stock code prefix."""

        if symbol.startswith("688"):
            return "STAR Market"
        if symbol.startswith(("300", "301")):
            return "ChiNext"
        return "Main Board"

    def _infer_sector_industry(self, company_name: str, market_region: str, board: str) -> tuple[str, str]:
        """Infer a coarse sector and industry from the security name."""

        normalized = company_name.lower()
        rules = [
            (("银行", "bank"), ("Financials", "Banking")),
            (("保险", "insurance"), ("Financials", "Insurance")),
            (("证券", "brokerage", "capital"), ("Financials", "Brokerage")),
            (("医药", "药业", "bio", "biotech", "pharma", "therapeut"), ("Healthcare", "Pharmaceuticals")),
            (("半导体", "芯片", "semiconductor", "micro", "chip"), ("Technology", "Semiconductors")),
            (("软件", "云", "internet", "software", "tech", "digital", "ai"), ("Technology", "Software & Platforms")),
            (("汽车", "motor", "auto", "vehicle"), ("Consumer Discretionary", "Automobiles")),
            (("电池", "锂", "新能源", "solar", "energy", "oil", "gas"), ("Energy", "Energy & New Materials")),
            (("白酒", "酒", "beverage", "brew", "drink"), ("Consumer Staples", "Beverages")),
            (("食品", "food"), ("Consumer Staples", "Food Products")),
            (("家电", "appliance"), ("Consumer Discretionary", "Home Appliances")),
            (("地产", "property", "real estate", "reit"), ("Real Estate", "Property & REITs")),
            (("物流", "shipping", "port", "airline", "travel", "tour"), ("Industrials", "Transportation")),
            (("煤", "mining", "steel", "metal"), ("Materials", "Metals & Mining")),
            (("传媒", "media", "game", "gaming"), ("Communication Services", "Media & Entertainment")),
            (("零售", "retail", "commerce", "consumer"), ("Consumer Discretionary", "Retail & Commerce")),
        ]
        for keywords, result in rules:
            if any(keyword in normalized for keyword in keywords):
                return result
        fallback_sector = {"CN": "China Equities", "HK": "Hong Kong Equities", "US": "US Equities"}.get(
            market_region,
            "Imported Equities",
        )
        return fallback_sector, board

    def _is_equity_security_name(self, security_name: str) -> bool:
        """Return whether a US symbol-directory row looks like an equity security."""

        normalized = security_name.lower()
        blocked_tokens = (
            "etf",
            "etn",
            "warrant",
            "rights",
            "units",
            "unit",
            "note",
            "bond",
            "preferred",
            "depositary share",
            "trust certificate",
            "closed end",
            "fund",
        )
        return not any(token in normalized for token in blocked_tokens)

    def _clean_security_name(self, security_name: str) -> str:
        """Remove common listing suffixes from US security names."""

        cleaned = re.sub(r"\s*[-|]\s*(common stock|ordinary shares?|common shares?|class [a-z] ordinary shares?)\s*$", "", security_name, flags=re.I)
        cleaned = re.sub(r"\s+(common stock|ordinary shares?|common shares?|class [a-z] common stock)\s*$", "", cleaned, flags=re.I)
        cleaned = re.sub(r"\s{2,}", " ", cleaned).strip(" -")
        return self._clean_company_name(cleaned)

    def _clean_company_name(self, company_name: str) -> str:
        """Normalize company names for the stock directory."""

        cleaned = re.sub(r"\s{2,}", " ", company_name.replace("\n", " ")).strip()
        return cleaned or company_name

    def _cached_text(self, filename: str, url: str, *, refresh_cache: bool) -> str:
        """Return cached text or fetch it from the remote source."""

        path = self.cache_dir / filename
        if path.exists() and not refresh_cache:
            return path.read_text(encoding="utf-8")
        payload = self._fetch_text(url)
        path.write_text(payload, encoding="utf-8")
        return payload

    def _cached_bytes(self, filename: str, url: str, *, refresh_cache: bool) -> bytes:
        """Return cached bytes or fetch them from the remote source."""

        path = self.cache_dir / filename
        if path.exists() and not refresh_cache:
            return path.read_bytes()
        payload = self._fetch_bytes(url)
        path.write_bytes(payload)
        return payload

    def _default_fetch_text(self, url: str) -> str:
        """Fetch UTF-8 text from a remote URL with a relaxed SSL fallback."""

        return self._default_fetch_bytes(url).decode("utf-8")

    def _default_fetch_bytes(self, url: str) -> bytes:
        """Fetch bytes from a remote URL with a relaxed SSL fallback."""

        request = urllib.request.Request(url, headers={"User-Agent": "quant-exchange/0.1"})
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return response.read()
        except Exception:
            insecure_context = ssl._create_unverified_context()
            with urllib.request.urlopen(request, timeout=30, context=insecure_context) as response:
                return response.read()

    def _default_a_share_downloader(self) -> BaoStockAShareHistoryDownloader:
        """Create the default BaoStock-based A-share universe loader."""

        return BaoStockAShareHistoryDownloader(output_dir=self.cache_dir / "a_share_history_cache")

    def _deduplicate(self, records: list[StockMasterRecord]) -> list[StockMasterRecord]:
        """Keep the latest unique record for each instrument identifier."""

        deduped: dict[str, StockMasterRecord] = {}
        for record in records:
            deduped[record.instrument_id] = record
        return [deduped[key] for key in sorted(deduped)]

    def _xlsx_rows(self, payload: bytes) -> list[list[str]]:
        """Decode worksheet rows from an XLSX workbook using stdlib zip/xml parsing."""

        ns = {
            "main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
            "rel": "http://schemas.openxmlformats.org/package/2006/relationships",
        }
        with zipfile.ZipFile(io.BytesIO(payload)) as archive:
            shared_strings = self._xlsx_shared_strings(archive, ns)
            workbook = ET.fromstring(archive.read("xl/workbook.xml"))
            relations = ET.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
            rel_map = {
                relation.attrib["Id"]: relation.attrib["Target"]
                for relation in relations.findall("rel:Relationship", ns)
            }
            sheet = workbook.find("main:sheets/main:sheet", ns)
            if sheet is None:
                return []
            relationship_id = sheet.attrib.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id", "")
            target = rel_map.get(relationship_id, "")
            if not target:
                return []
            worksheet = ET.fromstring(archive.read(f"xl/{target}"))
            rows: list[list[str]] = []
            for row in worksheet.findall("main:sheetData/main:row", ns):
                values: dict[int, str] = {}
                max_index = 0
                for cell in row.findall("main:c", ns):
                    reference = cell.attrib.get("r", "")
                    column_letters = re.match(r"[A-Z]+", reference)
                    if column_letters is None:
                        continue
                    column_index = self._column_index(column_letters.group(0))
                    max_index = max(max_index, column_index)
                    values[column_index] = self._xlsx_cell_value(cell, shared_strings, ns)
                if max_index == 0:
                    rows.append([])
                    continue
                rows.append([values.get(index, "").strip() for index in range(1, max_index + 1)])
            return rows

    def _xlsx_shared_strings(self, archive: zipfile.ZipFile, ns: dict[str, str]) -> list[str]:
        """Load shared strings from an XLSX package."""

        if "xl/sharedStrings.xml" not in archive.namelist():
            return []
        root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
        shared_strings: list[str] = []
        for item in root.findall("main:si", ns):
            shared_strings.append("".join(node.text or "" for node in item.findall(".//main:t", ns)))
        return shared_strings

    def _xlsx_cell_value(self, cell, shared_strings: list[str], ns: dict[str, str]) -> str:
        """Extract one textual cell value from the worksheet XML."""

        cell_type = cell.attrib.get("t")
        if cell_type == "inlineStr":
            return "".join(node.text or "" for node in cell.findall(".//main:t", ns))
        value = cell.find("main:v", ns)
        if value is None:
            return ""
        text = value.text or ""
        if cell_type == "s" and text.isdigit():
            return shared_strings[int(text)]
        return text

    def _column_index(self, column_letters: str) -> int:
        """Convert Excel column letters into a one-based index."""

        index = 0
        for char in column_letters:
            index = index * 26 + (ord(char) - ord("A") + 1)
        return index

    def _cell(self, row: list[str], columns: dict[str, int], name: str) -> str:
        """Read one named cell value from a row using the header-index map."""

        index = columns.get(name)
        if index is None or index >= len(row):
            return ""
        return row[index].strip()

    def _parse_float(self, value: Any, *, default: float) -> float:
        """Parse a float from master-data text fields."""

        if value in ("", None):
            return float(default)
        try:
            return float(str(value).replace(",", ""))
        except ValueError:
            return float(default)


def _import_stock_master(database_path: str, *, refresh_cache: bool = False) -> dict[str, Any]:
    """Run the stock master-data import against one SQLite database."""

    persistence = SQLitePersistence(database_path)
    directory = StockDirectoryService(persistence)
    try:
        importer = StockMasterImportService(directory)
        summary = importer.import_all(refresh_cache=refresh_cache)
    finally:
        persistence.close()
    return summary


def main() -> None:
    """Run the stock master-data import as a command-line utility."""

    project_root = _project_root()
    parser = argparse.ArgumentParser(description="Import official A/H/US stock master data into the local stock screener DB.")
    parser.add_argument(
        "--database",
        default=str(project_root / "data" / "runtime" / "quant_exchange.sqlite3"),
        help="SQLite database path used by the web workbench.",
    )
    parser.add_argument(
        "--refresh-cache",
        action="store_true",
        help="Ignore any cached master-data files and fetch the latest source payloads again.",
    )
    args = parser.parse_args()
    summary = _import_stock_master(args.database, refresh_cache=args.refresh_cache)
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
