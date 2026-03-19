from __future__ import annotations

import io
import tempfile
import unittest
import zipfile
from pathlib import Path

from quant_exchange.ingestion.a_share_baostock import BaoStockAShareRef
from quant_exchange.ingestion.stock_master import StockMasterImportService
from quant_exchange.persistence.database import SQLitePersistence
from quant_exchange.stocks.service import StockDirectoryService


class _FakeAShareDownloader:
    """Minimal fake A-share universe downloader used by importer tests."""

    def fetch_stock_universe(self) -> list[BaoStockAShareRef]:
        return [
            BaoStockAShareRef(code="sh.600000", name="浦发银行", exchange_code="SSE"),
            BaoStockAShareRef(code="sz.000001", name="平安银行", exchange_code="SZSE"),
        ]


def _sample_hkex_workbook() -> bytes:
    """Build a tiny XLSX workbook containing two Hong Kong equity rows."""

    shared_strings = [
        "List of Securities",
        "Updated as at 17/03/2026",
        "Stock Code",
        "Name of Securities",
        "Category",
        "Sub-Category",
        "Board Lot",
        "ISIN",
        "Shortsell Eligible",
        "Trading Currency",
        "00001",
        "CKH HOLDINGS",
        "Equity",
        "Equity Securities (Main Board)",
        "500",
        "Y",
        "HKD",
        "00700",
        "TENCENT",
        "100",
    ]
    workbook_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"
 xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <sheets>
    <sheet name="Sheet1" sheetId="1" r:id="rId1"/>
  </sheets>
</workbook>"""
    workbook_rels_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
</Relationships>"""
    root_rels_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
</Relationships>"""
    content_types_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
  <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
  <Override PartName="/xl/sharedStrings.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml"/>
</Types>"""
    sheet_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <sheetData>
    <row r="1"><c r="A1" t="s"><v>0</v></c></row>
    <row r="2"><c r="A2" t="s"><v>1</v></c></row>
    <row r="3">
      <c r="A3" t="s"><v>2</v></c>
      <c r="B3" t="s"><v>3</v></c>
      <c r="C3" t="s"><v>4</v></c>
      <c r="D3" t="s"><v>5</v></c>
      <c r="E3" t="s"><v>6</v></c>
      <c r="F3" t="s"><v>7</v></c>
      <c r="G3" t="s"><v>8</v></c>
      <c r="H3" t="s"><v>9</v></c>
    </row>
    <row r="4">
      <c r="A4" t="s"><v>10</v></c>
      <c r="B4" t="s"><v>11</v></c>
      <c r="C4" t="s"><v>12</v></c>
      <c r="D4" t="s"><v>13</v></c>
      <c r="E4" t="s"><v>14</v></c>
      <c r="G4" t="s"><v>15</v></c>
      <c r="H4" t="s"><v>16</v></c>
    </row>
    <row r="5">
      <c r="A5" t="s"><v>17</v></c>
      <c r="B5" t="s"><v>18</v></c>
      <c r="C5" t="s"><v>12</v></c>
      <c r="D5" t="s"><v>13</v></c>
      <c r="E5" t="s"><v>19</v></c>
      <c r="G5" t="s"><v>15</v></c>
      <c r="H5" t="s"><v>16</v></c>
    </row>
  </sheetData>
</worksheet>"""
    shared_strings_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" count="20" uniqueCount="20">
  {}
</sst>""".format(
        "".join(f"<si><t>{item}</t></si>" for item in shared_strings)
    )

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("[Content_Types].xml", content_types_xml)
        archive.writestr("_rels/.rels", root_rels_xml)
        archive.writestr("xl/workbook.xml", workbook_xml)
        archive.writestr("xl/_rels/workbook.xml.rels", workbook_rels_xml)
        archive.writestr("xl/worksheets/sheet1.xml", sheet_xml)
        archive.writestr("xl/sharedStrings.xml", shared_strings_xml)
    return buffer.getvalue()


class StockMasterImportTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database_path = str(Path(self.temp_dir.name) / "stock_master.sqlite3")
        self.persistence = SQLitePersistence(self.database_path)
        self.directory = StockDirectoryService(self.persistence)
        self.importer = StockMasterImportService(
            self.directory,
            cache_dir=Path(self.temp_dir.name) / "cache",
            text_fetcher=self._fake_text_fetcher,
            bytes_fetcher=self._fake_bytes_fetcher,
            a_share_downloader_factory=_FakeAShareDownloader,
        )

    def tearDown(self) -> None:
        self.persistence.close()
        self.temp_dir.cleanup()

    def _fake_text_fetcher(self, url: str) -> str:
        if url.endswith("nasdaqlisted.txt"):
            return (
                "Symbol|Security Name|Market Category|Test Issue|Financial Status|Round Lot Size|ETF|NextShares\n"
                "AAL|American Airlines Group, Inc. - Common Stock|Q|N|N|100|N|N\n"
                "AADR|AdvisorShares Dorsey Wright ADR ETF|G|N|N|100|Y|N\n"
                "AACBR|Artius II Acquisition Inc. - Rights|G|N|N|100|N|N\n"
            )
        if url.endswith("otherlisted.txt"):
            return (
                "ACT Symbol|Security Name|Exchange|CQS Symbol|ETF|Round Lot Size|Test Issue|NASDAQ Symbol\n"
                "AA|Alcoa Corporation Common Stock |N|AA|N|100|N|AA\n"
                "AAA|Alternative Access First Priority CLO Bond ETF|P|AAA|Y|100|N|AAA\n"
            )
        raise AssertionError(f"Unexpected text URL: {url}")

    def _fake_bytes_fetcher(self, url: str) -> bytes:
        if url.endswith("ListOfSecurities.xlsx"):
            return _sample_hkex_workbook()
        raise AssertionError(f"Unexpected bytes URL: {url}")

    def test_import_all_persists_full_market_master_data(self) -> None:
        summary = self.importer.import_all(refresh_cache=True)

        self.assertEqual(summary["regions"]["CN"], 2)
        self.assertEqual(summary["regions"]["HK"], 2)
        self.assertEqual(summary["regions"]["US"], 2)
        self.assertEqual(summary["total_imported"], 6)
        self.assertEqual(self.persistence.count("ref_stock_profiles"), 6)

        reloaded = StockDirectoryService(self.persistence)
        self.assertEqual(reloaded.load_from_persistence(), 6)
        self.assertEqual(reloaded.count_stocks({"market_region": "CN"}), 2)
        self.assertEqual(reloaded.count_stocks({"market_region": "HK"}), 2)
        self.assertEqual(reloaded.count_stocks({"market_region": "US"}), 2)

        hk_stock = reloaded.get_stock_core("00001.HK")
        self.assertEqual(hk_stock["company_name"], "CKH HOLDINGS")
        self.assertEqual(hk_stock["board"], "Main Board")
        self.assertEqual(hk_stock["exchange_code"], "HKEX")

        us_stock = reloaded.get_stock_core("AAL.US")
        self.assertEqual(us_stock["company_name"], "American Airlines Group, Inc.")
        self.assertEqual(us_stock["exchange_code"], "NASDAQ")

        cn_stock = reloaded.get_stock_core("600000.SH")
        self.assertEqual(cn_stock["board"], "Main Board")
        self.assertEqual(cn_stock["exchange_code"], "SSE")
        self.assertIsNotNone(cn_stock["pe_ttm"])
        self.assertIsNotNone(cn_stock["roe"])
        self.assertIsNotNone(cn_stock["revenue_growth"])
        self.assertIsNotNone(cn_stock["dividend_yield"])

        listing = reloaded.list_stocks({"sort_by": "symbol", "sort_desc": False, "limit": 3})
        self.assertEqual([item["symbol"] for item in listing], ["000001.SZ", "00001.HK", "00700.HK"])
        self.assertTrue(all(item["pe_ttm"] is not None for item in listing))
        self.assertTrue(all(item["roe"] is not None for item in listing))

        summary_payload = reloaded.universe_summary(featured_limit=4)
        self.assertEqual(summary_payload["total_count"], 6)
        self.assertEqual(summary_payload["market_counts"]["HK"], 2)
        self.assertEqual(len(summary_payload["featured_stocks"]), 4)
