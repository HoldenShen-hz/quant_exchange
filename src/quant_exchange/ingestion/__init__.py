"""Data ingestion utilities for external market datasets."""

from .a_share_history import AShareDownloadResult, AShareInstrumentRef, EastmoneyAShareHistoryDownloader
from .a_share_baostock import BaoStockAShareHistoryDownloader, BaoStockAShareRef
from .stock_master import StockMasterImportService, StockMasterRecord

__all__ = [
    "AShareDownloadResult",
    "AShareInstrumentRef",
    "BaoStockAShareHistoryDownloader",
    "BaoStockAShareRef",
    "EastmoneyAShareHistoryDownloader",
    "StockMasterImportService",
    "StockMasterRecord",
]
