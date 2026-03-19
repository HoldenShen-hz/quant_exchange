from __future__ import annotations

import io
import json
import tempfile
import unittest
from pathlib import Path
from wsgiref.util import setup_testing_defaults

from quant_exchange.config import AppSettings
from quant_exchange.ingestion.background_downloader import HistoryDownloadTarget
from quant_exchange.platform import QuantTradingPlatform


class WebHistoryProvider:
    """Simple fake provider used by the web tests for download-center actions."""

    provider_code = "a_share_baostock"

    def __init__(self) -> None:
        self.targets = [
            HistoryDownloadTarget(target_id="sh.600000", symbol="600000", name="浦发银行", exchange_code="SSE"),
            HistoryDownloadTarget(target_id="sz.000001", symbol="000001", name="平安银行", exchange_code="SZSE"),
        ]

    def discover_targets(self) -> list[HistoryDownloadTarget]:
        return list(self.targets)

    def download_target(self, target: HistoryDownloadTarget, *, skip_existing: bool) -> dict:
        return {"code": target.target_id, "name": target.name, "path": "/tmp/mock.csv.gz", "rows": 10, "status": "downloaded"}


class StockScreenerWebTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database_path = str(Path(self.temp_dir.name) / "webapp.sqlite3")
        self._build_platform()

    def tearDown(self) -> None:
        self.platform.close()
        self.temp_dir.cleanup()

    def _build_platform(self) -> None:
        self.platform = QuantTradingPlatform(AppSettings.from_mapping({"database": {"url": self.database_path}}))
        self.app = self.platform.web_app

    def _full_score_answers(self) -> dict[str, str]:
        return {
            question["question_id"]: question["correct_option_id"]
            for question in self.platform.learning._hub["quiz"]["questions"]
        }

    def _request(
        self,
        path: str,
        *,
        method: str = "GET",
        body: dict | None = None,
        headers: dict[str, str] | None = None,
    ) -> tuple[str, bytes]:
        environ = {}
        setup_testing_defaults(environ)
        query = ""
        if "?" in path:
            path, query = path.split("?", 1)
        environ["REQUEST_METHOD"] = method
        environ["PATH_INFO"] = path
        environ["QUERY_STRING"] = query
        raw = json.dumps(body).encode("utf-8") if body is not None else b""
        environ["CONTENT_LENGTH"] = str(len(raw))
        environ["wsgi.input"] = io.BytesIO(raw)
        if body is not None:
            environ["CONTENT_TYPE"] = "application/json"
        for key, value in (headers or {}).items():
            environ[key] = value
        response = {}

        def start_response(status, response_headers):
            response["status"] = status
            response["headers"] = response_headers

        payload = b"".join(self.app(environ, start_response))
        return response["status"], payload

    def test_sc_01_sector_and_f10_screening_work(self) -> None:
        payload = self.platform.api.list_stocks(
            market_region="CN",
            sector="Consumer Staples",
            min_roe=30,
            f10_query="品牌龙头",
        )
        self.assertEqual(payload["code"], "OK")
        symbols = [item["symbol"] for item in payload["data"]]
        self.assertEqual(symbols, ["600519.SH"])

    def test_sc_02_two_stocks_can_be_compared(self) -> None:
        payload = self.platform.api.compare_stocks("0700.HK", "MSFT.US")
        self.assertEqual(payload["code"], "OK")
        self.assertEqual(payload["data"]["left"]["company_name"], "腾讯控股")
        self.assertEqual(payload["data"]["right"]["company_name"], "Microsoft")
        self.assertGreaterEqual(len(payload["data"]["metrics"]), 5)
        self.assertGreaterEqual(len(payload["data"]["financial_scores"]), 5)

    def test_sc_03_financial_analysis_is_available(self) -> None:
        payload = self.platform.api.analyze_stock_financials("600519.SH")
        self.assertEqual(payload["code"], "OK")
        analysis = payload["data"]
        self.assertGreater(analysis["overall_score"], 70)
        self.assertIn("评级", analysis["summary"])
        self.assertGreaterEqual(len(analysis["strengths"]), 1)

    def test_sc_04_stock_history_is_available_for_charting(self) -> None:
        payload = self.platform.api.get_stock_history("MSFT.US", limit=90)
        self.assertEqual(payload["code"], "OK")
        self.assertEqual(payload["data"]["instrument_id"], "MSFT.US")
        self.assertEqual(payload["data"]["source"], "generated_demo")
        self.assertEqual(len(payload["data"]["bars"]), 90)
        self.assertGreater(payload["data"]["summary"]["latest_close"], 0)

    def test_sc_05_financial_history_is_persisted_for_each_stock(self) -> None:
        payload = self.platform.api.get_stock_financial_history("MSFT.US", limit=5)
        self.assertEqual(payload["code"], "OK")
        snapshots = payload["data"]["snapshots"]
        self.assertEqual(len(snapshots), 5)
        self.assertEqual(snapshots[0]["period_type"], "FY")
        self.assertGreater(snapshots[0]["revenue"], 0)
        self.assertGreaterEqual(snapshots[0]["fiscal_year"], snapshots[-1]["fiscal_year"])

    def test_sc_06_minute_bars_are_persisted_for_each_stock(self) -> None:
        payload = self.platform.api.get_stock_minute_bars("600519.SH", limit=120)
        self.assertEqual(payload["code"], "OK")
        bars = payload["data"]["bars"]
        self.assertEqual(len(bars), 120)
        self.assertEqual(payload["data"]["interval"], "1m")
        self.assertGreater(bars[-1]["close"], 0)
        self.assertGreaterEqual(bars[-1]["volume"], 0)

    def test_ui_01_web_app_serves_workbench_and_json_endpoints(self) -> None:
        status, body = self._request("/")
        self.assertEqual(status, "200 OK")
        self.assertIn("全球交易研究终端".encode("utf-8"), body)
        self.assertIn("主功能导航".encode("utf-8"), body)
        self.assertIn("首页".encode("utf-8"), body)
        self.assertIn("学习".encode("utf-8"), body)
        self.assertIn("自选".encode("utf-8"), body)
        self.assertIn("选股".encode("utf-8"), body)
        self.assertIn("个股".encode("utf-8"), body)
        self.assertIn("加密".encode("utf-8"), body)
        self.assertIn("数据".encode("utf-8"), body)
        self.assertIn("动态".encode("utf-8"), body)
        self.assertIn("模拟".encode("utf-8"), body)
        self.assertIn("模拟交易".encode("utf-8"), body)
        self.assertIn("加密货币市场".encode("utf-8"), body)
        self.assertIn("新手学习中心".encode("utf-8"), body)
        self.assertIn("知识库".encode("utf-8"), body)
        self.assertIn("搜索术语、主题或关键字".encode("utf-8"), body)
        self.assertIn("学习计划".encode("utf-8"), body)
        self.assertIn("学习检验".encode("utf-8"), body)
        self.assertIn("用户空间".encode("utf-8"), body)
        self.assertIn("策略机器人".encode("utf-8"), body)
        self.assertIn("通知中心".encode("utf-8"), body)
        self.assertIn("自选列表".encode("utf-8"), body)
        self.assertIn("智能选股器".encode("utf-8"), body)
        self.assertIn("首页概览".encode("utf-8"), body)
        self.assertIn("财务分析".encode("utf-8"), body)
        self.assertIn("历史数据下载中心".encode("utf-8"), body)
        self.assertIn("最近操作记录".encode("utf-8"), body)
        self.assertIn("最新价".encode("utf-8"), body)
        self.assertIn("行情引擎".encode("utf-8"), body)

        status, body = self._request("/api/stock/options", headers={"HTTP_X_CLIENT_ID": "browser-1"})
        self.assertEqual(status, "200 OK")
        filters = json.loads(body.decode("utf-8"))
        self.assertEqual(filters["code"], "OK")
        self.assertIn("CN", filters["data"]["market_regions"])
        self.assertIn("HK", filters["data"]["market_regions"])
        self.assertIn("US", filters["data"]["market_regions"])

        status, body = self._request("/api/stocks/universe?featured_limit=5", headers={"HTTP_X_CLIENT_ID": "browser-1"})
        self.assertEqual(status, "200 OK")
        universe = json.loads(body.decode("utf-8"))
        self.assertEqual(universe["code"], "OK")
        self.assertGreaterEqual(universe["data"]["total_count"], 7)
        self.assertEqual(len(universe["data"]["featured_stocks"]), 5)

        status, body = self._request("/api/stocks/count?market_region=US", headers={"HTTP_X_CLIENT_ID": "browser-1"})
        self.assertEqual(status, "200 OK")
        stock_count = json.loads(body.decode("utf-8"))
        self.assertEqual(stock_count["code"], "OK")
        self.assertGreaterEqual(stock_count["data"]["count"], 3)

        status, body = self._request("/api/stocks?market_region=US&f10_query=%E4%BA%91%E6%9C%8D%E5%8A%A1", headers={"HTTP_X_CLIENT_ID": "browser-1"})
        self.assertEqual(status, "200 OK")
        stocks = json.loads(body.decode("utf-8"))
        self.assertEqual(stocks["code"], "OK")
        symbols = [item["symbol"] for item in stocks["data"]]
        self.assertIn("MSFT.US", symbols)

        status, body = self._request("/api/stocks?market_region=CN&limit=10", headers={"HTTP_X_CLIENT_ID": "browser-1"})
        self.assertEqual(status, "200 OK")
        cn_stocks = json.loads(body.decode("utf-8"))
        self.assertEqual(cn_stocks["code"], "OK")
        self.assertGreaterEqual(len(cn_stocks["data"]), 1)
        first_row = cn_stocks["data"][0]
        self.assertIsNotNone(first_row["pe_ttm"])
        self.assertIsNotNone(first_row["roe"])
        self.assertIsNotNone(first_row["revenue_growth"])
        self.assertIsNotNone(first_row["dividend_yield"])

        status, body = self._request("/api/stocks/financials?instrument_id=MSFT.US", headers={"HTTP_X_CLIENT_ID": "browser-1"})
        self.assertEqual(status, "200 OK")
        analysis = json.loads(body.decode("utf-8"))
        self.assertEqual(analysis["code"], "OK")
        self.assertGreater(analysis["data"]["profitability_score"], 70)

        status, body = self._request("/api/stocks/klines?instrument_id=MSFT.US&limit=60", headers={"HTTP_X_CLIENT_ID": "browser-1"})
        self.assertEqual(status, "200 OK")
        history = json.loads(body.decode("utf-8"))
        self.assertEqual(history["code"], "OK")
        self.assertEqual(len(history["data"]["bars"]), 60)
        self.assertEqual(history["data"]["source"], "generated_demo")

        status, body = self._request("/api/crypto/universe?featured_limit=3", headers={"HTTP_X_CLIENT_ID": "browser-1"})
        self.assertEqual(status, "200 OK")
        crypto_universe = json.loads(body.decode("utf-8"))
        self.assertEqual(crypto_universe["code"], "OK")
        self.assertGreaterEqual(crypto_universe["data"]["total_count"], 5)
        self.assertEqual(len(crypto_universe["data"]["featured_assets"]), 3)

        status, body = self._request("/api/crypto/assets", headers={"HTTP_X_CLIENT_ID": "browser-1"})
        self.assertEqual(status, "200 OK")
        crypto_assets = json.loads(body.decode("utf-8"))
        self.assertEqual(crypto_assets["code"], "OK")
        self.assertGreaterEqual(len(crypto_assets["data"]), 5)
        self.assertTrue(any(item["instrument_id"] == "BTCUSDT" for item in crypto_assets["data"]))

        status, body = self._request("/api/crypto/detail?instrument_id=BTCUSDT", headers={"HTTP_X_CLIENT_ID": "browser-1"})
        self.assertEqual(status, "200 OK")
        crypto_detail = json.loads(body.decode("utf-8"))
        self.assertEqual(crypto_detail["code"], "OK")
        self.assertEqual(crypto_detail["data"]["asset_name"], "Bitcoin")

        status, body = self._request("/api/crypto/klines?instrument_id=BTCUSDT&limit=45", headers={"HTTP_X_CLIENT_ID": "browser-1"})
        self.assertEqual(status, "200 OK")
        crypto_history = json.loads(body.decode("utf-8"))
        self.assertEqual(crypto_history["code"], "OK")
        self.assertEqual(len(crypto_history["data"]["bars"]), 45)
        self.assertEqual(crypto_history["data"]["source"], "simulated_crypto_exchange")

        status, body = self._request(
            "/api/stocks/financial-history?instrument_id=MSFT.US&limit=4",
            headers={"HTTP_X_CLIENT_ID": "browser-1"},
        )
        self.assertEqual(status, "200 OK")
        financial_history = json.loads(body.decode("utf-8"))
        self.assertEqual(financial_history["code"], "OK")
        self.assertEqual(len(financial_history["data"]["snapshots"]), 4)

        status, body = self._request(
            "/api/stocks/minutes?instrument_id=600519.SH&limit=30",
            headers={"HTTP_X_CLIENT_ID": "browser-1"},
        )
        self.assertEqual(status, "200 OK")
        minute_bars = json.loads(body.decode("utf-8"))
        self.assertEqual(minute_bars["code"], "OK")
        self.assertEqual(len(minute_bars["data"]["bars"]), 30)

        status, body = self._request(
            "/api/market/realtime?instrument_ids=MSFT.US%2C600519.SH",
            headers={"HTTP_X_CLIENT_ID": "browser-1"},
        )
        self.assertEqual(status, "200 OK")
        realtime = json.loads(body.decode("utf-8"))
        self.assertEqual(realtime["code"], "OK")
        self.assertEqual(realtime["data"]["source"], "background_market_stream")
        self.assertEqual(len(realtime["data"]["quotes"]), 2)
        self.assertIn("recommended_poll_ms", realtime["data"])
        self.assertIn("live_window", realtime["data"])
        self.assertIn("top_gainers", realtime["data"]["summary"])

        status, body = self._request("/api/history-downloads", headers={"HTTP_X_CLIENT_ID": "browser-1"})
        self.assertEqual(status, "200 OK")
        downloads = json.loads(body.decode("utf-8"))
        self.assertEqual(downloads["code"], "OK")
        self.assertEqual(len(downloads["data"]), 3)
        a_share = next(item for item in downloads["data"] if item["job_id"] == "a_share_daily_history")
        self.assertTrue(a_share["supported"])
        self.assertIn("download", a_share["available_actions"])

        status, body = self._request("/api/learning/hub", headers={"HTTP_X_CLIENT_ID": "browser-1"})
        self.assertEqual(status, "200 OK")
        learning = json.loads(body.decode("utf-8"))
        self.assertEqual(learning["code"], "OK")
        self.assertGreaterEqual(len(learning["data"]["hub"]["lessons"]), 10)
        self.assertGreaterEqual(len(learning["data"]["hub"]["knowledge_base"]), 10)
        categories = {section["category"] for section in learning["data"]["hub"]["knowledge_base"]}
        self.assertIn("区块链与数字资产", categories)
        self.assertIn("保险与保障体系", categories)
        self.assertNotIn("correct_option_id", learning["data"]["hub"]["quiz"]["questions"][0])
        self.assertEqual(learning["data"]["progress"]["quiz_attempts"], 0)

        status, body = self._request("/api/strategy/templates", headers={"HTTP_X_CLIENT_ID": "browser-1"})
        self.assertEqual(status, "200 OK")
        templates = json.loads(body.decode("utf-8"))
        self.assertEqual(templates["code"], "OK")
        self.assertGreaterEqual(len(templates["data"]), 3)

        status, body = self._request("/api/paper/account?instrument_id=MSFT.US", headers={"HTTP_X_CLIENT_ID": "browser-1"})
        self.assertEqual(status, "200 OK")
        paper = json.loads(body.decode("utf-8"))
        self.assertEqual(paper["code"], "OK")
        self.assertIn("snapshot", paper["data"])
        self.assertIn("strategy_diff", paper["data"])

    def test_ui_02_workspace_state_is_saved_and_restored(self) -> None:
        client_headers = {"HTTP_X_CLIENT_ID": "browser-restore"}
        workspace_state = {
            "filters": {"market_region": "US", "f10_query": "云服务", "min_roe": "20"},
            "compare": {"left": "MSFT.US", "right": "AAPL.US"},
            "active_instrument_id": "MSFT.US",
            "watchlist": ["MSFT.US", "AAPL.US"],
            "chart": {"range": 60, "mode": "line"},
            "crypto": {"active_instrument_id": "ETHUSDT", "chart": {"range": 30, "mode": "line"}},
            "preset": "us_growth",
            "active_tab": "downloads",
            "learning": {"selected_lesson_id": "lesson_blockchain_digital_assets", "search_query": "保险"},
        }

        status, body = self._request(
            "/api/web/state",
            method="POST",
            body={"state": workspace_state},
            headers=client_headers,
        )
        self.assertEqual(status, "200 OK")
        payload = json.loads(body.decode("utf-8"))
        self.assertEqual(payload["code"], "OK")
        self.assertEqual(payload["data"]["state"]["filters"]["market_region"], "US")

        self.platform.close()
        self._build_platform()

        status, body = self._request("/api/web/state", headers=client_headers)
        self.assertEqual(status, "200 OK")
        restored = json.loads(body.decode("utf-8"))
        self.assertEqual(restored["code"], "OK")
        self.assertEqual(restored["data"]["state"]["compare"]["left"], "MSFT.US")
        self.assertEqual(restored["data"]["state"]["compare"]["right"], "AAPL.US")
        self.assertEqual(restored["data"]["state"]["active_instrument_id"], "MSFT.US")
        self.assertEqual(restored["data"]["state"]["watchlist"], ["MSFT.US", "AAPL.US"])
        self.assertEqual(restored["data"]["state"]["chart"]["range"], 60)
        self.assertEqual(restored["data"]["state"]["chart"]["mode"], "line")
        self.assertEqual(restored["data"]["state"]["crypto"]["active_instrument_id"], "ETHUSDT")
        self.assertEqual(restored["data"]["state"]["crypto"]["chart"]["range"], 30)
        self.assertEqual(restored["data"]["state"]["crypto"]["chart"]["mode"], "line")
        self.assertEqual(restored["data"]["state"]["preset"], "us_growth")
        self.assertEqual(restored["data"]["state"]["active_tab"], "downloads")
        self.assertEqual(restored["data"]["state"]["learning"]["selected_lesson_id"], "lesson_blockchain_digital_assets")
        self.assertEqual(restored["data"]["state"]["learning"]["search_query"], "保险")

    def test_ui_03_activity_log_is_recorded(self) -> None:
        client_headers = {"HTTP_X_CLIENT_ID": "browser-audit"}

        status, _ = self._request(
            "/api/web/events",
            method="POST",
            body={"event_type": "open_page", "payload": {"path": "/"}},
            headers=client_headers,
        )
        self.assertEqual(status, "200 OK")

        status, _ = self._request(
            "/api/web/events",
            method="POST",
            body={"event_type": "submit_filters", "payload": {"result_count": 2}},
            headers=client_headers,
        )
        self.assertEqual(status, "200 OK")

        status, body = self._request("/api/web/events?limit=5", headers=client_headers)
        self.assertEqual(status, "200 OK")
        payload = json.loads(body.decode("utf-8"))
        self.assertEqual(payload["code"], "OK")
        self.assertEqual(len(payload["data"]["events"]), 2)
        self.assertEqual(payload["data"]["events"][0]["event_type"], "submit_filters")
        self.assertEqual(payload["data"]["events"][1]["event_type"], "open_page")

    def test_ui_04_history_download_endpoint_starts_job_and_updates_status(self) -> None:
        self.platform.history_downloads.provider_factories["a_share_baostock"] = lambda config: WebHistoryProvider()
        client_headers = {"HTTP_X_CLIENT_ID": "browser-download"}

        status, body = self._request(
            "/api/history-downloads/start",
            method="POST",
            body={"job_id": "a_share_daily_history"},
            headers=client_headers,
        )
        self.assertEqual(status, "200 OK")
        started = json.loads(body.decode("utf-8"))
        self.assertEqual(started["code"], "OK")

        self.platform.history_downloads._jobs["a_share_daily_history"].join(timeout=2.0)

        status, body = self._request("/api/history-downloads", headers=client_headers)
        self.assertEqual(status, "200 OK")
        downloads = json.loads(body.decode("utf-8"))
        a_share = next(item for item in downloads["data"] if item["job_id"] == "a_share_daily_history")
        self.assertEqual(a_share["status"], "completed")
        self.assertEqual(a_share["completed_count"], 2)
        self.assertEqual(a_share["downloaded_rows"], 20)

    def test_ui_05_strategy_bot_endpoints_create_start_interact_and_notify(self) -> None:
        client_headers = {"HTTP_X_CLIENT_ID": "browser-bot"}

        status, body = self._request(
            "/api/bots",
            method="POST",
            body={"template_code": "ma_sentiment", "instrument_id": "600519.SH", "bot_name": "茅台趋势机器人"},
            headers=client_headers,
        )
        self.assertEqual(status, "200 OK")
        created = json.loads(body.decode("utf-8"))
        self.assertEqual(created["code"], "OK")
        bot_id = created["data"]["bot_id"]

        status, body = self._request(
            "/api/bots/start",
            method="POST",
            body={"bot_id": bot_id},
            headers=client_headers,
        )
        self.assertEqual(status, "200 OK")
        started = json.loads(body.decode("utf-8"))
        self.assertEqual(started["code"], "OK")
        self.assertEqual(started["data"]["status"], "running")

        status, body = self._request(
            "/api/bots/interact",
            method="POST",
            body={
                "bot_id": bot_id,
                "command": "set_param",
                "payload": {"updates": {"fast_window": "4", "max_weight": "0.72"}},
            },
            headers=client_headers,
        )
        self.assertEqual(status, "200 OK")
        updated = json.loads(body.decode("utf-8"))
        self.assertEqual(updated["code"], "OK")
        self.assertEqual(updated["data"]["params"]["fast_window"], 4)
        self.assertAlmostEqual(updated["data"]["params"]["max_weight"], 0.72)

        status, body = self._request("/api/bots", headers=client_headers)
        self.assertEqual(status, "200 OK")
        bots = json.loads(body.decode("utf-8"))
        self.assertEqual(bots["code"], "OK")
        self.assertEqual(len(bots["data"]), 1)
        self.assertEqual(bots["data"][0]["bot_id"], bot_id)

        status, body = self._request("/api/notifications?limit=10", headers=client_headers)
        self.assertEqual(status, "200 OK")
        notifications = json.loads(body.decode("utf-8"))
        self.assertEqual(notifications["code"], "OK")
        self.assertGreaterEqual(len(notifications["data"]["notifications"]), 3)
        self.assertTrue(
            any(item["event_type"] == "bot_command_set_param" for item in notifications["data"]["notifications"])
        )

    def test_ui_06_paper_trading_endpoints_submit_and_cancel_order(self) -> None:
        client_headers = {"HTTP_X_CLIENT_ID": "browser-paper"}
        self.platform.stocks.save_minute_bar(
            "000333.SZ",
            {
                "instrument_id": "000333.SZ",
                "bar_time": "2026-03-17T02:01:00+00:00",
                "open": 68.8,
                "high": 69.2,
                "low": 68.6,
                "close": 69.0,
                "volume": 1200,
                "turnover": 82_800.0,
                "market_region": "CN",
                "exchange_code": "SZSE",
                "source": "test_seed",
            },
        )

        status, body = self._request(
            "/api/paper/orders",
            method="POST",
            body={"instrument_id": "000333.SZ", "side": "buy", "quantity": 100, "order_type": "market"},
            headers=client_headers,
        )
        self.assertEqual(status, "200 OK")
        submitted = json.loads(body.decode("utf-8"))
        self.assertEqual(submitted["code"], "OK")
        self.assertEqual(submitted["data"]["last_action"]["type"], "submitted")
        self.assertGreaterEqual(len(submitted["data"]["positions"]), 1)

        status, body = self._request(
            "/api/paper/orders",
            method="POST",
            body={"instrument_id": "000333.SZ", "side": "buy", "quantity": 100, "order_type": "limit", "limit_price": 1.0},
            headers=client_headers,
        )
        self.assertEqual(status, "200 OK")
        pending = json.loads(body.decode("utf-8"))
        pending_order_id = pending["data"]["orders"][0]["order_id"]
        self.assertEqual(pending["data"]["orders"][0]["status"], "accepted")

        status, body = self._request(
            "/api/paper/orders/cancel",
            method="POST",
            body={"order_id": pending_order_id},
            headers=client_headers,
        )
        self.assertEqual(status, "200 OK")
        cancelled = json.loads(body.decode("utf-8"))
        self.assertEqual(cancelled["code"], "OK")
        self.assertEqual(cancelled["data"]["last_action"]["type"], "cancelled")

    def test_ui_07_learning_page_and_quiz_submit_work(self) -> None:
        client_headers = {"HTTP_X_CLIENT_ID": "browser-learning"}

        status, body = self._request("/learn", headers=client_headers)
        self.assertEqual(status, "200 OK")
        self.assertIn("新手学习中心".encode("utf-8"), body)

        status, body = self._request("/api/learning/hub", headers=client_headers)
        self.assertEqual(status, "200 OK")
        hub = json.loads(body.decode("utf-8"))
        self.assertEqual(hub["code"], "OK")
        self.assertGreaterEqual(len(hub["data"]["hub"]["knowledge_base"]), 10)

        status, body = self._request(
            "/api/learning/quiz",
            method="POST",
            body={"answers": self._full_score_answers()},
            headers=client_headers,
        )
        self.assertEqual(status, "200 OK")
        quiz = json.loads(body.decode("utf-8"))
        self.assertEqual(quiz["code"], "OK")
        self.assertTrue(quiz["data"]["result"]["passed"])
        self.assertEqual(quiz["data"]["result"]["score"], 100)
        self.assertEqual(quiz["data"]["progress"]["best_score"], 100)

    def test_ui_08_multi_user_workspace_and_learning_history_are_isolated(self) -> None:
        shared_client_headers = {"HTTP_X_CLIENT_ID": "shared-browser"}

        status, body = self._request(
            "/api/auth/register",
            method="POST",
            body={"username": "alice", "password": "pass-alice", "display_name": "Alice"},
            headers=shared_client_headers,
        )
        self.assertEqual(status, "200 OK")
        alice_login = json.loads(body.decode("utf-8"))
        self.assertEqual(alice_login["code"], "OK")
        alice_headers = {
            **shared_client_headers,
            "HTTP_AUTHORIZATION": f"Bearer {alice_login['data']['access_token']}",
        }

        status, body = self._request(
            "/api/auth/register",
            method="POST",
            body={"username": "bob", "password": "pass-bob", "display_name": "Bob"},
            headers=shared_client_headers,
        )
        self.assertEqual(status, "200 OK")
        bob_login = json.loads(body.decode("utf-8"))
        self.assertEqual(bob_login["code"], "OK")
        bob_headers = {
            **shared_client_headers,
            "HTTP_AUTHORIZATION": f"Bearer {bob_login['data']['access_token']}",
        }

        status, body = self._request(
            "/api/web/state",
            method="POST",
            body={
                "state": {
                    "filters": {"market_region": "US"},
                    "compare": {"left": "MSFT.US", "right": "AAPL.US"},
                    "active_instrument_id": "MSFT.US",
                    "watchlist": ["MSFT.US"],
                    "chart": {"range": 60, "mode": "line"},
                    "crypto": {"active_instrument_id": "ETHUSDT", "chart": {"range": 30, "mode": "line"}},
                    "preset": "us_growth",
                    "active_tab": "learning",
                    "learning": {"selected_lesson_id": "lesson_factor_backtest_bias"},
                }
            },
            headers=alice_headers,
        )
        self.assertEqual(status, "200 OK")

        status, body = self._request(
            "/api/web/state",
            method="POST",
            body={
                "state": {
                    "filters": {"market_region": "CN"},
                    "compare": {"left": "600519.SH", "right": "000333.SZ"},
                    "active_instrument_id": "600519.SH",
                    "watchlist": ["600519.SH"],
                    "chart": {"range": 120, "mode": "candles"},
                    "crypto": {"active_instrument_id": "BTCUSDT", "chart": {"range": 120, "mode": "candles"}},
                    "preset": "quality_cn",
                    "active_tab": "screener",
                    "learning": {"selected_lesson_id": "lesson_macro_cycle"},
                }
            },
            headers=bob_headers,
        )
        self.assertEqual(status, "200 OK")

        status, body = self._request("/api/web/state", headers=alice_headers)
        alice_state = json.loads(body.decode("utf-8"))
        self.assertEqual(alice_state["data"]["principal_type"], "user")
        self.assertEqual(alice_state["data"]["principal_id"], "alice")
        self.assertEqual(alice_state["data"]["state"]["filters"]["market_region"], "US")
        self.assertEqual(alice_state["data"]["state"]["crypto"]["active_instrument_id"], "ETHUSDT")
        self.assertEqual(alice_state["data"]["state"]["learning"]["selected_lesson_id"], "lesson_factor_backtest_bias")

        status, body = self._request("/api/web/state", headers=bob_headers)
        bob_state = json.loads(body.decode("utf-8"))
        self.assertEqual(bob_state["data"]["principal_id"], "bob")
        self.assertEqual(bob_state["data"]["state"]["filters"]["market_region"], "CN")
        self.assertEqual(bob_state["data"]["state"]["crypto"]["active_instrument_id"], "BTCUSDT")
        self.assertEqual(bob_state["data"]["state"]["learning"]["selected_lesson_id"], "lesson_macro_cycle")

        status, body = self._request(
            "/api/learning/quiz",
            method="POST",
            body={
                "answers": self._full_score_answers(),
                "learning": {"selected_lesson_id": "lesson_factor_backtest_bias"},
            },
            headers=alice_headers,
        )
        self.assertEqual(status, "200 OK")
        alice_quiz = json.loads(body.decode("utf-8"))
        self.assertEqual(alice_quiz["data"]["progress"]["quiz_attempts"], 1)

        status, body = self._request("/api/learning/hub", headers=alice_headers)
        alice_hub = json.loads(body.decode("utf-8"))
        self.assertEqual(alice_hub["data"]["progress"]["best_score"], 100)
        self.assertEqual(alice_hub["data"]["progress"]["current_lesson_id"], "lesson_factor_backtest_bias")

        status, body = self._request("/api/learning/hub", headers=bob_headers)
        bob_hub = json.loads(body.decode("utf-8"))
        self.assertEqual(bob_hub["data"]["progress"]["quiz_attempts"], 0)
        self.assertEqual(bob_hub["data"]["progress"]["current_lesson_id"], "lesson_macro_cycle")


if __name__ == "__main__":
    unittest.main()
