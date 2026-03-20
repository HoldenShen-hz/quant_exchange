"""Pure-stdlib WSGI app that serves the stock screener web workbench."""

from __future__ import annotations

import base64
import hashlib
import json
import socket
import struct
import threading
import queue
import select
from pathlib import Path
from typing import Callable
from urllib.parse import parse_qs
from wsgiref.simple_server import make_server


class SSEEventBroadcaster:
    """Thread-safe SSE event broadcaster for real-time push to browser clients.

    Clients subscribe with a client_id and receive events broadcast by the server.
    Used to replace HTTP polling for bot state and dashboard updates.
    """

    def __init__(self) -> None:
        self._clients: dict[str, queue.Queue] = {}
        self._lock = threading.Lock()

    def subscribe(self, client_id: str) -> queue.Queue:
        """Register a new client and return its event queue."""
        q: queue.Queue = queue.Queue(maxsize=16)
        with self._lock:
            self._clients[client_id] = q
        return q

    def unsubscribe(self, client_id: str) -> None:
        """Remove a client from the broadcaster."""
        with self._lock:
            self._clients.pop(client_id, None)

    def broadcast(self, event: dict) -> None:
        """Push an event to all connected clients (non-blocking)."""
        with self._lock:
            clients = list(self._clients.items())
        for client_id, q in clients:
            try:
                q.put_nowait(event)
            except queue.Full:
                # Drop event if client queue is full (slow consumer)
                pass

    @property
    def client_count(self) -> int:
        """Return the number of connected clients."""
        with self._lock:
            return len(self._clients)


class _WebSocketServer:
    """A simple WebSocket server that broadcasts market data to all connected clients.

    Uses the WebSocket protocol (RFC 6455) to handle connections and frame
    processing via Python's socket module.
    """

    GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

    def __init__(self, port: int) -> None:
        self.port = port
        self._socket: socket.socket | None = None
        self._thread: threading.Thread | None = None
        self._running = False
        self._clients: set[socket.socket] = set()
        self._clients_lock = threading.Lock()

    def start(self) -> None:
        """Start the WebSocket server in a background thread."""
        if self._running:
            return
        self._running = True
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._socket.bind(("", self.port))
        self._socket.listen(5)
        self._socket.settimeout(1.0)
        self._thread = threading.Thread(target=self._run, name="websocket-server", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop the WebSocket server and close all client connections."""
        self._running = False
        if self._socket:
            try:
                self._socket.close()
            except Exception:
                pass
        with self._clients_lock:
            for client in list(self._clients):
                try:
                    client.close()
                except Exception:
                    pass
            self._clients.clear()
        if self._thread:
            self._thread.join(timeout=2.0)

    def _run(self) -> None:
        """Accept WebSocket connections and handle them in separate threads."""
        while self._running:
            try:
                client_sock, _ = self._socket.accept()
            except socket.timeout:
                continue
            except Exception:
                break
            threading.Thread(target=self._handle_client, args=(client_sock,), daemon=True).start()

    def _handle_client(self, client: socket.socket) -> None:
        """Handle a single WebSocket client connection."""
        try:
            if self._do_handshake(client):
                with self._clients_lock:
                    self._clients.add(client)
                self._recv_loop(client)
        except Exception:
            pass
        finally:
            with self._clients_lock:
                self._clients.discard(client)
            try:
                client.close()
            except Exception:
                pass

    def _do_handshake(self, client: socket.socket) -> bool:
        """Perform the WebSocket handshake (HTTP Upgrade)."""
        try:
            data = b""
            while b"\r\n\r\n" not in data:
                chunk = client.recv(4096)
                if not chunk:
                    return False
                data += chunk
            lines = data.decode("utf-8", errors="ignore").split("\r\n")
            headers: dict[str, str] = {}
            for line in lines[1:]:
                if ": " in line:
                    key, val = line.split(": ", 1)
                    headers[key.lower()] = val.strip()
            if headers.get("upgrade") != "websocket" or "websocket-key" not in headers:
                return False
            key = headers["websocket-key"]
            accept = base64.b64encode(hashlib.sha1((key + self.GUID).encode()).digest()).decode()
            response = (
                "HTTP/1.1 101 Switching Protocols\r\n"
                "Upgrade: websocket\r\n"
                "Connection: Upgrade\r\n"
                f"Sec-WebSocket-Accept: {accept}\r\n"
                "\r\n"
            )
            client.sendall(response.encode())
            return True
        except Exception:
            return False

    def _recv_loop(self, client: socket.socket) -> None:
        """Receive and process WebSocket frames from a client."""
        while self._running:
            try:
                frame = self._recv_frame(client)
                if frame is None:
                    break
                opcode, payload = frame
                if opcode == 0x8:
                    self._send_frame(client, 0x8, b"")
                    break
            except Exception:
                break

    def _recv_frame(self, client: socket.socket) -> tuple[int, bytes] | None:
        """Receive and decode a WebSocket frame."""
        try:
            header = client.recv(2)
            if len(header) < 2:
                return None
            first_byte, second_byte = header
            opcode = first_byte & 0x0F
            length = second_byte & 0x7F
            if length == 126:
                ext = client.recv(2)
                length = struct.unpack(">H", ext)[0]
            elif length == 127:
                ext = client.recv(8)
                length = struct.unpack(">Q", ext)[0]
            payload = b""
            while len(payload) < length:
                chunk = client.recv(length - len(payload))
                if not chunk:
                    return None
                payload += chunk
            return opcode, payload
        except Exception:
            return None

    def _send_frame(self, client: socket.socket, opcode: int, payload: bytes) -> None:
        """Encode and send a WebSocket frame to a client."""
        try:
            first_byte = 0x80 | opcode
            if len(payload) < 126:
                header = bytes([first_byte, len(payload)])
            elif len(payload) < 65536:
                header = bytes([first_byte, 126]) + struct.pack(">H", len(payload))
            else:
                header = bytes([first_byte, 127]) + struct.pack(">Q", len(payload))
            client.sendall(header + payload)
        except Exception:
            pass

    def broadcast(self, message: str) -> None:
        """Broadcast a text message to all connected WebSocket clients."""
        if not message:
            return
        payload = message.encode("utf-8")
        dead = set()
        with self._clients_lock:
            clients = list(self._clients)
        for client in clients:
            try:
                self._send_frame(client, 0x1, payload)
            except Exception:
                dead.add(client)
        if dead:
            with self._clients_lock:
                self._clients -= dead

    @property
    def client_count(self) -> int:
        """Return the number of connected WebSocket clients."""
        with self._clients_lock:
            return len(self._clients)


# Global broadcaster instance shared by the app
_ws_broadcaster: _WebSocketServer | None = None


def get_market_broadcaster() -> _WebSocketServer | None:
    """Return the global WebSocket market broadcaster instance."""
    return _ws_broadcaster


class _SSEResponse:
    """Wrapper to make a generator behave as a WSGI iterable with proper cleanup."""

    def __init__(self, gen) -> None:
        self._gen = gen
        self._started = False

    def __iter__(self):
        return self

    def __next__(self):
        if not self._started:
            self._started = True
        return next(self._gen)

    def close(self):
        """Called by WSGI server when response is complete."""
        if hasattr(self._gen, "close"):
            self._gen.close()


class StockScreenerWebApp:
    """Serve a small web UI and JSON endpoints for stock screening workflows."""

    def __init__(self, platform) -> None:
        self.platform = platform
        self.static_dir = Path(__file__).resolve().parent / "static"

    def __call__(self, environ, start_response):
        """Route WSGI requests to static assets or JSON stock screener endpoints."""

        path = environ.get("PATH_INFO", "/")
        method = environ.get("REQUEST_METHOD", "GET").upper()
        if path == "/" and method == "GET":
            return self._static(start_response, "index.html", "text/html; charset=utf-8")
        if path == "/learn" and method == "GET":
            return self._static(start_response, "index.html", "text/html; charset=utf-8")
        if path == "/static/styles.css" and method == "GET":
            return self._static(start_response, "styles.css", "text/css; charset=utf-8")
        if path == "/static/app.js" and method == "GET":
            return self._static(start_response, "app.js", "application/javascript; charset=utf-8")
        if path == "/manifest.json" and method == "GET":
            return self._static(start_response, "manifest.json", "application/manifest+json; charset=utf-8")
        if path == "/service-worker.js" and method == "GET":
            return self._static(start_response, "service-worker.js", "application/javascript; charset=utf-8")
        if path == "/api/auth/current" and method == "GET":
            session = self._session(environ)
            if session is None:
                return self._json(start_response, {"code": "OK", "data": {"authenticated": False, "user": None}})
            return self._json(
                start_response,
                {
                    "code": "OK",
                    "data": {
                        "authenticated": True,
                        "user": {
                            "username": session["username"],
                            "display_name": session.get("display_name") or session["username"],
                            "roles": session.get("roles") or [],
                        },
                    },
                },
            )
        if path == "/api/auth/register" and method == "POST":
            payload = self._read_json(environ)
            username = str(payload.get("username") or "").strip()
            password = str(payload.get("password") or "")
            if not username or not password:
                return self._json(
                    start_response,
                    {"code": "BAD_REQUEST", "error": {"message": "username and password are required."}},
                    status="400 Bad Request",
                )
            return self._json(
                start_response,
                self.platform.api.register_web_user(username, password, display_name=payload.get("display_name")),
            )
        if path == "/api/auth/login" and method == "POST":
            payload = self._read_json(environ)
            username = str(payload.get("username") or "").strip()
            password = str(payload.get("password") or "")
            if not username or not password:
                return self._json(
                    start_response,
                    {"code": "BAD_REQUEST", "error": {"message": "username and password are required."}},
                    status="400 Bad Request",
                )
            return self._json(start_response, self.platform.api.login(username, password))
        if path == "/api/auth/logout" and method == "POST":
            token = self._auth_token(environ)
            if not token:
                return self._json(
                    start_response,
                    {"code": "BAD_REQUEST", "error": {"message": "auth token is required."}},
                    status="400 Bad Request",
                )
            return self._json(start_response, self.platform.api.logout(token))
        if path == "/api/learning/hub" and method == "GET":
            actor = self._web_actor(environ)
            if actor is None:
                return self._json(
                    start_response,
                    {"code": "BAD_REQUEST", "error": {"message": "client_id is required."}},
                    status="400 Bad Request",
                )
            return self._json(
                start_response,
                self.platform.api.get_learning_hub(
                    actor["principal_id"],
                    principal_type=actor["principal_type"],
                    client_id=actor["client_id"],
                    username=actor["username"],
                ),
            )
        if path == "/api/learning/quiz" and method == "POST":
            payload = self._read_json(environ)
            actor = self._web_actor(environ, payload=payload)
            if actor is None:
                return self._json(
                    start_response,
                    {"code": "BAD_REQUEST", "error": {"message": "client_id is required."}},
                    status="400 Bad Request",
                )
            current_lesson_id = ((payload.get("learning") or {}) or {}).get("selected_lesson_id")
            return self._json(
                start_response,
                self.platform.api.submit_learning_quiz(
                    payload.get("answers"),
                    principal_id=actor["principal_id"],
                    principal_type=actor["principal_type"],
                    client_id=actor["client_id"],
                    username=actor["username"],
                    current_lesson_id=current_lesson_id,
                ),
            )
        if path == "/api/stock/options" and method == "GET":
            return self._json(start_response, self.platform.api.stock_filter_options())
        if path == "/api/stocks/count" and method == "GET":
            filters = self._parse_filters(environ)
            return self._json(start_response, self.platform.api.count_stocks(**filters))
        # SW-14: AI Smart Screener
        if path == "/api/screener/ai" and method == "POST":
            payload = self._read_json(environ)
            query = payload.get("query", "")
            return self._json(start_response, self.platform.api.smart_screen_from_query(query))
        if path == "/api/screener/results" and method == "GET":
            query = parse_qs(environ.get("QUERY_STRING", ""))
            screener_id = query.get("screener_id", [""])[0]
            return self._json(start_response, self.platform.api.smart_screen_results(screener_id))
        if path == "/api/screener/factors" and method == "GET":
            return self._json(start_response, self.platform.api.smart_screen_factors())
        if path == "/api/stocks/universe" and method == "GET":
            query = parse_qs(environ.get("QUERY_STRING", ""))
            featured_limit = int(query.get("featured_limit", ["24"])[0])
            return self._json(start_response, self.platform.api.stock_universe_summary(featured_limit=featured_limit))
        if path == "/api/crypto/universe" and method == "GET":
            query = parse_qs(environ.get("QUERY_STRING", ""))
            featured_limit = int(query.get("featured_limit", ["8"])[0])
            return self._json(start_response, self.platform.api.crypto_universe_summary(featured_limit=featured_limit))
        if path == "/api/crypto/assets" and method == "GET":
            return self._json(start_response, self.platform.api.list_crypto_assets())
        if path == "/api/crypto/detail" and method == "GET":
            query = parse_qs(environ.get("QUERY_STRING", ""))
            instrument_id = query.get("instrument_id", [""])[0]
            if not instrument_id:
                return self._json(
                    start_response,
                    {"code": "BAD_REQUEST", "error": {"message": "instrument_id is required."}},
                    status="400 Bad Request",
                )
            return self._json(start_response, self.platform.api.get_crypto_detail(instrument_id))
        if path == "/api/crypto/klines" and method == "GET":
            query = parse_qs(environ.get("QUERY_STRING", ""))
            instrument_id = query.get("instrument_id", [""])[0]
            if not instrument_id:
                return self._json(
                    start_response,
                    {"code": "BAD_REQUEST", "error": {"message": "instrument_id is required."}},
                    status="400 Bad Request",
                )
            limit = int(query.get("limit", ["120"])[0])
            interval = query.get("interval", ["1d"])[0]
            return self._json(start_response, self.platform.api.get_crypto_history(instrument_id, interval=interval, limit=limit))
        if path == "/api/stocks" and method == "GET":
            filters = self._parse_filters(environ)
            return self._json(start_response, self.platform.api.list_stocks(**filters))
        if path == "/api/stocks/detail" and method == "GET":
            query = parse_qs(environ.get("QUERY_STRING", ""))
            instrument_id = query.get("instrument_id", [""])[0]
            if not instrument_id:
                return self._json(
                    start_response,
                    {"code": "BAD_REQUEST", "error": {"message": "instrument_id is required."}},
                    status="400 Bad Request",
                )
            return self._json(start_response, self.platform.api.get_stock_detail(instrument_id))
        if path == "/api/stocks/financials" and method == "GET":
            query = parse_qs(environ.get("QUERY_STRING", ""))
            instrument_id = query.get("instrument_id", [""])[0]
            if not instrument_id:
                return self._json(
                    start_response,
                    {"code": "BAD_REQUEST", "error": {"message": "instrument_id is required."}},
                    status="400 Bad Request",
                )
            return self._json(start_response, self.platform.api.analyze_stock_financials(instrument_id))
        if path == "/api/stocks/financial-history" and method == "GET":
            query = parse_qs(environ.get("QUERY_STRING", ""))
            instrument_id = query.get("instrument_id", [""])[0]
            if not instrument_id:
                return self._json(
                    start_response,
                    {"code": "BAD_REQUEST", "error": {"message": "instrument_id is required."}},
                    status="400 Bad Request",
                )
            limit = int(query.get("limit", ["8"])[0])
            return self._json(start_response, self.platform.api.get_stock_financial_history(instrument_id, limit=limit))
        if path == "/api/stocks/klines" and method == "GET":
            query = parse_qs(environ.get("QUERY_STRING", ""))
            instrument_id = query.get("instrument_id", [""])[0]
            if not instrument_id:
                return self._json(
                    start_response,
                    {"code": "BAD_REQUEST", "error": {"message": "instrument_id is required."}},
                    status="400 Bad Request",
                )
            limit = int(query.get("limit", ["120"])[0])
            return self._json(start_response, self.platform.api.get_stock_history(instrument_id, limit=limit))
        if path == "/api/stocks/minutes" and method == "GET":
            query = parse_qs(environ.get("QUERY_STRING", ""))
            instrument_id = query.get("instrument_id", [""])[0]
            if not instrument_id:
                return self._json(
                    start_response,
                    {"code": "BAD_REQUEST", "error": {"message": "instrument_id is required."}},
                    status="400 Bad Request",
                )
            limit = int(query.get("limit", ["240"])[0])
            return self._json(start_response, self.platform.api.get_stock_minute_bars(instrument_id, limit=limit))
        if path == "/api/stocks/compare" and method == "GET":
            query = parse_qs(environ.get("QUERY_STRING", ""))
            left = query.get("left", [""])[0]
            right = query.get("right", [""])[0]
            if not left or not right:
                return self._json(
                    start_response,
                    {"code": "BAD_REQUEST", "error": {"message": "left and right are required."}},
                    status="400 Bad Request",
                )
            return self._json(start_response, self.platform.api.compare_stocks(left, right))
        if path == "/api/market/realtime" and method == "GET":
            query = parse_qs(environ.get("QUERY_STRING", ""))
            raw_ids = query.get("instrument_ids", [""])[0]
            instrument_ids = [item for item in raw_ids.split(",") if item] if raw_ids else None
            return self._json(start_response, self.platform.api.get_realtime_market_snapshot(instrument_ids))
        if path == "/api/paper/account" and method == "GET":
            query = parse_qs(environ.get("QUERY_STRING", ""))
            actor = self._web_actor(environ, allow_anonymous=True)
            account_code = query.get("account_code", [""])[0] or self._paper_account_code(actor)
            instrument_id = query.get("instrument_id", [""])[0] or None
            return self._json(start_response, self.platform.api.get_paper_trading_dashboard(account_code, instrument_id))
        if path == "/api/paper/orders" and method == "POST":
            payload = self._read_json(environ)
            actor = self._web_actor(environ, payload=payload, allow_anonymous=True)
            instrument_id = payload.get("instrument_id", "")
            side = payload.get("side", "")
            quantity = payload.get("quantity", 0)
            if not instrument_id or not side or not quantity:
                return self._json(
                    start_response,
                    {"code": "BAD_REQUEST", "error": {"message": "instrument_id, side and quantity are required."}},
                    status="400 Bad Request",
                )
            result = self.platform.api.submit_paper_order(
                instrument_id=instrument_id,
                side=side,
                quantity=float(quantity),
                account_code=payload.get("account_code") or self._paper_account_code(actor),
                order_type=payload.get("order_type", "market"),
                limit_price=payload.get("limit_price"),
            )
            if result.get("code") == "OK":
                self._broadcast_sse({"type": "paper_order_submitted"})
            return self._json(start_response, result)
        if path == "/api/paper/orders/cancel" and method == "POST":
            payload = self._read_json(environ)
            actor = self._web_actor(environ, payload=payload, allow_anonymous=True)
            order_id = payload.get("order_id", "")
            if not order_id:
                return self._json(
                    start_response,
                    {"code": "BAD_REQUEST", "error": {"message": "order_id is required."}},
                    status="400 Bad Request",
                )
            return self._json(
                start_response,
                self.platform.api.cancel_paper_order(
                    order_id,
                    account_code=payload.get("account_code") or self._paper_account_code(actor),
                ),
            )
        if path == "/api/paper/reset" and method == "POST":
            payload = self._read_json(environ)
            actor = self._web_actor(environ, payload=payload, allow_anonymous=True)
            return self._json(
                start_response,
                self.platform.api.reset_paper_account(payload.get("account_code") or self._paper_account_code(actor)),
            )
        if path == "/api/history-downloads" and method == "GET":
            return self._json(start_response, self.platform.api.list_history_download_overview())
        if path == "/api/strategy/templates" and method == "GET":
            return self._json(start_response, self.platform.api.list_strategy_templates())
        if path == "/api/bots" and method == "GET":
            return self._json(start_response, self.platform.api.list_strategy_bots())
        if path == "/api/bots" and method == "POST":
            payload = self._read_json(environ)
            template_code = payload.get("template_code", "")
            instrument_id = payload.get("instrument_id", "")
            if not template_code or not instrument_id:
                return self._json(
                    start_response,
                    {"code": "BAD_REQUEST", "error": {"message": "template_code and instrument_id are required."}},
                    status="400 Bad Request",
                )
            result = self.platform.api.create_strategy_bot(
                template_code=template_code,
                instrument_id=instrument_id,
                bot_name=payload.get("bot_name"),
                mode=payload.get("mode", "paper"),
                params=payload.get("params"),
            )
            if result.get("code") == "OK":
                self._broadcast_sse({"type": "bot_list_changed"})
            return self._json(start_response, result)
        if path == "/api/bots/start" and method == "POST":
            payload = self._read_json(environ)
            bot_id = payload.get("bot_id", "")
            if not bot_id:
                return self._json(
                    start_response,
                    {"code": "BAD_REQUEST", "error": {"message": "bot_id is required."}},
                    status="400 Bad Request",
                )
            result = self.platform.api.start_strategy_bot(bot_id)
            # BOT-03: Broadcast full bot state with PnL data
            if result.get("code") == "OK":
                bot_data = result.get("data", {})
                self._broadcast_sse({
                    "type": "bot_state_changed",
                    "bot_id": bot_id,
                    "action": "started",
                    "bot": bot_data,
                })
            return self._json(start_response, result)
        if path == "/api/bots/pause" and method == "POST":
            payload = self._read_json(environ)
            bot_id = payload.get("bot_id", "")
            if not bot_id:
                return self._json(
                    start_response,
                    {"code": "BAD_REQUEST", "error": {"message": "bot_id is required."}},
                    status="400 Bad Request",
                )
            result = self.platform.api.pause_strategy_bot(bot_id)
            if result.get("code") == "OK":
                bot_data = result.get("data", {})
                self._broadcast_sse({
                    "type": "bot_state_changed",
                    "bot_id": bot_id,
                    "action": "paused",
                    "bot": bot_data,
                })
            return self._json(start_response, result)
        if path == "/api/bots/stop" and method == "POST":
            payload = self._read_json(environ)
            bot_id = payload.get("bot_id", "")
            if not bot_id:
                return self._json(
                    start_response,
                    {"code": "BAD_REQUEST", "error": {"message": "bot_id is required."}},
                    status="400 Bad Request",
                )
            result = self.platform.api.stop_strategy_bot(bot_id)
            if result.get("code") == "OK":
                bot_data = result.get("data", {})
                self._broadcast_sse({
                    "type": "bot_state_changed",
                    "bot_id": bot_id,
                    "action": "stopped",
                    "bot": bot_data,
                })
            return self._json(start_response, result)
        if path == "/api/bots/interact" and method == "POST":
            payload = self._read_json(environ)
            bot_id = payload.get("bot_id", "")
            command = payload.get("command", "")
            if not bot_id or not command:
                return self._json(
                    start_response,
                    {"code": "BAD_REQUEST", "error": {"message": "bot_id and command are required."}},
                    status="400 Bad Request",
                )
            return self._json(
                start_response,
                self.platform.api.interact_strategy_bot(bot_id, command, payload.get("payload") or {}),
            )
        # BOT-02: Dedicated parameter update endpoint
        if path == "/api/bots/params" and method == "POST":
            payload = self._read_json(environ)
            bot_id = payload.get("bot_id", "")
            params = payload.get("params", {})
            if not bot_id:
                return self._json(
                    start_response,
                    {"code": "BAD_REQUEST", "error": {"message": "bot_id is required."}},
                    status="400 Bad Request",
                )
            result = self.platform.api.update_strategy_bot_params(bot_id, params)
            self._broadcast_sse({"type": "bot_params_updated", "bot_id": bot_id})
            return self._json(start_response, result)
        # BOT-03: Bot detail endpoint with PnL
        if path.startswith("/api/bots/") and path.endswith("/detail") and method == "GET":
            bot_id = path[len("/api/bots/"):-len("/detail")]
            result = self.platform.api.get_strategy_bot_details(bot_id)
            return self._json(start_response, result)
        # FT-08: Futures-spot basis arbitrage endpoints
        if path == "/api/futures/basis" and method == "GET":
            query = parse_qs(environ.get("QUERY_STRING", ""))
            contract_id = query.get("contract_id", [None])[0]
            if not contract_id:
                return self._json(start_response, {"code": "BAD_REQUEST", "error": {"message": "contract_id is required."}}, status="400 Bad Request")
            return self._json(start_response, self.platform.api.get_basis_data(contract_id))
        if path == "/api/futures/basis/signal" and method == "GET":
            query = parse_qs(environ.get("QUERY_STRING", ""))
            contract_id = query.get("contract_id", [None])[0]
            if not contract_id:
                return self._json(start_response, {"code": "BAD_REQUEST", "error": {"message": "contract_id is required."}}, status="400 Bad Request")
            return self._json(start_response, self.platform.api.get_basis_trading_signal(contract_id))
        # BOT-06: Composite bot endpoints
        if path == "/api/bots/composite" and method == "POST":
            payload = self._read_json(environ)
            result = self.platform.api.create_composite_bot(
                instrument_id=payload.get("instrument_id"),
                bot_name=payload.get("bot_name"),
                mode=payload.get("mode", "paper"),
                sub_bot_configs=payload.get("sub_bot_configs"),
                auto_rebalance=payload.get("auto_rebalance", False),
                rebalance_threshold=float(payload.get("rebalance_threshold", 0.15)),
            )
            return self._json(start_response, result)
        if path == "/api/bots/composite" and method == "GET":
            return self._json(start_response, self.platform.api.list_composite_bots())
        if path == "/api/bots/composite/rebalance" and method == "POST":
            payload = self._read_json(environ)
            result = self.platform.api.rebalance_composite_bot(
                composite_id=payload.get("composite_id", ""),
                new_weights=payload.get("new_weights"),
                mode=payload.get("mode", "signal_proportional"),
            )
            return self._json(start_response, result)
        if path == "/api/bots/composite/start" and method == "POST":
            payload = self._read_json(environ)
            return self._json(start_response, self.platform.api.start_composite_bot(payload.get("composite_id", "")))
        if path == "/api/bots/composite/stop" and method == "POST":
            payload = self._read_json(environ)
            return self._json(start_response, self.platform.api.stop_composite_bot(payload.get("composite_id", "")))
        if path.startswith("/api/bots/composite/") and path.endswith("/metrics") and method == "GET":
            composite_id = path[len("/api/bots/composite/"):-len("/metrics")]
            return self._json(start_response, self.platform.api.get_composite_metrics(composite_id))
        if path == "/api/notifications" and method == "GET":
            query = parse_qs(environ.get("QUERY_STRING", ""))
            limit = int(query.get("limit", ["20"])[0])
            return self._json(start_response, self.platform.api.list_strategy_notifications(limit=limit))
        if path == "/api/history-downloads/start" and method == "POST":
            payload = self._read_json(environ)
            job_id = payload.get("job_id", "")
            if not job_id:
                return self._json(
                    start_response,
                    {"code": "BAD_REQUEST", "error": {"message": "job_id is required."}},
                    status="400 Bad Request",
                )
            return self._json(start_response, self.platform.api.start_default_history_download_job(job_id))
        if path == "/api/history-downloads/pause" and method == "POST":
            payload = self._read_json(environ)
            job_id = payload.get("job_id", "")
            if not job_id:
                return self._json(
                    start_response,
                    {"code": "BAD_REQUEST", "error": {"message": "job_id is required."}},
                    status="400 Bad Request",
                )
            return self._json(start_response, self.platform.api.pause_default_history_download_job(job_id))
        if path == "/api/history-downloads/stop" and method == "POST":
            payload = self._read_json(environ)
            job_id = payload.get("job_id", "")
            if not job_id:
                return self._json(
                    start_response,
                    {"code": "BAD_REQUEST", "error": {"message": "job_id is required."}},
                    status="400 Bad Request",
                )
            return self._json(start_response, self.platform.api.stop_default_history_download_job(job_id))
        if path == "/api/web/state" and method == "GET":
            actor = self._web_actor(environ)
            if actor is None:
                return self._json(
                    start_response,
                    {"code": "BAD_REQUEST", "error": {"message": "client_id is required."}},
                    status="400 Bad Request",
                )
            return self._json(
                start_response,
                self.platform.api.get_web_workspace_state(
                    actor["principal_id"],
                    principal_type=actor["principal_type"],
                    client_id=actor["client_id"],
                    username=actor["username"],
                ),
            )
        if path == "/api/web/state" and method == "POST":
            payload = self._read_json(environ)
            actor = self._web_actor(environ, payload=payload)
            if actor is None:
                return self._json(
                    start_response,
                    {"code": "BAD_REQUEST", "error": {"message": "client_id is required."}},
                    status="400 Bad Request",
                )
            return self._json(
                start_response,
                self.platform.api.save_web_workspace_state(
                    actor["principal_id"],
                    payload.get("state") or {},
                    principal_type=actor["principal_type"],
                    client_id=actor["client_id"],
                    username=actor["username"],
                ),
            )
        if path == "/api/web/events" and method == "GET":
            actor = self._web_actor(environ)
            if actor is None:
                return self._json(
                    start_response,
                    {"code": "BAD_REQUEST", "error": {"message": "client_id is required."}},
                    status="400 Bad Request",
                )
            query = parse_qs(environ.get("QUERY_STRING", ""))
            limit = int(query.get("limit", ["20"])[0])
            return self._json(
                start_response,
                self.platform.api.list_web_activity(
                    actor["principal_id"],
                    principal_type=actor["principal_type"],
                    client_id=actor["client_id"],
                    limit=limit,
                ),
            )
        if path == "/api/web/events" and method == "POST":
            payload = self._read_json(environ)
            actor = self._web_actor(environ, payload=payload)
            event_type = payload.get("event_type")
            if actor is None or not event_type:
                return self._json(
                    start_response,
                    {"code": "BAD_REQUEST", "error": {"message": "client_id and event_type are required."}},
                    status="400 Bad Request",
                )
            return self._json(
                start_response,
                self.platform.api.record_web_activity(
                    actor["principal_id"],
                    event_type,
                    payload=payload.get("payload") or {},
                    path=payload.get("path") or path,
                    principal_type=actor["principal_type"],
                    client_id=actor["client_id"],
                    username=actor["username"],
                ),
            )
        if path == "/api/intelligence/recent" and method == "GET":
            query = parse_qs(environ.get("QUERY_STRING", ""))
            limit = int(query.get("limit", ["20"])[0])
            return self._json(start_response, self.platform.api.intelligence_recent(limit=limit))
        if path == "/api/risk/dashboard" and method == "GET":
            return self._json(start_response, self.platform.api.risk_dashboard())
        if path == "/api/paper/quick-trade" and method == "POST":
            payload = self._read_json(environ)
            actor = self._web_actor(environ, payload=payload, allow_anonymous=True)
            symbol = payload.get("symbol", "")
            if not symbol:
                return self._json(
                    start_response,
                    {"code": "BAD_REQUEST", "error": {"message": "symbol is required."}},
                    status="400 Bad Request",
                )
            return self._json(
                start_response,
                self.platform.api.quick_paper_trade(
                    symbol=symbol,
                    side=payload.get("side", "buy"),
                    quantity=int(payload.get("quantity", 100)),
                    account_code=payload.get("account_code") or self._paper_account_code(actor),
                ),
            )
        if path == "/api/futures/universe" and method == "GET":
            return self._json(start_response, self.platform.api.futures_universe_summary())
        if path == "/api/futures/contracts" and method == "GET":
            return self._json(start_response, self.platform.api.list_futures_contracts())
        if path == "/api/futures/detail" and method == "GET":
            query = parse_qs(environ.get("QUERY_STRING", ""))
            instrument_id = query.get("instrument_id", [""])[0]
            return self._json(start_response, self.platform.api.get_futures_detail(instrument_id))
        if path == "/api/futures/klines" and method == "GET":
            query = parse_qs(environ.get("QUERY_STRING", ""))
            instrument_id = query.get("instrument_id", [""])[0]
            interval = query.get("interval", ["1d"])[0]
            limit = int(query.get("limit", ["120"])[0])
            return self._json(start_response, self.platform.api.get_futures_klines(instrument_id, interval=interval, limit=limit))
        # FT-08: Trading Calendar
        if path == "/api/futures/calendar" and method == "GET":
            return self._json(start_response, self.platform.api.get_futures_trading_calendar())
        if path == "/api/futures/sessions" and method == "GET":
            query = parse_qs(environ.get("QUERY_STRING", ""))
            instrument_id = query.get("instrument_id", [""])[0]
            return self._json(start_response, self.platform.api.get_futures_trading_sessions(instrument_id))
        # FT-09: Main Contract and Continuous Contract Mapping
        if path == "/api/futures/main-contract" and method == "GET":
            query = parse_qs(environ.get("QUERY_STRING", ""))
            product_code = query.get("product_code", [""])[0]
            return self._json(start_response, self.platform.api.get_main_contract(product_code))
        if path == "/api/futures/continuous-contract" and method == "GET":
            query = parse_qs(environ.get("QUERY_STRING", ""))
            product_code = query.get("product_code", [""])[0]
            return self._json(start_response, self.platform.api.get_continuous_contract(product_code))
        if path == "/api/futures/rollover" and method == "GET":
            query = parse_qs(environ.get("QUERY_STRING", ""))
            product_code = query.get("product_code", [""])[0]
            return self._json(start_response, self.platform.api.get_rollover_recommendation(product_code))
        # FT-10: Futures Simulated Trading
        if path == "/api/futures/dashboard" and method == "GET":
            query = parse_qs(environ.get("QUERY_STRING", ""))
            account_code = query.get("account_code", ["futures_main"])[0]
            return self._json(start_response, self.platform.api.get_futures_dashboard(account_code))
        if path == "/api/futures/positions" and method == "GET":
            query = parse_qs(environ.get("QUERY_STRING", ""))
            account_code = query.get("account_code", ["futures_main"])[0]
            return self._json(start_response, self.platform.api.get_futures_positions(account_code))
        if path == "/api/futures/orders" and method == "POST":
            payload = self._read_json(environ)
            return self._json(
                start_response,
                self.platform.api.submit_futures_order(
                    account_code=payload.get("account_code", "futures_main"),
                    instrument_id=payload.get("instrument_id", ""),
                    direction=payload.get("direction", "long"),
                    quantity=int(payload.get("quantity", 0)),
                    order_type=payload.get("order_type", "market"),
                    limit_price=payload.get("limit_price"),
                    contract_multiplier=float(payload.get("contract_multiplier", 1.0)),
                ),
            )
        if path == "/api/futures/mark-to-market" and method == "POST":
            payload = self._read_json(environ)
            return self._json(
                start_response,
                self.platform.api.mark_futures_to_market(
                    account_code=payload.get("account_code", "futures_main"),
                    instrument_id=payload.get("instrument_id", ""),
                    current_price=float(payload.get("current_price", 0)),
                ),
            )
        # FT-06: Futures Margin Risk and Liquidation Warnings
        if path == "/api/futures/margin-risk" and method == "GET":
            query = parse_qs(environ.get("QUERY_STRING", ""))
            account_code = query.get("account_code", ["futures_main"])[0]
            return self._json(start_response, self.platform.api.get_futures_margin_risk(account_code))
        if path == "/api/futures/liquidation-risk" and method == "GET":
            query = parse_qs(environ.get("QUERY_STRING", ""))
            account_code = query.get("account_code", ["futures_main"])[0]
            return self._json(start_response, self.platform.api.get_futures_liquidation_risk(account_code))
        # FT-11: Unified Portfolio View
        if path == "/api/unified-portfolio" and method == "GET":
            query = parse_qs(environ.get("QUERY_STRING", ""))
            stock_positions = None
            crypto_positions = None
            futures_positions = None
            return self._json(
                start_response,
                self.platform.api.get_unified_portfolio_summary(
                    stock_positions=stock_positions,
                    crypto_positions=crypto_positions,
                    futures_positions=futures_positions,
                ),
            )
        # PF-01~PF-06: Portfolio Allocation API
        if path == "/api/portfolio/allocator" and method == "POST":
            payload = self._read_json(environ)
            return self._json(
                start_response,
                self.platform.api.create_portfolio_allocator(
                    allocator_type=payload.get("allocator_type", "equal_weight"),
                    name=payload.get("name", "My Allocator"),
                    description=payload.get("description", ""),
                    max_weight=float(payload.get("max_weight", 0.3)),
                    allow_short=bool(payload.get("allow_short", False)),
                ),
            )
        if path == "/api/portfolio/allocation" and method == "POST":
            payload = self._read_json(environ)
            return self._json(
                start_response,
                self.platform.api.calculate_portfolio_allocation(
                    allocator_config_id=payload.get("allocator_config_id", ""),
                    expected_returns=payload.get("expected_returns", {}),
                    volatilities=payload.get("volatilities", {}),
                    correlations=payload.get("correlations", {}),
                ),
            )
        if path == "/api/portfolio/rebalance" and method == "POST":
            payload = self._read_json(environ)
            return self._json(
                start_response,
                self.platform.api.calculate_rebalance_plan(
                    target_weights=payload.get("target_weights", {}),
                    current_weights=payload.get("current_weights", {}),
                    current_prices=payload.get("current_prices", {}),
                    notional=float(payload.get("notional", 100000)),
                ),
            )
        if path == "/api/portfolio/exposure" and method == "GET":
            query = parse_qs(environ.get("QUERY_STRING", ""))
            # Parse positions from query params: positions=A:10,B:-5
            positions = {}
            for item in query.get("positions", [""])[0].split(","):
                if ":" in item:
                    k, v = item.split(":", 1)
                    positions[k] = float(v)
            prices = {}
            for item in query.get("prices", [""])[0].split(","):
                if ":" in item:
                    k, v = item.split(":", 1)
                    prices[k] = float(v)
            return self._json(
                start_response,
                self.platform.api.get_risk_exposure_summary(prices=prices, positions=positions),
            )
        if path == "/api/portfolio/attribution" and method == "POST":
            payload = self._read_json(environ)
            return self._json(
                start_response,
                self.platform.api.get_attribution_analysis(
                    portfolio_weights=payload.get("portfolio_weights", {}),
                    benchmark_weights=payload.get("benchmark_weights", {}),
                    portfolio_returns=payload.get("portfolio_returns", {}),
                    benchmark_returns=payload.get("benchmark_returns", {}),
                ),
            )
        # PF-06: Multi-Account API
        if path == "/api/account/create" and method == "POST":
            payload = self._read_json(environ)
            return self._json(
                start_response,
                self.platform.api.create_multi_account(
                    user_id=payload.get("user_id", "default"),
                    account_type=payload.get("account_type", "primary"),
                    initial_cash=float(payload.get("initial_cash", 0)),
                ),
            )
        if path == "/api/account/summary" and method == "GET":
            query = parse_qs(environ.get("QUERY_STRING", ""))
            account_id = query.get("account_id", [""])[0]
            return self._json(
                start_response,
                self.platform.api.get_multi_account_summary(account_id=account_id),
            )
        if path == "/api/account/transfer" and method == "POST":
            payload = self._read_json(environ)
            return self._json(
                start_response,
                self.platform.api.transfer_between_accounts(
                    from_account_id=payload.get("from_account_id", ""),
                    to_account_id=payload.get("to_account_id", ""),
                    amount=float(payload.get("amount", 0)),
                ),
            )
        # ── Market Data APIs ──────────────────────────────
        if path == "/api/orderbook" and method == "GET":
            query = parse_qs(environ.get("QUERY_STRING", ""))
            instrument_id = query.get("instrument_id", [""])[0]
            if not instrument_id:
                return self._json(start_response, {"code": "BAD_REQUEST", "error": {"message": "instrument_id is required."}}, status="400 Bad Request")
            return self._json(start_response, self.platform.api.get_orderbook(instrument_id))
        if path == "/api/trade-ticks" and method == "GET":
            query = parse_qs(environ.get("QUERY_STRING", ""))
            instrument_id = query.get("instrument_id", [""])[0]
            limit = int(query.get("limit", ["50"])[0])
            if not instrument_id:
                return self._json(start_response, {"code": "BAD_REQUEST", "error": {"message": "instrument_id is required."}}, status="400 Bad Request")
            return self._json(start_response, self.platform.api.get_trade_ticks(instrument_id, limit=limit))
        # ── Alert History APIs (MO-06) ────────────────────
        if path == "/api/alerts/history" and method == "GET":
            query = parse_qs(environ.get("QUERY_STRING", ""))
            window_hours = int(query.get("window_hours", ["24"])[0])
            severity = query.get("severity", [None])[0]
            return self._json(start_response, self.platform.api.get_alert_history(window_hours=window_hours, severity=severity))
        # ── Report APIs (RP-05/06) ────────────────────────
        if path == "/api/reports/daily" and method == "GET":
            query = parse_qs(environ.get("QUERY_STRING", ""))
            account_id = query.get("account_id", ["paper_stock_main"])[0]
            return self._json(start_response, self.platform.api.get_daily_report(account_id=account_id))
        if path == "/api/reports/weekly" and method == "GET":
            query = parse_qs(environ.get("QUERY_STRING", ""))
            account_id = query.get("account_id", ["paper_stock_main"])[0]
            return self._json(start_response, self.platform.api.get_weekly_report(account_id=account_id))
        if path == "/api/reports/monthly" and method == "GET":
            query = parse_qs(environ.get("QUERY_STRING", ""))
            account_id = query.get("account_id", ["paper_stock_main"])[0]
            return self._json(start_response, self.platform.api.get_monthly_report(account_id=account_id))
        # ── Watchlist Grouping APIs ────────────────────────
        if path == "/api/watchlist/groups" and method == "GET":
            actor = self._web_actor(environ, allow_anonymous=True)
            user_id = actor["username"] or actor["principal_id"] if actor else "anonymous"
            return self._json(start_response, self.platform.api.get_watchlist_groups(user_id))
        if path == "/api/watchlist/groups" and method == "POST":
            payload = self._read_json(environ)
            actor = self._web_actor(environ, payload=payload, allow_anonymous=True)
            user_id = actor["username"] or actor["principal_id"] if actor else "anonymous"
            return self._json(start_response, self.platform.api.create_watchlist_group(user_id, payload.get("group_name", "默认分组")))
        if path == "/api/watchlist/group/add" and method == "POST":
            payload = self._read_json(environ)
            actor = self._web_actor(environ, payload=payload, allow_anonymous=True)
            user_id = actor["username"] or actor["principal_id"] if actor else "anonymous"
            return self._json(start_response, self.platform.api.add_to_watchlist_group(
                user_id=user_id,
                group_name=payload.get("group_name", "默认分组"),
                instrument_id=payload.get("instrument_id", ""),
            ))
        # ── Futures Trading APIs (FT-05) ──────────────────
        if path == "/api/futures/order" and method == "POST":
            payload = self._read_json(environ)
            return self._json(start_response, self.platform.api.submit_futures_order(
                instrument_id=payload.get("instrument_id", ""),
                side=payload.get("side", "buy"),
                quantity=int(payload.get("quantity", 1)),
                order_type=payload.get("order_type", "market"),
                limit_price=payload.get("limit_price"),
            ))
        if path == "/api/futures/positions" and method == "GET":
            return self._json(start_response, self.platform.api.get_futures_positions())
        if path == "/api/futures/position-analytics" and method == "GET":
            account_code = params.get("account_code", ["futures_main"])[0]
            return self._json(start_response, {
                "code": "OK",
                "data": self.platform.futures_trading.get_position_analytics(account_code),
            })
        # ── Technical Indicator APIs (CHART-02) ──────────
        if path == "/api/indicators/calculate" and method == "POST":
            payload = self._read_json(environ)
            return self._json(start_response, self.platform.api.calculate_indicator(
                indicator=payload.get("indicator", ""),
                prices=payload.get("prices", []),
                **payload.get("params", {}),
            ))
        # ── Redis Cache Stats (Redis CACHE) ────────────────────────────────
        if path == "/api/cache/stats" and method == "GET":
            cache = getattr(self.platform, "cache", None)
            if cache is None:
                return self._json(start_response, {"code": "OK", "data": {"available": False, "backend": "none"}})
            fallback_stats = getattr(cache, "_fallback", None)
            fallback_data = {}
            if fallback_stats is not None:
                s = fallback_stats.stats()
                fallback_data = {"hits": s["hits"], "misses": s["misses"], "keys": s["keys"]}
            return self._json(start_response, {
                "code": "OK",
                "data": {
                    "available": cache.is_available(),
                    "backend": "redis" if cache.is_available() else "in_memory",
                    "fallback_stats": fallback_data,
                    "ping": cache.ping(),
                },
            })
        # ── ST-08 / DSL-01~DSL-05: QuantScript DSL ──────────────────────
        if path == "/api/dsl/compile" and method == "POST":
            payload = self._read_json(environ)
            return self._json(start_response, self.platform.api.compile_dsl_strategy(
                code=payload.get("code", ""),
                name=payload.get("name", ""),
            ))
        if path == "/api/dsl/evaluate" and method == "POST":
            payload = self._read_json(environ)
            return self._json(start_response, self.platform.api.evaluate_dsl_expression(
                expression=payload.get("expression", ""),
                data=payload.get("data"),
            ))
        if path == "/api/dsl/factor" and method == "POST":
            payload = self._read_json(environ)
            return self._json(start_response, self.platform.api.create_dsl_factor(
                name=payload.get("name", ""),
                expression=payload.get("expression", ""),
                description=payload.get("description", ""),
                author=payload.get("author", ""),
                tags=payload.get("tags"),
            ))
        if path == "/api/dsl/strategies" and method == "GET":
            return self._json(start_response, self.platform.api.list_dsl_strategies())
        if path.startswith("/api/dsl/strategy/") and method == "GET":
            strategy_id = path.split("/api/dsl/strategy/")[1]
            return self._json(start_response, self.platform.api.get_dsl_strategy(strategy_id))
        # ── SSE Real-time Event Stream (WebSocket replacement) ──────────
        if path == "/api/events/stream" and method == "GET":
            return self._sse_stream(start_response)
        if method not in {"GET", "POST"}:
            return self._json(start_response, {"code": "METHOD_NOT_ALLOWED"}, status="405 Method Not Allowed")
        return self._json(start_response, {"code": "NOT_FOUND"}, status="404 Not Found")

    def _sse_stream(self, start_response):
        """Serve a Server-Sent Events stream for real-time updates."""
        import json
        import threading

        # Get or create the broadcaster on the platform
        broadcaster = getattr(self.platform, "_sse_broadcaster", None)
        if broadcaster is None:
            broadcaster = SSEEventBroadcaster()
            self.platform._sse_broadcaster = broadcaster

        client_id = self._client_id(self._make_environ_for_sse())
        queue = broadcaster.subscribe(client_id)

        def generate():
            # Send initial connection event
            yield f"data: {json.dumps({'type': 'connected', 'client_id': client_id})}\n\n"
            try:
                while True:
                    # Wait for events with timeout (30s keepalive)
                    event = queue.get(timeout=30)
                    if event is None:
                        # Keepalive
                        yield f": keepalive\n\n"
                    else:
                        yield f"data: {json.dumps(event, default=str)}\n\n"
            except Exception:
                pass
            finally:
                broadcaster.unsubscribe(client_id)

        start_response("200 OK", [
            ("Content-Type", "text/event-stream; charset=utf-8"),
            ("Cache-Control", "no-cache"),
            ("Connection", "keep-alive"),
            ("X-Accel-Buffering", "no"),
        ])
        return _SSEResponse(generate())

    def _make_environ_for_sse(self):
        """Create a minimal environ dict for SSE subscription."""
        return {"PATH_INFO": "/", "QUERY_STRING": "", "HTTP_X_CLIENT_ID": ""}

    def _broadcast_sse(self, event: dict) -> None:
        """Push an event to all SSE clients if the broadcaster is available."""
        broadcaster = getattr(self.platform, "_sse_broadcaster", None)
        if broadcaster is not None:
            broadcaster.broadcast(event)

    def _broadcast_bot_state(self, bot_id: str, action: str) -> None:
        """Broadcast a bot state change event to all SSE clients."""
        self._broadcast_sse({"type": "bot_state_changed", "bot_id": bot_id, "action": action})

    def _parse_filters(self, environ) -> dict[str, str]:
        """Translate query-string values into stock screener filter keys."""

        raw = parse_qs(environ.get("QUERY_STRING", ""))
        return {key: values[0] for key, values in raw.items() if values and values[0] != ""}

    def _client_id(self, environ) -> str:
        """Resolve a browser client identifier from the HTTP request."""

        query = parse_qs(environ.get("QUERY_STRING", ""))
        return environ.get("HTTP_X_CLIENT_ID", "") or query.get("client_id", [""])[0]

    def _auth_token(self, environ) -> str:
        """Resolve a bearer token from the current request when one is present."""

        header = environ.get("HTTP_AUTHORIZATION", "")
        if header.lower().startswith("bearer "):
            return header.split(" ", 1)[1].strip()
        return environ.get("HTTP_X_AUTH_TOKEN", "")

    def _session(self, environ) -> dict | None:
        """Return the authenticated web session when a valid token is provided."""

        return self.platform.api.resolve_session(self._auth_token(environ))

    def _web_actor(self, environ, payload: dict | None = None, allow_anonymous: bool = False) -> dict | None:
        """Resolve the acting principal for a web request."""

        payload = payload or {}
        client_id = payload.get("client_id") or self._client_id(environ)
        session = self._session(environ)
        if session is not None:
            return {
                "principal_type": "user",
                "principal_id": session["username"],
                "username": session["username"],
                "client_id": client_id or session["username"],
            }
        if client_id:
            return {
                "principal_type": "client",
                "principal_id": client_id,
                "username": None,
                "client_id": client_id,
            }
        if allow_anonymous:
            return {
                "principal_type": "client",
                "principal_id": "anonymous",
                "username": None,
                "client_id": "anonymous",
            }
        return None

    def _paper_account_code(self, actor: dict | None) -> str:
        """Build a stable paper-account code for the current web principal."""

        if actor is None:
            return "paper_stock_main"
        principal = str(actor["principal_id"]).replace(":", "_").replace("-", "_")
        return f"paper_{actor['principal_type']}_{principal}"

    def _read_json(self, environ) -> dict:
        """Read a JSON request body from the WSGI environment."""

        try:
            length = int(environ.get("CONTENT_LENGTH", "0") or "0")
        except ValueError:
            length = 0
        raw = environ["wsgi.input"].read(length) if length > 0 else b"{}"
        if not raw:
            return {}
        return json.loads(raw.decode("utf-8"))

    def _static(self, start_response: Callable, filename: str, content_type: str):
        """Serve a static asset from the local webapp directory."""

        body = (self.static_dir / filename).read_bytes()
        start_response("200 OK", [("Content-Type", content_type), ("Content-Length", str(len(body)))])
        return [body]

    def _json(self, start_response: Callable, payload: dict, status: str = "200 OK"):
        """Return a JSON response with UTF-8 encoding."""

        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        start_response(status, [("Content-Type", "application/json; charset=utf-8"), ("Content-Length", str(len(body)))])
        return [body]


def run_dev_server(platform, host: str = "127.0.0.1", port: int = 8080) -> None:
    """Run the stock screener workbench with the Python stdlib server."""

    global _ws_broadcaster
    ws_port = 8081
    _ws_broadcaster = _WebSocketServer(ws_port)
    _ws_broadcaster.start()
    print(f"WebSocket market server started on ws://{host}:{ws_port}/ws/market")

    # Inject broadcaster into RealtimeMarketService post-refresh callback
    realtime_svc = getattr(platform, "realtime_market", None)
    if realtime_svc is not None:
        _inject_market_broadcast(realtime_svc, _ws_broadcaster)

    app = StockScreenerWebApp(platform)
    with make_server(host, port, app) as server:
        print(f"Serving Stock Screener on http://{host}:{port}")
        try:
            server.serve_forever()
        finally:
            if _ws_broadcaster:
                _ws_broadcaster.stop()


def _inject_market_broadcast(realtime_svc, broadcaster: _WebSocketServer) -> None:
    """Inject WebSocket broadcast into RealtimeMarketService refresh cycle.

    After each refresh_once(), the latest market snapshot is sent to all
    WebSocket clients for sub-second real-time updates.
    """
    _original_run = realtime_svc._run

    def _patched_run() -> None:
        """Patched _run that broadcasts after each refresh."""
        while not realtime_svc._stop_event.wait(realtime_svc.update_interval_seconds):
            realtime_svc.refresh_once()
            try:
                snapshot = realtime_svc.snapshot()
                payload = json.dumps({"type": "market_snapshot", "data": snapshot}, default=str)
                broadcaster.broadcast(payload)
            except Exception:
                pass

    realtime_svc._run = _patched_run
    # Restart the thread if it was already running
    if realtime_svc._thread is not None and realtime_svc._thread.is_alive():
        realtime_svc.stop()
        realtime_svc.start()
