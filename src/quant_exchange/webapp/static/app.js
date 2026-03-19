const PRESETS = {
  quality_cn: {
    label: "A股质量",
    description: "高 ROE + 稳健现金流",
    filters: {
      market_region: "CN",
      min_roe: "18",
      min_net_margin: "10",
      min_operating_cashflow_growth: "8",
      max_debt_to_asset: "65",
    },
  },
  hk_dividend: {
    label: "港股股息",
    description: "高股息 + 低估值",
    filters: {
      market_region: "HK",
      min_dividend_yield: "2",
      max_pe_ttm: "22",
      max_debt_to_asset: "70",
    },
  },
  us_growth: {
    label: "美股成长",
    description: "成长与盈利并重",
    filters: {
      market_region: "US",
      min_revenue_growth: "8",
      min_net_profit_growth: "10",
      min_roe: "15",
    },
  },
  global_value: {
    label: "全球价值",
    description: "低 PE + 正现金流",
    filters: {
      max_pe_ttm: "20",
      min_free_cashflow_margin: "3",
      max_debt_to_asset: "60",
    },
  },
};

const CHART_RANGES = [30, 60, 120, 240];
const CHART_MODES = [
  { value: "candles", label: "K线" },
  { value: "line", label: "折线" },
];
const REALTIME_ACTIVE_POLL_MS = 4000;
const REALTIME_IDLE_POLL_MS = 30000;
const DOWNLOAD_POLL_MS = 5000;
const DEFAULT_TAB = "overview";
const DEFAULT_STOCK_LIST_LIMIT = 300;
const TAB_LABELS = {
  overview: "首页",
  learning: "学习",
  watchlist: "自选",
  screener: "选股",
  research: "个股",
  crypto: "加密",
  futures: "期货",
  compare: "对比",
  paper: "模拟",
  downloads: "数据",
  activity: "动态",
};

const state = {
  clientId: null,
  authToken: null,
  equityHistory: [],
  currentUser: null,
  sortBy: "symbol",
  sortDesc: false,
  compare: { left: null, right: null },
  activeInstrumentId: null,
  recentStockVisits: [],
  currentStocks: [],
  universe: [],
  universeSummary: null,
  stockMap: {},
  futuresContracts: [],
  futuresOverview: null,
  activeFuturesInstrumentId: null,
  futuresChartMode: "candles",
  futuresChartRange: 120,
  cryptoAssets: [],
  cryptoSummary: null,
  cryptoMap: {},
  activeCryptoInstrumentId: "BTCUSDT",
  watchlist: [],
  chartRange: 120,
  chartMode: "candles",
  cryptoChartRange: 120,
  cryptoChartMode: "candles",
  activePreset: null,
  activeTab: DEFAULT_TAB,
  marketSnapshot: null,
  downloadJobs: [],
  paperDashboard: null,
  learningHub: null,
  learningProgress: null,
  selectedLessonId: null,
  learningSearchQuery: "",
  lastLearningQuizResult: null,
  botTemplates: [],
  bots: [],
  notifications: [],
  realtimeTimer: null,
  downloadTimer: null,
  stockResultCount: 0,
};

function generateClientId() {
  if (window.crypto && typeof window.crypto.randomUUID === "function") {
    return window.crypto.randomUUID();
  }
  return `client-${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

function ensureClientId() {
  let clientId = window.localStorage.getItem("quant_exchange_client_id");
  if (!clientId) {
    clientId = generateClientId();
    window.localStorage.setItem("quant_exchange_client_id", clientId);
  }
  state.clientId = clientId;
}

function tabLabel(tab) {
  return TAB_LABELS[tab] || TAB_LABELS[DEFAULT_TAB];
}

function normalizeTab(tab) {
  return Object.prototype.hasOwnProperty.call(TAB_LABELS, tab) ? tab : DEFAULT_TAB;
}

async function fetchJson(path, options = {}) {
  const headers = new Headers(options.headers || {});
  if (state.clientId) {
    headers.set("X-Client-Id", state.clientId);
  }
  if (state.authToken) {
    headers.set("Authorization", `Bearer ${state.authToken}`);
  }
  const response = await fetch(path, { ...options, headers });
  if (!response.ok) {
    throw new Error(`Request failed: ${response.status}`);
  }
  return response.json();
}

async function postJson(path, payload) {
  return fetchJson(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ client_id: state.clientId, ...payload }),
  });
}

function buildQuery(filters) {
  const params = new URLSearchParams();
  Object.entries(filters).forEach(([key, value]) => {
    if (value !== "") {
      params.set(key, value);
    }
  });
  return params.toString();
}

function collectFilters() {
  const form = document.getElementById("filters-form");
  const formData = new FormData(form);
  const filters = {};
  for (const [key, value] of formData.entries()) {
    if (value !== "") {
      filters[key] = value;
    }
  }
  if (state.sortBy) {
    filters.sort_by = state.sortBy;
    filters.sort_desc = state.sortDesc ? "1" : "0";
  }
  return filters;
}

function updateSortIndicators() {
  document.querySelectorAll("thead th[data-sort-key]").forEach(function (th) {
    var key = th.getAttribute("data-sort-key");
    var arrow = th.querySelector(".sort-arrow");
    if (key === state.sortBy) {
      th.classList.add("sort-active");
      if (arrow) arrow.textContent = state.sortDesc ? "▼" : "▲";
    } else {
      th.classList.remove("sort-active");
      if (arrow) arrow.textContent = "▲";
    }
  });
}

function handleSortClick(sortKey) {
  if (state.sortBy === sortKey) {
    state.sortDesc = !state.sortDesc;
  } else {
    state.sortBy = sortKey;
    // Numeric fields default descending, text fields ascending
    var numericKeys = ["last_price", "change_pct", "amplitude", "volume", "turnover", "market_cap", "pe_ttm", "pb", "roe", "revenue_growth", "net_profit_growth", "gross_margin", "net_margin", "dividend_yield", "debt_to_asset"];
    state.sortDesc = numericKeys.indexOf(sortKey) !== -1;
  }
  updateSortIndicators();
  loadStocks({ persist: true, eventType: "sort_change" });
}

function applyFilters(filters) {
  document.querySelectorAll("#filters-form [name]").forEach((field) => {
    field.value = "";
  });
  Object.entries(filters || {}).forEach(([key, value]) => {
    const field = document.querySelector(`#filters-form [name="${key}"]`);
    if (field) {
      field.value = value;
    }
  });
}

function loadAuthToken() {
  state.authToken = window.localStorage.getItem("quant_exchange_auth_token");
}

function storeAuthToken(token) {
  state.authToken = token || null;
  if (token) {
    window.localStorage.setItem("quant_exchange_auth_token", token);
  } else {
    window.localStorage.removeItem("quant_exchange_auth_token");
  }
}

function snapshotWorkspaceState() {
  return {
    filters: collectFilters(),
    compare: { ...state.compare },
    active_instrument_id: state.activeInstrumentId,
    watchlist: [...state.watchlist],
    sort: { sort_by: state.sortBy, sort_desc: state.sortDesc },
    chart: { range: state.chartRange, mode: state.chartMode },
    crypto: {
      active_instrument_id: state.activeCryptoInstrumentId,
      chart: { range: state.cryptoChartRange, mode: state.cryptoChartMode },
    },
    preset: state.activePreset,
    active_tab: state.activeTab,
    learning: { selected_lesson_id: state.selectedLessonId, search_query: state.learningSearchQuery },
  };
}

function updateWorkspaceStatus(message) {
  document.getElementById("workspace-status").textContent = message;
}

function updateDownloadHubStatus(message) {
  document.getElementById("download-hub-status").textContent = message;
}

function renderAuthState() {
  const summary = document.getElementById("auth-summary");
  const username = document.getElementById("auth-username");
  const password = document.getElementById("auth-password");
  const displayName = document.getElementById("auth-display-name");
  const loginButton = document.getElementById("auth-login");
  const registerButton = document.getElementById("auth-register");
  const logoutButton = document.getElementById("auth-logout");
  if (!summary || !username || !password || !displayName || !loginButton || !registerButton || !logoutButton) {
    return;
  }
  const authCard = summary.closest(".auth-card");
  const authForm = document.getElementById("auth-form");
  if (state.currentUser) {
    summary.textContent = `当前用户：${state.currentUser.display_name || state.currentUser.username} (${state.currentUser.username})`;
    if (authCard) authCard.classList.add("authenticated");
    if (authForm) authForm.style.display = "none";
    logoutButton.disabled = false;
    logoutButton.style.display = "";
    loginButton.style.display = "none";
    registerButton.style.display = "none";
    password.value = "";
  } else {
    summary.textContent = "当前未登录，登录后会自动切换到该用户独立空间。";
    if (authCard) authCard.classList.remove("authenticated");
    if (authForm) authForm.style.display = "";
    username.disabled = false;
    password.disabled = false;
    displayName.disabled = false;
    loginButton.disabled = false;
    loginButton.style.display = "";
    registerButton.disabled = false;
    registerButton.style.display = "";
    logoutButton.disabled = true;
    logoutButton.style.display = "none";
  }
}

function formatTime(value) {
  if (!value) {
    return "-";
  }
  return new Date(value).toLocaleString("zh-CN", { hour12: false });
}

function formatNumber(value) {
  if (value === null || value === undefined || value === "" || Number.isNaN(Number(value))) {
    return "-";
  }
  return Number(value).toLocaleString("zh-CN", { maximumFractionDigits: 2 });
}

function formatLargeNumber(value) {
  if (value === null || value === undefined || value === "" || Number.isNaN(Number(value))) return "-";
  var n = Math.abs(Number(value));
  var sign = Number(value) < 0 ? "-" : "";
  if (n >= 1e8) return sign + (n / 1e8).toFixed(2) + "亿";
  if (n >= 1e4) return sign + (n / 1e4).toFixed(2) + "万";
  return sign + n.toLocaleString("zh-CN", { maximumFractionDigits: 2 });
}

function metricClass(value) {
  if (value === null || value === undefined || value === "") {
    return "";
  }
  const number = Number(value);
  if (Number.isNaN(number)) {
    return "";
  }
  if (number > 0) {
    return "metric-positive";
  }
  if (number < 0) {
    return "metric-negative";
  }
  return "";
}

function marketStatusClass(status) {
  if (!status) {
    return "status-closed";
  }
  return `status-${String(status).toLowerCase()}`;
}

function marketStatusBadge(status) {
  const normalized = status || "CLOSED";
  return `<span class="status-pill ${marketStatusClass(normalized)}">${normalized}</span>`;
}

function downloadStatusLabel(status) {
  const mapping = {
    not_started: "未开始",
    idle: "空闲",
    running: "下载中",
    pause_requested: "暂停中",
    paused: "已暂停",
    cancel_requested: "停止中",
    cancelled: "已停止",
    failed: "失败",
    completed: "已完成",
    completed_with_errors: "完成(有错误)",
    unsupported: "未接入",
  };
  return mapping[status] || status || "未开始";
}

function downloadStatusBadge(status) {
  const normalized = status || "not_started";
  return `<span class="status-pill status-${normalized}">${downloadStatusLabel(normalized)}</span>`;
}

function liveValue(stock, key, fallback = null) {
  if (stock && Object.prototype.hasOwnProperty.call(stock, key)) {
    return stock[key];
  }
  return fallback;
}

function populateSelect(selectId, values) {
  const select = document.getElementById(selectId);
  select.innerHTML = '<option value="">全部</option>';
  values.forEach((value) => {
    const option = document.createElement("option");
    option.value = value;
    option.textContent = value;
    select.appendChild(option);
  });
}

function renderTags(tags) {
  return `<div class="tag-row">${tags.map((tag) => `<span class="tag">${tag}</span>`).join("")}</div>`;
}

function renderBulletList(items) {
  return `<div class="tag-row">${items.map((item) => `<span class="tag">${item}</span>`).join("")}</div>`;
}

function stockLabel(instrumentId) {
  const stock = state.stockMap[instrumentId];
  if (!stock) {
    return instrumentId;
  }
  return `${stock.company_name} (${stock.symbol})`;
}

function cryptoLabel(instrumentId) {
  const asset = state.cryptoMap[instrumentId];
  if (!asset) {
    return instrumentId;
  }
  return `${asset.asset_name} (${asset.symbol})`;
}

function renderTabs() {
  document.querySelectorAll("[data-tab]").forEach((button) => {
    button.classList.toggle("is-active", button.dataset.tab === state.activeTab);
  });
  document.querySelectorAll("[data-tab-panel]").forEach((panel) => {
    panel.classList.toggle("is-active", panel.dataset.tabPanel === state.activeTab);
  });
}

function renderBotInstrumentHint() {
  const element = document.getElementById("bot-current-instrument");
  if (!element) {
    return;
  }
  const stock = state.activeInstrumentId ? state.stockMap[state.activeInstrumentId] : null;
  if (!stock) {
    element.className = "inline-badge empty-badge";
    element.textContent = "请先在选股或自选页选择一只股票";
    return;
  }
  element.className = "inline-badge";
  element.textContent = `${stock.company_name} (${stock.symbol}) · ${stock.market_region}`;
}

function renderPaperInstrumentHint() {
  const element = document.getElementById("paper-current-instrument");
  if (!element) {
    return;
  }
  const stock = state.activeInstrumentId ? state.stockMap[state.activeInstrumentId] : null;
  if (!stock) {
    element.className = "inline-badge empty-badge";
    element.textContent = "请先在选股、自选或 F10 页选择一只股票";
    return;
  }
  element.className = "inline-badge";
  element.textContent = `${stock.company_name} (${stock.symbol}) · ${stock.market_region} · ${stock.exchange_code}`;
}

function watchToggleButton(instrumentId) {
  const active = state.watchlist.includes(instrumentId);
  return `
    <button
      class="watch-toggle ${active ? "is-active" : ""}"
      type="button"
      data-watch-toggle="${instrumentId}"
      aria-label="${active ? "从自选移除" : "加入自选"}"
      title="${active ? "从自选移除" : "加入自选"}"
    >${active ? "★" : "☆"}</button>
  `;
}

function renderEmptyDetail(message) {
  const card = document.getElementById("detail-card");
  card.className = "detail-card empty-state";
  card.textContent = message;
  renderFocusCard();
  renderBotInstrumentHint();
  renderPaperInstrumentHint();
}

function renderEmptyHistory(containerId, statusId, message, statusMessage) {
  const card = document.getElementById(containerId);
  const status = document.getElementById(statusId);
  if (!card || !status) {
    return;
  }
  card.className = "kline-card empty-state";
  card.textContent = message;
  status.textContent = statusMessage;
}

function renderDetail(stock) {
  const analysis = stock.financial_analysis;
  const lastPrice = liveValue(stock, "last_price");
  const changePct = liveValue(stock, "live_change_pct", 0);
  const changeValue = liveValue(stock, "live_change", 0);
  const marketStatus = liveValue(stock, "live_market_status", "CLOSED");
  const quoteTime = liveValue(stock, "live_quote_time");
  const changeColor = changeValue >= 0 ? "var(--up)" : "var(--down)";
  const changeSign = changeValue >= 0 ? "+" : "";
  const limitUp = lastPrice ? formatNumber(lastPrice * 1.1) : "-";
  const limitDown = lastPrice ? formatNumber(lastPrice * 0.9) : "-";
  const mcap = stock.market_cap ? formatNumber(stock.market_cap) + "亿" : "-";
  const floatMcap = stock.float_market_cap ? formatNumber(stock.float_market_cap) + "亿" : "-";

  /* === 1. Stock header (top banner like 同花顺) === */
  const header = document.getElementById("stock-header");
  header.className = "stock-header";
  header.innerHTML = `
    <div class="stock-header-left">
      <div class="stock-header-name">
        <span class="stock-name-cn">${stock.company_name}</span>
        <span class="stock-name-code">${stock.symbol}</span>
        ${watchToggleButton(stock.instrument_id)}
      </div>
      <div class="stock-header-price">
        <span class="stock-big-price" style="color:${changeColor}">${formatNumber(lastPrice)}</span>
        <div class="stock-price-change" style="color:${changeColor}">
          <span>${changeSign}${formatNumber(changeValue)}</span>
          <span>${changeSign}${formatNumber(changePct)}%</span>
        </div>
      </div>
      <div class="stock-header-limits">
        <span>涨停: ${limitUp}</span>
        <span>跌停: ${limitDown}</span>
        <span>${quoteTime ? formatTime(quoteTime) : ""}</span>
      </div>
    </div>
    <div class="stock-header-metrics">
      <div class="shm-item"><span>最新价</span><strong>${formatNumber(lastPrice)}</strong></div>
      <div class="shm-item"><span>PE(TTM)</span><strong>${formatNumber(stock.pe_ttm)}</strong></div>
      <div class="shm-item"><span>PB</span><strong>${formatNumber(stock.pb)}</strong></div>
      <div class="shm-item"><span>总市值</span><strong>${mcap}</strong></div>
      <div class="shm-item"><span>流通市值</span><strong>${floatMcap}</strong></div>
      <div class="shm-item"><span>市净率</span><strong>${formatNumber(stock.pb)}</strong></div>
      <div class="shm-item"><span>市盈率(动)</span><strong>${formatNumber(stock.pe_ttm)}</strong></div>
      <div class="shm-item"><span>交易状态</span><strong>${marketStatusBadge(marketStatus)}</strong></div>
    </div>
  `;

  /* === 2. Recent visits sidebar === */
  renderStockRecentSidebar();

  /* === 3. Simulated order book === */
  renderSimulatedOrderbook(stock, lastPrice);

  /* === 4. F10 sub-tab content (default: overview) === */
  renderStockSubTab("overview", stock);

  renderFocusCard();
  renderBotInstrumentHint();
  renderPaperInstrumentHint();
}

/* Render the recent visits sidebar */
function renderStockRecentSidebar() {
  var container = document.getElementById("stock-recent-list");
  if (!container) return;
  var history = state.recentStockVisits || [];
  if (!history.length) {
    container.innerHTML = '<div class="empty-state" style="font-size:12px;padding:8px">暂无访问记录</div>';
    return;
  }
  container.innerHTML = history.map(function(id) {
    var s = state.stockMap[id];
    if (!s) return "";
    var price = liveValue(s, "last_price");
    var pct = liveValue(s, "live_change_pct", 0);
    var cls = pct >= 0 ? "metric-positive" : "metric-negative";
    return '<div class="recent-stock-item" data-instrument-id="' + id + '">' +
      '<span class="recent-stock-name">' + s.company_name + '</span>' +
      '<span class="recent-stock-price">' + formatNumber(price) + '</span>' +
      '<span class="recent-stock-pct ' + cls + '">' + formatNumber(pct) + '%</span>' +
      '</div>';
  }).join("");
}

/* Simulated order book (5 levels) */
function renderSimulatedOrderbook(stock, lastPrice) {
  var asksEl = document.getElementById("orderbook-asks");
  var bidsEl = document.getElementById("orderbook-bids");
  var midEl = document.getElementById("orderbook-mid-price");
  if (!asksEl || !bidsEl || !midEl) return;
  var p = lastPrice || stock.last_price || 100;
  var tick = p * 0.001;
  var asks = [], bids = [];
  for (var i = 5; i >= 1; i--) {
    var ap = p + tick * i;
    var av = Math.floor(Math.random() * 500 + 50);
    asks.push('<tr><td class="ob-label">卖' + i + '</td><td class="ob-price ob-ask">' + formatNumber(ap) + '</td><td class="ob-vol">' + av + '</td></tr>');
  }
  for (var j = 1; j <= 5; j++) {
    var bp = p - tick * j;
    var bv = Math.floor(Math.random() * 500 + 50);
    bids.push('<tr><td class="ob-label">买' + j + '</td><td class="ob-price ob-bid">' + formatNumber(bp) + '</td><td class="ob-vol">' + bv + '</td></tr>');
  }
  asksEl.innerHTML = asks.join("");
  bidsEl.innerHTML = bids.join("");
  midEl.innerHTML = '<strong style="color:var(--accent);font-size:16px">' + formatNumber(p) + '</strong>';

  /* Simulated trade ticks */
  var ticksEl = document.getElementById("stock-trade-ticks");
  if (!ticksEl) return;
  var ticks = [];
  for (var t = 0; t < 10; t++) {
    var side = Math.random() > 0.5 ? "买入" : "卖出";
    var sColor = side === "买入" ? "var(--up)" : "var(--down)";
    var tp = p + (Math.random() - 0.5) * tick * 4;
    var tv = Math.floor(Math.random() * 300 + 10);
    var hh = String(14 + Math.floor(t / 4)).padStart(2, "0");
    var mm = String(56 - t * 2).padStart(2, "0");
    ticks.push('<div class="tick-row"><span>' + hh + ':' + mm + '</span><span style="color:' + sColor + '">' + formatNumber(tp) + '</span><span>' + tv + '</span><span style="color:' + sColor + '">' + side + '</span></div>');
  }
  ticksEl.innerHTML = '<div class="tick-header"><span>时间</span><span>成交价</span><span>现手</span><span>性质</span></div>' + ticks.join("");
}

/* F10 sub-tab rendering */
function renderStockSubTab(subtab, stockOverride) {
  var stock = stockOverride || (state.activeInstrumentId ? state.stockMap[state.activeInstrumentId] : null);
  if (!stock) return;
  var card = document.getElementById("detail-card");
  card.className = "detail-card";
  var analysis = stock.financial_analysis || {};
  var html = "";

  if (subtab === "overview") {
    html = '<div class="f10-overview">' +
      '<div class="f10-section"><h4>公司简介</h4><p>' + (stock.f10_summary || "-") + '</p>' + renderTags(stock.concepts) +
      '<div class="tag-row"><span class="tag">' + stock.market_region + '</span><span class="tag">' + stock.exchange_code + '</span><span class="tag">' + (stock.board || "") + '</span><span class="tag">' + (stock.sector || "") + ' / ' + (stock.industry || "") + '</span></div></div>' +
      '<div class="f10-section"><h4>主营业务</h4><p>' + (stock.main_business || "-") + '</p></div>' +
      '<div class="f10-section"><h4>产品与服务</h4><p>' + (stock.products_services || "-") + '</p></div>' +
      '<div class="f10-section"><h4>竞争优势</h4><p>' + (stock.competitive_advantages || "-") + '</p></div>' +
      '<div class="f10-section"><h4>风险提示</h4><p>' + (stock.risks || "-") + '</p></div>' +
      '</div>';
  } else if (subtab === "finance") {
    html = '<div class="f10-finance">' +
      '<div class="f10-metrics-grid">' +
      '<div class="f10-metric"><span>PE(TTM)</span><strong>' + formatNumber(stock.pe_ttm) + '</strong></div>' +
      '<div class="f10-metric"><span>PB</span><strong>' + formatNumber(stock.pb) + '</strong></div>' +
      '<div class="f10-metric"><span>ROE</span><strong>' + formatNumber(stock.roe) + '%</strong></div>' +
      '<div class="f10-metric"><span>营收增速</span><strong class="' + metricClass(stock.revenue_growth) + '">' + formatNumber(stock.revenue_growth) + '%</strong></div>' +
      '<div class="f10-metric"><span>净利润增速</span><strong class="' + metricClass(stock.net_profit_growth) + '">' + formatNumber(stock.net_profit_growth) + '%</strong></div>' +
      '<div class="f10-metric"><span>毛利率</span><strong>' + formatNumber(stock.gross_margin) + '%</strong></div>' +
      '<div class="f10-metric"><span>净利率</span><strong>' + formatNumber(stock.net_margin) + '%</strong></div>' +
      '<div class="f10-metric"><span>资产负债率</span><strong>' + formatNumber(stock.debt_to_asset) + '%</strong></div>' +
      '<div class="f10-metric"><span>股息率</span><strong>' + formatNumber(stock.dividend_yield) + '%</strong></div>' +
      '<div class="f10-metric"><span>流动比率</span><strong>' + formatNumber(stock.current_ratio) + '</strong></div>' +
      '<div class="f10-metric"><span>速动比率</span><strong>' + formatNumber(stock.quick_ratio) + '</strong></div>' +
      '<div class="f10-metric"><span>利息保障</span><strong>' + formatNumber(stock.interest_coverage) + '</strong></div>' +
      '</div>' +
      '<div class="f10-section"><h4>财务分析结论</h4><p>' + (analysis.summary || "-") + '</p>' +
      '<p><strong>评分：</strong>' + formatNumber(analysis.overall_score) + ' / ' + (analysis.rating || "-") + '</p>' +
      '<p><strong>估值：</strong>' + formatNumber(analysis.valuation_score) + ' | <strong>盈利：</strong>' + formatNumber(analysis.profitability_score) +
      ' | <strong>成长：</strong>' + formatNumber(analysis.growth_score) + ' | <strong>现金流：</strong>' + formatNumber(analysis.cashflow_score) +
      ' | <strong>偿债：</strong>' + formatNumber(analysis.solvency_score) + '</p></div>' +
      '</div>';
  } else if (subtab === "business") {
    html = '<div class="f10-overview">' +
      '<div class="f10-section"><h4>主营业务</h4><p>' + (stock.main_business || "-") + '</p></div>' +
      '<div class="f10-section"><h4>产品与服务</h4><p>' + (stock.products_services || "-") + '</p></div>' +
      '<div class="f10-section"><h4>竞争优势</h4><p>' + (stock.competitive_advantages || "-") + '</p></div>' +
      '</div>';
  } else if (subtab === "shareholders") {
    html = '<div class="f10-section"><h4>股东信息</h4><p>总市值：' + (stock.market_cap ? formatNumber(stock.market_cap) + '亿' : '-') +
      '</p><p>流通市值：' + (stock.float_market_cap ? formatNumber(stock.float_market_cap) + '亿' : '-') +
      '</p><p>上市日期：' + (stock.listing_date || "-") + '</p><p>货币：' + (stock.currency || "-") + '</p></div>';
  } else if (subtab === "news") {
    html = '<div class="f10-section"><h4>公司大事</h4><p>暂无最新公告与新闻数据。</p></div>';
  } else if (subtab === "valuation") {
    html = '<div class="f10-finance">' +
      '<div class="f10-section"><h4>亮点</h4>' + renderBulletList(analysis.strengths && analysis.strengths.length ? analysis.strengths : ["暂无明显财务亮点标签"]) + '</div>' +
      '<div class="f10-section"><h4>关注点</h4>' + renderBulletList(analysis.concerns && analysis.concerns.length ? analysis.concerns : ["暂无明显财务风险标签"]) + '</div>' +
      '</div>';
  }
  card.innerHTML = html;
}

function renderFocusCard() {
  const card = document.getElementById("focus-card");
  if (!card) {
    return;
  }
  const stock = state.activeInstrumentId ? state.stockMap[state.activeInstrumentId] : null;
  if (!stock) {
    card.className = "focus-card empty-state";
    card.textContent = "选中一只股票后，这里会显示当前关注标的的价格、状态和财务概览。";
    return;
  }
  const analysis = stock.financial_analysis || {};
  const lastPrice = liveValue(stock, "last_price");
  const changePct = liveValue(stock, "live_change_pct", 0);
  const marketStatus = liveValue(stock, "live_market_status", "CLOSED");
  card.className = "focus-card";
  card.innerHTML = `
    <div class="focus-header">
      <div>
        <h3>${stock.company_name}</h3>
        <p>${stock.symbol} · ${stock.market_region} · ${stock.exchange_code}</p>
      </div>
      <div class="focus-price">
        <strong>${formatNumber(lastPrice)}</strong>
        <span class="${metricClass(changePct)}">${formatNumber(changePct)}%</span>
      </div>
    </div>
    <div class="focus-metrics">
      <div class="focus-metric">
        <span>交易状态</span>
        <strong>${marketStatusBadge(marketStatus)}</strong>
      </div>
      <div class="focus-metric">
        <span>财务评分</span>
        <strong>${formatNumber(analysis.overall_score)} / ${analysis.rating || "-"}</strong>
      </div>
      <div class="focus-metric">
        <span>ROE</span>
        <strong>${formatNumber(stock.roe)}</strong>
      </div>
      <div class="focus-metric">
        <span>营收增速</span>
        <strong class="${metricClass(stock.revenue_growth)}">${formatNumber(stock.revenue_growth)}</strong>
      </div>
    </div>
    <p class="focus-summary">${stock.f10_summary || "暂无 F10 摘要。"}</p>
    ${renderTags(stock.concepts || [])}
    <div class="focus-actions">
      <button class="ghost-button" type="button" data-tab="research">打开 F10</button>
      <button class="ghost-button" type="button" data-tab="research">查看个股</button>
      <button class="ghost-button" type="button" data-tab="compare">进入对比</button>
    </div>
  `;
}

function renderCompareTray() {
  const tray = document.getElementById("compare-tray");
  const compareTabSummary = document.getElementById("compare-tab-summary");
  if (!state.compare.left && !state.compare.right) {
    tray.className = "compare-tray empty-state";
    tray.textContent = "点击列表中的左右按钮后，这里会显示当前对比组合。";
    if (compareTabSummary) {
      compareTabSummary.className = "compare-card empty-state";
      compareTabSummary.textContent = "当前还没有完成双股票选择。";
    }
    return;
  }
  tray.className = "compare-tray";
  const summaryHtml = `
    <div class="compare-pill">
      <strong>左侧</strong>
      <span>${state.compare.left ? stockLabel(state.compare.left) : "未选择"}</span>
    </div>
    <div class="compare-pill">
      <strong>右侧</strong>
      <span>${state.compare.right ? stockLabel(state.compare.right) : "未选择"}</span>
    </div>
  `;
  tray.innerHTML = summaryHtml;
  if (compareTabSummary) {
    compareTabSummary.className = "compare-card";
    compareTabSummary.innerHTML = summaryHtml;
  }
}

function renderCompare(compare) {
  const card = document.getElementById("compare-card");
  if (!compare) {
    card.className = "compare-card empty-state";
    card.textContent = "选择左侧和右侧股票后，将展示估值、成长、现金流、偿债和 F10 摘要对比。";
    renderCompareTray();
    document.getElementById("compare-status").textContent = "先在列表中选择两只股票";
    return;
  }
  document.getElementById("compare-status").textContent = `${compare.left.company_name} vs ${compare.right.company_name}`;
  renderCompareTray();
  const metrics = compare.metrics
    .map(
      ([label, left, right]) =>
        `<tr><th>${label}</th><td class="${metricClass(left)}">${formatNumber(left)}</td><td class="${metricClass(right)}">${formatNumber(right)}</td></tr>`
    )
    .join("");
  const financialScores = compare.financial_scores
    .map(
      ([label, left, right]) =>
        `<tr><th>${label}</th><td>${formatNumber(left)}</td><td>${formatNumber(right)}</td></tr>`
    )
    .join("");
  card.className = "compare-card";
  card.innerHTML = `
    <div class="compare-grid">
      <div>
        <h3>${compare.left.company_name}</h3>
        <p>${compare.left.f10_summary}</p>
        ${renderTags(compare.left.concepts)}
        <p><strong>财务结论：</strong>${compare.left.financial_analysis.summary}</p>
      </div>
      <div>
        <h3>${compare.right.company_name}</h3>
        <p>${compare.right.f10_summary}</p>
        ${renderTags(compare.right.concepts)}
        <p><strong>财务结论：</strong>${compare.right.financial_analysis.summary}</p>
      </div>
    </div>
    <div class="table-wrap">
      <table class="metric-table">
        <thead>
          <tr><th>财务评分</th><th>${compare.left.symbol}</th><th>${compare.right.symbol}</th></tr>
        </thead>
        <tbody>${financialScores}</tbody>
      </table>
    </div>
    <div class="table-wrap">
      <table class="metric-table">
        <thead>
          <tr><th>指标</th><th>${compare.left.symbol}</th><th>${compare.right.symbol}</th></tr>
        </thead>
        <tbody>${metrics}</tbody>
      </table>
    </div>
  `;
}

function updateCryptoMap(assets) {
  (assets || []).forEach((asset) => {
    state.cryptoMap[asset.instrument_id] = asset;
  });
}

function renderCryptoChartControls() {
  document.getElementById("crypto-chart-range-buttons").innerHTML = CHART_RANGES
    .map(
      (value) => `
        <button type="button" class="range-button ${state.cryptoChartRange === value ? "is-active" : ""}" data-crypto-chart-range="${value}">
          ${value}D
        </button>
      `
    )
    .join("");
  document.getElementById("crypto-chart-mode-buttons").innerHTML = CHART_MODES
    .map(
      (mode) => `
        <button type="button" class="mode-button ${state.cryptoChartMode === mode.value ? "is-active" : ""}" data-crypto-chart-mode="${mode.value}">
          ${mode.label}
        </button>
      `
    )
    .join("");
}

function renderCryptoOverview(summary) {
  const container = document.getElementById("crypto-overview");
  const status = document.getElementById("crypto-result-summary");
  if (!container || !status) {
    return;
  }
  if (!summary) {
    container.innerHTML = "";
    status.textContent = "正在加载加密货币市场概览...";
    return;
  }
  const topGainer = (summary.top_gainers || [])[0];
  const topLoser = (summary.top_losers || [])[0];
  const mostActive = (summary.most_active || [])[0];
  const topCategory = Object.entries(summary.category_counts || {}).sort((left, right) => Number(right[1]) - Number(left[1]))[0];
  status.textContent = `已接入 ${formatNumber(summary.total_count)} 个加密货币标的，市场 24x7 运行，最近更新时间 ${formatTime(summary.as_of)}`;
  container.innerHTML = [
    {
      title: "市场标的数",
      value: formatNumber(summary.total_count),
      note: `主报价 ${Object.keys(summary.quote_currency_counts || {}).join(" / ") || "-"}`,
    },
    {
      title: "平均涨跌",
      value: `${formatNumber(summary.average_change_pct_24h)}%`,
      note: "按最近 24 小时价格变化估算",
    },
    {
      title: "最强币种",
      value: topGainer ? topGainer.symbol : "-",
      note: topGainer ? `${topGainer.asset_name} · ${formatNumber(topGainer.change_pct_24h)}%` : "暂无数据",
    },
    {
      title: "最弱币种",
      value: topLoser ? topLoser.symbol : "-",
      note: topLoser ? `${topLoser.asset_name} · ${formatNumber(topLoser.change_pct_24h)}%` : "暂无数据",
    },
    {
      title: "最活跃赛道",
      value: topCategory ? topCategory[0] : "-",
      note: mostActive ? `${mostActive.asset_name} · 24h 成交额 ${formatNumber(mostActive.turnover_24h)}` : "暂无数据",
    },
  ]
    .map(
      (card) => `
        <div class="pulse-card">
          <h3>${card.title}</h3>
          <strong>${card.value}</strong>
          <p>${card.note}</p>
        </div>
      `
    )
    .join("");
}

function renderEmptyCryptoDetail(message) {
  const card = document.getElementById("crypto-detail-card");
  if (!card) {
    return;
  }
  card.className = "detail-card empty-state";
  card.textContent = message;
}

function renderCryptoList() {
  const container = document.getElementById("crypto-watchlist");
  if (!container) {
    return;
  }
  if (!state.cryptoAssets.length) {
    container.className = "watchlist-list empty-state";
    container.textContent = "当前还没有可展示的加密货币。";
    return;
  }
  container.className = "watchlist-list";
  container.innerHTML = state.cryptoAssets
    .map(
      (asset) => `
        <div class="watch-item ${state.activeCryptoInstrumentId === asset.instrument_id ? "is-active" : ""}">
          <div class="watch-main">
            <strong>${asset.symbol}</strong>
            <span>${asset.asset_name} · ${asset.category}</span>
            <div class="watch-pricing">
              <span>${formatNumber(asset.last_price)}</span>
              <span class="${metricClass(asset.change_pct_24h)}">${formatNumber(asset.change_pct_24h)}%</span>
              <span>24h 成交额 ${formatNumber(asset.turnover_24h)}</span>
              <span>30d 波动 ${formatNumber(asset.volatility_30d)}%</span>
            </div>
          </div>
          <button class="watch-open" type="button" data-crypto-open="${asset.instrument_id}">查看</button>
        </div>
      `
    )
    .join("");
}

function renderCryptoDetail(asset) {
  const card = document.getElementById("crypto-detail-card");
  if (!card) {
    return;
  }
  card.className = "detail-card";
  card.innerHTML = `
    <div class="live-quote-strip">
      <div class="live-quote-card">
        <span>最新价</span>
        <strong>${formatNumber(asset.last_price)}</strong>
      </div>
      <div class="live-quote-card">
        <span>24h 涨跌</span>
        <strong class="${metricClass(asset.change_24h)}">${formatNumber(asset.change_24h)} / ${formatNumber(asset.change_pct_24h)}%</strong>
      </div>
      <div class="live-quote-card">
        <span>交易状态</span>
        <strong>${marketStatusBadge(asset.market_status)}</strong>
      </div>
      <div class="live-quote-card">
        <span>更新时间</span>
        <strong>${formatTime(asset.quote_time)}</strong>
      </div>
    </div>
    <div class="detail-grid">
      <div>
        <h3>${asset.asset_name} <small>${asset.symbol}</small></h3>
        <p>${asset.summary}</p>
        ${renderTags([asset.category, asset.base_currency, asset.quote_currency, asset.trading_mode])}
      </div>
      <div>
        <p><strong>24h 成交额：</strong>${formatNumber(asset.turnover_24h)}</p>
        <p><strong>24h 量：</strong>${formatNumber(asset.volume_24h)}</p>
        <p><strong>30d 波动率：</strong>${formatNumber(asset.volatility_30d)}%</p>
        <p><strong>30d 趋势：</strong><span class="${metricClass(asset.trend_30d_pct)}">${formatNumber(asset.trend_30d_pct)}%</span></p>
        <p><strong>最小价格跳动：</strong>${formatNumber(asset.tick_size)}</p>
        <p><strong>最小交易单位：</strong>${formatNumber(asset.lot_size)}</p>
      </div>
      <div>
        <h4>主要用途</h4>
        ${renderBulletList(asset.use_cases || [])}
        <h4>市场结构</h4>
        ${renderBulletList([
          `交易对 ${asset.base_currency}/${asset.quote_currency}`,
          `品类 ${asset.category}`,
          asset.microstructure && asset.microstructure.trades_24x7 ? "24x7 连续交易" : "存在交易时段限制",
        ])}
      </div>
      <div>
        <h4>关键风险</h4>
        ${renderBulletList(asset.risks || [])}
        <h4>研究提示</h4>
        ${renderBulletList([
          "优先观察趋势、波动率和成交额是否同步放大",
          "注意强叙事资产的回撤速度通常也更快",
          "与股票不同，价格行为会持续跨越夜间和周末",
        ])}
      </div>
    </div>
  `;
}

function historySourceLabel(payload) {
  if (payload.source === "local_a_share_raw") {
    return "本地真实历史";
  }
  if (payload.source === "generated_demo") {
    return "可复现演示历史";
  }
  if (payload.source === "simulated_crypto_exchange") {
    return "模拟加密市场";
  }
  return payload.source || "未知来源";
}

function calculateMA(bars, period) {
  return bars.map((_, i) => {
    if (i < period - 1) return null;
    let sum = 0;
    for (let j = i - period + 1; j <= i; j++) sum += bars[j].close;
    return sum / period;
  });
}

function renderHistoryChart(payload, { containerId, statusId, mode }) {
  const card = document.getElementById(containerId);
  const status = document.getElementById(statusId);
  const bars = payload.bars || [];
  if (!bars.length) {
    renderEmptyHistory(containerId, statusId, "当前标的没有可展示的历史走势。", "点击标的后加载最近一段走势");
    return;
  }
  const width = 980;
  const height = 380;
  const margin = { top: 22, right: 18, bottom: 34, left: 18 };
  const plotWidth = width - margin.left - margin.right;
  const plotHeight = height - margin.top - margin.bottom;
  const volumeHeight = plotHeight * 0.24;
  const pricePlotHeight = plotHeight - volumeHeight - 12;
  const maxHigh = Math.max(...bars.map((bar) => bar.high));
  const minLow = Math.min(...bars.map((bar) => bar.low));
  const priceRange = Math.max(maxHigh - minLow, 0.01);
  const maxVolume = Math.max(...bars.map((bar) => bar.volume || 0), 1);
  const step = plotWidth / Math.max(bars.length, 1);
  const candleWidth = Math.max(step * 0.66, 2.6);
  const priceY = (price) => margin.top + ((maxHigh - price) / priceRange) * pricePlotHeight;
  const volumeY = (volume) => margin.top + pricePlotHeight + 12 + (1 - volume / maxVolume) * volumeHeight;
  const linePath = bars
    .map((bar, index) => {
      const x = margin.left + step * index + step / 2;
      const y = priceY(bar.close);
      return `${index === 0 ? "M" : "L"} ${x.toFixed(2)} ${y.toFixed(2)}`;
    })
    .join(" ");
  const gridLines = [0, 0.25, 0.5, 0.75, 1]
    .map((ratio) => {
      const y = margin.top + pricePlotHeight * ratio;
      return `<line x1="${margin.left}" y1="${y}" x2="${width - margin.right}" y2="${y}" stroke="rgba(48,54,61,0.4)" stroke-dasharray="3 6" />`;
    })
    .join("");
  const volumeBars = bars
    .map((bar, index) => {
      const x = margin.left + step * index + Math.max(step * 0.15, 1);
      const y = volumeY(bar.volume || 0);
      const h = margin.top + pricePlotHeight + 12 + volumeHeight - y;
      const color = bar.close >= bar.open ? "rgba(63,185,80,0.3)" : "rgba(248,81,73,0.3)";
      return `<rect x="${x}" y="${y}" width="${Math.max(step * 0.7, 1.8)}" height="${Math.max(h, 1)}" fill="${color}" rx="1.1" />`;
    })
    .join("");
  const candles = bars
    .map((bar, index) => {
      const x = margin.left + step * index + step / 2 - candleWidth / 2;
      const wickX = x + candleWidth / 2;
      const openY = priceY(bar.open);
      const closeY = priceY(bar.close);
      const highY = priceY(bar.high);
      const lowY = priceY(bar.low);
      const bodyY = Math.min(openY, closeY);
      const bodyHeight = Math.max(Math.abs(openY - closeY), 1.5);
      const color = bar.close >= bar.open ? "#3fb950" : "#f85149";
      return `
        <line x1="${wickX}" y1="${highY}" x2="${wickX}" y2="${lowY}" stroke="${color}" stroke-width="1.3" />
        <rect x="${x}" y="${bodyY}" width="${candleWidth}" height="${bodyHeight}" fill="${color}" rx="1.2" />
      `;
    })
    .join("");
  const ma5 = calculateMA(bars, 5);
  const ma10 = calculateMA(bars, 10);
  const ma20 = calculateMA(bars, 20);

  function maPath(maValues, color) {
    const points = maValues.map((v, i) => {
      if (v === null) return null;
      const x = margin.left + step * i + step / 2;
      const y = priceY(v);
      return `${x.toFixed(2)} ${y.toFixed(2)}`;
    }).filter(Boolean);
    if (!points.length) return '';
    return `<polyline points="${points.join(' ')}" fill="none" stroke="${color}" stroke-width="1.2" opacity="0.85" />`;
  }

  status.textContent = `${payload.symbol} 最近 ${bars.length} 根K线`;
  card.className = "kline-card";
  card.innerHTML = `
    <div class="kline-summary">
      <span class="kline-stat">最新收盘 ${formatNumber(payload.summary.latest_close)}</span>
      <span class="kline-stat ${metricClass(payload.summary.change_pct)}">区间涨跌 ${formatNumber(payload.summary.change_pct)}%</span>
      <span class="kline-stat">区间高点 ${formatNumber(payload.summary.period_high)}</span>
      <span class="kline-stat">区间低点 ${formatNumber(payload.summary.period_low)}</span>
      <span class="kline-stat">数据来源 ${historySourceLabel(payload)}</span>
      <span class="kline-stat" style="border-color:rgba(240,136,62,0.3);color:#f0883e">MA5 ${ma5[ma5.length-1] !== null ? formatNumber(ma5[ma5.length-1]) : '-'}</span>
      <span class="kline-stat" style="border-color:rgba(88,166,255,0.3);color:#58a6ff">MA10 ${ma10[ma10.length-1] !== null ? formatNumber(ma10[ma10.length-1]) : '-'}</span>
      <span class="kline-stat" style="border-color:rgba(210,168,255,0.3);color:#d2a8ff">MA20 ${ma20[ma20.length-1] !== null ? formatNumber(ma20[ma20.length-1]) : '-'}</span>
    </div>
    <svg class="kline-svg" viewBox="0 0 ${width} ${height}" role="img" aria-label="${payload.symbol} historical chart">
      <rect x="0" y="0" width="${width}" height="${height}" fill="transparent"></rect>
      ${gridLines}
      <line x1="${margin.left}" y1="${margin.top + pricePlotHeight}" x2="${width - margin.right}" y2="${margin.top + pricePlotHeight}" stroke="rgba(48,54,61,0.6)" />
      <line x1="${margin.left}" y1="${margin.top + pricePlotHeight + 12}" x2="${width - margin.right}" y2="${margin.top + pricePlotHeight + 12}" stroke="rgba(48,54,61,0.4)" />
      ${volumeBars}
      ${mode === "candles" ? candles : `<path d="${linePath}" fill="none" stroke="#f08a24" stroke-width="2.2" stroke-linejoin="round" stroke-linecap="round" />`}
      ${maPath(ma5, '#f0883e')}
      ${maPath(ma10, '#58a6ff')}
      ${maPath(ma20, '#d2a8ff')}
      <text x="${margin.left}" y="${height - 10}" fill="#7d8590" font-size="12">${bars[0].trade_date}</text>
      <text x="${width - margin.right - 74}" y="${height - 10}" fill="#7d8590" font-size="12">${bars[bars.length - 1].trade_date}</text>
      <text x="${width - margin.right - 58}" y="${margin.top + 10}" fill="#7d8590" font-size="12">${formatNumber(maxHigh)}</text>
      <text x="${width - margin.right - 58}" y="${margin.top + pricePlotHeight - 2}" fill="#7d8590" font-size="12">${formatNumber(minLow)}</text>
    </svg>
  `;
}

function renderKlineChart(payload) {
  renderHistoryChart(payload, { containerId: "kline-card", statusId: "chart-status", mode: state.chartMode });
}

function renderCryptoChart(payload) {
  renderHistoryChart(payload, { containerId: "crypto-kline-card", statusId: "crypto-chart-status", mode: state.cryptoChartMode });
}

// ── Futures ──

function renderFuturesChart(payload) {
  renderHistoryChart(payload, { containerId: "futures-kline-card", statusId: "futures-chart-status", mode: state.futuresChartMode });
}

async function loadFuturesUniverse() {
  try {
    const overview = await fetchJson("/api/futures/universe");
    if (overview.data) {
      state.futuresOverview = overview.data;
      renderFuturesOverview(overview.data);
    }
    const list = await fetchJson("/api/futures/contracts");
    if (list.data && list.data.contracts) {
      state.futuresContracts = list.data.contracts;
      renderFuturesWatchlist(list.data.contracts);
      if (!state.activeFuturesInstrumentId && list.data.contracts.length > 0) {
        await loadFuturesDetail(list.data.contracts[0].instrument_id);
      }
    }
  } catch (err) {
    document.getElementById("futures-result-summary").textContent = "加载期货数据失败。";
  }
}

function renderFuturesOverview(data) {
  document.getElementById("futures-result-summary").textContent =
    data.total_count + " 个合约 · 平均涨跌 " + formatNumber(data.average_change_pct) + "%";
  var container = document.getElementById("futures-overview");
  var cards = [
    { title: "合约总数", value: data.total_count, note: "模拟期货交易所" },
    { title: "平均涨跌", value: formatNumber(data.average_change_pct) + "%", note: "全部合约均值" },
  ];
  var gainers = data.top_gainers || [];
  var losers = data.top_losers || [];
  var active = data.most_active || [];
  if (gainers.length) cards.push({ title: "领涨合约", value: gainers[0].symbol, note: gainers[0].display_name + " · " + formatNumber(gainers[0].change_pct) + "%" });
  if (losers.length) cards.push({ title: "领跌合约", value: losers[0].symbol, note: losers[0].display_name + " · " + formatNumber(losers[0].change_pct) + "%" });
  if (active.length) cards.push({ title: "最活跃", value: active[0].symbol, note: active[0].display_name + " · 成交额 " + formatNumber(active[0].turnover_24h) });
  container.innerHTML = cards.map(function (c) {
    return '<div class="pulse-card"><h3>' + c.title + "</h3><strong>" + c.value + "</strong><p>" + c.note + "</p></div>";
  }).join("");
}

function renderFuturesWatchlist(contracts) {
  var container = document.getElementById("futures-watchlist");
  if (!contracts.length) {
    container.className = "watchlist-list empty-state";
    container.textContent = "没有可用的期货合约。";
    return;
  }
  container.className = "watchlist-list";
  container.innerHTML = contracts.map(function (c) {
    var active = state.activeFuturesInstrumentId === c.instrument_id;
    var changeClass = c.change_pct >= 0 ? "metric-positive" : "metric-negative";
    return '<div class="watch-item' + (active ? " is-active" : "") + '" data-futures-id="' + c.instrument_id + '">' +
      '<div class="watch-main"><strong>' + c.display_name + " " + c.symbol + "</strong>" +
      '<span>' + c.category + " · " + (c.market_label || "") + " · 到期 " + (c.expiry_at ? c.expiry_at.slice(0, 10) : "-") + "</span>" +
      '<div class="watch-pricing"><strong>' + formatNumber(c.last_price) + "</strong>" +
      ' <span class="' + changeClass + '">' + (c.change_pct >= 0 ? "+" : "") + formatNumber(c.change_pct) + "%</span></div></div>" +
      '<button class="watch-open" data-futures-open="' + c.instrument_id + '">详情</button></div>';
  }).join("");
}

async function loadFuturesDetail(instrumentId) {
  state.activeFuturesInstrumentId = instrumentId;
  try {
    var detail = await fetchJson("/api/futures/detail?instrument_id=" + encodeURIComponent(instrumentId));
    if (detail.data) renderFuturesDetail(detail.data);
    var klines = await fetchJson("/api/futures/klines?instrument_id=" + encodeURIComponent(instrumentId) + "&limit=" + state.futuresChartRange);
    if (klines.data) renderFuturesChart(klines.data);
  } catch (err) {}
  renderFuturesWatchlist(state.futuresContracts);
}

function renderFuturesDetail(data) {
  var card = document.getElementById("futures-detail-card");
  card.className = "detail-card";
  card.innerHTML =
    '<div class="live-quote-strip">' +
    '<div class="live-quote-card"><span>最新价</span><strong>' + formatNumber(data.last_price) + "</strong></div>" +
    '<div class="live-quote-card"><span>涨跌幅</span><strong class="' + (data.change_pct >= 0 ? "metric-positive" : "metric-negative") + '">' + formatNumber(data.change_pct) + "%</strong></div>" +
    '<div class="live-quote-card"><span>合约乘数</span><strong>' + formatNumber(data.contract_multiplier) + "</strong></div>" +
    '<div class="live-quote-card"><span>到期日</span><strong>' + (data.expiry_at ? data.expiry_at.slice(0, 10) : "-") + "</strong></div>" +
    "</div>" +
    '<div class="detail-grid">' +
    '<div class="detail-card" style="padding:14px"><h3>' + data.symbol + " " + (data.display_name || "") + "</h3>" +
    "<p>" + (data.summary || "") + "</p>" +
    '<div class="tag-row">' +
    '<span class="tag">类型: ' + (data.instrument_type || "future") + "</span>" +
    '<span class="tag">最小变动: ' + formatNumber(data.tick_size) + "</span>" +
    '<span class="tag">手数: ' + formatNumber(data.lot_size) + "</span>" +
    "</div></div>" +
    '<div class="detail-card" style="padding:14px"><h3>保证金要求</h3>' +
    "<p>初始保证金率: " + formatNumber((data.margin_info || {}).initial_margin_pct || 12) + "%</p>" +
    "<p>维持保证金率: " + formatNumber((data.margin_info || {}).maintenance_margin_pct || 8) + "%</p>" +
    "<p>波动率(30日): " + formatNumber(data.volatility_30d) + "%</p>" +
    "</div></div>";
}

function renderFuturesChartControls() {
  var rangeContainer = document.getElementById("futures-chart-range-buttons");
  var modeContainer = document.getElementById("futures-chart-mode-buttons");
  if (!rangeContainer || !modeContainer) return;
  var ranges = [30, 60, 120, 180];
  rangeContainer.innerHTML = ranges.map(function (r) {
    return '<button class="range-button' + (state.futuresChartRange === r ? " is-active" : "") + '" type="button" data-futures-chart-range="' + r + '">' + r + "D</button>";
  }).join("");
  var modes = [{ key: "candles", label: "K线" }, { key: "line", label: "折线" }];
  modeContainer.innerHTML = modes.map(function (m) {
    return '<button class="mode-button' + (state.futuresChartMode === m.key ? " is-active" : "") + '" type="button" data-futures-chart-mode="' + m.key + '">' + m.label + "</button>";
  }).join("");
}

function describeEvent(event) {
  const details = event.details || {};
  switch (event.event_type) {
    case "open_page":
      return "打开了股票筛选工作台";
    case "submit_filters":
      return `执行了一次筛股，返回 ${details.result_count ?? "-"} 只股票`;
    case "reset_filters":
      return "重置了筛股条件";
    case "view_stock_detail":
      return `查看了股票详情：${details.instrument_id || "-"}`;
    case "pick_compare_stock":
      return `设置了 ${details.side === "left" ? "左侧" : "右侧"} 对比股票：${details.instrument_id || "-"}`;
    case "toggle_watchlist":
      return `${details.action === "added" ? "加入" : "移除"}自选：${details.instrument_id || "-"}`;
    case "apply_preset":
      return `切换了筛股预设：${details.preset || "-"}`;
    case "change_chart_view":
      return `切换图表：${details.mode === "line" ? "折线" : "K线"} / ${details.range || "-"} bars`;
    case "view_crypto_detail":
      return `查看了加密货币详情：${details.instrument_id || "-"}`;
    case "change_crypto_chart_view":
      return `切换加密图表：${details.mode === "line" ? "折线" : "K线"} / ${details.range || "-"} bars`;
    case "switch_tab":
      return `切换到了 ${tabLabel(details.tab)} 标签页`;
    case "history_download_start":
      return `启动或续传了历史下载任务：${details.job_id || "-"}`;
    case "history_download_pause":
      return `暂停了历史下载任务：${details.job_id || "-"}`;
    case "history_download_stop":
      return `停止了历史下载任务：${details.job_id || "-"}`;
    case "paper_submit_order":
      return `提交了模拟订单：${details.instrument_id || "-"} / ${details.side || "-"}`;
    case "paper_cancel_order":
      return `撤销了模拟订单：${details.order_id || "-"}`;
    case "paper_reset_account":
      return "重置了模拟交易账户";
    case "open_learning_lesson":
      return `打开了学习课程：${details.lesson_id || "-"}`;
    case "submit_learning_quiz":
      return `提交了学习测验：${details.score ?? "-"} 分`;
    case "create_bot":
      return `创建了策略机器人：${details.bot_name || details.bot_id || "-"}`;
    case "bot_action":
      return `执行了机器人动作：${details.action || "-"} / ${details.bot_id || "-"}`;
    case "bot_interact":
      return `执行了机器人交互：${details.command || "-"} / ${details.bot_id || "-"}`;
    default:
      return event.event_type;
  }
}

function renderActivity(events) {
  const container = document.getElementById("activity-log");
  if (!events || events.length === 0) {
    container.className = "activity-log empty-state";
    container.textContent = "当前还没有操作记录。";
    return;
  }
  container.className = "activity-log";
  container.innerHTML = events
    .map(
      (event) => `
        <div class="activity-item">
          <strong>${describeEvent(event)}</strong>
          <p>${formatTime(event.created_at)}</p>
        </div>
      `
    )
    .join("");
}

function renderBotTemplates() {
  const select = document.getElementById("bot-template-select");
  if (!select) {
    return;
  }
  const currentValue = select.value;
  select.innerHTML = state.botTemplates
    .map((template) => `<option value="${template.template_code}">${template.template_name}</option>`)
    .join("");
  if (currentValue && state.botTemplates.some((template) => template.template_code === currentValue)) {
    select.value = currentValue;
  }
}

function botStatusBadge(status) {
  const mapping = {
    draft: "草稿",
    running: "运行中",
    paused: "已暂停",
    stopped: "已停止",
  };
  return `<span class="status-pill status-${status || "not_started"}">${mapping[status] || status || "未知"}</span>`;
}

function notificationLevelClass(level) {
  if (level === "warning") {
    return "metric-negative";
  }
  if (level === "critical") {
    return "metric-negative";
  }
  return "metric-positive";
}

function renderBotOverview() {
  const container = document.getElementById("bot-overview");
  if (!container) {
    return;
  }
  const running = state.bots.filter((bot) => bot.status === "running").length;
  const paused = state.bots.filter((bot) => bot.status === "paused").length;
  const draft = state.bots.filter((bot) => bot.status === "draft").length;
  const activeSignals = state.bots.filter((bot) => Math.abs(Number(((bot.last_signal || {}).target_weight) || 0)) > 0.001).length;
  container.innerHTML = `
    <div class="bot-overview-card"><span>运行中</span><strong>${formatNumber(running)}</strong></div>
    <div class="bot-overview-card"><span>已暂停</span><strong>${formatNumber(paused)}</strong></div>
    <div class="bot-overview-card"><span>草稿</span><strong>${formatNumber(draft)}</strong></div>
    <div class="bot-overview-card"><span>有信号</span><strong>${formatNumber(activeSignals)}</strong></div>
  `;
}

function renderBots() {
  const container = document.getElementById("bot-list");
  if (!container) {
    return;
  }
  renderBotOverview();
  if (!state.bots.length) {
    container.className = "bot-list empty-state";
    container.textContent = "当前还没有策略机器人，选择一只股票后可以从模板快速创建。";
    return;
  }
  container.className = "bot-list";
  container.innerHTML = state.bots
    .map((bot) => {
      const lastSignal = bot.last_signal || {};
      const metrics = bot.metrics || {};
      const currentPrice = bot.last_price ?? "-";
      const maxWeight = bot.params && bot.params.max_weight !== undefined ? bot.params.max_weight : "";
      const fastWindow = bot.params && bot.params.fast_window !== undefined ? bot.params.fast_window : "";
      const slowWindow = bot.params && bot.params.slow_window !== undefined ? bot.params.slow_window : "";
      return `
        <div class="bot-card">
          <div class="bot-card-head">
            <div>
              <h3>${bot.bot_name}</h3>
              <p>${bot.template_name} · ${bot.symbol} · ${bot.mode.toUpperCase()}</p>
            </div>
            <div>${botStatusBadge(bot.status)}</div>
          </div>
          <div class="bot-metrics-grid">
            <div class="bot-metric"><span>最新价</span><strong>${formatNumber(currentPrice)}</strong></div>
            <div class="bot-metric"><span>价格变化</span><strong class="${metricClass(metrics.price_change_pct)}">${formatNumber(metrics.price_change_pct)}%</strong></div>
            <div class="bot-metric"><span>目标仓位</span><strong class="${metricClass(metrics.signal_weight)}">${formatNumber(metrics.signal_weight)}</strong></div>
            <div class="bot-metric"><span>信号原因</span><strong>${metrics.signal_reason || "-"}</strong></div>
          </div>
          <div class="bot-param-grid">
            <label>
              <span>快线</span>
              <input type="number" step="1" data-bot-param="${bot.bot_id}:fast_window" value="${fastWindow}" />
            </label>
            <label>
              <span>慢线</span>
              <input type="number" step="1" data-bot-param="${bot.bot_id}:slow_window" value="${slowWindow}" />
            </label>
            <label>
              <span>最大仓位</span>
              <input type="number" step="0.01" data-bot-param="${bot.bot_id}:max_weight" value="${maxWeight}" />
            </label>
          </div>
          <div class="bot-actions">
            <button class="primary-button" type="button" data-bot-action="start" data-bot-id="${bot.bot_id}">启动</button>
            <button class="ghost-button" type="button" data-bot-action="pause" data-bot-id="${bot.bot_id}">暂停</button>
            <button class="ghost-button" type="button" data-bot-action="stop" data-bot-id="${bot.bot_id}">停止</button>
            <button class="ghost-button" type="button" data-bot-command="sync_now" data-bot-id="${bot.bot_id}">同步</button>
            <button class="ghost-button" type="button" data-bot-command="liquidate" data-bot-id="${bot.bot_id}">清仓</button>
            <button class="ghost-button" type="button" data-bot-save-params="${bot.bot_id}">更新参数</button>
          </div>
          <p class="bot-foot">更新时间 ${formatTime(bot.updated_at)} · 最近心跳 ${formatTime(metrics.heartbeat_at)}${metrics.manual_override ? ` · 手动覆盖 ${metrics.manual_override}` : ""}</p>
        </div>
      `;
    })
    .join("");
}

function renderNotifications() {
  const container = document.getElementById("notification-list");
  if (!container) {
    return;
  }
  if (!state.notifications.length) {
    container.className = "notification-list empty-state";
    container.textContent = "当前还没有机器人通知。";
    return;
  }
  container.className = "notification-list";
  container.innerHTML = state.notifications
    .map(
      (item) => `
        <div class="notification-item">
          <div class="notification-head">
            <strong class="${notificationLevelClass(item.level)}">${item.title}</strong>
            <span>${formatTime(item.created_at)}</span>
          </div>
          <p>${item.message}</p>
        </div>
      `
    )
    .join("");
}

function renderPaperOrderTypeState() {
  const orderType = document.getElementById("paper-order-type");
  const limitPrice = document.getElementById("paper-order-limit-price");
  if (!orderType || !limitPrice) {
    return;
  }
  const isLimit = orderType.value === "limit";
  limitPrice.disabled = !isLimit;
  if (!isLimit) {
    limitPrice.value = "";
  }
}

function renderPaperFeedback(message, isError = false) {
  const container = document.getElementById("paper-order-feedback");
  if (!container) {
    return;
  }
  container.className = `paper-feedback ${isError ? "paper-feedback-error" : ""}`;
  container.textContent = message;
}

function paperOrderStatusBadge(status) {
  return `<span class="status-pill status-${status || "not_started"}">${status || "-"}</span>`;
}

function renderPaperDashboard(payload) {
  state.paperDashboard = payload;
  renderPaperInstrumentHint();
  const overview = document.getElementById("paper-account-overview");
  const positions = document.getElementById("paper-positions");
  const orders = document.getElementById("paper-orders");
  const fills = document.getElementById("paper-fills");
  const diff = document.getElementById("paper-strategy-diff");
  if (!overview || !positions || !orders || !fills || !diff) {
    return;
  }
  const snapshot = payload && payload.snapshot ? payload.snapshot : null;
  if (!snapshot) {
    overview.innerHTML = "";
    positions.className = "paper-list empty-state";
    positions.textContent = "模拟账户正在初始化。";
    orders.className = "paper-list empty-state";
    orders.textContent = "当前还没有模拟订单。";
    fills.className = "paper-list empty-state";
    fills.textContent = "当前还没有模拟成交。";
    diff.className = "paper-diff empty-state";
    diff.textContent = "选择股票后，这里会显示当前模拟仓位与策略目标仓位的差异摘要。";
    return;
  }
  overview.innerHTML = `
    <div class="paper-overview-card"><span>账户权益</span><strong>${formatNumber(snapshot.equity)}</strong></div>
    <div class="paper-overview-card"><span>可用现金</span><strong>${formatNumber(snapshot.cash)}</strong></div>
    <div class="paper-overview-card"><span>持仓市值</span><strong>${formatNumber(snapshot.positions_value)}</strong></div>
    <div class="paper-overview-card"><span>总敞口</span><strong>${formatNumber(snapshot.gross_exposure)}</strong></div>
    <div class="paper-overview-card"><span>杠杆</span><strong>${formatNumber(snapshot.leverage)}</strong></div>
    <div class="paper-overview-card"><span>回撤</span><strong class="${metricClass(snapshot.drawdown)}">${formatNumber(snapshot.drawdown * 100)}%</strong></div>
  `;
  renderPaperVisualizations(payload, snapshot);

  if (!payload.positions || !payload.positions.length) {
    positions.className = "paper-list empty-state";
    positions.textContent = "当前还没有模拟持仓。";
  } else {
    positions.className = "paper-list";
    positions.innerHTML = payload.positions
      .map(
        (item) => `
          <div class="paper-item">
            <div class="paper-item-head">
              <div>
                <strong>${item.company_name}</strong>
                <p>${item.symbol} · ${formatNumber(item.quantity)} 股</p>
              </div>
              <div class="paper-item-side">
                <strong>${formatNumber(item.market_value)}</strong>
                <span class="${metricClass(item.unrealized_pnl)}">${formatNumber(item.unrealized_pnl)}</span>
              </div>
            </div>
            <div class="paper-metric-row">
              <span>成本 ${formatNumber(item.average_cost)}</span>
              <span>现价 ${formatNumber(item.last_price)}</span>
              <span>权重 ${formatNumber(item.weight * 100)}%</span>
              <span>已实现 ${formatNumber(item.realized_pnl)}</span>
            </div>
          </div>
        `
      )
      .join("");
  }

  if (!payload.orders || !payload.orders.length) {
    orders.className = "paper-list empty-state";
    orders.textContent = "当前还没有模拟订单。";
  } else {
    orders.className = "paper-list";
    orders.innerHTML = payload.orders
      .map(
        (item) => `
          <div class="paper-item">
            <div class="paper-item-head">
              <div>
                <strong>${item.symbol}</strong>
                <p>${item.side === "buy" ? "买入" : "卖出"} · ${item.order_type === "limit" ? "限价" : "市价"} · ${formatNumber(item.quantity)} 股</p>
              </div>
              <div class="paper-item-side">
                ${paperOrderStatusBadge(item.status)}
              </div>
            </div>
            <div class="paper-metric-row">
              <span>成交 ${formatNumber(item.filled_quantity)}</span>
              <span>剩余 ${formatNumber(item.remaining_quantity)}</span>
              <span>均价 ${formatNumber(item.average_fill_price)}</span>
              <span>${formatTime(item.updated_at)}</span>
            </div>
            ${
              item.status === "accepted" || item.status === "partially_filled"
                ? `<div class="paper-inline-actions"><button class="ghost-button" type="button" data-paper-cancel-order="${item.order_id}">撤单</button></div>`
                : ""
            }
            ${item.rejection_reason ? `<p class="paper-error-text">拒单原因：${item.rejection_reason}</p>` : ""}
          </div>
        `
      )
      .join("");
  }

  if (!payload.fills || !payload.fills.length) {
    fills.className = "paper-list empty-state";
    fills.textContent = "当前还没有模拟成交。";
  } else {
    fills.className = "paper-list";
    fills.innerHTML = payload.fills
      .map(
        (item) => `
          <div class="paper-item">
            <div class="paper-item-head">
              <div>
                <strong>${item.symbol}</strong>
                <p>${item.side === "buy" ? "买入成交" : "卖出成交"} · ${formatNumber(item.quantity)} 股</p>
              </div>
              <div class="paper-item-side">
                <strong>${formatNumber(item.price)}</strong>
                <span>${formatTime(item.timestamp)}</span>
              </div>
            </div>
            <div class="paper-metric-row">
              <span>手续费 ${formatNumber(item.fee)}</span>
              <span>订单 ${item.order_id}</span>
            </div>
          </div>
        `
      )
      .join("");
  }

  if (!payload.strategy_diff) {
    diff.className = "paper-diff empty-state";
    diff.textContent = "选择股票后，这里会显示当前模拟仓位与策略目标仓位的差异摘要。";
    return;
  }
  const strategyDiff = payload.strategy_diff;
  const backtestMetrics = strategyDiff.backtest_metrics || {};
  diff.className = "paper-diff";
  diff.innerHTML = `
    <div class="paper-item-head">
      <div>
        <strong>${strategyDiff.symbol}</strong>
        <p>${strategyDiff.summary}</p>
      </div>
      <div class="paper-item-side">
        <strong class="${metricClass(strategyDiff.weight_gap)}">${formatNumber(strategyDiff.weight_gap * 100)}%</strong>
        <span>仓位差</span>
      </div>
    </div>
    <div class="paper-metric-row">
      <span>策略目标 ${formatNumber(strategyDiff.signal_target_weight * 100)}%</span>
      <span>模拟仓位 ${formatNumber(strategyDiff.paper_weight * 100)}%</span>
      <span>信号原因 ${strategyDiff.signal_reason}</span>
    </div>
    ${
      strategyDiff.backtest_metrics
        ? `
          <div class="paper-metric-row">
            <span>参考回测收益 ${formatNumber(backtestMetrics.total_return * 100)}%</span>
            <span>参考回测回撤 ${formatNumber(backtestMetrics.max_drawdown * 100)}%</span>
            <span>参考回测换手 ${formatNumber(backtestMetrics.turnover)}</span>
          </div>
        `
        : ""
    }
  `;
}

function lessonById(lessonId) {
  if (!state.learningHub || !state.learningHub.lessons) {
    return null;
  }
  return state.learningHub.lessons.find((lesson) => lesson.lesson_id === lessonId) || null;
}

function renderLearningOverview() {
  const container = document.getElementById("learning-overview");
  if (!container || !state.learningHub || !state.learningHub.overview) {
    return;
  }
  const overview = state.learningHub.overview;
  const progress = state.learningProgress || {};
  const progressCards = `
    <div class="learning-overview-card">
      <span class="learning-step-index">P</span>
      <strong>当前章节</strong>
      <p>${lessonById(progress.current_lesson_id)?.title || "还没有开始，建议从课程 1 开始。"}</p>
    </div>
    <div class="learning-overview-card">
      <span class="learning-step-index">S</span>
      <strong>最佳得分</strong>
      <p>${progress.best_score !== null && progress.best_score !== undefined ? `${formatNumber(progress.best_score)} 分` : "还没有提交测验"}</p>
    </div>
    <div class="learning-overview-card">
      <span class="learning-step-index">Q</span>
      <strong>测验次数</strong>
      <p>${formatNumber(progress.quiz_attempts || 0)} 次</p>
    </div>
  `;
  container.innerHTML =
    (overview.steps || [])
    .map(
      (item) => `
        <div class="learning-overview-card">
          <span class="learning-step-index">0${item.step}</span>
          <strong>${item.title}</strong>
          <p>${item.description}</p>
        </div>
      `
    )
    .join("") + progressCards;
}

function renderLearningKnowledgeBase() {
  const container = document.getElementById("learning-knowledge-base");
  const summary = document.getElementById("learning-knowledge-summary");
  const searchInput = document.getElementById("learning-knowledge-search");
  if (!container) {
    return;
  }
  if (searchInput && searchInput.value !== state.learningSearchQuery) {
    searchInput.value = state.learningSearchQuery || "";
  }
  if (!state.learningHub || !state.learningHub.knowledge_base) {
    container.className = "learning-knowledge-grid empty-state";
    container.textContent = "知识库正在加载。";
    if (summary) {
      summary.textContent = "";
    }
    return;
  }
  const normalizedQuery = (state.learningSearchQuery || "").trim().toLowerCase();
  const sections = state.learningHub.knowledge_base;
  const totalEntries = sections.reduce((count, section) => count + (section.entries || []).length, 0);
  const filteredSections = sections
    .map((section) => {
      if (!normalizedQuery) {
        return section;
      }
      const matchingEntries = (section.entries || []).filter((entry) => {
        const haystack = [
          section.category,
          section.description,
          ...((section.keywords || [])),
          entry.term,
          entry.summary,
          entry.why_it_matters,
          ...((entry.keywords || [])),
        ]
          .join(" ")
          .toLowerCase();
        return haystack.includes(normalizedQuery);
      });
      if (!matchingEntries.length) {
        return null;
      }
      return { ...section, entries: matchingEntries };
    })
    .filter(Boolean);
  const visibleEntries = filteredSections.reduce((count, section) => count + (section.entries || []).length, 0);
  if (summary) {
    summary.textContent = normalizedQuery
      ? `已筛选到 ${formatNumber(filteredSections.length)} 个主题 / ${formatNumber(visibleEntries)} 个知识点，关键词：${state.learningSearchQuery}`
      : `当前知识库覆盖 ${formatNumber(sections.length)} 个主题 / ${formatNumber(totalEntries)} 个知识点。`;
  }
  if (!filteredSections.length) {
    container.className = "learning-knowledge-grid empty-state";
    container.textContent = "没有找到匹配的知识点，试试搜索别的术语。";
    return;
  }
  container.className = "learning-knowledge-grid";
  container.innerHTML = filteredSections
    .map(
      (section) => `
        <article class="learning-knowledge-card">
          <div class="learning-card-head">
            <h3>${section.category}</h3>
            <p>${section.description}</p>
          </div>
          <div class="learning-entry-list">
            ${(section.entries || [])
              .map(
                (entry) => `
                  <div class="learning-entry-item">
                    <strong>${entry.term}</strong>
                    <p>${entry.summary}</p>
                    <span>${entry.why_it_matters}</span>
                  </div>
                `
              )
              .join("")}
          </div>
        </article>
      `
    )
    .join("");
}

function renderLearningStudyPlan() {
  const container = document.getElementById("learning-study-plan");
  if (!container) {
    return;
  }
  if (!state.learningHub || !state.learningHub.study_plan) {
    container.className = "learning-plan-list empty-state";
    container.textContent = "学习计划正在加载。";
    return;
  }
  container.className = "learning-plan-list";
  container.innerHTML = state.learningHub.study_plan
    .map(
      (stage) => `
        <article class="learning-plan-card">
          <div class="learning-card-head">
            <h3>${stage.title}</h3>
            <p>${stage.goal}</p>
          </div>
          <div class="learning-metric-row">
            <span>建议时长 ${stage.duration}</span>
            <span>关联课程 ${(stage.lessons || []).length} 节</span>
          </div>
          <ul class="learning-check-list">
            ${(stage.deliverables || []).map((item) => `<li>${item}</li>`).join("")}
          </ul>
        </article>
      `
    )
    .join("");
}

function renderLearningLessonNav() {
  const container = document.getElementById("learning-lesson-nav");
  if (!container) {
    return;
  }
  if (!state.learningHub || !state.learningHub.lessons) {
    container.className = "learning-lesson-nav empty-state";
    container.textContent = "课程目录正在加载。";
    return;
  }
  if (
    (!state.selectedLessonId || !state.learningHub.lessons.some((lesson) => lesson.lesson_id === state.selectedLessonId)) &&
    state.learningHub.lessons.length
  ) {
    state.selectedLessonId = state.learningHub.lessons[0].lesson_id;
  }
  container.className = "learning-lesson-nav";
  container.innerHTML = state.learningHub.lessons
    .map(
      (lesson, index) => `
        <button
          class="learning-lesson-button ${lesson.lesson_id === state.selectedLessonId ? "is-active" : ""}"
          type="button"
          data-learning-lesson="${lesson.lesson_id}"
        >
          <span class="learning-lesson-index">${index + 1}</span>
          <span class="learning-lesson-text">
            <strong>${lesson.title}</strong>
            <small>${lesson.duration} · ${lesson.level}</small>
          </span>
        </button>
      `
    )
    .join("");
}

function renderLearningContent() {
  const container = document.getElementById("learning-content");
  const status = document.getElementById("learning-content-status");
  if (!container || !status) {
    return;
  }
  const lesson = lessonById(state.selectedLessonId);
  if (!lesson) {
    container.className = "learning-content empty-state";
    container.textContent = "请选择一节课程开始学习。";
    status.textContent = "请选择左侧课程。";
    return;
  }
  status.textContent = `${lesson.title} · ${lesson.duration} · ${lesson.level}`;
  container.className = "learning-content";
  container.innerHTML = `
    <div class="learning-content-head">
      <h3>${lesson.title}</h3>
      <p>${lesson.goals.join("；")}</p>
    </div>
    <div class="learning-goal-row">
      ${lesson.goals.map((goal) => `<span class="tag">${goal}</span>`).join("")}
    </div>
    <div class="learning-section-list">
      ${lesson.sections
        .map(
          (section) => `
            <article class="learning-section-card">
              <h4>${section.heading}</h4>
              <p>${section.body}</p>
              <ul class="learning-check-list">
                ${section.bullets.map((item) => `<li>${item}</li>`).join("")}
              </ul>
            </article>
          `
        )
        .join("")}
    </div>
    <div class="learning-practice-card">
      <h4>课后练习</h4>
      <p>${lesson.practice.prompt}</p>
      <ul class="learning-check-list">
        ${lesson.practice.checklist.map((item) => `<li>${item}</li>`).join("")}
      </ul>
    </div>
  `;
}

function renderLearningQuiz() {
  const container = document.getElementById("learning-quiz");
  if (!container) {
    return;
  }
  if (!state.learningHub || !state.learningHub.quiz) {
    container.className = "learning-quiz empty-state";
    container.textContent = "测验题库正在加载。";
    return;
  }
  const quiz = state.learningHub.quiz;
  container.className = "learning-quiz";
  container.innerHTML = quiz.questions
    .map(
      (question, index) => `
        <article class="learning-quiz-card">
          <div class="learning-quiz-question">${index + 1}. ${question.prompt}</div>
          <div class="learning-option-list">
            ${question.options
              .map(
                (option) => `
                  <label class="learning-option">
                    <input type="radio" name="${question.question_id}" value="${option.option_id}" />
                    <span>${option.text}</span>
                  </label>
                `
              )
              .join("")}
          </div>
        </article>
      `
    )
    .join("");
}

function renderLearningQuizResult(result) {
  const container = document.getElementById("learning-quiz-result");
  if (!container) {
    return;
  }
  if (!result) {
    container.className = "learning-quiz-result empty-state";
    const progress = state.learningProgress || {};
    if (progress.last_score !== null && progress.last_score !== undefined) {
      container.className = "learning-quiz-result";
      container.innerHTML = `
        <div class="learning-result-head">
          <div>
            <strong>最近一次学习检验</strong>
            <p>${progress.updated_at ? `最近更新：${formatTime(progress.updated_at)}` : "已保存最近一次测验结果。"}</p>
          </div>
          <div class="learning-result-score">${formatNumber(progress.last_score)} 分</div>
        </div>
        <div class="learning-metric-row">
          <span>最佳成绩 ${progress.best_score !== null && progress.best_score !== undefined ? `${formatNumber(progress.best_score)} 分` : "-"}</span>
          <span>累计测验 ${formatNumber(progress.quiz_attempts || 0)} 次</span>
        </div>
      `;
      return;
    }
    container.textContent = "提交后，这里会显示得分、错题解释和建议复习章节。";
    return;
  }
  const weakLessons = result.weak_lessons || [];
  const incorrectResults = (result.results || []).filter((item) => !item.is_correct);
  container.className = "learning-quiz-result";
  container.innerHTML = `
    <div class="learning-result-head">
      <div>
        <strong>${result.title}</strong>
        <p>${result.passed ? "已达标，可以继续进入平台实战。" : "还没完全掌握，先把错题对应章节再复习一轮。"}</p>
      </div>
      <div class="learning-result-score ${result.passed ? "metric-positive" : "metric-negative"}">
        ${formatNumber(result.score)} 分
      </div>
    </div>
    <div class="learning-metric-row">
      <span>通过线 ${formatNumber(result.pass_score)} 分</span>
      <span>答对 ${formatNumber(result.correct_count)} / ${formatNumber(result.total_questions)}</span>
    </div>
    ${
      weakLessons.length
        ? `
          <div class="learning-result-block">
            <h4>建议复习章节</h4>
            <div class="tag-row">${weakLessons.map((lesson) => `<span class="tag">${lesson.title}</span>`).join("")}</div>
          </div>
        `
        : ""
    }
    ${
      incorrectResults.length
        ? `
          <div class="learning-result-block">
            <h4>错题解释</h4>
            <div class="learning-review-list">
              ${incorrectResults
                .map(
                  (item) => `
                    <div class="learning-review-item">
                      <strong>${item.prompt}</strong>
                      <p>${item.explanation}</p>
                    </div>
                  `
                )
                .join("")}
            </div>
          </div>
        `
        : ""
    }
    <div class="learning-result-block">
      <h4>下一步建议</h4>
      <ul class="learning-check-list">
        ${(result.recommended_next_steps || []).map((item) => `<li>${item}</li>`).join("")}
      </ul>
    </div>
  `;
}

function renderLearningHub(payload) {
  state.learningHub = payload.hub || null;
  state.learningProgress = payload.progress || null;
  if (!state.selectedLessonId) {
    state.selectedLessonId = (state.learningProgress || {}).current_lesson_id || null;
  }
  renderLearningOverview();
  renderLearningKnowledgeBase();
  renderLearningStudyPlan();
  renderLearningLessonNav();
  renderLearningContent();
  renderLearningQuiz();
  renderLearningQuizResult(state.lastLearningQuizResult);
}

function updateStockMap(stocks) {
  stocks.forEach((stock) => {
    state.stockMap[stock.instrument_id] = stock;
  });
}

function computePulseCards() {
  if (state.marketSnapshot && state.marketSnapshot.summary) {
    const summary = state.marketSnapshot.summary;
    const topGainer = (summary.top_gainers || [])[0];
    const topLoser = (summary.top_losers || [])[0];
    const mostActive = (summary.most_active || [])[0];
    return [
      {
        title: "市场宽度",
        value: `${summary.advancing} / ${summary.declining}`,
        note: `上涨 ${summary.advancing} | 下跌 ${summary.declining} | 平盘 ${summary.unchanged}`,
      },
      {
        title: "领涨股",
        value: topGainer ? topGainer.symbol : "-",
        note: topGainer ? `${topGainer.company_name} · ${formatNumber(topGainer.change_pct)}%` : "暂无数据",
      },
      {
        title: "领跌股",
        value: topLoser ? topLoser.symbol : "-",
        note: topLoser ? `${topLoser.company_name} · ${formatNumber(topLoser.change_pct)}%` : "暂无数据",
      },
      {
        title: "最活跃",
        value: mostActive ? mostActive.symbol : "-",
        note: mostActive ? `${mostActive.company_name} · ${formatNumber(mostActive.turnover)}` : "暂无数据",
      },
      {
        title: "总成交额",
        value: formatNumber(summary.total_turnover),
        note: `行情时间 ${formatTime(state.marketSnapshot.as_of)}`,
      },
    ];
  }
  if (state.universeSummary) {
    const marketCounts = state.universeSummary.market_counts || {};
    const boardCounts = state.universeSummary.board_counts || {};
    const exchangeCounts = state.universeSummary.exchange_counts || {};
    const topBoard = Object.entries(boardCounts).sort((left, right) => Number(right[1]) - Number(left[1]))[0];
    const topExchange = Object.entries(exchangeCounts).sort((left, right) => Number(right[1]) - Number(left[1]))[0];
    return [
      {
        title: "股票池",
        value: formatNumber(state.universeSummary.total_count),
        note: `A股 ${formatNumber(marketCounts.CN || 0)} · 港股 ${formatNumber(marketCounts.HK || 0)} · 美股 ${formatNumber(marketCounts.US || 0)}`,
      },
      {
        title: "主要板块",
        value: topBoard ? topBoard[0] : "-",
        note: topBoard ? `挂牌数量 ${formatNumber(topBoard[1])}` : "暂无板块统计",
      },
      {
        title: "主要交易所",
        value: topExchange ? topExchange[0] : "-",
        note: topExchange ? `挂牌数量 ${formatNumber(topExchange[1])}` : "暂无交易所统计",
      },
      {
        title: "当前选股页",
        value: formatNumber(state.stockResultCount || 0),
        note: `默认显示前 ${formatNumber(DEFAULT_STOCK_LIST_LIMIT)} 只，按全市场股票池进行筛选`,
      },
      {
        title: "主数据来源",
        value: "Official",
        note: "A股 / 港股 / 美股挂牌清单已进入统一股票目录",
      },
    ];
  }
  const stocks = state.universe;
  if (!stocks.length) {
    return [];
  }
  const highestRoe = [...stocks].filter((item) => item.roe !== null).sort((a, b) => b.roe - a.roe)[0];
  const highestGrowth = [...stocks].filter((item) => item.revenue_growth !== null).sort((a, b) => b.revenue_growth - a.revenue_growth)[0];
  const highestDividend = [...stocks].filter((item) => item.dividend_yield !== null).sort((a, b) => b.dividend_yield - a.dividend_yield)[0];
  const lowestPe = [...stocks].filter((item) => item.pe_ttm !== null && item.pe_ttm > 0).sort((a, b) => a.pe_ttm - b.pe_ttm)[0];
  return [
    {
      title: "股票池",
      value: stocks.length,
      note: "当前已接入的 A/H/US 股票目录",
    },
    {
      title: "质量领先",
      value: highestRoe ? highestRoe.symbol : "-",
      note: highestRoe ? `${highestRoe.company_name} · ROE ${formatNumber(highestRoe.roe)}` : "暂无数据",
    },
    {
      title: "成长领先",
      value: highestGrowth ? highestGrowth.symbol : "-",
      note: highestGrowth ? `${highestGrowth.company_name} · 营收增速 ${formatNumber(highestGrowth.revenue_growth)}` : "暂无数据",
    },
    {
      title: "股息领先",
      value: highestDividend ? highestDividend.symbol : "-",
      note: highestDividend ? `${highestDividend.company_name} · 股息率 ${formatNumber(highestDividend.dividend_yield)}` : "暂无数据",
    },
    {
      title: "价值镜头",
      value: lowestPe ? lowestPe.symbol : "-",
      note: lowestPe ? `${lowestPe.company_name} · PE ${formatNumber(lowestPe.pe_ttm)}` : "暂无数据",
    },
  ];
}

function renderMarketPulse() {
  const container = document.getElementById("market-pulse");
  const cards = computePulseCards();
  container.innerHTML = cards
    .map(
      (card) => `
        <div class="pulse-card">
          <h3>${card.title}</h3>
          <strong>${card.value}</strong>
          <p>${card.note}</p>
        </div>
      `
    )
    .join("");
  renderSectorHeatmap();
  renderSentimentPanel();
}

function renderSectorHeatmap() {
  const container = document.getElementById("sector-heatmap");
  if (!container) return;
  const stocks = state.universe;
  if (!stocks.length) {
    container.innerHTML = '<div class="empty-state" style="min-height:80px">加载股票数据后将显示行业热力图</div>';
    return;
  }
  const sectorMap = {};
  stocks.forEach(function (s) {
    const sec = s.sector || "其他";
    if (!sectorMap[sec]) sectorMap[sec] = { count: 0, totalChange: 0 };
    sectorMap[sec].count++;
    sectorMap[sec].totalChange += (s.change_pct || 0);
  });
  const sectors = Object.entries(sectorMap)
    .map(function (entry) {
      return { name: entry[0], count: entry[1].count, avgChange: entry[1].totalChange / entry[1].count };
    })
    .sort(function (a, b) { return b.count - a.count; })
    .slice(0, 20);
  const maxAbs = Math.max.apply(null, sectors.map(function (s) { return Math.abs(s.avgChange); }).concat([0.01]));
  container.innerHTML = sectors
    .map(function (s) {
      var intensity = Math.min(Math.abs(s.avgChange) / maxAbs, 1);
      var bg = s.avgChange >= 0
        ? "rgba(63,185,80," + (0.1 + intensity * 0.35) + ")"
        : "rgba(248,81,73," + (0.1 + intensity * 0.35) + ")";
      var textColor = s.avgChange >= 0 ? "#3fb950" : "#f85149";
      return '<div class="heatmap-tile" style="background:' + bg + '"><strong>' + s.name + "</strong><span style=\"color:" + textColor + "\">" + (s.avgChange >= 0 ? "+" : "") + formatNumber(s.avgChange) + "%</span><span>" + s.count + "只</span></div>";
    })
    .join("");
}

function renderSentimentPanel() {
  var scoreEl = document.getElementById("sentiment-score");
  var labelEl = document.getElementById("sentiment-label");
  var biasEl = document.getElementById("bias-direction");
  var confEl = document.getElementById("bias-confidence");
  var countEl = document.getElementById("intel-count");
  var latestEl = document.getElementById("intel-latest");
  if (!scoreEl) return;
  fetchJson("/api/report/sentiment").then(function (payload) {
    if (payload && payload.data) {
      var d = payload.data;
      var score = d.avg_score !== undefined ? d.avg_score : 0;
      scoreEl.textContent = formatNumber(score);
      scoreEl.style.color = score > 0 ? "#3fb950" : score < 0 ? "#f85149" : "#7d8590";
      labelEl.textContent = score > 0.2 ? "偏多" : score < -0.2 ? "偏空" : "中性";
    }
  }).catch(function () {});
  fetchJson("/api/report/bias").then(function (payload) {
    if (payload && payload.data) {
      var d = payload.data;
      biasEl.textContent = d.direction || "-";
      biasEl.style.color = d.direction === "LONG" ? "#3fb950" : d.direction === "SHORT" ? "#f85149" : "#7d8590";
      confEl.textContent = "置信度 " + formatNumber((d.confidence || 0) * 100) + "%";
    }
  }).catch(function () {});
  fetchJson("/api/intelligence/recent").then(function (payload) {
    if (payload && payload.data) {
      var docs = payload.data.documents || [];
      countEl.textContent = String(docs.length);
      latestEl.textContent = docs.length > 0 ? docs[0].title || "最新情报" : "暂无最新情报";
    }
  }).catch(function () {});
}

function renderRiskDashboard() {
  fetchJson("/api/risk/dashboard").then(function (payload) {
    if (!payload || !payload.data) return;
    var d = payload.data;
    var ksEl = document.getElementById("risk-kill-switch");
    var lvEl = document.getElementById("risk-leverage");
    var ddEl = document.getElementById("risk-drawdown");
    var alEl = document.getElementById("risk-alerts");
    if (ksEl) {
      var active = d.kill_switch_active;
      ksEl.innerHTML = '<h3>杀开关</h3><strong class="' + (active ? "risk-critical" : "risk-normal") + '">' + (active ? "ON" : "OFF") + "</strong><p>" + (active ? "系统已暂停交易" : "系统正常运行") + "</p>";
    }
    var metrics = d.metrics || {};
    if (lvEl) {
      var lev = metrics.leverage || 0;
      lvEl.innerHTML = '<h3>杠杆率</h3><strong>' + formatNumber(lev) + "x</strong><p>" + (lev > 3 ? "高杠杆警告" : "正常范围") + "</p>";
    }
    if (ddEl) {
      var dd = metrics.drawdown || 0;
      ddEl.innerHTML = '<h3>最大回撤</h3><strong class="' + (dd > 0.1 ? "risk-warning" : "risk-normal") + '">' + formatNumber(dd * 100) + "%</strong><p>" + (dd > 0.1 ? "超过10%阈值" : "正常范围") + "</p>";
    }
    var alerts = d.alerts || [];
    if (alEl) {
      alEl.innerHTML = '<h3>活跃告警</h3><strong class="' + (alerts.length > 0 ? "risk-warning" : "risk-normal") + '">' + alerts.length + "</strong><p>" + (alerts.length > 0 ? alerts[0].message : "无告警") + "</p>";
    }
  }).catch(function () {});
}

function renderPaperVisualizations(payload, snapshot) {
  var donutContainer = document.getElementById("paper-donut");
  var equityCurveContainer = document.getElementById("paper-equity-curve");
  if (snapshot && snapshot.equity) {
    state.equityHistory.push({ time: new Date().toLocaleTimeString("zh-CN", { hour12: false }), equity: snapshot.equity });
    if (state.equityHistory.length > 100) state.equityHistory.shift();
  }
  if (donutContainer && payload.positions && payload.positions.length) {
    var total = snapshot.equity || 1;
    var segments = payload.positions.map(function (p) {
      return {
        label: p.company_name || p.symbol,
        value: Math.abs(p.market_value),
        pct: (Math.abs(p.market_value) / total) * 100,
        color: p.unrealized_pnl >= 0 ? "#3fb950" : "#f85149",
      };
    });
    segments.push({ label: "现金", value: snapshot.cash, pct: (snapshot.cash / total) * 100, color: "#7d8590" });
    var cx = 100, cy = 100, r = 80, inner = 50;
    var angle = -90;
    var paths = segments.map(function (seg) {
      var sweep = (seg.pct / 100) * 360;
      var startAngle = (angle * Math.PI) / 180;
      var endAngle = ((angle + sweep) * Math.PI) / 180;
      angle += sweep;
      var x1 = cx + r * Math.cos(startAngle);
      var y1 = cy + r * Math.sin(startAngle);
      var x2 = cx + r * Math.cos(endAngle);
      var y2 = cy + r * Math.sin(endAngle);
      var ix1 = cx + inner * Math.cos(endAngle);
      var iy1 = cy + inner * Math.sin(endAngle);
      var ix2 = cx + inner * Math.cos(startAngle);
      var iy2 = cy + inner * Math.sin(startAngle);
      var large = sweep > 180 ? 1 : 0;
      return '<path d="M' + x1 + " " + y1 + " A" + r + " " + r + " 0 " + large + " 1 " + x2 + " " + y2 + " L" + ix1 + " " + iy1 + " A" + inner + " " + inner + " 0 " + large + " 0 " + ix2 + " " + iy2 + ' Z" fill="' + seg.color + '" opacity="0.85"><title>' + seg.label + " " + formatNumber(seg.pct) + "%</title></path>";
    });
    donutContainer.innerHTML =
      '<h4 style="margin:0 0 8px">持仓分布</h4>' +
      '<svg class="donut-chart-svg" viewBox="0 0 200 200" width="180" height="180">' + paths.join("") + "</svg>" +
      '<div style="margin-top:8px;font-size:12px;color:var(--muted)">' + segments.map(function (s) { return '<span style="color:' + s.color + '">' + s.label + " " + formatNumber(s.pct) + "%</span>"; }).join(" · ") + "</div>";
  } else if (donutContainer) {
    donutContainer.innerHTML = '<div class="empty-state" style="min-height:120px">暂无持仓数据</div>';
  }
  if (equityCurveContainer && state.equityHistory.length > 1) {
    var pts = state.equityHistory;
    var minE = Math.min.apply(null, pts.map(function (p) { return p.equity; }));
    var maxE = Math.max.apply(null, pts.map(function (p) { return p.equity; }));
    var eRange = Math.max(maxE - minE, 0.01);
    var w = 400, h = 120, pad = 20;
    var pw = w - 2 * pad, ph = h - 2 * pad;
    var linePts = pts.map(function (p, i) {
      var x = pad + (i / (pts.length - 1)) * pw;
      var y = pad + ((maxE - p.equity) / eRange) * ph;
      return x.toFixed(1) + "," + y.toFixed(1);
    }).join(" ");
    equityCurveContainer.innerHTML =
      '<h4 style="margin:0 0 8px">资金曲线</h4>' +
      '<svg class="equity-curve-svg" viewBox="0 0 ' + w + " " + h + '" width="100%" height="' + h + '">' +
      '<polyline points="' + linePts + '" fill="none" stroke="#3fb950" stroke-width="1.5" />' +
      '<text x="' + pad + '" y="' + (h - 4) + '" fill="#7d8590" font-size="10">' + pts[0].time + "</text>" +
      '<text x="' + (w - pad - 50) + '" y="' + (h - 4) + '" fill="#7d8590" font-size="10">' + pts[pts.length - 1].time + "</text>" +
      '<text x="' + (w - pad - 60) + '" y="' + (pad + 10) + '" fill="#7d8590" font-size="10">' + formatNumber(maxE) + "</text>" +
      "</svg>";
  } else if (equityCurveContainer) {
    equityCurveContainer.innerHTML = '<div class="empty-state" style="min-height:80px">交易后将显示资金曲线</div>';
  }
}

function setupQuickTrade() {
  var buyBtn = document.getElementById("quick-buy-btn");
  var sellBtn = document.getElementById("quick-sell-btn");
  if (!buyBtn || !sellBtn) return;
  function doQuickTrade(side) {
    var symbol = state.activeInstrumentId;
    if (!symbol) { alert("请先选择一只股票"); return; }
    var qty = parseInt(document.getElementById("quick-trade-qty").value) || 100;
    postJson("/api/paper/quick-trade", { symbol: symbol, side: side, quantity: qty }).then(function (r) {
      if (r.code === "OK") {
        var fb = document.getElementById("paper-order-feedback");
        if (fb) fb.textContent = side === "buy" ? "快捷买入成功" : "快捷卖出成功";
      }
    }).catch(function () {});
  }
  buyBtn.addEventListener("click", function () { doQuickTrade("buy"); });
  sellBtn.addEventListener("click", function () { doQuickTrade("sell"); });
}

function updateQuickTradeBar() {
  var symEl = document.getElementById("quick-trade-symbol");
  var priceEl = document.getElementById("quick-trade-price");
  if (!symEl) return;
  if (state.activeInstrumentId) {
    var stock = state.universe.find(function (s) { return s.instrument_id === state.activeInstrumentId; });
    symEl.textContent = stock ? (stock.symbol + " " + stock.company_name) : state.activeInstrumentId;
    priceEl.textContent = stock && stock.last_price ? formatNumber(stock.last_price) : "-";
    priceEl.style.color = stock && stock.change_pct > 0 ? "#3fb950" : stock && stock.change_pct < 0 ? "#f85149" : "#e6edf3";
  } else {
    symEl.textContent = "未选中标的";
    priceEl.textContent = "-";
  }
}

function downloadProgress(job) {
  const total = Number(job.total_discovered || 0);
  if (!total) {
    return 0;
  }
  return Math.max(0, Math.min(100, Math.round((Number(job.completed_count || 0) / total) * 100)));
}

function humanizeDownloadError(message) {
  if (!message) {
    return "";
  }
  if (message.includes("baostock is not installed")) {
    return "未安装 BaoStock 依赖，请在项目虚拟环境中安装后再下载 A 股历史数据。";
  }
  return message;
}

function renderDownloadOverview(jobs) {
  const container = document.getElementById("download-overview");
  if (!container) {
    return;
  }
  const safeJobs = jobs || [];
  const running = safeJobs.filter((job) => ["running", "pause_requested"].includes(job.status)).length;
  const completed = safeJobs.filter((job) => ["completed", "completed_with_errors"].includes(job.status)).length;
  const failed = safeJobs.filter((job) => job.status === "failed").length;
  const unsupported = safeJobs.filter((job) => job.status === "unsupported").length;
  const totalRows = safeJobs.reduce((sum, job) => sum + Number(job.downloaded_rows || 0), 0);
  container.innerHTML = `
    <div class="download-overview-card">
      <span>运行中任务</span>
      <strong>${formatNumber(running)}</strong>
    </div>
    <div class="download-overview-card">
      <span>已完成任务</span>
      <strong>${formatNumber(completed)}</strong>
    </div>
    <div class="download-overview-card">
      <span>失败任务</span>
      <strong>${formatNumber(failed)}</strong>
    </div>
    <div class="download-overview-card">
      <span>未接入市场</span>
      <strong>${formatNumber(unsupported)}</strong>
    </div>
    <div class="download-overview-card">
      <span>累计下载行数</span>
      <strong>${formatNumber(totalRows)}</strong>
    </div>
  `;
}

function renderDownloadJobs(jobs) {
  state.downloadJobs = jobs || [];
  renderDownloadOverview(state.downloadJobs);
  const container = document.getElementById("download-jobs");
  if (!state.downloadJobs.length) {
    container.className = "download-job-grid empty-state";
    container.textContent = "当前还没有历史数据下载任务。";
    updateDownloadHubStatus("当前没有历史数据下载任务。");
    return;
  }
  container.className = "download-job-grid";
  container.innerHTML = state.downloadJobs
    .map((job) => {
      const progress = downloadProgress(job);
      const total = Number(job.total_discovered || 0);
      const downloadLabel = job.completed_count > 0 || job.status === "paused" ? "继续下载" : "开始下载";
      const disableDownload = !job.available_actions.download || ["running", "pause_requested", "cancel_requested"].includes(job.status);
      const disablePause = !job.available_actions.pause;
      const disableStop = !job.available_actions.stop;
      const lastError = humanizeDownloadError(job.last_error);
      return `
        <div class="download-job-card">
          <div class="download-job-head">
            <div>
              <h3>${job.title}</h3>
              <p>${job.description}</p>
            </div>
            <div>${downloadStatusBadge(job.status)}</div>
          </div>
          <p class="download-job-meta">${job.market_region} · ${job.granularity} · ${job.source_name}</p>
          <div class="download-stat-grid">
            <div class="download-stat">
              <span>已完成</span>
              <strong>${formatNumber(job.completed_count || 0)}</strong>
            </div>
            <div class="download-stat">
              <span>待处理</span>
              <strong>${formatNumber(job.pending_count || 0)}</strong>
            </div>
            <div class="download-stat">
              <span>失败数</span>
              <strong>${formatNumber(job.failed_count || 0)}</strong>
            </div>
            <div class="download-stat">
              <span>已下载行数</span>
              <strong>${formatNumber(job.downloaded_rows || 0)}</strong>
            </div>
          </div>
          <div class="download-progress" aria-hidden="true">
            <div class="download-progress-bar" style="width:${progress}%"></div>
          </div>
          <p class="download-job-foot">
            进度 ${progress}% · 总标的 ${total || "-"} · 最近更新 ${formatTime(job.updated_at)}${lastError ? ` · 最近错误 ${lastError}` : ""}
          </p>
          <div class="download-actions">
            <button class="primary-button" type="button" data-download-action="start" data-job-id="${job.job_id}" ${disableDownload ? "disabled" : ""}>${downloadLabel}</button>
            <button class="ghost-button" type="button" data-download-action="pause" data-job-id="${job.job_id}" ${disablePause ? "disabled" : ""}>暂停</button>
            <button class="ghost-button" type="button" data-download-action="stop" data-job-id="${job.job_id}" ${disableStop ? "disabled" : ""}>停止</button>
          </div>
        </div>
      `;
    })
    .join("");
  const activeJob = state.downloadJobs.find((job) => job.status === "running" || job.status === "pause_requested");
  if (activeJob) {
    updateDownloadHubStatus(`当前任务：${activeJob.title} · ${downloadStatusLabel(activeJob.status)} · 最近更新 ${formatTime(activeJob.updated_at)}`);
    return;
  }
  updateDownloadHubStatus("可在这里启动、暂停或停止历史数据下载任务。");
}

function renderWatchlist() {
  const container = document.getElementById("watchlist-list");
  if (!state.watchlist.length) {
    container.className = "watchlist-list empty-state";
    container.textContent = "还没有加入自选的股票。";
    return;
  }
  container.className = "watchlist-list";
  container.innerHTML = state.watchlist
    .map((instrumentId) => {
      const stock = state.stockMap[instrumentId];
      if (!stock) {
        return "";
      }
      return `
        <div class="watch-item">
          <div class="watch-main">
            <strong>${stock.symbol}</strong>
            <span>${stock.company_name}</span>
            <div class="watch-pricing">
              <span class="${metricClass(liveValue(stock, "live_change_pct", 0))}">${formatNumber(liveValue(stock, "last_price"))}</span>
              <span class="${metricClass(liveValue(stock, "live_change_pct", 0))}">${formatNumber(liveValue(stock, "live_change_pct", 0))}%</span>
              <span>${liveValue(stock, "live_market_status", "CLOSED")}</span>
            </div>
          </div>
          <button class="watch-open" type="button" data-watch-open="${stock.instrument_id}">查看</button>
        </div>
      `;
    })
    .join("");
}

function renderPresetButtons() {
  const container = document.getElementById("preset-buttons");
  container.innerHTML = Object.entries(PRESETS)
    .map(
      ([key, preset]) => `
        <button type="button" class="preset-button ${state.activePreset === key ? "is-active" : ""}" data-preset="${key}">
          <strong>${preset.label}</strong><br />
          <span>${preset.description}</span>
        </button>
      `
    )
    .join("");
}

function renderChartControls() {
  document.getElementById("chart-range-buttons").innerHTML = CHART_RANGES
    .map(
      (value) => `
        <button type="button" class="range-button ${state.chartRange === value ? "is-active" : ""}" data-chart-range="${value}">
          ${value}D
        </button>
      `
    )
    .join("");
  document.getElementById("chart-mode-buttons").innerHTML = CHART_MODES
    .map(
      (mode) => `
        <button type="button" class="mode-button ${state.chartMode === mode.value ? "is-active" : ""}" data-chart-mode="${mode.value}">
          ${mode.label}
        </button>
      `
    )
    .join("");
}

function updateMarketFeedStatus(snapshot, message = null) {
  const element = document.getElementById("market-runtime-status");
  if (message) {
    element.textContent = message;
    return;
  }
  if (state.activeTab === "crypto" && state.cryptoSummary) {
    element.textContent = `当前处于加密货币 24x7 观察模式，页面每 ${Math.round(REALTIME_ACTIVE_POLL_MS / 1000)} 秒刷新一次，最近更新时间：${formatTime(state.cryptoSummary.as_of)}`;
    return;
  }
  if (!snapshot) {
    element.textContent = "后台行情流尚未返回快照。";
    return;
  }
  const browserPollMs = Number(snapshot.recommended_poll_ms || REALTIME_IDLE_POLL_MS);
  const browserPollSeconds = Math.max(1, Math.round(browserPollMs / 1000));
  const backendSeconds = Number(snapshot.interval_seconds || 0);
  if (snapshot.live_window) {
    element.textContent = `当前处于交易时段，页面每 ${browserPollSeconds} 秒自动刷新一次价格；后台行情引擎每 ${backendSeconds} 秒推进一次，最近更新时间：${formatTime(snapshot.as_of)}`;
    return;
  }
  element.textContent = `当前处于非交易时段，页面每 ${browserPollSeconds} 秒检查一次最新状态；后台行情引擎每 ${backendSeconds} 秒维护快照，最近更新时间：${formatTime(snapshot.as_of)}`;
}

function mergeRealtimeFields(target, quote) {
  if (!target || !quote) {
    return;
  }
  target.last_price = quote.last_price;
  target.live_previous_close = quote.previous_close;
  target.live_open_price = quote.open_price;
  target.live_high_price = quote.high_price;
  target.live_low_price = quote.low_price;
  target.live_volume = quote.volume;
  target.live_turnover = quote.turnover;
  target.live_change = quote.change;
  target.live_change_pct = quote.change_pct;
  target.live_market_status = quote.market_status;
  target.live_quote_time = quote.quote_time;
  target.live_source = quote.source;
}

function mergeRealtimeQuote(quote) {
  mergeRealtimeFields(state.stockMap[quote.instrument_id], quote);
  state.currentStocks.forEach((stock) => {
    if (stock.instrument_id === quote.instrument_id) {
      mergeRealtimeFields(stock, quote);
    }
  });
  state.universe.forEach((stock) => {
    if (stock.instrument_id === quote.instrument_id) {
      mergeRealtimeFields(stock, quote);
    }
  });
}

function applyRealtimeSnapshot(snapshot) {
  state.marketSnapshot = snapshot;
  (snapshot.quotes || []).forEach((quote) => mergeRealtimeQuote(quote));
  updateMarketFeedStatus(snapshot);
  renderMarketPulse();
  renderWatchlist();
  renderCompareTray();
  renderFocusCard();
  if (state.currentStocks.length) {
    renderTable(state.currentStocks);
  }
  if (state.activeInstrumentId && state.stockMap[state.activeInstrumentId]) {
    renderDetail(state.stockMap[state.activeInstrumentId]);
  }
}

function buildRealtimeInstrumentIds() {
  const instrumentIds = new Set(state.currentStocks.map((stock) => stock.instrument_id));
  state.watchlist.forEach((instrumentId) => instrumentIds.add(instrumentId));
  if (state.activeInstrumentId) {
    instrumentIds.add(state.activeInstrumentId);
  }
  if (state.compare.left) {
    instrumentIds.add(state.compare.left);
  }
  if (state.compare.right) {
    instrumentIds.add(state.compare.right);
  }
  return [...instrumentIds];
}

async function loadRealtimeSnapshot() {
  const instrumentIds = buildRealtimeInstrumentIds();
  const query = instrumentIds.length ? `?instrument_ids=${encodeURIComponent(instrumentIds.join(","))}` : "";
  const payload = await fetchJson(`/api/market/realtime${query}`);
  applyRealtimeSnapshot(payload.data);
}

function scheduleRealtimePolling() {
  if (state.realtimeTimer) {
    window.clearTimeout(state.realtimeTimer);
  }
  const stockPollMs = Math.max(1000, Number((state.marketSnapshot || {}).recommended_poll_ms || REALTIME_IDLE_POLL_MS));
  const nextPollMs = state.activeTab === "crypto" ? REALTIME_ACTIVE_POLL_MS : stockPollMs;
  state.realtimeTimer = window.setTimeout(async () => {
    try {
      await loadRealtimeSnapshot();
      if (state.activeTab === "crypto") {
        await loadCryptoUniverse({ preferredInstrumentId: state.activeCryptoInstrumentId, silent: true });
      }
      if (
        state.activeTab === "paper" ||
        (state.paperDashboard &&
          (((state.paperDashboard.positions || []).length > 0) || Number(state.paperDashboard.open_order_count || 0) > 0))
      ) {
        await loadPaperDashboardSafely({ silent: true });
      }
      await refreshBotConsole();
    } catch (error) {
      updateMarketFeedStatus(null, "后台行情流暂时不可用，稍后自动重试。");
    } finally {
      scheduleRealtimePolling();
    }
  }, nextPollMs);
}

async function loadDownloadJobs() {
  const payload = await fetchJson("/api/history-downloads");
  renderDownloadJobs(payload.data);
}

async function loadPaperDashboard() {
  const query = state.activeInstrumentId ? `?instrument_id=${encodeURIComponent(state.activeInstrumentId)}` : "";
  const payload = await fetchJson(`/api/paper/account${query}`);
  if (payload.code !== "OK") {
    throw new Error(payload.message || "模拟交易面板暂时不可用。");
  }
  renderPaperDashboard(payload.data);
}

async function loadPaperDashboardSafely({ silent = false } = {}) {
  try {
    await loadPaperDashboard();
  } catch (error) {
    state.paperDashboard = null;
    renderPaperDashboard(null);
    if (!silent) {
      renderPaperFeedback("模拟交易暂时不可用，稍后自动重试。", true);
    }
  }
}

async function loadLearningHub() {
  const payload = await fetchJson("/api/learning/hub");
  renderLearningHub(payload.data);
}

function scheduleDownloadPolling() {
  if (state.downloadTimer) {
    window.clearTimeout(state.downloadTimer);
  }
  state.downloadTimer = window.setTimeout(async () => {
    try {
      await loadDownloadJobs();
    } catch (error) {
      updateDownloadHubStatus("历史下载中心暂时不可用，稍后自动重试。");
    } finally {
      scheduleDownloadPolling();
    }
  }, DOWNLOAD_POLL_MS);
}

async function setActiveTab(tab, { persist = false, log = false } = {}) {
  state.activeTab = normalizeTab(tab);
  renderTabs();
  if (state.activeTab === "learning") {
    await loadLearningHub();
  }
  if (state.activeTab === "crypto" && !state.cryptoAssets.length) {
    await loadCryptoUniverse({ preferredInstrumentId: state.activeCryptoInstrumentId });
  }
  if (state.activeTab === "futures" && !state.futuresContracts.length) {
    await loadFuturesUniverse();
  }
  if (state.activeTab === "paper") {
    await loadPaperDashboardSafely();
  }
  if (state.activeTab === "downloads") {
    renderRiskDashboard();
  }
  updateMarketFeedStatus(state.marketSnapshot);
  if (persist) {
    await saveWorkspace();
  }
  if (log) {
    await logEvent("switch_tab", { tab: state.activeTab });
    await loadActivity();
  }
}

async function controlDownloadJob(action, jobId) {
  const path = {
    start: "/api/history-downloads/start",
    pause: "/api/history-downloads/pause",
    stop: "/api/history-downloads/stop",
  }[action];
  if (!path) {
    return;
  }
  updateDownloadHubStatus(`正在执行 ${jobId} 的${action === "start" ? "下载" : action === "pause" ? "暂停" : "停止"}操作...`);
  const payload = await postJson(path, { job_id: jobId });
  if (payload.code !== "OK") {
    updateDownloadHubStatus(payload.error && payload.error.message ? payload.error.message : "历史下载任务操作失败。");
    return;
  }
  await loadDownloadJobs();
  await logEvent(`history_download_${action}`, { job_id: jobId });
  await loadActivity();
}

async function submitPaperOrder() {
  if (!state.activeInstrumentId) {
    renderPaperFeedback("请先选择一只股票，再提交模拟订单。", true);
    return;
  }
  const orderType = document.getElementById("paper-order-type").value;
  const quantity = Number(document.getElementById("paper-order-quantity").value || 0);
  const limitPriceField = document.getElementById("paper-order-limit-price").value;
  const payload = await postJson("/api/paper/orders", {
    instrument_id: state.activeInstrumentId,
    side: document.getElementById("paper-order-side").value,
    quantity,
    order_type: orderType,
    limit_price: orderType === "limit" && limitPriceField !== "" ? Number(limitPriceField) : null,
  });
  if (payload.code !== "OK") {
    renderPaperFeedback(payload.error && payload.error.message ? payload.error.message : "模拟订单提交失败。", true);
    return;
  }
  renderPaperDashboard(payload.data);
  const action = payload.data.last_action || {};
  const feedback =
    action.type === "risk_reject" || action.type === "reject"
      ? `订单被拒绝：${(action.reasons || []).join(", ")}`
      : `模拟订单已提交，当前状态 ${action.status || "-" }，成交 ${action.fill_count || 0} 笔。`;
  renderPaperFeedback(feedback, action.type === "risk_reject" || action.type === "reject");
  await logEvent("paper_submit_order", {
    instrument_id: state.activeInstrumentId,
    side: document.getElementById("paper-order-side").value,
    order_type: orderType,
    quantity,
  });
  await loadActivity();
}

async function submitLearningQuiz() {
  const form = document.getElementById("learning-quiz-form");
  if (!form) {
    return;
  }
  const formData = new FormData(form);
  const answers = {};
  for (const [key, value] of formData.entries()) {
    answers[key] = value;
  }
  const payload = await postJson("/api/learning/quiz", {
    answers,
    learning: { selected_lesson_id: state.selectedLessonId },
  });
  state.lastLearningQuizResult = payload.data.result;
  state.learningProgress = payload.data.progress;
  renderLearningOverview();
  renderLearningQuizResult(payload.data.result);
  await logEvent("submit_learning_quiz", {
    score: payload.data.result.score,
    passed: payload.data.result.passed,
  });
  await loadActivity();
}

async function cancelPaperOrder(orderId) {
  const payload = await postJson("/api/paper/orders/cancel", { order_id: orderId });
  if (payload.code !== "OK") {
    renderPaperFeedback(payload.error && payload.error.message ? payload.error.message : "模拟订单撤单失败。", true);
    return;
  }
  renderPaperDashboard(payload.data);
  renderPaperFeedback(`模拟订单 ${orderId} 已撤销。`);
  await logEvent("paper_cancel_order", { order_id: orderId });
  await loadActivity();
}

async function resetPaperAccount() {
  const payload = await postJson("/api/paper/reset", {});
  if (payload.code !== "OK") {
    renderPaperFeedback(payload.error && payload.error.message ? payload.error.message : "模拟账户重置失败。", true);
    return;
  }
  renderPaperDashboard(payload.data);
  renderPaperFeedback("模拟账户已重置。");
  await logEvent("paper_reset_account", {});
  await loadActivity();
}

async function loadActivity() {
  const payload = await fetchJson("/api/web/events?limit=12");
  renderActivity(payload.data.events);
}

async function loadBotTemplates() {
  const payload = await fetchJson("/api/strategy/templates");
  state.botTemplates = payload.data;
  renderBotTemplates();
}

async function loadBots() {
  const payload = await fetchJson("/api/bots");
  state.bots = payload.data;
  renderBots();
}

async function loadNotifications() {
  const payload = await fetchJson("/api/notifications?limit=12");
  state.notifications = payload.data.notifications || [];
  renderNotifications();
}

async function refreshBotConsole() {
  await loadBots();
  await loadNotifications();
  renderBotInstrumentHint();
}

function collectBotParamUpdates(botId) {
  const updates = {};
  document.querySelectorAll(`[data-bot-param^="${botId}:"]`).forEach((field) => {
    const [, paramName] = field.dataset.botParam.split(":");
    const raw = String(field.value || "").trim();
    if (!raw) {
      return;
    }
    updates[paramName] = field.type === "number" ? Number(raw) : raw;
  });
  return updates;
}

async function createBotFromSelection() {
  if (!state.activeInstrumentId) {
    updateWorkspaceStatus("请先在选股、自选或 F10 页选择一只股票，然后再创建策略机器人。");
    return;
  }
  const templateCode = document.getElementById("bot-template-select").value;
  if (!templateCode) {
    updateWorkspaceStatus("当前没有可用的策略模板。");
    return;
  }
  const botName = document.getElementById("bot-name").value.trim();
  const payload = await postJson("/api/bots", {
    template_code: templateCode,
    instrument_id: state.activeInstrumentId,
    bot_name: botName || undefined,
  });
  if (payload.code !== "OK") {
    updateWorkspaceStatus(payload.error && payload.error.message ? payload.error.message : "策略机器人创建失败。");
    return;
  }
  document.getElementById("bot-name").value = "";
  await refreshBotConsole();
  await logEvent("create_bot", {
    bot_id: payload.data.bot_id,
    bot_name: payload.data.bot_name,
    instrument_id: payload.data.instrument_id,
    template_code: payload.data.template_code,
  });
  await loadActivity();
  updateWorkspaceStatus(`已创建策略机器人：${payload.data.bot_name}`);
}

async function controlBot(botId, action) {
  const path = {
    start: "/api/bots/start",
    pause: "/api/bots/pause",
    stop: "/api/bots/stop",
  }[action];
  if (!path) {
    return;
  }
  const payload = await postJson(path, { bot_id: botId });
  if (payload.code !== "OK") {
    updateWorkspaceStatus(payload.error && payload.error.message ? payload.error.message : "机器人操作失败。");
    return;
  }
  await refreshBotConsole();
  await logEvent("bot_action", { bot_id: botId, action });
  await loadActivity();
}

async function interactBot(botId, command, payload = {}) {
  const response = await postJson("/api/bots/interact", {
    bot_id: botId,
    command,
    payload,
  });
  if (response.code !== "OK") {
    updateWorkspaceStatus(response.error && response.error.message ? response.error.message : "机器人交互失败。");
    return;
  }
  await refreshBotConsole();
  await logEvent("bot_interact", { bot_id: botId, command });
  await loadActivity();
}

async function saveWorkspace() {
  const payload = await postJson("/api/web/state", { state: snapshotWorkspaceState() });
  const updatedAt = payload.data.updated_at ? formatTime(payload.data.updated_at) : "刚刚";
  window.localStorage.setItem("quant_exchange_workspace_state", JSON.stringify(snapshotWorkspaceState()));
  updateWorkspaceStatus(`已自动保存你的筛股、自选和图表设置，最近保存时间：${updatedAt}`);
}

async function logEvent(eventType, payload = {}) {
  await postJson("/api/web/events", {
    event_type: eventType,
    path: window.location.pathname,
    payload,
  });
}

async function loadCompare({ persist = false, eventPayload = null } = {}) {
  if (!state.compare.left || !state.compare.right) {
    renderCompare(null);
    return;
  }
  const compare = await fetchJson(
    `/api/stocks/compare?left=${encodeURIComponent(state.compare.left)}&right=${encodeURIComponent(state.compare.right)}`
  );
  updateStockMap([compare.data.left, compare.data.right]);
  renderCompare(compare.data);
  if (persist) {
    await saveWorkspace();
    if (eventPayload) {
      await logEvent("pick_compare_stock", eventPayload);
    }
    await loadActivity();
  }
}

async function loadKlines(instrumentId) {
  const payload = await fetchJson(
    `/api/stocks/klines?instrument_id=${encodeURIComponent(instrumentId)}&limit=${encodeURIComponent(state.chartRange)}`
  );
  renderChartControls();
  renderKlineChart(payload.data);
}

async function loadCryptoKlines(instrumentId) {
  const payload = await fetchJson(
    `/api/crypto/klines?instrument_id=${encodeURIComponent(instrumentId)}&limit=${encodeURIComponent(state.cryptoChartRange)}`
  );
  renderCryptoChartControls();
  renderCryptoChart(payload.data);
}

async function loadDetail(instrumentId, { persist = false } = {}) {
  const detail = await fetchJson(`/api/stocks/detail?instrument_id=${encodeURIComponent(instrumentId)}`);
  state.activeInstrumentId = instrumentId;
  /* Track recent visits (max 10) */
  state.recentStockVisits = [instrumentId].concat(state.recentStockVisits.filter(function(id) { return id !== instrumentId; })).slice(0, 10);
  updateStockMap([detail.data]);
  if (state.marketSnapshot) {
    const quote = (state.marketSnapshot.quotes || []).find((item) => item.instrument_id === instrumentId);
    if (quote) {
      mergeRealtimeFields(detail.data, quote);
    }
  }
  renderDetail(detail.data);
  await loadKlines(instrumentId);
  await loadPaperDashboard();
  highlightActiveRow();
  renderWatchlist();
  updateQuickTradeBar();
  if (persist) {
    await saveWorkspace();
    await logEvent("view_stock_detail", { instrument_id: instrumentId });
    await loadActivity();
  }
}

function pickPreferredCrypto(assets, preferredInstrumentId) {
  if (!assets.length) {
    return null;
  }
  if (preferredInstrumentId && assets.some((asset) => asset.instrument_id === preferredInstrumentId)) {
    return preferredInstrumentId;
  }
  return assets[0].instrument_id;
}

async function loadCryptoDetail(instrumentId, { persist = false } = {}) {
  const detail = await fetchJson(`/api/crypto/detail?instrument_id=${encodeURIComponent(instrumentId)}`);
  state.activeCryptoInstrumentId = detail.data.instrument_id;
  updateCryptoMap([detail.data]);
  renderCryptoDetail(detail.data);
  renderCryptoList();
  await loadCryptoKlines(detail.data.instrument_id);
  if (persist) {
    await saveWorkspace();
    await logEvent("view_crypto_detail", { instrument_id: detail.data.instrument_id });
    await loadActivity();
  }
}

function compareButtons(instrumentId) {
  return `
    <button class="pick-button" type="button" data-compare-side="left" data-instrument-id="${instrumentId}">左侧</button>
    <button class="pick-button" type="button" data-compare-side="right" data-instrument-id="${instrumentId}">右侧</button>
  `;
}

function highlightActiveRow() {
  document.querySelectorAll("tr[data-instrument-id]").forEach((row) => {
    row.classList.toggle("is-active", row.dataset.instrumentId === state.activeInstrumentId);
  });
}

function renderTable(stocks) {
  state.currentStocks = stocks;
  updateStockMap(stocks);
  const body = document.getElementById("stock-table-body");
  body.innerHTML = stocks
    .map(
      (stock) => `
        <tr data-instrument-id="${stock.instrument_id}" class="${state.activeInstrumentId === stock.instrument_id ? "is-active" : ""}">
          <td>${watchToggleButton(stock.instrument_id)}</td>
          <td>
            <div class="table-symbol">
              <strong>${stock.symbol}</strong>
              <span>${stock.exchange_code}</span>
            </div>
          </td>
          <td>${stock.company_name}</td>
          <td>${stock.market_region}</td>
          <td>${formatNumber(stock.last_price)}</td>
          <td class="${metricClass(stock.change_pct)}">${stock.change_pct != null ? formatNumber(stock.change_pct) + "%" : "-"}</td>
          <td class="${metricClass(stock.amplitude)}">${stock.amplitude != null ? formatNumber(stock.amplitude) + "%" : "-"}</td>
          <td>${formatLargeNumber(stock.volume)}</td>
          <td>${formatLargeNumber(stock.turnover)}</td>
          <td>${formatLargeNumber(stock.market_cap)}</td>
          <td>${stock.sector}</td>
          <td>${formatNumber(stock.pe_ttm)}</td>
          <td>${formatNumber(stock.pb)}</td>
          <td class="${metricClass(stock.roe)}">${stock.roe != null ? formatNumber(stock.roe) + "%" : "-"}</td>
          <td class="${metricClass(stock.revenue_growth)}">${stock.revenue_growth != null ? formatNumber(stock.revenue_growth) + "%" : "-"}</td>
          <td class="${metricClass(stock.net_profit_growth)}">${stock.net_profit_growth != null ? formatNumber(stock.net_profit_growth) + "%" : "-"}</td>
          <td>${stock.gross_margin != null ? formatNumber(stock.gross_margin) + "%" : "-"}</td>
          <td>${stock.net_margin != null ? formatNumber(stock.net_margin) + "%" : "-"}</td>
          <td>${stock.dividend_yield != null ? formatNumber(stock.dividend_yield) + "%" : "-"}</td>
          <td>${stock.debt_to_asset != null ? formatNumber(stock.debt_to_asset) + "%" : "-"}</td>
          <td>${compareButtons(stock.instrument_id)}</td>
        </tr>
      `
    )
    .join("");
  const realtimeNote = state.marketSnapshot ? ` · 行情时间 ${formatTime(state.marketSnapshot.as_of)}` : "";
  const hiddenCount = Math.max(Number(state.stockResultCount || 0) - stocks.length, 0);
  const visibilityNote =
    hiddenCount > 0 ? `，当前仅展示前 ${stocks.length} 只，仍有 ${hiddenCount} 只可继续缩小条件后查看` : `，当前展示全部 ${stocks.length} 只`;
  document.getElementById("result-summary").textContent = `当前股票池命中 ${state.stockResultCount} 只股票${visibilityNote}${realtimeNote}`;
  updateSortIndicators();
}

function pickPreferredInstrument(stocks, preferredInstrumentId) {
  if (!stocks.length) {
    return null;
  }
  if (preferredInstrumentId && stocks.some((stock) => stock.instrument_id === preferredInstrumentId)) {
    return preferredInstrumentId;
  }
  if (state.watchlist.length) {
    const watchHit = state.watchlist.find((instrumentId) => stocks.some((stock) => stock.instrument_id === instrumentId));
    if (watchHit) {
      return watchHit;
    }
  }
  return stocks[0].instrument_id;
}

async function loadStocks({ persist = false, eventType = null, preferredInstrumentId = null } = {}) {
  const filters = collectFilters();
  const listFilters = { ...filters, limit: String(DEFAULT_STOCK_LIST_LIMIT) };
  const [payload, countPayload] = await Promise.all([
    fetchJson(`/api/stocks${buildQuery(listFilters) ? `?${buildQuery(listFilters)}` : ""}`),
    fetchJson(`/api/stocks/count${buildQuery(filters) ? `?${buildQuery(filters)}` : ""}`),
  ]);
  const stocks = payload.data;
  state.stockResultCount = Number((countPayload.data || {}).count || stocks.length);
  renderTable(stocks);

  if (!stocks.length) {
    state.activeInstrumentId = null;
    renderEmptyDetail("当前筛选条件下没有匹配股票。");
    renderEmptyHistory("kline-card", "chart-status", "当前筛选条件下没有匹配股票，因此没有可展示的 K 线图。", "点击股票后加载最近一段日线走势");
  } else {
    const selectedInstrumentId = pickPreferredInstrument(stocks, preferredInstrumentId || state.activeInstrumentId);
    await loadDetail(selectedInstrumentId, { persist: false });
  }

  renderWatchlist();
  renderCompareTray();
  if (state.marketSnapshot) {
    applyRealtimeSnapshot(state.marketSnapshot);
  }
  if (persist) {
    await saveWorkspace();
    if (eventType) {
      await logEvent(eventType, {
        filters: collectFilters(),
        result_count: state.stockResultCount,
        visible_count: stocks.length,
        preset: state.activePreset,
      });
    }
    await loadActivity();
  }
}

async function loadCryptoUniverse({ preferredInstrumentId = null, persist = false, silent = false } = {}) {
  const [summaryPayload, assetsPayload] = await Promise.all([
    fetchJson("/api/crypto/universe?featured_limit=5"),
    fetchJson("/api/crypto/assets"),
  ]);
  state.cryptoSummary = summaryPayload.data;
  state.cryptoAssets = assetsPayload.data || [];
  updateCryptoMap(state.cryptoAssets);
  renderCryptoOverview(state.cryptoSummary);
  renderCryptoList();
  renderCryptoChartControls();
  if (!state.cryptoAssets.length) {
    state.activeCryptoInstrumentId = null;
    renderEmptyCryptoDetail("当前还没有可展示的加密货币。");
    renderEmptyHistory("crypto-kline-card", "crypto-chart-status", "当前没有可展示的历史走势。", "等待加密货币数据加载");
    return;
  }
  const selectedInstrumentId = pickPreferredCrypto(state.cryptoAssets, preferredInstrumentId || state.activeCryptoInstrumentId);
  if (selectedInstrumentId) {
    await loadCryptoDetail(selectedInstrumentId, { persist: false });
  }
  if (persist && !silent) {
    await saveWorkspace();
  }
}

async function loadUniverseSnapshot() {
  const payload = await fetchJson("/api/stocks/universe?featured_limit=48");
  state.universeSummary = payload.data;
  state.universe = payload.data.featured_stocks || [];
  updateStockMap(state.universe);
  document.getElementById("stock-count").textContent = String(payload.data.total_count || state.universe.length);
  renderMarketPulse();
  renderWatchlist();
}

async function initializeFilters() {
  const payload = await fetchJson("/api/stock/options");
  populateSelect("market_region", payload.data.market_regions);
  populateSelect("exchange_code", payload.data.exchange_codes);
  populateSelect("board", payload.data.boards);
  populateSelect("sector", payload.data.sectors);
  populateSelect("industry", payload.data.industries);
  populateSelect("concept", payload.data.concepts);
}

async function loadWorkspaceState() {
  try {
    const payload = await fetchJson("/api/web/state");
    return payload.data;
  } catch (error) {
    const raw = window.localStorage.getItem("quant_exchange_workspace_state");
    return {
      client_id: state.clientId,
      workspace_code: "stock_screener",
      state: raw
        ? JSON.parse(raw)
        : {
            filters: {},
            compare: { left: null, right: null },
            active_instrument_id: null,
            watchlist: [],
            chart: { range: 120, mode: "candles" },
            crypto: {
              active_instrument_id: "BTCUSDT",
              chart: { range: 120, mode: "candles" },
            },
            preset: null,
            active_tab: DEFAULT_TAB,
            learning: { selected_lesson_id: null },
          },
      updated_at: null,
    };
  }
}

async function loadCurrentUser() {
  if (!state.authToken) {
    state.currentUser = null;
    renderAuthState();
    return null;
  }
  try {
    const payload = await fetchJson("/api/auth/current");
    state.currentUser = payload.data && payload.data.authenticated ? payload.data.user : null;
    if (!state.currentUser) {
      storeAuthToken(null);
    }
  } catch (error) {
    state.currentUser = null;
    storeAuthToken(null);
  }
  renderAuthState();
  return state.currentUser;
}

async function authenticateUser(mode) {
  const usernameField = document.getElementById("auth-username");
  const passwordField = document.getElementById("auth-password");
  const displayNameField = document.getElementById("auth-display-name");
  const username = usernameField.value.trim();
  const password = passwordField.value;
  if (!username || !password) {
    document.getElementById("auth-summary").textContent = "请输入用户名和密码。";
    return;
  }
  const path = mode === "register" ? "/api/auth/register" : "/api/auth/login";
  try {
    const payload = await postJson(path, {
      username,
      password,
      display_name: displayNameField.value.trim() || undefined,
    });
    if (payload.code !== "OK") {
      document.getElementById("auth-summary").textContent = payload.error?.message || "登录失败。";
      return;
    }
    storeAuthToken(payload.data.access_token);
    state.currentUser = payload.data.user;
    renderAuthState();
    await reloadUserScopedWorkspace(mode === "register" ? "注册成功，已切换到你的独立空间。" : "登录成功，已加载该用户的独立空间。");
  } catch (error) {
    document.getElementById("auth-summary").textContent = mode === "register" ? "注册失败，请稍后重试。" : "登录失败，请稍后重试。";
  }
}

async function logoutCurrentUser() {
  try {
    if (state.authToken) {
      await postJson("/api/auth/logout", {});
    }
  } catch (error) {
    // Ignore logout transport failures and clear local session anyway.
  }
  storeAuthToken(null);
  state.currentUser = null;
  renderAuthState();
  await reloadUserScopedWorkspace("已退出登录，当前回到浏览器本地工作空间。");
}

function applyWorkspaceState(workspace) {
  const saved = workspace && workspace.state ? workspace.state : {};
  applyFilters(saved.filters || {});
  state.compare.left = saved.compare && saved.compare.left ? saved.compare.left : null;
  state.compare.right = saved.compare && saved.compare.right ? saved.compare.right : null;
  state.activeInstrumentId = saved.active_instrument_id || null;
  state.watchlist = Array.isArray(saved.watchlist) ? saved.watchlist : [];
  state.sortBy = ((saved.sort || {}).sort_by) || "symbol";
  state.sortDesc = !!((saved.sort || {}).sort_desc);
  state.chartRange = Number((saved.chart || {}).range || 120);
  state.chartMode = (saved.chart || {}).mode || "candles";
  state.activeCryptoInstrumentId = ((saved.crypto || {}).active_instrument_id) || "BTCUSDT";
  state.cryptoChartRange = Number((((saved.crypto || {}).chart || {}).range) || 120);
  state.cryptoChartMode = (((saved.crypto || {}).chart || {}).mode) || "candles";
  state.activePreset = saved.preset || null;
  state.activeTab = normalizeTab(saved.active_tab || DEFAULT_TAB);
  state.selectedLessonId = (saved.learning || {}).selected_lesson_id || null;
  state.learningSearchQuery = (saved.learning || {}).search_query || "";
  renderPresetButtons();
  renderChartControls();
  renderCryptoChartControls();
  renderFuturesChartControls();
  renderTabs();
}

async function reloadUserScopedWorkspace(message) {
  const workspace = await loadWorkspaceState();
  applyWorkspaceState(workspace);
  await loadLearningHub();
  if (!state.selectedLessonId) {
    state.selectedLessonId = (state.learningProgress || {}).current_lesson_id || null;
  }
  renderLearningLessonNav();
  renderLearningContent();
  renderLearningQuizResult(null);
  await loadStocks({ preferredInstrumentId: state.activeInstrumentId });
  await loadCryptoUniverse({ preferredInstrumentId: state.activeCryptoInstrumentId });
  if (state.compare.left && state.compare.right) {
    await loadCompare();
  } else {
    renderCompare(null);
  }
  await loadPaperDashboard();
  await refreshBotConsole();
  await loadActivity();
  updateWorkspaceStatus(message);
}

async function toggleWatchlist(instrumentId) {
  const existing = state.watchlist.includes(instrumentId);
  state.watchlist = existing
    ? state.watchlist.filter((item) => item !== instrumentId)
    : [...state.watchlist, instrumentId];
  renderWatchlist();
  renderTable(state.currentStocks);
  renderDetailIfActiveWatchButton(instrumentId);
  await saveWorkspace();
  await logEvent("toggle_watchlist", {
    action: existing ? "removed" : "added",
    instrument_id: instrumentId,
  });
  await loadActivity();
}

function renderDetailIfActiveWatchButton(instrumentId) {
  if (state.activeInstrumentId !== instrumentId) {
    return;
  }
  const stock = state.stockMap[instrumentId];
  if (stock) {
    renderDetail(stock);
  }
}

async function applyPreset(presetKey) {
  const preset = PRESETS[presetKey];
  if (!preset) {
    return;
  }
  state.activePreset = presetKey;
  applyFilters(preset.filters);
  renderPresetButtons();
  await loadStocks({ persist: true, eventType: "submit_filters" });
  await logEvent("apply_preset", { preset: preset.label });
  await loadActivity();
}

document.addEventListener("click", async (event) => {
  const sortHeader = event.target.closest("th[data-sort-key]");
  if (sortHeader) {
    handleSortClick(sortHeader.getAttribute("data-sort-key"));
    return;
  }

  const tabButton = event.target.closest("[data-tab]");
  if (tabButton) {
    await setActiveTab(tabButton.dataset.tab, { persist: true, log: true });
    return;
  }

  const downloadAction = event.target.closest("[data-download-action]");
  if (downloadAction) {
    await setActiveTab("downloads");
    await controlDownloadJob(downloadAction.dataset.downloadAction, downloadAction.dataset.jobId);
    return;
  }

  const watchToggle = event.target.closest("[data-watch-toggle]");
  if (watchToggle) {
    await toggleWatchlist(watchToggle.dataset.watchToggle);
    return;
  }

  const watchOpen = event.target.closest("[data-watch-open]");
  if (watchOpen) {
    await loadDetail(watchOpen.dataset.watchOpen, { persist: true });
    await setActiveTab("research", { persist: true, log: true });
    return;
  }

  const presetButton = event.target.closest("[data-preset]");
  if (presetButton) {
    await setActiveTab("screener");
    await applyPreset(presetButton.dataset.preset);
    return;
  }

  const rangeButton = event.target.closest("[data-chart-range]");
  if (rangeButton) {
    state.chartRange = Number(rangeButton.dataset.chartRange);
    renderChartControls();
    if (state.activeInstrumentId) {
      await loadKlines(state.activeInstrumentId);
      await setActiveTab("research");
      await saveWorkspace();
      await logEvent("change_chart_view", { range: state.chartRange, mode: state.chartMode });
      await loadActivity();
    }
    return;
  }

  const modeButton = event.target.closest("[data-chart-mode]");
  if (modeButton) {
    state.chartMode = modeButton.dataset.chartMode;
    renderChartControls();
    if (state.activeInstrumentId) {
      await loadKlines(state.activeInstrumentId);
      await setActiveTab("research");
      await saveWorkspace();
      await logEvent("change_chart_view", { range: state.chartRange, mode: state.chartMode });
      await loadActivity();
    }
    return;
  }

  const cryptoRangeButton = event.target.closest("[data-crypto-chart-range]");
  if (cryptoRangeButton) {
    state.cryptoChartRange = Number(cryptoRangeButton.dataset.cryptoChartRange);
    renderCryptoChartControls();
    if (state.activeCryptoInstrumentId) {
      await loadCryptoKlines(state.activeCryptoInstrumentId);
      await setActiveTab("crypto");
      await saveWorkspace();
      await logEvent("change_crypto_chart_view", { range: state.cryptoChartRange, mode: state.cryptoChartMode });
      await loadActivity();
    }
    return;
  }

  const cryptoModeButton = event.target.closest("[data-crypto-chart-mode]");
  if (cryptoModeButton) {
    state.cryptoChartMode = cryptoModeButton.dataset.cryptoChartMode;
    renderCryptoChartControls();
    if (state.activeCryptoInstrumentId) {
      await loadCryptoKlines(state.activeCryptoInstrumentId);
      await setActiveTab("crypto");
      await saveWorkspace();
      await logEvent("change_crypto_chart_view", { range: state.cryptoChartRange, mode: state.cryptoChartMode });
      await loadActivity();
    }
    return;
  }

  // Futures click handlers
  var futuresId = event.target.closest("[data-futures-id]");
  if (futuresId) {
    loadFuturesDetail(futuresId.dataset.futuresId);
    return;
  }
  var futuresOpen = event.target.closest("[data-futures-open]");
  if (futuresOpen) {
    loadFuturesDetail(futuresOpen.dataset.futuresOpen);
    return;
  }
  var futuresRange = event.target.closest("[data-futures-chart-range]");
  if (futuresRange) {
    state.futuresChartRange = Number(futuresRange.dataset.futuresChartRange);
    renderFuturesChartControls();
    if (state.activeFuturesInstrumentId) loadFuturesDetail(state.activeFuturesInstrumentId);
    return;
  }
  var futuresMode = event.target.closest("[data-futures-chart-mode]");
  if (futuresMode) {
    state.futuresChartMode = futuresMode.dataset.futuresChartMode;
    renderFuturesChartControls();
    if (state.activeFuturesInstrumentId) loadFuturesDetail(state.activeFuturesInstrumentId);
    return;
  }

  /* F10 sub-tab clicks */
  const subTabBtn = event.target.closest("[data-subtab]");
  if (subTabBtn) {
    document.querySelectorAll("#stock-sub-tabs .sub-tab-btn").forEach(function(b) { b.classList.toggle("is-active", b === subTabBtn); });
    renderStockSubTab(subTabBtn.dataset.subtab);
    return;
  }

  /* Recent stock item clicks */
  const recentItem = event.target.closest(".recent-stock-item[data-instrument-id]");
  if (recentItem) {
    await loadDetail(recentItem.dataset.instrumentId, { persist: true });
    return;
  }

  const compareButton = event.target.closest("[data-compare-side]");
  if (compareButton) {
    state.compare[compareButton.dataset.compareSide] = compareButton.dataset.instrumentId;
    await loadCompare({
      persist: true,
      eventPayload: {
        side: compareButton.dataset.compareSide,
        instrument_id: compareButton.dataset.instrumentId,
      },
    });
    await setActiveTab("compare", { persist: true, log: true });
    return;
  }

  const cryptoOpen = event.target.closest("[data-crypto-open]");
  if (cryptoOpen) {
    await loadCryptoDetail(cryptoOpen.dataset.cryptoOpen, { persist: true });
    await setActiveTab("crypto", { persist: true, log: true });
    return;
  }

  const lessonButton = event.target.closest("[data-learning-lesson]");
  if (lessonButton) {
    state.selectedLessonId = lessonButton.dataset.learningLesson;
    renderLearningLessonNav();
    renderLearningContent();
    await setActiveTab("learning");
    await saveWorkspace();
    await logEvent("open_learning_lesson", { lesson_id: state.selectedLessonId });
    await loadActivity();
    return;
  }

  const cancelPaper = event.target.closest("[data-paper-cancel-order]");
  if (cancelPaper) {
    await setActiveTab("paper");
    await cancelPaperOrder(cancelPaper.dataset.paperCancelOrder);
    return;
  }

  const botAction = event.target.closest("[data-bot-action]");
  if (botAction) {
    await setActiveTab("activity");
    await controlBot(botAction.dataset.botId, botAction.dataset.botAction);
    return;
  }

  const botCommand = event.target.closest("[data-bot-command]");
  if (botCommand) {
    await setActiveTab("activity");
    await interactBot(botCommand.dataset.botId, botCommand.dataset.botCommand);
    return;
  }

  const saveParams = event.target.closest("[data-bot-save-params]");
  if (saveParams) {
    await setActiveTab("activity");
    await interactBot(saveParams.dataset.botSaveParams, "set_param", {
      updates: collectBotParamUpdates(saveParams.dataset.botSaveParams),
    });
    return;
  }

  const row = event.target.closest("tr[data-instrument-id]");
  if (row) {
    await loadDetail(row.dataset.instrumentId, { persist: true });
    await setActiveTab("research", { persist: true, log: true });
  }
});

document.getElementById("filters-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  state.activePreset = null;
  renderPresetButtons();
  await setActiveTab("screener");
  await loadStocks({ persist: true, eventType: "submit_filters" });
});

document.getElementById("reset-filters").addEventListener("click", async () => {
  document.getElementById("filters-form").reset();
  state.activePreset = null;
  state.sortBy = "symbol";
  state.sortDesc = false;
  renderPresetButtons();
  updateSortIndicators();
  await setActiveTab("screener");
  await loadStocks({ persist: true, eventType: "reset_filters" });
});

document.getElementById("refresh-download-jobs").addEventListener("click", async () => {
  await setActiveTab("downloads");
  await loadDownloadJobs();
});

document.getElementById("paper-refresh").addEventListener("click", async () => {
  await setActiveTab("paper");
  await loadPaperDashboard();
});

document.getElementById("paper-reset").addEventListener("click", async () => {
  await setActiveTab("paper");
  await resetPaperAccount();
});

document.getElementById("paper-order-type").addEventListener("change", () => {
  renderPaperOrderTypeState();
});

document.getElementById("paper-order-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  await setActiveTab("paper");
  await submitPaperOrder();
});

document.getElementById("learning-quiz-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  await setActiveTab("learning");
  await submitLearningQuiz();
});

document.getElementById("learning-knowledge-search").addEventListener("input", () => {
  state.learningSearchQuery = document.getElementById("learning-knowledge-search").value.trim();
  renderLearningKnowledgeBase();
});

document.getElementById("learning-knowledge-search").addEventListener("change", async () => {
  state.learningSearchQuery = document.getElementById("learning-knowledge-search").value.trim();
  await setActiveTab("learning");
  await saveWorkspace();
});

document.getElementById("auth-login").addEventListener("click", async () => {
  await authenticateUser("login");
});

document.getElementById("auth-register").addEventListener("click", async () => {
  await authenticateUser("register");
});

document.getElementById("auth-logout").addEventListener("click", async () => {
  await logoutCurrentUser();
});

document.getElementById("bot-create-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  await setActiveTab("activity");
  await createBotFromSelection();
});

window.addEventListener("DOMContentLoaded", async () => {
  ensureClientId();
  loadAuthToken();
  document.getElementById("workspace-url").innerHTML = `访问地址：<a href="${window.location.origin}/">${window.location.origin}/</a>`;
  renderPresetButtons();
  renderChartControls();
  renderCryptoChartControls();
  renderFuturesChartControls();
  renderPaperOrderTypeState();
  renderTabs();
  renderAuthState();
  await loadCurrentUser();
  await initializeFilters();
  await loadLearningHub();
  await loadBotTemplates();
  await refreshBotConsole();
  await loadDownloadJobs();
  await loadUniverseSnapshot();
  const workspace = await loadWorkspaceState();
  applyWorkspaceState(workspace);
  if (window.location.pathname === "/learn") {
    state.activeTab = "learning";
    renderTabs();
  }
  renderBotInstrumentHint();
  renderPaperInstrumentHint();
  if (workspace.updated_at) {
    updateWorkspaceStatus(`已恢复你上次保存的筛股、自选和图表设置，最近保存时间：${formatTime(workspace.updated_at)}`);
  } else {
    updateWorkspaceStatus("这是首次打开工作台，后续你的筛股、自选和图表设置会自动保存。");
  }
  await loadStocks({ preferredInstrumentId: state.activeInstrumentId });
  await loadCryptoUniverse({ preferredInstrumentId: state.activeCryptoInstrumentId });
  if (state.compare.left && state.compare.right) {
    await loadCompare();
  } else {
    renderCompare(null);
  }
  await loadRealtimeSnapshot();
  await loadLearningHub();
  await loadPaperDashboardSafely({ silent: true });
  await refreshBotConsole();
  setupQuickTrade();
  updateQuickTradeBar();
  renderRiskDashboard();
  scheduleRealtimePolling();
  scheduleDownloadPolling();
  await logEvent("open_page", { path: window.location.pathname });
  await loadActivity();
});
