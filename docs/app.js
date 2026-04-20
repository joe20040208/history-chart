// History Chart dashboard app — static GitHub Pages version
const state = {
  all: [],
  view: [],
  selected: null,
  sort: { key: "pct_gain", dir: -1 },
  hasPerf3m: false,
  hasPerf6m: false,
  minAdrPct: 0,
  excludeSectors: [],
  excludeIndustries: [],
};

const $ = (s) => document.querySelector(s);
const fmtMcap = (v) => v ? "$" + (v / 1e9).toFixed(2) + "B" : "—";
const fmtVol = (v) => v ? "$" + (v / 1e6).toFixed(1) + "M" : "—";
const fmtPct = (v) => v == null ? "—" : v.toFixed(1) + "%";

// ──────────────── data load ────────────────
async function loadRunners() {
  try {
    const res = await fetch("runners.json");
    if (!res.ok) throw new Error(await res.text());
    const raw = await res.json();
    state.all = raw.filter(r =>
      r.pct_gain <= 3000
      && r.start_price >= 1.0
      && r.days_to_peak >= 5
    );
    state.hasPerf3m = state.all.length > 0 && state.all[0].pre_perf_3m !== undefined;
    state.hasPerf6m = state.all.length > 0 && state.all[0].pre_perf_6m !== undefined;
    $("#summary").textContent = `${state.all.length.toLocaleString()} historical events loaded`;
    applyFilters();
  } catch (e) {
    console.error("loadRunners error:", e);
    $("#summary").textContent = "Error: " + e.message;
    $("#summary").style.color = "var(--red)";
  }
}

// ──────────────── filtering ────────────────
function activeSet(selector, attr) {
  return new Set([...document.querySelectorAll(selector + ".on")].map(b => b.dataset[attr]));
}

function applyFilters() {
  const q = ($("#searchBox").value || "").trim().toUpperCase();
  const parseNum = (val, fallback = 0) => {
    const n = +val;
    return isNaN(n) ? fallback : n;
  };
  const minPct      = parseNum($("#pctSlider").value, 0);
  const minMcap     = parseNum($("#mcapMinSlider").value, 0) * 1e9;
  const maxMcapRaw  = parseNum($("#mcapMaxSlider").value, 0);
  const maxMcap     = (maxMcapRaw === 0 ? Infinity : maxMcapRaw * 1e9);
  const minShareVol = parseNum($("#shareVolSlider").value, 0) * 1e3;
  const minPrice    = parseNum($("#priceSlider").value, 0);
  const minPerf3m   = $("#perf3mInput").value.trim() === "" ? null : parseNum($("#perf3mInput").value, null);
  const minPerf6m   = $("#perf6mInput").value.trim() === "" ? null : parseNum($("#perf6mInput").value, null);
  const countries   = activeSet("#countryChips .chip", "c");
  const setups      = activeSet("#setupChips .chip", "s");
  const exchanges   = activeSet("#exchangeChips .chip", "e");

  state.view = state.all.filter(r =>
    r.pct_gain >= minPct
    && (minMcap === 0 || (r.start_mcap_usd != null && r.start_mcap_usd >= minMcap))
    && (maxMcap === Infinity || (r.start_mcap_usd != null && r.start_mcap_usd <= maxMcap))
    && (minShareVol === 0 || (r.avg_vol_30d_shares != null && r.avg_vol_30d_shares >= minShareVol))
    && (r.start_price ?? 0) >= minPrice
    && countries.has(r.country)
    && setups.has(r.setup_tag || "none")
    && exchanges.has(r.sub_exchange || "—")
    && (!q || r.ticker.toUpperCase().includes(q) || (r.name || "").toUpperCase().includes(q))
    && (minPerf3m === null || state.hasPerf3m === false || (r.pre_perf_3m != null && r.pre_perf_3m >= minPerf3m))
    && (minPerf6m === null || state.hasPerf6m === false || (r.pre_perf_6m != null && r.pre_perf_6m >= minPerf6m))
    && (state.minAdrPct === 0 || (r.pre_atr_pct != null && r.pre_atr_pct >= state.minAdrPct))
    && (state.excludeSectors.length === 0 || !state.excludeSectors.some(
         s => (r.sector || "").toLowerCase() === s.toLowerCase()))
    && (state.excludeIndustries.length === 0 || !state.excludeIndustries.some(
         s => (r.industry || "").toLowerCase() === s.toLowerCase()))
  );
  sortView();
  renderTable();
  $("#summary").textContent =
    `${state.view.length.toLocaleString()} / ${state.all.length.toLocaleString()} match`;
}

function sortView() {
  const { key, dir } = state.sort;
  state.view.sort((a, b) => {
    let x = a[key], y = b[key];
    if (x == null) return 1;
    if (y == null) return -1;
    if (typeof x === "string") return dir * x.localeCompare(y);
    return dir * (x - y);
  });
}

// ──────────────── table ────────────────
function renderTable() {
  const tbody = $("#tbl tbody");
  const rows = state.view.slice(0, 2000);
  tbody.innerHTML = rows.map((r, i) => `
    <tr data-i="${i}" class="${state.selected === r ? "active" : ""}">
      <td>${r.ticker}</td>
      <td>${r.country}</td>
      <td>${r.start_date}</td>
      <td class="pos">+${r.pct_gain.toFixed(0)}%</td>
      <td>${r.days_to_peak}</td>
      <td>${fmtMcap(r.start_mcap_usd)}</td>
      <td class="tag">${r.setup_tag || "—"}</td>
    </tr>
  `).join("");
  tbody.onclick = (e) => {
    const tr = e.target.closest("tr"); if (!tr) return;
    selectRow(+tr.dataset.i);
  };
}

// ──────────────── TradingView chart ────────────────
function tvSymbol(r) {
  const map = {
    "NASDAQ": "", "NYSE": "", "NYSE MKT": "", "NYSE ARCA": "", "AMEX": "", "BATS": "",
    "TW":  "TWSE:",
    "TWO": "TPEX:",
    "HK":  "HKEX:",
    "KO":  "KRX:",
    "KQ":  "KRX:",
  };
  const prefix = map[r.sub_exchange] ?? "";
  return `${prefix}${r.ticker}`;
}

function renderChart(r) {
  const container = $("#chart-container");
  container.innerHTML = "";

  // TradingView's free embed widget is licensed for US exchanges only —
  // KRX/TWSE/TPEX/HKEX symbols load but display "only available on TradingView".
  // Show a link button instead so the user can open the full chart on tradingview.com.
  if (r.country !== "US") {
    const url = `https://www.tradingview.com/chart/?symbol=${tvSymbol(r)}`;
    container.innerHTML = `
      <div style="display:flex;flex-direction:column;align-items:center;justify-content:center;height:100%;gap:16px;color:var(--muted)">
        <div style="font-size:14px">TradingView 嵌入圖表不支援非美國市場</div>
        <a href="${url}" target="_blank" style="background:var(--accent);color:#fff;padding:10px 24px;border-radius:6px;text-decoration:none;font-size:14px">
          在 TradingView 開啟 ${tvSymbol(r)} ↗
        </a>
      </div>`;
    return;
  }

  const widget = document.createElement("div");
  widget.id = "tv-widget";
  widget.style.width = "100%";
  widget.style.height = "100%";
  container.appendChild(widget);

  new TradingView.widget({
    container_id: "tv-widget",
    symbol: tvSymbol(r),
    interval: "D",
    timezone: "Etc/UTC",
    theme: "dark",
    style: "1",
    locale: "en",
    hide_top_toolbar: false,
    hide_legend: false,
    save_image: false,
    allow_symbol_change: true,
    width: "100%",
    height: "100%",
    autosize: true,
  });
}

// ──────────────── selection / insights ────────────────
function selectRow(i) {
  const r = state.view[i]; if (!r) return;
  state.selected = r;
  [...$("#tbl tbody").children].forEach((tr, k) =>
    tr.classList.toggle("active", k === i));
  $("#tbl tbody").children[i]?.scrollIntoView({ block: "nearest" });

  $("#chart-title").textContent = `${r.ticker}.${r.exchange} — ${r.name || ""}`;
  $("#chart-meta").textContent =
    `${r.start_date} → ${r.peak_date} · +${r.pct_gain.toFixed(0)}% in ${r.days_to_peak}d · ${r.sector || "—"}`;
  $("#tv-link").href = `https://www.tradingview.com/chart/?symbol=${tvSymbol(r)}`;

  renderChart(r);
  renderInsights(r);
}

function renderInsights(r) {
  const items = [
    ["Start price", r.start_price],
    ["Peak price", r.peak_price],
    ["Days to peak", r.days_to_peak],
    ["Mcap at start", fmtMcap(r.start_mcap_usd)],
    ["Avg 30d $vol", fmtVol(r.start_dollar_vol_30d_usd)],
    ["Post-90d return", fmtPct(r.post_90d_return)],
    ["Perf 3M (pre)", r.pre_perf_3m != null ? r.pre_perf_3m.toFixed(1) + "%" : "—"],
    ["Perf 6M (pre)", r.pre_perf_6m != null ? r.pre_perf_6m.toFixed(1) + "%" : "—"],
    ["52w-high ratio (pre)", r.pre_52w_high ?? "—"],
    ["Consolidation days", r.pre_consolidation_days],
    ["ATR% (pre 20d)", r.pre_atr_pct != null ? (r.pre_atr_pct * 100).toFixed(2) + "%" : "—"],
    ["Range% (pre 60d)", r.pre_range_pct != null ? (r.pre_range_pct * 100).toFixed(1) + "%" : "—"],
    ["Vol contraction", r.pre_vol_contraction ?? "—"],
    ["Weinstein stage", r.stage],
    ["Setup tag", r.setup_tag],
    ["Sector", r.sector || "—"],
    ["Delisted", r.delisted ? "yes" : "no"],
  ];
  const commentary = buildCommentary(r);
  $("#insights").innerHTML = `
    <h3>Setup fingerprint</h3>
    <div class="kv">${items.map(([k, v]) => `<div><span>${k}</span><span>${v}</span></div>`).join("")}</div>
    <h3 style="margin-top:10px">Reading</h3>
    <div>${commentary}</div>
  `;
}

function buildCommentary(r) {
  const lines = [];
  if (r.setup_tag === "vcp")
    lines.push("Classic VCP: tight range with drying volume before the break — low-risk entry usually at the pivot high of the last contraction.");
  if (r.setup_tag === "flat_base")
    lines.push("Flat base near 52w highs — ideal entry is the top of the box on volume expansion.");
  if (r.setup_tag === "ipo_base")
    lines.push("Early IPO base — short trading history means high conviction once it clears the IPO high, smaller position sizing.");
  if (r.setup_tag === "stage2_breakout")
    lines.push("Stage-2 breakout over 30w EMA, tight ATR — Weinstein-style low-risk add.");
  if (r.setup_tag === "power_trend")
    lines.push("Already in a power trend — entries are pullbacks to 10/21 EMA, not fresh bases.");
  if (r.setup_tag === "pocket_pivot")
    lines.push("Pocket pivot volume signature inside the base — buy before the breakout for lower-risk entry.");
  if (r.setup_tag === "none")
    lines.push("No textbook base detected. Check: was there a specific catalyst (earnings, FDA, M&A)? These are often un-chartable and higher-risk.");
  if ((r.pre_52w_high ?? 0) < 0.75)
    lines.push("Started well off 52w highs — expect base-on-base dynamics; the second leg tends to be where the real money moves.");
  if ((r.post_90d_return ?? 0) < -30)
    lines.push("Gave back more than 30% in 90 days post-peak — study the topping structure for sell-signal practice.");
  return lines.join(" ") || "—";
}

// ──────────────── wiring ────────────────
function wire() {
  const bind = (id) => $(id).addEventListener("input", applyFilters);
  $("#searchBox").addEventListener("input", applyFilters);
  bind("#pctSlider");
  bind("#mcapMinSlider");
  bind("#mcapMaxSlider");
  bind("#shareVolSlider");
  bind("#priceSlider");
  bind("#perf3mInput");
  bind("#perf6mInput");

  document.querySelectorAll(".chip").forEach(c =>
    c.addEventListener("click", () => { c.classList.toggle("on"); applyFilters(); }));

  const PRESETS = {
    usfilter: {
      pct: 50, price: 5, shareVol: 500,
      mcapMin: 0.5, mcapMax: 20,
      exchanges: ["NASDAQ", "NYSE"],
      countries: ["US"],
      minAdrPct: 0.05,
      excludeSectors: ["Health Services", "Health Technology"],
      excludeIndustries: ["Biotechnology"],
      perf3m: 30, perf6m: 50,
    },
    twfilter: {
      pct: 50, price: 150, shareVol: 500,   // TWD ~$5 USD
      mcapMin: 0, mcapMax: 0,
      exchanges: ["TW", "TWO"],
      countries: ["TW"],
      minAdrPct: 0,
      excludeSectors: [],
      perf3m: 30, perf6m: 50,
    },
    krfilter: {
      pct: 50, price: 7000, shareVol: 500,  // KRW ~$5 USD
      mcapMin: 0, mcapMax: 0,
      exchanges: ["KO", "KQ"],
      countries: ["KR"],
      minAdrPct: 0,
      excludeSectors: [],
      perf3m: 30, perf6m: 50,
    },
    hkfilter: {
      pct: 50, price: 40, shareVol: 500,    // HKD ~$5 USD
      mcapMin: 0, mcapMax: 0,
      exchanges: ["HK"],
      countries: ["HK"],
      minAdrPct: 0,
      excludeSectors: [],
      perf3m: 30, perf6m: 50,
    },
  };

  function applyPreset(cfg) {
    const set = (id, val) => { $(id).value = val; };
    set("#pctSlider",      cfg.pct);
    set("#perf3mInput",    cfg.perf3m ?? "");
    set("#perf6mInput",    cfg.perf6m ?? "");
    set("#shareVolSlider", cfg.shareVol);
    set("#priceSlider",    cfg.price);
    set("#mcapMinSlider",  cfg.mcapMin);
    set("#mcapMaxSlider",  cfg.mcapMax);
    document.querySelectorAll("#exchangeChips .chip").forEach(c =>
      c.classList.toggle("on", cfg.exchanges.includes(c.dataset.e)));
    document.querySelectorAll("#countryChips .chip").forEach(c =>
      c.classList.toggle("on", cfg.countries.includes(c.dataset.c)));
    state.minAdrPct = cfg.minAdrPct ?? 0;
    state.excludeSectors = cfg.excludeSectors ?? [];
    state.excludeIndustries = cfg.excludeIndustries ?? [];
    applyFilters();
  }

  document.querySelectorAll(".preset[data-preset]").forEach(b =>
    b.addEventListener("click", () => {
      const cfg = PRESETS[b.dataset.preset];
      if (cfg) applyPreset(cfg);
    }));

  document.querySelectorAll("th[data-sort]").forEach(th =>
    th.addEventListener("click", () => {
      const key = th.dataset.sort;
      if (state.sort.key === key) state.sort.dir *= -1;
      else state.sort = { key, dir: -1 };
      sortView(); renderTable();
    }));

  document.addEventListener("keydown", (e) => {
    if (!state.selected) return;
    const idx = state.view.indexOf(state.selected);
    if (e.key === "j" && idx < state.view.length - 1) selectRow(idx + 1);
    else if (e.key === "k" && idx > 0) selectRow(idx - 1);
  });
}

// Load TradingView widget script then start
const tvScript = document.createElement("script");
tvScript.src = "https://s3.tradingview.com/tv.js";
tvScript.onload = () => { wire(); loadRunners(); };
document.head.appendChild(tvScript);
