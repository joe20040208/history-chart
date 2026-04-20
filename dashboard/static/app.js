// History Chart dashboard app — filter, sort, flip, render
const state = {
  all: [],
  view: [],
  selected: null,
  sort: { key: "pct_gain", dir: -1 },
  chart: null,
  candles: null,
  volume: null,
  ema10: null,
  ema20: null,
  ema50: null,
  minAdrPct: 0,          // ATR% threshold (0 = off); 0.05 = 5%
  excludeSectors: [],    // sector names to exclude (exact match, case-insensitive)
};

const $ = (s) => document.querySelector(s);
const fmtMcap = (v) => v ? "$" + (v / 1e9).toFixed(2) + "B" : "—";
const fmtVol = (v) => v ? "$" + (v / 1e6).toFixed(1) + "M" : "—";
const fmtPct = (v) => v == null ? "—" : v.toFixed(1) + "%";

// ──────────────── data load ────────────────
async function loadRunners() {
  try {
    const res = await fetch("/api/runners");
    if (!res.ok) throw new Error(await res.text());
    const raw = await res.json();
    // strip junk: cap at 3000%, min $1 start price, min 5 days to peak (removes 1-day data errors)
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
    $("#summary").textContent = "No data yet — run the fetch + screen scripts first.";
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
  const rows = state.view.slice(0, 2000);  // cap render
  tbody.innerHTML = rows.map((r, i) => `
    <tr data-i="${i}" class="${state.selected === r ? "active" : ""}">
      <td>${r.ticker}</td>
      <td class="pos">+${r.pct_gain.toFixed(0)}%</td>
      <td>${r.days_to_peak}<span class="date-range">${r.start_date} → ${r.peak_date}</span></td>
      <td>${fmtMcap(r.start_mcap_usd)}</td>
      <td class="tag">${r.setup_tag || "—"}</td>
    </tr>
  `).join("");
  tbody.onclick = (e) => {
    const tr = e.target.closest("tr"); if (!tr) return;
    selectRow(+tr.dataset.i);
  };
}

// ──────────────── chart ────────────────
function ensureChart() {
  if (state.chart) return;
  const el = $("#chart-container");
  state.chart = LightweightCharts.createChart(el, {
    layout: { background: { color: "#0e1117" }, textColor: "#e6edf3" },
    grid: { vertLines: { color: "#1f2630" }, horzLines: { color: "#1f2630" } },
    crosshair: { mode: 1 },
    rightPriceScale: { borderColor: "#2a3240" },
    timeScale: { borderColor: "#2a3240", rightOffset: 12, barSpacing: 5 },
  });
  // Candlesticks on right scale — leave bottom 20% for volume
  state.candles = state.chart.addCandlestickSeries({
    upColor: "#3fb950", downColor: "#f85149",
    borderUpColor: "#3fb950", borderDownColor: "#f85149",
    wickUpColor: "#3fb950", wickDownColor: "#f85149",
    priceScaleId: "right",
  });
  state.chart.priceScale("right").applyOptions({
    scaleMargins: { top: 0.05, bottom: 0.2 },
  });

  // Volume as overlay on its own named scale, bottom 20% only
  state.volume = state.chart.addHistogramSeries({
    priceFormat: { type: "volume" },
    priceScaleId: "vol",
    color: "#2a3240",
    lastValueVisible: false,
    priceLineVisible: false,
  });
  state.chart.priceScale("vol").applyOptions({
    scaleMargins: { top: 0.8, bottom: 0 },
    visible: false,
    autoScale: true,
  });

  const ema = (color) => state.chart.addLineSeries({
    color, lineWidth: 1, priceLineVisible: false, lastValueVisible: false });
  state.ema10 = ema("#d29922");
  state.ema20 = ema("#58a6ff");
  state.ema50 = ema("#a371f7");

  window.addEventListener("resize", () => state.chart.applyOptions({
    width: el.clientWidth, height: el.clientHeight }));
}

function calcEMA(candles, span) {
  const k = 2 / (span + 1);
  let prev = null;
  return candles.map(c => {
    const v = c.close;
    prev = prev == null ? v : v * k + prev * (1 - k);
    return { time: c.time, value: prev };
  });
}

function destroyChart() {
  if (state.chart) { state.chart.remove(); state.chart = null; }
}

async function renderChart(row) {
  destroyChart();
  ensureChart();
  // Pull ±120 days around the event so you see setup + aftermath
  const start = new Date(row.start_date); start.setDate(start.getDate() - 120);
  const end = new Date(row.peak_date); end.setDate(end.getDate() + 120);
  const qs = new URLSearchParams({
    from_: start.toISOString().slice(0, 10),
    to: end.toISOString().slice(0, 10),
  });
  const url = `/api/ohlc/${row.country}/${row.exchange}/${encodeURIComponent(row.ticker)}?${qs}`;
  const res = await fetch(url);
  if (!res.ok) { $("#chart-meta").textContent = "chart data missing"; return; }
  const bars = await res.json();
  state.candles.setData(bars);
  state.volume.setData(bars.map(b => ({
    time: b.time,
    value: (b.volume > 0 && isFinite(b.volume)) ? b.volume : 0,
    color: b.close >= b.open ? "#1a5a2d" : "#5f1f24",
  })));
  state.ema10.setData(calcEMA(bars, 10));
  state.ema20.setData(calcEMA(bars, 20));
  state.ema50.setData(calcEMA(bars, 50));

  // Markers for start / peak
  state.candles.setMarkers([
    { time: row.start_date, position: "belowBar", color: "#58a6ff", shape: "arrowUp", text: "START" },
    { time: row.peak_date,  position: "aboveBar", color: "#d29922", shape: "arrowDown", text: "PEAK" },
  ]);
  state.chart.timeScale().fitContent();
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
  $("#tv-link").href = tvUrl(r);

  renderChart(r);
  renderInsights(r);
}

function tvUrl(r) {
  const map = {
    "NASDAQ": "", "NYSE": "", "NYSE MKT": "", "NYSE ARCA": "", "AMEX": "", "BATS": "",
    "TW":  "TWSE:",
    "TWO": "TPEX:",
    "HK":  "HKEX:",
    "KO":  "KRX:",
    "KQ":  "KRX:",
  };
  const prefix = map[r.sub_exchange] ?? "";
  return `https://www.tradingview.com/chart/?symbol=${prefix}${r.ticker}`;
}

function renderInsights(r) {
  const items = [
    ["Start price", r.start_price],
    ["Peak price", r.peak_price],
    ["Days to peak", r.days_to_peak],
    ["Mcap at start", fmtMcap(r.start_mcap_usd)],
    ["Avg 30d $vol", fmtVol(r.start_dollar_vol_30d_usd)],
    ["Post-90d return", fmtPct(r.post_90d_return)],
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

function fmtDateRange(isoA, isoB) {
  const fmt = (iso) => {
    const [y, m, d] = iso.split("-").map(Number);
    return new Date(y, m - 1, d).toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
  };
  const a = fmt(isoA), b = fmt(isoB);
  // share the year suffix if both dates fall in the same year
  const yearA = isoA.slice(0, 4), yearB = isoB.slice(0, 4);
  if (yearA === yearB) {
    const aNoYear = new Date(...isoA.split("-").map((v, i) => i === 1 ? v - 1 : +v))
      .toLocaleDateString("en-US", { month: "short", day: "numeric" });
    return `${aNoYear} – ${b}`;
  }
  return `${a} – ${b}`;
}

function buildCommentary(r) {
  const lines = [];
  if (r.setup_tag === "vcp")
    lines.push("Classic VCP: tight range with drying volume before the break — low-risk entry usually at the pivot high of the last contraction.");
  if (r.setup_tag === "flat_base")
    lines.push("Flat base near 52w highs — ideal entry is the top of the box on volume expansion.");
  if (r.setup_tag === "ipo_base")
    lines.push("Early IPO base — short trading history means high conviction once it clears the IPO high, smaller position sizing.");
  if (r.setup_tag === "stage2_breakout") {
    const range = (r.start_date && r.peak_date) ? ` (${fmtDateRange(r.start_date, r.peak_date)})` : "";
    lines.push(`Stage-2 breakout over 30w EMA, tight ATR${range} — Weinstein-style low-risk add.`);
  }
  if (r.setup_tag === "power_trend")
    lines.push("Already in a power trend — entries are pullbacks to 10/21 EMA, not fresh bases.");
  if (r.setup_tag === "pocket_pivot")
    lines.push("Pocket pivot volume signature inside the base — buy before the breakout for lower-risk entry.");
  if (r.setup_tag === "none")
    lines.push("No textbook base detected. Check: was there a specific catalyst (earnings, FDA, M&A)? These are often un-chartable and higher-risk.");
  if ((r.pre_52w_high ?? 0) < 0.75)
    lines.push("Started well off 52w highs — expect base-on-base dynamics; the second leg tends to be where the real money moves.");
  if ((r.post_90d_return ?? 0) < -30) {
    let postRange = "";
    if (r.peak_date) {
      const peak = new Date(r.peak_date);
      const end90 = new Date(peak); end90.setDate(end90.getDate() + 90);
      postRange = ` (${fmtDateRange(r.peak_date, end90.toISOString().slice(0, 10))})`;
    }
    lines.push(`Gave back more than 30% in 90 days post-peak${postRange} — study the topping structure for sell-signal practice.`);
  }
  return lines.join(" ") || "—";
}

// ──────────────── wiring ────────────────
function wire() {
  // text input live update
  const bind = (id) => {
    $(id).addEventListener("input", applyFilters);
  };
  $("#searchBox").addEventListener("input", applyFilters);
  bind("#pctSlider");
  bind("#mcapMinSlider");
  bind("#mcapMaxSlider");
  bind("#shareVolSlider");
  bind("#priceSlider");
  bind("#perf3mInput");
  bind("#perf6mInput");

  // toggle chips
  document.querySelectorAll(".chip").forEach(c =>
    c.addEventListener("click", () => { c.classList.toggle("on"); applyFilters(); }));


  // preset definitions
  const PRESETS = {
    usfilter: {
      pct: 50, price: 5, shareVol: 500,
      mcapMin: 0.5, mcapMax: 0,
      exchanges: ["NASDAQ","NYSE"],
      countries: ["US"],
      minAdrPct: 0.05,
      excludeSectors: ["Health Services", "Health Technology"],
      perf3m: 30, perf6m: 50,
    },
    twfilter: {
      pct: 50, price: 150, shareVol: 500,   // TWD ~$5 USD
      mcapMin: 0, mcapMax: 0,
      exchanges: ["TW","TWO"],
      countries: ["TW"],
      minAdrPct: 0,
      excludeSectors: [],
      perf3m: 30, perf6m: 50,
    },
    krfilter: {
      pct: 50, price: 7000, shareVol: 500,  // KRW ~$5 USD
      mcapMin: 0, mcapMax: 0,
      exchanges: ["KO","KQ"],
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
    set("#mcapMaxSlider",  cfg.mcapMax || "");
    document.querySelectorAll("#exchangeChips .chip").forEach(c =>
      c.classList.toggle("on", cfg.exchanges.includes(c.dataset.e)));
    document.querySelectorAll("#countryChips .chip").forEach(c =>
      c.classList.toggle("on", cfg.countries.includes(c.dataset.c)));
    state.minAdrPct = cfg.minAdrPct ?? 0;
    state.excludeSectors = cfg.excludeSectors ?? [];
    applyFilters();
  }

  document.querySelectorAll(".preset[data-preset]").forEach(b =>
    b.addEventListener("click", () => {
      const cfg = PRESETS[b.dataset.preset];
      if (cfg) applyPreset(cfg);
    }));

  // sort
  document.querySelectorAll("th[data-sort]").forEach(th =>
    th.addEventListener("click", () => {
      const key = th.dataset.sort;
      if (state.sort.key === key) state.sort.dir *= -1;
      else state.sort = { key, dir: -1 };
      sortView(); renderTable();
    }));

  // keyboard flip
  document.addEventListener("keydown", (e) => {
    if (!state.selected) return;
    const idx = state.view.indexOf(state.selected);
    if (e.key === "j" && idx < state.view.length - 1) selectRow(idx + 1);
    else if (e.key === "k" && idx > 0) selectRow(idx - 1);
  });
}

// ──────────────── similar setups modal ────────────────
const simState = { matches: [], charts: [] };

async function openSimilar() {
  if (!state.selected) return;
  const r = state.selected;
  const modal = $("#similar-modal");
  $("#similar-meta").textContent = "loading…";
  $("#similar-grid").innerHTML = "";
  modal.style.display = "block";

  const url = `/api/similar/${r.country}/${r.exchange}/${encodeURIComponent(r.ticker)}?asof=${r.start_date}&max_results=30`;
  let data;
  try {
    const res = await fetch(url);
    if (!res.ok) {
      const err = await res.text();
      $("#similar-meta").textContent = `error: ${err}`;
      return;
    }
    data = await res.json();
  } catch (e) {
    $("#similar-meta").textContent = `error: ${e.message}`;
    return;
  }

  simState.matches = data.matches || [];
  $("#similar-meta").textContent =
    `Query ${data.query.ticker}.${data.query.exchange} as of ${data.query.asof} · setup_tag=${data.query.setup_tag} · ${simState.matches.length} matches`;

  if (simState.matches.length === 0) {
    $("#similar-grid").innerHTML = `<div class="muted" style="grid-column:1/-1;text-align:center;padding:40px">${data.note || "no matches"}</div>`;
    return;
  }
  renderSimilarGrid();
}

function renderSimilarGrid() {
  // Tear down any prior charts to free memory
  simState.charts.forEach(c => { try { c.remove(); } catch (e) {} });
  simState.charts = [];

  const minSim = (+$("#sim-threshold").value) / 100;
  const grid = $("#similar-grid");
  grid.innerHTML = "";
  const visible = simState.matches.filter(m => m.similarity >= minSim);

  if (visible.length === 0) {
    grid.innerHTML = `<div class="muted" style="grid-column:1/-1;text-align:center;padding:40px">no matches above ${(minSim*100).toFixed(0)}% similarity</div>`;
    return;
  }

  visible.forEach((m, i) => {
    const post = m.post_90d_return;
    const postColor = post == null ? "var(--muted)" : (post >= 0 ? "var(--green)" : "var(--red)");
    const postTxt = post == null ? "—" : `${post >= 0 ? "+" : ""}${post.toFixed(1)}%`;
    const card = document.createElement("div");
    card.style.cssText = "background:var(--panel2);border:1px solid var(--border);border-radius:6px;padding:10px";
    card.innerHTML = `
      <div style="display:flex;justify-content:space-between;align-items:baseline;margin-bottom:6px">
        <div><strong>${m.ticker}</strong>.${m.exchange} <span class="muted">${m.country}</span></div>
        <div style="font-size:11px;color:var(--accent)">sim ${(m.similarity*100).toFixed(0)}%</div>
      </div>
      <div class="muted" style="font-size:11px;margin-bottom:6px">
        ${m.start_date} → ${m.peak_date} · <span style="color:var(--green)">+${m.pct_gain.toFixed(0)}%</span> in ${m.days_to_peak}d
        · post-90d <span style="color:${postColor}">${postTxt}</span>
      </div>
      <div id="sim-chart-${i}" style="width:100%;height:180px"></div>
      <div class="muted" style="font-size:10px;margin-top:4px">${(m.name || "").slice(0, 60)}</div>
    `;
    grid.appendChild(card);

    // Render mini chart in this card
    const el = card.querySelector(`#sim-chart-${i}`);
    const chart = LightweightCharts.createChart(el, {
      width: el.clientWidth, height: 180,
      layout: { background: { color: "transparent" }, textColor: "#8b949e", fontSize: 9 },
      grid: { vertLines: { visible: false }, horzLines: { color: "#1f2630" } },
      rightPriceScale: { borderColor: "#2a3240" },
      timeScale: { borderColor: "#2a3240", timeVisible: false, rightOffset: 4 },
      handleScroll: false, handleScale: false,
    });
    const candles = chart.addCandlestickSeries({
      upColor: "#3fb950", downColor: "#f85149",
      borderUpColor: "#3fb950", borderDownColor: "#f85149",
      wickUpColor: "#3fb950", wickDownColor: "#f85149",
    });
    candles.setData(m.bars);
    candles.setMarkers([
      { time: m.start_date, position: "belowBar", color: "#58a6ff", shape: "arrowUp", text: "S" },
      { time: m.peak_date, position: "aboveBar", color: "#d29922", shape: "arrowDown", text: "P" },
    ]);
    chart.timeScale().fitContent();
    simState.charts.push(chart);
  });
}

function closeSimilar() {
  $("#similar-modal").style.display = "none";
  simState.charts.forEach(c => { try { c.remove(); } catch (e) {} });
  simState.charts = [];
}

$("#find-similar-btn").onclick = openSimilar;
$("#similar-close").onclick = closeSimilar;
$("#similar-modal").onclick = (e) => { if (e.target.id === "similar-modal") closeSimilar(); };
$("#sim-threshold").oninput = (e) => {
  $("#sim-threshold-val").textContent = `${e.target.value}%`;
  renderSimilarGrid();
};

wire();
loadRunners();
