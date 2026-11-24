"use strict";
// Black Friday Command Center - simulated preview.
// Everything here runs in the browser. It imitates the shape of the live Confluent
// pipeline (topics -> Flink windows -> anomaly detection -> advice) but is NOT Kafka/Flink.

const $ = (id) => document.getElementById(id);
const rnd = (a, b) => a + Math.random() * (b - a);
const pick = (arr) => arr[Math.floor(Math.random() * arr.length)];
const fmt = (n) => Math.round(n).toLocaleString("en-GB");

// Stock levels are small on purpose: a surge visibly drains them in about 30-60 seconds.
// A distribution centre replenishes slightly faster than normal demand, so idle stays healthy.
const SURGE_TICKS = 120; // 60 s at 500 ms per tick
const SKUS = [
  { id: "AIRMAX-90", name: "Trail Runner X", price: 89, stock: 300, max: 300, base: 1.0 },
  { id: "HOOD-CORE", name: "Core Hoodie", price: 49, stock: 450, max: 450, base: 1.4 },
  { id: "PUFFER-22", name: "Puffer Jacket", price: 139, stock: 180, max: 180, base: 0.6 },
  { id: "BUDS-PRO", name: "Wireless Buds", price: 79, stock: 240, max: 240, base: 1.1 },
  { id: "WATCH-S3", name: "Smart Watch S3", price: 199, stock: 100, max: 100, base: 0.4 },
];

const STORES = [
  ["Glasgow", .3, .1], ["Newcastle", .55, .2], ["Manchester", .44, .38], ["Leeds", .62, .34],
  ["Liverpool", .22, .46], ["Birmingham", .5, .56], ["Cardiff", .24, .74], ["Bristol", .45, .82],
  ["London Oxford St", .64, .7], ["London Stratford", .8, .64], ["Brighton", .72, .92], ["Norwich", .86, .46],
].map(([name, x, y]) => ({ name, x, y, pulse: 0, orders: 0 }));

const state = {
  tick: 0, surge: 0, viral: null, present: false,
  hist: [], rev: [], ords: [], eps: 0,
  recovered: 0, anomalies: 0, inAnomaly: false, lastAdvice: -99,
  counts: { clicks: 0, carts: 0, orders: 0, inv: 0, metrics: 0, anom: 0, advice: 0 },
  pending: [], mean: null, varr: 0, n: 0, lowSeen: {},
};

// ---- pipeline strip ---------------------------------------------------------
const PIPE = [
  ["topic", "clickstream", "clicks"], ["topic", "orders", "orders"], ["topic", "inventory_updates", "inv"],
  ["job", "Flink SQL: 5s TUMBLE", null], ["topic", "sales_metrics", "metrics"],
  ["job", "ML_DETECT_ANOMALIES", null], ["topic", "sales_anomalies", "anom"],
  ["job", "Advisor", null], ["topic", "ops_advice", "advice"],
];
const pipeEl = $("pipe");
PIPE.forEach(([kind, label, key], i) => {
  if (i) { const a = document.createElement("div"); a.className = "arrow"; a.textContent = "\u2192"; pipeEl.appendChild(a); }
  const d = document.createElement("div");
  d.className = "node " + kind;
  d.id = "n" + i;
  d.innerHTML = `<small>${kind === "topic" ? "Kafka topic" : "Flink job"}</small>${label}<div class="c" id="c${i}">${key ? 0 : "&#9679;"}</div>`;
  pipeEl.appendChild(d);
});
function pulse(i) { const n = $("n" + i); n.classList.remove("pulse"); void n.offsetWidth; n.classList.add("pulse"); }
function refreshPipe() { PIPE.forEach(([, , k], i) => { if (k) $("c" + i).textContent = fmt(state.counts[k]); }); }

// ---- inventory --------------------------------------------------------------
const skuEl = $("skus");
SKUS.forEach((s) => {
  const d = document.createElement("div");
  d.className = "sku"; d.id = "s_" + s.id;
  d.innerHTML = `<span>${s.name}</span><div class="bar"><i style="width:100%"></i></div><span class="n">${s.stock}</span>`;
  skuEl.appendChild(d);
});
function drawSkus() {
  SKUS.forEach((s) => {
    const el = $("s_" + s.id), pct = s.stock / s.max;
    el.querySelector("i").style.width = (pct * 100).toFixed(1) + "%";
    el.querySelector(".n").textContent = fmt(s.stock);
    el.classList.toggle("low", pct < 0.35 && pct >= 0.15);
    el.classList.toggle("crit", pct < 0.15);
  });
}

// ---- feed / advice ----------------------------------------------------------
function feed(topic, text, cls) {
  const f = $("feed"), d = document.createElement("div");
  d.innerHTML = `<span class="${cls || "t"}">${topic}</span> ${text}`;
  f.insertBefore(d, f.firstChild);
  while (f.children.length > 9) f.removeChild(f.lastChild);
}
function advise(sev, title, body) {
  state.counts.advice++; pulse(7); pulse(8);
  const a = document.createElement("div");
  a.className = "adv " + sev;
  a.innerHTML = `<b>${sev} &middot; ${title} &middot; ${new Date().toLocaleTimeString("en-GB")}</b>${body}`;
  const box = $("advice"); box.insertBefore(a, box.firstChild);
  while (box.children.length > 6) box.removeChild(box.lastChild);
  feed("ops_advice", `${sev} ${title}`, "a");
}
function banner(msg) {
  const b = $("banner");
  b.textContent = msg; b.style.display = msg ? "block" : "none";
}

// ---- simulation -------------------------------------------------------------
function simulate() {
  state.tick++;
  // A flash sale hits at once (a step, so it is a real anomaly the band cannot absorb) and tapers off over the last 10 ticks.
  const surgeMul = state.surge > 0 ? 1 + 3 * Math.min(1, state.surge / 10) : 1;
  const diurnal = 1 + 0.12 * Math.sin(state.tick / 18);
  let orders = 0, revenue = 0, events = 0;
  const perSku = SKUS.map((s) => {
    const viral = state.viral && state.viral.id === s.id ? 3.2 : 1;
    const lam = 1.2 * s.base * diurnal * surgeMul * viral;
    const n = Math.max(0, Math.round(lam + rnd(-1, 1) * Math.sqrt(lam + 1)));
    const sold = Math.min(n, s.stock);
    s.stock -= sold; orders += sold; revenue += sold * s.price;
    // distribution centre replenishment (fractional accumulator, capped at max)
    s.acc = (s.acc || 0) + s.base * 1.26;
    const add = Math.floor(s.acc); s.acc -= add;
    s.stock = Math.min(s.max, s.stock + add);
    return sold;
  });
  const clicks = Math.round(orders * rnd(14, 20)), carts = Math.round(orders * rnd(2.6, 3.4));
  events = clicks + carts + orders + SKUS.length;
  state.counts.clicks += clicks; state.counts.carts += carts;
  state.counts.orders += orders; state.counts.inv += SKUS.length;
  state.counts.metrics++;
  [0, 1, 2, 3, 4].forEach(pulse);

  // distribute order pulses to stores (web orders get no pulse)
  STORES.forEach((st) => { const k = Math.round(orders * rnd(0.02, 0.09)); st.orders += k; if (k) st.pulse = 1; });

  // anomaly detection (EWMA mean/variance band; imitates ML_DETECT_ANOMALIES in spirit)
  let flagged = false, lo = null, hi = null, fc = null;
  if (state.mean === null) state.mean = orders;
  if (state.n >= 30) {
    const sd = Math.max(1.5, Math.sqrt(state.varr));
    fc = state.mean; lo = fc - 3.5 * sd; hi = fc + 3.5 * sd;
    flagged = orders > hi || orders < lo;
  }
  // Learn from normal points only. If a new level persists for 5 windows it is the new normal,
  // so the band adapts fast (a flagged episode is a burst, not a permanent alarm).
  state.flagRun = flagged ? (state.flagRun || 0) + 1 : 0;
  if (!flagged || state.flagRun > 5) {
    const rate = flagged ? 0.35 : 0.08, d = orders - state.mean;
    state.mean += rate * d; state.varr = (1 - rate) * (state.varr + rate * d * d);
  }
  state.n++;
  pulse(5);
  if (flagged) {
    state.counts.anom++; state.anomalies++; pulse(6);
    feed("sales_anomalies", `orders=${orders} forecast=${fc.toFixed(0)} band=[${lo.toFixed(0)},${hi.toFixed(0)}]`, "a");
    if (!state.inAnomaly && state.tick - state.lastAdvice > 6) {
      state.lastAdvice = state.tick;
      const top = SKUS.map((s, i) => [s, perSku[i]]).sort((a, b) => b[1] - a[1])[0][0];
      advise("HIGH", "Demand spike", `Orders are ${(orders / fc).toFixed(1)}x forecast. Hottest item: ${top.name} (${fmt(top.stock)} left). Suggested: pre-position stock, scale pick-and-pack, hold paid promos on low-stock lines.`);
    }
  }
  state.inAnomaly = flagged;
  state.hist.push({ o: orders, fc, lo, hi, flagged });
  if (state.hist.length > 90) state.hist.shift();

  // low stock transitions
  SKUS.forEach((s) => {
    const pct = s.stock / s.max, band = pct < 0.15 ? "crit" : pct < 0.35 ? "low" : "ok";
    if (band !== "ok" && state.lowSeen[s.id] !== band) {
      advise(band === "crit" ? "HIGH" : "MED", `${s.name} ${band === "crit" ? "almost gone" : "running low"}`,
        `${fmt(s.stock)} units left (${(pct * 100).toFixed(0)}%). Suggested: ${band === "crit" ? "pause paid ads, offer click-and-collect from nearest store" : "reroute stock from low-demand stores"}.`);
    }
    state.lowSeen[s.id] = band;
    if (band === "ok") delete state.lowSeen[s.id];
  });

  // cart abandonment -> event-driven nudge -> maybe recovered
  const abandons = Math.round(carts * 0.12);
  for (let i = 0; i < abandons; i++) state.pending.push({ due: state.tick + 4, v: rnd(40, 180) });
  state.pending = state.pending.filter((p) => {
    if (p.due > state.tick) return true;
    if (Math.random() < 0.32) { state.recovered++; revenue += p.v; }
    return false;
  });
  if (state.tick % 4 === 0) feed("orders", `${pick(STORES).name} order ${pick(SKUS).id} x${1 + Math.floor(rnd(0, 3))}`);

  state.rev.push(revenue); state.ords.push(orders);
  if (state.rev.length > 12) { state.rev.shift(); state.ords.shift(); }
  state.eps = events / 0.5;

  if (state.surge > 0) { state.surge--; if (!state.surge) banner(""); }
  if (state.viral && --state.viral.left <= 0) { state.viral = null; if (!state.surge) banner(""); }
  render();
}

// ---- rendering --------------------------------------------------------------
function render() {
  const rev = state.rev.reduce((a, b) => a + b, 0) * (60 / (state.rev.length * 0.5));
  const ord = state.ords.reduce((a, b) => a + b, 0) * (60 / (state.ords.length * 0.5));
  $("kRev").textContent = "\u00a3" + fmt(rev);
  $("kOrd").textContent = fmt(ord);
  $("kEps").textContent = fmt(state.eps);
  $("kRec").textContent = fmt(state.recovered);
  $("kAnom").textContent = fmt(state.anomalies);
  refreshPipe(); drawSkus(); drawChart(); drawMap();
}

const CANVAS_H = { chart: 230, map: 260 };
function setupCanvas(c) {
  // Size from a fixed-height wrapper, never from the canvas itself, so the bitmap cannot feed back into layout.
  // If a cached older index.html has no wrapper, build one here.
  if (!c.parentElement.classList.contains("cv")) {
    const w = document.createElement("div");
    w.className = "cv";
    w.style.cssText = `position:relative;width:100%;overflow:hidden;border-radius:8px;background:#0e1422;height:${CANVAS_H[c.id]}px`;
    c.parentElement.insertBefore(w, c); w.appendChild(c);
    c.removeAttribute("height"); c.style.cssText = "position:absolute;left:0;top:0;display:block";
  }
  const p = c.parentElement, W = p.clientWidth, H = p.clientHeight, dpr = Math.min(window.devicePixelRatio || 1, 3);
  if (c.width !== Math.round(W * dpr) || c.height !== Math.round(H * dpr)) {
    c.width = Math.round(W * dpr); c.height = Math.round(H * dpr);
    c.style.width = W + "px"; c.style.height = H + "px";
  }
  const g = c.getContext("2d"); g.setTransform(dpr, 0, 0, dpr, 0, 0);
  return [g, W, H];
}

function drawChart() {
  const [g, W, H] = setupCanvas($("chart")), h = state.hist;
  g.clearRect(0, 0, W, H);
  const max = Math.max(60, ...h.map((p) => Math.max(p.o, p.hi || 0))) * 1.1;
  const X = (i) => 36 + (i / 89) * (W - 48), Y = (v) => H - 22 - (v / max) * (H - 36);
  g.strokeStyle = "#1c2640"; g.fillStyle = "#6c7b99"; g.font = "11px sans-serif";
  for (let k = 0; k <= 4; k++) { const v = (max / 4) * k; g.beginPath(); g.moveTo(36, Y(v)); g.lineTo(W - 8, Y(v)); g.stroke(); g.fillText(Math.round(v), 4, Y(v) + 4); }
  const off = 90 - h.length;
  g.beginPath(); let started = false;
  h.forEach((p, i) => { if (p.hi == null) return; const x = X(i + off); if (!started) { g.moveTo(x, Y(p.hi)); started = true; } else g.lineTo(x, Y(p.hi)); });
  for (let i = h.length - 1; i >= 0; i--) { const p = h[i]; if (p.lo != null) g.lineTo(X(i + off), Y(Math.max(0, p.lo))); }
  g.closePath(); g.fillStyle = "rgba(74,163,255,.14)"; g.fill();
  g.beginPath(); g.strokeStyle = "#3ddc97"; g.lineWidth = 2;
  h.forEach((p, i) => { const x = X(i + off); i ? g.lineTo(x, Y(p.o)) : g.moveTo(x, Y(p.o)); }); g.stroke();
  h.forEach((p, i) => { if (!p.flagged) return; g.beginPath(); g.fillStyle = "#ff5d6c"; g.arc(X(i + off), Y(p.o), 5, 0, 7); g.fill(); });
  if (state.n < 30) { g.fillStyle = "#ffb84a"; g.fillText(`model warming up: ${state.n}/30 windows before anomaly detection starts`, 44, 16); }
}

function drawMap() {
  const [g, W, H] = setupCanvas($("map"));
  g.clearRect(0, 0, W, H);
  STORES.forEach((s) => {
    const x = s.x * W, y = s.y * H;
    if (s.pulse > 0) { g.beginPath(); g.strokeStyle = `rgba(61,220,151,${s.pulse})`; g.lineWidth = 2; g.arc(x, y, 6 + (1 - s.pulse) * 26, 0, 7); g.stroke(); s.pulse -= 0.12; }
    g.beginPath(); g.fillStyle = "#4aa3ff"; g.arc(x, y, 5, 0, 7); g.fill();
    g.fillStyle = "#8b9ab8"; g.font = "11px sans-serif"; g.fillText(`${s.name} ${fmt(s.orders)}`, x + 9, y + 4);
  });
}

// ---- controls ---------------------------------------------------------------
$("bSurge").onclick = () => {
  state.surge = SURGE_TICKS; $("bSurge").classList.add("hot");
  banner("FLASH SALE LIVE: 20% off trainers, 30 minute pickup. Traffic is climbing.");
  feed("clickstream", "campaign=flash_sale_20 started", "a");
};
$("bViral").onclick = () => {
  const s = pick(SKUS); state.viral = { id: s.id, left: 80 };
  banner(`${s.name} is going viral on social. Watch its stock.`);
  feed("clickstream", `social_spike product=${s.id}`, "a");
};
$("bPresent").onclick = () => { state.present = !state.present; document.body.classList.toggle("present", state.present); $("bPresent").classList.toggle("on", state.present); };
$("bReset").onclick = () => {
  SKUS.forEach((s) => (s.stock = s.max)); state.surge = 0; state.viral = null; state.lowSeen = {};
  $("bSurge").classList.remove("hot"); banner(""); feed("inventory_updates", "restock all SKUs");
};

simulate();
setInterval(simulate, 500);
