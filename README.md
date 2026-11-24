# Retail demo: Black Friday Command Center

A live-looking retail dashboard that runs entirely in your browser: KPIs, an orders chart with a
forecast band and anomaly markers, live inventory, a store map, and a streaming-pipeline view
(Kafka topics and Flink jobs), plus an advisor that reacts to demand spikes and low stock.

**It is a simulation.** All numbers are generated in the browser. The anomaly detection is a simple
statistical band that imitates Flink's `ML_DETECT_ANOMALIES`, and the advisor is rule-based, not an
LLM. There is no Kafka or Confluent Cloud in this version.

## Run it with Bob

1. Clone the repo and open the folder in Bob:
   ```
   git clone https://github.com/githubkolja/retail.git
   ```
2. In a new Bob task, say: **run retail demo**

Bob starts a local server and opens the dashboard.

## Run it without Bob

Requires Python 3 only.

```
python3 serve.py
```

Then open the URL it prints (default http://127.0.0.1:8722/).

## What to try

- **Flash sale surge:** a traffic spike. The anomaly is flagged, the advisor fires, stock drains.
- **Viral product:** one item's demand jumps.
- **Present:** a larger view for presenting.
- **Reset:** restocks everything.

## Files

| File | Purpose |
|---|---|
| `index.html`, `app.js` | The dashboard and its simulation |
| `serve.py` | Local server (127.0.0.1 only, no caching, picks a free port) |
| `.bob/skills/run-retail-demo/SKILL.md` | Makes "run retail demo" work in Bob |
