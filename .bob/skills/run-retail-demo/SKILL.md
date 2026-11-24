---
name: run-retail-demo
description: Use when the user says "run retail demo", "start the retail demo", "open the Black Friday demo" or "launch the command center" — starts the local Black Friday Command Center dashboard from this repo and opens it in the browser.
---

# Run the retail demo

This repo contains the Black Friday Command Center: a static, in-browser simulation of a retail
streaming pipeline (clickstream, orders and inventory flowing through Kafka and Flink into
anomaly detection and an advisor). It needs only Python 3. No keys, no cloud, no install step.

Run all commands from the repo root (the folder that contains `serve.py` and `index.html`).

## Steps

1. **Check Python.** Run `python3 --version` (on Windows try `python --version`). If neither
   works, tell the user to install Python 3 and stop.

2. **Start the server in the background.** Use `execute_command` with `background: true`:
   `python3 serve.py` (use `python serve.py` on Windows). It picks port 8722, or the next free
   port if that one is taken.

3. **Get the URL.** Read the process's stdout log file (the path is returned when the background
   process starts) with `read_file`. Find the line `Serving on http://127.0.0.1:<port>/`.
   If there is no such line, or the log shows an error, report the error and stop.

4. **Verify.** Run `curl -s -o /dev/null -w "%{http_code}" <url>` and confirm it prints `200`.

5. **Open the browser.** `open <url>` on macOS, `xdg-open <url>` on Linux, `start <url>` on
   Windows. Always use the `127.0.0.1` address from step 3, not `localhost`: on some machines
   `localhost` resolves to IPv6 and can hit a different program on the same port.

6. **Tell the user**, briefly:
   - The URL, and the PID of the server so they can stop it (`kill <pid>`).
   - What to click: **Flash sale surge** (traffic spike: anomaly flagged, advisor fires, stock
     drains), **Viral product** (one item's demand jumps), **Present** (big-screen view),
     **Reset** (restock).
   - That it is a **simulation**: all numbers are generated in the browser, the anomaly
     detection is a simple statistical band that imitates Flink's `ML_DETECT_ANOMALIES`, and the
     advisor is rule-based, not an LLM. There is no Kafka or Confluent Cloud in this version.

## Troubleshooting

- **Chart or layout looks broken or blank:** hard-refresh (Cmd+Shift+R / Ctrl+Shift+R). The
  server sends no-cache headers, but an old tab may hold stale files.
- **Page shows "404 page not found":** another program owns that port on IPv6. Open the
  `127.0.0.1` URL exactly as printed by the server.
- **Stop the demo:** `kill <pid>` using the PID reported in step 6.
