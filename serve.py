"""Serve this folder on 127.0.0.1 with browser caching disabled.

Usage: python3 serve.py [port]

Tries the given port (default 8722) and, if it is taken, the next 20 ports.
Prints one line, "Serving on http://127.0.0.1:<port>/", once it is listening.
"""
import sys
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer


class NoCache(SimpleHTTPRequestHandler):
    def end_headers(self):
        self.send_header("Cache-Control", "no-store, max-age=0")
        super().end_headers()


def main():
    start = int(sys.argv[1]) if len(sys.argv) > 1 else 8722
    for port in range(start, start + 21):
        try:
            server = ThreadingHTTPServer(("127.0.0.1", port), NoCache)
        except OSError:
            continue
        print(f"Serving on http://127.0.0.1:{port}/", flush=True)
        server.serve_forever()
        return
    sys.exit(f"No free port found between {start} and {start + 20}.")


if __name__ == "__main__":
    main()
