"""테스트용 인메모리 Kafka Connect REST 서버."""

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class FakeConnect:
    def __init__(self):
        self.configs: dict[str, dict] = {}
        self.failed: set[tuple[str, int]] = set()
        self.calls: list[tuple[str, str]] = []
        fake = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def _send(self, code, obj=None):
                body = b"" if obj is None else json.dumps(obj).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def _parts(self):
                return self.path.split("?")[0].strip("/").split("/")

            def do_GET(self):
                fake.calls.append(("GET", self.path))
                p = self._parts()
                if p == ["connectors"]:
                    if "expand=status" in self.path:
                        return self._send(200, {n: {"status": fake._status(n)} for n in fake.configs})
                    return self._send(200, list(fake.configs))
                if len(p) == 3 and p[2] == "config":
                    if p[1] not in fake.configs:
                        return self._send(404, {"error_code": 404})
                    return self._send(200, {**fake.configs[p[1]], "name": p[1]})
                self._send(404, {})

            def do_PUT(self):
                fake.calls.append(("PUT", self.path))
                p = self._parts()
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                created = p[1] not in fake.configs
                fake.configs[p[1]] = body
                self._send(201 if created else 200, {"name": p[1], "config": body})

            def do_POST(self):
                fake.calls.append(("POST", self.path))
                p = self._parts()
                fake.failed.discard((p[1], int(p[3])))
                self._send(204)

            def do_DELETE(self):
                fake.calls.append(("DELETE", self.path))
                fake.configs.pop(self._parts()[1], None)
                self._send(204)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def _status(self, name):
        state = "FAILED" if (name, 0) in self.failed else "RUNNING"
        trace = "ConnectException: boom\n\tat x" if state == "FAILED" else None
        return {
            "name": name,
            "connector": {"state": "RUNNING"},
            "tasks": [{"id": 0, "state": state, "trace": trace}],
        }

    def close(self):
        self.server.shutdown()
