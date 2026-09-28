"""Kafka Connect REST API 최소 클라이언트."""

from __future__ import annotations

import time

import requests


class ConnectClient:
    def __init__(self, base_url: str, timeout: float = 10.0, session: requests.Session | None = None):
        self.base = base_url.rstrip("/")
        self.timeout = timeout
        self.http = session or requests.Session()

    def _req(self, method: str, path: str, **kw) -> requests.Response:
        r = self.http.request(method, f"{self.base}{path}", timeout=self.timeout, **kw)
        if r.status_code >= 400 and r.status_code != 404:
            raise RuntimeError(f"{method} {path} → {r.status_code}: {r.text[:300]}")
        return r

    def wait_until_ready(self, max_wait: float = 300, interval: float = 5) -> None:
        deadline = time.monotonic() + max_wait
        while True:
            try:
                self.http.get(f"{self.base}/connectors", timeout=self.timeout).raise_for_status()
                return
            except requests.RequestException as e:
                if time.monotonic() > deadline:
                    raise TimeoutError(f"Kafka Connect({self.base}) 가 {max_wait}s 안에 응답하지 않습니다") from e
                time.sleep(interval)

    def list_names(self) -> list[str]:
        return self._req("GET", "/connectors").json()

    def get_config(self, name: str) -> dict[str, str] | None:
        r = self._req("GET", f"/connectors/{name}/config")
        return None if r.status_code == 404 else r.json()

    def put_config(self, name: str, config: dict[str, str]) -> None:
        # PUT 은 없으면 생성, 있으면 갱신 → 몇 번을 호출해도 결과가 같다
        self._req("PUT", f"/connectors/{name}/config", json=config)

    def statuses(self) -> dict[str, dict]:
        data = self._req("GET", "/connectors?expand=status").json()
        return {name: v["status"] for name, v in data.items()}

    def restart_task(self, name: str, task_id: int) -> None:
        self._req("POST", f"/connectors/{name}/tasks/{task_id}/restart")

    def delete(self, name: str) -> None:
        self._req("DELETE", f"/connectors/{name}")
