"""desired state(connectors.yaml) 와 Kafka Connect 의 실제 상태를 맞춘다."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from registrar.client import ConnectClient
from registrar.config import Connector
from registrar.notify import notify

log = logging.getLogger(__name__)


@dataclass
class Result:
    created: list[str] = field(default_factory=list)
    updated: list[str] = field(default_factory=list)
    unchanged: list[str] = field(default_factory=list)
    restarted: list[str] = field(default_factory=list)
    unmanaged: list[str] = field(default_factory=list)
    deleted: list[str] = field(default_factory=list)

    def summary(self) -> str:
        return ", ".join(f"{k}={len(v)}" for k, v in self.__dict__.items())


def _differs(desired: dict[str, str], actual: dict[str, str]) -> bool:
    # Connect 가 붙여주는 name 키는 비교에서 제외
    actual = {k: v for k, v in actual.items() if k != "name"}
    return desired != actual


def apply(client: ConnectClient, desired: list[Connector], *, prune: bool = False, dry_run: bool = False) -> Result:
    res = Result()
    for c in desired:
        actual = client.get_config(c.name)
        if actual is None:
            res.created.append(c.name)
        elif _differs(c.config, actual):
            res.updated.append(c.name)
        else:
            res.unchanged.append(c.name)
            continue
        if not dry_run:
            client.put_config(c.name, c.config)

    wanted = {c.name for c in desired}
    for name in client.list_names():
        if name in wanted:
            continue
        if prune:
            res.deleted.append(name)
            if not dry_run:
                client.delete(name)
        else:
            res.unmanaged.append(name)
    return res


def restart_failed(client: ConnectClient, dry_run: bool = False) -> list[str]:
    restarted = []
    for name, st in client.statuses().items():
        for t in st.get("tasks", []):
            if t.get("state") != "FAILED":
                continue
            first_line = (t.get("trace") or "").splitlines()[:1]
            label = f"{name}/task{t['id']}"
            restarted.append(label)
            notify(f"FAILED {label}: {first_line[0] if first_line else ''} → 재시작")
            if not dry_run:
                client.restart_task(name, t["id"])
    return restarted


def reconcile_once(client: ConnectClient, desired: list[Connector], *, prune: bool = False) -> Result:
    res = apply(client, desired, prune=prune)
    res.restarted = restart_failed(client)
    if res.created or res.updated or res.deleted:
        log.info("변경 반영: created=%s updated=%s deleted=%s", res.created, res.updated, res.deleted)
    return res
