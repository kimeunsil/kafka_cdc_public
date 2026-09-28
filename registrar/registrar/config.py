"""connectors.yaml 로드 및 desired state 계산.

파일 형식::

    defaults:
      source: {...}            # 모든 소스 커넥터 공통 설정
      sink: {...}              # 모든 싱크 커넥터 공통 설정
    sources:
      - name: mysql-source-shop
        config: {...}          # defaults.source 위에 덮어씀
    sinks:
      name_prefix: bq-sink-    # 커넥터 이름 = prefix + topic
      topics: [customers, orders]
      config: {...}            # defaults.sink 위에 덮어씀
      overrides:               # (선택) 특정 토픽만 다른 설정
        orders: {...}

값 안의 ``${env:NAME}`` 은 registrar 가 환경변수로 치환합니다.
``${file:...}`` 같은 다른 형식은 그대로 두어 Kafka Connect ConfigProvider 가 해석하게 합니다
(비밀번호가 registrar 나 REST 요청 로그에 남지 않음).
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

_ENV_REF = re.compile(r"\$\{env:([A-Za-z_][A-Za-z0-9_]*)\}")


class ConfigError(ValueError):
    pass


@dataclass(frozen=True)
class Connector:
    name: str
    config: dict[str, str]
    kind: str  # "source" | "sink"


def _stringify(value: Any) -> str:
    # Connect REST API 는 모든 설정값을 문자열로 돌려주므로, 비교를 위해 미리 맞춰둔다
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _expand_env(value: str, env: dict[str, str]) -> str:
    def repl(m: re.Match) -> str:
        key = m.group(1)
        if key not in env:
            raise ConfigError(f"환경변수 {key} 가 설정되지 않았습니다")
        return env[key]

    return _ENV_REF.sub(repl, value)


def _render(*layers: dict[str, Any], env: dict[str, str]) -> dict[str, str]:
    merged: dict[str, Any] = {}
    for layer in layers:
        merged.update(layer or {})
    return {k: _expand_env(_stringify(v), env) for k, v in merged.items()}


def _merge(docs: list[dict]) -> dict:
    """여러 파일을 순서대로 합친다: defaults 는 덮어쓰기, sources/topics 는 이어붙이기.

    k8s 에서 공통(defaults, sinks) 파일과 클러스터별(sources) 파일을 나눠 두기 위함.
    """
    out: dict = {"defaults": {"source": {}, "sink": {}}, "sources": [], "sinks": {"topics": [], "overrides": {}}}
    for d in docs:
        for kind in ("source", "sink"):
            out["defaults"][kind].update((d.get("defaults") or {}).get(kind) or {})
        out["sources"].extend(d.get("sources") or [])
        sinks = d.get("sinks") or {}
        out["sinks"]["topics"].extend(sinks.get("topics") or [])
        out["sinks"]["overrides"].update(sinks.get("overrides") or {})
        for k in ("name_prefix", "config"):
            if k in sinks:
                out["sinks"][k] = sinks[k]
    return out


def load(
    paths: str | Path | list[str | Path],
    env: dict[str, str] | None = None,
    only: str | None = None,
) -> list[Connector]:
    """only="source" 또는 "sink" 이면 해당 종류만 로드 (예: GCP 없이 로컬에서 소스만 검증)."""
    env = dict(os.environ) if env is None else env
    if isinstance(paths, (str, Path)):
        paths = [paths]
    raw = _merge([yaml.safe_load(Path(p).read_text(encoding="utf-8")) or {} for p in paths])
    defaults = raw.get("defaults", {})
    connectors: list[Connector] = []

    for src in raw.get("sources", []) if only in (None, "source") else []:
        name = src["name"]
        cfg = _render(defaults.get("source", {}), src.get("config", {}), env=env)
        # 소스마다 schema history 토픽을 분리 (공유하면 재시작 시 다른 DB DDL 복원 시도)
        cfg.setdefault("database.history.kafka.topic", f"schema-changes.{name}")
        connectors.append(Connector(name, cfg, "source"))

    sinks = raw.get("sinks") or {}
    prefix = sinks.get("name_prefix", "bq-sink-")
    overrides = sinks.get("overrides", {})
    for topic in sinks.get("topics", []) if only in (None, "sink") else []:
        cfg = _render(
            defaults.get("sink", {}),
            sinks.get("config", {}),
            overrides.get(topic, {}),
            {"topics": topic},
            env=env,
        )
        connectors.append(Connector(f"{prefix}{topic}", cfg, "sink"))

    _validate(connectors)
    return connectors


def _validate(connectors: list[Connector]) -> None:
    names = [c.name for c in connectors]
    dup = {n for n in names if names.count(n) > 1}
    if dup:
        raise ConfigError(f"커넥터 이름 중복: {sorted(dup)}")

    for c in connectors:
        # 복사/붙여넣기로 섞여 들어온 보이지 않는 문자는 Connect 가 조용히 무시하므로 사전에 차단
        bad = [k for k in c.config if not k.isascii() or any(ch.isspace() for ch in k)]
        if bad:
            raise ConfigError(f"{c.name}: 비정상 문자가 포함된 설정 키 {bad!r}")
        if "connector.class" not in c.config:
            raise ConfigError(f"{c.name}: connector.class 누락")

    # 같은 MySQL 서버에 붙는 소스끼리 server.id 가 겹치면 복제 연결을 서로 끊는다
    seen: dict[tuple[str, str], str] = {}
    for c in connectors:
        if c.kind != "source":
            continue
        key = (c.config.get("database.hostname", ""), c.config.get("database.server.id", ""))
        if key in seen:
            raise ConfigError(f"database.server.id 중복: {seen[key]} / {c.name} ({key[0]})")
        seen[key] = c.name
