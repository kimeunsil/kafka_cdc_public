"""CLI.

python -m registrar apply     --config connectors.yaml [--dry-run] [--prune]
python -m registrar reconcile --config connectors.yaml [--loop --interval 60]
python -m registrar status
python -m registrar restart-failed
python -m registrar delete <name>
python -m registrar validate  --config connectors.yaml
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time

from registrar import config
from registrar.client import ConnectClient
from registrar.notify import notify
from registrar.reconcile import apply, reconcile_once, restart_failed

log = logging.getLogger("registrar")


def _config_files(args) -> list[str]:
    return args.config or os.environ.get("CONNECTORS_FILE", "config/connectors.yaml").split(",")


def _client(args) -> ConnectClient:
    return ConnectClient(args.connect_url)


def cmd_validate(args) -> int:
    conns = config.load(_config_files(args), only=args.only)
    print(f"OK: source {sum(c.kind == 'source' for c in conns)}개, sink {sum(c.kind == 'sink' for c in conns)}개")
    return 0


def cmd_apply(args) -> int:
    client = _client(args)
    client.wait_until_ready(max_wait=args.wait)
    res = apply(client, config.load(_config_files(args), only=args.only), prune=args.prune, dry_run=args.dry_run)
    for key in ("created", "updated", "deleted", "unmanaged"):
        for name in getattr(res, key):
            print(f"{key:9} {name}")
    print(("[dry-run] " if args.dry_run else "") + res.summary())
    return 0


def cmd_reconcile(args) -> int:
    client = _client(args)
    while True:
        try:
            # 매 주기마다 다시 읽는다 → ConfigMap 이 갱신되면 파드 재시작 없이 반영
            desired = config.load(_config_files(args), only=args.only)
            client.wait_until_ready(max_wait=args.wait)
            res = reconcile_once(client, desired, prune=args.prune)
            log.info(res.summary())
        except Exception as e:  # 루프 모드에서는 죽지 않고 다음 주기에 재시도
            notify(f"reconcile 실패: {e}")
            if not args.loop:
                return 1
        if not args.loop:
            return 0
        time.sleep(args.interval)


def cmd_status(args) -> int:
    failed = 0
    for name, st in sorted(_client(args).statuses().items()):
        tasks = st.get("tasks", [])
        print(f"{st['connector']['state']:8} {name:45} " + ",".join(f"task{t['id']}={t['state']}" for t in tasks))
        for t in tasks:
            if t["state"] == "FAILED":
                failed += 1
                print(f"         └ {(t.get('trace') or '').splitlines()[0] if t.get('trace') else ''}")
    return 1 if failed else 0


def cmd_restart_failed(args) -> int:
    for label in restart_failed(_client(args), dry_run=args.dry_run):
        print(f"restart {label}")
    return 0


def cmd_delete(args) -> int:
    _client(args).delete(args.name)
    print(f"deleted {args.name}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="registrar", description="Declarative Kafka Connect connector registrar")
    p.add_argument("--connect-url", default=os.environ.get("CONNECT_URL", "http://localhost:8083"))
    p.add_argument("-v", "--verbose", action="store_true")
    sub = p.add_subparsers(dest="cmd", required=True)

    def with_config(sp):
        sp.add_argument(
            "--config",
            action="append",
            help="설정 파일 (여러 번 지정하면 순서대로 병합). 기본: $CONNECTORS_FILE (콤마 구분)",
        )
        sp.add_argument("--only", choices=["source", "sink"], default=os.environ.get("REGISTRAR_ONLY") or None)

    sp = sub.add_parser("validate", help="설정 파일 검증만 수행")
    with_config(sp)
    sp.set_defaults(func=cmd_validate)

    sp = sub.add_parser("apply", help="설정 파일 기준으로 커넥터 생성/갱신")
    with_config(sp)
    sp.add_argument("--dry-run", action="store_true")
    sp.add_argument("--prune", action="store_true", help="설정 파일에 없는 커넥터 삭제")
    sp.add_argument("--wait", type=float, default=60)
    sp.set_defaults(func=cmd_apply)

    sp = sub.add_parser("reconcile", help="apply + FAILED 태스크 재시작 (sidecar 용)")
    with_config(sp)
    sp.add_argument("--loop", action="store_true")
    sp.add_argument("--interval", type=float, default=float(os.environ.get("RECONCILE_INTERVAL", 60)))
    sp.add_argument("--prune", action="store_true")
    sp.add_argument("--wait", type=float, default=600)
    sp.set_defaults(func=cmd_reconcile)

    sp = sub.add_parser("status", help="커넥터/태스크 상태 (FAILED 가 있으면 exit 1)")
    sp.set_defaults(func=cmd_status)

    sp = sub.add_parser("restart-failed", help="FAILED 태스크만 재시작")
    sp.add_argument("--dry-run", action="store_true")
    sp.set_defaults(func=cmd_restart_failed)

    sp = sub.add_parser("delete", help="커넥터 삭제")
    sp.add_argument("name")
    sp.set_defaults(func=cmd_delete)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    if getattr(args, "prune", False) and getattr(args, "only", None):
        print("--prune 은 --only 와 함께 쓸 수 없습니다 (다른 종류 커넥터가 삭제됨)", file=sys.stderr)
        return 2
    try:
        return args.func(args)
    except config.ConfigError as e:
        print(f"설정 오류: {e}", file=sys.stderr)
        return 2
