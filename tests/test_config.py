import textwrap

import pytest

from registrar import config

BASE = """
defaults:
  source:
    connector.class: io.debezium.connector.mysql.MySqlConnector
    database.password: ${file:/secrets/mysql/mysql.properties:password}
    include.schema.changes: true
    transforms.route.replacement: '$3'
  sink:
    connector.class: com.wepay.kafka.connect.bigquery.BigQuerySinkConnector
    project: ${env:GCP_PROJECT}
sources:
  - name: src-a
    config: {database.hostname: db1, database.server.id: 1}
sinks:
  config: {datasets: .*=raw}
  topics: [orders, customers]
  overrides:
    orders: {tasks.max: 3}
"""
ENV = {"GCP_PROJECT": "demo"}


def write(tmp_path, text):
    p = tmp_path / "c.yaml"
    p.write_text(textwrap.dedent(text))
    return p


def test_merge_and_stringify(tmp_path):
    conns = {c.name: c for c in config.load(write(tmp_path, BASE), env=ENV)}
    assert set(conns) == {"src-a", "bq-sink-orders", "bq-sink-customers"}
    src = conns["src-a"].config
    assert src["include.schema.changes"] == "true"
    assert src["database.server.id"] == "1"
    # 소스별 history 토픽 자동 분리
    assert src["database.history.kafka.topic"] == "schema-changes.src-a"


def test_file_provider_and_regex_group_are_preserved(tmp_path):
    src = config.load(write(tmp_path, BASE), env=ENV)[0].config
    assert src["database.password"] == "${file:/secrets/mysql/mysql.properties:password}"
    assert src["transforms.route.replacement"] == "$3"


def test_env_expansion_and_overrides(tmp_path):
    conns = {c.name: c for c in config.load(write(tmp_path, BASE), env=ENV)}
    assert conns["bq-sink-orders"].config["project"] == "demo"
    assert conns["bq-sink-orders"].config["tasks.max"] == "3"
    assert conns["bq-sink-orders"].config["topics"] == "orders"
    assert "tasks.max" not in conns["bq-sink-customers"].config


def test_missing_env_raises(tmp_path):
    with pytest.raises(config.ConfigError, match="GCP_PROJECT"):
        config.load(write(tmp_path, BASE), env={})


def test_only_source_skips_sink_env(tmp_path):
    conns = config.load(write(tmp_path, BASE), env={}, only="source")
    assert [c.name for c in conns] == ["src-a"]


def test_zero_width_space_in_key_is_rejected(tmp_path):
    bad = BASE.replace("include.schema.changes", "include.schema\u200b.changes")
    with pytest.raises(config.ConfigError, match="비정상 문자"):
        config.load(write(tmp_path, bad), env=ENV)


def test_duplicate_server_id_on_same_host_is_rejected(tmp_path):
    dup = BASE.replace(
        "sinks:",
        "  - name: src-b\n    config: {database.hostname: db1, database.server.id: 1}\nsinks:",
    )
    with pytest.raises(config.ConfigError, match="server.id"):
        config.load(write(tmp_path, dup), env=ENV)


def test_same_server_id_on_different_hosts_is_allowed(tmp_path):
    ok = BASE.replace(
        "sinks:",
        "  - name: src-b\n    config: {database.hostname: db2, database.server.id: 1}\nsinks:",
    )
    assert len(config.load(write(tmp_path, ok), env=ENV)) == 4


def test_example_files_are_valid():
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    env = {"GCP_PROJECT": "p", "BQ_DATASET": "d"}
    assert config.load(root / "config/connectors.yaml", env=env)

    base = [root / "k8s/base/connectors/defaults.yaml", root / "k8s/base/connectors/sinks.yaml"]
    overlays = sorted(root.glob("k8s/overlays/*/connectors.yaml"))
    assert len(overlays) == 2
    for ov in overlays:
        conns = config.load([*base, ov], env=env)
        assert {c.kind for c in conns} == {"source", "sink"}
