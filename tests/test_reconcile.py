import pytest

from fake_connect import FakeConnect
from registrar import cli
from registrar.client import ConnectClient
from registrar.config import Connector
from registrar.reconcile import apply, reconcile_once

A = Connector("a", {"connector.class": "X", "k": "1"}, "source")
B = Connector("b", {"connector.class": "Y", "k": "2"}, "sink")


@pytest.fixture
def fake():
    f = FakeConnect()
    yield f
    f.close()


def test_apply_creates_then_is_idempotent(fake):
    client = ConnectClient(fake.url)
    res = apply(client, [A, B])
    assert sorted(res.created) == ["a", "b"]

    res = apply(client, [A, B])
    assert sorted(res.unchanged) == ["a", "b"]
    assert [c for c in fake.calls if c[0] == "PUT"].__len__() == 2  # 두 번째 apply 는 PUT 없음


def test_apply_updates_drifted_config(fake):
    client = ConnectClient(fake.url)
    apply(client, [A])
    fake.configs["a"]["k"] = "changed-by-hand"
    res = apply(client, [A])
    assert res.updated == ["a"]
    assert fake.configs["a"]["k"] == "1"


def test_unmanaged_kept_unless_prune(fake):
    client = ConnectClient(fake.url)
    fake.configs["legacy"] = {"connector.class": "Z"}
    assert apply(client, [A]).unmanaged == ["legacy"]
    assert "legacy" in fake.configs
    assert apply(client, [A], prune=True).deleted == ["legacy"]
    assert "legacy" not in fake.configs


def test_dry_run_changes_nothing(fake):
    res = apply(ConnectClient(fake.url), [A], dry_run=True)
    assert res.created == ["a"] and fake.configs == {}


def test_reconcile_restarts_failed_tasks(fake):
    client = ConnectClient(fake.url)
    apply(client, [A, B])
    fake.failed.add(("b", 0))
    res = reconcile_once(client, [A, B])
    assert res.restarted == ["b/task0"]
    assert ("POST", "/connectors/b/tasks/0/restart") in fake.calls


def test_cli_status_exit_code(fake, capsys):
    apply(ConnectClient(fake.url), [A])
    assert cli.main(["--connect-url", fake.url, "status"]) == 0
    fake.failed.add(("a", 0))
    assert cli.main(["--connect-url", fake.url, "status"]) == 1
    assert "boom" in capsys.readouterr().out


def test_cli_rejects_prune_with_only(fake):
    assert cli.main(["--connect-url", fake.url, "apply", "--only", "source", "--prune"]) == 2
