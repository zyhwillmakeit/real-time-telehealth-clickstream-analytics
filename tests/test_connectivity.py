from scripts.check_connectivity import Result, blocked, exit_code, required_values


def test_required_values_returns_names_only() -> None:
    env = {"HOST": "service.example", "PASSWORD": ""}

    assert required_values(env, ("HOST", "PASSWORD", "TOKEN")) == ["PASSWORD", "TOKEN"]


def test_blocked_result_does_not_receive_values() -> None:
    result = blocked("example", ["PASSWORD", "TOKEN"])

    assert result.status == "BLOCKED"
    assert result.detail == "Missing: PASSWORD, TOKEN"


def test_exit_codes_distinguish_failure_from_configuration() -> None:
    assert exit_code([Result("a", "PASS", "read", "ok")]) == 0
    assert exit_code([Result("a", "BLOCKED", "configuration", "missing")]) == 2
    assert exit_code([Result("a", "FAILED", "read", "TimeoutError")]) == 1
