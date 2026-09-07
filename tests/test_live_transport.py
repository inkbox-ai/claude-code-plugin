"""Connection-only retries must not replay an uncertain HTTP request."""

import pytest


@pytest.mark.parametrize("failure,expected_attempts", [("ConnectError", 3), ("ReadError", 1)])
def test_live_transport_retries_only_connection_establishment(monkeypatch, failure, expected_attempts):
    httpx = pytest.importorskip("httpx")
    import httpcore
    from tests.live import conftest

    monkeypatch.setattr(conftest, "REMOTE_KEY", "test-remote")
    monkeypatch.setattr(conftest, "AUT_KEY", "test-aut")
    conftest.retry_connection_setup.__wrapped__(monkeypatch)
    attempts = []

    def fail_connect(*args, **kwargs):
        attempts.append(1)
        raise getattr(httpcore, failure)("synthetic failure")

    monkeypatch.setattr(httpcore.SyncBackend, "connect_tcp", fail_connect)
    monkeypatch.setattr(httpcore.SyncBackend, "sleep", lambda *args: None)
    with httpx.Client(trust_env=False) as client:
        with pytest.raises(getattr(httpx, failure)):
            client.post("https://example.test/send", json={"text": "test"})
    assert len(attempts) == expected_attempts
