import logging

from inkbox_claude import gateway


def _record(message, *args, level=logging.WARNING):
    return logging.LogRecord("inkbox.tunnels", level, __file__, 1, message, args, None)


def test_expected_intake_idle_cap_warning_is_filtered():
    record = _record(
        "/_system/intake slot=%d -> status=%s reason=%r body=%r",
        10,
        "408",
        "intake-idle-cap",
        b"",
    )
    assert gateway._ExpectedTunnelIdleFilter().filter(record) is False


def test_auth_failure_warning_remains_visible():
    record = _record(
        "/_system/intake slot=%d -> status=%s reason=%r body=%r",
        10,
        "401",
        "owner-token-invalid",
        b"",
    )
    assert gateway._ExpectedTunnelIdleFilter().filter(record) is True


def test_408_with_other_reason_remains_visible():
    # Same status, different reason — only the exact idle-cap shape is muted.
    record = _record(
        "/_system/intake slot=%d -> status=%s reason=%r body=%r",
        10,
        "408",
        "handshake-timeout",
        b"",
    )
    assert gateway._ExpectedTunnelIdleFilter().filter(record) is True


def test_unrelated_tunnel_warning_remains_visible():
    assert gateway._ExpectedTunnelIdleFilter().filter(_record("tunnel runtime disconnected")) is True


def test_installing_filter_is_idempotent():
    logger = logging.getLogger("inkbox.tunnels")
    original_filters = list(logger.filters)
    try:
        logger.filters = [
            item for item in logger.filters
            if not isinstance(item, gateway._ExpectedTunnelIdleFilter)
        ]

        gateway._install_tunnel_log_filter()
        gateway._install_tunnel_log_filter()

        installed = [
            item for item in logger.filters
            if isinstance(item, gateway._ExpectedTunnelIdleFilter)
        ]
        assert len(installed) == 1
    finally:
        logger.filters = original_filters
