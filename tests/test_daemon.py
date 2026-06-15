import os

from inkbox_claude import cli, daemon


def test_read_pid_none_when_no_file(tmp_path, monkeypatch):
    monkeypatch.setenv("INKBOX_CLAUDE_HOME", str(tmp_path))
    assert daemon._read_pid() is None


def test_read_pid_returns_live_process(tmp_path, monkeypatch):
    monkeypatch.setenv("INKBOX_CLAUDE_HOME", str(tmp_path))
    daemon._pid_file().write_text(f"{os.getpid()}\n")  # our own pid is alive
    assert daemon._read_pid() == os.getpid()


def test_read_pid_clears_stale_file(tmp_path, monkeypatch):
    monkeypatch.setenv("INKBOX_CLAUDE_HOME", str(tmp_path))
    # PID 0 is never a normal user process — os.kill(0, 0) raises, so it's stale.
    daemon._pid_file().write_text("999999999\n")
    assert daemon._read_pid() is None
    assert not daemon._pid_file().exists()  # stale file is cleaned up


def test_status_reports_not_running(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("INKBOX_CLAUDE_HOME", str(tmp_path))
    assert daemon.status() == 1
    assert "not running" in capsys.readouterr().out


def test_stop_is_a_noop_when_not_running(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("INKBOX_CLAUDE_HOME", str(tmp_path))
    assert daemon.stop() == 0
    assert "Not running" in capsys.readouterr().out


def test_maybe_load_env_file_fills_missing(tmp_path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text('export INKBOX_API_KEY="ApiKey_x"\nINKBOX_IDENTITY=agent\n')
    monkeypatch.setenv("INKBOX_CLAUDE_ENV_FILE", str(env_file))
    monkeypatch.delenv("INKBOX_API_KEY", raising=False)
    monkeypatch.setenv("INKBOX_IDENTITY", "already-set")

    daemon._maybe_load_env_file()

    assert os.environ["INKBOX_API_KEY"] == "ApiKey_x"   # filled from file
    assert os.environ["INKBOX_IDENTITY"] == "already-set"  # real env wins


def test_maybe_load_env_file_falls_back_to_state_dir(tmp_path, monkeypatch):
    # No INKBOX_CLAUDE_ENV_FILE and no ./.env — the global install location
    # (~/.inkbox-claude/.env, via INKBOX_CLAUDE_HOME) is the fallback.
    home = tmp_path / "home"
    home.mkdir()
    (home / ".env").write_text("INKBOX_API_KEY=ApiKey_global\n")
    cwd = tmp_path / "elsewhere"
    cwd.mkdir()
    monkeypatch.chdir(cwd)
    monkeypatch.delenv("INKBOX_CLAUDE_ENV_FILE", raising=False)
    monkeypatch.setenv("INKBOX_CLAUDE_HOME", str(home))
    monkeypatch.delenv("INKBOX_API_KEY", raising=False)

    daemon._maybe_load_env_file()

    assert os.environ["INKBOX_API_KEY"] == "ApiKey_global"


def test_launcher_path_is_a_string():
    assert isinstance(daemon._launcher_path(), str)


def test_install_autostart_writes_and_enables_systemd_unit(tmp_path, monkeypatch):
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setattr(daemon.Path, "home", classmethod(lambda cls: home))
    monkeypatch.setattr(daemon.platform, "system", lambda: "Linux")
    monkeypatch.setattr(daemon, "_read_pid", lambda: None)  # nothing to stop

    calls = []

    class _OK:
        returncode = 0
        stderr = ""

    def fake_run(cmd, **_kwargs):
        calls.append(cmd)
        return _OK()

    monkeypatch.setattr(daemon.subprocess, "run", fake_run)

    assert daemon.install_autostart("/home/me/.inkbox-claude/.env") is True

    unit = home / ".config" / "systemd" / "user" / "inkbox-claude.service"
    text = unit.read_text()
    assert "ExecStart=" in text and " run" in text
    assert "INKBOX_CLAUDE_ENV_FILE=/home/me/.inkbox-claude/.env" in text
    # daemon-reload + enable --now were invoked.
    assert ["systemctl", "--user", "daemon-reload"] in calls
    assert any("enable" in c for c in calls)


def test_install_autostart_reports_failure_when_enable_fails(tmp_path, monkeypatch):
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setattr(daemon.Path, "home", classmethod(lambda cls: home))
    monkeypatch.setattr(daemon.platform, "system", lambda: "Linux")
    monkeypatch.setattr(daemon, "_read_pid", lambda: None)

    class _Fail:
        returncode = 1
        stderr = "Failed to connect to bus"

    monkeypatch.setattr(daemon.subprocess, "run", lambda cmd, **_k: _Fail())

    # Unit still written, but returns False so the wizard falls back.
    assert daemon.install_autostart("/x/.env") is False
    assert (home / ".config" / "systemd" / "user" / "inkbox-claude.service").exists()


def test_cli_routes_daemon_commands(monkeypatch):
    calls = []
    monkeypatch.setattr(cli.daemon, "start", lambda: calls.append("start") or 0)
    monkeypatch.setattr(cli.daemon, "stop", lambda: calls.append("stop") or 0)
    monkeypatch.setattr(cli.daemon, "status", lambda: calls.append("status") or 0)
    monkeypatch.setattr(cli.daemon, "restart", lambda: calls.append("restart") or 0)
    monkeypatch.setattr(cli.daemon, "run_foreground", lambda: calls.append("run") or 0)

    for cmd in ("run", "start", "stop", "restart", "status"):
        assert cli.main([cmd]) == 0
    assert calls == ["run", "start", "stop", "restart", "status"]
