import json
import os
import plistlib
import subprocess
from pathlib import Path
from unittest.mock import Mock

import pytest

from inkbox_claude import daemon


@pytest.fixture(autouse=True)
def isolated_home(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("INKBOX_CLAUDE_HOME", str(tmp_path / ".inkbox-claude"))
    monkeypatch.setattr(daemon.os, "getuid", lambda: 501)
    monkeypatch.setattr(daemon.subprocess, "run", Mock(side_effect=AssertionError("mock service commands")))


def install_definition(monkeypatch, system, *, profile=None):
    monkeypatch.setattr(daemon.platform, "system", lambda: system)
    if system == "Darwin":
        definition = Path.home() / "Library/LaunchAgents/ai.inkbox.claude.plist"
        definition.parent.mkdir(parents=True)
        values = {"Label": "ai.inkbox.claude", "ProgramArguments": ["inkbox-claude", "run"], "KeepAlive": True}
        if profile is not None:
            values["EnvironmentVariables"] = {"INKBOX_CLAUDE_HOME": str(profile)}
        definition.write_bytes(plistlib.dumps(values))
    else:
        definition = Path.home() / ".config/systemd/user/inkbox-claude.service"
        definition.parent.mkdir(parents=True)
        content = "[Service]\nExecStart=inkbox-claude run\nRestart=on-failure\n"
        if profile is not None:
            content += f"Environment={json.dumps('INKBOX_CLAUDE_HOME=' + str(profile).replace('%', '%%'))}\n"
        definition.write_text(content)
    return definition


def status_result(system, *, pid=4242, loaded=True):
    if system == "Darwin":
        if not loaded:
            return subprocess.CompletedProcess([], 113, "", 'Could not find service "ai.inkbox.claude" in domain')
        state = "running" if pid else "waiting"
        stdout = f"gui/501/ai.inkbox.claude = {{\n\tstate = {state}\n"
        if pid:
            stdout += f"\tpid = {pid}\n"
        stdout += "\tenvironment = {\n\t\tpid = 99999\n\t}\n}\n"
    else:
        state = "active" if pid else "inactive"
        stdout = f"LoadState={'loaded' if loaded else 'not-found'}\nActiveState={state}\nMainPID={pid or 0}\n"
    return subprocess.CompletedProcess([], 0, stdout, "")


@pytest.mark.parametrize("system", ["Darwin", "Linux"])
def test_running_service_is_visible_without_a_pid_file(monkeypatch, system, capsys):
    install_definition(monkeypatch, system)
    run = Mock(return_value=status_result(system))
    monkeypatch.setattr(daemon.subprocess, "run", run)

    assert daemon.status() == 0
    assert daemon.running_pid() == 4242
    assert not daemon._pid_file().exists()
    assert "running (pid 4242" in capsys.readouterr().out
    if system == "Darwin":
        expected = ["launchctl", "print", "gui/501/ai.inkbox.claude"]
    else:
        expected = ["systemctl", "--user", "show", "inkbox-claude.service", "--property=LoadState,ActiveState,MainPID"]
    assert all(call.args[0] == expected and call.kwargs["timeout"] == 5 for call in run.call_args_list)


@pytest.mark.parametrize("system", ["Darwin", "Linux"])
@pytest.mark.parametrize("loaded", [True, False])
def test_stopped_service_is_not_reported_as_running(monkeypatch, system, loaded, capsys):
    install_definition(monkeypatch, system)
    monkeypatch.setattr(daemon.subprocess, "run", Mock(return_value=status_result(system, pid=None, loaded=loaded)))
    assert daemon.status() == 1
    assert daemon.running_pid() is None
    assert "not running" in capsys.readouterr().out


@pytest.mark.parametrize("system", ["Darwin", "Linux"])
def test_start_does_not_duplicate_a_running_service(monkeypatch, system):
    install_definition(monkeypatch, system)
    run = Mock(return_value=status_result(system))
    spawn = Mock(side_effect=AssertionError("must not spawn another bridge"))
    monkeypatch.setattr(daemon.subprocess, "run", run)
    monkeypatch.setattr(daemon.subprocess, "Popen", spawn)
    assert daemon.start() == 0
    assert run.call_count == 1
    spawn.assert_not_called()


@pytest.mark.parametrize("system,action,loaded,expected", [
    ("Darwin", "start", True, ["launchctl", "kickstart", "gui/501/ai.inkbox.claude"]),
    ("Darwin", "start", False, ["launchctl", "bootstrap", "gui/501"]),
    ("Darwin", "restart", True, ["launchctl", "kickstart", "-k", "gui/501/ai.inkbox.claude"]),
    ("Darwin", "restart", False, ["launchctl", "bootstrap", "gui/501"]),
    ("Darwin", "stop", True, ["launchctl", "bootout", "gui/501/ai.inkbox.claude"]),
    ("Linux", "start", True, ["systemctl", "--user", "start", "inkbox-claude.service"]),
    ("Linux", "restart", True, ["systemctl", "--user", "restart", "inkbox-claude.service"]),
    ("Linux", "stop", True, ["systemctl", "--user", "stop", "inkbox-claude.service"]),
])
def test_lifecycle_uses_the_service_manager(monkeypatch, system, action, loaded, expected):
    definition = install_definition(monkeypatch, system)
    run = Mock(side_effect=[status_result(system, pid=None, loaded=loaded), subprocess.CompletedProcess([], 0, "", "")])
    monkeypatch.setattr(daemon.subprocess, "run", run)
    monkeypatch.setattr(daemon.subprocess, "Popen", Mock(side_effect=AssertionError("unexpected standalone process")))
    monkeypatch.setattr(daemon.os, "kill", Mock(side_effect=AssertionError("unexpected direct signal")))

    assert getattr(daemon, action)() == 0
    if "bootstrap" in expected:
        expected = [*expected, str(definition)]
    assert run.call_args.args[0] == expected
    assert run.call_args.kwargs["timeout"] == 30
    assert definition.exists()


@pytest.mark.parametrize("system", ["Darwin", "Linux"])
@pytest.mark.parametrize("operation", ["status", "start", "stop", "restart"])
@pytest.mark.parametrize("failure", ["timeout", "missing_command", "denied", "malformed"])
def test_unavailable_status_never_becomes_a_standalone_start(monkeypatch, system, operation, failure, capsys):
    install_definition(monkeypatch, system)
    if failure == "timeout":
        run = Mock(side_effect=subprocess.TimeoutExpired("service-manager", 5))
    elif failure == "missing_command":
        run = Mock(side_effect=FileNotFoundError())
    elif failure == "denied":
        run = Mock(return_value=subprocess.CompletedProcess([], 1, "", "permission denied; private diagnostic"))
    else:
        run = Mock(return_value=subprocess.CompletedProcess([], 0, "unexpected output", ""))
    monkeypatch.setattr(daemon.subprocess, "run", run)
    monkeypatch.setattr(daemon.subprocess, "Popen", Mock(side_effect=AssertionError("unexpected standalone process")))
    monkeypatch.setattr(daemon.os, "kill", Mock(side_effect=AssertionError("unexpected direct signal")))

    assert getattr(daemon, operation)() == 1
    assert daemon.running_pid() is None
    output = capsys.readouterr().out
    assert "Cannot determine bridge status" in output
    assert "not running" not in output
    assert "private diagnostic" not in output


@pytest.mark.parametrize("system", ["Darwin", "Linux"])
@pytest.mark.parametrize("failure", ["timeout", "nonzero"])
def test_control_failure_is_reported_without_starting_another_process(monkeypatch, system, failure):
    install_definition(monkeypatch, system)
    result = subprocess.TimeoutExpired("service-manager", 30) if failure == "timeout" else subprocess.CompletedProcess([], 1)
    run = Mock(side_effect=[status_result(system), result])
    monkeypatch.setattr(daemon.subprocess, "run", run)
    monkeypatch.setattr(daemon.subprocess, "Popen", Mock(side_effect=AssertionError("unexpected standalone process")))
    assert daemon.restart() == 1
    assert run.call_count == 2


@pytest.mark.parametrize("system", ["Darwin", "Linux"])
def test_standalone_profile_does_not_control_another_profiles_service(tmp_path, monkeypatch, system):
    install_definition(monkeypatch, system)
    profile = tmp_path / "another-profile"
    monkeypatch.setenv("INKBOX_CLAUDE_HOME", str(profile))
    daemon._pid_file().write_text(f"{os.getpid()}\n")
    assert daemon._managed_service() is None
    assert daemon.running_pid() == os.getpid()
    assert daemon.status() == 0
    daemon.subprocess.run.assert_not_called()


@pytest.mark.parametrize("system", ["Darwin", "Linux"])
def test_custom_service_profile_is_detected(tmp_path, monkeypatch, system):
    profile = tmp_path / "custom profile 50%"
    monkeypatch.setenv("INKBOX_CLAUDE_HOME", str(profile))
    install_definition(monkeypatch, system, profile=profile)
    monkeypatch.setattr(daemon.subprocess, "run", Mock(return_value=status_result(system)))
    assert daemon.running_pid() == 4242


def test_launchd_stop_then_start_unloads_and_bootstraps(monkeypatch):
    definition = install_definition(monkeypatch, "Darwin")
    loaded = True
    commands = []

    def run(command, **kwargs):
        nonlocal loaded
        commands.append(command)
        if command[1] == "print":
            return status_result("Darwin", pid=4242 if loaded else None, loaded=loaded)
        loaded = command[1] == "bootstrap"
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(daemon.subprocess, "run", run)
    monkeypatch.setattr(daemon.subprocess, "Popen", Mock(side_effect=AssertionError("unexpected standalone process")))
    assert daemon.stop() == 0
    assert not loaded
    assert daemon.start() == 0
    assert loaded
    assert commands[1] == ["launchctl", "bootout", "gui/501/ai.inkbox.claude"]
    assert commands[3] == ["launchctl", "bootstrap", "gui/501", str(definition)]


def test_launchd_install_preserves_profile_and_escapes_paths(tmp_path, monkeypatch):
    monkeypatch.setattr(daemon.platform, "system", lambda: "Darwin")
    profile = tmp_path / "profile & work"
    monkeypatch.setenv("INKBOX_CLAUDE_HOME", str(profile))
    monkeypatch.setattr(daemon.subprocess, "run", Mock(return_value=subprocess.CompletedProcess([], 0, "", "")))
    assert daemon._install_launchd("/Applications/My & Tools/inkbox-claude", "/work/a&b/.env")
    definition = Path.home() / "Library/LaunchAgents/ai.inkbox.claude.plist"
    data = plistlib.loads(definition.read_bytes())
    assert data["ProgramArguments"] == ["/Applications/My & Tools/inkbox-claude", "run"]
    assert data["EnvironmentVariables"]["INKBOX_CLAUDE_HOME"] == str(profile)
    assert data["EnvironmentVariables"]["INKBOX_CLAUDE_ENV_FILE"] == "/work/a&b/.env"
    assert daemon._service_profile_matches("launchd", definition)


def test_systemd_install_preserves_profile_with_spaces_percent_and_unicode(tmp_path, monkeypatch):
    monkeypatch.setattr(daemon.platform, "system", lambda: "Linux")
    profile = tmp_path / "profile ü 50%"
    monkeypatch.setenv("INKBOX_CLAUDE_HOME", str(profile))
    monkeypatch.setattr(daemon.subprocess, "run", Mock(return_value=subprocess.CompletedProcess([], 0, "", "")))
    executable = str(profile / "bin/inkbox-claude")
    assert daemon._install_systemd_user(executable, "/work/my project/.env")
    definition = Path.home() / ".config/systemd/user/inkbox-claude.service"
    assert 'Environment="INKBOX_CLAUDE_ENV_FILE=/work/my project/.env"' in definition.read_text()
    assert f'ExecStart="{executable.replace("%", "%%")}" run' in definition.read_text()
    assert daemon._service_profile_matches("systemd", definition)


@pytest.mark.parametrize("system", ["Darwin", "Linux"])
def test_existing_standalone_process_prevents_a_second_managed_start(monkeypatch, system):
    install_definition(monkeypatch, system)
    run = Mock(return_value=status_result(system, pid=None))
    monkeypatch.setattr(daemon.subprocess, "run", run)
    monkeypatch.setattr(daemon, "_read_pid", lambda: 31337)
    monkeypatch.setattr(daemon.subprocess, "Popen", Mock(side_effect=AssertionError("unexpected standalone process")))
    assert daemon.start() == 0
    assert daemon.running_pid() == 31337
    assert all(call.args[0][1] in {"print", "--user"} for call in run.call_args_list)
    assert all("start" not in call.args[0] for call in run.call_args_list)


@pytest.mark.parametrize("system", ["Darwin", "Linux"])
def test_failed_standalone_stop_prevents_managed_restart(monkeypatch, system):
    install_definition(monkeypatch, system)
    run = Mock(return_value=status_result(system, pid=None))
    monkeypatch.setattr(daemon.subprocess, "run", run)
    monkeypatch.setattr(daemon, "_read_pid", lambda: 31337)
    stop = Mock(return_value=1)
    monkeypatch.setattr(daemon, "_stop_standalone", stop)
    assert daemon.restart() == 1
    stop.assert_called_once_with()
    assert run.call_count == 1


def test_standalone_restart_does_not_start_after_stop_failure(monkeypatch):
    monkeypatch.setattr(daemon, "_stop_standalone", Mock(return_value=1))
    start = Mock(side_effect=AssertionError("unexpected start"))
    monkeypatch.setattr(daemon, "_start_standalone", start)
    assert daemon.restart() == 1
    start.assert_not_called()
