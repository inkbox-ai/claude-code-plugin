"""Keep the long-running live scenario strict while specifying native wait mode."""
from types import SimpleNamespace

import pytest
from tests.live import a2a_driver as driver


@pytest.mark.parametrize('failure', [None, 'missing-progress', 'bad-cadence', 'bad-order', 'bad-final'])
def test_foreground_scenario_retains_progress_and_completion_requirements(monkeypatch, failure):
    task = SimpleNamespace(id='synthetic-task')
    receipt = 'Task synthetic-task received. ' + driver.PROGRESS_RECEIPT_SUFFIX
    history = [receipt, 'I am calculating the second sum. (60s elapsed)',
               'I am combining the two sums. (120s elapsed)',
               'a2a-ci-inbound-progress-synthetic 4 + 6 = 10']
    if failure == 'missing-progress': history[1:3] = []
    elif failure == 'bad-cadence': history[2] = 'I am combining the two sums. (180s elapsed)'
    elif failure == 'bad-order': history[0], history[1] = history[1], history[0]
    elif failure == 'bad-final': history[-1] = 'Started the background command.'
    final = SimpleNamespace(raw={'history': [
        {'role': 'agent', 'parts': [{'text': text}]} for text in history]})
    prompts, cancelled = [], []
    def send(_a2a, _target, text):
        prompts.append(text)
        return task
    monkeypatch.setattr(driver, '_send_task', send)
    monkeypatch.setattr(driver, '_wait_for_history_message', lambda *a, **kw: (task, receipt))
    monkeypatch.setattr(driver, '_wait_protocol_task', lambda *a, **kw: final)
    monkeypatch.setattr(driver, '_cancel_if_open', lambda *a: cancelled.append(True))
    if failure:
        with pytest.raises(AssertionError): driver._inbound_progress(None, None, 210, 'synthetic')
    else:
        driver._inbound_progress(None, None, 210, 'synthetic')
    assert len(prompts) == 1 and cancelled == [True]
    assert 'run_in_background=false' in prompts[0] and 'timeout=180000' in prompts[0]
    assert prompts[0].count('65 seconds') == 2
    assert "Wait for the command's completed result before answering" in prompts[0]
