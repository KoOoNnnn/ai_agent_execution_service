import pytest


pytestmark = pytest.mark.integration


def test_delivery_ack_and_dead_letter(api):
    api.run("scripts.check_async_runs", "prepare")
    with api.process("worker", "app.worker", "--decision-provider", "fake-caller"):
        api.run("scripts.check_async_runs", "verify")


@pytest.mark.parametrize("mode", [[], ["--after-reservation"], ["--during-retry"]],
                         ids=["after-progress", "after-effect", "during-third-attempt"])
def test_real_worker_crash_recovery(api, mode):
    # The check starts and terminates only its own fixture/replacement Workers.
    api.run("scripts.check_worker_crash", *mode, timeout=60)
