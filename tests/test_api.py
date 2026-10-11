import pytest


pytestmark = pytest.mark.integration


@pytest.mark.parametrize("module", ["check_idempotency", "check_cancellation", "check_steps_api"])
def test_api_scenarios(api, module):
    # No consumer is running, so cancellation and submission races are deterministic.
    api.run("scripts." + module)
