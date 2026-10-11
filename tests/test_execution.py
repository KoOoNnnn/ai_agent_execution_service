"""Retain existing fault scenarios, with one independent process per scenario group."""

import pytest


pytestmark = pytest.mark.integration


@pytest.mark.parametrize("module", [
    "check_engine", "check_actions", "check_inventory", "check_execution_budgets",
    "check_leases", "check_retry",
])
def test_execution_scenarios(harness, module):
    overrides = {"LEASE_TTL_SECONDS": "2", "HEARTBEAT_INTERVAL_SECONDS": "0.5"} if module == "check_retry" else None
    harness.run("scripts." + module, overrides=overrides)


def test_schema_matches_migrations(harness):
    harness.run("alembic", "check")
