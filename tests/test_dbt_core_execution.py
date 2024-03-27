import os

from dbt.cli.main import dbtRunner, dbtRunnerResult
from importlib.metadata import version


def test_dbt_core_runs_loom_plugin():
    """Verify that dbt-core runs the dbt-loom plugin and nodes are injected."""

    runner = dbtRunner()

    # Compile the revenue project
    starting_path = os.getcwd()
    os.chdir(f"{starting_path}/test_projects/revenue")
    runner.invoke(["deps"])
    runner.invoke(["compile"])

    # Run `ls`` in the customer_success project
    os.chdir(f"{starting_path}/test_projects/customer_success")
    runner.invoke(["deps"])
    output: dbtRunnerResult = runner.invoke(["ls"])

    # Make sure nothing failed
    assert output.exception is None

    # Check for injection
    assert isinstance(output.result, list)

    # Check that the versioned models work.
    subset = {
        "revenue.orders.v1",
        "revenue.orders.v2",
    }

    assert set(output.result).issuperset(
        subset
    ), "The child project is missing expected nodes. Check that injection still works."
