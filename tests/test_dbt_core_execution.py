import os
from pathlib import Path

import dbt
from dbt.cli.main import dbtRunner, dbtRunnerResult


import dbt.exceptions


starting_path = os.getcwd()


def test_dbt_core_runs_loom_plugin():
    """Verify that dbt-core runs the dbt-loom plugin and nodes are injected."""

    runner = dbtRunner()

    # Compile the revenue project

    os.chdir(f"{starting_path}/test_projects/revenue")
    runner.invoke(["clean"])
    runner.invoke(["deps"])
    runner.invoke(["compile"])

    # Run `build` in the customer_success project
    os.chdir(f"{starting_path}/test_projects/customer_success")
    runner.invoke(["clean"])
    runner.invoke(["deps"])
    output: dbtRunnerResult = runner.invoke(["build"])

    # Make sure nothing failed
    assert output.exception is None

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

    # Excluded packages do not get injected and loaded into a manifest.
    assert not any(["dbt_project_evaluator" in item for item in output.result])

    os.chdir(starting_path)

    assert set(output.result).issuperset(
        subset
    ), "The child project is missing expected nodes. Check that injection still works."


def test_dbt_loom_injects_dependencies():
    """Verify that dbt-core runs the dbt-loom plugin and that it flags access violations."""

    runner = dbtRunner()

    # Compile the revenue project
    os.chdir(f"{starting_path}/test_projects/revenue")
    runner.invoke(["clean"])
    runner.invoke(["deps"])
    output = runner.invoke(["compile"])

    assert output.exception is None, output.exception.get_message()  # type: ignore

    path = Path(
        f"{starting_path}/test_projects/customer_success/models/staging/stg_orders_enhanced.sql"
    )

    with open(path, "w") as file:
        file.write(
            """
            with
            upstream as (
                select * from {{ ref('revenue', 'stg_orders') }}
            )

            select * from upstream
            """
        )

    # Run `ls`` in the customer_success project
    os.chdir(f"{starting_path}/test_projects/customer_success")
    runner.invoke(["clean"])
    runner.invoke(["deps"])
    output: dbtRunnerResult = runner.invoke(["build"])

    path.unlink()

    os.chdir(starting_path)

    # Make sure nothing failed
    assert isinstance(output.exception, dbt.exceptions.DbtReferenceError)


def test_dbt_loom_injects_groups():
    """Verify that dbt-core runs the dbt-loom plugin and that it flags group violations."""

    runner = dbtRunner()

    # Compile the revenue project
    os.chdir(f"{starting_path}/test_projects/revenue")
    runner.invoke(["clean"])
    runner.invoke(["deps"])
    output = runner.invoke(["compile"])

    assert output.exception is None

    path = Path(
        f"{starting_path}/test_projects/customer_success/models/marts/marketing_lists.sql"
    )

    with open(path, "w") as file:
        file.write(
            """
            with
            upstream as (
                select * from {{ ref('accounts') }}
            )

            select * from upstream
            """
        )

    # Run `ls`` in the customer_success project
    os.chdir(f"{starting_path}/test_projects/customer_success")
    runner.invoke(["clean"])
    runner.invoke(["deps"])
    output: dbtRunnerResult = runner.invoke(["build"])

    path.unlink()

    os.chdir(starting_path)

    # Make sure nothing failed
    assert isinstance(output.exception, dbt.exceptions.DbtReferenceError)


def test_dbt_core_telemetry_blocking():
    """Verify that dbt-loom prevents telemetry about itself from being sent."""
    import shutil

    runner = dbtRunner()

    # Compile the revenue project

    os.chdir(f"{starting_path}/test_projects/revenue")
    runner.invoke(["clean"])
    runner.invoke(["deps"])
    shutil.rmtree("logs")
    runner.invoke(["compile"])

    # Check that no plugin events were sent. This is important to verify that
    # telemetry blocking is working.
    with open("logs/dbt.log") as log_file:
        assert "plugin_get_nodes" not in log_file.read()

    os.chdir(starting_path)
