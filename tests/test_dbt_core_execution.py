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

    # Make sure nothing failed
    assert isinstance(output.exception, dbt.exceptions.DbtReferenceError)
