import os
from pathlib import Path

import dbt
from dbt.cli.main import dbtRunner, dbtRunnerResult
from dbt.contracts.results import RunExecutionResult, NodeResult

from dbt.contracts.graph.nodes import ModelNode


import dbt.exceptions


def test_dbt_core_runs_loom_plugin():
    """Verify that dbt-core runs the dbt-loom plugin and nodes are injected."""

    runner = dbtRunner()

    # Compile the revenue project
    starting_path = os.getcwd()
    os.chdir(f"{starting_path}/test_projects/revenue")
    runner.invoke(["deps"])
    runner.invoke(["compile"])

    # Run `build` in the customer_success project
    os.chdir(f"{starting_path}/test_projects/customer_success")
    runner.invoke(["deps"])
    output: dbtRunnerResult = runner.invoke(["build"])

    # Make sure nothing failed
    assert output.exception is None

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

    os.chdir(starting_path)

    assert set(output.result).issuperset(
        subset
    ), "The child project is missing expected nodes. Check that injection still works."


def test_dbt_loom_injects_dependencies():
    """Verify that dbt-core runs the dbt-loom plugin and that it flags access violations."""

    starting_path = os.getcwd()
    path = Path(
        f"{starting_path}/test_projects/customer_success/models/staging/stg_orders_enhanced.sql"
    )
    print(path)
    with open(path, "w") as file:
        file.write(
            """
            with
            upstream as (
                select * from {{ ref('stg_orders') }}
            )

            select * from upstream
            """
        )

    runner = dbtRunner()

    # Compile the revenue project
    os.chdir(f"{starting_path}/test_projects/revenue")
    runner.invoke(["deps"])
    runner.invoke(["compile"])

    # Run `ls`` in the customer_success project
    os.chdir(f"{starting_path}/test_projects/customer_success")
    runner.invoke(["deps"])
    output: dbtRunnerResult = runner.invoke(["build"])

    path.unlink()
    os.chdir(starting_path)

    # Make sure nothing failed
    assert isinstance(output.exception, dbt.exceptions.DbtReferenceError)
