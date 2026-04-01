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

def test_dbt_loom_injects_microbatch_event_time():
    """Verify that dbt-loom injects the 'event_time' property to allow proper microbatch configuration"""
    import shutil

    runner = dbtRunner()

    os.chdir(f"{starting_path}/test_projects/revenue")
    shutil.rmtree("logs")
    runner.invoke(["clean"])
    runner.invoke(["deps"])
    runner.invoke(["compile"])
    runner.invoke(["build"])

    os.chdir(f"{starting_path}/test_projects/customer_success")
    runner.invoke(["clean"])
    runner.invoke(["deps"])
    shutil.rmtree("logs")
    output: dbtRunnerResult = runner.invoke([
            "build",
            "--event-time-start", "2016-09-01",
            "--event-time-end", "2016-09-15"
        ],
        vars={ 'test_microbatch_event_time': True }
    )

    assert output.exception is None

    # Use the log output to confirm that the 'event_time' warning is not present and that materialisation succeeded
    with open("logs/dbt.log") as log_file:
        log_contents = log_file.read()

        # The duckdb adapter used for testing doesn't actually support microbatch
        # This assertion can be uncommented when https://github.com/duckdb/dbt-duckdb/pull/644 is merged
        # and the adapter updated to the appropriate version
        #assert "ERROR creating sql microbatch model main.orders" not in log_contents

        # For now, we just confirm that dbt-core itself doesn't complain about missing event_time configurations
        # for models outside the 'current' project
        assert "has no 'ref' or 'source' input with an 'event_time' configuration" not in log_contents

    os.chdir(starting_path)
