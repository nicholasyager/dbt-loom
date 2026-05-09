from typing import Generator

import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs


@pytest.fixture(scope="session")
def postgres(request: pytest.FixtureRequest) -> Generator[None, None, None]:
    """Start a PostgreSQL container and set connection details as environment variables."""
    container = (
        DockerContainer(image="postgres:16")
        .with_env("POSTGRES_USER", "testuser")
        .with_env("POSTGRES_PASSWORD", "testpass")
        .with_env("POSTGRES_DB", "testdb")
        .with_exposed_ports(5432)
    )
    container.start()
    wait_for_logs(container, "database system is ready to accept connections", timeout=30)

    request.addfinalizer(container.stop)

    with pytest.MonkeyPatch.context() as context:
        context.setenv("POSTGRES_HOST", container.get_container_host_ip())
        context.setenv("POSTGRES_PORT", str(container.get_exposed_port(5432)))
        context.setenv("POSTGRES_USER", container.env["POSTGRES_USER"])
        context.setenv("POSTGRES_PASSWORD", container.env["POSTGRES_PASSWORD"])
        context.setenv("POSTGRES_DB", container.env["POSTGRES_DB"])
        context.setenv("DBT_TARGET", "postgres-dev")
        yield
