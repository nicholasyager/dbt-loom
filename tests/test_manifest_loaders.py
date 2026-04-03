import json
from pathlib import Path

from typing import Dict, Generator, Tuple
from urllib.parse import urlparse

import pytest
import responses
from dbt_loom.config import (
    FileReferenceConfig,
    ManifestReference,
    ManifestReferenceType,
    LoomConfigurationError,
)
from dbt_loom.clients.dagster_cloud import (
    DagsterCloudClient,
    DagsterCloudReferenceConfig,
)
from dagster_shared.plus.config import DagsterPlusCliConfig
from dbt_loom.clients.dbx import DatabricksReferenceConfig
from dbt_loom.manifests import ManifestLoader, UnknownManifestPathType


@pytest.fixture
def example_file() -> Generator[Tuple[Path, Dict], None, None]:
    example_content = {"foo": "bar"}
    path = Path("example.json")
    with open(path, "w") as file:
        json.dump(example_content, file)
    yield path, example_content
    path.unlink()


def test_load_from_local_filesystem_pass(example_file):
    """Test that ManifestLoader can load a local JSON file."""

    path, example_content = example_file

    file_config = FileReferenceConfig(
        path=urlparse("file://" + str(Path(path).absolute()))
    )

    output = ManifestLoader.load_from_local_filesystem(file_config)

    assert output == example_content


def test_load_from_local_filesystem_local_path(example_file):
    """Test that ManifestLoader can load a local JSON file."""

    path, example_content = example_file

    file_config = FileReferenceConfig(path=str(path))  # type: ignore

    output = ManifestLoader.load_from_local_filesystem(file_config)

    assert output == example_content


def test_load_from_path_fails_invalid_scheme(example_file):
    """
    est that ManifestLoader will raise the appropriate exception if an invalid
    scheme is applied.
    """

    file_config = FileReferenceConfig(
        path=urlparse("ftp://example.com/example.json"),
    )  # type: ignore

    with pytest.raises(UnknownManifestPathType):
        ManifestLoader.load_from_path(file_config)


def test_load_from_remote_pass(example_file):
    """Test that ManifestLoader can load a remote JSON file via HTTP(S)."""

    _, example_content = example_file

    file_config = FileReferenceConfig(
        path=urlparse(
            "https://s3.us-east-2.amazonaws.com/com.nicholasyager.dbt-loom/example.json"
        ),
    )

    output = ManifestLoader.load_from_http(file_config)

    assert output == example_content


def test_manifest_loader_selection(example_file):
    """Confirm scheme parsing works for picking the manifest loader."""
    _, example_content = example_file
    manifest_loader = ManifestLoader()

    file_config = FileReferenceConfig(
        path=urlparse(
            "https://s3.us-east-2.amazonaws.com/com.nicholasyager.dbt-loom/example.json"
        ),
    )

    manifest_reference = ManifestReference(
        name="example", type=ManifestReferenceType.file, config=file_config
    )

    manifest = manifest_loader.load(manifest_reference)

    assert manifest == example_content


def test_load_from_local_filesystem_optional_missing():
    """If the manifest file does not exist, it should not raise an error if optional=True."""
    file_config = FileReferenceConfig(
        path="not_exist_manifest.json"
    )
    manifest_reference = ManifestReference(
        name="missing",
        type=ManifestReferenceType.file,
        config=file_config,
        optional=True,
    )
    manifest_loader = ManifestLoader()
    manifest = manifest_loader.load(manifest_reference)
    assert manifest is None


def test_load_from_local_filesystem_not_optional_missing():
    """If the manifest file does not exist, it should raise an error if optional=False."""
    file_config = FileReferenceConfig(
        path="not_exist_manifest.json"
    )
    manifest_reference = ManifestReference(
        name="missing",
        type=ManifestReferenceType.file,
        config=file_config,
        optional=False,
    )
    manifest_loader = ManifestLoader()
    with pytest.raises(LoomConfigurationError):
        manifest_loader.load(manifest_reference)


def test_manifest_reference_resolves_databricks_config():
    """Verify that type=databricks produces DatabricksReferenceConfig, not FileReferenceConfig."""
    ref = ManifestReference(
        name="test_dbx",
        type=ManifestReferenceType.databricks,
        config={"path": "/Volumes/my_catalog/my_schema/my_volume/manifest.json.gz"},
    )
    assert isinstance(ref.config, DatabricksReferenceConfig)
    assert ref.config.path == "/Volumes/my_catalog/my_schema/my_volume/manifest.json.gz"


def test_manifest_reference_resolves_file_config():
    """Verify that type=file still produces FileReferenceConfig."""
    ref = ManifestReference(
        name="test_file",
        type=ManifestReferenceType.file,
        config={"path": "manifest.json"},
    )
    assert isinstance(ref.config, FileReferenceConfig)


class TestDagsterCloudManifestLoader:
    """Tests for the Dagster Cloud manifest loader."""

    @responses.activate
    def test_load_manifest(self):
        """Test that DagsterCloudClient can load a manifest via the two-step API flow."""
        manifest_content = {"nodes": {"model.my_project.my_model": {}}}
        presigned_url = "https://storage.example.com/presigned/manifest.json"

        responses.post(
            "https://dagster.cloud/my-org/gen_artifact_get",
            json={"url": presigned_url},
            status=200,
        )
        responses.get(
            presigned_url,
            json=manifest_content,
            status=200,
        )

        client = DagsterCloudClient(
            organization="my-org",
            key="dagster/manifest.json",
            token="test-token",
        )
        result = client.load_manifest()
        assert result == manifest_content

    def test_missing_token(self, monkeypatch):
        """Test that DagsterCloudClient raises when no token is available."""
        monkeypatch.delenv("DAGSTER_CLOUD_API_TOKEN", raising=False)
        monkeypatch.setattr(DagsterPlusCliConfig, "exists", staticmethod(lambda: False))

        with pytest.raises(Exception, match="dg plus login"):
            DagsterCloudClient(
                organization="my-org",
                key="dagster/manifest.json",
            )

    def test_token_from_env_var(self, monkeypatch):
        """Test that DagsterCloudClient falls back to the DAGSTER_CLOUD_API_TOKEN env var."""
        monkeypatch.setenv("DAGSTER_CLOUD_API_TOKEN", "env-token")
        monkeypatch.setattr(DagsterPlusCliConfig, "exists", staticmethod(lambda: False))

        client = DagsterCloudClient(
            organization="my-org",
            key="dagster/manifest.json",
        )
        assert client._DagsterCloudClient__token == "env-token"

    def test_token_from_cli_config(self, monkeypatch):
        """Test that DagsterCloudClient falls back to DagsterPlusCliConfig."""
        monkeypatch.delenv("DAGSTER_CLOUD_API_TOKEN", raising=False)
        monkeypatch.setattr(DagsterPlusCliConfig, "exists", staticmethod(lambda: True))
        monkeypatch.setattr(
            DagsterPlusCliConfig,
            "get",
            classmethod(lambda cls: DagsterPlusCliConfig(user_token="cli-token")),
        )

        client = DagsterCloudClient(
            organization="my-org",
            key="dagster/manifest.json",
        )
        assert client._DagsterCloudClient__token == "cli-token"

    def test_explicit_token_takes_precedence(self, monkeypatch):
        """Test that an explicit token takes precedence over env var and CLI config."""
        monkeypatch.setenv("DAGSTER_CLOUD_API_TOKEN", "env-token")
        monkeypatch.setattr(DagsterPlusCliConfig, "exists", staticmethod(lambda: True))
        monkeypatch.setattr(
            DagsterPlusCliConfig,
            "get",
            classmethod(lambda cls: DagsterPlusCliConfig(user_token="cli-token")),
        )

        client = DagsterCloudClient(
            organization="my-org",
            key="dagster/manifest.json",
            token="explicit-token",
        )
        assert client._DagsterCloudClient__token == "explicit-token"

    def test_config_resolution(self):
        """Verify that type=dagster_cloud produces DagsterCloudReferenceConfig."""
        ref = ManifestReference(
            name="test_dagster",
            type=ManifestReferenceType.dagster_cloud,
            config={"organization": "my-org", "key": "dagster/manifest.json"},
        )
        assert isinstance(ref.config, DagsterCloudReferenceConfig)
        assert ref.config.organization == "my-org"
        assert ref.config.key == "dagster/manifest.json"
