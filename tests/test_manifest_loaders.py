import json
from pathlib import Path

from typing import Dict, Generator, Tuple
from urllib.parse import urlparse

import pytest
from dbt_loom.config import (
    FileReferenceConfig,
    ManifestReference,
    ManifestReferenceType,
)
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
