import json
from pathlib import Path
import subprocess
from pydantic import AnyUrl
import pytest
from dbt_loom.config import FileReferenceConfig
from dbt_loom.manifests import ManifestLoader, UnknownManifestPathType


def test_load_from_local_filesystem_pass():
    """Test that ManifestLoader can load a local JSON file."""

    example_content = {"foo": "bar"}
    path = Path("example.json")

    with open(path, "w") as file:
        json.dump(example_content, file)

    file_config = FileReferenceConfig(
        path=AnyUrl("file://" + str(Path(path).absolute()))
    )

    output = ManifestLoader.load_from_local_filesystem(file_config)
    path.unlink()

    assert output == example_content


def test_load_from_local_filesystem_local_path():
    """Test that ManifestLoader can load a local JSON file."""

    example_content = {"foo": "bar"}
    path = Path("example.json")

    with open(path, "w") as file:
        json.dump(example_content, file)

    file_config = FileReferenceConfig(path=str(path))  # type: ignore

    output = ManifestLoader.load_from_local_filesystem(file_config)
    path.unlink()

    assert output == example_content


def test_load_from_path_fails_invalid_scheme():
    """
    est that ManifestLoader will raise the appropriate exception if an invalid
    scheme is applied.
    """

    file_config = FileReferenceConfig(path=AnyUrl("ftp://example.com/example.json"))  # type: ignore

    with pytest.raises(UnknownManifestPathType):
        ManifestLoader.load_from_path(file_config)


def test_load_from_remote_pass():
    """Test that ManifestLoader can load a remote JSON file via HTTP(S)."""

    example_content = {"foo": "bar"}
    path = Path("example3.json")
    base_url = "http://127.0.0.1:8000"

    with open(path, "w") as file:
        json.dump(example_content, file)

    file_config = FileReferenceConfig(path=AnyUrl(f"{base_url}/example3.json"))

    # Invoke a server for hosting the test file.
    process = subprocess.Popen(["python3", "-m", "http.server", "8000"])

    output = ManifestLoader.load_from_http(file_config)

    process.terminate()
    path.unlink()

    assert output == example_content
