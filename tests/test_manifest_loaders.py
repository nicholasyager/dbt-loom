import json
from pathlib import Path
from pydantic import AnyUrl
from dbt_loom.config import FileReferenceConfig
from dbt_loom.manifests import ManifestLoader


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
