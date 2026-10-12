from dbt_loom import dbtLoom
from dbt_loom.config import (
    FileReferenceConfig,
    ManifestReference,
    ManifestReferenceType,
)
from dbt_loom.manifests import ManifestNode

try:
    from dbt.artifacts.resources.types import NodeType
except ModuleNotFoundError:
    from dbt.node_types import NodeType  # type: ignore


def test_filter_nodes_from_excluded_packages_list():
    """Confirm that nodes from packages in the excluded packages list are removed."""

    manifest_reference = ManifestReference(
        name="foobar",
        type=ManifestReferenceType.file,
        config=FileReferenceConfig(path="/tmp/foo/manifest.json"),  # type: ignore
        excluded_packages=["bar"],
    )

    node = ManifestNode(
        name="example",
        package_name="bar",
        unique_id="model.bar.example",
        resource_type=NodeType.Model,
        schema="bar",
        original_file_path="models/bar/foo",
    )

    assert not dbtLoom.filter_models(manifest_reference, node)


def test_filter_nodes_not_in_excluded_packages_list():
    """Confirm that nodes from packages not in the excluded packages list are preserved."""

    manifest_reference = ManifestReference(
        name="foobar",
        type=ManifestReferenceType.file,
        config=FileReferenceConfig(path="/tmp/foo/manifest.json"),  # type: ignore
        excluded_packages=["bar"],
    )

    node = ManifestNode(
        name="example",
        package_name="baz",
        unique_id="model.baz.example",
        resource_type=NodeType.Model,
        schema="baz",
        original_file_path="models/baz/example",
    )

    assert dbtLoom.filter_models(manifest_reference, node)


def test_filter_nodes_not_in_included_packages_list():
    """Confirm that nodes from packages not in the included packages list are removed."""
    manifest_reference = ManifestReference(
        name="foobar",
        type=ManifestReferenceType.file,
        config=FileReferenceConfig(path="/tmp/foo/manifest.json"),  # type: ignore
        included_packages=["bar"],
    )

    node = ManifestNode(
        name="example",
        package_name="baz",
        unique_id="model.baz.example",
        resource_type=NodeType.Model,
        schema="baz",
        original_file_path="models/baz/example",
    )

    assert not dbtLoom.filter_models(manifest_reference, node)


def test_filter_nodes_in_included_packages_list():
    """Confirm that nodes from packages in the included packages list are preserved."""
    manifest_reference = ManifestReference(
        name="foobar",
        type=ManifestReferenceType.file,
        config=FileReferenceConfig(path="/tmp/foo/manifest.json"),  # type: ignore
        included_packages=["bar"],
    )

    node = ManifestNode(
        name="example",
        package_name="bar",
        unique_id="model.bar.example",
        resource_type=NodeType.Model,
        schema="bar",
        original_file_path="models/bar/example",
    )

    assert dbtLoom.filter_models(manifest_reference, node)
