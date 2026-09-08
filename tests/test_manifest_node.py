import pytest

from dbt_loom.manifests import ManifestNode


try:
    from dbt.artifacts.resources.types import NodeType
except ModuleNotFoundError:
    from dbt.node_types import NodeType  # type: ignore


def test_rewrite_resource_types():
    """Confirm that resource types are rewritten if they are incorrect due to previous injections."""

    node = {
        "unique_id": "seed.example.foo",
        "name": "foo",
        "package_name": "example",
        "schema": "bar",
        "resource_type": "model",
        "original_file_path": "seeds/example/foo",
    }

    manifest_node = ManifestNode(**(node))  # type: ignore

    assert manifest_node.resource_type == NodeType.Seed


def test_rewrite_identifiers_true_negative():
    """Confirm that resource identifier rewriting works in the simplest case"""

    node = {
        "unique_id": "seed.example.foo",
        "name": "foo",
        "package_name": "example",
        "schema": "bar",
        "resource_type": "model",
        "resource_name": "prod.bar.foo",
        "original_file_path": "models/bar/foo",
    }

    manifest_node = ManifestNode(**(node))  # type: ignore

    assert manifest_node.identifier == "foo"


@pytest.mark.parametrize("quote_char", ['"', "`", "[]"])
def test_rewrite_identifiers_true_positives(quote_char):
    """Confirm that resource identifier rewriting works for different quote types."""

    if len(quote_char) == 1:
        relation_name = f"prod.bar.{quote_char}foo{quote_char}"
    else:
        relation_name = f"prod.bar.{quote_char[0]}foo{quote_char[-1]}"

    node = {
        "unique_id": "seed.example.foo",
        "name": "foo",
        "package_name": "example",
        "schema": "bar",
        "resource_type": "model",
        "relation_name": relation_name,
        "original_file_path": "seed/example/foo",
    }

    manifest_node = ManifestNode(**(node))  # type: ignore

    assert manifest_node.identifier == "foo"
