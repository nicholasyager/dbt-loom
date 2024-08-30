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
    }

    manifest_node = ManifestNode(**(node))  # type: ignore

    assert manifest_node.resource_type == NodeType.Seed
