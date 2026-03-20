from dbt_loom import apply_database_alias
from dbt_loom.manifests import ManifestNode
from dbt_loom.config import ManifestReference, ManifestReferenceType, FileReferenceConfig


try:
    from dbt.artifacts.resources.types import NodeType
except ModuleNotFoundError:
    from dbt.node_types import NodeType  # type: ignore


def _make_node(name, database=None, relation_name=None):
    return ManifestNode(
        unique_id=f"model.pkg.{name}",
        name=name,
        package_name="pkg",
        schema="main",
        resource_type="model",
        database=database,
        relation_name=relation_name,
    )


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


def test_apply_database_alias_empty():
    """Confirm empty override dict is a no-op for backwards compatibility."""
    node = _make_node("my_model", "db1", '"db1"."schema"."table"')
    nodes = {"model.pkg.my_model": node}

    result = apply_database_alias(nodes, {})

    assert result["model.pkg.my_model"].database == "db1"


def test_apply_database_alias_basic():
    """Confirm that database and relation_name are both overridden."""
    node1 = _make_node("my_model1", "db1", '"db1"."schema"."table"')
    node2 = _make_node("my_model2", "db1", "`db1`.`schema`.`table`")
    node3 = _make_node("my_model3", "db2", '"db2"."schema"."table"')
    node4 = _make_node("my_model4", None, None)
    nodes = {
        "model.pkg.my_model1": node1,
        "model.pkg.my_model2": node2,
        "model.pkg.my_model3": node3,
        "model.pkg.my_model4": node4
    }

    result = apply_database_alias(nodes, {"db1": "db1_alias"})

    # Alias provided
    assert result["model.pkg.my_model1"].database == "db1_alias"
    assert result["model.pkg.my_model1"].relation_name == '"db1_alias"."schema"."table"'

    # Alias provided with backticks (BigQuery style)
    assert result["model.pkg.my_model2"].database == "db1_alias"
    assert result["model.pkg.my_model2"].relation_name == '`db1_alias`.`schema`.`table`'

    # No alias provided
    assert result["model.pkg.my_model3"].database == "db2"
    assert result["model.pkg.my_model3"].relation_name == '"db2"."schema"."table"'

    # No node
    assert result["model.pkg.my_model4"].database is None
    assert result["model.pkg.my_model4"].relation_name is None
