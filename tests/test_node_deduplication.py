"""Tests for node deduplication when the same node appears in multiple upstream manifests."""

from dbt_loom import (
    convert_model_nodes_to_model_node_args,
    identify_node_subgraph,
    merge_loom_nodes,
)


def _make_manifest(project_name, nodes):
    """Helper to create a minimal manifest dict."""
    return {
        "metadata": {"project_name": project_name},
        "nodes": nodes,
    }


def _make_node(unique_id, package_name, access="protected"):
    """Helper to create a minimal manifest node dict."""
    return {
        "unique_id": unique_id,
        "name": unique_id.split(".")[-1],
        "package_name": package_name,
        "schema": "core",
        "resource_type": "model",
        "access": access,
    }


def _merge_manifests(manifest_pairs):
    """Simulate the initialize() merging logic from dbtLoom.

    Args:
        manifest_pairs: list of (manifest_dict, project_name) tuples in load order.
    """
    models = {}
    for manifest, manifest_name in manifest_pairs:
        selected_nodes = identify_node_subgraph(manifest)
        loom_nodes = convert_model_nodes_to_model_node_args(selected_nodes)
        merge_loom_nodes(models, loom_nodes, manifest_name)
    return models


def test_authoritative_node_wins_over_transitive_copy():
    """The owning manifest's public node should not be overwritten by a
    protected transitive copy from a later manifest."""

    uid = "model.project_a.shared_model"

    models = _merge_manifests(
        [
            (
                _make_manifest(
                    "project_a", {uid: _make_node(uid, "project_a", access="public")}
                ),
                "project_a",
            ),
            (
                _make_manifest(
                    "project_b", {uid: _make_node(uid, "project_a", access="protected")}
                ),
                "project_b",
            ),
        ]
    )

    assert models[uid].access == "public"


def test_authoritative_node_wins_regardless_of_load_order():
    """Even if the transitive copy is loaded first, the authoritative manifest
    should overwrite it."""

    uid = "model.project_a.shared_model"

    models = _merge_manifests(
        [
            (
                _make_manifest(
                    "project_b", {uid: _make_node(uid, "project_a", access="protected")}
                ),
                "project_b",
            ),
            (
                _make_manifest(
                    "project_a", {uid: _make_node(uid, "project_a", access="public")}
                ),
                "project_a",
            ),
        ]
    )

    assert models[uid].access == "public"


def test_transitive_node_added_when_no_authoritative_source():
    """Nodes from packages without their own manifest entry should still be
    injected via the first manifest that provides them."""

    uid = "model.project_c.some_model"

    models = _merge_manifests(
        [
            (
                _make_manifest(
                    "project_a", {uid: _make_node(uid, "project_c", access="protected")}
                ),
                "project_a",
            ),
        ]
    )

    assert uid in models


def test_first_transitive_copy_preserved_over_later_copies():
    """When a non-authoritative node appears in multiple manifests, the first
    loaded version should be kept."""

    uid = "model.project_c.some_model"

    models = _merge_manifests(
        [
            (
                _make_manifest(
                    "project_a", {uid: _make_node(uid, "project_c", access="public")}
                ),
                "project_a",
            ),
            (
                _make_manifest(
                    "project_b", {uid: _make_node(uid, "project_c", access="protected")}
                ),
                "project_b",
            ),
        ]
    )

    assert models[uid].access == "public"
