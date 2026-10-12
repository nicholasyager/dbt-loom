from dataclasses import dataclass, field
import os
import re
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Dict, Optional, Set

try:
    from dbt.artifacts.resources.v1.components import ColumnInfo
except ModuleNotFoundError:
    from dbt.contracts.graph.nodes import ColumnInfo  # type: ignore

from dbt.artifacts.resources.v1.components import ColumnInfo
import yaml
from dbt.contracts.graph.node_args import ModelNodeArgs
from dbt.contracts.graph.nodes import ModelNode

from dbt.plugins.manager import dbt_hook, dbtPlugin
from dbt.plugins.manifest import PluginNodes
from dbt.config.project import VarProvider

from dbt_loom.shims import is_invalid_private_ref, is_invalid_protected_ref


try:
    from dbt.artifacts.resources.types import NodeType
except ModuleNotFoundError:
    from dbt.node_types import NodeType  # type: ignore


from dbt_loom.config import ManifestReference, dbtLoomConfig
from dbt_loom.logging import fire_event
from dbt_loom.manifests import ManifestLoader, ManifestNode

import importlib.metadata


@dataclass
class LoomModelNodeArgs(ModelNodeArgs):
    """A dbt-loom extension of ModelNodeArgs to preserve resource types across lineages."""

    resource_type: NodeType = NodeType.Model
    # This is a required field according to the JSON schema
    # but needs a default value for dataclasse magic-init related reasons
    original_file_path: Optional[str] = None
    group: Optional[str] = None
    event_time: Optional[str] = None
    description: Optional[str] = None
    tags: Optional[list[str]] = field(default_factory=list)
    columns: Optional[dict[str, dict]] = None
    compiled: Optional[bool] = None
    raw_code: Optional[str] = None
    compiled_code: Optional[str] = None

    def __init__(self, **kwargs):
        super().__init__(
            **{
                key: value
                for key, value in kwargs.items()
                if key
                not in (
                    "resource_type",
                    "group",
                    "config",
                    "description",
                    "tags",
                    "columns",
                    "original_file_path",
                    "raw_code",
                    "compiled_code",
                    "compiled",
                )
            }
        )
        self.resource_type = kwargs.get("resource_type", NodeType.Model)
        self.group = kwargs.get("group")
        self.event_time = kwargs.get("config", {}).get("event_time", None)
        self.original_file_path = kwargs.get("original_file_path", None)
        self.description = kwargs.get("description", None)
        self.tags = kwargs.get("tags", [])
        self.columns = kwargs.get("columns", {})
        self.compiled = kwargs.get("compiled", None)
        self.raw_code = kwargs.get("raw_code", None)
        self.compiled_code = kwargs.get("compiled_code", None)

    @property
    def unique_id(self) -> str:
        unique_id = f"{self.resource_type}.{self.package_name}.{self.name}"
        if self.version:
            unique_id = f"{unique_id}.v{self.version}"

        return unique_id


def identify_node_subgraph(manifest) -> Dict[str, ManifestNode]:
    """
    Identify all nodes that should be selected from the manifest, and return ManifestNodes.
    """

    output = {}

    # We're going to temporarily allow all nodes here.
    for unique_id in manifest["nodes"].keys():
        if unique_id.split(".")[0] in (NodeType.Test.value, NodeType.Macro.value):
            continue

        node = manifest.get("nodes", {}).get(unique_id)

        if not node:
            continue

        if node.get("access") is None:
            node["access"] = node.get("config", {}).get("access", "protected")

        # Versions may be floats or strings. Standardize on strings for compatibility.
        for key in ("version", "latest_version"):
            if node.get(key):
                node[key] = str(node[key])

        output[unique_id] = ManifestNode(**(node))

    return output


def convert_model_nodes_to_model_node_args(
    selected_nodes: Dict[str, ManifestNode],
) -> Dict[str, LoomModelNodeArgs]:
    """Generate a dictionary of ModelNodeArgs based on a dictionary of ModelNodes"""
    return {
        unique_id: LoomModelNodeArgs(
            schema=node.schema_name,
            identifier=node.identifier,
            **(node.dump()),
        )
        for unique_id, node in selected_nodes.items()
        if node is not None
    }


def merge_loom_nodes(
    existing_nodes: Dict[str, LoomModelNodeArgs],
    new_nodes: Dict[str, LoomModelNodeArgs],
    manifest_name: str,
) -> None:
    """Merge ``new_nodes`` from a manifest into ``existing_nodes`` in place.

    Nodes owned by this manifest's project are authoritative and should
    always be used. Transitive dependency nodes (from other packages) should
    only be added if not already provided by a previous, more authoritative
    manifest.
    """
    for key, value in new_nodes.items():
        is_authoritative = value.package_name == manifest_name
        if is_authoritative or key not in existing_nodes:
            existing_nodes[key] = value


@dataclass
class LoomRunnableConfig:
    """A shim class to allow is_invalid_*_ref functions to correctly handle access for loom-injected models."""

    restrict_access: bool = True
    vars: VarProvider = VarProvider(vars={})


class dbtLoom(dbtPlugin):
    """
    dbtLoom is a dbt plugin that loads manifest files, parses a DAG from the manifest,
    and injects public nodes from imported manifest.
    """

    def __init__(self, project_name: str):
        # Log the version of dbt-loom being initialized
        fire_event(
            msg=f"Initializing dbt-loom={importlib.metadata.version('dbt-loom')}"
        )

        configuration_path = Path(
            os.environ.get("DBT_LOOM_CONFIG", "dbt_loom.config.yml")
        )

        self._manifest_loader = ManifestLoader()
        self.manifests: Dict[str, Dict] = {}

        self.config: Optional[dbtLoomConfig] = self.read_config(configuration_path)
        self.models: Dict[str, LoomModelNodeArgs] = {}

        if self.config is not None:
            self._patch_ref_protection()
            self._patch_after_run()

        if not self.config or (self.config and not self.config.enable_telemetry):
            self._patch_plugin_telemetry()

        super().__init__(project_name)

    def _patch_ref_protection(self) -> None:
        """Patch out the ref protection functions for proper protections"""
        import dbt.contracts.graph.manifest

        fire_event(
            msg="dbt-loom: Patching ref protection methods to support dbt-loom dependencies."
        )

        dbt.contracts.graph.manifest.Manifest.is_invalid_protected_ref = (  # type: ignore
            self.dependency_wrapper(is_invalid_protected_ref)
        )
        dbt.contracts.graph.manifest.Manifest.is_invalid_private_ref = (  # type: ignore
            self.dependency_wrapper(is_invalid_private_ref)
        )

        dbt.parser.manifest.ManifestLoader.check_valid_group_config_node = (  # type: ignore
            self.group_validation_wrapper(
                dbt.parser.manifest.ManifestLoader.check_valid_group_config_node  # type: ignore
            )
        )

        dbt.contracts.graph.nodes.ModelNode.from_args = (  # type: ignore
            self.model_node_wrapper(dbt.contracts.graph.nodes.ModelNode.from_args)  # type: ignore
        )

    def _patch_plugin_telemetry(self) -> None:
        """Patch the plugin telemetry function to prevent tracking of dbt plugins."""
        import dbt.tracking

        dbt.tracking.track = self.tracking_wrapper(dbt.tracking.track)

    def tracking_wrapper(self, function) -> Callable:
        """Wrap the telemetry `track` function and return early if we're tracking plugin actions."""

        def outer_function(*args, **kwargs):
            """Check the context of the snowplow tracker message for references to loom. Return if present."""

            if any(
                [
                    self.__class__.__name__ in str(context_item.__dict__)
                    or "dbt-loom" in str(context_item.__dict__)
                    or "dbt_loom" in str(context_item.__dict__)
                    for context_item in kwargs.get("context", [])
                ]
            ):
                return

            return function(*args, **kwargs)

        return outer_function

    def model_node_wrapper(self, function) -> Callable:
        """Wrap the ModelNode.from_args function and inject extra properties from the LoomModelNodeArgs."""

        def outer_function(args: LoomModelNodeArgs) -> ModelNode:
            model = function(args)
            model.group = args.group
            model.config.event_time = args.event_time
            model.description = args.description
            model.columns = (
                {k: ColumnInfo.from_dict(v) for k, v in args.columns.items()}
                if args.columns is not None
                else {}
            )
            model.original_file_path = args.original_file_path
            model.tags = args.tags

            return model

        return outer_function

    def group_validation_wrapper(self, function) -> Callable:
        """Wrap the check_valid_group_config_node function to inject upstream group names."""

        def outer_function(
            inner_self, groupable_node, valid_group_names: Set[str]
        ) -> bool:
            new_groups: Set[str] = {
                model.group for model in self.models.values() if model.group is not None
            }

            return function(
                inner_self, groupable_node, valid_group_names.union(new_groups)
            )

        return outer_function

    def dependency_wrapper(self, function) -> Callable:
        def outer_function(inner_self, node, target_model, dependencies) -> bool:
            if self.config is not None:
                for manifest_name in self.manifests.keys():
                    if manifest_name in dependencies:
                        continue

                    dependencies[manifest_name] = LoomRunnableConfig()

            return function(inner_self, node, target_model, dependencies)

        return outer_function

    def _patch_after_run(self) -> None:
        """Patch `after_run` method to add more complete docs for injected nodes."""

        from dbt.task.build import BuildTask
        from dbt.task.compile import CompileTask

        try:
            from dbt.task.docs.generate import GenerateTask
        except ModuleNotFoundError:
            from dbt.task.generate import GenerateTask  # type: ignore
        from dbt.task.run import RunTask

        fire_event(
            msg="dbt-loom: Patching after-run method to improve docs for injected nodes."
        )

        BuildTask.after_run = self.after_run_wrapper(  # type: ignore[method-assign]
            BuildTask.after_run
        )
        CompileTask.after_run = self.after_run_wrapper(  # type: ignore[method-assign]
            CompileTask.after_run
        )
        GenerateTask.after_run = self.after_run_wrapper(  # type: ignore[method-assign]
            GenerateTask.after_run
        )
        RunTask.after_run = self.after_run_wrapper(RunTask.after_run)  # type: ignore[method-assign]

    def after_run_wrapper(self, function) -> Callable:
        """
        Wrap the `after_run` method for GraphRunnableTasks to add extra node attrs for dbt docs.

        These additionals attributes MUST be set after the compile phase is finished. Setting the
        additional attributes prior to compile may result in compilation failures.

        For example, because the plugin interface only injects ModelNodes in PluginNodes, compilation
        will fail if the injected PluginNodes depend on macros defined in another dbt project, including
        their own root project.
        """
        if TYPE_CHECKING:
            from dbt.task.runnable import GraphRunnableTask

        def outer_function(
            task_instance: "GraphRunnableTask", adapter, results
        ) -> None:
            function(task_instance, adapter, results)
            if task_instance.manifest is not None:
                models = {
                    node_id: node
                    for node_id, node in task_instance.manifest.nodes.items()
                    if node.resource_type == NodeType.Model
                }
                for node_id, node in models.items():
                    if args := self.models.get(node_id, None):
                        node.compiled = args.compiled or False
                        node.raw_code = args.raw_code or ""
                        node.compiled_code = args.compiled_code

        return outer_function

    def get_groups(self) -> Set[str]:
        """Get all groups defined in injected models."""

        return {
            model.group for model in self.models.values() if model.group is not None
        }

    def read_config(self, path: Path) -> Optional[dbtLoomConfig]:
        """Read the dbt-loom configuration file."""
        if not path.exists():
            fire_event(msg=f"dbt-loom: Config file `{path}` does not exist")
            return None

        with open(path) as file:
            config_content = file.read()

        config_content = self.replace_env_variables(config_content)

        return dbtLoomConfig(**yaml.load(config_content, yaml.SafeLoader))

    @staticmethod
    def replace_env_variables(config_str: str) -> str:
        """Replace environment variable placeholders in the configuration string."""
        pattern = r"\$(\w+)|\$\{([^}]+)\}"
        return re.sub(
            pattern,
            lambda match: os.environ.get(
                match.group(1) if match.group(1) is not None else match.group(2), ""
            ),
            config_str,
        )

    @staticmethod
    def filter_models(reference: ManifestReference, node: ManifestNode) -> bool:
        """Evaluate if a node should be included based on the node's package and the manifest reference."""
        if len(reference.included_packages) > 0:
            if node.package_name in reference.included_packages:
                return True
            return False

        if node.package_name not in reference.excluded_packages:
            return True

        return False

    def initialize(self) -> None:
        """Initialize the plugin"""

        if self.models != {} or not self.config:
            return

        for manifest_reference in self.config.manifests:
            fire_event(
                msg=f"dbt-loom: Loading manifest for `{manifest_reference.name}`"
                f" from `{manifest_reference.type.value}`"
            )

            manifest = self._manifest_loader.load(manifest_reference)
            if manifest is None:
                continue

            # Find the official project name from the manifest metadata and use that as the manifests key.
            manifest_name = manifest.get("metadata", {}).get(
                "project_name", manifest_reference.name
            )
            self.manifests[manifest_name] = manifest

            selected_nodes = identify_node_subgraph(manifest)

            # Remove nodes from excluded packages.
            filtered_nodes = {
                key: value
                for key, value in selected_nodes.items()
                if self.filter_models(manifest_reference, value)
            }

            loom_nodes = convert_model_nodes_to_model_node_args(filtered_nodes)

            merge_loom_nodes(self.models, loom_nodes, manifest_name)

    @dbt_hook
    def get_nodes(self) -> PluginNodes:
        """
        Inject PluginNodes to dbt for injection into dbt's DAG.
        """
        fire_event(msg="dbt-loom: Injecting nodes")
        return PluginNodes(models=self.models)  # type: ignore


plugins = [dbtLoom]
