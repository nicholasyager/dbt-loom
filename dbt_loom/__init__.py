from dataclasses import dataclass
import os
import re
from pathlib import Path
from typing import Callable, Dict, Optional

import yaml
from dbt.contracts.graph.node_args import ModelNodeArgs


from dbt.plugins.manager import dbt_hook, dbtPlugin
from dbt.plugins.manifest import PluginNodes
from dbt.config.project import VarProvider

try:
    from dbt.artifacts.resources.types import NodeType
except ModuleNotFoundError:
    from dbt.node_types import NodeType  # type: ignore


from dbt_loom.config import dbtLoomConfig
from dbt_loom.logging import fire_event
from dbt_loom.manifests import ManifestLoader, ManifestNode

import importlib.metadata


@dataclass
class LoomModelNodeArgs(ModelNodeArgs):
    """A dbt-loom extension of ModelNodeArgs to preserve resource types across lineages."""

    resource_type: NodeType = NodeType.Model

    def __init__(self, **kwargs):
        super().__init__(
            **{
                key: value
                for key, value in kwargs.items()
                if key not in ("resource_type")
            }
        )
        self.resource_type = kwargs["resource_type"]

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
            **(
                # Small bit of logic to support both pydantic 2 and pydantic 1
                node.model_dump(exclude={"schema_name", "depends_on", "node_config"})
                if hasattr(node, "model_dump")
                else node.dict(exclude={"schema_name", "depends_on", "node_config"})
            ),
        )
        for unique_id, node in selected_nodes.items()
        if node is not None
    }


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
        # Log the version of dbt-loom being intialized
        fire_event(
            msg=f'Initializing dbt-loom={importlib.metadata.version("dbt-loom")}'
        )

        configuration_path = Path(
            os.environ.get("DBT_LOOM_CONFIG", "dbt_loom.config.yml")
        )

        self._manifest_loader = ManifestLoader()

        self.config: Optional[dbtLoomConfig] = self.read_config(configuration_path)
        self.models: Dict[str, LoomModelNodeArgs] = {}

        import dbt.contracts.graph.manifest

        fire_event(
            msg="dbt-loom: Patching ref protection methods to support dbt-loom dependencies."
        )
        dbt.contracts.graph.manifest.Manifest.is_invalid_protected_ref = (  # type: ignore
            self.dependency_wrapper(
                dbt.contracts.graph.manifest.Manifest.is_invalid_protected_ref
            )
        )
        dbt.contracts.graph.manifest.Manifest.is_invalid_private_ref = (  # type: ignore
            self.dependency_wrapper(
                dbt.contracts.graph.manifest.Manifest.is_invalid_private_ref
            )
        )

        super().__init__(project_name)

    def dependency_wrapper(self, function) -> Callable:
        def outer_function(inner_self, node, target_model, dependencies) -> bool:
            if self.config is not None:
                for manifest in self.config.manifests:
                    dependencies[manifest.name] = LoomRunnableConfig()

            return function(inner_self, node, target_model, dependencies)

        return outer_function

    def read_config(self, path: Path) -> Optional[dbtLoomConfig]:
        """Read the dbt-loom configuration file."""
        if not path.exists():
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

            selected_nodes = identify_node_subgraph(manifest)
            self.models.update(convert_model_nodes_to_model_node_args(selected_nodes))

    @dbt_hook
    def get_nodes(self) -> PluginNodes:
        """
        Inject PluginNodes to dbt for injection into dbt's DAG.
        """
        fire_event(msg="dbt-loom: Injecting nodes")
        return PluginNodes(models=self.models)  # type: ignore


plugins = [dbtLoom]
