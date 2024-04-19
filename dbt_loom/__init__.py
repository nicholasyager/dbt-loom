import os
import re
from pathlib import Path
from typing import Any, Callable, Dict, Mapping, Optional

import yaml
from dbt.contracts.graph.node_args import ModelNodeArgs
from dbt.events.functions import fire_event
from dbt.events.types import Note
from dbt.plugins.manager import dbt_hook, dbtPlugin
from dbt.plugins.manifest import PluginNodes
from dbt.config.project import VarProvider

from networkx import DiGraph

from dbt_loom.config import dbtLoomConfig
from dbt_loom.manifests import ManifestLoader, ManifestNode


def identify_node_subgraph(manifest) -> Dict[str, ManifestNode]:
    """
    Identify all nodes that should be selected from the manifest, and return ManifestNodes.
    """

    # We're going to temporarily allow all nodes here.

    return {
        unique_id: ManifestNode(**(manifest.get("nodes", {}).get(unique_id)))
        for unique_id in manifest["nodes"].keys()
        if unique_id.split(".")[0] in ("model")
    }


def convert_model_nodes_to_model_node_args(
    selected_nodes: Dict[str, ManifestNode],
) -> Dict[str, ModelNodeArgs]:
    """Generate a dictionary of ModelNodeArgs based on a dictionary of ModelNodes"""
    return {
        unique_id: ModelNodeArgs(
            schema=node.schema_name,
            identifier=node.identifier,
            **(
                # Small bit of logic to support both pydantic 2 and pydantic 1
                node.model_dump(exclude={"schema_name", "depends_on"})
                if hasattr(node, "model_dump")
                else node.dict(exclude={"schema_name", "depends_on"})
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
        configuration_path = Path(
            os.environ.get("DBT_LOOM_CONFIG", "dbt_loom.config.yml")
        )

        self._manifest_loader = ManifestLoader()

        self.config: Optional[dbtLoomConfig] = self.read_config(configuration_path)
        self.models: Dict[str, ModelNodeArgs] = {}

        import dbt.contracts.graph.manifest

        fire_event(
            Note(
                msg="dbt-loom: Patching ref protection methods to support dbt-loom dependencies."
            )
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
                Note(
                    msg=f"dbt-loom: Loading manifest for `{manifest_reference.name}`"
                    f" from `{manifest_reference.type.value}`"
                )
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
        fire_event(Note(msg="dbt-loom: Injecting nodes"))
        return PluginNodes(models=self.models)


plugins = [dbtLoom]
