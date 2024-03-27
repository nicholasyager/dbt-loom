import os
import re
from pathlib import Path
from typing import Dict, Optional

import yaml
from dbt.contracts.graph.node_args import ModelNodeArgs
from dbt.events.functions import fire_event
from dbt.events.types import Note
from dbt.plugins.manager import dbt_hook, dbtPlugin
from dbt.plugins.manifest import PluginNodes
from networkx import DiGraph

from dbt_loom.config import dbtLoomConfig
from dbt_loom.manifests import ManifestLoader, ManifestNode


def identify_public_node_subgraph(manifest) -> Dict[str, ManifestNode]:
    """
    Identify all nodes that are ancestors of public nodes, and the public nodes
    themselves.
    """
    graph: DiGraph = DiGraph()
    public_nodes = set()
    selected_node_ids = set()

    for unique_id, node in manifest["nodes"].items():
        graph.add_edges_from(
            [
                (parent, unique_id)
                for parent in node.get("depends_on", {"nodes": []}).get("nodes", [])
                if parent.startswith("model")
            ]
        )

        if node.get("access", "protected") == "public":
            public_nodes.add(unique_id)
    selected_node_ids.update(public_nodes)

    return {
        unique_id: ManifestNode(**(manifest.get("nodes", {}).get(unique_id)))
        for unique_id in selected_node_ids
    }


def convert_model_nodes_to_model_node_args(
    selected_nodes: Dict[str, ManifestNode],
) -> Dict[str, ModelNodeArgs]:
    """Generate a dictionary of ModelNodeArgs based on a dictionary of ModelNodes"""
    return {
        unique_id: ModelNodeArgs(
            schema=node.schema_name,
            identifier=node.identifier,
            **(node.model_dump(exclude={"schema_name"})),
        )
        for unique_id, node in selected_nodes.items()
        if node is not None
    }


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

        super().__init__(project_name)

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

            selected_nodes = identify_public_node_subgraph(manifest)
            self.models.update(convert_model_nodes_to_model_node_args(selected_nodes))

    @dbt_hook
    def get_nodes(self) -> PluginNodes:
        """
        Inject PluginNodes to dbt for injection into dbt's DAG.
        """
        fire_event(Note(msg="dbt-loom: Injecting nodes"))
        return PluginNodes(models=self.models)


plugins = [dbtLoom]
