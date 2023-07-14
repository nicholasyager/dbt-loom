import os
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Set

import networkx
import yaml
from dbt.contracts.graph.node_args import ModelNodeArgs
from dbt.plugins.manager import dbtPlugin, dbt_hook
from dbt.plugins.manifest import PluginNodes
from networkx import DiGraph
from pydantic import BaseModel
from dbt.logger import GLOBAL_LOGGER as logger


class ManifestConfigType(str, Enum):
    """Type of ManifestConfig"""
    file = 'file'


class ManifestConfig(BaseModel):
    """Configuration for a manifest to be loaded into dbt-loom."""

    path: Optional[str]
    type: ManifestConfigType


class dbtLoomConfig(BaseModel):
    """Configuration for dbt Loom"""
    manifests: List[ManifestConfig]


class dbtLoom(dbtPlugin):
    """
    dbtLoom is a dbt plugin that loads manifest files, parses a DAG from the manifest,
    and injects public nodes from imported manifest.
    """

    def __init__(self, project_name: str):
        super().__init__(project_name)

        self.plugins = [self.get_nodes]

        configuration_path = Path(os.environ.get('DBT_LOOM_CONFIG', 'dbt_loom.config.yml'))
        self.config: Optional[dbtLoomConfig] = self.read_config(configuration_path)
        self.models: Dict[str, ModelNodeArgs] = {}

    def initialize(self) -> None:
        """Initialize the plugin"""
        self.models = self.load_models(self.config) if self.config else {}

    def _load_manifest(self, manifest_configuration: ManifestConfig) -> Optional[Dict]:
        """Load a manifest based on the manifest configuration."""

        if manifest_configuration.type == ManifestConfigType.file:
           return yaml.load(open(manifest_configuration.path), yaml.SafeLoader)

        return None

    @staticmethod
    def identify_public_node_subgraph(manifest) -> Set[str]:
        """Identify all nodes that are ancestors of public nodes, and the public nodes themselves."""
        graph: DiGraph = networkx.DiGraph()
        public_nodes = set()
        selected_nodes = set()

        for unique_id, node in manifest['nodes'].items():
            graph.add_edges_from([
                (parent, unique_id)
                for parent in node.get('depends_on', {'nodes': []}).get('nodes')
                if not parent.startswith('source')
            ])

            if node.get('access', 'protected') == 'public':
                public_nodes.add(unique_id)
        selected_nodes.update(public_nodes)
        for public_node in public_nodes:
            selected_nodes.update(networkx.ancestors(graph, public_node))

        return selected_nodes

    def load_models(self, configuration: dbtLoomConfig) -> Dict[str, ModelNodeArgs]:
        output = {}

        for manifest_configuration in configuration.manifests:

            manifest = self._load_manifest(manifest_configuration)
            if manifest is None:
                continue

            selected_nodes = self.identify_public_node_subgraph(manifest)

            for unique_id in selected_nodes:
                node = manifest.get('nodes', {}).get(unique_id)

                if node is None:
                    logger.warning(f'Unable to find node {unique_id} in the manifest {manifest_configuration.path}')
                    continue

                logger.warning(f'Injecting node `{unique_id}`')
                output[unique_id] = ModelNodeArgs(
                    name=node.get('name'),
                    package_name=node.get('package_name'),
                    identifier=node.get('relation_name').split('.')[-1].replace('"', ''),
                    schema=node.get('schema'),
                    database=node.get('database'),
                    relation_name=node.get('relation_name'),
                    version=node.get('version'),
                    latest_version=node.get('latest_version'),
                    deprecation_date=node.get('deprecation_date'),
                    access=node.get('access', 'public'),
                    generated_at=node.get('created_at'),
                    depends_on_nodes=list(filter(
                        lambda x: not x.startswith('source'),
                        node.get('depends_on', {'nodes': []}).get('nodes')
                    )),
                    enabled=node['config'].get('enabled')
                )

        return output

    def read_config(self, path: Path) -> Optional[dbtLoomConfig]:
        """Read the dbt-loom configuration file."""
        if not path.exists():
            return None

        return dbtLoomConfig(**yaml.load(open(path), yaml.SafeLoader))

    @dbt_hook
    def get_nodes(self) -> PluginNodes:
        """
        Inject PluginNodes to dbt for injection into dbt's DAG.
        """
        return PluginNodes(models=self.models)


plugins = [dbtLoom]
