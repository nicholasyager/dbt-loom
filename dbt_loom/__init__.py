import os
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional

import yaml
from dbt.contracts.graph.node_args import ModelNodeArgs
from dbt.plugins.manager import dbtPlugin, dbt_hook
from dbt.plugins.manifest import PluginNodes
from pydantic import BaseModel


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

    def __init__(self, project_name: str):
        super().__init__(project_name)

        self.plugins = [self.get_nodes]

        configuration_path = Path(os.environ.get('DBT_LOOM_CONFIG', 'dbt_loom.config.yml'))
        config: Optional[dbtLoomConfig] = self.read_config(configuration_path)

        self.models: Dict[str, ModelNodeArgs] = self.load_models(config) if config else {}


    def load_models(self, configuration: dbtLoomConfig) -> Dict[str, ModelNodeArgs]:
        output = {}

        for manifest_configuration in configuration.manifests:

            manifest = None
            if manifest_configuration.type == ManifestConfigType.file:

                manifest = yaml.load(open(manifest_configuration.path), yaml.SafeLoader)

            if manifest is None:
                continue

            for unique_id, node in manifest['nodes'].items():

                if node.get('access', 'protected') != 'public':
                    continue

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
                    access=node.get('access'),
                    generated_at=node.get('created_at'),
                    # TODO: Add actual dependencies. Might need to construct an internal graph for selecting models to
                    #  inject.
                    depends_on_nodes=[], # node['depends_on'].get('nodes'),
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
