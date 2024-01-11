import datetime
import json
import os
import re
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Union

import yaml
from dbt.contracts.graph.node_args import ModelNodeArgs
from dbt.plugins.manager import dbt_hook, dbtPlugin
from dbt.plugins.manifest import PluginNodes
from networkx import DiGraph
from pydantic import BaseModel, Field

from dbt_loom.clients.az_blob import AzureClient
from dbt_loom.clients.s3 import S3Client

from .clients.dbt_cloud import DbtCloud
from .clients.gcs import GCSClient


class ManifestReferenceType(str, Enum):
    """Type of ManifestReference"""

    file = "file"
    dbt_cloud = "dbt_cloud"
    gcs = "gcs"
    s3 = "s3"
    azure = "azure"


class FileReferenceConfig(BaseModel):
    """Configuration for a file reference"""

    path: Path


class DbtCloudReferenceConfig(BaseModel):
    """Configuration for a dbt Cloud reference."""

    account_id: int
    job_id: int
    api_endpoint: Optional[str] = None
    step: Optional[int] = None


class GCSReferenceConfig(BaseModel):
    """Configuration for a GCS reference"""

    project_id: str
    bucket_name: str
    object_name: str
    credentials: Optional[Path] = None


class S3ReferenceConfig(BaseModel):
    """Configuration for an reference stored in S3"""

    bucket_name: str
    object_name: str
    credentials: Optional[Path] = None


class AzureReferenceConfig(BaseModel):
    """Configuration for an reference stored in Azure Storage"""

    container_name: str
    object_name: str
    account_name: str


class ManifestReference(BaseModel):
    """Reference information for a manifest to be loaded into dbt-loom."""

    name: str
    type: ManifestReferenceType
    config: Union[
        FileReferenceConfig,
        DbtCloudReferenceConfig,
        GCSReferenceConfig,
        S3ReferenceConfig,
        AzureReferenceConfig,
    ]


class dbtLoomConfig(BaseModel):
    """Configuration for dbt Loom"""

    manifests: List[ManifestReference]


class LoomConfigurationError(BaseException):
    """Error raised when dbt-loom has been misconfigured."""


class ManifestNode(BaseModel):
    """A basic ManifestNode that can be referenced across projects."""

    name: str
    package_name: str
    schema_name: str = Field(alias="schema")
    database: Optional[str] = None
    relation_name: Optional[str] = None
    version: Optional[Union[str, float]] = None
    latest_version: Optional[Union[str, float]] = None
    deprecation_date: Optional[datetime.datetime] = None
    access: Optional[str] = "protected"
    generated_at: datetime.datetime = Field(default_factory=datetime.datetime.utcnow)
    depends_on_nodes: List[str] = Field(default_factory=list)
    enabled: bool = True

    @property
    def identifier(self) -> str:
        if not self.relation_name:
            return self.name

        return self.relation_name.split(".")[-1].replace('"', "").replace("`", "")


class ManifestLoader:
    def __init__(self):
        self.loading_functions = {
            ManifestReferenceType.file: self.load_from_local_filesystem,
            ManifestReferenceType.dbt_cloud: self.load_from_dbt_cloud,
            ManifestReferenceType.gcs: self.load_from_gcs,
            ManifestReferenceType.s3: self.load_from_s3,
            ManifestReferenceType.azure: self.load_from_azure,
        }

    @staticmethod
    def load_from_local_filesystem(config: FileReferenceConfig) -> Dict:
        """Load a manifest dictionary from a local file"""
        if not config.path.exists():
            raise LoomConfigurationError(f"The path `{config.path}` does not exist.")

        return json.load(open(config.path))

    @staticmethod
    def load_from_dbt_cloud(config: DbtCloudReferenceConfig) -> Dict:
        """Load a manifest dictionary from dbt Cloud."""
        client = DbtCloud(
            account_id=config.account_id, api_endpoint=config.api_endpoint
        )

        return client.get_models(config.job_id, step=config.step)

    @staticmethod
    def load_from_gcs(config: GCSReferenceConfig) -> Dict:
        """Load a manifest dictionary from a GCS bucket."""
        gcs_client = GCSClient(
            project_id=config.project_id,
            bucket_name=config.bucket_name,
            object_name=config.object_name,
            credentials=config.credentials,
        )

        return gcs_client.load_manifest()

    @staticmethod
    def load_from_s3(config: S3ReferenceConfig) -> Dict:
        """Load a manifest dictionary from an S3-compatible bucket."""
        gcs_client = S3Client(
            bucket_name=config.bucket_name,
            object_name=config.object_name,
        )

        return gcs_client.load_manifest()

    @staticmethod
    def load_from_azure(config: AzureReferenceConfig) -> Dict:
        """Load a manifest dictionary from Azure storage."""
        azure_client = AzureClient(
            container_name=config.container_name,
            object_name=config.object_name,
            account_name=config.account_name,
        )

        return azure_client.load_manifest()

    def load(self, manifest_reference: ManifestReference) -> Dict:
        """Load a manifest dictionary based on a ManifestReference input."""

        if manifest_reference.type not in self.loading_functions:
            raise LoomConfigurationError(
                f"The manifest reference provided for {manifest_reference.name} does "
                "not have a valid type."
            )

        manifest = self.loading_functions[manifest_reference.type](
            manifest_reference.config
        )

        return manifest


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
    selected_nodes: Dict[str, ManifestNode]
) -> Dict[str, ModelNodeArgs]:
    """Generate a dictionary of ModelNodeArgs based on a dictionary of ModelNodes"""
    return {
        unique_id: ModelNodeArgs(
            schema=node.schema_name,
            identifier=node.identifier,
            **(node.dict(exclude={"schema_name"})),
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
            print(
                f"dbt-loom: Loading manifest for `{manifest_reference.name}` from "
                f"`{manifest_reference.type.value}`"
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
        print("dbt-loom: Injecting nodes")
        return PluginNodes(models=self.models)


plugins = [dbtLoom]
