import datetime
import json
from typing import Dict, List, Optional

from pydantic import BaseModel, Field, validator

try:
    from dbt.artifacts.resources.types import NodeType
except ModuleNotFoundError:
    from dbt.node_types import NodeType  # type: ignore

from dbt_loom.clients.az_blob import AzureClient, AzureReferenceConfig
from dbt_loom.clients.dbt_cloud import DbtCloud, DbtCloudReferenceConfig
from dbt_loom.clients.gcs import GCSClient, GCSReferenceConfig
from dbt_loom.clients.s3 import S3Client, S3ReferenceConfig
from dbt_loom.config import (
    FileReferenceConfig,
    LoomConfigurationError,
    ManifestReference,
    ManifestReferenceType,
)


class DependsOn(BaseModel):
    """Wrapper for storing dependencies"""

    nodes: List[str] = Field(default_factory=list)
    macros: List[str] = Field(default_factory=list)


class ManifestNode(BaseModel):
    """A basic ManifestNode that can be referenced across projects."""

    name: str
    resource_type: NodeType
    package_name: str
    schema_name: str = Field(alias="schema")
    database: Optional[str] = None
    relation_name: Optional[str] = None
    version: Optional[str] = None
    latest_version: Optional[str] = None
    deprecation_date: Optional[datetime.datetime] = None
    access: Optional[str] = "protected"
    group: Optional[str] = None
    generated_at: datetime.datetime = Field(default_factory=datetime.datetime.utcnow)
    depends_on: Optional[DependsOn] = None
    depends_on_nodes: List[str] = Field(default_factory=list)
    enabled: bool = True

    @validator("depends_on_nodes", always=True)
    def default_depends_on_nodes(cls, v, values):
        depends_on = values.get("depends_on")
        if depends_on is None:
            return []

        return [
            node for node in depends_on.nodes if node.split(".")[0] not in ("source")
        ]

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
