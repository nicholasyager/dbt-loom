import datetime
from io import BytesIO
import json
import gzip
import os
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import unquote, urlunparse

from pydantic import BaseModel, Field, validator
import requests

from dbt_loom.clients.snowflake_stage import SnowflakeReferenceConfig, SnowflakeClient

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
    package_name: str
    unique_id: str
    resource_type: NodeType
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

    @validator("resource_type", always=True)
    def fix_resource_types(cls, v, values):
        """If the resource type does not match the unique_id prefix, then rewrite the resource type."""

        node_type = values.get("unique_id").split(".")[0]
        if v != node_type:
            return node_type
        return v

    @property
    def identifier(self) -> str:
        if not self.relation_name:
            return self.name

        return self.relation_name.split(".")[-1].replace('"', "").replace("`", "")

    def dump(self) -> Dict:
        """Dump the ManifestNode to a Dict, with support for pydantic 1 and 2"""
        exclude_set = {"schema_name", "depends_on", "node_config", "unique_id"}
        if hasattr(self, "model_dump"):
            return self.model_dump(exclude=exclude_set)  # type: ignore

        return self.dict(exclude=exclude_set)


class UnknownManifestPathType(Exception):
    """Raised when the ManifestLoader receives a FileReferenceConfig with a path that does not have a known URL scheme."""


class InvalidManifestPath(Exception):
    """Raised when the ManifestLoader receives a FileReferenceConfig with an invalid path."""


class ManifestLoader:
    def __init__(self):
        self.loading_functions = {
            ManifestReferenceType.file: self.load_from_path,
            ManifestReferenceType.dbt_cloud: self.load_from_dbt_cloud,
            ManifestReferenceType.gcs: self.load_from_gcs,
            ManifestReferenceType.s3: self.load_from_s3,
            ManifestReferenceType.azure: self.load_from_azure,
            ManifestReferenceType.snowflake: self.load_from_snowflake,
        }

    @staticmethod
    def load_from_path(config: FileReferenceConfig) -> Dict:
        """
        Load a manifest dictionary based on a FileReferenceConfig. This config's
        path can point to either a local file or a URL to a remote location.
        """

        if config.path.scheme in ("http", "https"):
            return ManifestLoader.load_from_http(config)

        if config.path.scheme in ("file"):
            return ManifestLoader.load_from_local_filesystem(config)

        raise UnknownManifestPathType()

    @staticmethod
    def load_from_local_filesystem(config: FileReferenceConfig) -> Dict:
        """Load a manifest dictionary from a local file"""

        if not config.path.path:
            raise InvalidManifestPath()

        if config.path.netloc:
            file_path = Path(f"//{config.path.netloc}{config.path.path}")
        else:
            file_path = Path(
                unquote(
                    config.path.path.lstrip("/")
                    if os.name == "nt"
                    else config.path.path
                )
            )

        if not file_path.exists():
            raise LoomConfigurationError(f"The path `{file_path}` does not exist.")

        if file_path.suffix == ".gz":
            with gzip.open(file_path, "rt") as file:
                return json.load(file)

        return json.load(open(file_path))

    @staticmethod
    def load_from_http(config: FileReferenceConfig) -> Dict:
        """Load a manifest dictionary from a local file"""

        if not config.path.path:
            raise InvalidManifestPath()

        response = requests.get(urlunparse(config.path), stream=True)
        response.raise_for_status()  # Check for request errors

        # Check for compression on the file. If compressed, store it in a buffer
        # and decompress it.
        if (
            config.path.path.endswith(".gz")
            or response.headers.get("Content-Encoding") == "gzip"
        ):
            with gzip.GzipFile(fileobj=BytesIO(response.content)) as gz_file:
                return json.load(gz_file)

        return response.json()

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

    @staticmethod
    def load_from_snowflake(config: SnowflakeReferenceConfig) -> Dict:
        """Load a manifest dictionary from Snowflake stage."""
        snowflake_client = SnowflakeClient(
            stage=config.stage, stage_path=config.stage_path
        )

        return snowflake_client.load_manifest()

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
