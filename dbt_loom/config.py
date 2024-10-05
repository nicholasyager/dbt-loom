from enum import Enum
from pathlib import Path
import re
from typing import List, Union

from pydantic import BaseModel, AnyUrl, validator

from dbt_loom.clients.az_blob import AzureReferenceConfig
from dbt_loom.clients.dbt_cloud import DbtCloudReferenceConfig
from dbt_loom.clients.gcs import GCSReferenceConfig
from dbt_loom.clients.s3 import S3ReferenceConfig


class ManifestReferenceType(str, Enum):
    """Type of ManifestReference"""

    file = "file"
    dbt_cloud = "dbt_cloud"
    gcs = "gcs"
    s3 = "s3"
    azure = "azure"


class FileReferenceConfig(BaseModel):
    """Configuration for a file reference"""

    path: AnyUrl

    @validator("path", pre=True, always=True)
    def default_path(cls, v, values):
        """
        Check if the provided path is a valid URL. If not, convert it into an
        absolute file path.
        """

        if isinstance(v, AnyUrl):
            return v

        if bool(re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*://", v)):
            return v

        return "file://" + str(Path(v).absolute())


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
