import json
import os
import gzip
from io import BytesIO
from typing import Dict


from pydantic import BaseModel

from dbt_loom.logging import fire_event


class AzureReferenceConfig(BaseModel):
    """Configuration for an reference stored in Azure Storage"""

    container_name: str
    object_name: str
    account_name: str


class AzureClient:
    """A client for loading manifest files from Azure storage."""

    def __init__(
        self, container_name: str, object_name: str, account_name: str
    ) -> None:
        self.account_name = account_name
        self.container_name = container_name
        self.object_name = object_name

    def load_manifest(self) -> Dict:
        """Load the manifest.json file from Azure storage."""

        try:
            from azure.identity import DefaultAzureCredential
        except ImportError:
            fire_event(msg="dbt-loom expected azure-identity to be installed.")
            raise

        try:
            from azure.storage.blob import BlobServiceClient
        except ImportError:
            fire_event(msg="dbt-loom expected azure-storage-blob to be installed.")
            raise

        connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        try:
            if connection_string:
                blob_service_client = BlobServiceClient.from_connection_string(
                    connection_string
                )
            else:
                account_url = f"{self.account_name}.blob.core.windows.net"
                blob_service_client = BlobServiceClient(
                    account_url, credential=DefaultAzureCredential()
                )
            blob_client = blob_service_client.get_blob_client(
                container=self.container_name, blob=self.object_name
            )
        except Exception as e:
            raise Exception(
                "Unable to connect to Azure. Please confirm your credentials, connection details, and network."
            )

        # Deserialize the body of the object.
        try:
            if self.object_name.endswith(".gz"):
                with gzip.GzipFile(
                    fileobj=BytesIO(blob_client.download_blob().readall())
                ) as gzipfile:
                    content = gzipfile.read().decode("utf-8")
            else:
                content = blob_client.download_blob(encoding="utf-8").readall()
        except Exception:
            raise Exception(
                f"Unable to read the data contained in the object `{self.object_name}"
            )

        try:
            return json.loads(content)
        except Exception:
            raise Exception(
                f"The object `{self.object_name}` does not contain valid JSON."
            )
