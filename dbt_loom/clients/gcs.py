import json
import gzip
import os
from io import BytesIO
from pathlib import Path
from typing import Dict, Optional

from pydantic import BaseModel

from dbt_loom.logging import fire_event


class GCSReferenceConfig(BaseModel):
    """Configuration for a GCS reference"""

    project_id: str
    bucket_name: str
    object_name: str
    credentials: Optional[Path] = None


class GCSClient:
    """Client for GCS. Fetches manifest for a given bucket."""

    def __init__(
        self,
        project_id: str,
        bucket_name: str,
        object_name: str,
        credentials: Optional[Path] = None,
    ) -> None:
        self.project_id = project_id
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.credentials = credentials

    def load_manifest(self) -> Dict:
        """Load a manifest json from a GCS bucket."""

        try:
            from google.cloud import storage
        except ImportError:
            fire_event(msg="dbt-loom expected google-cloud-storage to be installed.")
            raise

        # Check if credentials parameter set. If file doesn't exist fall back to ADC. If that fails raise exception.
        if self.credentials:
            try:
                client = storage.Client.from_service_account_json(
                    json_credentials_path=self.credentials,
                    project=self.project_id
                )
            except FileNotFoundError:
                try:
                    client = storage.Client(project=self.project_id)
                except Exception as e:
                    fire_event(msg=f"Failed to load credentials: {e}")
                    raise
        else:
            # Default fall-back to Application Default Credentials
            client = storage.Client(project=self.project_id)

        bucket = client.get_bucket(self.bucket_name)
        blob = bucket.get_blob(self.object_name)
        if not blob:
            raise Exception(
                f"The object `{self.object_name}` does not exist in bucket "
                f"`{self.bucket_name}`."
            )

        if self.object_name.endswith(".gz"):
            compressed_manifest = blob.download_as_bytes()
            with gzip.GzipFile(fileobj=BytesIO(compressed_manifest)) as gzip_file:
                manifest_json = gzip_file.read()
        else:
            manifest_json = blob.download_as_text()

        try:
            return json.loads(manifest_json)
        except Exception:
            raise Exception(
                f"The object `{self.object_name}` does not contain valid JSON."
            )
