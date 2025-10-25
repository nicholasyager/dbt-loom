import json
import gzip
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
    impersonate_service_account: Optional[str] = None


class GCSClient:
    """Client for GCS. Fetches manifest for a given bucket."""

    def __init__(
        self,
        project_id: str,
        bucket_name: str,
        object_name: str,
        credentials: Optional[Path] = None,
        impersonate_service_account: Optional[str] = None
    ) -> None:
        self.project_id = project_id
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.credentials = credentials
        self.impersonate_service_account = impersonate_service_account

    def load_manifest(self) -> Dict:
        """Load a manifest json from a GCS bucket."""

        try:
            from google.cloud import storage
        except ImportError:
            fire_event(msg="dbt-loom expected google-cloud-storage to be installed.")
            raise

        if self.impersonate_service_account:
            try:
                import google.auth
            except ImportError:
                fire_event(
                    msg="dbt-loom expected google-auth to be installed for service account impersonation."
                )
                raise
            source_credentials, _ = google.auth.default() if self.credentials is None else google.auth.load_credentials_from_file(self.credentials)
            impersonated_credentials = google.auth.impersonated_credentials.Credentials(
                source_credentials=source_credentials,
                target_principal=self.impersonate_service_account,
                target_scopes=["https://www.googleapis.com/auth/devstorage.read_only"],
                lifetime=60
            )
            fire_event(msg=f"Impersonating service account '{self.impersonate_service_account}' for GCS access.")
            client = storage.Client(
                project=self.project_id,
                credentials=impersonated_credentials
            )
        else:
            try:
                client = (
                    storage.Client.from_service_account_json(
                        self.credentials, project=self.project_id
                    )
                    if self.credentials
                    else storage.Client(project=self.project_id)
                )
            except FileNotFoundError:
                fire_event(
                    msg=f"The credentials file '{self.credentials}' was not found. attempting to use default application credentials."
                )
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
