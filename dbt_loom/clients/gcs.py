import json
from pathlib import Path
from typing import Dict, Optional

from google.cloud import storage
from google.auth import exceptions
from google.oauth2 import service_account

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

    def _get_client(self):
        """Get a storage client using the provided credentials or default credentials."""
        if self.credentials:
            try:
                credentials = service_account.Credentials.from_service_account_file(
                    self.credentials
                )
                client = storage.Client(credentials=credentials, project=self.project_id)
            except exceptions.GoogleAuthError as e:
                raise Exception(f"Failed to load specified credentials: {e}")
        else:
            # Will attempt to use default credentials
            try:
                client = storage.Client(project=self.project_id)
            except exceptions.DefaultCredentialsError as e:
                raise Exception(f"Could not automatically determine credentials: {e}")
        return client

    def load_manifest(self) -> Dict:
        """Load a manifest json from a GCS bucket."""
        client = self._get_client()
        bucket = client.get_bucket(self.bucket_name)
        blob = bucket.get_blob(self.object_name)
        if not blob:
            raise Exception(
                f"The object `{self.object_name}` does not exist in bucket `{self.bucket_name}`."
            )

        manifest_json = blob.download_as_text()

        try:
            return json.loads(manifest_json)
        except Exception:
            raise Exception(
                f"The object `{self.object_name}` does not contain valid JSON."
            )
