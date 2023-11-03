from typing import Dict, Optional
import json

from google.cloud import storage

class GCSClient:
    """Client for GCS. Fetches manifest for a given bucket."""
    
    def __init__(
        self,
        bucket_name: str,
        blob_name: str,
        credentials: Optional[str] = None,
    ) -> None:
        self.bucket_name = bucket_name
        self.blob_name = blob_name
        self.credentials = credentials

    def load_manifest(self) -> Dict:
        """Load a manifest json from a GCS bucket."""
        client = storage.Client.from_service_account_json(self.credentials) if self.credentials else storage.Client()
        bucket = client.get_bucket(self.bucket_name)
        blob = bucket.get_blob(self.blob_name)
        if not blob:
            raise Exception(f"The blob `{self.blob_name}` does not exist in bucket `{self.bucket_name}`.")
        manifest_json = blob.download_as_text()
        return json.loads(manifest_json)
