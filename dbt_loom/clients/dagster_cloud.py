import gzip
import json
import os
from io import BytesIO
from typing import Any, Dict, Optional
from dagster_shared.plus.config import DagsterPlusCliConfig
from pydantic import BaseModel
import requests

from dbt_loom.logging import fire_event


class DagsterCloudReferenceConfig(BaseModel):
    """Configuration for a Dagster Cloud reference."""

    organization: str
    key: str


class DagsterCloudClient:
    """API Client for Dagster Cloud. Fetches manifest artifacts from Dagster Cloud."""

    def __init__(
        self,
        organization: str,
        key: str,
        token: Optional[str] = None,
    ) -> None:
        resolved_token = token or self._get_user_token_from_config()
        if resolved_token is None:
            raise Exception(
                "A Dagster Cloud API token must be provided to dbt-loom when fetching "
                "manifest data from Dagster Cloud. Please login to dagster cloud using "
                "`dg plus login` or provide one via the `DAGSTER_CLOUD_API_TOKEN` "
                "environment variable."
            )

        self.__token: str = resolved_token
        self.organization = organization
        self.key = key
        self.base_url = "https://dagster.cloud"

    def _get_user_token_from_config(self) -> Optional[str]:
        env_value = os.environ.get("DAGSTER_CLOUD_API_TOKEN")
        if env_value:
            return env_value

        if DagsterPlusCliConfig.exists():
            try:
                config = DagsterPlusCliConfig.get()
                return config.user_token
            except Exception:
                pass

        return None

    def load_manifest(self) -> Dict[str, Any]:
        """Load a manifest from Dagster Cloud artifact storage.

        1. POST to gen_artifact_get with the artifact key to get a presigned URL.
        2. GET the presigned URL to fetch the manifest JSON.
        """
        fire_event(msg=f"Requesting manifest from Dagster Cloud ({self.base_url}/{self.organization})")

        response = requests.post(
            f"{self.base_url}/{self.organization}/gen_artifact_get",
            json={"key": self.key},
            headers={"Dagster-Cloud-Api-Token": self.__token},
        )
        response.raise_for_status()

        presigned_url = response.json()["url"]
        fire_event(msg="Fetching manifest from presigned URL")

        manifest_response = requests.get(presigned_url)
        manifest_response.raise_for_status()

        if manifest_response.headers.get("Content-Encoding") == "gzip":
            with gzip.GzipFile(fileobj=BytesIO(manifest_response.content)) as gz_file:
                return json.load(gz_file)

        return manifest_response.json()
