import os
from typing import Any, Dict, Optional

from pydantic import BaseModel
import requests

from dbt_loom.logging import fire_event


class DbtCloudReferenceConfig(BaseModel):
    """Configuration for a dbt Cloud reference."""

    account_id: int
    job_id: int
    api_endpoint: Optional[str] = None
    step: Optional[int] = None


class DbtCloud:
    """API Client for dbt Cloud. Fetches latest manifest for a given dbt job."""

    def __init__(
        self,
        account_id: int,
        token: Optional[str] = None,
        api_endpoint: Optional[str] = None,
    ) -> None:
        resolved_token = token or os.environ.get("DBT_CLOUD_API_TOKEN")
        if resolved_token is None:
            raise Exception(
                "A DBT Cloud token must be provided to dbt-loom when fetching manifest "
                "data from dbt Cloud. Please provide one via the `DBT_CLOUD_API_TOKEN` "
                "environment variable."
            )

        self.__token: str = resolved_token

        self.account_id = account_id
        self.api_endpoint = api_endpoint or "https://cloud.getdbt.com/api/v2"

    def _query(self, endpoint: str, **kwargs) -> Dict:
        """Query the dbt Cloud Administrative API."""
        url = f"{self.api_endpoint}/{endpoint}"
        fire_event(msg=f"Querying {url}")
        response = requests.get(
            url,
            headers={
                "authorization": "Bearer " + self.__token,
                "content-type": "application/json",
            },
            **kwargs,
        )
        return response.json()

    def _get_manifest(self, run_id: int, step: Optional[int] = None) -> Dict[str, Any]:
        """Get the manifest json for a given dbt Cloud run."""
        params = {}
        if step:
            params["step"] = step

        return self._query(
            f"accounts/{self.account_id}/runs/{run_id}/artifacts/manifest.json",
            params=params,
        )

    def _get_latest_run(self, job_id: int) -> Dict[str, Any]:
        """Get the latest run performed by a dbt Cloud job."""
        return self._query(
            f"accounts/{self.account_id}/runs/",
            params={
                "job_definition_id": job_id,
                "status": 10,
                "order_by": "-finished_at",
                "limit": 1,
            },
        )["data"][0]

    def get_models(self, job_id: int, step: Optional[int] = None) -> Dict[str, Any]:
        """Get the latest state of all models by Job ID."""
        latest_run = self._get_latest_run(job_id=job_id)
        return self._get_manifest(run_id=latest_run["id"], step=step)
