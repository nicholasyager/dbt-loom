import os
from typing import Dict, Optional

from pydantic import BaseModel

from dbt_loom.logging import fire_event


class ParadimeReferenceConfig(BaseModel):
    """Configuration for a Paradime reference."""

    schedule_name: str
    api_key: Optional[str] = None
    api_secret: Optional[str] = None
    api_endpoint: Optional[str] = None
    command_index: Optional[int] = None


class ParadimeClient:
    """
    API Client for Paradime. Fetches latest manifest for a given Bolt schedule.
    """

    api_key: str
    api_secret: str
    api_endpoint: str
    schedule_name: str
    command_index: Optional[int]

    def __init__(
        self,
        schedule_name: str,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        api_endpoint: Optional[str] = None,
        command_index: Optional[int] = None,
    ) -> None:
        api_key_resolved = api_key or os.environ.get("PARADIME_API_KEY")
        api_secret_resolved = api_secret or os.environ.get("PARADIME_API_SECRET")
        api_endpoint_resolved = api_endpoint or os.environ.get("PARADIME_API_ENDPOINT")

        if not (api_key_resolved and api_secret_resolved and api_endpoint_resolved):
            raise Exception(
                "A Paradime API key, secret, and endpoint must be provided to dbt-loom when fetching manifest "
                "data from Paradime. Please provide them via the `PARADIME_API_KEY`, `PARADIME_API_SECRET`, and "
                "`PARADIME_API_ENDPOINT` environment variables, or via the dbt-loom config yml file. "
            )

        self.api_key = api_key_resolved
        self.api_secret = api_secret_resolved
        self.api_endpoint = api_endpoint_resolved
        self.schedule_name = schedule_name
        self.command_index = command_index

    def load_manifest(self) -> Dict:
        """Load the manifest.json for the latest run of the schedule."""

        try:
            from paradime import Paradime
        except ImportError:
            fire_event(msg="dbt-loom expected paradime-io to be installed.")
            raise

        paradime_client = Paradime(
            api_key=self.api_key,
            api_secret=self.api_secret,
            api_endpoint=self.api_endpoint,
        )

        return paradime_client.bolt.get_latest_manifest_json(
            schedule_name=self.schedule_name,
            command_index=self.command_index,
        )
