import json
import gzip
from io import BytesIO
from typing import Dict
from dbt_loom.logging import fire_event
from pydantic import BaseModel


class DatabricksReferenceConfig(BaseModel):
    """Configuration for a reference stored in Databricks"""

    path: str


class DatabricksClient:
    """A client for loading manifest files from Databricks."""

    def __init__(self, path: str) -> None:
        self.path = path

    def load_manifest(self) -> Dict:
        """Load the manifest.json file from Databricks."""

        # Import the Databricks SDK, which is a dependency of the dbt-databricks adapter
        try:
            from databricks.sdk import WorkspaceClient
        except ImportError:
            fire_event(msg="dbt-loom expected the Databricks SDK to be installed.")
            raise

        try:
            # Initialize the workspace client; auth is handled via Databricks Unified Authentication model
            w = WorkspaceClient()
            # Retrieve the manifest file object
            resp = w.files.download(self.path)
        except Exception:
            fire_event(msg="Unable to retrieve file from Databricks.")
            raise

        # Deserialize the object.
        try:
            if self.path.endswith('.gz'):
                with gzip.GzipFile(fileobj=BytesIO(resp.contents.read())) as gzipfile:
                    content = gzipfile.read().decode('utf-8')
            else:
                content = resp.contents.read()
            return json.loads(content)
        except json.decoder.JSONDecodeError:
            fire_event(msg=f"The object `{self.path}` does not contain valid JSON.")
            raise
        except Exception:
            fire_event(msg=f"Unable to read the data contained in the object `{self.path}")
            raise
