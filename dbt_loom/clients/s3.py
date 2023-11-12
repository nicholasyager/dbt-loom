import json
from typing import Dict

import boto3


class S3Client:
    """A client for loading manifest files from S3-compatible object stores."""

    def __init__(self, bucket_name: str, object_name: str) -> None:
        self.bucket_name = bucket_name
        self.object_name = object_name

    def load_manifest(self) -> Dict:
        """Load the manifest.json file from an S3 bucket."""

        client = boto3.client("s3")

        # TODO: Determine if I need to add args for SSE
        try:
            response = client.get_object(Bucket=self.bucket_name, Key=self.object_name)
        except client.exceptions.NoSuchBucket:
            raise Exception(f"The bucket `{self.bucket_name}` does not exist.")
        except client.exceptions.NoSuchKey:
            raise Exception(
                f"The object `{self.object_name}` does not exist in bucket "
                f"`{self.bucket_name}`."
            )

        # Deserialize the body of the object.
        try:
            content = response["Body"].read().decode("utf-8")
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
