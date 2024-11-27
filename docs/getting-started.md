# Getting Started

To begin, install the `dbt-loom` python package.

```console
pip install dbt-loom
```

Next, create a `dbt-loom` configuration file. This configuration file provides the paths for your
upstream project's manifest files.

```yaml
manifests:
  - name: project_name # This should match the project's real name
    type: file
    config:
      # A path to your manifest. This can be either a local path, or a remote
      # path accessible via http(s).
      path: path/to/manifest.json
```

By default, `dbt-loom` will look for `dbt_loom.config.yml` in your working directory. You can also set the
`DBT_LOOM_CONFIG` environment variable.

## Using dbt Cloud as an artifact source

You can use dbt-loom to fetch model definitions from dbt Cloud by setting up a `dbt-cloud` manifest in your `dbt-loom` config, and setting the `DBT_CLOUD_API_TOKEN` environment variable in your execution environment.

```yaml
manifests:
  - name: project_name
    type: dbt_cloud
    config:
      account_id: <YOUR DBT CLOUD ACCOUNT ID>

      # Job ID pertains to the job that you'd like to fetch artifacts from.
      job_id: <REFERENCE JOB ID>

      api_endpoint: <DBT CLOUD ENDPOINT>
      # dbt Cloud has multiple regions with different URLs. Update this to
      # your appropriate dbt cloud endpoint.

      step_id: <JOB STEP>
      # If your job generates multiple artifacts, you can set the step from
      # which to fetch artifacts. Defaults to the last step.
```

## Using an S3-compatible object store as an artifact source

You can use dbt-loom to fetch manifest files from S3-compatible object stores
by setting up ab `s3` manifest in your `dbt-loom` config. Please note that this
approach supports all standard boto3-compatible environment variables and authentication mechanisms. Please see the [boto3 documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html#environment-variables) for more details.

```yaml
manifests:
  - name: project_name
    type: s3
    config:
      bucket_name: <YOUR S3 BUCKET NAME>
      # The name of the bucket where your manifest is stored.

      object_name: <YOUR OBJECT NAME>
      # The object name of your manifest file.
```

## Using GCS as an artifact source

You can use dbt-loom to fetch manifest files from Google Cloud Storage by setting up a `gcs` manifest in your `dbt-loom` config.

```yaml
manifests:
  - name: project_name
    type: gcs
    config:
      project_id: <YOUR GCP PROJECT ID>
      # The alphanumeric ID of the GCP project that contains your target bucket.

      bucket_name: <YOUR GCS BUCKET NAME>
      # The name of the bucket where your manifest is stored.

      object_name: <YOUR OBJECT NAME>
      # The object name of your manifest file.

      credentials: <PATH TO YOUR SERVICE ACCOUNT JSON CREDENTIALS>
      # The OAuth2 Credentials to use. If not passed, falls back to the default inferred from the environment.
```

## Using Azure Storage as an artifact source

You can use dbt-loom to fetch manifest files from Azure Storage
by setting up an `azure` manifest in your `dbt-loom` config. The `azure` type implements
the [DefaultAzureCredential](https://learn.microsoft.com/en-us/python/api/azure-identity/azure.identity.defaultazurecredential?view=azure-python)
class, supporting all environment variables and authentication mechanisms.
Alternatively, set the `AZURE_STORAGE_CONNECTION_STRING` environment variable to
authenticate via a connection string.

```yaml
manifests:
  - name: project_name
    type: azure
    config:
      account_name: <YOUR AZURE STORAGE ACCOUNT NAME> # The name of your Azure Storage account
      container_name: <YOUR AZURE STORAGE CONTAINER NAME> # The name of your Azure Storage container
      object_name: <YOUR OBJECT NAME> # The object name of your manifest file.
```
