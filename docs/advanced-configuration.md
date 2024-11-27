# Advanced Configuration

`dbt-loom` also has a couple advanced configuration options for power users.

## Using environment variables in the `dbt-loom` config

You can easily incorporate your own environment variables into the config file. This allows for dynamic configuration values that can change based on the environment. To specify an environment variable in the `dbt-loom` config file, use one of the following formats:

`${ENV_VAR}` or `$ENV_VAR`

### Example:

```yaml
manifests:
  - name: revenue
    type: gcs
    config:
      project_id: ${GCP_PROJECT}
      bucket_name: ${GCP_BUCKET}
      object_name: ${MANIFEST_PATH}
```

## Exclude nested packages

In some circumstances, like running `dbt-project-evaluator`, you may not want a
given package in an upstream project to be imported into a downstream project.
You can manually exclude downstream projects from injecting assets from packages
by adding the package name to the downstream project's `excluded_packages` list.

```yaml
manifests:
  - name: revenue
    type: file
    config:
      path: ../revenue/target/manifest.json
    excluded_packages:
      # Provide the string name of the package to exclude during injection.
      - dbt_project_evaluator
```

## Gzipped files

`dbt-loom` natively supports decompressing gzipped manifest files. This is useful to reduce object storage size and to minimize loading times when reading manifests from object storage. Compressed file detection is triggered when the file path for the manifest is suffixed
with `.gz`.

```yaml
manifests:
  - name: revenue
    type: s3
    config:
      bucket_name: example_bucket_name
      object_name: manifest.json.gz
```

## Enabling Telemetry

By default, the `dbt-loom` plugin blocks outbound telemetry that reports on
the use of this plugin. This is a privacy-preserving measure for `dbt-loom`
users that does not impact the function of dbt-core and does not impede
dbt-core development in any way. If you _want_ this telemetry to be sent, you
can re-enable this behavior by setting the `enable_telemetry` property
in the `dbt_loom.config.yml` file.

```yaml
enable_telemetry: true
manifests: ...
```
