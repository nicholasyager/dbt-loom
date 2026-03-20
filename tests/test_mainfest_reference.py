from dbt_loom.config import ManifestReference, ManifestReferenceType, FileReferenceConfig


def test_manifest_reference_with_database_alias():
    """Confirm ManifestReference accepts database_alias."""
    ref = ManifestReference(
        name="test",
        type=ManifestReferenceType.file,
        config=FileReferenceConfig(path="./manifest.json"),
        database_alias={"prod_db": "dev_db"},
    )
    assert ref.database_alias == {"prod_db": "dev_db"}


def test_manifest_reference_default_database_alias():
    """Confirm ManifestReference defaults database_alias to empty dict."""
    ref = ManifestReference(
        name="test",
        type=ManifestReferenceType.file,
        config=FileReferenceConfig(path="./manifest.json"),
    )
    assert ref.database_alias == {}
