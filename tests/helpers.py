import dbt_common.semver as semver
from dbt.version import installed

import pytest


def dbt_version(*version_specs: str):
    """Skip the test unless the installed dbt-core satisfies all given version specs.

    Supports any semver specifier that dbt itself understands:
        @dbt_version(">=1.10.0")            — 1.10+
        @dbt_version(">=1.10.0", "<1.11.0") — 1.10.x only
        @dbt_version("1.9.3")               — exactly 1.9.3
        @dbt_version(">1.8.0", "<=1.10.5")  — range
    """
    specs = [semver.VersionSpecifier.from_version_string(s) for s in version_specs]
    compatible = semver.versions_compatible(installed, *specs)

    return pytest.mark.skipif(
        not compatible,
        reason=f"Requires dbt-core {', '.join(version_specs)} (installed: {installed})",
    )


