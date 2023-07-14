# Jaffle Shop BI Reports

This project uses [Evidence.dev](https://Evidence.dev) for BI reporting.

## Getting Started

Run the BI server from your local workstation:

```shell
cd reports
npm run dev
```

This will launch the Evidence webserver in developer mode.

## Testing for breakages

The following command can be used to confirm that reports and queries are still valid:

```console
npm run build:strict
```

## Updating to the latest version of Evidence

1. Check your version against the version number for the [latest release]([Evidence.dev Releases](https://github.com/evidence-dev/evidence/releases)).
2. Run `npm install evidence-dev/evidence@latest` to bump the version in `package.json` and automatically update dependenceis in `package-lock.json`.

## Learning More

- [Getting Started Walkthrough](https://docs.evidence.dev/getting-started/install-evidence)
- [Project Home Page](https://www.evidence.dev)
- [Github](https://github.com/evidence-dev/evidence)
- [Evidence.dev Releases](https://github.com/evidence-dev/evidence/releases)
