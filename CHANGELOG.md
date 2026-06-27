# Changelog

All notable changes to `@causal-order/testing` will be documented in this file.

## 0.1.0

- Extracted the deployment-style runtime harness into a standalone package.
- Added package-relative workload profile loading for installed usage.
- Added CLI entrypoints for runtime execution and artifact summaries.
- Added npm-facing package metadata, keywords, and publish configuration for `@causal-order/testing`.
- Added wrapper executables under `bin/` so packaged CLI commands install cleanly for end users.
- Added a lightweight smoke test and release-check scripts for first-user and first-publish validation.
- Reworked the README to be more package-facing while retaining maintainer and repo-setup guidance.
- Added GitHub Actions CI and npm publish workflow scaffolding.
