# @causal-order/testing

Deterministic deployment-style runtime harness and fault-injection toolkit for `causal-order` pipelines.

This package is the cleaned extraction of the repo harness used to stress:

- workload profiles
- topology expansion through `--node-ids`
- dark-node and jitter-node fault injection
- rejoin-aware recovery pacing
- run artifact generation and summary tooling

## Included

- `src/deployment-runtime.ts`: baseline multi-node runtime harness
- `src/deployment-runtime-rejoin.ts`: rejoin-aware runtime harness
- `src/deployment-common.ts`: config parsing, workload profiles, artifact helpers, simulation utilities
- `src/deployment-collector.ts`: collector and ordering pipeline
- `src/deployment-node*.ts`: node simulators
- `src/summarize-run.ts`, `src/compare-runs.ts`, `src/duplicate-leak-summary.ts`, `src/latest-summary.ts`: artifact readers
- `profiles/`: copied workload profile JSON files from the source repo

## Install

```bash
npm install
```

For a package publish/install shape:

```bash
npm install @causal-order/testing @causal-order/dedupe causal-order
```

## CLI

Baseline runtime:

```bash
causal-order-testing-runtime --duration 20m --time-scale 60 --profile expected-production-3way-mesh
```

Rejoin-aware runtime:

```bash
causal-order-testing-runtime-rejoin --duration 20m --time-scale 60 --profile expected-production-mesh-dark-jitter --dark-nodes edge-b --jitter-nodes edge-c
```

Summaries:

```bash
causal-order-testing-latest
causal-order-testing-summary artifacts/runs/<run-folder>
causal-order-testing-compare artifacts/runs/<older-run> artifacts/runs/<newer-run>
causal-order-testing-duplicates artifacts/runs/<run-folder>
```

## Library API

The package also exposes the reusable config and harness helpers:

```ts
import {
  buildConfig,
  buildRejoinHarnessConfig,
  parseDurationToMs,
  formatDuration,
  createSimulationClock,
} from "@causal-order/testing";
```

This is the intended public API for composition. The CLI runtime files remain package bins rather than import-first modules.

## Local Development

If you are working on the package itself:

```bash
npm run build
npm run ci
npm run pack:check
```

## GitHub Repo Setup

If you move this into its own GitHub repository, the current package metadata assumes:

- repo: `https://github.com/GazaliAhmad/causal-order-testing`
- package: `@causal-order/testing`

Before first publish:

1. Create the GitHub repository.
2. Add an `NPM_TOKEN` repository secret with npm publish access.
3. Push the default branch and confirm the `CI` workflow passes.
4. Publish either with the `Publish` workflow or from a GitHub Release event.

The repo includes:

- `.github/workflows/ci.yml`: install, typecheck, build, and dry-run pack
- `.github/workflows/publish.yml`: verified npm publish with provenance
- `CONTRIBUTING.md`: local workflow and publish expectations
- `CHANGELOG.md`: release history starter

## Notes

- Built-in workload profiles now resolve relative to the installed package instead of the caller's working directory.
- The harness imports `DedupeGateway` and dedupe config helpers from the published `@causal-order/dedupe` package instead of the original repo-local source file.
- This extraction keeps the runtime harness and summary utilities together, but it does not include the original contract tests from the source repo.
- Default run artifacts still land under `artifacts/runs/` in the caller's working directory.
