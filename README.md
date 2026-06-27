# @causal-order/testing

Deterministic deployment-style runtime harness and fault-injection toolkit for `causal-order` pipelines.

Status: ready for early use. The package is usable now, but the CLI and library surface may still evolve before `1.0.0`.

## Relationship to `causal-order` and `@causal-order/dedupe`

`@causal-order/testing` is the deployment-style test harness for the `causal-order` ecosystem.

It is meant to exercise the runtime path around:

- `causal-order`: the ordering core
- `@causal-order/dedupe`: the duplicate-filtering layer in front of the ordering core

In other words, this package is not another processing stage in the pipeline. It is the simulation, fault-injection, and runtime-validation harness used to test how `causal-order` and `@causal-order/dedupe` behave under realistic deployment pressure.

Conceptually:

```text
simulated nodes -> @causal-order/dedupe -> causal-order
         ^                                  
         |                                  
  @causal-order/testing
  (workload, topology, faults, summaries)
```

## What It Does

This package gives you a deployment-style harness for stressing the `causal-order` stack under realistic conditions such as:

- workload profiles
- topology expansion through `--node-ids`
- dark-node and jitter-node fault injection
- rejoin-aware recovery pacing
- run artifact generation and summary tooling

## Install

From npm as a user of the package:

```bash
npm install @causal-order/testing @causal-order/dedupe causal-order
```

From source while working on the package itself:

```bash
npm install
```

## Quick Start

Run a short deployment-style simulation:

```bash
causal-order-testing-runtime --duration 5m --time-scale 60 --profile expected-production-3way-mesh
```

Then inspect the latest run:

```bash
causal-order-testing-latest
causal-order-testing-summary artifacts/runs/<run-folder>
```

The package writes run artifacts under `artifacts/runs/` in the caller's working directory by default.

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

## For Maintainers

This package is also the cleaned extraction of the repo harness originally used to validate the `causal-order` + `@causal-order/dedupe` runtime path, so it keeps repo-facing tooling and release scaffolding as well.

### Included

- `src/deployment-runtime.ts`: baseline multi-node runtime harness
- `src/deployment-runtime-rejoin.ts`: rejoin-aware runtime harness
- `src/deployment-common.ts`: config parsing, workload profiles, artifact helpers, simulation utilities
- `src/deployment-collector.ts`: collector and ordering pipeline
- `src/deployment-node*.ts`: node simulators
- `src/summarize-run.ts`, `src/compare-runs.ts`, `src/duplicate-leak-summary.ts`, `src/latest-summary.ts`: artifact readers
- `profiles/`: bundled workload profile JSON files

### Local Development

If you are working on the package itself:

```bash
npm run build
npm run test:smoke
npm run ci
npm run pack:check
```

The smoke test is intentionally lightweight. It verifies the packaged CLI help output, runs one tiny runtime pass, and confirms the summary tools can read the produced artifact.

### First Manual npm Publish

For the first publish, keep it manual before relying on GitHub Actions:

```bash
npm login
npm run release:check
npm publish --access public
```

Recommended order:

1. Push the repo to GitHub first.
2. Run `npm login`.
3. Run `npm run release:check`.
4. Publish manually once with `npm publish --access public`.
5. After the first successful publish, let the GitHub Actions publish workflow handle later releases.

### GitHub Repo Setup

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
