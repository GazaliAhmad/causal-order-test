# Contributing

## Local Setup

```bash
npm install
npm run ci
```

## Development Workflow

1. Make your change.
2. Run `npm run ci`.
3. Update `README.md` or `CHANGELOG.md` when behavior or release-facing docs change.

## Publishing Expectations

- CI should pass on the default branch before publishing.
- npm publishing is handled through the GitHub Actions publish workflow.
- The GitHub repo should define an `NPM_TOKEN` secret with publish access to `@causal-order/testing`.
