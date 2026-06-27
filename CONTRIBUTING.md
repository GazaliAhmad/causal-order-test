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
- Do the first npm publish manually so the package name and access settings are confirmed once end to end.
- After the first successful manual publish, npm publishing can be handled through the GitHub Actions publish workflow.
- The GitHub repo should define an `NPM_TOKEN` secret with publish access to `@causal-order/testing`.
