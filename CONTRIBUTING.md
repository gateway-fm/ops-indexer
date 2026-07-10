# Contributing

Thank you for contributing to chain-indexer.

## License

By contributing, you agree that your contribution is licensed under the Apache
License, Version 2.0.

## Developer Certificate of Origin

This project uses the Developer Certificate of Origin 1.1. Sign off each commit
with:

```bash
git commit -s
```

The sign-off certifies that you have the right to submit the contribution under
the project license.

## Development

Before opening a pull request:

- Keep changes focused and reviewable.
- Add or update tests when changing behavior.
- Do not commit secrets, private audit reports, local agent state, or generated
  credentials.
- Do not report security issues in public pull requests or issues; follow
  SECURITY.md.

Useful checks:

```bash
go test ./...
go vet ./...
buf lint
```

Some checks require optional services such as Postgres, Docker, or an EVM node.
