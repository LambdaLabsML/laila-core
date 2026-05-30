# Contributing to laila-core

Thanks for your interest in contributing. This document covers the day-to-day
workflow: setting up a dev environment, running checks, and getting a change
merged.

## Development environment

Requirements:

- Python 3.11 or 3.12
- `git` and a POSIX-ish shell
- (Optional) Docker, for the cloud-backed integration tests

One-shot setup:

```bash
git clone git@github.com:LambdaLabsML/laila.git
cd laila
make init
```

`make init` will:

- create an editable install of the package with the `[all,dev,docs]` extras
- install the `pre-commit` framework and register the project hooks
- register `nbstripout` for the repo so Jupyter notebook outputs are stripped
  on commit

## Day-to-day commands

```bash
make lint            # ruff check
make fmt             # ruff format + ruff check --fix
make typecheck       # mypy
make test-fast       # pytest, skipping cloud-marked tests
make cov             # pytest with coverage report
make docs-serve      # live-reload docs at http://127.0.0.1:8000
make release-dryrun  # build + twine check + wheel-contents audit
```

Run `make help` for the full list.

## Style and quality gates

- Formatting and linting are enforced by **ruff**. The config lives in
  `pyproject.toml` under `[tool.ruff]`.
- Type-checking uses **mypy**. Strictness is intentionally permissive today
  and is being tightened module-by-module — see `[tool.mypy.overrides]`.
- Tests use **pytest**. Use markers (`@pytest.mark.cloud`, `@pytest.mark.slow`,
  `@pytest.mark.integration`) for anything that should not run in the default
  PR CI matrix.
- Pre-commit hooks (`ruff`, `nbstripout`, `gitleaks`, project-local secret
  scanner) run on every commit. Bypass only with strong justification.

## Branching and pull requests

1. Branch from `main`: `git checkout -b your-handle/short-topic`.
2. Keep commits focused. Use imperative present tense (`add foo`, not
   `added foo`).
3. Run `make lint typecheck test-fast` locally before pushing.
4. Open a PR against `main`. Fill in the PR template.
5. CI must be green. At least one approving review is required.
6. Squash-merge is the default. The PR title becomes the squashed commit
   message — keep it Conventional-Commit-ish (`feat:`, `fix:`, `docs:`,
   `chore:`, `refactor:`, `test:`, `perf:`, `ci:`).

## Tests that need credentials

Tests under `tests/functional/pools/{s3,gcs,azure,redis,postgres,mongo,…}`
talk to real services and are marked with `@pytest.mark.cloud`. They are
skipped by default in PR CI. To run them locally:

```bash
make test-cloud
```

A local `docker-compose.test.yml` is on the roadmap for the services that
have OSS equivalents (Redis, Postgres, Mongo, MinIO).

## Releasing

Releases are cut from the **private** `LambdaLabsML/laila` repo only.

1. Bump `project.version` in `pyproject.toml`.
2. Update `CHANGELOG.md` (move `Unreleased` items under the new version
   heading).
3. Commit, then tag: `git tag -a vX.Y.Z -m "Release vX.Y.Z" && git push --tags`.
4. The `Release` workflow will:
   - verify the tag matches `pyproject.toml`'s version,
   - run the full private test suite,
   - build sdist + wheel and audit their contents,
   - publish to PyPI via OIDC trusted publishing,
   - mirror the (sanitized) working tree to `LambdaLabsML/laila-core`
     and push the same tag there,
   - open a GitHub Release with the wheel and sdist attached.

The public `laila-core` repository contains **no release automation** — it
is a read-only mirror.

## Reporting bugs and requesting features

Use the GitHub issue templates. For security issues, **do not file a public
issue** — follow `SECURITY.md` instead.

## Code of conduct

By participating you agree to abide by the [Code of Conduct](CODE_OF_CONDUCT.md).
