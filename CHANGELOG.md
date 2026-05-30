# Changelog

All notable changes to **laila-core** are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Consolidated release pipeline: tags pushed to the private `LambdaLabsML/laila`
  repo now drive a single workflow that runs tests, builds, publishes to PyPI
  via OIDC trusted publishing, and mirrors the tagged tree to the public
  `LambdaLabsML/laila-core` repo.
- Pull-request CI (`.github/workflows/ci.yml`) running `ruff`, `mypy`, and
  `pytest` across Python 3.11 / 3.12.
- `ruff`, `mypy`, `pytest`, and `coverage` configuration in `pyproject.toml`.
- `.pre-commit-config.yaml` integrating `ruff`, `nbstripout`, and `gitleaks`
  on top of the existing project-local secret scanner.
- `CHANGELOG.md`, `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`, `SECURITY.md`,
  `CITATION.cff`, issue and PR templates, and a Dependabot config.
- `make help`, `make lint`, `make fmt`, `make typecheck`, `make build`,
  `make cov`, `make docs`, `make release-dryrun` targets.
- Distribution-contents audit in CI, the release pipeline, and
  `make release-dryrun`. Both the wheel and the sdist are now inspected and
  rejected if they contain `vault/`, `_dev/`, `tests/`, `hooks/`, `examples/`,
  `docs/`, `site/`, `.github/`, `.venv/`, `CLAUDE.md`, `conftest.py`, or
  `Makefile`. Listings are normalized (the wheel's `laila/` package prefix
  and the sdist's `<name>-<version>/` top-level dir are stripped) so the
  anchored regex correctly catches forbidden paths regardless of where they
  appear in the archive. Positive controls verify exactly one wheel and one
  sdist are present and that each contains its expected sentinel file
  (`laila/__init__.py`, `pyproject.toml`).
- `laila.__version__` resolved from installed package metadata.

### Changed

- All direct and optional dependencies now carry explicit lower and upper
  version bounds. `pydantic` is pinned to `>=2.5,<3` (v2 API is required).
- Developer tooling is installed via the standard `pre-commit` framework;
  the test suite no longer mutates `.git/hooks` or `git config`.
- Consolidated `publish-to-pypi.yml` and `sync-to-public.yml` into a single
  `release.yml` workflow. The merged workflow detects its mode at runtime:
  a `v*` tag push (or `workflow_dispatch` with the `tag` input) runs the
  full verify -> test -> build -> audit -> PyPI publish -> mirror -> tag ->
  GitHub Release pipeline; a plain push to `main` (with `paths-ignore`)
  only runs the mirror step. A single `concurrency: { group: release }`
  serializes every push to `laila-core`, eliminating the non-fast-forward
  race that two parallel sync jobs could otherwise hit.
- Bumped the `twine` pin in the `dev` extras from `<6` to `<7`. Setuptools
  now writes `Metadata-Version: 2.4`, which twine 5.x rejects in
  `--strict` mode; twine 6.x supports it. CI installs `twine` unpinned,
  so this only affected local `make release-dryrun`.
- Ruff baseline normalized: ran `ruff format` and `ruff check --fix`
  across the codebase (252 files reformatted, 977 lint issues auto-fixed).
  The `PL` (pylint) and `SIM` (flake8-simplify) rule sets are temporarily
  disabled rather than ignoring 30+ individual codes -- they conflict
  with several intentional patterns (lazy/circular imports, best-effort
  `try/except/pass` cleanup, lambdas in dispatch tables). The remaining
  stylistic noise (E731 lambdas, F405 star-re-exports, B007 unused loop
  vars, etc.) is enumerated in `[tool.ruff.lint].ignore` with comments
  explaining each. Tighten per-rule as the codebase is normalized.

### Fixed

- `entry/__init__.py` imported `EntryIdentityView` from `.entry`, but the
  class actually lives in `.entry_metadata` (the move was never reflected
  in the public re-export). This broke `import laila` at the very first
  CI test collection and was the root cause of every CI test job failure.
  Corrected to `from .entry_metadata import EntryIdentityView`.
- `__init__.py:terminate` reused the name `policy` (also a module-level
  import) as a loop variable, shadowing the import inside the function.
  Renamed the loop variable to `pol`.
- `policy/central/memory/schema/base.py:borrow` used a mutable default
  argument (`keys=[]`). Switched to `keys=None` with in-body normalization.
- `tests/functional/logger/unit_tests/test_logger.py::TestHDF5PoolSink`
  now skips cleanly when `h5py` is not installed instead of erroring out
  on the bare `from laila.pool.hdf5.hdf5 import HDF5Pool` in `setUp`.

### Removed

- Tag/publish automation from the public `laila-core` mirror. PyPI is now
  published exclusively from the private repo.
- Root-level `conftest.py`. With `[tool.setuptools] package-dir = { laila = "." }`,
  any `.py` file at the repo root is implicitly part of the `laila` package
  and ships in the wheel as `laila/<file>.py`. The file was already empty
  (a docstring noting that pre-commit handles hook installation), so it was
  deleted outright. No `tests/conftest.py` is required; pytest works without
  one. The distribution audit retains a `conftest.py` entry in its forbidden
  list as defense-in-depth against any future regression.

## [1.0.6]

Initial public history baseline. Earlier versions tracked privately.

[Unreleased]: https://github.com/LambdaLabsML/laila-core/compare/v1.0.6...HEAD
[1.0.6]: https://github.com/LambdaLabsML/laila-core/releases/tag/v1.0.6
