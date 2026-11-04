# Use bash so the release audit can rely on `shopt` and arrays.
SHELL := /bin/bash
.SHELLFLAGS := -euo pipefail -c

.PHONY: help init lint fmt typecheck test test-fast test-cloud test-workloads test-interop test-go cov build clean docs docs-serve release-dryrun

help:
	@echo "laila — developer targets"
	@echo ""
	@echo "  make init           Install package (editable) + dev extras + pre-commit hooks"
	@echo "  make lint           Run ruff lint"
	@echo "  make fmt            Run ruff format (writes changes)"
	@echo "  make typecheck      Run mypy"
	@echo "  make test           Run the full test suite"
	@echo "  make test-fast      Run only non-cloud, non-slow tests"
	@echo "  make test-cloud     Run cloud-marked tests (requires creds)"
	@echo "  make test-workloads Run the long-running workload/stress suite (tests/workloads)"
	@echo "  make test-interop   Run the cross-language interop matrix (Python/JS/C/Go/Rust; builds c_harness + go_harness + rs_harness)"
	@echo "  make test-go        Run the Go port: conformance + facades check, unit tests, Python<->Go interop"
	@echo "  make cov            Run tests with coverage report"
	@echo "  make build          Build sdist + wheel into dist/"
	@echo "  make clean          Remove build/dist/cache artifacts"
	@echo "  make docs           Build MkDocs site into site/"
	@echo "  make docs-serve     Live-reload docs server on http://127.0.0.1:8000"
	@echo "  make release-dryrun Build, twine check, and audit wheel + sdist contents"

init:
	python -m pip install --upgrade pip
	python -m pip install -e ".[all,dev,docs]"
	pre-commit install
	pre-commit install --hook-type pre-push || true

lint:
	ruff check .

fmt:
	ruff format .
	ruff check --fix .

typecheck:
	mypy .

test:
	python -m pytest tests/ -v

test-fast:
	python -m pytest tests/ -m "not cloud and not slow"

test-cloud:
	python -m pytest tests/ -m "cloud"

# Long-running real-workload / stress suite (opt-in; ~1h on a 2-CPU box).
# Tune with LAILA_WORKLOAD_SCALE=<float> and LAILA_WORKLOAD_TIMEOUT=<seconds>.
test-workloads:
	python -m pytest tests/workloads -m "workload" --tb=short -p no:cacheprovider

# Cross-language interop matrix (tests/interop). Languages whose toolchain is
# absent are skipped; set LAILA_QEMU_URI/LAILA_QEMU_GID to include the emulated
# ESP32 (laila-c/emulator/run_qemu_test.sh does this automatically).
test-interop:
	LAILA_INTEROP_BUILD_C=1 LAILA_INTEROP_BUILD_GO=1 LAILA_INTEROP_BUILD_RUST=1 python -m pytest tests/interop -m interop -v -p no:cacheprovider

# Go port (laila-go). Mirrors .github/workflows/laila-go.yml minus the race job.
test-go:
	python3 laila-go/tools/conformance/check.py
	python3 laila-go/tools/facades/gen_facades.py --check
	cd laila-go && go vet ./... && go test -count=1 -short ./...
	cd laila-go/tests && go test -count=1 -short -timeout 45m ./functional/... ./interop/

transports-emulators-up:
	docker compose -f tests/transports/emulators/docker-compose.yml up -d
	@echo "Run 'sudo tests/transports/emulators/provision.sh up' for kernel/serial interfaces."

transports-emulators-down:
	docker compose -f tests/transports/emulators/docker-compose.yml down
	-sudo tests/transports/emulators/provision.sh down

cov:
	python -m pytest tests/ -m "not cloud and not slow" \
		--cov=laila --cov-report=term-missing --cov-report=html

build: clean
	python -m build

clean:
	rm -rf build/ dist/ *.egg-info/ .pytest_cache/ .mypy_cache/ .ruff_cache/ htmlcov/ .coverage coverage.xml site/

docs:
	mkdocs build --strict

docs-serve:
	mkdocs serve

release-dryrun: build
	python -m twine check --strict dist/*
	@echo ""
	@echo "── Distribution contents ──────────────────────"
	@shopt -s nullglob; \
	WHEELS=(dist/*.whl); SDISTS=(dist/*.tar.gz); \
	if [ $${#WHEELS[@]} -ne 1 ]; then echo "FATAL: expected exactly 1 wheel in dist/, found $${#WHEELS[@]}."; exit 1; fi; \
	if [ $${#SDISTS[@]} -ne 1 ]; then echo "FATAL: expected exactly 1 sdist in dist/, found $${#SDISTS[@]}."; exit 1; fi; \
	python -m zipfile -l "$${WHEELS[0]}" | tee dist/.wheel-listing-raw.txt; \
	echo ""; \
	tar -tzf "$${SDISTS[0]}" | tee dist/.sdist-listing-raw.txt
	@sed 's|^laila/||' dist/.wheel-listing-raw.txt > dist/.wheel-listing.txt
	@sed 's|^[^/]*/||' dist/.sdist-listing-raw.txt > dist/.sdist-listing.txt
	@FORBIDDEN='^(agentic/|_dev/|tests/|hooks/|examples/|docs/|site/|\.github/|\.venv/|CLAUDE\.md|conftest\.py|Makefile)'; \
	if ! grep -q '^__init__\.py' dist/.wheel-listing.txt; then \
		echo "FATAL: wheel missing laila/__init__.py (sanity check)."; exit 1; \
	fi; \
	if ! grep -q '^pyproject\.toml$$' dist/.sdist-listing.txt; then \
		echo "FATAL: sdist missing pyproject.toml (sanity check)."; exit 1; \
	fi; \
	if grep -E "$$FORBIDDEN" dist/.wheel-listing.txt; then \
		echo "FATAL: forbidden paths in wheel."; exit 1; \
	fi; \
	if grep -E "$$FORBIDDEN" dist/.sdist-listing.txt; then \
		echo "FATAL: forbidden paths in sdist."; exit 1; \
	fi; \
	echo "OK: wheel and sdist are clean."
