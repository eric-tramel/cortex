SHELL := /bin/bash
UV ?= uv
DOCS_ADDR ?= 127.0.0.1:8000

.PHONY: help docs-build docs-serve docs-clean docs-prepare docs-qc docs-rewrite-report

help:
	@echo "Available targets:"
	@echo "  make docs-build   Build static docs site into ./site"
	@echo "  make docs-serve   Run live docs server at $(DOCS_ADDR)"
	@echo "  make docs-clean   Remove generated docs site output"
	@echo "  make docs-prepare Generate local source-link pages for citations"
	@echo "  make docs-qc      Run hard-fail documentation quality gates"
	@echo "  make docs-rewrite-report  Write docs QC report without failing"

docs-prepare:
	python3 scripts/docs_qc/generate_source_pages.py --repo-root .

docs-build: docs-prepare
	$(UV) run --with mkdocs --with mkdocs-material mkdocs build --config-file mkdocs.yml --strict

docs-serve: docs-prepare
	$(UV) run --with mkdocs --with mkdocs-material mkdocs serve --config-file mkdocs.yml --dev-addr $(DOCS_ADDR)

docs-clean:
	rm -rf site
	rm -rf docs/_source

docs-qc:
	python3 scripts/docs_qc/run.py --repo-root .

docs-rewrite-report:
	python3 scripts/docs_qc/run.py --repo-root . --no-fail
