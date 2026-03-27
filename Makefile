.PHONY: init test

init:
	pip install -e ".[dev]"
	nbstripout --install
	ln -sf ../../hooks/pre-commit .git/hooks/pre-commit

test:
	python -m pytest tests/ -v
