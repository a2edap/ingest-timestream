cookies:
	@ python templates/generate.py ingest

format:
	ruff . --fix --ignore E501 --per-file-ignores="__init__.py:F401" --exclude templates/
	isort .
	black .
