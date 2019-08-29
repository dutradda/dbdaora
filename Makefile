build-virtualenv:
	@virtualenv venv --python python3.7 --prompt 'dataclassdb-> '

build-docs:
	@python -m mkdocs build
	@cp ./docs/index.md ./README.md

