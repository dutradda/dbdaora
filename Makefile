build-virtualenv:
	@virtualenv venv --python python3.7 --prompt 'dbdaora-> '

build-docs:
	@python -m mkdocs build
	@cp ./docs/index.md ./README.md
	@cp ./docs/changelog.md ./CHANGELOG.md

deploy-docs: build-docs
	@python -m mkdocs gh-deploy

serve-docs:
	@mkdocs serve

release-pypi:
	@flit publish

deploy: deploy-docs release-pypi

integration: isort black flake8 mypy tests

isort:
	isort -y -w 100 -up -tc -rc -lai 2 -ac -w 79 -m 3 dbdaora

black:
	black -S -t py38 -l 79 dbdaora

flake8:
	flake8 --max-line-length 100 dbdaora

mypy:
	mypy dbdaora --strict

tests:
	pytest dbdaora -xvv --disable-warnings
