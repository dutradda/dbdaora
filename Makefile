build-virtualenv:
	@virtualenv venv --python python3.7 --prompt 'dataclassdb-> '

build-docs:
	@python -m mkdocs build
	@cp ./docs/index.md ./README.md
	@cp ./docs/changelog.md ./CHANGELOG.md

deploy-docs: build-docs
	@python -m mkdocs gh-deploy

release-pypi:
	@flit publish
