build-virtualenv:
	@virtualenv venv --python python3.7 --prompt 'dataclassesdb-> '

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
