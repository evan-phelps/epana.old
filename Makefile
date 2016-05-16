PACKAGE := cdwrxnorm
.PHONY: clean-pyc clean-build

help:
	@echo "clean-build - remove build artifacts"
	@echo "clean-pyc - remove Python file artifacts"
	@echo "test - run tests quickly with the default Python"
	@echo "release - package and upload a release"
	@echo "debian - package"

clean: clean-build clean-pyc

clean-build:
	rm -rf .tox/
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info
	rm -rf *.egg

clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +

test: clean-build clean-pyc release
	python setup.py test

release: clean
	python setup.py sdist

debian: test
	sh make_deb.sh

