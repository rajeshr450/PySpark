
help:
	@echo "clean - remove all build, test, coverage and Python artifacts"
	@echo "clean-pyc - remove Python file artifacts"
	@echo "clean-test - remove test and coverage artifacts"
	@echo "lint - check style"
	@echo "test - run tests quickly with the default Python"
	@echo "coverage - check code coverage quickly with the default Python"
	@echo "build - package"

all: default

default: clean deps build

.venv:
	if [ ! -e ".venv/bin/activate_this.py" ] ; then virtualenv --clear .venv ; fi

clean: clean-build clean-pyc clean-test

clean-build:
	rm -fr dist/

clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

clean-test:
	rm -fr .tox/
	rm -f .coverage
	rm -fr htmlcov/

deps: .venv
	. .venv/bin/activate && pip install -U -r requirements.txt -t ./src

dev_deps: .venv
	. .venv/bin/activate && pip install -U -r dev_requirements.txt

lint:
	. .venv/bin/activate && pylint -r n src/main.py src/utils.py

test:
	. .venv/bin/activate && nosetests ./tests/* --config=.noserc

build: clean
	mkdir ./dist
	cp ./src/main.py ./dist
	cd ./src && zip -x main.py -r ../dist/mods.zip .
