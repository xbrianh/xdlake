MODULES=xdlake
tests:=$(wildcard tests/test_*.py)

test: lint mypy $(tests)

# A pattern rule that runs a single test script
$(tests): %.py :
	python -m unittest $*.py

quick_tests:
	python -m unittest tests/test_xdlake.py tests/test_compatibility.py tests/test_delta_log.py

lint:
	ruff check $(MODULES)

mypy:
	mypy --ignore-missing-imports $(MODULES)

clean:
	git clean -dfx

build: clean
	python -m build

sdist: clean
	python -m build --sdist

install: build
	pip install --upgrade dist/*.whl

.PHONY: $(tests) quick_tests clean build install
