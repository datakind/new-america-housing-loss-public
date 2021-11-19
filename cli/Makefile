.PHONY: check pytest flake8 typecheck format_check format pkg_check requirements \
    test coverage

BLACK=black --skip-string-normalization --line-length 88 --target-version py38

check:
	flake8 .
	python -m isort . --only-sections --quiet --check-only --diff
	$(BLACK) --fast --check .

pytest:
	coverage run -m unittest --buffer --catch

flake8:
	python -m flake8 .

typecheck:
	python -m mypy --pretty --show-error-codes .

format_check:
	python -m isort . --only-sections --quiet --check-only --diff
	python -m $(BLACK) --fast --check .

format:
	python -m isort . --only-sections
	python -m $(BLACK) .

pkg_check:
	pip list --format=columns --outdated

reqs requirements:
	pip install -r requirements.txt

test: check pytest

coverage: test
	coverage report

