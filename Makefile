install:
	pip install --upgrade pip &&\
		pip install -r requirements.txt
lint:
	pylint --disable=R,C CICD_ETL.py

format:
	black *.py

test:
	python -m pytest -vv --cov=hello test_CICD_ETL.py

all: install lint format