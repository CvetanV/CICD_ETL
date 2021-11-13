install:
	pip install --upgrade pip &&\
		pip install -r requirements.txt
format:
	black *.py
lint:
	pylint --disable=R,C cicd_etl.py
#test:
#	python -m pytest -vv --cov=hello test_cicd_etl.py

all: install lint format