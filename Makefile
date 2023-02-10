pytest:
	python -m pytest --log-cli-level info -p no:warnings -v ./tests

format:
	python -m black -S --line-length 79 --preview ./
	isort ./

type:
	python -m mypy --no-implicit-reexport --ignore-missing-imports --no-namespace-packages ./

lint:
	flake8 ./socialetl
	flake8 ./tests

ci: format type lint pytest

reddit-etl:
	python ./socialetl/main.py --etl reddit --tx sd --log info

twitter-etl:
	python ./socialetl/main.py --etl twitter --log info

db:
	sqlite3 ./data/socialetl.db

reset-db:
	python ./socialetl/schema_manager.py --reset-db