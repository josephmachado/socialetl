## Setup

1. Create a venv `python3 -m venv venv`
2. Activate venv with `. venv/bin/activate`
3. Run tests with `python -m pytest --log-cli-level info -p no:warnings -v ./tests`
4. Call ETL with `python ./socialetl/main.py`, use `python ./socialetl/main.py --etl twitter --log info` to specify twitter with info level logging
5. Check types with `python -m mypy --ignore-missing-imports --no-namespace-packages ./`