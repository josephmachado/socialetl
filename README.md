## Setup

The following commands are to be run via the terminal

1. Create a venv `python3 -m venv venv`
2. Activate venv with `. venv/bin/activate`, & install requirements `pip install -r requirements.txt`.
3. Run tests, check linting, etc with the [Makefile](./Makefile)
4. To view all available options run `python ./socialetl/main.py --help`
5. Deactive venv with `deactivate` command

## Make commands

1. **make ci**: Runs code formatting, lint checking, type checking and pytests. You can also run them individually with `make format`, `make lint`, `make type`, and `make pytests` respectively.
2. **make twitter-etl** and **make reddit-etl**: Runs the twitter and reddit etl with standard settings. Use `python ./socialetl/main.py --help` to determine the parameters and run them using commands like `python ./socialetl/main.py --etl reddit --tx no_tx --log info --reset-db`, etc.