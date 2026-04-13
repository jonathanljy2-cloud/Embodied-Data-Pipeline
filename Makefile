.PHONY: install run clean test

install:
	poetry install

run:
	poetry run python -m scripts.run_extract --config configs/extract.yaml

clean:
	rm -rf data/output

test:
	poetry run pytest