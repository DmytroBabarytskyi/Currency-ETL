.PHONY: extract transform load analyze all

extract:
	python src/extract.py

transform:
	python src/transform.py

load:
	python src/load.py

analyze:
	python src/analyze.py

all: extract transform load analyze
