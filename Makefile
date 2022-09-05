clean:
	find . -name __pycache__ | xargs rm -rf
	rm -fr .pytest_cache dump.rdb redis-localhost-*.log runinfo

new: clean
	python3 workflow.py init

run:
	python workflow.py

test:
	python -m pytest tests
