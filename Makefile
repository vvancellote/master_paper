clean:
	find . -name __pycache__ | xargs rm -rf
	rm -fr .pytest_cache

test:
	python -m pytest tests