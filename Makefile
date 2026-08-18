.PHONY: test run up
test:
	pytest
run:
	PYTHONPATH=src INSTANCE_ID=1 uvicorn shortener.app:app --port 8000
up:
	docker compose up --build
