.PHONY: test run up sweep compare
test:
	pytest
run:
	PYTHONPATH=src INSTANCE_ID=1 uvicorn shortener.app:app --port 8000
up:
	docker compose up --build
sweep:
	PYTHONPATH=src python -m shortener.openloop sweep-isolated --api-key ol --duration 10 --budget-ms 50 --repeats 3 --rates 400,800,1200,1600,2000,2400
compare:
	PYTHONPATH=src python -m shortener.openloop compare --api-key ol --rate 2000 --duration 10 --repeats 5
