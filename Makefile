run: build
	docker-compose up streamad

build:
	docker-compose build

local:
	python3 main.py worker -l info

produce:
	python3 test.py produce --path $(path)

graph:
	python3 test.py worker -l info

infra:
	docker-compose up zookeeper broker control-center redis

clean:
	docker-compose down