run: build
	docker-compose up streamad

build:
	docker-compose build

local:
	python3 main.py worker -l info

produce:
	python3 test.py produce --path $(path)

infra:
	docker-compose up zookeeper broker control-center

clean:
	docker-compose down