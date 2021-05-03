run: build
	docker-compose up streamad

build:
	docker-compose build

local:
	python3 -m faust -A main worker -l info

kafka:
	docker-compose up zookeeper broker control-center

clean:
	docker-compose down