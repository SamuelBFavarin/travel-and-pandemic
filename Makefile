PIPELINE_NAME ?= travel_covid
TAG ?= dev

run-pipeline:    ./infra/Dockerfile

	echo "Run Full Execution"
	docker build -t ${PIPELINE_NAME}:${TAG} -f ./infra/Dockerfile .
	docker run -v $(PWD):/app -t ${PIPELINE_NAME}:${TAG}
