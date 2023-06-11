PIPELINE_NAME ?= travel_covid
TAG ?= dev

run-data-pipeline:    ./infra/Dockerfile

	echo "Data Pipeline Process started"
	docker build -t ${PIPELINE_NAME}:${TAG} -f ./infra/Dockerfile .
	docker run -v $(PWD)/pipeline:/app -t ${PIPELINE_NAME}:${TAG}