CONTAINER_TAG := spark3.5-rapids1.12-azure3.4-auto-spark
VERSION := $(shell python setup.py --version 2>/dev/null)
FULLNAME := $(shell python setup.py --fullname 2>/dev/null)
DOCKER_REPO ?= varlogtim
DOCKER_IMAGE ?= pyspark-training
BASE_IMAGE ?= nolanc/pyspark-nb:spark3.5-rapids1.12-azure3.4

.PHONY: build
build:
	python -m build


.PHONY: install
install:
	pip install --force-reinstall dist/$(FULLNAME)-py3-none-any.whl


.PHONY: test
test: build
	cp -vf dist/auto_spark_session-0.0.1-py3-none-any.whl tests/exp_context/
	det e create tests/exp_context/test_read_retaildata0_purchases.yaml tests/exp_context/ -f

.PHONY: test-multiple
test-multiple: build
	cp -vf dist/auto_spark_session-0.0.1-py3-none-any.whl tests/exp_context/
	det e create tests/exp_context/test_multiple_storage_accounts.yaml tests/exp_context/ -f

.PHONY: docker
docker: build
	docker buildx build -f docker/Dockerfile \
		--platform linux/amd64 \
        --build-arg BASE_IMAGE=$(BASE_IMAGE) \
		-t $(DOCKER_REPO)/$(DOCKER_IMAGE):$(CONTAINER_TAG)-auto-spark$(VERSION) \
		.

.PHONY: docker-push
docker-push: docker
	docker push $(DOCKER_REPO)/$(DOCKER_IMAGE):$(CONTAINER_TAG)-auto-spark$(VERSION)

