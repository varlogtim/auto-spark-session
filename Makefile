.PHONY: build
build:
	python -m build


# TODO: detect the version automatically
.PHONY: install
install:
	pip install --force-reinstall dist/auto_spark_session-0.0.1-py3-none-any.whl


.PHONY: test
test: build
	cp -vf dist/auto_spark_session-0.0.1-py3-none-any.whl tests/exp_context/
	det e create tests/exp_context/test_read_retaildata0_purchases.yaml tests/exp_context/ -f
