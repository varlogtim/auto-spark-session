ARG BASE_IMAGE
FROM ${BASE_IMAGE}

COPY ../dist/auto_spark_session-0.0.1-py3-none-any.whl /tmp/auto_spark_session-0.0.1-py3-none-any.whl
RUN python -m pip install /tmp/auto_spark_session-0.0.1-py3-none-any.whl
