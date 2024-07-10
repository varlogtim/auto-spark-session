FROM nolanc/pyspark-nb:spark3.5-rapids1.12-azure3.4
COPY ../dist/auto_spark_session-0.0.1-py3-none-any.whl /tmp/auto_spark_session-0.0.1-py3-none-any.whl
RUN python -m pip install /tmp/auto_spark_session-0.0.1-py3-none-any.whl
