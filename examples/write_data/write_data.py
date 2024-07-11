import determined as det
import logging

from auto_spark_session import get_spark_session
from datetime import datetime
from pyspark.sql.functions import col
from urllib.parse import urljoin


def main(context, spark_session, storage_path):
    data = spark_session.range(1_000_000)

    before = datetime.now()
    squared_data = data.withColumn("squared_value", col("id") * col("id"))
    diff = datetime.now() - before
    logging.info(f"Datetime diff of withColumn: {diff}")

    before = datetime.now()
    squared_data.write.mode("overwrite").parquet(storage_path)
    diff = datetime.now() - before
    logging.info(f"Datetime diff of write: {diff}")

    before = datetime.now()
    read_data = spark_session.read.parquet(storage_path)
    read_data.show()
    diff = datetime.now() - before
    logging.info(f"Datetime diff of read: {diff}")



if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format=det.LOG_FORMAT)

    info = det.get_cluster_info()

    storage_account =  info.user_data.get("storage_account")
    container_name = info.user_data.get("container_name")
    storage_uri = info.user_data.get("storage_uri").lstrip("/")

    storage_path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/{storage_uri}"
    print(f"Using storage_path: {storage_path}")

    spark_session = get_spark_session(storage_account)

    with det.core.init() as core_context:
        main(core_context, spark_session, storage_path)
