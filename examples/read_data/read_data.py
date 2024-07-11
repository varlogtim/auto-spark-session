import determined as det
import logging

from auto_spark_session import get_spark_session
from datetime import datetime
from pyspark.sql.functions import col
from urllib.parse import urljoin


def main(context, spark_session, storage_path):
    sum_of_squares = 0
    data = spark_session.read.parquet(storage_path)
    for ii, row in enumerate(data.toLocalIterator()):
        sum_of_squares += int(row["squared_value"])
        if ii % 10000 == 0:
            rid = row["id"]
            rval = row["squared_value"]
            core_context.train.report_training_metrics(
                steps_completed=ii, metrics={"squared_value": rval / 10_000}
            )
            logging.info(f"total rows processed ({ii}), current row: id: {rid}, squared_value: {rval}, running_sum: {sum_of_squares}")

    logging.info(f"Final sum of squares: {sum_of_squares}")
    core_context.train.report_validation_metrics(steps_completed=data.count(), metrics={"average_of_squares": sum_of_squares / data.count()})


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
