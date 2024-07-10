import determined as det

from auto_spark_session import get_spark_session
from urllib.parse import urljoin


info = det.get_cluster_info()

storage_account =  info.user_data.get("storage_account")
container_name = info.user_data.get("container_name")
storage_uri = info.user_data.get("storage_uri").lstrip("/")

storage_path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/{storage_uri}"
print(f"Using storage_path: {storage_path}")


spark_session = get_spark_session(storage_account)
sum_of_squares = 0
for ii, row in enumerate(spark_session.read.parquet(storage_path).toLocalIterator()):
    sum_of_squares += int(row["squared_value"])
    if ii % 10000 == 0:
        rid = row["id"]
        rval = row["squared_value"]
        print(f"total rows processed ({ii}), current row: id: {rid}, squared_value: {rval}, running_sum: {sum_of_squares}")

print(f"Final sum of squares: {sum_of_squares}")
