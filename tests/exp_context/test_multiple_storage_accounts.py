import determined as det

from auto_spark_session import get_spark_session, get_storage_path
from urllib.parse import urljoin


info = det.get_cluster_info()

storage_accounts =  info.user_data.get("storage_accounts")

storage_account_names = [sa['storage_account_name'] for sa in storage_accounts]
storage_paths = [
    get_storage_path(sa['storage_account_name'], sa['container_name'], sa['uri'].lstrip("/"))
    for sa in storage_accounts
]

spark_session = get_spark_session(storage_account_names)


spark_readers = [spark_session.read.parquet(sp) for sp in storage_paths]

left_rows = spark_readers[0].toLocalIterator()
right_rows = spark_readers[1].toLocalIterator()

for ii in range(min([sr.count() for sr in spark_readers])):
    row_left = next(left_rows)
    row_right = next(right_rows)

    if not ii % 1000:
        print(f"total rows processed ({ii}), "
            f"row_left: id: {row_left['id']}, squared: {row_left['squared_value']}, "
            f"row_right: id: {row_right['id']}, squared: {row_right['squared_value']}")
