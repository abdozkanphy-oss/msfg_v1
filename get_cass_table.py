import os
import json
import csv
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import DCAwareRoundRobinPolicy, RetryPolicy, TokenAwarePolicy
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from utils.config_reader import ConfigReader
from tqdm import tqdm
from datetime import datetime, timedelta


# ---------------- Cassandra connection ----------------
cfg = ConfigReader()
cassandra_config = cfg["cassandra"]

CASSANDRA_HOST = cassandra_config["host"]
USERNAME = cassandra_config["username"]
PASSWORD = cassandra_config["password"]
KEYSPACE = cassandra_config["keyspace"]

auth_provider = PlainTextAuthProvider(USERNAME, PASSWORD)

profile = ExecutionProfile(
    load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy(local_dc=None)),
    request_timeout=60.0,
    consistency_level=ConsistencyLevel.LOCAL_ONE,
    retry_policy=RetryPolicy(),
)

cluster = Cluster(
    [CASSANDRA_HOST],
    auth_provider=auth_provider,
    execution_profiles={EXEC_PROFILE_DEFAULT: profile},
)
session = cluster.connect(KEYSPACE)


# ---------------- Query ----------------
# ⚠️ This works ONLY if work_station_id is in PRIMARY KEY
def add_one_day(date_str):
    return (datetime.strptime(date_str, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")

work_center_id = 957
work_station_id = 5055
first_date = "2026-01-28"
second_date = add_one_day(first_date)

query = f"""
SELECT
  *
FROM
  dw_tbl_raw_data_by_ws
ALLOW FILTERING
"""
149

query_special = f"""
SELECT
  *
FROM
  dw_tbl_raw_data
WHERE
  "customer" = 'teknia_group'
  AND "plant_id" = 149
  AND "work_center_id" = 951
  AND "work_station_id" = 441165
ALLOW FILTERING
"""

query_special2 = f"""
SELECT
    *
FROM
    dw_tbl_raw_data
WHERE
  "customer" = 'teknia_group'
  AND "plant_id" = 149
  AND "work_center_id" = 957
  AND "work_station_id" = 5056
  AND "create_date" > '2026-01-20 10:00:00.000+0000'
  AND "create_date" < '2026-02-03 10:00:00.000+0000'
ALLOW FILTERING
"""

query_special3= f"""
SELECT
    *
FROM
    dw_tbl_raw_data_by_ws
WHERE
  "customer" = 'teknia_group'
  AND "plant_id" = 152
  AND "work_center_id" = 957
  AND "work_station_id" = 5056
  AND "create_date" > '2026-01-20 10:00:00.000+0000'
  AND "create_date" < '2026-02-03 10:00:00.000+0000'

LIMIT 10
ALLOW FILTERING
"""

print(query_special3)
statement = SimpleStatement(query_special3)
rows = session.execute(statement)


# ---------------- CSV export ----------------
output_dir = "tables"
os.makedirs(output_dir, exist_ok=True) #dw_tbl_raw_data_wc957_ws5055_2026-01-20.csv
output_file = os.path.join(output_dir, f"dw_tbl_raw_data_by_ws_samples.csv")
# output_file = os.path.join(output_dir, f"dw_tbl_raw_data_wc{work_center_id}_ws{work_station_id}_{first_date}.csv")

csv_file = open(output_file, mode="w", newline="", encoding="utf-8")
writer = None


for row in tqdm(rows):
    row_dict = row._asdict()

    # Convert Cassandra complex types to string
    for k, v in row_dict.items():
        if isinstance(v, (dict, list, tuple, set)):
            row_dict[k] = json.dumps(v, ensure_ascii=False, default=str)

    if writer is None:
        writer = csv.DictWriter(csv_file, fieldnames=row_dict.keys())
        writer.writeheader()

    writer.writerow(row_dict)

csv_file.close()

print(f"✅ Exported to CSV: {output_file}")

session.shutdown()
cluster.shutdown()
