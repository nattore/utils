import awswrangler as wr
import pandas as pd

def get_partitions_from_csv(csv_path: str) -> list[dict]:
    df = pd.read_csv(csv_path)
    results = []

    for _, row in df.iterrows():
        database = row["database"]
        table = row["table"]

        partitions_df = wr.catalog.get_partitions(database=database, table=table)
        table_desc = wr.catalog.describe_table(database=database, table=table)
        partition_cols = table_desc["PartitionKeys"]
        partition_names = tuple(col["Name"] for col in partition_cols)

        # Drop duplicates and extract values as tuples
        partition_list = list(
            map(tuple, partitions_df[list(partition_names)].drop_duplicates().values)
        )

        results.append({
            "database": database,
            "table": table,
            "partition_name": partition_names,
            "partition_list": partition_list
        })

    return results
