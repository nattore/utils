from dataclasses import dataclass, field
from typing import Optional, Dict, List, Literal
from enum import Enum


class State(Enum):
    SUCCESS = "success"
    FAIL = "fail"
    OK = "ok"

class Status:
    def __init__(self, initial_state: State):
        if not isinstance(initial_state, State):
            raise ValueError("Initial state must be a valid State (SUCCESS or FAIL)")
        self._state = initial_state

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, new_state: State):
        if not isinstance(new_state, State):
            raise ValueError("New state must be a valid State (SUCCESS or FAIL)")
        self._state = new_state


@dataclass
class AwsConfig:
    """
    Configuration for AWS Wrangler methods such as create_parquet_table, unload, and repair_table.

    Attributes:
        sql (str): SQL query for unloading data or other operations.
        table (str): Name of the target table in the database.
        database (str): Name of the database where the table is located.
        path (str): S3 path for data storage or table creation.
        columns_types (Dict[str, str]): Column names and their respective data types.
        partitions_types (Optional[Dict[str, str]]): Partition column names and their data types.
        partitioned_by (Optional[List[str]]): Columns to partition the table by.
        file_format (str): File format for data (e.g., 'parquet', 'csv').
        compression (Optional[str]): Compression type (e.g., 'snappy', 'gzip').
        data_source (Optional[str]): Name of the data source for catalog integration.
        description (Optional[str]): Description of the table.
        mode (Literal['overwrite', 'append']): Write mode for table operations.
        workgroup (Optional[str]): Workgroup to use for Athena queries.
    """

    sql: str
    table: str
    database: str
    path: str
    columns_types: Dict[str, str]
    partitions_types: Optional[Dict[str, str]] = None
    partitioned_by: Optional[List[str]] = None
    file_format: str = field(default="parquet")
    compression: Optional[str] = field(default="snappy")
    data_source: Optional[str] = None
    description: Optional[str] = None
    mode: Literal["overwrite", "append"] = field(default="overwrite")
    workgroup: Optional[str] = None

    def __post_init__(self):
        """
        Validates the provided configuration parameters after initialization.
        """
        valid_file_formats = {"parquet", "csv", "json"}
        valid_compressions = {"snappy", "gzip", "bzip2", None}

        if self.file_format not in valid_file_formats:
            raise ValueError(
                f"Invalid file_format '{self.file_format}'. Must be one of {valid_file_formats}."
            )

        if self.compression not in valid_compressions:
            raise ValueError(
                f"Invalid compression '{self.compression}'. Must be one of {valid_compressions}."
            )

        if self.mode not in {"overwrite", "append"}:
            raise ValueError(
                f"Invalid mode '{self.mode}'. Must be 'overwrite' or 'append'."
            )

        if self.partitioned_by and not self.partitions_types:
            raise ValueError(
                "If 'partitioned_by' is specified, 'partitions_types' must also be provided."
            )

        if not self.path.startswith("s3://"):
            raise ValueError(f"Invalid path '{self.path}'. Must start with 's3://'.")


def build_aws_config(params):
    return AwsConfig(
        sql=params["select_query"],
        table=params["table"],
        database=params["database"],
        path=params["location"],
        columns_types=params["columns_types"],
        partitions_types=params["partitions_cols"],
        partitioned_by=params["partitioned_by"],
        description=params["description"],
        file_format=params["file_format"],
        compression=params["compression"],
        data_source=params["data_source"],
        workgroup=params["workgroup"],
    )
