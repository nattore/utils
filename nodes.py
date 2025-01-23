from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Tuple, Dict, Optional, List
from ..utils import AwsConfig, Status, State
import awswrangler as wr
import logging

logger = logging.getLogger(__name__)


# Must re-raise any exception
# Not implemented yet
def _check_partitions(config: AwsConfig) -> Status:
    """Check if the partitions required to build the table are available."""
    status = Status(State.OK)
    return status


# Must re-raise any exception
def _delete_table(config: AwsConfig) -> None:
    """Delete the table metadata in Glue and its objects in S3."""
    table = config.table
    database = config.database

    logger.warning(f"Deleting objects within {config.path} (if any).")
    try:
        wr.s3.delete_objects(config.path)
    except Exception:
        logger.exception(
            f"An exception occurred while deleting objects within {config.path}.\n"
        )
        raise
    logger.debug(f"Objects within {config.path} deleted (if any).")

    logger.warning(f"Deleting the table {config.table} metadata (if any).")
    try:
        deleted_table = wr.catalog.delete_table_if_exists(
            table=table, database=database
        )
    except Exception:
        logger.exception(
            f"An exception occurred while deleting the table {config.table} metadata.\n"
        )
        raise
    else:
        if deleted_table:
            logger.debug(f"Table {config.table} did exist and was deleted.")
        else:
            logger.debug(f"Table {config.table} does not exist.")


# Must re-raise any exception
def _create_table(config: AwsConfig) -> None:
    """Wraps the function awswrangler.catalog.create_parquet_table."""
    logger.debug(f"Creating table {config.table}.")
    try:
        wr.catalog.create_parquet_table(
            table=config.table,
            database=config.database,
            path=config.path,
            columns_types=config.columns_types,
            partitions_types=config.partitions_types,
            compression=config.compression,
            description=config.description,
            mode=config.mode,
        )
    except Exception:
        logger.exception(
            f"An exception occurred while creating table {config.table}.\n"
        )
        raise


# Must re-raise any exception
def _infer_create_table(
    config: AwsConfig,
) -> Tuple[Dict[str, str], Optional[Dict[str, str]], Optional[Dict[str, List[str]]]]:
    """Wraps the function awswrangler.s3.store_parquet_metadata."""
    logger.debug(
        f"Inferring and storing table {config.table} metadata\n"
        f"from s3 prefix {config.path}."
    )
    try:
        columns_types, partitions_types, partitions_values = (
            wr.s3.store_parquet_metadata(
                path=config.path,
                database=config.database,
                table=config.table,
                dataset=True,
                description=config.description,
                columns_comments=config.columns_comments,
                compression=config.compression,
                mode=config.mode,
                regular_partitions=True,
            )
        )
    except Exception:
        logger.exception(
            "An exception occurred while inferring and storing\n"
            f"the table {config.table} metadata from prefix {config.path}."
        )
    else:
        return columns_types, partitions_types, partitions_values


# Must re-raise any exception
def _unload_partition(query: str, query_id: str, config: AwsConfig) -> Any:
    """Wraps the function awswrangler.athena.unload."""
    logger.debug(f"Starting unload query {query_id}.")
    try:
        query_metadata = wr.athena.unload(
            sql=query,
            path=config.path,
            database=config.database,
            file_format=config.file_format,
            compression=config.compression,
            partitioned_by=config.partitioned_by,
            data_source=config.data_source,
            workgroup=config.workgroup,
        )
    except Exception:
        logger.exception(f"An exception ocurred while unloading {query_id}.\n")
        raise
    else:
        logger.debug(f"Query {query_id} completed.\nQuery metadata:\n{query_metadata}.")
        return query_metadata


# Must handle any exception
def _execute_unload_partitions(
    query: str, periods: Dict, config: AwsConfig
) -> Dict[str, Any]:
    """Execute multiple UNLOAD queries concurrently using a ThreadPoolExecutor."""
    logger.debug(
        f"Starting ThreadPoolExecutor\nUnload query:\n{query}.\nPeriods: {periods}."
    )
    with ThreadPoolExecutor(max_workers=3) as executor:
        future_to_query = {}
        for period in periods:
            formatted_query = query.format(**period)  # Format the query once
            partition_str = "/".join([f"{k}={v}" for k, v in period.items()])
            query_id = f"{config.table}/{partition_str}"  # Generate query_id once
            future = executor.submit(
                _unload_partition, formatted_query, query_id, config
            )
            future_to_query[future] = (formatted_query, query_id)

        results = {}
        for future in as_completed(future_to_query):
            formatted_query, query_id = future_to_query[future]
            try:
                result = future.result()
            except Exception:
                results[query_id] = None
                logger.exception(f"Query {query_id} generated an exception.")
            else:
                results[query_id] = result
                logger.debug(f"Query {query_id} succeeded.\nQuery metadata:\n{result}.")
        return results


# Must re-raise any exception
def _repair_table(config: AwsConfig) -> Status:
    """Wraps the function awswrangler.athena.repair_table."""
    status = Status(State.FAIL)
    logger.debug(f"Repairing the table {config.table} in database {config.database}.")
    try:
        state = wr.athena.repair_table(
            table=config.table,
            database=config.database,
            data_source=config.data_source,
            s3_output=config.s3_output,
            workgroup=config.workgroup,
        )
    except Exception:
        logger.exception(
            f"An exception occurred while repairing the table {config.table}.\n"
        )
        raise
    else:
        if state == "SUCCEEDED":
            logger.debug(f"Repair (MSCK) on table {config.table} was successful.")
            status.state = State.SUCCEED
        elif state == "FAIL":
            logger.error(f"Repair (MSCK) on table {config.table} has failed.")
        elif state == "CANCELLED":
            logger.warning(f"Repair (MSCK) on table {config.table} was cancelled.")
        else:
            logger.error(
                f"Repair (MSCK) on table {config.table} resulted in an unknown state: {state}."
            )
            raise ValueError(
                f"Encountered unknown state: {state}\n"
                f"during repair (MSCK) on table {config.table}."
            )
    return state


# With inferring the table metadata, we have:
#   1. Delete the table (if it exists) and its data (if any)
#   2. Unload the data to given location (where the table will point to)
#   3. Create the table (by inferring its schema or manually)
# With manually creating the table metadata, we have:
#   1. Delete the table (if is exists) and its data (if any)
#   2. Create the table metadata
#   3. Unload the data to given location (where the table will point to)
#   4. Repair the table metadata
def build_table(table_params: Dict[str:Any]) -> Tuple[Dict[str, Status], Any]:
    """Builds a table based on the provided table parameters."""
    config = AwsConfig(table_params)
    build_table = Status(State.SUCCESS)

    logger.info(f"Checking partitions for building the table {config.table}.")
    partitions_state = _check_partitions(config).state
    if partitions_state is not State.OK:
        logger.error(
            f"Partitions for building the table {config.table} are missing. Aborting."
        )
        build_table.state = State.FAIL
        return {config.table: build_table}, None
    logger.info(f"Partitions to build the table {config.table} are ok.")

    # TODO: Format the table_params for better visualization.
    # Consider using the config class and implement the __str__ method.
    logger.info(f"Building the table {config.table} with params:\n{table_params}.")
    try:
        _delete_table(config)
        _create_table(config)
        query = config.sql
        periods = list(table_params["periods"].values())
        unload_results = _execute_unload_partitions(query, periods, config)
        repair_table = _repair_table(config)
    except Exception:
        logger.exception(
            f"An exception occurred while building the table {config.table}.\n"
            "Partial results have not been deleted."
        )
        build_table.state = State.FAIL
    else:
        if repair_table.state is not State.SUCCESS:
            logger.error(f"Table {config.table} created, but MSCK failed.")
            build_table.state = State.FAIL
        logger.info(f"Table {config.table} created.")

    return {config.table: build_table}, unload_results
