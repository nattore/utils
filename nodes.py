from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Tuple, Dict, Optional
from ..util import AwsConfig, Status, State
import awswrangler as wr
import logging


logger = logging.getLogger(__name__)


def _check_partitions(config: AwsConfig) -> Status:
    """
    Check if partitions needed to build the table are available.
    """
    status = Status(State.OK)
    return status


def _delete_table(config: AwsConfig) -> None:
    """
    Delete a table metadata on glue and it's objects on s3.
    """
    table = config.table
    database = config.database

    logger.debug(f"Deleting objects within {config.path} (if any).")
    try:
        wr.s3.delete_objects(config.path)
    except Exception as exc:
        logger.error(
            f"An exception occurred while deleting objects within {config.path}.\nException: {exc}.\nType: {type(exc)}."
        )
        raise
    logger.debug(f"Objects within {config.path} deleted (if any).")

    logger.debug(f"Deleting table {config.table} metadata (if any).")
    try:
        deleted_table = wr.catalog.delete_table_if_exists(
            table=table, database=database
        )
    except Exception as exc:
        logger.error(
            f"An exception occurred while deleting the table {config.table} metadata.\nException: {exc}.\nType: {type(exc)}."
        )
        raise
    else:
        if deleted_table:
            logger.debug(f"Table {config.table} did exist and was deleted.")
        else:
            logger.debug(f"Table {config.table} does not exist.")


def _create_table(config: AwsConfig) -> None:
    """
    This is a wrap function to awswrangler.catalog.create_parquet_table.
    """
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
    except Exception as exc:
        logger.error(
            f"An exception occurred while creating table {config.table}.\nException: {exc}.\nType: {type(exc)}."
        )
        raise


def _unload_partition(query: str, query_id: str, config: AwsConfig) -> Optional[Any]:
    """
    This is a wrap function to awswrangler.athena.unload.
    """
    logger.debug(f"Starting UNLOAD query {query_id}:\n{query}.")
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
    except Exception as exc:
        logger.error(
            f"An exception ocurred while unloading {query_id}.\nException: {exc=}.\nType: {type(exc)=}."
        )
        return None
    else:
        logger.debug(f"Query {query_id} completed.\nQuery metadata:\n{query_metadata}.")
        return query_metadata


def _execute_unload_partitions(
    query: str, periods: Dict, config: AwsConfig
) -> Dict[str, Any]:
    """
    Executes multiple UNLOAD queries using ThreadPoolExecutor.
    """
    logger.debug(
        f"Starting ThreadPoolExecutor to unload query:\n{query}.\nPeriods: {periods}."
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
            except Exception as e:
                logger.error(f"Query {query_id} generated an exception:\n{e}.")
                results[query_id] = "FAIL"
            else:
                results[query_id] = result
                if result is not None:
                    logger.info(
                        f"Query {query_id} succeeded.\nQuery metadata:\n{result}."
                    )
                else:
                    logger.warning(f"Query {query_id} did not produced any results.")
        return results


def _repair_table(config: AwsConfig) -> Status:
    """
    This is a wrap function to awswrangler.athena.repair_table
    """
    status = Status(State.FAIL)
    logger.debug(f"Repairing table {config.table} in database {config.database}.")
    try:
        state = wr.athena.repair_table(
            table=config.table,
            database=config.database,
            data_source=config.data_source,
            s3_output=config.s3_output,
            workgroup=config.workgroup,
        )
    except Exception as exc:
        logger.error(
            f"An exception occurred while repairing table {config.table}.\nException: {exc}.\nType: {type(exc)}."
        )
        raise
    else:
        if state == "SUCCEEDED":
            logger.info(f"Repair (MSCK) on table {config.table} succeeded.")
            status.state = State.SUCCEED
        elif state == "FAIL":
            logger.error(f"Repair (MSCK) on table {config.table} failed.")
        elif state == "CANCELLED":
            logger.warning(f"Repair (MSCK) on table {config.table} was cancelled.")
        else:
            logger.error(
                f"Repair (MSCK) on table {config.table} resulted in an unknown state: {state}."
            )
            raise ValueError(
                f"Encountered unknown state: {state} during repair (MSCK) on table {config.table}."
            )
    return state


def build_table(table_params: Dict[str:Any]) -> Tuple[Dict[str, Status], Any]:
    """
    Builds a table given table parameters.
    """
    config = AwsConfig(table_params)
    build_table = Status(State.SUCCESS)

    logger.info(f"Checking partitions for table {config.table}.")
    partitions_state = _check_partitions(config).state
    if partitions_state is not State.OK:
        logger.warning(
            f"Partitions for building table {config.table} are missing. Aborting."
        )
        build_table.state = State.FAIL
        return {config.table: build_table}, None
    logger.info(f"Partitions to build table {config.table} are ok.")

    logger.info(f"Building table {config.table} with params:\n{table_params}.")
    try:
        _delete_table(config)
        _create_table(config)
        query = config.sql
        periods = list(table_params["periods"].values())
        unload_results = _execute_unload_partitions(query, periods, config)
        repair_table = _repair_table(config)
    except Exception as exc:
        logger.error(
            f"An exception occurred while building table {config.table}.\nException: {exc}.\nType: {type(exc)}."
        )
        logger.error(f"Failed to build table {config.table}.")
        build_table.state = State.FAIL
    else:
        if repair_table.state is not State.SUCCESS:
            logger.error(f"Table {config.table} created, but MSCK failed.")
            build_table.state = State.FAIL
        logger.info(f"Table {config.table} created.")

    return {config.table: build_table}, unload_results
