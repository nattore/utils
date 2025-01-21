from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict
from ..util import AwsConfig, Status, State
import awswrangler as wr
import logger


logger = logger.getLogger(__name__)


def _create_table(config):
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
            f"An exception occurred while creating table {config.table}. Exception: {exc}, Type: {type(exc)}"
        )
        raise


def _unload_partition(query, query_id, config):
    """
    This is a wrap function to awswrangler.athena.unload
    """
    logger.debug(f"Starting UNLOAD query {query_id}:\n{query}")
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
    except Exception as err:
        logger.error(f"Unexpected {err=}, {type(err)=}")
        return None
    else:
        logger.debug(f"Query {query_id} completed.\nQuery metadata:\n{query_metadata}")
        return query_metadata


def _execute_unload_partitions(query, periods, config):
    """
    Executes multiple UNLOAD queries using ThreadPoolExecutor.
    """
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
                logger.error(f"Query {query_id} generated an exception: {e}")
                results[query_id] = "FAIL"
            else:
                results[query_id] = result
                if result is not None:
                    logger.info(f"Query {query_id} succeeded. Query metadata: {result}")
                else:
                    logger.warning(f"Query {query_id} did not produced any results.")
        return results


def _repair_table(config):
    """
    This is a wrap function to awswrangler.athena.repair_table
    """
    logger.info(f"Repairing table {config.table} in database {config.database}.")
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
            f"An exception occurred while repairing table {config.table}. Exception: {exc}, Type: {type(exc)}"
        )
        raise
    else:
        if state == "SUCCEEDED":
            logger.info(f"MSCK on table {config.table} succeeded.")
        elif state == "FAIL":
            logger.error(f"MSCK on table {config.table} failed.")
        elif state == "CANCELLED":
            logger.warning(f"MSCK on table {config.table} was cancelled.")
        else:
            logger.error(
                f"MSCK on table {config.table} resulted in an unknown state: {state}."
            )
            raise ValueError(
                f"Unknown state: {state} encountered during MSCK on table {config.table}."
            )
    return state


def build_table(table_params: AwsConfig) -> Dict[str, Any]:
    """
    Builds a table given table parameters.
    """
    config = AwsConfig(table_params)
    logger.info(f"Building table {config.table} with params:\n {table_params}.")
    build_status = Status(State.SUCCESS)
    try:
        _create_table(config)
        query = config.sql
        periods = list(table_params["periods"].values())
        unload_results = _execute_unload_partitions(query, periods, config)
        _repair_table(config)
    except Exception as exc:
        logger.error(
            f"An exception occurred while building table {config.table}. Exception: {exc}, Type: {type(exc)}"
        )
        build_status.state = State.FAIL
    logger.info(f"Table {config.table} created.")
    return build_status, unload_results
