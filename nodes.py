from concurrent.futures import ThreadPoolExecutor, as_completed
import awswrangler as wr
import logging

logging.basicConfig(level=logging.INFO)

def _create_table(config):
    """
    This is a wrap function to awswrangler.catalog.create_parquet_table.
    """
    logging.debug(f"Creating table {config.table}.")
    try:
        wr.catalog.create_parquet_table(
            table=config.table,
            database=config.database,
            path=config.path,
            columns_types=config.columns_types,
            partitions_types=config.partitions_types,
            compression=config.compression,
            description=config.description,
            mode=config.mode
        )
    except Exception as err:
        logging.error(f"Unexpected {err=}, {type(err)=}")
        raise

def _unload_partition(query, query_id, config):
    """
    This is a wrap function to awswrangler.athena.unload
    """
    logging.debug(f"Starting UNLOAD query {query_id}:\n{query}")
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
        logging.error(f"Unexpected {err=}, {type(err)=}")
        return None
    else:
        logging.debug(f"Query {query_id} completed.\nQuery metadata:\n{query_metadata}")
        return query_metadata

def _execute_unload_partitions(query, periods, config):
    """
    Executes multiple UNLOAD queries using ThreadPoolExecutor.
    """
    with ThreadPoolExecutor(max_workers=3) as executor:
        future_to_query = {}
        for period in periods:
            formatted_query = query.format(**period)  # Format the query once
            query_id = str(uuid.uuid4())[-4:]         # Generate query_id once
            future = executor.submit(_unload_partition, formatted_query, query_id, config)
            future_to_query[future] = (formatted_query, query_id)

        for future in as_completed(future_to_query):
            formatted_query, query_id = future_to_query[future]
            try:
                result = future.result()
            except Exception as e:
                logging.error(f"Query {query_id} generated an exception: {e}")
            else:
                if result is not None:
                    logging.info(f"Query {query_id} succeeded. Query metadata: {result}")
                else:
                    logging.warning(f"Query {query_id} did not produced any results.")


def _repair_table(config):
    """
    This is a wrap function to awswrangler.athena.repair_table
    """
    logging.info(f"Repairing table {config.table} in database {config.database}.")
    try:
        state = wr.athena.repair_table(
            table=config.table,
            database=config.database,
            data_source=config.data_source,
            s3_output=config.s3_output,
            workgroup=config.workgroup
        )
    except Exception as err:
        logging.error(f"Unexpected error occurred while repairing table {config.table}. Error: {err}, Type: {type(err)}")
        raise
    else:
        if state == 'SUCCEEDED':
            logging.info(f"MSCK on table {config.table} succeeded.")
        elif state == 'FAIL':
            logging.error(f"MSCK on table {config.table} failed.")
        elif state == 'CANCELLED':
            logging.warning(f"MSCK on table {config.table} was cancelled.")
        else:
            logging.error(f"MSCK on table {config.table} resulted in an unknown state: {state}.")
            raise ValueError(f"Unknown state: {state} encountered during MSCK on table {config.table}.")


def build_table(table_params):
    """
    Builds a table given table parameters.
    """
    config = AwsConfig(table_params)
    _create_table(config)

    query = params['select_query']
    periods = list(table_params['periods'].values())
    _execute_unload_partitions(query, periods, config)
    _repair_table(config)
