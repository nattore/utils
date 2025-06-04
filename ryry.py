import dask.dataframe as dd
from dask.distributed import Client, LocalCluster

def process_parquet_files(file_list, n_workers=1, threads_per_worker=None, memory_limit='4GB'):
    """
    Process Parquet files from S3 into a pandas DataFrame using Dask with resource constraints.
    
    Args:
        file_list (list): S3 paths to Parquet files (e.g., ['s3://bucket/file1.parquet']).
        n_workers (int): Number of worker processes (default: 1).
        threads_per_worker (int): Threads per worker (default: use all cores).
        memory_limit (str/int): Memory limit per worker (e.g., '4GB').
    
    Returns:
        pd.DataFrame: Resulting DataFrame after computation.
    """
    # Set up a local Dask cluster with constrained resources
    cluster = LocalCluster(
        n_workers=n_workers,
        threads_per_worker=threads_per_worker,
        memory_limit=memory_limit,
        processes=True  # Use processes for memory isolation
    )
    client = Client(cluster)
    
    try:
        # Read the Parquet files from S3 (lazy Dask DataFrame)
        ddf = dd.read_parquet(
            file_list,
            storage_options={'anon': True}  # Adjust for your S3 authentication
        )
        
        # Compute the result as a pandas DataFrame
        pandas_df = ddf.compute()
        return pandas_df
    
    finally:
        # Ensure resources are released even if errors occur
        client.close()
        cluster.close()

# Example Usage:
if __name__ == "__main__":
    # List of S3 Parquet files to process
    s3_files = [
        's3://your-bucket/path/to/file1.parquet',
        's3://your-bucket/path/to/file2.parquet'
    ]
    
    # Process data with 2 workers, 2 threads/worker, and 2GB memory limit per worker
    df = process_parquet_files(
        s3_files,
        n_workers=2,
        threads_per_worker=2,
        memory_limit='2GB'
    )
    print(f"Result DataFrame shape: {df.shape}")
