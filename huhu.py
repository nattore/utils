from dask.distributed import Client, LocalCluster

if __name__ == '__main__':
    # Configure the LocalCluster
    # n_workers specifies the number of worker processes Dask should use.
    # It's common to set this to the number of available CPU cores.
    # memory_limit specifies the total memory Dask workers can use.
    # This string ('128GB') will be divided equally among the workers.
    cluster = LocalCluster(
        n_workers=32,          # Number of cores
        memory_limit='128GB',  # Total RAM for Dask workers
        # processes=True by default, meaning Dask uses separate Python processes for workers.
        # This is generally good for CPU-bound tasks due to the GIL.
        # For I/O-bound tasks, you might experiment with processes=False
        # and adjust threads_per_worker.
    )

    # Connect a client to the local cluster
    client = Client(cluster)

    print(f"Dask client connected: {client}")
    print(f"Dashboard link: {client.dashboard_link}")

    # --- Your Dask computations would go here ---
    # For example:
    # import dask.array as da
    # x = da.random.random((10000, 10000), chunks=(1000, 1000)).persist()
    # y = (x + x.T).sum().compute()
    # print(f"Computation result: {y}")
    # --------------------------------------------

    # It's good practice to close the client and cluster when done
    print("Shutting down Dask client and cluster...")
    client.close()
    cluster.close()
    print("Dask setup has been shut down.")

