import pandas as pd
from multiprocessing import Process, Queue
import logging

# Set up basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def worker(queue: Queue, num_elements: int):
    """
    Generates a DataFrame with a range of numbers, filters for even values,
    and puts the filtered DataFrame into a queue.

    Args:
        queue: A multiprocessing.Queue to store the result.
        num_elements: The number of elements to generate in the DataFrame.
    """
    try:
        if not isinstance(num_elements, int) or num_elements < 0:
            raise ValueError("num_elements must be a non-negative integer.")

        logging.info(f"Worker processing {num_elements} elements.")
        df = pd.DataFrame({"value": range(num_elements)})
        df_filtered = df[df["value"] % 2 == 0]
        queue.put(df_filtered)
        logging.info(f"Worker finished and put DataFrame in queue.")
    except ValueError as ve:
        logging.error(f"ValueError in worker: {ve}")
        queue.put(None) # Indicate failure to the main process
    except Exception as e:
        logging.error(f"An unexpected error occurred in worker: {e}")
        queue.put(None) # Indicate failure to the main process

if __name__ == "__main__":
    # It's good practice to protect the main part of the script
    # when using multiprocessing.
    results_queue = Queue()
    num_processes = 4
    processes = []

    print("Starting worker processes...")
    for i in range(num_processes):
        # Ensure n is non-negative; handle cases where it might be zero.
        n_for_worker = max(0, i * 1000)
        process = Process(target=worker, args=(results_queue, n_for_worker))
        processes.append(process)
        try:
            process.start()
            logging.info(f"Started process {process.pid} with n={n_for_worker}")
        except Exception as e:
            logging.error(f"Failed to start process for n={n_for_worker}: {e}")

    # Collect results
    results = []
    for i in range(num_processes):
        try:
            # Add a timeout to q.get() to prevent indefinite blocking
            # if a worker fails to put anything in the queue.
            result = results_queue.get(timeout=10) # Timeout in seconds
            if result is not None:
                results.append(result)
            else:
                logging.warning(f"Received None from a worker, indicating an error in that worker.")
        except Empty: # from queue import Empty
            logging.error("Queue was empty, a worker might have failed before putting a result.")
        except Exception as e:
            logging.error(f"Error getting result from queue: {e}")


    print(f"\nCollected {len(results)} results:")
    for idx, res_df in enumerate(results):
        print(f"--- Result {idx+1} ---")
        print(res_df.head()) # Print head for brevity if DataFrames are large

    print("\nWaiting for worker processes to complete...")
    for process in processes:
        try:
            process.join(timeout=5) # Add a timeout to join
            if process.is_alive():
                logging.warning(f"Process {process.pid} did not terminate, forcing termination.")
                process.terminate() # Force terminate if join times out
                process.join() # Wait for termination to complete
            else:
                logging.info(f"Process {process.pid} joined successfully with exit code {process.exitcode}.")
        except Exception as e:
            logging.error(f"Error joining process {process.pid}: {e}")

    print("\nAll processes finished.")

