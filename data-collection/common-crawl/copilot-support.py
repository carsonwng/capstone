import os
import asyncio
import multiprocessing

# GIVEN: if i wanted to run multiple async worker instances within processes, and wanted to run multiple instances of said process, how would I properly execute this (on Ubuntu 20.04)? You may use asyncio.sleep(1) to emulate the awaitable functions

# To run multiple async worker instances within processes, you can use the multiprocessing module in Python. Here's a step-by-step plan:
# Define an async function that will act as your worker. This function will contain the logic that each worker will execute.
# Define a function that sets up and runs the asyncio event loop. This function will create multiple instances of your worker function and run them in the event loop.
# Use the multiprocessing module to create multiple processes. Each process will run the function from step 2.
# Here's a simplified example:

# Step 1: Define your worker function
async def worker(name):
    while True:
        print(f"Worker {name} is working... {os.getpid()}")
        await asyncio.sleep(1)  # Emulate an awaitable function

# Step 2: Define a function that sets up and runs the asyncio event loop
def run_workers(worker_count):
    print("Starting workers...")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        loop.run_until_complete(asyncio.gather(*(worker(i) for i in range(worker_count))))
    finally:
        loop.close()

# Step 3: Use multiprocessing to create multiple processes
if __name__ == "__main__":
    process_count = 4  # Number of processes
    worker_count = 5  # Number of workers per process

    with multiprocessing.Pool(process_count) as p:
        p.map(run_workers, [worker_count]*process_count)

# This will create 4 processes, each running 5 async workers. Adjust the process_count and worker_count variables as needed.

# To run this script in the background on Ubuntu, you can use the nohup command:
