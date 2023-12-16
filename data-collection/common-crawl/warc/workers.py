import os
import asyncio
import aiohttp
import logging
from pymongo.collection import Collection
import pymongo
from bson import ObjectId
from threading import Event, Lock
from multiprocessing.queues import Queue
from multiprocessing.managers import ValueProxy

from uworkers import range_generator, parse_warc

def _start_node(
        log_name: str = "",
        mongo_connection_string: str = "",
        queue: Queue = None,
        dir_o: str = "",
        i: int = 0,
        instances: int = 0,
        ticker: int = 1,
        chunk_size: int = 1024,
        lock: Lock = None,
        killswitch: Event = None,
        warc_count: ValueProxy = None
):
    if not log_name:
        raise ValueError("log_name is required.")
    
    if not mongo_connection_string:
        raise ValueError("mongo_connection_string is required.")

    if not queue:
        raise ValueError("queue is required.")

    if not dir_o:
        raise ValueError("dir_o is required.")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    log = logging.getLogger(log_name)
    log.setLevel(logging.INFO)

    mongo_client = pymongo.MongoClient(mongo_connection_string)
    db = mongo_client["cc_capstone"]
    col_warc = db["warc_index"]

    log.info(f"Starting node {i} of {instances} workers in PID {os.getpid()}...")

    try:
        loop.run_until_complete(asyncio.gather(*(_warc_worker(
            log=log,
            col_warc=col_warc,
            queue=queue,
            dir_o=dir_o,
            i=i,
            j=j,
            ticker=ticker,
            chunk_size=chunk_size,
            lock=lock,
            killswitch=killswitch,
            warc_count=warc_count
        ) for j in range(instances))))
    finally:
        loop.close()

# async def _dummy_worker(
#         i: int = 0,
#         log: logging.Logger = None,
#         killswitch: Event = None
# ):
#     pid = os.getpid()

#     while True:
#         log.info(killswitch.is_set())
#         if killswitch.is_set():
#             log.info(f"Worker {pid}-{i} received killswitch.")
#             break

#         log.info(f"PID: {os.getpid()}. I'm working... {i}")
#         await asyncio.sleep(1)

async def _warc_worker(
        log: logging.Logger = None,
        # log_name: str = "",
        # mongo_connection_string: str = "",
        col_warc: Collection = None,

        # aio_session: aiohttp.ClientSession = None,
        queue: asyncio.Queue = None,

        dir_o: str = "",
        i: int = 0,
        j: int = 0,

        ticker: int = 1,
        chunk_size: int = 1024,

        lock: Lock = None,
        killswitch: Event = None,
        warc_count: ValueProxy = None
):

    # mongo_client = pymongo.MongoClient(mongo_connection_string)
    # db = mongo_client["cc-capstone"]
    # col_warc = db["warc_index"]

    aio_session = aiohttp.ClientSession()

    log.info(f"Starting Warc Worker {j}... in node {i} with pid: {os.getpid()}")

    worker_id = f"{i}-{j} (PID: {os.getpid()})"

    try:
        while True:
            # log.info("I'm still working...")
            if killswitch.is_set():
                log.info(f"Worker {worker_id} received killswitch.")
                break

            # log.info(killswitch.is_set())
            if queue.empty():
                log.warning(f"Worker {worker_id} waiting for queue to fill...")

                await asyncio.sleep(ticker * 10)
                continue

            url, byte_ranges = queue.get()

            if not url or not byte_ranges:
                continue

            byte_ranges_only = [b_range["range"] for b_range in byte_ranges]

            retry_flag = True
            updated_count = 0

            while retry_flag:
                try:
                    async with aio_session.get(url, headers={ "Range": f"bytes={', '.join(byte_ranges_only)}" }, raise_for_status=True) as stream:
                        boundary = "--".encode() + stream.headers["Content-Type"].split("boundary=")[1].encode("utf-8")

                        range_gen = range_generator(byte_ranges)
                        file = open(f"{dir_o}/{next(range_gen)['filename']}.warc.gz", "wb")
                        
                        retry_flag = False

                        async for chunk in stream.content.iter_chunked(chunk_size):
                            buffer = chunk

                            try:
                                while boundary in buffer:
                                    range_end, buffer = buffer.split(boundary, 1)

                                    try:
                                        new_range = buffer.split(b"\r\n\r\n", 1)[1]
                                    
                                    except:
                                        new_range = buffer
                                    
                                    filename = file.name

                                    file.write(range_end)
                                    file.close()

                                    try:
                                        schema = parse_warc(filename)
                                    except:
                                        schema = None
                                        log.error(f"Error parsing {filename}")

                                    tmp_doc = {}
                                    
                                    if schema:
                                        tmp_doc["status"] = "partial_processed"
                                        tmp_doc["schema"] = schema
                                    else:
                                        tmp_doc["status"] = "processed"

                                    col_warc.update_one({ "_id": ObjectId(filename.split("/")[-1].split(".", 1)[0]) }, { "$set": tmp_doc })

                                    updated_count += 1

                                    file = open(f"{dir_o}/{next(range_gen)['filename']}.warc.gz", "wb")
                                    buffer = new_range
                                
                                file.write(buffer)
                            
                            except StopIteration:
                                break

                            except Exception as e:
                                log.error(e)
                                break
                    
                        log.info(f"Worker {worker_id} updated {updated_count} documents.")

                except (aiohttp.ClientResponseError, aiohttp.ClientConnectorError, aiohttp.ServerDisconnectedError) as e:
                    log.error(e)
                    continue
            
            # AWAIT WARC COUNT
            lock.acquire()
            warc_count.value += updated_count
            lock.release()

    except Exception as e:
        log.error(e)

    finally:
        await aio_session.close()
        log.info(f"Worker {worker_id} closed aio_session.")
