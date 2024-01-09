import logging
import asyncio
import os
from threading import Event
from multiprocessing.queues import Queue
from pymongo.collection import Collection
from motor.core import AgnosticCollection
import motor.motor_asyncio
import threading
from multiprocessing.managers import ValueProxy

def _start_manager_node(
        log_name: str = "",
        mongo_connection_string: str = "",
        queue: Queue = None,
        dir_o: str = "",
        i: int = 0,
        ticker: int = 1,
        query_size: int = 100,
        killswitch: Event = None,
        manager_lock: threading.Lock = None,
        manager_count: ValueProxy = None
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

    log = logging.getLogger("cc-capstone")
    log.setLevel(logging.INFO)

    mongo_client = motor.motor_asyncio.AsyncIOMotorClient(mongo_connection_string)
    # mongo_client = pymongo.MongoClient(mongo_connection_string)
    db = mongo_client["cc_capstone"]
    col_warc = db["warc_index"]

    # loop: asyncio.AbstractEventLoop = mongo_client.get_io_loop()

    try:
        loop.run_until_complete(asyncio.gather(_warc_manager(
            log_name=log_name,
            col_warc=col_warc,
            queue=queue,
            dir_o=dir_o,
            i=i,
            ticker=ticker,
            query_size=query_size,
            killswitch=killswitch,
            manager_lock=manager_lock,
            manager_count=manager_count
        )))
    finally:
        loop.close()

async def _warc_manager(
        log_name: str = "",
        col_warc: AgnosticCollection = None,
        queue: Queue = None,
        dir_o: str = "",
        i: int = 0,
        ticker: int = 1,
        query_size: int = 1024,
        killswitch: Event = None,
        manager_lock: threading.Lock = None,
        manager_count: ValueProxy = None
):
    log = logging.getLogger("cc-capstone")
    log.setLevel(logging.INFO)

    log.info(f"Starting Warc Manager with pid: {os.getpid()}")

    manager_id = f"{i} (PID: {os.getpid()})"

    jobs = []
    
    async def _add_url(url: dict, re_queue: bool = False):
        warcs = col_warc.find({ "url": url["_id"] }).sort("offset", 1)

        if not warcs:
            return

        byte_ranges = [{ "filename": w["_id"], "range": w["range"] } async for w in warcs]
        
        if not re_queue:
            status_update = await col_warc.update_many({ "url": warc["_id"] }, { "$set": { "status": "queued" } })

            if not status_update:
                log.warning(f"Warc Manager {manager_id} failed to update status for url: {url['_id']}")
                return
        
        log.info(f"Warc Manager {manager_id} {'re-' if re_queue else ''}adding url with {len(byte_ranges)} WARCs to queue...")

        queue.put((url["_id"], byte_ranges), block=True, timeout=ticker * 100)
        
        manager_lock.acquire()
        manager_count.value += len(byte_ranges)
        manager_lock.release()

    if i == 0:
        dropped_queue = col_warc.aggregate([
            {
                "$match": {
                    "status": "queued"
                }
            },
            {
                "$group": {
                    "_id": "$url"
                }
            }
        ])

    while i == 0:
        if killswitch.is_set():
            log.info(f"Warc Manager {manager_id} recieved killswitch.")
            break

        if not dropped_queue.alive:
            log.info(f"Warc Manager {manager_id} finished dropped queue.")
            break

        if not dropped_queue:
            break

        log.info(f"Warc Manager {manager_id} found dropped WARCs. Re-adding to queue...")
        

        for tmp_url in await dropped_queue.to_list(query_size):
            if killswitch.is_set():
                log.info(f"Warc Manager {manager_id} recieved killswitch.")
                break
            
            url_job = asyncio.create_task(_add_url(tmp_url, re_queue=True))
            jobs.append(url_job)
            # byte_ranges = [{ "filename": warc["_id"], "range": warc["range"] }]
            # queue.put((warc["url"], byte_ranges), block=True)        

        await asyncio.gather(*jobs)

    await asyncio.sleep(ticker)

    log.info(f"Warc Manager {manager_id} starting main loop...")

    while True:
        jobs = []

        if killswitch.is_set():
            log.info(f"Warc Manager {manager_id} recieved killswitch.")
            break

        # if queue.full():
        #     # self.log.info("Manager waiting for queue to empty...")
        #     await asyncio.sleep(ticker)
        #     continue

        # warc = self.col_warc.find_one({ "status": "not_processed", "url": { "$nin": list(self.ad_queue.queue) } })

        # log.info(f"Unique urls: {queue.qsize()}")

        # warc = self.col_warc.find_one({ "status": "not_processed" })
        warcs = col_warc.aggregate([
            {
                "$match": {
                    "status": "not_processed"
                }
            },
            {
                "$group": {
                    "_id": "$url"
                }
            },
            {
                "$limit": query_size
            }
        ])

        async for warc in warcs:
            if not warc:
                continue

            url_job = asyncio.create_task(_add_url(warc, re_queue=False))
            jobs.append(url_job)
            # tmp_warcs = col_warc.find({ "url": warc["_id"] }).sort("offset", 1)

            # if not tmp_warcs:
            #     continue

            # byte_ranges = [{ "filename": w["_id"], "range": w["range"] } for w in tmp_warcs]

            # queue.put_nowait((warc["_id"], byte_ranges))
            # added_count += 1

        await asyncio.gather(*jobs)
        # log.info(f"Warc Manager {manager_id} added url with {len(byte_ranges)} WARCs to queue...")

        log.info("Warc Manager re-querying for WARCs...")