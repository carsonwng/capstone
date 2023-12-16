import logging
import asyncio
import os
from threading import Event
from multiprocessing.queues import Queue
from pymongo.collection import Collection
import pymongo

def _start_manager_node(
        log_name: str = "",
        mongo_connection_string: str = "",
        queue: Queue = None,
        dir_o: str = "",
        i: int = 0,
        ticker: int = 1,
        chunk_size: int = 1024,
        killswitch: Event = None
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

    mongo_client = pymongo.MongoClient(mongo_connection_string)
    db = mongo_client["cc_capstone"]
    col_warc = db["warc_index"]

    try:
        loop.run_until_complete(asyncio.gather(_warc_manager(
            log_name=log_name,
            col_warc=col_warc,
            queue=queue,
            dir_o=dir_o,
            i=i,
            ticker=ticker,
            chunk_size=chunk_size,
            killswitch=killswitch
        )))
    finally:
        loop.close()
    
async def _warc_manager(
        log_name: str = "",
        col_warc: Collection = None,
        queue: Queue = None,
        dir_o: str = "",
        i: int = 0,
        ticker: int = 1,
        chunk_size: int = 1024,
        killswitch: Event = None
):
    log = logging.getLogger("cc-capstone")
    log.setLevel(logging.INFO)

    log.info(f"Starting Warc Manager {i}... in pid {os.getpid()}")

    while True:
        if killswitch.is_set():
            log.info(f"Warc Manager {i} recieved killswitch.")
            break
        
        if queue.full():
            # self.log.info("Manager waiting for queue to empty...")
            await asyncio.sleep(ticker)
            continue
        # warc = self.col_warc.find_one({ "status": "not_processed", "url": { "$nin": list(self.ad_queue.queue) } })

        # log.info(f"Unique urls: {queue.qsize()}")

        # warc = self.col_warc.find_one({ "status": "not_processed" })
        warc = col_warc.find_one({ "status": "not_processed" })

        if not warc:
            continue

        warcs = col_warc.find({ "url": warc["url"] }).sort("offset", 1)

        if not warcs:
            continue

        status_update = col_warc.update_many({ "url": warc["url"] }, { "$set": { "status": "queued" } }).acknowledged
        
        if not status_update:
            continue

        byte_ranges = [{ "filename": warc["_id"], "range": warc["range"] } for warc in warcs]

        queue.put((warc["url"], byte_ranges), block=True)

        log.info(f"Warc Manager {i} added url with {len(byte_ranges)} WARCs to queue...")
