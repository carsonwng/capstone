import os
import logging
import time
# import aiohttp
import asyncio
from mcrawl import ProcessCrawl
from mdoc import ProcessDocument
from utils import QueueMongo
from bson.objectid import ObjectId
from pymongo import collection
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Pool, pool, Queue, Manager
import multiprocessing
from uworkers import range_generator, parse_warc
import signal
from multiprocessing import Process, Event

from warc.manager import _start_manager_node
from warc.workers import _start_node

from utils import ram_usage

class WorkerHanlder:
    def __init__(
            self,

            col_crawl,
            col_cdx,
            col_warc,

            dir_o,

            n_crawl = 1,
            n_cdx = 1,
            n_warc = 1,
            # n_warc_instances = 4,
            n_warc_manager = 1,
            n_warc_manager_qsize = 10,
            n_garb = 1,

            p_crawl = None,
            p_doc = None,

            log = None,
            ticker = 10,
            max_q = 10,
            mongo_connection_string = None
    ):
        self.log: logging.Logger = log

        self.col_crawl: collection.Collection = col_crawl
        self.col_cdx: collection.Collection = col_cdx
        self.col_warc: collection.Collection = col_warc

        self.dir_o: str = dir_o

        # self.n_crawl: int = n_crawl
        # self.n_cdx: int = n_cdx
        self.n_warc: int = n_warc
        # self.n_warc_instances: int = n_warc_instances

        self.n_warc_manager: int = n_warc_manager
        self.n_warc_manager_qsize: int = n_warc_manager_qsize

        # self.n_garb: int = n_garb

        # self.p_crawl: ProcessCrawl = p_crawl
        # self.p_doc: ProcessDocument = p_doc

        self.warc_manager = multiprocessing.Manager()
        self.warc_manager_queue = self.warc_manager.Queue(max_q)

        self.warc_manager_lock = self.warc_manager.Lock()
        self.warc_manager_count = self.warc_manager.Value("i", 0)

        self.warc_lock = self.warc_manager.Lock()
        self.warc_count = self.warc_manager.Value("i", 0)
        self.killswitch = self.warc_manager.Event()

        self.processes: list[Process] = []

        # self.pool: pool.Pool = Pool(n_warc + 1) # TODO: DEFINE AS PASSABLE
        
        # self.ac_queue = asyncio.Queue(max_q // 2)
        # self.ad_queue = asyncio.Queue(max_q)

        # self.crawl_workers: list = []
        # self.warc_workers: list[Process] = []
        
        # self.mc_queue = QueueMongo(col_crawl)
        # self.md_queue = QueueMongo(col_warc)

        self.chunk_size: int = 1024 # TODO: DEFINE AS PASSABLE
        self.ticker: int = ticker
        self.max_q: int = max_q
        
        # self.counter: int = 0

        self.mongo_connection_string: str = mongo_connection_string

    
        if not log:
            raise ValueError("No logger provided.")

        # if not p_crawl or not p_doc:
            # raise ValueError("No Processors provided.")

    # def _increment(self, n = 1):
    #     self.counter += n

    async def _kill(self):
        self.log.info("Killing Workers...")

        self.killswitch.set()

        while True:
            for p in self.processes:
                if p.is_alive():
                    break
                else:
                    self.log.info("Workers terminated!")

                    #TODO https://stackoverflow.com/questions/34337514/updated-variable-into-multiprocessing-python

                    try:
                        if self.warc_lock.locked():
                            self.warc_lock.release()
                    except:
                        pass

                    return

    async def start(self):
        try:
            self.log.info("Starting Workers... Hold on!")

            start_t = time.monotonic()

            for i in range(self.n_warc_manager):
                p = Process(target=_start_manager_node, args=(
                    self.log.name,
                    self.mongo_connection_string,
                    self.warc_manager_queue,
                    self.dir_o,
                    i,
                    self.ticker,
                    self.n_warc_manager_qsize,
                    self.killswitch,
                    self.warc_manager_lock,
                    self.warc_manager_count
                ))

                p.start()
                self.processes.append(p)

            await asyncio.sleep(10)

            for i in range(self.n_warc):
                p = Process(target=_start_node, args=(
                    self.log.name,
                    self.mongo_connection_string,
                    self.warc_manager_queue,
                    self.dir_o,
                    i,
                    # self.n_warc_instances,
                    self.ticker,
                    self.chunk_size,
                    self.warc_lock,
                    self.killswitch,
                    self.warc_count
                ), daemon=True)
                # p = Process(target=_start_node)
                p.start()
                self.processes.append(p)

            await asyncio.sleep(self.ticker)

            while True:
                if self.killswitch.is_set():
                    self.log.info("Parent process recieved killswitch!")
                    break

                current_t = time.monotonic()
                spread_t = round(current_t - start_t, 2)

                self.log.info(f"Warc Manager added {self.warc_manager_count.value} WARCs in {current_t - start_t} seconds. (@ {round(self.warc_manager_count.value / (spread_t), 2)} WARCs/sec)")
                self.log.info(f"WARC queue is {self.warc_manager_queue.qsize() / self.max_q * 100}% full. ({self.warc_manager_queue.qsize()} / {self.max_q})")

                await asyncio.sleep(self.ticker * 10)
                
                used, total, percent = ram_usage()

                self.warc_lock.acquire()
                self.log.info(f"Processed {self.warc_count.value} WARCs in {current_t - start_t} seconds. (@ {round(self.warc_count.value / (spread_t), 2)} WARCs/sec)")
                self.warc_lock.release()

                self.log.info(f"RAM Usage: {used} MB / {total} MB ({percent}%)")

        # run self._kill() if interrupted
        except (asyncio.exceptions.CancelledError ,KeyboardInterrupt):
            self.log.info("KeyboardInterrupt detected!")
            await self._kill()
        # finally:
            # await self._kill()

# async def _test_worker(self, i = 0):
#     self.log.info(f"Worker {i} starting in pid {os.getpid()}...")
#     await self._dummy_await()

#     # async def _crawl_manager(self):
#     #     while True: # block until queue empties
#     #         if self.killswitch == True:
#     #             break

#     #         self.log.info("Manager checking queue...")
#     #         await self._dummy_await()
#             # if self.killswitch == True:
#             #     break

#             # if self.ac_queue.full():
#             #     await asyncio.sleep(self.ticker)
            
#             # else:
#             #     crawl = self.col_crawl.find_one({ "status": "not_processed" })

#             #     if not crawl:
#             #         continue

#             #     crawl_id = crawl["id"]
#             #     crawl_mid = crawl["_id"]
#             #     # crawl_baseurl = crawl["url"]
                
#             #     self.log.info(f"Adding {crawl_id} to queue...")
#             #     self.ac_queue.put_nowait(crawl_mid)

#     # async def _crawl_worker(self, i):
#     #     while True:
#     #         if self.killswitch == True:
#     #             break
            
#     #         self.log.info(f"Worker {i} working...")
#     #         await self._dummy_await()


#             # self.log.info(f"Hey I'm working here {i}")
#             # if self.ac_queue.empty():
#             #     await asyncio.sleep(self.ticker)
#             #     continue

#             # qc_item = await self.ac_queue.get()

#             # if self.killswitch or qc_item is None:
#             #     break
            
#             # qc_mid = ObjectId(qc_item)
#             # crawl = self.col_crawl.find_one({ "_id": qc_mid })

#             # self.log.info(f"Crawl worker {i}; Found {crawl['id']}!")
            
#             # res = await self._dummy_await()
#             # self.ac_queue.task_done()

#     async def _dummy_await(self):
#         await asyncio.sleep(1)
#     # create functions that properly employ a provider-consumer pattern that adds items to the self.ac_queue if there is space, simultaneously processing the crawl (similar to how ./main.py comments employ it). This should include kill functionality, as well as continuous addition until the queue is full.
    