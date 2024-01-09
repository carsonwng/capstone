# TAG:IMPORTS
import os
import pymongo
from dotenv import load_dotenv
import requests
import logging
import asyncio

from mcrawl import ProcessCrawl
from mdoc import ProcessDocument

from utils import QueueMongo, unzip, unzip_ranges
from mworkers import WorkerHanlder

# TAG:DIRS
dir_tmp = "./tmp"
dir_cdx = "./tmp/cdx"
dir_warc = "./tmp/warc"
dir_o = "./filtered"

# create nessesary dirs
for dir in [dir_tmp, dir_cdx, dir_warc, dir_o]:
    if not os.path.isdir(dir):
        os.mkdir(dir)

# TAG:VARS
cc_index_baseurl = "https://index.commoncrawl.org"
cc_data_baseurl = "https://data.commoncrawl.org"
cc_crawls_url = "https://index.commoncrawl.org/collinfo.json"

# TAG:INIT
log = logging.getLogger("cc-capstone")
log_format = logging.Formatter('%(asctime)s | %(module)s:%(levelname)-8s %(message)s')

log_handler = logging.StreamHandler()
log_handler.setFormatter(log_format)

log.addHandler(log_handler)
log.setLevel(logging.INFO)

load_dotenv()

mongo_host, mongo_username, mongo_password = os.getenv("MONGO_HOST"), os.getenv("MONGO_USERNAME"), os.getenv("MONGO_PASSWORD")
mongo_connection_string = f"mongodb://{mongo_username}:{mongo_password}@{mongo_host}/?authMechanism=DEFAULT"

client = pymongo.MongoClient(mongo_connection_string)

# create cc_capstone if not exists, init db as cc_capstone db if exists
db = client["cc_capstone"]

# create respective collections is not exists, init vars as collection if does
col_crawls = db["cc_crawls"]
col_cdx = db["cdx_index"]
col_warc = db["warc_index"]

# fetch list of known cc crawls
if not col_crawls.find_one():
    tmp = requests.get(url=cc_crawls_url).json()
    tmp_crawls = map(lambda crawl: {
        "id": crawl["id"],
        "url": f"{cc_data_baseurl}/crawl-data/{crawl['id']}",
        "status": "not_processed"
    }, tmp)

    col_crawls.insert_many(list(tmp_crawls))

# crawl_processor = ProcessCrawl(log, col_crawls, col_cdx, col_warc, dir_tmp, dir_cdx, cc_data_baseurl)

# crawl_processor.preload_crawl()
# added_id = crawl_processor.load_crawl()

# crawl_processor.filter_cdx("cdx-00000")

# async def main():
    # warc_processor = ProcessDocument(log, col_warc, dir_warc, dir_o, cc_data_baseurl, n_workers=1, chunk_size=2)

#     warc_async_workers = asyncio.create_task(warc_processor.start())
#     warc_processor.process_url("https://data.commoncrawl.org/crawl-data/CC-MAIN-2023-40/segments/1695233506339.10/warc/CC-MAIN-20230922070214-20230922100214-00874.warc.gz")

#     await warc_async_workers

# if __name__ == "__main__":
#     asyncio.run(main())

# unzip(f"{dir_o}/656bde0a961b7e68744dfda7.warc.gz", f"{dir_o}/656bde0a961b7e68744dfda7.warc")

# for i in os.listdir(dir_o):
# #     # TODO: ignore trailing gzip garbage
    # unzip(f"{dir_o}/{i}", "".join(i.split(".")[:-1]))

# warc_processor.parse_warc(f"{dir_o}/test.warc.gz", f"{dir_o}/test.warc")

# class CrawlWorker():
#     def __init__(self, col_cdx, col_warc, dir_warc, dir_o, cc_data_baseurl="https://data.commoncrawl.org"):
#         self.col_cdx = col_cdx
#         self.col_warc = col_warc
#         self.dir_warc = dir_warc
#         self.dir_o = dir_o
#         self.cc_data_baseurl = cc_data_baseurl

#         self.queue = asyncio.Queue(2)

#     def start(self):
#         pass

#     def process_crawl(self):
#         # check the queue

# crawl_processor = ProcessCrawl(
#     log=log,
#     col_crawls=col_crawls,
#     col_cdx=col_cdx,
#     col_warc=col_warc,
#     dir_tmp=dir_tmp,
#     dir_cdx=dir_cdx,
#     cc_data_baseurl=cc_data_baseurl,
# )

# warc_processor = ProcessDocument(
#     log=log,
#     col_warc=col_warc,
#     dir_warc=dir_warc,
#     dir_o=dir_o,
#     cc_data_baseurl=cc_data_baseurl,
#     n_workers=1
# )

async def main():
    # warc = col_warc.find_one_and_update({ "status": "not_processed" }, { "$set": { "status": "queued" } })

    w_handler = WorkerHanlder(
        col_crawl=col_crawls,
        col_cdx=col_cdx,
        col_warc=col_warc,
        
        dir_o=dir_o,

        # n_crawl=5,
        # n_cdx=1,
        n_warc=1,
        # n_warc_instances=8,
        n_warc_manager=1, # 1/3 of (n_warc * n_warc_instances)
        n_warc_manager_qsize=5,
        # n_garb=1,

        # p_crawl=crawl_processor,
        # p_doc=warc_processor,

        log=log,
        ticker=1,
        max_q=5000, # (n_warc * n_warc_instances) * 5

        mongo_connection_string=mongo_connection_string
    )

    workers = asyncio.create_task(w_handler.start())
    # w_handler.start()

    await workers

if __name__ == "__main__":
    # loop = asyncio.get_event_loop()
    # loop = asyncio.new_event_loop()
    # asyncio.set_event_loop(loop)

    asyncio.run(main())
    # pass