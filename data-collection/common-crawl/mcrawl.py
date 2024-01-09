from pymongo import collection
import logging
import requests
from utils import unzip, chunk_response, remove_nline, n_lines, is_target_mime, rawcount, QueueMongo
import json
from tqdm import tqdm

class ProcessCrawl:
    def __init__(self, log, col_crawls, col_cdx, col_warc, dir_tmp, dir_cdx, cc_data_baseurl="https://data.commoncrawl.org", chunk_size=1024):
        self.log: logging.Logger = log
        self.col_crawls: collection.Collection = col_crawls
        self.col_cdx: collection.Collection = col_cdx
        self.col_warc: collection.Collection = col_warc
        self.dir_tmp: str = dir_tmp
        self.dir_cdx: str = dir_cdx
        self.cc_data_baseurl: str = cc_data_baseurl
        self.chunk_size: int = chunk_size

        if not log:
            raise ValueError("No logger provided.")
        
        if (col_crawls is None) or (col_cdx is None):
            raise ValueError("MongoDB collections not passed, must include BOTH crawl AND cdx collections. (or does not exist).")

        if not dir_tmp:
            raise ValueError("No tmp dir provided.")

    def preload_crawl(self):
        crawl = self.col_crawls.find_one({ "status": "not_processed" })
        crawl_id = crawl["id"]
        crawl_mid = crawl["_id"]
        crawl_baseurl = crawl["url"]

        self.log.info(f"Fetching {crawl['id']}")

        try:
            cdx_list = requests.get(f"{crawl_baseurl}/cc-index.paths.gz").content
            
            with open(f"{self.dir_tmp}/tmp-cc-index.paths.gz", "wb") as gzip_file:
                gzip_file.write(cdx_list)

            unzip(f"{self.dir_tmp}/tmp-cc-index.paths.gz", f"{self.dir_tmp}/tmp-cc-index.paths")
            
            tmp_cdx = []

            with open(f"{self.dir_tmp}/tmp-cc-index.paths", "r", encoding="utf-8") as cdx_indexes:
                for line in cdx_indexes:
                    if not line.split("/")[-1].startswith("cdx"):
                        continue

                    line = remove_nline(line)

                    tmp_cdx.append({
                        "id": line.split("/")[-1].split(".")[0],
                        "crawl": crawl_id,
                        "crawl_mid": crawl_mid,
                        "url":  f"{self.cc_data_baseurl}/{line}",
                        "status": "not_processed"
                    })

                try:
                    self.col_cdx.insert_many(tmp_cdx)
                    self.col_crawls.update_one({ "_id": crawl_mid }, { "$set": { "status": "partial_processed" } })
                    self.log.info(f"Loaded path files for crawl: {crawl_id}")

                except Exception as e:
                    self.log.error("Error processing/inserting cdx indexes into mongoDB...")

        except Exception as e:
            self.log.error("Error requesting OR unzipping crawl index file.", e)
    
    def load_crawl(self):
        cdx = self.col_cdx.find_one({ "status": "not_processed" })
        cdx_id = cdx["id"]
        crawl_id = cdx["crawl"]
        cdx_mid = cdx["_id"]
        cdx_url = cdx["url"]

        self.log.info(f"Downloading {cdx_id} in {crawl_id}")

        try:
            chunk_response(cdx_url, f"{self.dir_tmp}/tmp-cdx.gz", f"CDX {cdx_mid}")

            self.log.info(f"{cdx_id} downloaded, decompressing...")
            
            unzip(f"{self.dir_tmp}/tmp-cdx.gz", f"{self.dir_tmp}/cdx/{cdx_id}", chunk_size=1024 * 2)
            
            self.log.info(f"{cdx_id} decompressed")

            self.col_cdx.update_one({ "_id": cdx_mid }, { "$set": { "status": "partial_processed" } })

            return cdx_id
        except Exception as e:
            self.log.error("Error requesting OR unzipping warc index file.", e)
    
    def filter_cdx(self, cdx):
        self.log.info(f"Found {cdx}, filtering...")

        q_mongo = QueueMongo(self.col_warc)
        m_cdx = self.col_cdx.find_one({ "id": cdx })
        m_crawl = self.col_crawls.find_one({ "id": m_cdx["crawl"] })

        if not m_cdx:
            raise LookupError(f"{cdx} not found in MongoDB.")

        with open(f"{self.dir_cdx}/{cdx}", "r", encoding="utf-8") as file:
            self.log.info(f"Reading {self.dir_cdx}/{cdx}...")
            progress = tqdm(total=rawcount(f"{self.dir_cdx}/{cdx}"))

            for line in file:
                line = line.split(" ", maxsplit=2)
                json_line: dict = json.loads(remove_nline(line[-1]))
                warc_timestamp = line[1]
                warc_url = json_line["filename"]
                res_domain = line[0].split(")")[0]

                progress.update(1)

                if not {"status", "mime-detected", "charset", "languages", "length", "offset", "digest", "url"}.issubset(json_line.keys()):
                    continue

                if not json_line["status"] == "200":
                    continue

                if not is_target_mime(json_line["mime-detected"]):
                    continue

                if "utf-8" not in json_line["charset"].lower():
                    continue

                if "eng" not in json_line["languages"]:
                    continue

                length = int(json_line["length"])
                offset = int(json_line["offset"])

                q_mongo.add_document({
                    "id": warc_url,
                    "digest": json_line["digest"],
                    "crawl": cdx,
                    "cdx_mid": m_cdx["_id"],
                    "crawl_mid": m_crawl["_id"],
                    "warc_timestamp": warc_timestamp,
                    "url": f"{self.cc_data_baseurl}/{warc_url}",
                    "res_url": json_line["url"],
                    "length": int(json_line["length"]),
                    "offset": int(json_line["offset"]),
                    "range": f"{offset}-{length + offset - 1}",
                    "res_domain": res_domain,
                    "status": "not_processed",
                    "is_recipe": False
                })