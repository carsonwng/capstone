import logging
import json
import aiohttp, asyncio
from utils import QueueMongo
from pymongo import collection
from bson.objectid import ObjectId
from warcio.archiveiterator import ArchiveIterator
from extruct import JsonLdExtractor
from tqdm import tqdm
from schemaorg.main.parse import RecipeParser
# from utils import async_chunk_response

class ProcessDocument:
    def __init__(self, log, col_warc, dir_warc, dir_o, cc_data_baseurl="https://data.commoncrawl.org", n_workers=1, chunk_size=1000):
        self.log: logging.Logger = log
        self.col_warc: collection.Collection = col_warc
        self.dir_warc: str = dir_warc
        self.dir_o: str = dir_o
        self.cc_data_baseurl: str = cc_data_baseurl
        
        self.n_workers: int = n_workers
        self.chunk_size: int = chunk_size
        
        self.clientsession = aiohttp.ClientSession()
        self.jsonldsession = JsonLdExtractor()
        
        self.a_queue = asyncio.Queue()
        self.m_queue = QueueMongo(col_warc, chunk_size=2)

        if not log:
            raise ValueError("No logger provided.")

        if col_warc is None:
            raise ValueError("MongoDB collection no passed or does not exist.")

        if not dir_warc:
            raise ValueError("No warc dir provided.")

    async def _worker(self, i):
        while True:
            q_item = await self.a_queue.get()

            # kill
            if q_item is None:
                break

            url, warcs = q_item[0], q_item[1]
            callback: function = q_item[2]

            self.log.info(f"Running _worker with id: {i}")
            await self._async_request(i, url, warcs)
            self.log.info(f"Completed {url} in _worker with id: {i}")

            # callback()
            self.a_queue.task_done()

    async def _async_request(self, id, url: str, warcs):
        self.log.info(f"Starting request for WARC: {url}")
        if not url:
            raise ValueError("No URL provided.")
        
        if not warcs:
            raise ValueError("No WARC provided.")
        
        warcs = list(warcs)
        # b_ranges = [warc["range"] for warc in warcs]
        ranges = [{ "filename": warc["_id"], "range": warc["range"] } for warc in warcs]

        try:
            return await self._async_process_warc(
                url,
                self.dir_o,
                f"_worker {id} | WARC {url}",
                chunk_size=1024,
                ranges=ranges
            )
        
        except Exception as e:
            self.log.error(e)
            # raise RuntimeError("Generic error with url:", url, e)

    async def _async_process_warc(
        self,
        url: str = None,
        output: str = None,
        desc: str = "",
        chunk_size: int = 1024,
        ranges: list = []
    ):
        if not url or not output:
            raise ValueError("Must include url AND output args.")
        
        if not self.clientsession:
            raise ValueError("Must include session object for asynchronous chrunking.")
        
        b_ranges = [range["range"] for range in ranges]

        async with self.clientsession.get(url, headers={"Range": f"bytes={', '.join(b_ranges)}"}, raise_for_status=True) as r_stream:
            # print({"Ranges": f"bytes={', '.join(b_ranges)}"})
            progress = tqdm(total=int(r_stream.headers["Content-Length"]), unit="B", unit_scale=True, unit_divisor=1000)
            boundary = "--".encode() + r_stream.headers["Content-Type"].split("boundary=")[1].encode("utf-8")
            # print(boundary)

            buffer = b''

            # TODO: make this shorter somehow - super ugly for simple func.
            def range_generator():
                for range in ranges:
                    yield range

            range_gen = range_generator()

            file = open(f"{output}/{next(range_gen)['filename']}.warc.gz", "wb")

            # print(file)
            # print(type(ranges))

            # TODO: make this more elegant. This is a mess.
            async for chunk in r_stream.content.iter_chunked(chunk_size):
                # if buffer:
                    # chunk = buffer + chunk

                buffer = chunk

                try:
                    while boundary in buffer:
                        range_end, buffer = buffer.split(boundary, 1)
                        # remove response headers that follow boundary line
                        
                        try:
                            # copy buffer into new var
                            new_range = buffer.split(b"\r\n\r\n", 1)[1]
                        except:
                            new_range = buffer
                            pass
                            # self.log.info(new_range)

                        filename = file.name
                        # self.log.info(filename)

                        file.write(range_end)
                        file.close()

                        self._parse_warc(filename, filename.split(".gz")[0])

                        file = open(f"{output}/{next(range_gen)['filename']}.warc.gz", "wb")
                        buffer = new_range

                    file.write(buffer)

                except StopIteration:
                    progress.update(progress.total - progress.n)
                    self.log.info("Finished processing WARC.")
                    break

                except Exception as e:
                    self.log.error(e)
                    break

                progress.update(len(chunk))

    async def process_url(self, callback, url: str = None):
        self.log.info(f"Loading WARCs with segment: {url}")

        if not url:
            raise ValueError("No WARC provided.")

        # warc = self.col_warc.find_one({ "_id": ObjectId(warc) })
        warcs = self.col_warc.find({ "url": url }).sort("offset", 1)

        if not warcs:
            raise LookupError("No WARCs found.")

        self.a_queue.put_nowait((url, warcs, callback))
    
    def _parse_warc(
        self,
        input: str = None,
        output: str = None,
    ):
        if not input or not output:
            raise ValueError("Must include input AND output file path.")

        with open(input, "rb") as input_file:
            # seek past the header

            for record in ArchiveIterator(input_file):
                html = record.content_stream().read().decode("utf-8", "replace")

                # schemaorg = filter(lambda context: context["@context"] == "https://schema.org", microdata)
                
                if not html:
                    continue

                microdata = self.jsonldsession.extract(html)

                if not microdata:
                    continue

                # with open(f"{output}", "w", encoding="utf-8") as output_file:
                    # self.log.info([microdata[schema] for schema in microdata if schema["@context"] == "https://schema.org"])
                for schema in microdata:
                    if not self._filter_schema(schema):
                        continue
                        
                    # output_file.write(str(schema))

                    self.log.info(output.split("/")[-1].split(".")[0])
                    self.m_queue.update_document({ "_id": ObjectId(output.split("/")[-1].split(".")[0]) }, { "$set": { "status": "partial_processed", "schema": schema } })

                    # output_file.write(str([schema for schema in microdata if schema["@context"] == "https://schema.org"]))

    def _filter_schema(self, schema: dict = None):
        if not schema:
            raise ValueError("No schema provided.")
        
        if schema.get("@context") not in ["https://schema.org", "http://schema.org"]:
            return False

        # TODO: reimplement this
        # if schema.get("@graph"):
        #     graph = schema["@graph"]

        #     if not [s_type for s_type in graph if "Recipe" in s_type["@type"]]:
        #         return False
       
        # elif "Recipe" not in schema["@type"]:
        #     return False        

        # self.log.info(schema)
        return True
    async def start(self):
        self.log.info("Starting ProcessDocument watchdog...")

        workers = [asyncio.create_task(self._worker(i)) for i in range(self.n_workers)]

        try:
            while True:
                await asyncio.sleep(10)
        
        except asyncio.CancelledError:
            pass

        finally:
            for _ in range(self.n_workers):
                self.a_queue.put_nowait(None)
            
            await asyncio.gather(*workers)
            await self.clientsession.close()