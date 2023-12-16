import os
import io
import gzip
import requests
from tqdm import tqdm
import psutil
from pymongo import collection, operations, InsertOne, UpdateOne, DeleteOne
import logging
import aiohttp
import aiofiles
from warcio.archiveiterator import ArchiveIterator
from extruct import JsonLdExtractor

class QueueMongo():
    def __init__(self, col, chunk_size = 1000):
        self.chunk_size: int = chunk_size
        self.col: collection.Collection = col
        self.ops: list = []
        self.ops_len: int = 0
    
    def _add_op(
        self,
        op: operations.InsertOne | operations.UpdateOne | operations.DeleteOne
    ):
        try:
            self.ops_len += 1

            self.ops.append(op)

            # TODO: REMOVE
            # if True:
            if self.ops_len == self.chunk_size:
                self.col.bulk_write(self.ops)
                self.ops_len = 0
                self.ops = []

        except Exception as e:
            raise ValueError("Generic MongoQueue error.", e.details["writeErrors"])
    
    # TODO: make more dynamic BUT keep low compute cost
    def add_document(self, doc):
        document = InsertOne(doc)
        
        self._add_op(document)
    
    def update_document(self, filter, update):
        document = UpdateOne(filter, update)

        self._add_op(document)

    def delete_document(self, doc):
        document = DeleteOne(doc)

        self._add_op(document)

def is_target_mime(mime: str):
    if mime in ["text/html", "application/xhtml+xml"]:
        return True
    
    return False

def remove_nline(string: str):
    if not string:
        raise ValueError("No string provided OR string invalid.", string)

    return string.strip("\n")

# TODO add to research.md
# partial steal from https://stackoverflow.com/questions/845058/how-to-get-the-line-count-of-a-large-file-cheaply-in-python
def n_lines(file: str):
    if not file:
        raise ValueError("No file provided.")
    
    print(file)
    with open(file, "rb") as f:
        lines = sum(1 for _ in f)

    print(lines)
    return lines

# TODO add to research.md
# steal from https://stackoverflow.com/a/27518377
def rawcount(filename):
    f = open(filename, 'rb')
    lines = 0
    buf_size = 1024 * 1024
    read_f = f.raw.read

    buf = read_f(buf_size)
    while buf:
        lines += buf.count(b'\n')
        buf = read_f(buf_size)

    return lines

# TODO add to research.md
# stolen from https://stackoverflow.com/questions/38543506/change-logging-print-function-to-tqdm-write-so-logging-doesnt-interfere-wit/38739634#38739634
def unzip(
        input: str = None,
        output: str = None,
        chunk_size: int = 1024
):
    if not input or not output:
        raise ValueError("Must include input file path AND output file path.")

    with open(input, "rb") as input_file, open(output, "w", encoding="utf-8") as output_file:
        gzip_input_file = gzip.GzipFile(fileobj=input_file)

        file_size = os.path.getsize(input)

        progress = tqdm(total=file_size, unit="B", unit_scale=True, unit_divisor=1000)

        while True:
            chunk = gzip_input_file.read(chunk_size)

            if not chunk:
                break

            decoded = chunk.decode("utf-8")
            output_file.write(decoded)

            # check cursor position, reset progress to position & flush tqdm
            progress.n = input_file.tell()
            progress.update(0)

def unzip_ranges(
        input: str = None,
        output: str = None,
        ranges: list = [],
        chunk_size: int = 1024
):
    # NOTE: range must have "_id" "length" 

    if not ranges:
        raise ValueError("No ranges provided.")
    
    if not input or not output:
        raise ValueError("Must include input AND output file path.")

    with open(input, "rb") as input_file:
        gzip_input_file = gzip.GzipFile(fileobj=input_file)

        for range in ranges:
            with open(f"{output}/{range['_id']}", "w", encoding="utf-8") as output_file:
                file_size = range["length"]

                progress = tqdm(total=file_size, unit="B", unit_scale=True, unit_divisor=1000)

                while output_file.tell() - 1 < file_size:
                    if chunk_size + output_file.tell() > file_size:
                        chunk = gzip_input_file.read(file_size - output_file.tell())

                        # set progress to file_size
                        progress.n = file_size
                    else:
                        chunk = gzip_input_file.read(chunk_size)

                        # check cursor position, reset progress to position & flush tqdm
                        progress.n = input_file.tell()

                    if not chunk:
                        break

                    decoded = chunk.decode("utf-8")
                    output_file.write(decoded)

def chunk_response(
        url: str = None,
        output: str = None,
        desc: str = "",
        chunk_size: int = 1024
):
    if not url or not output:
        raise ValueError("Must include url AND output args.")
    
    # use session instead of default request. Removes pinging between chunk downloads (2 pings per chunk + transmission time).
    # ~81.38s w/o // ~59.14s w/ on generic CDX file.
    session = requests.Session()

    with session.get(url, stream=True) as r_stream:
        progress = tqdm(total=int(r_stream.headers["Content-Length"]), unit="B", unit_scale=True, unit_divisor=1000)
        progress.set_description(desc)

        r_stream.raise_for_status()

        with open(output, "wb") as output_file:
            for chunk in r_stream.iter_content(chunk_size):
                progress.update(len(chunk))
                output_file.write(chunk)

# TODO: reference https://stackoverflow.com/questions/75663384/how-to-log-python-code-memory-consumption
def ram_usage():
    virtual_ram = psutil.virtual_memory()

    total = round(virtual_ram.total / 1000 / 1000, 4)
    used = round(virtual_ram.used / 1000 / 1000, 4)
    percent = round(used / total * 100, 1)

    return (used, total, percent)
    # log.info(f"Ram usage: {used} / {total} MB ({percent}%)")