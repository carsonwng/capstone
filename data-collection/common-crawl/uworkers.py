import gzip
from warcio.archiveiterator import ArchiveIterator
from extruct import JsonLdExtractor

def range_generator(ranges):
    for range in ranges:
        yield range

def _filter_schema(schema: dict = None):
    if not schema:
        return False
    
    if schema.get("@context") not in ["https://schema.org", "http://schema.org"]:
        return False
    
    return True

def parse_warc(
        input: str = None,
):
    if not input:
        raise ValueError("Must include input AND output file path.")

    with open(input, "rb") as input_file:
        extractor = JsonLdExtractor()
        for record in ArchiveIterator(input_file):
            html = record.content_stream().read().decode("utf-8", "replace")

            if not html:
                continue

            microdata = extractor.extract(html)

            if not microdata:
                continue

            for schema in microdata:
                if not _filter_schema(schema):
                    continue

                return schema

        return None