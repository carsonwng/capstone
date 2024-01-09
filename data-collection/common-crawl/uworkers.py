import gzip
# from warcio.archiveiterator import ArchiveIterator
from fastwarc.warc import ArchiveIterator
from extruct import JsonLdExtractor

def range_generator(ranges):
    for range in ranges:
        yield range

def _filter_schema(microdata: list[dict] = None):
    if not microdata:
        return False
    
    for schema in microdata:
        if schema.get("@context") in ["https://schema.org", "http://schema.org"]:
            return True
    
    return False
    # if schema.get("@context") not in ["https://schema.org", "http://schema.org"]:
        # return False
    
    # return True

def parse_warc(
        input: str = None,
):
    if not input:
        raise ValueError("Must include input AND output file path.")

    with open(input, "rb") as input_file:
        extractor = JsonLdExtractor()

        schemas = []
        count = 0
        for record in ArchiveIterator(input_file):
            html = record.reader.read()

            if not html:
                continue
            
            try:
                microdata = extractor.extract(html)

                if not microdata:
                    continue

                if _filter_schema(microdata):
                    schemas.append(microdata)
                else:
                    schemas.append([])
            except Exception as e:
                
                continue
        return count
        # return schemas