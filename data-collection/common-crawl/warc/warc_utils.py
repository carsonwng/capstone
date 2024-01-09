import aiohttp

async def request_warc(
        session: aiohttp.ClientSession = None,
        url: str = None,
        byte_range: str = None
):
    async with session.get(url, headers={ "Range": f"bytes={byte_range}" }, raise_for_status=True) as stream:
        return await stream.read()