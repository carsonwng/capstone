import asyncio
import aiohttp

# credit to GPT-3.5
class ScrapeQueue:
    def __init__(self):
        self.queue = asyncio.Queue()

    async def _worker(self):
        while True:
            url = await self.queue.get()
            if url is None:  # Signal to exit the loop
                break

            await self._scrape_url(url)
            self.queue.task_done()

    async def _scrape_url(self, url):
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                print(f"Scraping {url}, Status: {response.status}")

    def add_url(self, url):
        self.queue.put_nowait(url)

    async def run(self, num_workers=5):
        workers = [asyncio.create_task(self._worker()) for _ in range(num_workers)]

        try:
            while True:
                # Add URLs to scrape continuously
                for url in ["http://example.com", "http://example.org", "http://example.net"]:
                    self.add_url(url)

                # Optionally, add a delay between iterations
                await asyncio.sleep(60)  # Adjust the delay as needed

        except asyncio.CancelledError:
            pass
        finally:
            # Stop workers by putting None in the queue
            for _ in range(num_workers):
                self.queue.put_nowait(None)

            # Wait for all workers to finish
            await asyncio.gather(*workers)

# Example usage
async def main():
    scrape_queue = ScrapeQueue()

    # Run the scraper with 3 worker tasks
    await scrape_queue.run(num_workers=3)

if __name__ == "__main__":
    asyncio.run(main())
