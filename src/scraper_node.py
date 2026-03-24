import asyncio
import aiohttp
import random
from typing import Dict, List, Optional
from dataclasses import dataclass

@dataclass
class ScrapeRequest:
    url: str
    headers: Optional[Dict[str, str]] = None
    proxy: Optional[str] = None

@dataclass
class ScrapeResponse:
    url: str
    status: int
    content: str
    error: Optional[str] = None

class ScraperNode:
    def __init__(self, node_id: str, max_concurrent: int = 10,
                 rate_limit_per_second: float = 2.0):
        self.node_id = node_id
        self.session: Optional[aiohttp.ClientSession] = None
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.rate_limit = rate_limit_per_second
        self.last_request_time = 0.0
        self.proxy_list: List[str] = []

    async def init_session(self):
        if not self.session:
            self.session = aiohttp.ClientSession()

    async def close(self):
        if self.session:
            await self.session.close()
            self.session = None

    def add_proxies(self, proxies: List[str]):
        self.proxy_list.extend(proxies)

    async def _rate_limit_delay(self):
        now = asyncio.get_event_loop().time()
        time_since_last = now - self.last_request_time
        delay = max(0, (1 / self.rate_limit) - time_since_last)
        if delay > 0:
            await asyncio.sleep(delay)
        self.last_request_time = now

    async def scrape(self, request: ScrapeRequest) -> ScrapeResponse:
        await self.init_session()
        
        async with self.semaphore:
            await self._rate_limit_delay()
            
            try:
                proxy = request.proxy or (random.choice(self.proxy_list) if self.proxy_list else None)
                headers = request.headers or {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                }

                async with self.session.get(
                    request.url,
                    headers=headers,
                    proxy=proxy,
                    timeout=30
                ) as response:
                    content = await response.text()
                    return ScrapeResponse(
                        url=request.url,
                        status=response.status,
                        content=content
                    )

            except Exception as e:
                return ScrapeResponse(
                    url=request.url,
                    status=500,
                    content='',
                    error=str(e)
                )

    async def process_batch(self, requests: List[ScrapeRequest]) -> List[ScrapeResponse]:
        tasks = [self.scrape(req) for req in requests]
        return await asyncio.gather(*tasks)

# Example usage:
'''
async def main():
    node = ScraperNode('node-1', max_concurrent=5, rate_limit_per_second=1.0)
    node.add_proxies(['http://proxy1:8080', 'http://proxy2:8080'])
    
    requests = [
        ScrapeRequest('http://example.com'),
        ScrapeRequest('http://example.org')
    ]
    
    responses = await node.process_batch(requests)
    for resp in responses:
        print(f"URL: {resp.url}, Status: {resp.status}")
    
    await node.close()

if __name__ == "__main__":
    asyncio.run(main())
'''