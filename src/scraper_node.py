import asyncio
import aiohttp
from datetime import datetime
import redis
from ratelimit import limits, sleep_and_retry
from typing import Dict, List, Optional

class ScraperNode:
    def __init__(self, node_id: str, redis_url: str):
        self.node_id = node_id
        self.redis_client = redis.Redis.from_url(redis_url)
        self.session: Optional[aiohttp.ClientSession] = None
        self.rate_limits: Dict[str, int] = {}

    async def initialize(self):
        self.session = aiohttp.ClientSession()
        await self.register_node()

    async def shutdown(self):
        if self.session:
            await self.session.close()
        await self.deregister_node()

    async def register_node(self):
        self.redis_client.sadd('active_nodes', self.node_id)
        self.redis_client.hset(f'node:{self.node_id}', 'last_heartbeat', datetime.now().timestamp())

    async def deregister_node(self):
        self.redis_client.srem('active_nodes', self.node_id)
        self.redis_client.delete(f'node:{self.node_id}')

    @sleep_and_retry
    @limits(calls=60, period=60)
    async def rate_limited_request(self, url: str) -> dict:
        domain = url.split('/')[2]
        
        # Get domain-specific rate limit
        domain_key = f'ratelimit:{domain}'
        if domain not in self.rate_limits:
            limit = self.redis_client.get(domain_key)
            self.rate_limits[domain] = int(limit) if limit else 60

        # Distributed rate limiting using Redis
        current = self.redis_client.incr(f'requests:{domain}')
        if current > self.rate_limits[domain]:
            await asyncio.sleep(1)
            self.redis_client.decr(f'requests:{domain}')
            raise Exception(f'Rate limit exceeded for {domain}')

        try:
            async with self.session.get(url) as response:
                return {
                    'url': url,
                    'status': response.status,
                    'content': await response.text(),
                    'timestamp': datetime.now().isoformat()
                }
        except Exception as e:
            return {
                'url': url,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }

    async def heartbeat(self):
        while True:
            self.redis_client.hset(
                f'node:{self.node_id}',
                'last_heartbeat',
                datetime.now().timestamp()
            )
            await asyncio.sleep(30)

    async def process_queue(self):
        while True:
            # Pop URL from distributed queue
            url = self.redis_client.lpop('scrape_queue')
            if not url:
                await asyncio.sleep(1)
                continue

            # Process URL with rate limiting
            try:
                result = await self.rate_limited_request(url.decode('utf-8'))
                self.redis_client.rpush('results_queue', str(result))
            except Exception as e:
                self.redis_client.rpush('failed_queue', url)

    async def run(self):
        await self.initialize()
        try:
            await asyncio.gather(
                self.heartbeat(),
                self.process_queue()
            )
        finally:
            await self.shutdown()

if __name__ == '__main__':
    node = ScraperNode('node1', 'redis://localhost:6379')
    asyncio.run(node.run())