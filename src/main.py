import asyncio
import aiohttp
import json
import hashlib
import time

class DistributedWebScraper:
    def __init__(self, coordinator_url, worker_urls):
        self.coordinator_url = coordinator_url
        self.worker_urls = worker_urls
        self.tasks = []

    async def scrape_website(self, url):
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                content = await response.text()
                return content

    async def distribute_task(self, url):
        task_id = hashlib.md5(url.encode()).hexdigest()
        task_data = {
            'url': url,
            'task_id': task_id
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(self.coordinator_url, json=task_data) as response:
                response_data = await response.json()
                if response_data['status'] == 'success':
                    worker_url = response_data['worker_url']
                    content = await self.scrape_website(url)
                    async with session.post(worker_url, data=content) as worker_response:
                        return await worker_response.text()
                else:
                    return None

    async def run(self, urls):
        for url in urls:
            self.tasks.append(asyncio.create_task(self.distribute_task(url)))
        results = await asyncio.gather(*self.tasks)
        return results

if __name__ == '__main__':
    coordinator_url = 'http://coordinator.example.com/tasks'
    worker_urls = ['http://worker1.example.com/scrape', 'http://worker2.example.com/scrape']
    scraper = DistributedWebScraper(coordinator_url, worker_urls)
    urls = ['https://www.example.com', 'https://www.google.com', 'https://www.github.com']
    results = asyncio.run(scraper.run(urls))
    for result in results:
        if result:
            print(result)
        else:
            print('Failed to scrape website')