import asyncio
import aiohttp
from datetime import datetime
from typing import Dict, List, Optional
from dataclasses import dataclass
from enum import Enum

class TaskStatus(Enum):
    PENDING = 'pending'
    IN_PROGRESS = 'in_progress'
    COMPLETED = 'completed'
    FAILED = 'failed'

@dataclass
class ScrapingTask:
    url: str
    status: TaskStatus
    retries: int = 0
    max_retries: int = 3
    result: Optional[str] = None
    error: Optional[str] = None
    created_at: datetime = datetime.now()

class ScraperNode:
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.task_queue: List[ScrapingTask] = []
        self.active_tasks: Dict[str, ScrapingTask] = {}
        self.completed_tasks: List[ScrapingTask] = []
        self.session: Optional[aiohttp.ClientSession] = None

    async def initialize(self):
        self.session = aiohttp.ClientSession()

    async def shutdown(self):
        if self.session:
            await self.session.close()

    async def add_task(self, url: str) -> ScrapingTask:
        task = ScrapingTask(url=url, status=TaskStatus.PENDING)
        self.task_queue.append(task)
        return task

    async def process_task(self, task: ScrapingTask) -> None:
        task.status = TaskStatus.IN_PROGRESS
        try:
            if not self.session:
                await self.initialize()
            async with self.session.get(task.url) as response:
                if response.status == 200:
                    task.result = await response.text()
                    task.status = TaskStatus.COMPLETED
                else:
                    raise Exception(f'HTTP {response.status}')
        except Exception as e:
            task.error = str(e)
            task.retries += 1
            if task.retries < task.max_retries:
                task.status = TaskStatus.PENDING
                self.task_queue.append(task)
            else:
                task.status = TaskStatus.FAILED

    async def run(self):
        while True:
            if self.task_queue:
                task = self.task_queue.pop(0)
                await self.process_task(task)
                if task.status == TaskStatus.COMPLETED or task.status == TaskStatus.FAILED:
                    self.completed_tasks.append(task)
            await asyncio.sleep(1)

    def get_stats(self) -> Dict:
        return {
            'node_id': self.node_id,
            'pending_tasks': len(self.task_queue),
            'completed_tasks': len(self.completed_tasks),
            'failed_tasks': len([t for t in self.completed_tasks if t.status == TaskStatus.FAILED])
        }

    async def health_check(self) -> bool:
        try:
            if not self.session:
                await self.initialize()
            async with self.session.get('https://www.google.com') as response:
                return response.status == 200
        except:
            return False