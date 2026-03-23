import os
from typing import List
from collections import deque
from decentralized_governance import GovernanceProtocol
from scraping_agent import ScrapingAgent
from data_storage import DecentralizedStorage

class DWSNManager:
    def __init__(self):
        self.governance_protocol = GovernanceProtocol()
        self.agent_queue = deque()
        self.storage = DecentralizedStorage()

    def add_new_agent(self, agent: ScrapingAgent):
        self.agent_queue.append(agent)
        self.governance_protocol.register_agent(agent)

    def manage_agents(self):
        while self.agent_queue:
            agent = self.agent_queue.popleft()
            self.governance_protocol.assign_task(agent)
            self.storage.store_data(agent.scrape_data())

    def run(self):
        while True:
            self.manage_agents()
            self.governance_protocol.update_protocols()
