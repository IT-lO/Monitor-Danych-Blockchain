class BlockchainState:
    def __init__(self):
        self.latest_stats = None
        self.is_running = True
        self.logs = []

    def update_stats(self, new_stats: dict):
        self.latest_stats = new_stats

    def add_log(self, message: str):
        self.logs.append(message)
        if len(self.logs) > 30:
            self.logs.pop(0)
