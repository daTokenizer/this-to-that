from controller import DataSource

class TestSource(DataSource):
    def __init__(self, entries=None):
        self.entries = entries or []
        self.initialized = False
        self.closed = False
        self.init_params = None
    
    def initialize(self, params):
        self.initialized = True
        self.init_params = params
        self.entries = params.get("entries", self.entries)
    
    def get_entries(self):
        return self.entries
    
    def close(self):
        self.closed = True
