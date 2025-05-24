from controller import DataTarget

class TestTarget(DataTarget):
    def __init__(self):
        self.entries = []
        self.initialized = False
        self.closed = False
        self.init_params = None
    
    def initialize(self, params):
        self.initialized = True
        self.init_params = params

    def create_entries(self, entries):
        if isinstance(entries, (list, tuple)):
            self.entries.extend(entries)
        else:
            self.entries.append(entries)
    
    def close(self):
        self.closed = True
