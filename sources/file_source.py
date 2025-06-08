from controller import DataSource
import time

class FileSource(DataSource):
    def __init__(self):
        self.filepath = None
        self.file = None
        self.initialized = False
        self.closed = False
        self.continuous = False
        self.poll_interval = 0.5  # seconds

    def initialize(self, params):
        self.filepath = params.get("filepath")
        if not self.filepath:
            raise ValueError("Filepath must be provided in params")
        self.continuous = params.get("continuous", False)
        self.poll_interval = params.get("poll_interval", 0.5)
        self.file = open(self.filepath, "r")
        self.initialized = True

    def get_entries(self):
        if not self.file:
            raise RuntimeError("File not opened. Did you call initialize?")
        if not self.continuous:
            return [line.rstrip("\n") for line in self.file]
        else:
            # Generator for continuous reading (tail -f style)
            while True:
                where = self.file.tell()
                line = self.file.readline()
                if line:
                    yield line.rstrip("\n")
                else:
                    time.sleep(self.poll_interval)
                    self.file.seek(where)

    def close(self):
        if self.file:
            self.file.close()
            self.closed = True
