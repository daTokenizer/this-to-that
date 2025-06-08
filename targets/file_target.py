from controller import DataTarget

class FileTarget(DataTarget):
    def __init__(self):
        self.filepath = None
        self.file = None
        self.initialized = False
        self.closed = False

    def initialize(self, params):
        self.filepath = params.get("filepath")
        if not self.filepath:
            raise ValueError("Filepath must be provided in params")
        self.file = open(self.filepath, "w")
        self.initialized = True

    def create_entries(self, entries):
        if not self.file:
            raise RuntimeError("File not opened. Did you call initialize?")
        if isinstance(entries, (list, tuple)):
            for entry in entries:
                self.file.write(str(entry) + "\n")
        else:
            self.file.write(str(entries) + "\n")
        self.file.flush()

    def close(self):
        if self.file:
            self.file.close()
            self.closed = True

