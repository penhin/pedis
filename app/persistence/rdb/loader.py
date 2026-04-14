import os

from .callback_handler import StorageCallback
from .parser import RDBParser

class RDBLoader:
    def load(self, server):
        path = os.path.join(server.config.dir, server.config.dbfilename)
        if not os.path.exists(path):
            return False

        with open(path, "rb") as f:
            parser = RDBParser(f, StorageCallback(server.storage))
            parser.parse()

        return True
