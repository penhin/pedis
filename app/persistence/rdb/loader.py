from .parser import RDBParser

class RDBLoader:
    def load(self, server):
        path = server.config.dir + "/" + server.config.dbfilename
        
        with open(path, "rb") as f:
            parser = RDBParser(f)