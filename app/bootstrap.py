import app.commands

from app.persistence.rdb.loader import RDBLoader

def bootstrap_server(server):
    """Initialize command registration and optional persisted state."""
    _ = app.commands
    if not server.aof.load():
        RDBLoader().load(server)
    server.aof.open()
