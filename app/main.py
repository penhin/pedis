import logging
import os

from app.bootstrap import bootstrap_server
from app.server.server import RedisServer, ServerConfig


def configure_logging():
    level_name = os.getenv("PEDIS_LOG_LEVEL", "WARNING").upper()
    level = getattr(logging, level_name, logging.WARNING)
    logging.basicConfig(level=level, format="%(levelname)s:%(name)s:%(message)s")


def main():
    configure_logging()
    logger = logging.getLogger(__name__)

    try:
        config = ServerConfig()
        config = ServerConfig.parse_config(config)

        server = RedisServer(config)
        bootstrap_server(server)
        logger.info("Server created, starting...")
        server.start()
    except Exception as e:
        logger.exception("Error: %s", e)
           
if __name__ == "__main__":
    main()
