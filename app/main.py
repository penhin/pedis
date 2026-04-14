from app.bootstrap import bootstrap_server
from app.server.server import RedisServer, ServerConfig

def main():
    try:
        config = ServerConfig()
        config = ServerConfig.parse_config(config)

        server = RedisServer(config)
        bootstrap_server(server)
        print("Server created, starting...")
        server.start()
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
           
if __name__ == "__main__":
    main()
