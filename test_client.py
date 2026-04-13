import socket

def send_command(host, port, command):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))

    # RESP format for REPLCONF GETACK
    resp_command = b"*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"
    sock.sendall(resp_command)

    response = sock.recv(1024)
    print(f"Response: {response}")

    sock.close()

if __name__ == "__main__":
    send_command("localhost", 6379, "REPLCONF")