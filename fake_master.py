import socket

CRLF = b"\r\n"

def recv_all(sock, n):
    data = b""
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise ConnectionError
        data += chunk
    return data

def read_resp(sock):
    prefix = sock.recv(1)
    if not prefix:
        return None
    if prefix == b"*":
        line = b""
        while not line.endswith(CRLF):
            line += sock.recv(1)
        count = int(line[:-2])
        return [read_resp(sock) for _ in range(count)]
    if prefix == b"$":
        line = b""
        while not line.endswith(CRLF):
            line += sock.recv(1)
        length = int(line[:-2])
        if length == -1:
            return None
        data = recv_all(sock, length)
        sock.recv(2)
        return data
    if prefix in (b"+", b"-", b":"):
        line = prefix
        while not line.endswith(CRLF):
            line += sock.recv(1)
        return line[1:-2]
    raise ValueError(prefix)

def encode(v):
    if isinstance(v, list):
        out = b"*"+str(len(v)).encode()+CRLF
        for w in v:
            out += encode(w)
        return out
    if isinstance(v, bytes):
        return b"$"+str(len(v)).encode()+CRLF+v+CRLF
    if isinstance(v, str):
        return b"+"+v.encode()+CRLF
    if isinstance(v, int):
        return b":"+str(v).encode()+CRLF
    raise


def main():
    port=6390
    print('binding to', port)
    serv=socket.create_server(('localhost',port))
    conn,addr=serv.accept()
    conn.setblocking(True)
    # handshake
    print('got',read_resp(conn))
    conn.sendall(encode(b'PONG'))
    print('got',read_resp(conn))
    conn.sendall(encode(b'OK'))
    print('got',read_resp(conn))
    conn.sendall(encode(b'OK'))
    print('got',read_resp(conn))
    # fullresync
    conn.sendall(encode("FULLRESYNC fake 0"))
    conn.sendall(encode(b""))
    print('sent fullresync')
    # ask master_repl_offset
    conn.sendall(encode([b'REPLCONF',b'GETACK',b'*']))
    print('reply',read_resp(conn))
    # send a write command and ask again
    conn.sendall(encode([b'SET',b'k',b'v']))
    print('reply to set',read_resp(conn))
    conn.sendall(encode([b'REPLCONF',b'GETACK',b'*']))
    print('reply',read_resp(conn))

if __name__=='__main__':
    main()