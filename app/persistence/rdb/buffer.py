import struct

class RDBBufferError(Exception):
    pass

class RDBBuffer:
    _u8 = struct.Struct('B')
    _u16le = struct.Struct('<H')
    _u32le = struct.Struct('<I')
    _u32be = struct.Struct('>I')
    _u64le = struct.Struct('<Q')
    _u64be = struct.Struct('>Q')

    def __init__(self, file_handle):
        self.fp = file_handle
        self.offset = 0
    
    def read(self, n):
        data = self.fp.read(n)
        if (len(data) != n):
            raise RDBBufferError(f"Except {n} bytes but got {len(data)}")
        self.offset += n
        return data
        
    def read_uint8(self):
        return self._u8.unpack(self.read(1))[0]
    
    def read_uint16(self):
        return self._u16le.unpack(self.read(2))[0]

    def read_uint32(self):
        return self._u32le.unpack(self.read(4))[0]
    
    def read_uint32_be(self):
        return self._u32be.unpack(self.read(4))[0]
    
    def read_uint64(self):
        return self._u64le.unpack(self.read(8))[0]
    
    def read_uint64_be(self):
        return self._u64be.unpack(self.read(8))[0]
    
    def read_bytes(self, n):
        return self.read(n)

        
