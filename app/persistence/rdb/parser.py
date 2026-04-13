import time

from .constants import *
from .buffer import RDBBuffer
from .callback_handler import RdbCallback

class RDBParseError(Exception):
    pass

class RDBParser:
    def __init__(self, file_handle, callback: RdbCallback):
        self.buffer = RDBBuffer(file_handle)
        self.callback = callback
        
    def read_string(self):
        length, is_spec = self.decode_length()
        if is_spec:
            raise NotImplementedError(f"Special string read not supported yet")

        return self.buffer.read_bytes(length)
    
    def read_value_by_type(self, value_type):
        if value_type == ValueType.STRING:
            return self.read_string()
        
        raise NotImplementedError(f"Value type {value_type} not supported yet")
        
    def decode_length(self):
        first_byte = self.buffer.read_uint8()
        
        enc = (first_byte & 0xC0) >> 6 

        if enc == 0b00:
            return (first_byte & 0x3F), False
        elif enc == 0b01:
            return (((first_byte & 0x3F) << 8) | self.buffer.read_uint8(), False)
        elif enc == 0b10:
            if first_byte == 0x80:
                return self.buffer.read_uint32_be(), False
            elif first_byte == 0x81:
                return self.buffer.read_uint64_be(), False
        elif enc == 0b11:
            return (first_byte & 0x3F), True
        
    def parse(self):
        if self.buffer.read_bytes(9) == RDB_MAGIC + RDB_VERSION:
            self.callback.on_start(RDB_VERSION)
        else:
            raise RDBParseError(f"Invalid rdb file")

        while True:
            opcode = self.buffer.read_uint8()

            if opcode == OpCode.EOF:
                break
            
            if opcode == OpCode.SELECTDB:
                self.handle_select_db()
            elif opcode == OpCode.RESIZEDB:
                self.handle_resize_db()
            elif opcode == OpCode.AUX:
                self.handle_aux_field()
            elif self.next_is_kv_pair(opcode):
                self.extract_kv(opcode)
            else:
                raise Exception(f"Unexpected OpCode: {opcode}")

    """ Header section """

    """ Metadata section """
            
    """ Database section """

    def handle_select_db(self):
        db_id, = self.decode_length()
        self.callback.on_database_select(db_id)

    def next_is_kv_pair(self, opcode: OpCode):
        return True if opcode < 15 or opcode == OpCode.EXPIRETIME or opcode == OpCode.EXPIRETIMEMS else False
    
    def extract_kv(self, opcode: OpCode):
        expiry = None
        value_type = None

        if opcode == OpCode.EXPIRETIME:
            expiry = self.buffer.read_uint32() 
            value_type = self.buffer.read_uint8()   
        
        elif opcode == OpCode.EXPIRETIMEMS:
            expiry = self.buffer.read_uint64()
            value_type = self.buffer.read_uint8()           
        else:
            value_type = opcode
            expiry = None
            
        key = self.read_string()
        value = self.read_value_by_type(value_type)
        
        self.callback.on_set(key, value, expiry)



                
                