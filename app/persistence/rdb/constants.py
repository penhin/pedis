from enum import IntEnum

RDB_MAGIC = b"REDIS"
RDB_VERSION = b"0011"

class ValueType(IntEnum):
    STRING           = 0
    LIST             = 1
    SET              = 2
    ZSET             = 3
    HASH             = 4
    ZSET_2           = 5  
    MODULE           = 6
    MODULE_2         = 7
    HASH_ZIPMAP      = 9
    LIST_ZIPLIST     = 10
    SET_INTSET       = 11
    ZSET_ZIPLIST     = 12
    HASH_ZIPLIST     = 13
    LIST_QUICKLIST   = 14
    STREAM_LISTPACKS = 15
    
class OpCode(IntEnum):
    AUX          = 0xFA  
    SELECTDB     = 0xFE  
    RESIZEDB     = 0xFB  
    EXPIRETIME   = 0xFD  
    EXPIRETIMEMS = 0xFC
    EOF          = 0xFF  

