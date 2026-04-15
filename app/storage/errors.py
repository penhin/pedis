class StorageError(Exception):
    pass


class WrongTypeError(StorageError):
    pass


class InvalidValueError(StorageError):
    pass


class InvalidStreamIdError(StorageError):
    pass


class StreamIdOrderError(StorageError):
    pass
