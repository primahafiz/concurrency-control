from enum import Enum

class TypeTransaction(Enum):
    READ = 'READ',
    WRITE = 'WRITE',
    COMMIT = 'COMMIT',
    ABORT = 'ABORT'