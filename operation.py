from enum import Enum
class Operation(Enum):
    READ = 0
    WRITE = 1
    COMMIT = 2

class Command:
    def __init__(self, operation, object, transaction):
        self.operation = operation
        self.object = object
        self.transaction = transaction


class Lock:
    def __init__(self, command):
        self.__init__(command.operation, command.object, command.transaction)
        
    def __init__(self, operation, object, transaction):
        self.operation = operation
        self.object = object
        self.transaction = transaction