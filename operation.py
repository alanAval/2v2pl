from enum import Enum
class Operation(Enum):
    READ = 0
    WRITE = 1
    COMMIT = 2

    @staticmethod
    def fromString(string):
        if string == 'R':
            return Operation.READ
        elif string == 'W':
            return Operation.WRITE
        elif string == 'C':
            return Operation.COMMIT

class Command:
    def __init__(self, operation, object, transaction):
        self.operation = operation
        self.object = object
        self.transaction = transaction

    def __repr__(self) -> str:
        if self.operation == Operation.COMMIT:
            return 'Operation %s on transaction %s' % (self.operation, self.transaction)
        return 'Operation %s over object %s on transaction %s' % (self.operation, self.object, self.transaction)


class Lock:
    def __init__(self, command, intentional = False):
        self.operation = command.operation
        self.object = command.object
        self.transaction = command.transaction
        self.intentional = intentional

    def copyIntentional(self):
        auxCommand = Command(self.operation, self.object, self.transaction)
        return Lock(auxCommand, True)