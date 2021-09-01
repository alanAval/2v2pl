from enum import Enum
from operation import Lock

class ObjectType(Enum):
    DATABASE = 0,
    TABLESPACE = 1,
    TABLE = 2,
    PAGE = 3,
    ROW = 4

class Object:
    def __init__(self, parent, name, type) -> None:
        self.parent = parent
        self.name = name
        self.type = type
        self.children = []
        self.locks = []
        if parent:
            parent.children.append(self)

    def addLock(self, lock, setParentLock  = True):
        if self.parent and setParentLock:
            parentLock = lock.copyIntentional()
            self.parent.addLock(parentLock)
        for child in self.children:
            child.addLock(lock, False)
        self.locks.append(lock)

    def verifyLock(self, operations, transaction):
        for lock in self.locks:
            if lock.operation in operations and \
                lock.transaction != transaction:
                return lock
        return None

    def verifyLockForTransactionWithObject(self, operations, transactions):
        hasLock = False
        for lock in self.locks:
            if lock.operation in operations and lock.transaction in transactions:
                hasLock = True
        return hasLock

    def getAllLocks(self):
        locks = self.locks.copy()
        for child in self.children:
            locks += child.getAllLocks()
        return locks

    def verifyLockForTransaction(self, operations, transactions):
        locks = self.getAllLocks()
        for lock in locks:
            if lock.operation in operations and lock.transaction in transactions:
                return lock
        return None

    def removeLocksForTransaction(self, transaction):
        for child in self.children:
            child.removeLocksForTransaction(transaction)
        for lock in self.locks.copy():
            if lock.transaction == transaction:
                self.locks.remove(lock)

    def __repr__(self) -> str:
        return '%s %s' % (self.type, self.name)