from _typeshed import ReadableBuffer
from operation import Command, Lock, Operation


S = 'r1(x)r2(x)w1(x)w2(x)c1(x)c2(x)'

scheduler = [
    Command(Operation.READ, 'x', 1),
    Command(Operation.READ, 'x', 2),
    Command(Operation.WRITE, 'x', 1),
    Command(Operation.WRITE, 'x', 2),
    Command(Operation.COMMIT, 'x', 1),
    Command(Operation.COMMIT, 'x', 2)
]

locks = {'x' : []}

waiting = {1 : [], 2 : []}

transactions = []

def verifyLock(locks, command, operations):
    hasLock = False
    for lock in locks[command.object]:
        if lock.operation in operations and lock.transaction != command.transaction:
            hasLock = True
    return hasLock

def verifyLockForTransaction(locks, object, operations, transactions):
    hasLock = False
    for lock in locks[object]:
        if lock.operation in operations and lock.transaction in transactions:
            hasLock = True
    return hasLock

def verifyLockForTransaction(locks, operations, transactions):
    for allLocks in locks.values():
        for lock in allLocks:
            if lock.operation in operations and lock.transaction in transactions:
                return lock.object

    return None

def scheduleCommit(locks, command):
    print('Escalona ' + command)
    for allLocks in locks.values():
        for lock in allLocks:
            if lock.transaction == command.transaction:
                allLocks.remove(lock)

def tryScheduleCommand(locks, waiting, transactions, command):
    if command.operation == Operation.WRITE:
        hasLock = verifyLock(locks, command, [Operation.WRITE, Operation.COMMIT])
        if hasLock:
            waiting.append(command)
        else:
            locks[command.object].append(Lock(command))
            print('Escalona em outra versão' + command)
    elif command.Operation == Operation.READ:
        hasLock = verifyLock(locks, command, [Operation.COMMIT])
        if hasLock:
            waiting.append(command)
        else:
            locks[command.object].append(Lock(command))
            if verifyLockForTransaction(locks, command.object, [Operation.WRITE], [command.transaction]):
                print('Escalona em outra versão ' + command)
            else:
                print('Escalona ' + command)
    else:
        hasLock = False
        currentObject = verifyLockForTransaction(locks, [Operation.WRITE], [command.transaction])
        while currentObject is not None:
            if verifyLockForTransaction(locks, [Operation.READ], transactions):
                waiting.append(command)
                hasLock = True
            else:
                locks[command.object].append(Lock(command))
            currentObject = verifyLockForTransaction(locks, [Operation.WRITE], [command.transaction])
        if not hasLock:
            scheduleCommit(locks, command)

for command in scheduler:
    if command.transaction not in transactions:
        transactions.append(command.transaction)
    while not waiting[command.transaction]:
        
    tryScheduleCommand(locks, waiting, transactions, command)
