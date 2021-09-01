from operation import Command, Lock, Operation
import numpy as np


S = 'r1(x)r2(x)w1(x)c1(x)c2(x)'
S1 = 'r1xr2xw3xc3c2c1'

scheduler = [
    Command(Operation.READ, 'x', 1),
    Command(Operation.READ, 'x', 2),
    Command(Operation.WRITE, 'x', 1),
    Command(Operation.WRITE, 'x', 2),
    Command(Operation.COMMIT, 'x', 1),
    Command(Operation.COMMIT, 'x', 2)
]
# scheduler = [
#     Command(Operation.READ, 'x', 1),
#     Command(Operation.READ, 'x', 2),
#     Command(Operation.WRITE, 'x', 1),
#     Command(Operation.COMMIT, 'x', 1),
#     Command(Operation.COMMIT, 'x', 2)
# ]
# scheduler = [
#     Command(Operation.READ, 'x', 1),
#     Command(Operation.READ, 'x', 2),
#     Command(Operation.WRITE, 'x', 3),
#     Command(Operation.COMMIT, '', 3),
#     Command(Operation.COMMIT, '', 2),
#     Command(Operation.COMMIT, '', 1)
# ]

locks = {'x' : []}

waiting = []

transactions = []

deadLockGraph = {}

def verifyLock(locks, command, operations):
    for lock in locks[command.object]:
        if lock.operation in operations and lock.transaction != command.transaction:
            return lock
    return None

def verifyLockForObject(locks, object, operations, transaction):
    for lock in locks[object]:
        if lock.operation in operations and lock.transaction != transaction:
            return lock
    return None

def verifyLockForTransactionWithObject(locks, object, operations, transactions):
    hasLock = False
    for lock in locks[object]:
        if lock.operation in operations and lock.transaction in transactions:
            hasLock = True
    return hasLock

def verifyLockForTransaction(locks, operations, transactions):
    for allLocks in locks.values():
        for lock in allLocks:
            if lock.operation in operations and lock.transaction in transactions:
                return lock

    return None

def scheduleCommit(locks, command):
    print('Escalona ' + repr(command))
    for allLocks in locks.values():
        for lock in allLocks:
            if lock.transaction == command.transaction:
                allLocks.remove(lock)
    del deadLockGraph[command.transaction]
    for deadLockArray in deadLockGraph.values():
        while command.transaction in deadLockArray:
            deadLockArray.remove(command.transaction)

def tryScheduleCommand(locks, command):
    if command.operation == Operation.WRITE:
        hasLock = verifyLock(locks, command, [Operation.WRITE, Operation.COMMIT])
        if hasLock:
            deadLockGraph[command.transaction].append(hasLock.transaction)
            return False
        else:
            locks[command.object].append(Lock(command))
            print('Escalona em outra versão ' + repr(command))
    elif command.operation == Operation.READ:
        hasLock = verifyLock(locks, command, [Operation.COMMIT])
        if hasLock:
            deadLockGraph[command.transaction].append(hasLock.transaction)
            return False
        else:
            locks[command.object].append(Lock(command))
            if verifyLockForTransactionWithObject(locks, command.object, [Operation.WRITE], [command.transaction]):
                print('Escalona em outra versão ' + repr(command))
            else:
                print('Escalona ' + repr(command))
    else:
        currentObject = verifyLockForTransaction(locks, [Operation.WRITE], [command.transaction])
        while currentObject is not None:
            lock = verifyLockForObject(locks, currentObject.object, [Operation.READ, Operation.WRITE, Operation.COMMIT], command.transaction)
            if lock:
                deadLockGraph[command.transaction].append(lock.transaction)
                return False
            else:
                currentObject.operation = Operation.COMMIT
            currentObject = verifyLockForTransaction(locks, [Operation.WRITE], [command.transaction])
        scheduleCommit(locks, command)
    return True

def hasWaitingCommands(waiting, command):
    if command.operation == Operation.COMMIT:
        for c in waiting:
            if c.transaction == command.transaction:
                return True
    return False

def transferWaitingToScheduler():
    global scheduler
    global waiting
    scheduler = waiting + scheduler
    waiting = []

def hasDeadLock(scheduler, locks, waiting, deadLockGraph, command):
    hasDeadLock = False
    for deadLock in deadLockGraph[command.transaction]:
        if command.transaction in deadLockGraph[deadLock]:
            hasDeadLock = True
            print('Deadlock encontrado, matando transação %s' % command.transaction)
            for com in waiting.copy():
                if com.transaction == command.transaction:
                    waiting.remove(com)
            for com in scheduler.copy():
                if com.transaction == command.transaction:
                    scheduler.remove(com)
            for allLock in locks.values():
                for lock in allLock.copy():
                    if lock.transaction == command.transaction:
                        allLock.remove(lock)
            transferWaitingToScheduler()
    return hasDeadLock

while scheduler:
    command = scheduler[0]
    scheduler.remove(command)
    if command.transaction not in transactions:
        transactions.append(command.transaction)
        deadLockGraph[command.transaction] = []
    if hasWaitingCommands(waiting, command):
        waiting.append(command)
        continue
    if not tryScheduleCommand(locks, command):
        if not hasDeadLock(scheduler, locks, waiting, deadLockGraph, command):
            waiting.append(command)
    else:
        transferWaitingToScheduler()
    
