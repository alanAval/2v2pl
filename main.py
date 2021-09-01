from objects import Object, ObjectType
from operation import Command, Lock, Operation


S = 'r1(x)r2(x)w1(x)c1(x)c2(x)'
S1 = 'r1xr2xw3xc3c2c1'

db = Object(None, 'DB', ObjectType.DATABASE)
area1 = Object(db, '1', ObjectType.TABLESPACE)
x = Object(area1, 'x', ObjectType.TABLE)
xp1 = Object(x, '1', ObjectType.PAGE)
xp1r1 = Object(xp1, '1', ObjectType.ROW)
xp1r2 = Object(xp1, '2', ObjectType.ROW)
xp1r3 = Object(xp1, '3', ObjectType.ROW)
xp2 = Object(x, '2', ObjectType.PAGE)
xp2r1 = Object(xp2, '1', ObjectType.ROW)

scheduler = [
    Command(Operation.READ, xp2, 2),
    Command(Operation.WRITE, x, 2),
    Command(Operation.WRITE, xp1, 1),
    Command(Operation.COMMIT, None, 2),
    Command(Operation.COMMIT, None, 1)
]

waiting = []

transactions = []

deadLockGraph = {}

def verifyLock(command, operations):
    return command.object.verifyLock(operations, command.transaction)

def verifyLockForObject(object, operations, transaction):
    return object.verifyLock(operations, transaction)

def verifyLockForTransactionWithObject(object, operations, transactions):
    return object.verifyLockForTransactionWithObject(operations, transactions)

def verifyLockForTransaction(operations, transactions):
    return db.verifyLockForTransaction(operations, transactions)

def scheduleCommit(command):
    print('Escalona ' + repr(command))
    db.removeLocksForTransaction(command.transaction)
    del deadLockGraph[command.transaction]
    for deadLockArray in deadLockGraph.values():
        while command.transaction in deadLockArray:
            deadLockArray.remove(command.transaction)

def tryScheduleCommand(command):
    if command.operation == Operation.WRITE:
        hasLock = verifyLock(command, [Operation.WRITE, Operation.COMMIT])
        if hasLock:
            deadLockGraph[command.transaction].append(hasLock.transaction)
            return False
        else:
            command.object.addLock(Lock(command))
            print('Escalona em outra versão ' + repr(command))
    elif command.operation == Operation.READ:
        hasLock = verifyLock(command, [Operation.COMMIT])
        if hasLock:
            deadLockGraph[command.transaction].append(hasLock.transaction)
            return False
        else:
            command.object.addLock(Lock(command))
            if verifyLockForTransactionWithObject(command.object, [Operation.WRITE], [command.transaction]):
                print('Escalona em outra versão ' + repr(command))
            else:
                print('Escalona ' + repr(command))
    else:
        currentObject = verifyLockForTransaction([Operation.WRITE], [command.transaction])
        while currentObject is not None:
            lock = verifyLockForObject(currentObject.object, [Operation.READ, Operation.WRITE, Operation.COMMIT], command.transaction)
            if lock:
                deadLockGraph[command.transaction].append(lock.transaction)
                return False
            else:
                currentObject.operation = Operation.COMMIT
            currentObject = verifyLockForTransaction([Operation.WRITE], [command.transaction])
        scheduleCommit(command)
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

def hasDeadLock(scheduler, waiting, deadLockGraph, command):
    hasDeadLock = False
    for deadLock in deadLockGraph[command.transaction]:
        if command.transaction in deadLockGraph[deadLock]:
            hasDeadLock = True
            print('Deadlock encontrado, abortando transação %s' % command.transaction)
            for com in waiting.copy():
                if com.transaction == command.transaction:
                    waiting.remove(com)
            for com in scheduler.copy():
                if com.transaction == command.transaction:
                    scheduler.remove(com)
            db.removeLocksForTransaction(command.transaction)
            del deadLockGraph[command.transaction]
            for deadLockArray in deadLockGraph.values():
                while command.transaction in deadLockArray:
                    deadLockArray.remove(command.transaction)
            transferWaitingToScheduler()
    return hasDeadLock

def addCommandToWaitingList(command):
    global waiting
    print('Command (' + repr(command) + ') added to waiting list')
    waiting.append(command)

while scheduler:
    command = scheduler[0]
    scheduler.remove(command)
    if command.transaction not in transactions:
        transactions.append(command.transaction)
        deadLockGraph[command.transaction] = []
    if hasWaitingCommands(waiting, command):
        addCommandToWaitingList(command)
        continue
    if not tryScheduleCommand(command):
        if not hasDeadLock(scheduler, waiting, deadLockGraph, command):
            addCommandToWaitingList(command)
    elif command.operation == Operation.COMMIT:
        transferWaitingToScheduler()
    
