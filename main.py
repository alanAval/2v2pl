from operation import Command, Lock, Operation


S = 'r1(x)r2(x)w1(x)c1(x)w2(x)c2(x)'

blk = 'rl1(x)rl2(x)wl1(x)'

T1 -> <-  T2

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

def tryScheduleCommand(locks, transactions, command):
    if command.operation == Operation.WRITE:
        hasLock = verifyLock(locks, command, [Operation.WRITE, Operation.COMMIT])
        if hasLock:
            return False
        else:
            locks[command.object].append(Lock(command))
            print('Escalona em outra versão' + repr(command))
    elif command.operation == Operation.READ:
        hasLock = verifyLock(locks, command, [Operation.COMMIT])
        if hasLock:
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
            if verifyLockForTransactionWithObject(locks, currentObject.object, [Operation.READ], transactions):
                return False
            else:
                currentObject.operation = Operation.COMMIT
            currentObject = verifyLockForTransaction(locks, [Operation.WRITE], [command.transaction])
        scheduleCommit(locks, command)
    return True

for command in scheduler:
    if command.transaction not in transactions:
        transactions.append(command.transaction)
    shouldTrySchedule = True
    while waiting[command.transaction]:
        commandToExecute = waiting[command.transaction][0]
        if tryScheduleCommand(locks, transactions, commandToExecute):
            waiting[command.transaction].remove(commandToExecute)
        else:
            waiting[command.transaction].append(command)
            shouldTrySchedule = False
            break
    if shouldTrySchedule and not tryScheduleCommand(locks, transactions, command):
        waiting[command.transaction].append(command)
