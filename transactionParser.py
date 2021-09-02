from ast import parse
from operation import Command, Operation
from objects import Object, ObjectType
        

def parseObjects(db, objectStrings):
    for objectString in objectStrings:
        objects = objectString.split('_')
        currentObject = db
        currentType = ObjectType.TABLESPACE
        while objects:
            currentObjectString = objects[0]
            objects = objects[1:]
            auxObject = currentObject.find(currentObjectString)
            if auxObject:
                currentObject = auxObject
                currentType = currentType.succ()
                continue
            auxObject = Object(currentObject, currentObjectString, currentType)
            currentObject = auxObject
            currentType = currentType.succ()


def parseSchedule(db, schedule):
    commands = []
    operations = schedule.split('-')
    objectsStrings = []
    for operation in operations:
        if operation[0] == 'C':
            continue
        startIndex = operation.index('(')
        endIndex = operation.index(')')
        objectsStrings.append(operation[startIndex + 1 : endIndex])
    parseObjects(db, objectsStrings)
    for operation in operations:
        type = operation[0]
        transaction = operation[1]
        if type == 'C':
            command = Command(Operation.fromString(type), None, int(transaction))
        else:
            objectString = objectsStrings[operations.index(operation)]
            object = db.findRecursive(objectString)
            command = Command(Operation.fromString(type), object, int(transaction))
        commands.append(command)
    return commands