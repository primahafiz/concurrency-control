from Parser import Parser
from SimpleLock import SimpleLock
from SerialOptimistic import SerialOptimistic

if __name__ == "__main__":
    pathFile = input('Input transaction file name : ')
    parser = Parser(pathFile)
    arr = parser.getArr()

    print('1. Simple Lock')
    print('2. Serial Optimistic')
    inp = int(input('Input concurrency method (1/2) : '))

    if(inp == 1):
        schedule = SimpleLock(arr)
        schedule.runTransaction()
    elif(inp == 2):
        schedule = SerialOptimistic(arr)
        schedule.runTransaction()