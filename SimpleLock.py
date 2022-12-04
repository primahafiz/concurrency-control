from TypeTransaction import *
from numpy import * 

class SimpleLock:
    def __init__(self,listTransaction):
        self.queue = []
        # List of pair of transactionID(int) and resource(int)
        self.x_lock = []
        self.listTransaction = []
        for i in range(len(listTransaction)):
            # Timestamp, transaction id, resource id, type transaction
            self.listTransaction.append([i,listTransaction[i][1],listTransaction[i][2],listTransaction[i][0]])

    def isLockExists1(self,resourceID):
        for i in range(len(self.x_lock)):
            if(self.x_lock[i][1]==resourceID):
                return True
        return False

    def isLockExists2(self,transactionID,resourceID):
        for i in range(len(self.x_lock)):
            if(self.x_lock[i][0]==transactionID and self.x_lock[i][1]==resourceID):
                return True
        return False
    
    def removeLock(self,transactionID,resourceID):
        newArr = []
        for i in range(len(self.x_lock)):
            if(self.x_lock[i][0] == transactionID and self.x_lock[i][1] == resourceID):
                print(f'Release lock resource {self.x_lock[i][1]} from transaction {self.x_lock[i][0]}')
                continue
            newArr.append([self.x_lock[i][0],self.x_lock[i][1]])
        self.x_lock = []
        for i in range(len(newArr)):
            self.x_lock.append([newArr[i][0],newArr[i][1]])
    
    def addLock(self,transactionID,resourceID):
        self.x_lock.append([transactionID,resourceID])
    
    def releaseLock(self,transactionID):
        newArr = []
        for i in range(len(self.x_lock)):
            if(self.x_lock[i][0] == transactionID):
                print(f'Release lock resource {self.x_lock[i][1]} from transaction {self.x_lock[i][0]}')
                continue
            newArr.append([self.x_lock[i][0],self.x_lock[i][1]])
        self.x_lock = []
        for i in range(len(newArr)):
            self.x_lock.append([newArr[i][0],newArr[i][1]])
    
    def removeFromQueue(self,timestamp):
        newArr = []
        for i in range(len(self.queue)):
            if(self.queue[i][0] == timestamp):
                continue
            newArr.append([self.queue[i][0],self.queue[i][1],self.queue[i][2],self.queue[i][3]])
        self.queue = []
        for i in range(len(newArr)):
            self.queue.append([newArr[i][0],newArr[i][1],newArr[i][2],newArr[i][3]])
    
    def isInQueue(self,transactionID):
        for i in range(len(self.queue)):
            if(self.queue[i][1] == transactionID):
                return True
        return False

    def getStatusWriteOrRead(self,transactionID,resourceID,typeTransaction):
        if(self.isLockExists2(transactionID,resourceID)):
            return 0
        elif(self.isLockExists1(resourceID)):
            return 1
        else:
            return 2
    
    def runTransaction(self):
        print('======= Run All Transaction =======')
        while not(len(self.queue) == 0 and len(self.listTransaction) == 0):
            dontRun = []
            toRemove = []
            notDeadlock = False

            for i in range(len(self.queue)):
                newTransaction = self.queue[i].copy()
                if(self.queue[i][1] in dontRun):
                    continue
                status = self.getStatusWriteOrRead(newTransaction[1],newTransaction[2],newTransaction[3])
                if(status == 0):
                    print('QUEUE',newTransaction[3],newTransaction[1],newTransaction[2])
                    print(f'Executing transaction {newTransaction[3]} with ID {newTransaction[1]} on resource {newTransaction[2]}')
                    toRemove.append(newTransaction[0])
                    notDeadlock = True
                if(status == 1):
                    dontRun.append(newTransaction[1])
                else:
                    print('QUEUE',newTransaction[3],newTransaction[1],newTransaction[2])
                    print(f'Get transaction {newTransaction[3]} with ID {newTransaction[1]} on resource {newTransaction[2]} from queue')
                    print(f'Grant lock on resource {newTransaction[2]} to transaction {newTransaction[1]}')
                    print(f'Executing transaction {newTransaction[3]} with ID {newTransaction[1]} on resource {newTransaction[2]}')
                    toRemove.append(newTransaction[0])
                    self.addLock(newTransaction[1],newTransaction[2])
                    notDeadlock = True
            
            for i in range(len(toRemove)):
                self.removeFromQueue(toRemove[i])
            
            if(len(self.listTransaction) != 0):
                if(self.isInQueue(self.listTransaction[0][1])):
                    print(self.listTransaction[0][3],self.listTransaction[0][1],self.listTransaction[0][2])
                    if(self.listTransaction[0][3] == TypeTransaction.READ.value[0] or self.listTransaction[0][3] == TypeTransaction.WRITE.value[0]):
                        print(f'Put transaction {self.listTransaction[0][3]} with ID {self.listTransaction[0][1]} on resource {self.listTransaction[0][2]} into queue')
                    else:
                        print(f'Put transaction {self.listTransaction[0][3]} with ID {self.listTransaction[0][1]} into queue')
                    self.queue.append(self.listTransaction[0].copy())
                    notDeadlock = True
                    del self.listTransaction[0]
                    continue
                newTransaction = self.listTransaction[0].copy()
                if(newTransaction[3] == TypeTransaction.READ.value[0] or newTransaction[3] == TypeTransaction.WRITE.value[0]):
                    status = self.getStatusWriteOrRead(newTransaction[1],newTransaction[2],newTransaction[3])
                    if(status == 0):
                        print(newTransaction[3],newTransaction[1],newTransaction[2])
                        print(f'Executing transaction {newTransaction[3]} with ID {newTransaction[1]} on resource {newTransaction[2]}')
                        notDeadlock = True
                    elif(status == 1):
                        print(newTransaction[3],newTransaction[1],newTransaction[2])
                        print(f'Cannot execute transaction {newTransaction[3]} with ID {newTransaction[1]} on resource {newTransaction[2]}, put transaction into queue')
                        self.queue.append(self.listTransaction[0].copy())
                    elif(status == 2):
                        print(newTransaction[3],newTransaction[1],newTransaction[2])
                        print(f'Grant lock on resource {newTransaction[2]} to transaction {newTransaction[1]}')
                        print(f'Executing transaction {newTransaction[3]} with ID {newTransaction[1]} on resource {newTransaction[2]}')
                        self.addLock(newTransaction[1],newTransaction[2])
                        notDeadlock = True
                elif(newTransaction[3] == TypeTransaction.ABORT.value[0]):
                    if(self.isLockExists1(newTransaction[1])):
                        print(f'Cannot execute transaction {newTransaction[3]} with ID {newTransaction[1]}, put transaction into queue')
                        self.queue.append(self.listTransaction[0].copy())
                    else:
                        print(f'Executing transaction ABORT with ID {newTransaction[1]}')
                        self.releaseLock(newTransaction[1])
                        notDeadlock = True
                elif(newTransaction[3] == TypeTransaction.COMMIT.value[0]):
                    if(self.isLockExists1(newTransaction[1])):
                        print(f'Cannot execute transaction {newTransaction[3]} with ID {newTransaction[1]}, put transaction into queue')
                        self.queue.append(self.listTransaction[0].copy())
                    else:
                        print(f'Executing transaction COMMIT with ID {newTransaction[1]}')
                        self.releaseLock(newTransaction[1])
                        notDeadlock = True
                else:
                    print('Invalid Transaction Type')

                del self.listTransaction[0]
            if(notDeadlock == False and len(self.listTransaction) == 0):
                print('DEADLOCK')
                return