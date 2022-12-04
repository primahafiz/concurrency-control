from numpy import * 
from TypeTransaction import TypeTransaction

INF = 10**9

class SerialOptimistic:
    def __init__(self,listTransaction):
        # List of pair of transactionID(int) and resource(int)
        self.listTransaction = []
        self.writeTransactionResource = dict()
        self.finishTime = dict()
        self.startTime = dict()
        for i in range(len(listTransaction)):
            # Timestamp, transaction id, resource id, type transaction
            self.listTransaction.append([i,listTransaction[i][1],listTransaction[i][2],listTransaction[i][0]])
            self.finishTime[listTransaction[i][1]] = INF
            self.startTime[listTransaction[i][1]] = INF
            if(listTransaction[i][1] not in self.writeTransactionResource):
                self.writeTransactionResource[listTransaction[i][1]] = []
    
    def validate(self,transactionID):
        for i in self.finishTime:
            if(self.startTime[transactionID] < self.finishTime[i] < self.finishTime[transactionID]):
                arr1 = self.writeTransactionResource[transactionID]
                arr2 = self.writeTransactionResource[i]
                for j in range(len(arr1)):
                    for k in range(len(arr2)):
                        if(arr1[j] == arr1[k]):
                            print(f'Transaction {transactionID} is not valid because there is also transaction {i} executing on WRITE resource {arr2[k]}')
                            return False
        return True
    
    def abortTransaction(self,transactionID):
        self.finishTime[transactionID] = -INF
    
    def runTransaction(self):
        print('======= Run All Transaction =======')
        for i in range(len(self.listTransaction)):
            newTransaction = self.listTransaction[i].copy()
            print(f'{newTransaction[3]} {newTransaction[1]} {newTransaction[2]}')
            if(self.startTime[newTransaction[1]] == INF):
                self.startTime[newTransaction[1]] = newTransaction[0]
            
            if(newTransaction[3] == TypeTransaction.WRITE.value[0]):
                print(f'Pending WRITE transaction {newTransaction[1]} on resource {newTransaction[2]}')
                self.writeTransactionResource[newTransaction[1]].append(newTransaction[2])
            elif(newTransaction[3] == TypeTransaction.READ.value[0]):
                print(f'Executing READ transaction {newTransaction[1]} on resource {newTransaction[2]}')
            elif(newTransaction[3] == TypeTransaction.COMMIT.value[0]):
                self.finishTime[newTransaction[1]] = newTransaction[0]
                isValid = self.validate(newTransaction[1])
                if(isValid):
                    print(f'Transaction {newTransaction[1]} is valid')
                    for j in self.writeTransactionResource[newTransaction[1]]:
                        print(f'Executing pending write transaction {newTransaction[1]} on resource {j}')
                    print(f'Transaction {newTransaction[1]} is finished')
                else:
                    print(f'Transaction {newTransaction[1]} is aborted')
                    self.abortTransaction(newTransaction[1])
            elif(newTransaction[3] == TypeTransaction.ABORT.value[0]):
                print(f'Transaction {newTransaction[1]} is aborted')
            else:
                print('Invalid Transaction Type')
            