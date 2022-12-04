class Parser:
    def __init__(self,pathFile):
        self.arr = []
        f = open(pathFile, "r")
        for line in f:
            data = line.replace('\n','').split(' ')
            if(len(data) == 2):
                data.append('')
            self.arr.append(data)
    
    def getArr(self):
        return self.arr.copy()