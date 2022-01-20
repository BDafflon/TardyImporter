class DataStructure:
    def __init__(self):
        self.fileName=""
        self.start=0
        self.end=0
        self.date=None
        self.nbChannel=2
        self.rate=2000
        self.storeType=""
        self.data=[]

    def str(self):
        return ""+self.fileName+": "+str(len(self.data))+'datas'