from __future__ import division
import os
from copy import deepcopy
from mcts import mcts
import time


queryEncodeDictPath = './queryEncodedDict' 
predicatesEncodeDictPath = './predicatesEncodedDict'
shorttolongpath = '../resource/shorttolong'  
tablenamedir = '../resource/jobtablename' 
querydir = "../resource/jobquery"

f = open(queryEncodeDictPath, 'r')
a = f.read()
queryEncodeDict = eval(a)
f.close()

f = open(predicatesEncodeDictPath, 'r')
a = f.read()
predicatesEncodeDict = eval(a)
f.close()


# Get all tablenames
tables = []
f = open(shorttolongpath, 'r')
a = f.read()
short_to_long = eval(a)
f.close()
for i in short_to_long.keys():
    tables.append(i)
tables.sort()

# Mapping of tablename abbreviations and numbers (list subscripts)
totalNumberOfTables = len(tables)
tableToInt = {}
intToTable = {}
for i in range(totalNumberOfTables):
    intToTable[i] = tables[i]
    tableToInt[tables[i]] = i


class planState:
    def __init__(self, totalNumberOfTables, numberOfTables, queryEncode, predicatesEncode):
        self.tableNumber = totalNumberOfTables
        self.currentStep = numberOfTables
        self.board = [0 for _ in range(self.tableNumber * self.tableNumber)]
        self.joinMartix = queryEncode[:self.tableNumber * self.tableNumber]
        self.predicatesEncode = predicatesEncode


    def getPossibleActions(self):
        possibleActions = []
        for i in range(self.tableNumber):
            for j in range(self.tableNumber):
                if self.joinMartix[i * self.tableNumber + j] == 1:
                    possibleActions.append(Action(self.currentStep, x=i, y=j))
        return possibleActions

    def takeAction(self, action):
        newState = deepcopy(self)
        newState.currentStep = self.currentStep - 1
        newState.board[action.x * self.tableNumber + action.y] = action.currentStep
        newState.joinMartix[action.x * self.tableNumber + action.y] = 0
        newState.joinMartix[action.y * self.tableNumber + action.x] = 0
        ma = max(action.x, action.y)
        mi = min(action.x, action.y)
        for i in range(self.tableNumber):
            if newState.joinMartix[i * self.tableNumber + ma] == 1:
                newState.joinMartix[i * self.tableNumber + ma] = 0
                newState.joinMartix[i * self.tableNumber + mi] = 1
            if newState.joinMartix[ma * self.tableNumber + i] == 1:
                newState.joinMartix[ma * self.tableNumber + i] = 0
                newState.joinMartix[mi * self.tableNumber + i] = 1
        return newState

    def isTerminal(self):
        if self.currentStep == 1:
            return True
        return False

class Action:
    def __init__(self, step, x, y):
        self.currentStep = step
        self.x = x
        self.y = y

    def __str__(self):
        return str((self.x, self.y))

    def __repr__(self):
        return str(self)

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self.x == other.x and self.y == other.y and self.currentStep == other.currentStep

    def __hash__(self):
        return hash((self.x, self.y, self.currentStep))

def decode(currentState, tableList):
    tempdect = {}
    for i in range(len(tableList)):
        tempdect[tableList[i]] = tableList[i]
    # Record the number of connections
    correctcount = 0  
    while correctcount != len(tableList) - 1:
        index = currentState.board.index(max(currentState.board))  
        indexa = int(index / currentState.tableNumber)
        indexb = int(index % currentState.tableNumber)
        currentState.board[index] = 0

        string = "( " + tempdect[intToTable[indexa]] + " " + tempdect[intToTable[indexb]] + " )"
        correctcount += 1
        # Update dictionary
        for j in string.split():
            if j in tableList:
                tempdect[j] = string

    return tempdect[tableList[0]]


def findBestPlan():
    queryNameList = os.listdir(tablenamedir)
    queryNameList.sort()
    searchFactor = 15
    for queryName in queryNameList:
        # Get the list of queried tables
        tablenamepath = tablenamedir + "/" + queryName
        file_object = open(tablenamepath)
        file_context = file_object.read()
        tableList = eval(file_context)
        file_object.close()

        # Construct the initial state
        initialState = planState(totalNumberOfTables, len(tableList), queryEncodeDict[queryName],
                                predicatesEncodeDict[queryName])
        currentState = initialState

        mct = mcts(iterationLimit=(int)(len(currentState.getPossibleActions()) *  searchFactor))        
        start = time.time()
        while currentState.currentStep != 1:
            # Search for the best choice in the current state
            action = mct.search(initialState=currentState)
            # Apply the selection, the status changes
            currentState = currentState.takeAction(action)
            # Change search times
            mct.searchLimit = (int)(len(currentState.getPossibleActions()) *  searchFactor)
        elapsed = (time.time() - start) * 1000
        # Decode selected results
        hint = decode(currentState, tableList)
        print(queryName, ",", hint, ",%.3f" % elapsed)

if __name__ == '__main__':
    findBestPlan()
