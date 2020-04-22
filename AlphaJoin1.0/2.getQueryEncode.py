import os
from getResource import getResource

querydir = '../resource/jobquery'   
tablenamedir = '../resource/jobtablename'    
shorttolongpath = '../resource/shorttolong'  
predicatesEncodeDictpath = './predicatesEncodedDict'   
queryEncodeDictpath = './queryEncodedDict'  

# Get all the attributes used to select the filter vector
def getQueryAttributions():
    fileList = os.listdir(querydir)
    fileList.sort()
    attr = set()

    for queryName in fileList:
        querypath = querydir + "/" + queryName
        file_object = open(querypath)
        file_context = file_object.readlines()
        file_object.close()

        # find WHERE
        k = -1
        for i in range(len(file_context)):
            k = k + 1
            if file_context[i].find("WHERE") != -1:
                break

        # handle a sentence after WHERE
        for i in range(k, len(file_context)):
            temp = file_context[i].split()
            for word in temp:
                if '.' in word:
                    if word[0] == "'":
                        continue
                    if word[0] == '(':
                        word = word[1:]
                    if word[-1] == ';':
                        word = word[:-1]
                    attr.add(word)

    attrNames = list(attr)
    attrNames.sort()
    return attrNames

def getQueryEncode(attrNames):

    # Read all table abbreviations
    f = open(shorttolongpath, 'r')
    a = f.read()
    short_to_long = eval(a)
    f.close()
    tableNames = []

    for i in short_to_long:
        tableNames.append(i)
    tableNames.sort()

    # Mapping of table name abbreviations and numbers (list subscripts)
    table_to_int = {}
    int_to_table = {}
    for i in range(len(tableNames)):
        int_to_table[i] = tableNames[i]
        table_to_int[tableNames[i]] = i

    # Mapping of attributes and numbers (list subscripts)
    attr_to_int = {}
    int_to_attr = {}
    for i in range(len(attrNames)):
        int_to_attr[i] = attrNames[i]
        attr_to_int[attrNames[i]] = i
    # print(table_to_int)


    queryEncodeDict = {}
    joinEncodeDict = {}
    predicatesEncodeDict = {}
    fileList = os.listdir(querydir)
    fileList.sort()

    for queryName in fileList:
        joinEncode = [0 for _ in range(len(tableNames)*len(tableNames))]
        predicatesEncode = [0 for _ in range(len(attrNames))]

        # Read query statement
        querypath = querydir + "/" + queryName
        file_object = open(querypath)
        file_context = file_object.readlines()
        file_object.close()

        # find WHERE
        k = -1
        for i in range(len(file_context)):
            k = k + 1
            if file_context[i].find("WHERE") != -1:
                break

        # handle a sentence after WHERE
        for i in range(k, len(file_context)):
            temp = file_context[i].split()

            if "=" in temp:
                index = temp.index("=")
                if (filter(temp[index - 1]) in attrNames) & (filter(temp[index + 1]) in attrNames):
                    table1 = temp[index - 1].split('.')[0]
                    table2 = temp[index + 1].split('.')[0]
                    joinEncode[table_to_int[table1] * len(tableNames) + table_to_int[table2]] = 1
                    joinEncode[table_to_int[table2] * len(tableNames) + table_to_int[table1]] = 1
                else:
                    for word in temp:
                        if '.' in word:
                            if word[0] == "'":
                                continue
                            if word[0] == '(':
                                word = word[1:]
                            if word[-1] == ';':
                                word = word[:-1]
                            predicatesEncode[attr_to_int[word]] = 1
            else:
                for word in temp:
                    if '.' in word:
                        if word[0] == "'":
                            continue
                        if word[0] == '(':
                            word = word[1:]
                        if word[-1] == ';':
                            word = word[:-1]
                        predicatesEncode[attr_to_int[word]] = 1
        predicatesEncodeDict[queryName[:-4]] = predicatesEncode
        queryEncodeDict[queryName[:-4]] = joinEncode + predicatesEncode

    for i in queryEncodeDict.items():
        print(i)
    print(len(tableNames), tableNames)
    print(len(attrNames), attrNames)

    f = open(predicatesEncodeDictpath, 'w')
    f.write(str(predicatesEncodeDict))
    f.close()
    f = open(queryEncodeDictpath, 'w')
    f.write(str(queryEncodeDict))
    f.close()


def filter(word):
    if '.' in word:
        if word[0] == '(':
            word = word[1:]
        if word[-1] == ';':
            word = word[:-1]
    return word


if __name__ == '__main__':
    getResource()
    attrNames = getQueryAttributions()
    getQueryEncode(attrNames)




