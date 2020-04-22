# The original table name is sequentially transferred to the corresponding queryName file
import numpy as np

querydir = '../resource/jobquery'  # JOB query
tablenamedir = '../resource/jobtablename'  # tablename involved in the query statement
shorttolongpath = '../resource/shorttolong'  # Mapping of table abbreviations to full names

def getResource():

    short_to_long = {}
    fileList = os.listdir(querydir)
    fileList.sort()
    for queryName in fileList:
        # queryName = "9d.sql"
        querypath = querydir + "/" + queryName
        file_object = open(querypath)
        file_context = file_object.readlines()
        # print(file_context)
        file_object.close()

        j = 0
        k = 0
        tablenames = []
        for i in range(len(file_context)):
            if file_context[i].find("FROM") != -1:
                break
            j = j + 1
        for i in range(len(file_context)):
            if file_context[i].find("WHERE") != -1:
                break
            k = k + 1

        # The original table name is sequentially transferred to the corresponding queryName file, update tablename
        for i in range(j, k - 1):
            temp = file_context[i].split()
            tablenames.append(temp[temp.index("AS") + 1][:-1].lower())
        temp = file_context[k - 1].split()
        tablenames.append(temp[temp.index("AS") + 1].lower())

        f = open(tablenamedir + "/" + queryName[:-4], 'w')
        f.write(str(tablenames))
        f.close()
        # print(queryName, tablenames)

        # Read query
        querypath = querydir + "/" + queryName
        file_object = open(querypath)
        file_context = file_object.read()
        file_object.close()

        scan_language = []
        for line in rows:
            if line[0].find('Scan') != -1 & line[0].find('Bitmap Index') == -1:
                scan_language.append(line[0])
        for language in scan_language:
            word = language.split(' ')
            index = word.index('on')
            short_to_long[word[index + 2]] = word[index + 1]

    print(len(short_to_long))

    # Dump two dict to corresponding files
    f = open(shorttolongpath, 'w')
    f.write(str(short_to_long))
    f.close()

    cur.close()
    conn.close()


if __name__ == '__main__':
    getResource()