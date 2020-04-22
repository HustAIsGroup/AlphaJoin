# coding=utf-8

from datetime import datetime
import time
import random
import os
import numpy as np
import torch
import pickle
from models import ValueNet

shortToLongPath = '../resource/shorttolong'
queryEncodedDictPath = './queryEncodedDict'  


class data:
    def __init__(self, queryname, state, origintime, qpopttime, label):
        self.queryname = queryname
        self.state = state
        self.origintime = origintime
        self.qpopttime = qpopttime
        self.label = label


class supervised:
    def __init__(self, args):
        # Read dict predicatesEncoded 
        f = open(queryEncodedDictPath, 'r')
        a = f.read()
        self.queryEncodedDict = eval(a)
        f.close()

        # Read all tablenames and get tablename-number mapping
        tables = []
        f = open(shortToLongPath, 'r')
        a = f.read()
        short_to_long = eval(a)
        f.close()
        for i in short_to_long.keys():
            tables.append(i)
        tables.sort()
        self.table_to_int = {}
        for i in range(len(tables)):
            self.table_to_int[tables[i]] = i
        
        # The dimension of the network input vector
        self.num_inputs = len(tables) * len(tables) + len(self.predicatesEncodeDict["1a"])
        # The dimension of the vector output by the network
        self.num_output = 2  
        self.args = args

        # build up the network
        self.actor_net = ValueNet(self.num_inputs, self.num_output)
        self.actor_net.apply(self.init_weights)
        # check some dir
        if not os.path.exists(self.args.save_dir):
            os.mkdir(self.args.save_dir)

        self.datasetnumber = 4 
        self.trainList = []
        self.testList = []

    # Divide training set and test set
    def pretreatment(self, path):
        print("Pretreatment running...")
        start = time.clock()
        # Load data uniformly and randomly select for training
        file_test = open(path)
        line = file_test.readline()
        dataList = []
        while line:
            queryName = line.split(",")[0].encode('utf-8').decode('utf-8-sig').strip()
            state = self.queryEncodedDict[queryName]
            origintime = int(float(line.split(",")[1].strip()))
            qpopttime = int(float(line.split(",")[2].strip()))
            label = int(line.split(",")[3].strip())
            temp = data(queryName, state, origintime, qpopttime, label)
            dataList.append(temp)
            line = file_test.readline()

        random.shuffle(dataList)
        listtemp = []
        for i in range(self.datasetnumber):
            temptemp = []
            listtemp.append(temptemp)
        for i in range(dataList.__len__()):
            listtemp[i % listtemp.__len__()].append(dataList[i])
        for i in range(listtemp.__len__()):
            filepath = "./data/data" + str(i) + ".sql"
            file = open(filepath, 'wb')
            pickle.dump(len(listtemp[i]), file)
            for value in listtemp[i]:
                pickle.dump(value, file)
            file.close()

        elapsed = (time.clock() - start)
        print("Pretreatment time used:", elapsed)

    # train 
    def supervised(self):
        # model_path = self.args.save_dir + 'supervised.pt'
        # self.actor_net.load_state_dict(torch.load(model_path, map_location=lambda storage, loc: storage))
        # self.actor_net.eval()
        self.load_data()

        optim = torch.optim.SGD(self.actor_net.parameters(), lr=0.0005)
        # loss_func = torch.nn.MSELoss()
        loss_func = torch.nn.NLLLoss()
        loss1000 = 0
        count = 0

        # starttime = datetime.now()
        for step in range(1, 300001):
            index = random.randint(0, len(self.trainList) - 1)
            state = self.trainList[index].state
            state_tensor = torch.tensor(state, dtype=torch.float32)

            predictionRuntime = torch.log(self.value_net(state_tensor) + 1e-10)
            predictionRuntime = predictionRuntime.view(1,-1)
            
            label = []
            label.append(self.dataList[index].label)
            label_tensor = torch.tensor(label)

            loss = loss_func(predictionRuntime, label_tensor)
            optim.zero_grad()  
            loss.backward()  
            optim.step()  
            loss1000 += loss.item()
            if step % 1000 == 0:
                print('[{}]  Epoch: {}, Loss: {:.5f}'.format(datetime.now(), step, loss1000))
                loss1000 = 0
            # if step % 2000000 == 0:
            #     torch.save(self.actor_net.state_dict(), self.args.save_dir + 'supervised.pt')
            #     self.test_network()
        torch.save(self.actor_net.state_dict(), self.args.save_dir + 'supervised.pt')

    # functions to test the network
    def test_network(self):
        self.load_data()
        model_path = self.args.save_dir + 'supervised.pt'
        self.actor_net.load_state_dict(torch.load(model_path, map_location=lambda storage, loc: storage))
        self.actor_net.eval()

        # testset
        correct = 0
        for step in range(self.testList.__len__()):
            state = self.testList[step].state
            state_tensor = torch.tensor(state, dtype=torch.float32)

            prediction = self.actor_net(state_tensor).detach().cpu().numpy()
            maxindex = np.argmax(prediction)
            print(self.testList[step].queryname, ",", self.testList[step].origintime, ",",
                  self.testList[step].qpopttime, ",",
                  self.testList[step].label, ",", maxindex)
            if maxindex == self.testList[step].label:
                correct += 1
        print("testset：", correct, "\t", self.testList.__len__())

        # trainset
        correct1 = 0
        for step in range(self.trainList.__len__()):
            state = self.trainList[step].state
            state_tensor = torch.tensor(state, dtype=torch.float32)

            predictionRuntime = self.actor_net(state_tensor)
            prediction = predictionRuntime.detach().cpu().numpy()
            maxindex = np.argmax(prediction)
            label = self.trainList[step].label
            # print(self.trainList[step].queryname.strip(), "\t", label, "\t", maxindex)
            if maxindex == label:
                correct1 += 1
        print("trainset", correct1, "\t", self.trainList.__len__())

    def load_data(self, testnum=0):
        if self.trainList.__len__() != 0:
            return
        testpath = "./data/data" + str(testnum) + ".sql"
        file_test = open(testpath, 'rb')
        l = pickle.load(file_test)
        for _ in range(l):
            self.testList.append(pickle.load(file_test))
        file_test.close()

        for i in range(self.datasetnumber):
            if i == testnum:
                continue
            trainpath = "./data/data" + str(i) + ".sql"
            file_train = open(trainpath, 'rb')
            l = pickle.load(file_train)
            for _ in range(l):
                self.trainList.append(pickle.load(file_train))
            file_train.close()
        print("load data\ttrainSet:", self.trainList.__len__(), " testSet:", self.testList.__len__())

    def init_weights(self, m):
        if type(m) == torch.nn.Linear:
            torch.nn.init.xavier_uniform_(m.weight)
            m.bias.data.fill_(0.01)
