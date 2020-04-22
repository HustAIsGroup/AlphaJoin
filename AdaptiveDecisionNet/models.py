import torch
import torch.nn as nn
import torch.nn.functional as F


class ValueNet(nn.Module):
    def __init__(self, in_dim, out_dim):
        super(ValueNet, self).__init__()
        self.dim = in_dim
        self.layer1 = nn.Sequential(nn.Linear(in_dim, 2048), nn.ReLU(True))
        self.layer2 = nn.Sequential(nn.Linear(2048, 512), nn.ReLU(True))
        self.layer3 = nn.Sequential(nn.Linear(512, 128), nn.ReLU(True))
        self.layer4 = nn.Sequential(nn.Linear(128, 32), nn.ReLU(True))
        self.layer5 = nn.Sequential(nn.Linear(32, out_dim), nn.Softmax(dim = 0))

    def forward(self, x):
        # x = x.reshape(-1, self.dim)
        x = self.layer1(x)
        # x = nn.functional.dropout(x, p=0.5, training=self.training)
        x = self.layer2(x)
        # x = nn.functional.dropout(x, p=0.5, training=self.training)
        x = self.layer3(x)
        # x = nn.functional.dropout(x, p=0.5, training=self.training)
        x = self.layer4(x)
        # x = nn.functional.dropout(x, p=0.5, training=self.training)
        x = self.layer5(x)
        return x

