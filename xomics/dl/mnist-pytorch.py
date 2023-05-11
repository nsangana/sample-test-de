# Databricks notebook source
# MAGIC %md # Distributed deep learning training using PyTorch with HorovodRunner for MNIST
# MAGIC
# MAGIC This notebook illustrates the use of HorovodRunner for distributed training using PyTorch. 
# MAGIC It first shows how to train a model on a single node, and then shows how to adapt the code using HorovodRunner for distributed training. 
# MAGIC The notebook runs on CPU and GPU clusters.
# MAGIC
# MAGIC ## Requirements
# MAGIC Databricks Runtime 7.0 ML or above.  
# MAGIC HorovodRunner is designed to improve model training performance on clusters with multiple workers, but multiple workers are not required to run this notebook.

# COMMAND ----------

# MAGIC %md ## Set up checkpoint location
# MAGIC The next cell creates a directory for saved checkpoint models. Databricks recommends saving training data under `dbfs:/ml`, which maps to `file:/dbfs/ml` on driver and worker nodes.

# COMMAND ----------

PYTORCH_DIR = '/dbfs/ml/horovod_pytorch'

# COMMAND ----------

# MAGIC %md ## Prepare single-node code
# MAGIC
# MAGIC First, create single-node PyTorch code. This is modified from the [Horovod PyTorch MNIST Example](https://github.com/horovod/horovod/blob/master/examples/pytorch/pytorch_mnist.py).

# COMMAND ----------

# MAGIC %md ### Define a simple convolutional network

# COMMAND ----------

import torch
import torch.nn as nn
import torch.nn.functional as F

class Net(nn.Module):
    def __init__(self):
        super(Net, self).__init__()
        self.conv1 = nn.Conv2d(1, 10, kernel_size=5)
        self.conv2 = nn.Conv2d(10, 20, kernel_size=5)
        self.conv2_drop = nn.Dropout2d()
        self.fc1 = nn.Linear(320, 50)
        self.fc2 = nn.Linear(50, 10)

    def forward(self, x):
        x = F.relu(F.max_pool2d(self.conv1(x), 2))
        x = F.relu(F.max_pool2d(self.conv2_drop(self.conv2(x)), 2))
        x = x.view(-1, 320)
        x = F.relu(self.fc1(x))
        x = F.dropout(x, training=self.training)
        x = self.fc2(x)
        return F.log_softmax(x)

# COMMAND ----------

# MAGIC %md ###Configure single-node training

# COMMAND ----------

# Specify training parameters
batch_size = 100
num_epochs = 3
momentum = 0.5
log_interval = 100

# COMMAND ----------

def train_one_epoch(model, device, data_loader, optimizer, epoch):
    model.train()
    for batch_idx, (data, target) in enumerate(data_loader):
        data, target = data.to(device), target.to(device)
        optimizer.zero_grad()
        output = model(data)
        loss = F.nll_loss(output, target)
        loss.backward()
        optimizer.step()
        if batch_idx % log_interval == 0:
            print('Train Epoch: {} [{}/{} ({:.0f}%)]\tLoss: {:.6f}'.format(
                epoch, batch_idx * len(data), len(data_loader) * len(data),
                100. * batch_idx / len(data_loader), loss.item()))

# COMMAND ----------

# MAGIC %md ### Create methods for saving and loading model checkpoints

# COMMAND ----------

def save_checkpoint(log_dir, model, optimizer, epoch):
  filepath = log_dir + '/checkpoint-{epoch}.pth.tar'.format(epoch=epoch)
  state = {
    'model': model.state_dict(),
    'optimizer': optimizer.state_dict(),
  }
  torch.save(state, filepath)
  
def load_checkpoint(log_dir, epoch=num_epochs):
  filepath = log_dir + '/checkpoint-{epoch}.pth.tar'.format(epoch=epoch)
  return torch.load(filepath)

def create_log_dir():
  log_dir = os.path.join(PYTORCH_DIR, str(time()), 'MNISTDemo')
  os.makedirs(log_dir)
  return log_dir

# COMMAND ----------

# MAGIC %md ### Run single-node training with PyTorch

# COMMAND ----------

import torch.optim as optim
from torchvision import datasets, transforms
from time import time
import os

single_node_log_dir = create_log_dir()
print("Log directory:", single_node_log_dir)

def train(learning_rate):
  device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

  train_dataset = datasets.MNIST(
    'data', 
    train=True,
    download=True,
    transform=transforms.Compose([transforms.ToTensor(), transforms.Normalize((0.1307,), (0.3081,))]))
  data_loader = torch.utils.data.DataLoader(train_dataset, batch_size=batch_size, shuffle=True)

  model = Net().to(device)

  optimizer = optim.SGD(model.parameters(), lr=learning_rate, momentum=momentum)

  for epoch in range(1, num_epochs + 1):
    train_one_epoch(model, device, data_loader, optimizer, epoch)
    save_checkpoint(single_node_log_dir, model, optimizer, epoch)

    
def test(log_dir):
  device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
  loaded_model = Net().to(device)
  
  checkpoint = load_checkpoint(log_dir)
  loaded_model.load_state_dict(checkpoint['model'])
  loaded_model.eval()

  test_dataset = datasets.MNIST(
    'data', 
    train=False,
    download=True,
    transform=transforms.Compose([transforms.ToTensor(), transforms.Normalize((0.1307,), (0.3081,))]))
  data_loader = torch.utils.data.DataLoader(test_dataset)

  test_loss = 0
  for data, target in data_loader:
      data, target = data.to(device), target.to(device)
      output = loaded_model(data)
      test_loss += F.nll_loss(output, target)
  
  test_loss /= len(data_loader.dataset)
  print("Average test loss: {}".format(test_loss.item()))

# COMMAND ----------

# MAGIC %md
# MAGIC Run the `train` function you just created to train a model on the driver node.  

# COMMAND ----------

train(learning_rate = 0.001)

# COMMAND ----------

# MAGIC %md Load and use the model

# COMMAND ----------

test(single_node_log_dir)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Migrate to HorovodRunner
# MAGIC
# MAGIC HorovodRunner takes a Python method that contains deep learning training code with Horovod hooks. HorovodRunner pickles the method on the driver and distributes it to Spark workers. A Horovod MPI job is embedded as a Spark job using barrier execution mode.

# COMMAND ----------

import horovod.torch as hvd
from sparkdl import HorovodRunner

# COMMAND ----------

hvd_log_dir = create_log_dir()
print("Log directory:", hvd_log_dir)

def train_hvd(learning_rate):
  
  # Initialize Horovod
  hvd.init()  
  device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
  
  if device.type == 'cuda':
    # Pin GPU to local rank
    torch.cuda.set_device(hvd.local_rank())

  train_dataset = datasets.MNIST(
    # Use different root directory for each worker to avoid conflicts
    root='data-%d'% hvd.rank(),  
    train=True, 
    download=True,
    transform=transforms.Compose([transforms.ToTensor(), transforms.Normalize((0.1307,), (0.3081,))])
  )

  from torch.utils.data.distributed import DistributedSampler
  
  # Configure the sampler so that each worker gets a distinct sample of the input dataset
  train_sampler = DistributedSampler(train_dataset, num_replicas=hvd.size(), rank=hvd.rank())
  # Use train_sampler to load a different sample of data on each worker
  train_loader = torch.utils.data.DataLoader(train_dataset, batch_size=batch_size, sampler=train_sampler)

  model = Net().to(device)
  
  # The effective batch size in synchronous distributed training is scaled by the number of workers
  # Increase learning_rate to compensate for the increased batch size
  optimizer = optim.SGD(model.parameters(), lr=learning_rate * hvd.size(), momentum=momentum)

  # Wrap the local optimizer with hvd.DistributedOptimizer so that Horovod handles the distributed optimization
  optimizer = hvd.DistributedOptimizer(optimizer, named_parameters=model.named_parameters())
  
  # Broadcast initial parameters so all workers start with the same parameters
  hvd.broadcast_parameters(model.state_dict(), root_rank=0)

  for epoch in range(1, num_epochs + 1):
    train_one_epoch(model, device, train_loader, optimizer, epoch)
    # Save checkpoints only on worker 0 to prevent conflicts between workers
    if hvd.rank() == 0:
      save_checkpoint(hvd_log_dir, model, optimizer, epoch)

# COMMAND ----------

# MAGIC %md
# MAGIC Now that you have defined a training function with Horovod,  you can use HorovodRunner to distribute the work of training the model. 
# MAGIC
# MAGIC The HorovodRunner parameter `np` sets the number of processes. This example uses a cluster with two workers, each with a single GPU, so set `np=2`. (If you use `np=-1`, HorovodRunner trains using a single process on the driver node.)

# COMMAND ----------

hr = HorovodRunner(np=2) 
hr.run(train_hvd, learning_rate = 0.001)

# COMMAND ----------

test(hvd_log_dir)

# COMMAND ----------

# MAGIC %md 
# MAGIC Under the hood, HorovodRunner takes a Python method that contains deep learning training code with Horovod hooks. HorovodRunner pickles the method on the driver and distributes it to Spark workers. A Horovod MPI job is embedded as a Spark job using the barrier execution mode. The first executor collects the IP addresses of all task executors using BarrierTaskContext and triggers a Horovod job using `mpirun`. Each Python MPI process loads the pickled user program, deserializes it, and runs it.
# MAGIC
# MAGIC For more information, see [HorovodRunner API documentation](https://databricks.github.io/spark-deep-learning/#api-documentation). 
