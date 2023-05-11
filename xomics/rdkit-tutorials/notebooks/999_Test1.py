# Databricks notebook source
# MAGIC %md
# MAGIC # Test tutorial
# MAGIC
# MAGIC This isn't really intended to be a useful tutorial, it's there to allow us to excercise the automated testing code.
# MAGIC
# MAGIC @TAGS: #testing #basics 

# COMMAND ----------

from rdkit import Chem
from rdkit.Chem.Draw import IPythonConsole
IPythonConsole.ipython_useSVG=True

# COMMAND ----------

import time
print(time.asctime()) # doctest: IGNORE

# COMMAND ----------

Chem.CanonSmiles("c1ccccc1O")

# COMMAND ----------

Chem.MolFromSmiles('c1ccccc1O')

# COMMAND ----------

print("hello")
# comment
Chem.CanonSmiles("c1ccccc1NC")

# COMMAND ----------

# MAGIC %md
# MAGIC Some nice explanatory text

# COMMAND ----------

frags = Chem.FragmentOnBonds(Chem.MolFromSmiles('CCOC1CC1'),(2,))
frags

# COMMAND ----------

Chem.MolToSmiles(frags,True)

# COMMAND ----------

pieces = Chem.GetMolFrags(frags,asMols=True)
len(pieces)

# COMMAND ----------

[Chem.MolToSmiles(x,True) for x in pieces]

# COMMAND ----------

from rdkit.Chem import AllChem
m = Chem.MolFromSmiles('CCOC1CC1')
m.SetProp("_Name","test molecule")
AllChem.Compute2DCoords(m)

# COMMAND ----------

print(Chem.MolToMolBlock(m))

# COMMAND ----------

# test that for loops work:
atns = []
for at in m.GetAtoms():
    if at.GetAtomicNum()>6:
        atns.append((at.GetIdx(),at.GetAtomicNum()))
atns

# COMMAND ----------

atns = []
for at in m.GetAtoms():
    if at.GetAtomicNum()>6:
        atns.append((at.GetIdx(),at.GetAtomicNum()))
atns

# COMMAND ----------

for at in m.GetAtoms():
    if at.GetAtomicNum()>6:
        atns.append((at.GetIdx(),at.GetAtomicNum()))
if(len(atns)==1):        
    atns

# COMMAND ----------


