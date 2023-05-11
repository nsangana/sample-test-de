# Databricks notebook source
# MAGIC %md
# MAGIC # RDKit pandas support
# MAGIC This is a quick tutorial will show some of the ways you can use RDKit together with pandas.

# COMMAND ----------

# MAGIC %md
# MAGIC @TAGS: #basics #pandas

# COMMAND ----------

from __future__ import print_function
%matplotlib inline

# COMMAND ----------

import pandas as pd
from rdkit import Chem
from rdkit.Chem import PandasTools
from rdkit.Chem.Draw import IPythonConsole

# The next line is commented out 
# because GitHub does not render svg's embedded in notebooks
IPythonConsole.ipython_useSVG=False

# COMMAND ----------

# MAGIC %md
# MAGIC Load the table of drugs (downloaded from [ChEMBL](https://www.ebi.ac.uk/chembl/) )

# COMMAND ----------

df = pd.read_csv('../data/chembl_drugs.txt.gz', sep='\t')

# COMMAND ----------

[str(x) for x in df.columns]

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Keep only compounds with SMILES,that respect Ro5 and are on the market

# COMMAND ----------

df = df[df['CANONICAL_SMILES'].notnull() & # Keep cpds with SMILES
        (df['RULE_OF_FIVE'] == 'Y') & # that respect Ro5
        (df['DEVELOPMENT_PHASE'] == 4)] # are on the market

# COMMAND ----------

len(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Table contains CANONICAL_SMILES with SMILES which we can convert to RDKit molecules (default name ROMol)

# COMMAND ----------

PandasTools.AddMoleculeColumnToFrame(df, smilesCol='CANONICAL_SMILES')

# COMMAND ----------

# MAGIC %md
# MAGIC Remove rows where RDKit failed to generate a molecule from SMILES

# COMMAND ----------

df = df[~df['ROMol'].isnull()]

# COMMAND ----------

# MAGIC %md
# MAGIC Extract a name from SYNONYMS column by applying a row-wise operation

# COMMAND ----------

df['name'] = df.apply(lambda x: x['SYNONYMS'].split('(')[0] if type(x['SYNONYMS']) is str else None, axis=1)

# COMMAND ----------

# MAGIC %md
# MAGIC Depict first 8 mols

# COMMAND ----------

PandasTools.FrameToGridImage(df.head(8), legendsCol='name', molsPerRow=4)

# COMMAND ----------

# MAGIC %md
# MAGIC Calculate some descriptors and visualize distributions

# COMMAND ----------

from rdkit.Chem import Descriptors

# COMMAND ----------

df['MW'] = df['ROMol'].map(Descriptors.MolWt)
df['logP'] = df['ROMol'].map(Descriptors.MolLogP)

# COMMAND ----------

#doctest: IGNORE
df['MW'].hist();

# COMMAND ----------

#doctest: IGNORE
df['logP'].hist();

# COMMAND ----------

# MAGIC %md
# MAGIC Do a substructure search on a dataframe

# COMMAND ----------

query = Chem.MolFromSmarts('Nc1ccc(S(=O)(=O)-[*])cc1')
query

# COMMAND ----------

# MAGIC %md
# MAGIC Notice that unspecified bonds in the SMARTS are "single or aromatic" queries. We could make this query a bit more specific:

# COMMAND ----------

query = Chem.MolFromSmarts('N-c1ccc(-S(=O)(=O)-[*])cc1')
query

# COMMAND ----------

# MAGIC %md
# MAGIC We do a substruture search by using the operator `>=` on a molecule column.
# MAGIC
# MAGIC Here's an example where we do the search and count the number of matching rows:

# COMMAND ----------

len(df[df['ROMol'] >= query])

# COMMAND ----------

# MAGIC %md
# MAGIC RDKit by defaults highlights the matched substructures

# COMMAND ----------

df[df['ROMol'] >= query][['SYNONYMS', 'ROMol']].head()

# COMMAND ----------

# MAGIC %md
# MAGIC Just display the matching molecules:

# COMMAND ----------

PandasTools.FrameToGridImage(df[df['ROMol'] >= query].head(12), legendsCol='name', molsPerRow=4)

# COMMAND ----------

# MAGIC %md
# MAGIC Save the table as SD file

# COMMAND ----------

PandasTools.WriteSDF(df, '../data/approved_drugs.sdf', idName='CHEMBL_ID', properties=df.columns)

# COMMAND ----------

df.dtypes

# COMMAND ----------

from rdkit.Chem.rdchem import Mol

# COMMAND ----------

Mol

# COMMAND ----------

df.ROMol

# COMMAND ----------

to_save = spark.createDataFrame(df)

# COMMAND ----------

to_save.display()

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /

# COMMAND ----------

to_save.drop("ROMol").write.saveAsTable("uc_demos_seifeddine_saafi.dev.rdkit")

# COMMAND ----------

# MAGIC %md
# MAGIC Tutorial author: Samo Turk, Jan. 2017
