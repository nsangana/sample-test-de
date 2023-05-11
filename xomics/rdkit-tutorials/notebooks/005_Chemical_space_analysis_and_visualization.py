# Databricks notebook source
# MAGIC %md
# MAGIC # Chemical space analysis of small molecule drugs with RDKit and dimensionality reduction methods
# MAGIC This is a quick tutorial on chemical space analysis and visualization with RDKit and dimensionality reduction methods PCA and t-SNE (sklearn). Due to the high dimensionality of chemical fingerprints, we have to use dimensionality reduction methods in order to facilitate the plotting of chemical space. Two popular methods are:
# MAGIC   - [PCA](http://scikit-learn.org/stable/modules/generated/sklearn.decomposition.PCA.html) - Principal component analysis
# MAGIC   - [t-SNE](http://scikit-learn.org/stable/modules/generated/sklearn.manifold.TSNE.html) - t-Distributed Stochastic Neighbor Embedding
# MAGIC
# MAGIC We will use known drugs and try to see if there are differences between different classes of drugs in the projected chemical space.

# COMMAND ----------

# MAGIC %md
# MAGIC @TAGS: #advanced #sklearn #PCA #t-SNE

# COMMAND ----------

from __future__ import print_function
%matplotlib inline

# COMMAND ----------

import numpy as np
import pandas as pd
from rdkit import Chem
from rdkit.Chem import PandasTools
from rdkit.Chem.Draw import IPythonConsole

# COMMAND ----------

# MAGIC %md
# MAGIC Load the table of drugs (downloaded from [ChEMBL](https://www.ebi.ac.uk/chembl/) )

# COMMAND ----------

df = pd.read_csv('../data/chembl_drugs.txt.gz', sep='\t')

# COMMAND ----------

len(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Keep only compounds with SMILES, [USAN stems](https://www.ama-assn.org/about-us/united-states-adopted-names-approved-stems), that respect [Ro5](https://en.wikipedia.org/wiki/Lipinski's_rule_of_five) and are on the market

# COMMAND ----------

df = df[df['CANONICAL_SMILES'].notnull() & # Keep cpds with SMILES
        df['USAN_STEM'].notnull() & # USAN stem
        (df['RULE_OF_FIVE'] == 'Y') & # that respect Ro5
        (df['DEVELOPMENT_PHASE'] == 4)] # are on the market

# COMMAND ----------

len(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Add molecule column

# COMMAND ----------

PandasTools.AddMoleculeColumnToFrame(df, smilesCol='CANONICAL_SMILES')

# COMMAND ----------

df = df[~df['ROMol'].isnull()]

# COMMAND ----------

len(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Check most common compound classees (based on USAN stem)

# COMMAND ----------

common_stems = df.groupby('USAN_STEM').size().sort_values()[-10:]
common_stems

# COMMAND ----------

stems = df.drop_duplicates(['USAN_STEM'])[df.drop_duplicates(['USAN_STEM'])['USAN_STEM'].isin(common_stems.index)][['USAN_STEM','USAN_STEM_DEFINITION']]
stems.index = stems['USAN_STEM']
stems['count'] = common_stems

# COMMAND ----------

stems

# COMMAND ----------

# MAGIC %md
# MAGIC Helper functions

# COMMAND ----------

from rdkit import DataStructs
from rdkit.Chem import AllChem

class FP:
    """
    Molecular fingerprint class, useful to pack features in pandas df
    
    Parameters  
    ---------- 
    fp : np.array
        Features stored in numpy array
    names : list, np.array
        Names of the features
    """
    def __init__(self, fp, names):
        self.fp = fp
        self.names = names
    def __str__(self):
        return "%d bit FP" % len(self.fp)
    def __len__(self):
        return len(self.fp)

def get_cfps(mol, radius=2, nBits=1024, useFeatures=False, counts=False, dtype=np.float32):
    """Calculates circular (Morgan) fingerprint.  
    http://rdkit.org/docs/GettingStartedInPython.html#morgan-fingerprints-circular-fingerprints  
    
    Parameters
    ----------
    mol : rdkit.Chem.rdchem.Mol
    radius : float 
        Fingerprint radius, default 2
    nBits : int 
        Length of hashed fingerprint (without descriptors), default 1024
    useFeatures : bool  
        To get feature fingerprints (FCFP) instead of normal ones (ECFP), defaults to False
    counts : bool
        If set to true it returns for each bit number of appearances of each substructure (counts). Defaults to false (fingerprint is binary)
    dtype : np.dtype
        Numpy data type for the array. Defaults to np.float32 because it is the default dtype for scikit-learn
    
    Returns
    -------
    ML.FP
        Fingerprint (feature) object
    """
    arr = np.zeros((1,), dtype)
    
    if counts is True:
        info = {}
        fp = AllChem.GetHashedMorganFingerprint(mol, radius, nBits, useFeatures=useFeatures)
        DataStructs.ConvertToNumpyArray(fp, arr)
    else:
        DataStructs.ConvertToNumpyArray(AllChem.GetMorganFingerprintAsBitVect(mol, radius, nBits=nBits, useFeatures=useFeatures), arr)
    return FP(arr, range(nBits))

# COMMAND ----------

# MAGIC %md
# MAGIC Calculate fingerprints

# COMMAND ----------

df['FP'] = df['ROMol'].map(get_cfps)

# COMMAND ----------

# MAGIC %md
# MAGIC Extract compounds that belong to 10 most common USAN stems

# COMMAND ----------

df_small = df[df['USAN_STEM'].isin(list(stems.index))].copy()

# COMMAND ----------

len(df_small)

# COMMAND ----------

# MAGIC %md
# MAGIC ### PCA analysis

# COMMAND ----------

from sklearn.decomposition import PCA

# COMMAND ----------

X = np.array([x.fp for x in df_small['FP']])

# COMMAND ----------

pca = PCA(n_components=3, random_state=0)
pca_drugs = pca.fit_transform(X)

# COMMAND ----------

df_small['PC1'] = pca_drugs.T[0]
df_small['PC2'] = pca_drugs.T[1]
df_small['PC3'] = pca_drugs.T[2]

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns

# COMMAND ----------

# MAGIC %md
# MAGIC Plot principal components

# COMMAND ----------

#doctest: IGNORE
sns.pairplot(df_small, hue='USAN_STEM', vars=['PC1', 'PC2', 'PC3'], palette='viridis');

# COMMAND ----------

# MAGIC %md
# MAGIC ### t-SNE
# MAGIC Note that [t-SNE is very sensitive to hyperparameter settings](http://distill.pub/2016/misread-tsne/).
# MAGIC #### Without PCA preprocessing

# COMMAND ----------

from sklearn.manifold import TSNE

# COMMAND ----------

model = TSNE(n_components=2, random_state=0, perplexity=30, n_iter=5000)
tsne_drugs = model.fit_transform(X)

# COMMAND ----------

df_small['TSNE_C1'] = tsne_drugs.T[0]
df_small['TSNE_C2'] = tsne_drugs.T[1]

# COMMAND ----------

#doctest: IGNORE
sns.pairplot(df_small, hue='USAN_STEM', vars=['TSNE_C1', 'TSNE_C2'], palette='viridis');

# COMMAND ----------

# MAGIC %md
# MAGIC #### With PCA preprocessing
# MAGIC In case of high dimensionality data (like molecular fingerprints) it is recommended to reduce the number of dimensions with another method before proceeding with t-SNE.

# COMMAND ----------

pca_model = PCA(n_components=30, random_state=0)
tsne_model = TSNE(n_components=2, random_state=0, perplexity=30, n_iter=5000)
tsne_pca_drugs = tsne_model.fit_transform(pca_model.fit_transform(X))

# COMMAND ----------

df_small['TSNE_PCA_C1'] = tsne_pca_drugs.T[0]
df_small['TSNE_PCA_C2'] = tsne_pca_drugs.T[1]

# COMMAND ----------

#doctest: IGNORE
sns.pairplot(df_small, hue='USAN_STEM', vars=['TSNE_PCA_C1', 'TSNE_PCA_C2'], palette='viridis');

# COMMAND ----------

# MAGIC %md
# MAGIC Tutorial author: Samo Turk, Jan. 2017

# COMMAND ----------


