#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np


# In[2]:


basics_url = 'https://datasets.imdbws.com/title.basics.tsv.gz'
akas_url = 'https://datasets.imdbws.com/title.akas.tsv.gz'
ratings_url = 'https://datasets.imdbws.com/title.ratings.tsv.gz'


# In[3]:


basics = pd.read_csv(basics_url, sep='\t', low_memory=False)


# In[4]:


akas = pd.read_csv(akas_url, sep='\t', low_memory=False)
ratings = pd.read_csv(ratings_url, sep='\t', low_memory=False)


# In[5]:


basics.head()


# In[6]:


basics.info()


# In[7]:


# Replace '\N' with np.nan
basics.replace({'\\N':np.nan}, inplace=True)


# In[8]:


basics.dropna(subset = ["runtimeMinutes"], inplace = True)


# In[9]:


basics["runtimeMinutes"].isnull().sum()


# In[10]:


basics.dropna(subset = ["genres"], inplace = True)


# In[11]:


basics["genres"].isnull().sum()


# In[12]:


basics=basics.loc[(basics["titleType"]=="movie")]


# In[13]:


basics=basics.loc[(basics['startYear']>'1999')]


# In[14]:


basics.head()


# In[15]:


min_value=basics['startYear'].min()
min_value


# In[16]:


max_value=basics['startYear'].max()
max_value


# In[17]:


basics=basics.loc[(basics['startYear']<'2022')]


# In[18]:


basics['startYear'].value_counts(dropna=False)


# In[19]:


doc_filter = basics['genres'].str.contains('documentary', case=False)
basics = basics[~doc_filter]


# In[20]:


akas.replace({'\\N':np.nan}, inplace=True)


# In[21]:


akas = akas.loc[(akas['region'] == 'US')]


# In[22]:


akas


# In[23]:


keepers = basics['tconst'].isin(akas['titleId'])
keepers


# In[24]:


basics = basics[keepers]
basics


# In[25]:


ratings.replace({'\\N':np.nan}, inplace=True)


# In[26]:


ratings


# In[27]:


# example making new folder with os
import os
os.makedirs('Data/',exist_ok=True) 
# Confirm folder created
os.listdir("Data/")


# In[37]:


## Save current dataframe to file.
basics.to_csv("Data/title_basics.csv.gz",compression='gzip',index=False)


# In[29]:


# Open saved file and preview again
basics = pd.read_csv("Data/title_basics.csv.gz", low_memory = False)
basics.head()


# In[30]:


akas.to_csv("Data/title_akas.csv.gz", compression='gzip', index=False)


# In[31]:


akas = pd.read_csv('Data/title_akas.csv.gz', low_memory = False)
akas


# In[32]:


ratings.to_csv("Data/title_ratings.csv.gz", index = False)


# In[33]:


ratings = pd.read_csv('Data/title_ratings.csv.gz', low_memory = False)
ratings


# In[34]:


basics.info()


# In[35]:


akas.info()


# In[36]:


ratings.info()

