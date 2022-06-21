#!/usr/bin/env python
# coding: utf-8

# In[39]:


get_ipython().system(' pip install tmdbsimple')
import pandas as pd
import time
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from tqdm.notebook import tqdm_notebook


# In[12]:


import json
with open ('/Users/Bijan Emadi/.secret/tmdb_api.json', 'r') as f:
    login = json.load(f)
login.keys()


# In[13]:


import tmdbsimple as tmdb
tmdb.API_KEY = login['api-key']


# In[14]:


movie = tmdb.Movies(603)
info = movie.info()
info


# In[15]:


def get_movie_with_rating(movie_id):
    # Get the movie object for the current id
    movie = tmdb.Movies(movie_id)
    # save the .info .releases dictionaries
    info = movie.info()
    releases = movie.releases()
    # Loop through countries in releases
    for c in releases['countries']:
        # if the country abbreviation==US
        if c['iso_3166_1' ] =='US':
            ## save a "certification" key in the info dict with the certification
            info['certification'] = c['certification']
    return info


# In[16]:


def write_json(new_data, filename):
    
    with open(filename, 'r+') as file:
        file_data = json.load(file)
        if (type(new_data)==list) & (type(file_data)==list):
            file_data.extend(new_data)
        else:
            file_data.append(new_data)
        file.seek(0)
        json.dump(file_data, file)


# In[17]:


get_movie_with_rating(603)


# In[18]:


import os
FOLDER = "Data/"
os.makedirs(FOLDER, exist_ok=True)
os.listdir(FOLDER)


# In[19]:


basics = pd.read_csv('Data/title_basics.csv.gz')
basics.head()


# In[20]:


basics.info()


# In[21]:


YEARS_TO_GET = [2000,2001]


# In[22]:


YEAR = YEARS_TO_GET[0]
YEAR


# In[ ]:


# Start of OUTER Loop
for YEAR in tqdm_notebook(YEARS_TO_GET, desc='YEARS',
                         position=0):
    # Defining the JSON file to store results for year
    JSON_FILE = f'{FOLDER}tmdb_api_results_{YEAR}.json'
    # Check if file exists
    file_exists = os.path.isfile(JSON_FILE)

    if file_exists == False:
        with open(JSON_FILE,'w') as f:
            json.dump([{'imdb_id':0}],f)

    # Saving new year as the current df
    df = basics.loc[basics['startYear']==YEAR].copy()
    # Saving movie IDs to list
    movie_ids = df['tconst'].copy()
    movie_ids

    # Load existing data from json into a dataframe called 'previous_df'
    previous_df = pd.read_json(JSON_FILE)
    previous_df

    movie_ids_to_get = movie_ids[~movie_ids.isin(previous_df['imdb_id'])]

    #Get index and movie id from list
    # INNER Loop
    for movie_id in tqdm_notebook(movie_ids_to_get,
                                 desc=f'Movies from {YEAR}',
                                 position=1,
                                 leave=True):
        # Attempt to retrieve then data for the movie id
        try:
            temp = get_movie_with_rating(movie_id) # This uses your pre-made function
            # Append/extend results to existing file using a pre-made function
            write_json(temp, JSON_FILE)
            # Short 20 ms sleep to prevent overwhelming server
            time.sleep(0.02)

        except Exception as e:
            continue
    try:
        
            
        final_year_df = pd.read_json(JSON_FILE)
        final_year_df.to_csv(f"{FOLDER}final_tmdb_data_{YEAR}.csv.gz", compression="gzip", index=False)
        
    except:
        print(f'Error loading final {JSON_FILE}')


# In[23]:


Movies_2000 = pd.read_csv(r'Data/final_tmdb_data_2000.csv.gz')
Movies_2001 = pd.read_csv(r'Data/final_tmdb_data_2001.csv.gz')


# In[24]:


Movies_2000.head()


# In[25]:


Movies_2001.head()


# In[26]:


Movies_2000['Year'] = 2000


# In[27]:


Movies_2001['Year'] = 2001


# In[28]:


df_merge=pd.concat([Movies_2000, Movies_2001], axis=0)


# In[29]:


df_merge.info()


# Exploratory Data Analysis
# 
# 1. How many movies had at least some valid financial information? (Values > 0 for budget OR revenue?)

# In[30]:


df_merge = df_merge.dropna(subset=['budget','revenue'])


# In[32]:


df_money = df_merge[(df_merge['budget'] > 0) | (df_merge['revenue'] > 0)]


# In[36]:


df_money.head()


# 2. How many movies are there in each of the certification categories?

# In[50]:


chart=sns.countplot(x=df_money['certification'])
chart.set_title('Number of Films per Certification Category')


# 3. What is the average revenue per certification category?

# In[46]:


chart=sns.barplot(x=df_money['certification'], y=df_money['revenue'])
chart.set_title('Average Revenue per Certfication');


# 4. What is the average budget per certification category?

# In[48]:


chart=sns.barplot(x=df_money['certification'], y=df_money['budget'])
chart.set_title('Average Budget per Certification');


# In[ ]:




