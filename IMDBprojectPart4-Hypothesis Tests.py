#!/usr/bin/env python
# coding: utf-8

# # **Compiling the Data**

# In[2]:


get_ipython().system(' pip install tmdbsimple')
import pandas as pd
import time
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
from tqdm.notebook import tqdm_notebook


# In[3]:


import json
with open ('/Users/Bijan Emadi/.secret/tmdb_api.json', 'r') as f:
    login = json.load(f)
login.keys()


# In[4]:


import tmdbsimple as tmdb
tmdb.API_KEY = login['api-key']


# In[5]:


movie = tmdb.Movies(603)
info = movie.info()
info


# In[6]:


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


# In[7]:


def write_json(new_data, filename):
    
    with open(filename, 'r+') as file:
        file_data = json.load(file)
        if (type(new_data)==list) & (type(file_data)==list):
            file_data.extend(new_data)
        else:
            file_data.append(new_data)
        file.seek(0)
        json.dump(file_data, file)


# In[8]:


get_movie_with_rating(603)


# In[14]:


import os
FOLDER = "Data/"
os.makedirs(FOLDER, exist_ok=True)
os.listdir(FOLDER)


# In[6]:


basics = pd.read_csv('Data/title_basics.csv.gz')
basics.head()


# In[10]:


basics.info()


# In[11]:


YEARS_TO_GET = [2002,2003,2004,2005,2006,2007,2008,2009]


# In[12]:


YEAR = YEARS_TO_GET[0]
YEAR


# In[15]:


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


# In[7]:


tmdb_2000=pd.read_csv(r'Data/final_tmdb_data_2000.csv.gz')
tmdb_2001=pd.read_csv(r'Data/final_tmdb_data_2001.csv.gz')


# In[8]:


tmdb_2002=pd.read_csv(r'Data/final_tmdb_data_2002.csv.gz')
tmdb_2003=pd.read_csv(r'Data/final_tmdb_data_2003.csv.gz')
tmdb_2004=pd.read_csv(r'Data/final_tmdb_data_2004.csv.gz')
tmdb_2005=pd.read_csv(r'Data/final_tmdb_data_2005.csv.gz')
tmdb_2006=pd.read_csv(r'Data/final_tmdb_data_2006.csv.gz')
tmdb_2007=pd.read_csv(r'Data/final_tmdb_data_2007.csv.gz')
tmdb_2008=pd.read_csv(r'Data/final_tmdb_data_2008.csv.gz')
tmdb_2009=pd.read_csv(r'Data/final_tmdb_data_2009.csv.gz')


# In[9]:


tmdb_00_09=pd.concat([tmdb_2000,tmdb_2001,tmdb_2002,tmdb_2003,tmdb_2004,tmdb_2005,tmdb_2006,tmdb_2007,tmdb_2008,tmdb_2009], axis=0)
tmdb_00_09


# In[10]:


compression_opt = dict(method='zip',
                      archive_name='out.csv')
tmdb_00_09.to_csv('out.zip', index=False,compression=compression_opt)


# In[11]:


tmdb_00_09.head()


# In[12]:


sam_data = pd.read_csv(r'Data/combined_all_data.csv', lineterminator='\n')


# In[13]:


sam_data


# In[14]:


df=pd.concat([tmdb_00_09, sam_data], axis=0)


# In[15]:


df.head()


# In[16]:


df.info()


# # **Hypothesis Testing**
# 
# **1. Does the MPAA rating of a movie affect how much revenue the movie generates?**
# 
# Null: There is no significant difference in revenue between the MPAA ratings of films.
# 
# Alt: There is a significant difference in the revenues between films of different MPAA ratings. 
# 
# Our target variable is numeric (revenue) and we have more than 2 samples (G/PG/PG-13/R/NC-17/NR) so we will perform an **ANOVA** along with a **post-hoc tukey test** if we reject the null hypothesis. 
# 
# **Assumption Testing**

# In[41]:


df['certification'].value_counts()


# There are inconsistencies in our certification column, but their sample sizes are negligible and can be removed from the data frame entirely.

# In[42]:


counts = df['certification'].value_counts()


# In[43]:


df_certs = df[~df['certification'].isin(counts[counts < 30].index)]


# In[44]:


df_certs['certification'].value_counts()


# In[45]:


ax = sns.barplot(data=df_certs, x='certification', y='revenue')
ax.set_title('Average Revenue per Certfication Category');


# Judging by the barplot, it appears there is indeed a difference in the revenue between categories, but we will perform the ANOVA to be sure.
# 
# Our sample sizes are large enough that we can **ignore the assumption of normality**
# 
# **Testing Equal Variance**
# 

# In[54]:


df_certs = df_certs.dropna(subset=['certification'], how='all')
df_certs.head()


# In[55]:


groups = {}
for i in df_certs['certification'].unique():
  data = df_certs.loc[df_certs['certification']==i, 'revenue'].copy()
  groups[i] = data
groups.keys()


# In[56]:


df_certs['certification'].value_counts()


# In[57]:


stats.levene(*groups.values())


# In[58]:


if result.pvalue < 0.05:
  print('The groups do not have equal variance, so we will perform a kruskal test')
else:
  print('The groups do have equal variance')


# In[60]:


result = stats.f_oneway(*groups.values())
result


# Our pvalue is smaller than our alpha, so we will **reject the null hypothesis** and infer that there is a significant difference in the revenues between certification categories.
# 
# **2. Do higher-budget films also have higher average revenues?**
# 
# For this test, we will add a column that defines a film's budget as a categorical variable for the purpose of our hypothesis testing. For this example, we will define film budgets like so:
# 
# Low: Between 1 - 999,999
# 
# Med Low: 1,000,000 - 14,999,999
# 
# Med: 15,000,000 - 49,999,999
# 
# Med High: 50,000,000 - 99,999,999
# 
# High: +100,000,000
# 
# This according to example data obtained from the Directors Guild of America at https://www.dga.org/Contracts/Agreements/Low-Budget.aspx#:~:text=Level%201b%3A%20Films%20with%20budgets,to%20or%20less%20than%20%243%2C750%2C000. and StudioBinder at https://www.studiobinder.com/blog/production-budget/
# 
# **Null Hypothesis:** There is no difference in the revenue for films of different budget categories
# 
# **Alt Hypothesis:** There is some significant difference in the revenues for films of different budget categories.
# 
# Our target variable is numeric and we have more than 2 samples, so we will perform an ANOVA test.

# In[65]:


df_budg = df
df_budg.info()


# In[74]:


def classify_budget(budget):
    if budget > 1 and budget < 999999:
        return 'low'
    elif budget > 1000000 and budget < 14999999:
        return 'med low'
    elif budget > 15000000 and budget < 49999999:
        return 'medium'
    elif budget > 50000000 and budget < 99999999:
        return 'med high'
    elif budget > 100000000:
        return 'high'
    else:
        return np.nan


# In[75]:


df_budg['budget_class'] = df['budget'].apply(classify_budget)
df_budg.head()


# In[76]:


df_budg['budget_class'].value_counts()


# In[77]:


df_budg = df_budg.dropna(subset=['budget_class'], how='all')
df_budg.head()


# In[78]:


df_budg['budget_class'].value_counts()


# In[80]:


ax = sns.barplot(data=df_budg, x='budget_class', y='revenue')
ax.set_title('Average Revenue by Budget Class')
ax.set_xticklabels(ax.get_xticklabels(), rotation=45);


# In[81]:


groups = {}
for i in df_budg['budget_class'].unique():
    data = df_budg.loc[df_budg['budget_class']==i, 'revenue'].copy()
    groups[i] = data
groups.keys()


# In[82]:


stats.levene(*groups.values())


# Our groups do not have equal variance, so we will use a kruskal test for our hypothesis

# In[83]:


stats.kruskal(*groups.values())


# Our p value is less than 0.05, so we will **reject** the null hypothesis and infer that a film's budget does have some effect on its potential revenue.
# 
# **3. Does a film's budget class have an effect on it's vote average? (IMDB Quality rating)**
# 
# Null: There is no difference in the vote averages between budget classes
# 
# Alt: Budget class has a significant effect on a film's vote average.
# 
# Our target variable is numeric and we have more than one sample, so we will perform another ANOVA test.

# In[84]:


ax = sns.barplot(data=df_budg, x='budget_class', y='vote_average')
ax.set_title('Average Rating by Budget Class')
ax.set_xticklabels(ax.get_xticklabels(), rotation = 45);


# In[85]:


groups = {}
for i in df_budg['budget_class'].unique():
    data = df_budg.loc[df_budg['budget_class']==i, 'vote_average'].copy()
    groups[i] = data
groups.keys()


# In[86]:


stats.levene(*groups.values())


# In[87]:


stats.kruskal(*groups.values())

