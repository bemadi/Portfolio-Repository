#!/usr/bin/env python
# coding: utf-8

# Part 3 - ETL
# 
# Create a MySQL database for the data we've cleaned and collected. 
# 
# Normalize the tables before adding them to the database.
#     - Convert a single string of genres from title basics into 2 tables
#     
#     1. title_genres: with the columns:
#         - tconst
#         - genre_id
#     2. genres:
#         - genre_id
#         - genre_name
#         
# Discard unnecessary columns: original_title, isAdult, titleType, genres. DO NOT include titles_akas table in your database
# 
# MySQL Database Requirements
# - Use sqlalchemy with pandas to execute SQL quries inside your notebook
# - Create a new database on your MySQL server and call it 'movies' with the following tables

# In[2]:


import pandas as pd
import numpy as np


# In[3]:


basics = pd.read_csv('Data/title_basics.csv.gz')
basics.head()


# In[4]:


basics.info()


# **Step 1: Normalization**

# In[5]:


# Create a column with a list of genres
basics['genres_split'] = basics['genres'].str.split(',')


# In[6]:


basics.head()


# In[7]:


exploded_genres = basics.explode('genres_split')
exploded_genres


# In[8]:


genres_split = basics['genres'].str.split(",")


# In[9]:


unique_genres = genres_split.explode().unique()
unique_genres


# In[10]:


unique_genres = sorted(exploded_genres['genres_split'].unique())


# In[11]:


unique_genres


# **2. Create a new title_genres table**

# In[12]:


title_genres = exploded_genres[['tconst', 'genres_split']].copy()
title_genres.head()


# **3. Create a genre mapper dictionary to replace string genres with integers**

# In[13]:


genre_ints = range(len(unique_genres))
genre_map = dict(zip(unique_genres, genre_ints))
genre_map


# In[14]:


genre_id_map = dict(zip(unique_genres, range(len(unique_genres))))
genre_id_map


# **4. Replace the string genres in title_genres with the new integer ids**

# In[15]:


title_genres['genre_id'] = title_genres['genres_split'].map(genre_id_map)
title_genres = title_genres.drop(columns='genres_split')
title_genres


# **5. Convert the genre map dictionary into a dataframe**

# In[16]:


genre_lookup = pd.DataFrame({'Genre_Name': genre_id_map.keys(),
                            'Genre_ID': genre_id_map.values()})
genre_lookup.head()


# **Saving the MySQL tables with tconst as the Primary Key**
# 
# **Basics Table**
# 
# **1. Creating a datatype schema for to_sql**

# In[17]:


basics.head()


# In[18]:


basics = basics.drop(columns=['originalTitle', 'isAdult', 'titleType', 'genres', 'genres_split'])


# In[19]:


basics.head()


# In[20]:


max_str_length = basics['tconst'].fillna('').map(len).max()


# In[21]:


max_str_length


# In[22]:


from sqlalchemy.types import *
key_len = max_str_length
title_len = basics['primaryTitle'].fillna('').map(len).max()
basics_schema = {
    "tconst": String(key_len+1),
    "primaryTitle": Text(title_len+1),
    'startYear':Float(),
    'endYear':Float(),
    'runtimeMinutes':Integer()
}


# **2. Run df.to_sql with the dtype argument** 

# In[23]:


from sqlalchemy_utils import create_database, database_exists


# In[24]:


# create_database(connection)


# In[25]:


from sqlalchemy import create_engine


# In[26]:


connection = "mysql+pymysql://root:root@localhost/movies"
engine = create_engine(connection)


# In[27]:


engine


# In[28]:


# Save to sql with dtype and index=False
basics.to_sql('title_basics',engine,dtype=basics_schema,if_exists='replace',index=False)


# **3. Run the query to add the primary key**

# In[29]:


engine.execute('ALTER TABLE title_basics ADD PRIMARY KEY (`tconst`);')


# **Ratings Table**

# In[30]:


ratings = pd.read_csv('Data/title_ratings.csv.gz')
ratings.head()


# In[31]:


max_str_len = ratings['tconst'].fillna('').map(len).max()
max_str_len


# In[32]:


key_len = ratings['tconst'].fillna('').map(len).max()
ratings_schema = {
    "tconst": String(key_len+1),
    "averageRating": Float(),
    "numVotes": Integer()
}


# In[33]:


ratings.to_sql('title_ratings',engine,dtype=ratings_schema,if_exists='replace',index=False)


# **Genres**

# In[34]:


max_str_len = title_genres['tconst'].fillna('').map(len).max()
max_str_len


# In[35]:


key_len = title_genres['tconst'].fillna('').map(len).max()
title_genres_schema = {
    "tconst": String(key_len+1),
    "genre_id": Integer() 
}


# In[36]:


title_genres.to_sql('title_genres',engine,dtype=title_genres_schema,if_exists='replace',index=False)


# In[37]:


genre_lookup.head()


# In[38]:


max_str_length = genre_lookup['Genre_Name'].fillna('').map(len).max()


# In[39]:


genre_lookup_schema = {
    "Genre_Name": String(max_str_len+1),
    "Genre_ID": Integer()
}


# In[40]:


genre_lookup.to_sql('genres',engine,dtype=genre_lookup_schema,if_exists='replace',index=False)


# **TMDB Data**

# In[41]:


import glob
import os


# In[42]:


tmdb_2000 = pd.read_csv(r'Data/final_tmdb_data_2000.csv.gz')
tmdb_2000


# In[43]:


tmdb_2001 = pd.read_csv(r'Data/final_tmdb_data_2001.csv.gz')
tmdb_2001


# In[44]:


tmdb = pd.concat([tmdb_2000, tmdb_2001], axis=0)
tmdb


# In[45]:


tmdb_data = tmdb[["imdb_id","revenue","budget","certification"]]
tmdb_data.head()


# In[46]:


tmdb_data.rename(columns={"imdb_id":"tconst"}, inplace=True)


# In[47]:


tmdb_data.dtypes


# In[48]:


imdb_id_len = tmdb_data["tconst"].fillna('').map(len).max()
cert_len = tmdb_data["certification"].fillna('').map(len).max()
df_schema = {
    "tconst": String(imdb_id_len+1),
    "budget": Float(),
    "revenue": Float(),
    "certification":Text(cert_len+1)
}


# In[49]:


tmdb_data.to_sql("tmdb_data",engine,dtype=df_schema,if_exists='replace',index=False)


# In[50]:


q = """SHOW TABLES;"""
pd.read_sql(q,engine)


# In[52]:


q = """SELECT *
FROM genres
LIMIT 3;"""
pd.read_sql(q,engine)


# In[53]:


q = """SELECT *
FROM title_basics
LIMIT 3;"""
pd.read_sql(q,engine)


# In[54]:


q = """SELECT *
FROM title_genres
LIMIT 3;"""
pd.read_sql(q,engine)


# In[55]:


q = """SELECT *
FROM title_genres
LIMIT 3;"""
pd.read_sql(q,engine)


# In[56]:


q = """SELECT *
FROM tmdb_data
LIMIT 3"""
pd.read_sql(q,engine)

