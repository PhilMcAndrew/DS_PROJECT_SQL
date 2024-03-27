#!/usr/bin/env python
# coding: utf-8

# In[18]:


#installation of psycopg2

pip install psycopg2-binary


# In[17]:


# libraries
from psycopg2 import connect
import pandas as pd
import numpy as np


# In[6]:


# setting connection to POSTGRES DB
cnx = connect(user="postgres",
                password= "ugegebuge",
                host="localhost",
                database="dvdrental")


# In[12]:


cursor= cnx.cursor()


# In[ ]:


# SELECTION AND EXPLORATION OF "ACTORS" TABLE


# In[13]:


cursor.execute("select * from actor;")
for row in cursor:
    print(row)


# In[15]:


colnames = [desc[0] for desc in cursor.description]

for name in colnames:
    print(name)


# In[16]:


print(cursor.description)


# In[ ]:


# saving table as df


# In[43]:


actors= []

cursor.execute("select * from actor;")
for row in cursor:
    actors.append(row)

actors_df= pd.DataFrame(actors)   


# In[44]:


actors_df.head()


# In[45]:


actors_df= actors_df.drop([0], axis=1)

actors_df.head()


# In[48]:


columns= ['FirstName', 'LastName', 'Last Updated']

actors_df.columns= columns


# In[51]:


actors_df.head()


# In[52]:


# CHECKING THE WHOLE DB SCHEMA


# In[53]:


import psycopg2.extras


def get_tables(cnx):

    """
    Create and return a list of dictionaries with the
    schemas and names of tables in the database
    connected to by the connection argument.
    """

    cursor = cnx.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cursor.execute("""SELECT table_schema, table_name
                      FROM information_schema.tables
                      WHERE table_schema != 'pg_catalog'
                      AND table_schema != 'information_schema'
                      AND table_type='BASE TABLE'
                      ORDER BY table_schema, table_name""")
    
    tables = cursor.fetchall()
    cursor.close()
    return tables


def print_tables(tables):

    """
    Prints the list created by get_tables
    """

    for row in tables:
        print("{}.{}".format(row["table_schema"], row["table_name"]))


# In[54]:


get_tables(cnx)


# In[56]:


print_tables(get_tables(cnx))


# In[ ]:




