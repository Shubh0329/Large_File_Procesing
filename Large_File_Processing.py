#!/usr/bin/env python
# coding: utf-8

# In[45]:


import pandas as pd
import pyodbc as odbc
#import odbc


# In[6]:


df = pd.read_csv("C:\\Users\\Lenovo\\Downloads\\products.csv")


# In[7]:


df.memory_usage(index=False, deep=True)


# In[8]:


df.memory_usage(index=False, deep=True).sum()


# In[6]:


df.dtypes


# In[72]:


df_data = list(df.columns)
df_data


# In[66]:


class TableLoad:

    def __init__(self, df):
        self.df = df

"""
Step 3.1 Create SQL Servre Connection String
"""


    def connection_string(self,driver, server_name, database_name):
        conn_string = f"""
            DRIVER={{{driver}}};
            SERVER={server_name};
            DATABASE={database_name};
            Trust_Connection=yes;        
        """
        return conn_string

"""
Step 3.2 Create database connection instance
"""
        conn = odbc.connect(connection_string(DRIVER, SERVER_NAME, DATABASE_NAME))


        if !cursor.tables(table='TableName', tableType='TABLE').fetchone():
"""
Step 3.3 Create a cursor connection and insert records
"""
            cursor.execute('''BEGIN CREATE TABLE practice.dbo.Product_Detail
                            (
                               name nvarchar(50),
                               sku nvarchar(50) PRIMARY KEY,
                               description nvarchar(3550)
                               WITH (IGNORE_DUP_KEY = ON)
                           )
                             END
                             ''')
            sql_insert = '''
                'INSERT INTO Product_Detail(name,sku,description) values(?,?,?)
            '''
        else:
            sql_insert = '''
                'INSERT INTO Product_Detail(name,sku,description) values(?,?,?)
            '''
         
        cursor.execute('''BEGIN CREATE TABLE Product_agg_Detail
                            (
                               name nvarchar(50),
                               number_of_products DECIMAL(10,4)
                           )
                             END
                             ''')
        
          sql_insert_agg = '''
                'select name,count(sku) as counted_val from products  group by name INTO Product_agg_Detail(name,number_of_products) values(?,?)
            '''
        
        
        try:
            cursor = conn.cursor()
            cursor.executemany(sql_insert, df_data)
            cursor.commit();    
        except Exception as e:
            cursor.rollback()
            print(str(e[1]))
        finally:
            print('Task is complete.')
            cursor.close()
            conn.close()


# In[23]:


DRIVER = 'SQL Server'
SERVER_NAME = '<Server Name>'
DATABASE_NAME = '<Database Name>'


p1 = TableLoad(df)
p1.connection_string(DRIVER,SERVER_NAME,DATABASE_NAME)
cursor.close()


# In[ ]:




