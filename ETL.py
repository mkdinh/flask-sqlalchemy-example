#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
from sqlalchemy import create_engine, inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session
from sqlalchemy import Column, Integer, String, Float
from config import username, password

# create engine from original dataset
conn_str = 'sqlite:///Resources/purchasing_data.sqlite'
conn = create_engine(conn_str)

# new_conn_str = 'sqlite:///Resources/cleaned_purchasing_data.sqlite'
postgres_conn_str = f'postgresql://{username}:{password}@localhost:5432/heroes_of_pymoli'
conn_new = create_engine(postgres_conn_str)

# create Base class
Base = declarative_base(bind=conn_new)


inspector = inspect(conn)

tables = inspector.get_table_names()

for table in tables:
    columns = inspector.get_columns(table)
    print('-----------------')
    print(table)
    print('-----------------')
    for column in columns:
        print(column['name'], column['type'])


# create entities
class Purchase(Base):
    __tablename__ = 'purchase'
    PurchaseId = Column(Integer, primary_key=True)
    UserId = Column(Integer)
    ItemId = Column(Integer)
    
class User(Base):
    __tablename__ = 'user'
    UserId = Column(Integer, primary_key=True)
    UserName = Column(String(255))
    Age = Column(Integer)
    Gender = Column(String(255))
    
class Item(Base):
    __tablename__ = 'item'
    ItemId = Column(Integer, primary_key=True)
    ItemName = Column(String(255))
    Price = Column(Float)

Base.metadata.drop_all(conn_new)
Base.metadata.create_all(conn_new)

# find unique items
df = pd.read_sql('SELECT * FROM purchases', conn)
df.head()

unique_items = df.drop_duplicates(subset=['ItemID'])
unique_items = unique_items[['ItemID', 'ItemName', 'Price']]
unique_items = unique_items.sort_values('ItemID')
unique_items = unique_items.rename(columns={ 'ItemID': 'ItemId' })
unique_items.to_sql('item', conn_new, index=False, if_exists='append')

unique_users = df.drop_duplicates(subset=['SN'])
unique_users = unique_users[['SN', 'Gender', 'Age']]
unique_users = unique_users.reset_index()
unique_users = unique_users.rename(columns={ 'SN': 'UserName', 'index': 'UserId' })
unique_users = unique_users.sort_values('UserId')
unique_users.to_sql('user', conn_new, index=False, if_exists='append')

combined_purchase = df.copy()
combined_purchase = combined_purchase.merge(unique_users, left_on="SN", right_on="UserName")
combined_purchase = combined_purchase[['PurchaseID', 'ItemID', 'UserId']]
combined_purchase = combined_purchase.rename(columns={ 'PurchaseID': 'PurchaseId', 'ItemID': 'ItemId' })
combined_purchase = combined_purchase.sort_values('ItemId')
combined_purchase.to_sql('purchase', conn_new, index=False, if_exists='append')


session = Session(bind=conn_new)

query = session.query(Purchase.PurchaseId, User.UserName, User.Age, User.Gender, Item.ItemName, Item.Price)                .filter(Purchase.UserId == User.UserId)                .filter(Purchase.ItemId == Item.ItemId)
purchases = query.all()

print([col['name'] for col in query.column_descriptions])

purchases[0]

df.loc[df['PurchaseID'] == 133]





