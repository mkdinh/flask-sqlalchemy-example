import pandas as pd
from sqlalchemy import create_engine
import os

# create engine
conn_str = 'sqlite:///Resources/purchasing_data.sqlite'
conn = create_engine(conn_str)

# drop table if exists
conn.execute('DROP TABLE IF EXISTS purchases')
conn.execute('''
    CREATE TABLE purchases ( 
        "PurchaseID" INT PRIMARY KEY, 
        "SN" TEXT, "Age" BIGINT, 
        "Gender" TEXT, 
        "ItemID" BIGINT, 
        "ItemName" TEXT, 
        "Price" FLOAT )
    ''')

# load in csv
csv_path = os.path.join('Resources', 'purchase_data.csv')
df = pd.read_csv(csv_path)

df.to_sql('purchases', conn, if_exists='append', index=False)

purchases = conn.execute('SELECT * FROM purchases LIMIT 10')

for purchase in purchases:
    print(purchase)