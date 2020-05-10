import pandas as pd
from sqlalchemy import create_engine, inspect

# create engine
conn_str = 'sqlite:///Resources/purchasing_data.sqlite'
conn = create_engine(conn_str)

inspector = inspect(conn)

tables = inspector.get_table_names()

for table in tables:
    columns = inspector.get_columns(table)
    print('-----------------')
    print(table)
    print('-----------------')
    for column in columns:
        print(column['name'], column['type'])