from sqlalchemy import create_engine, inspect
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session

# database setup
conn_str = 'sqlite:///Resources/purchasing_data.sqlite'
conn = create_engine(conn_str)
inspector = inspect(conn)

# create base class for reflection
Base = automap_base()

# reflect tables in database
Base.prepare(conn, reflect=True)

Purchases = Base.classes.purchases

# create session query
session = Session(bind=conn)

first10 = session.query(Purchases).limit(10).all()

# - returns all purchasing data
all_purchases = session.query(Purchases).all()
print('all purchases count', len(all_purchases))

# - returns all purchasing data for a user
user_purchases = session.query(Purchases)\
    .filter((Purchases.SN == 'Lisim78') | (Purchases.SN == 'Lisovynya38'))\
    .all()
print('user purchases count', len(user_purchases))

# - returns all purchasing data for an item
item_purchases = session.query(Purchases)\
    .filter((Purchases.ItemID == 108) & (Purchases.SN == 'Lisim78'))\
    .all()
print('item purchases count', len(item_purchases))

# - returns a particular purchase by id
single_purchase = session.query(Purchases)\
    .filter(Purchases.PurchaseID == 0)\
    .first()
