from sqlalchemy import create_engine, inspect, func
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session

from flask import Flask, jsonify
from config import username, password
# database setup
conn_str = f'postgresql://{username}:{password}@localhost:5432/heroes_of_pymoli'
conn = create_engine(conn_str)
inspector = inspect(conn)

# create base class for reflection
Base = automap_base()

# reflect tables in database
Base.prepare(conn, reflect=True)
Purchase = Base.classes.purchase
User = Base.classes.user
Item = Base.classes.item

# create flask app
app = Flask(__name__)

def extract_data(record):
    simple_data = {
                'PurchaseID': record[0],
                'UserName': record[1],
                'Age': record[2],
                'Gender': record[3],
                'ItemName': record[4],
                'Price': record[5]
            }
    return simple_data

def convert(result):
    data = []
    for purchase in result:
        purchase_dict = extract_data(purchase)
        data.append(purchase_dict)
    
    return data

@app.route('/')
def index():
    return (
        f'Welcome to Purchasing Data API. These are the available endpoints:'
        f'<br>/api/purchases'
    )

# - returns all purchasing data for a user
@app.route('/api/purchases')
def user_purchases():
    session = Session(bind=conn)

    purchases = session.query(Purchase.PurchaseId, User.UserName, User.Age, User.Gender, Item.ItemName, Item.Price)\
        .filter(Purchase.UserId == User.UserId)\
        .filter(Purchase.ItemId == Item.ItemId)\
        .all()

    data = convert(purchases)

    session.close()

    return jsonify(data)

# - returns all purchasing data for a user
@app.route('/api/purchases/users/<username>/<item_id>')
def user_item_purchases(username, item_id):
    session = Session(bind=conn)

    user_purchases = session.query(Purchase)\
        .filter(Purchase.SN == username)\
        .filter(Purchase.ItemID == item_id)\
        .all()

    data = convert(user_purchases)
    session.close()

    return jsonify(data)

# - returns all purchasing data for an item
@app.route('/api/purchases/items/<item_id>')
def item_purchases(item_id):
    session = Session(bind=conn)

    item_purchases = session.query(Purchase)\
        .filter(Purchase.ItemID == item_id)\
        .all()

    data = convert(item_purchases)
    session.close()

    return jsonify(data)   

# - returns a particular purchase by id
@app.route('/api/purchases/id/<purchase_id>')
def purchase(purchase_id):
    session = Session(bind=conn)

    single_purchase = session.query(Purchase)\
        .filter(Purchase.PurchaseID == purchase_id)\
        .first()

    data = extract_data(single_purchase)
    session.close()

    return jsonify(data)   

# start app
if __name__ == '__main__':
    app.run(debug=True)