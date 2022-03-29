from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime

from models import Base, Order
engine = create_engine('sqlite:///orders.db')
Base.metadata.bind = engine
DBSession = sessionmaker(bind=engine)
session = DBSession()

def process_order(order):
    fields = ['sender_pk','receiver_pk','buy_currency','sell_currency','buy_amount','sell_amount']
    order_obj = Order(**{f:order[f] for f in fields})

    session.add(order_obj)
    session.commit()
    pass