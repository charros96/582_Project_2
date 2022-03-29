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

    
    unfilled_db = session.query(Order).filter(Order.filled == "").all()
    for existing_order in unfilled_db:
        if existing_order.buy_currency == order_obj.sell_currency:
            if existing_order.sell_currency == order_obj.buy_currency:
                if (existing_order.sell_amount / existing_order.buy_amount) >= (order_obj.buy_amount/order_obj.sell_amount) :
                    ct = datetime.datetime.now()
                    existing_order.filled = ct
                    order_obj.filled = ct
                    existing_order.counterparty_id = order_obj.id
                    order_obj.counterparty_id = existing_order.id
                    break

    session.add(order_obj)
    session.commit()
    pass