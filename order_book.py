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

    #print(order_obj)
    unfilled_db = session.query(Order).filter(Order.filled == None).all()
    session.add(order_obj)
    session.commit()
    for existing_order in unfilled_db:       
        if existing_order.buy_currency == order_obj.sell_currency:
            if existing_order.sell_currency == order_obj.buy_currency:
                if (existing_order.sell_amount / existing_order.buy_amount) >= (order_obj.buy_amount/order_obj.sell_amount) :
                    
                    existing_order.filled = datetime.now()
                    order_obj.filled = datetime.now()
                    existing_order.counterparty_id = order_obj.id
                    #existing_order.counterparty = order_obj
                    order_obj.counterparty_id = existing_order.id
                    #order_obj.counterparty = existing_order
                    session.commit()
                    if (existing_order.buy_amount > order_obj.sell_amount) | (order_obj.buy_amount > existing_order.sell_amount) :
                        if (existing_order.buy_amount > order_obj.sell_amount):
                            parent = existing_order
                            counter = order_obj
                        if order_obj.buy_amount > existing_order.sell_amount:
                            parent = order_obj
                            counter = existing_order
                        child = {}
                        child['sender_pk'] = parent.sender_pk
                        child['receiver_pk'] = parent.receiver_pk
                        child['buy_currency'] = parent.buy_currency
                        child['sell_currency'] = parent.sell_currency
                        child['buy_amount'] = parent.buy_amount-counter.sell_amount
                        child['sell_amount'] = (parent.buy_amount-counter.sell_amount)*(parent.sell_amount/parent.buy_amount)  
                        child_obj = Order(**{f:child[f] for f in fields})
                        child_obj.creator_id = parent.id
                        session.add(child_obj)
                        
                        session.commit()
                        break
                    
                    break

    
    session.commit()
    
    pass