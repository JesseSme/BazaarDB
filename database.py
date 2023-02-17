from datetime import datetime
import time
import json

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS, ASYNCHRONOUS

def success_cb(details, data):
    url, token, org = details
    print(url, token, org)
    data = data.decode('utf-8').split('\n')
    print('Total Rows Inserted:', len(data))  

def error_cb(details, data, exception):
    print(exception)

def retry_cb(details, data, exception):
    print('Retrying because of an exception:', exception)    


def write_to_db(client, org, bucket, data):
    unix_time = datetime.utcnow().isoformat()
    print(len(data["products"]))
    for key,value in data["products"].items():
        points = []
        itemcounter = 0
        amount = []
        priceperunit = []
        orders = []

        jiisön = {}

        buy_orders_size = 0
        sell_orders_size = 0
        if value["product_id"] == "ENCHANTED_SLIME_BALL":
            print("BALL BUY:")
            # INSTA SELL PRICES FOR 30 HIGHEST BUY ORDERS
            for buy_orders in value["sell_summary"]:
                buy_orders_size = len(value["sell_summary"])
                amount.append(buy_orders['amount'])
                priceperunit.append(buy_orders['pricePerUnit'])
                orders.append(buy_orders['orders'])

                
                itemcounter = itemcounter + 1
            if itemcounter != buy_orders_size:
                print("Counter mismatch: {}".format(value["product_id"]))
            itemcounter = 0
            
            # ADD TO JSON AND TO DATAPOINT LIST
            jiisön = {
                'measurement': 'item_buy_price',
                'tags': {
                    'product_id': value["product_id"]
                },
                'fields': {
                    'amount': amount,
                    'price_per_unit': priceperunit,
                    'orders': orders
                },
                'time': unix_time
            }
            print(jiisön)
            points.append(jiisön)

            jiisön = {}
            amount.clear()
            priceperunit.clear()
            orders.clear()
            print("BALL SELL:")
            # SELL ORDERS
            for sell_orders in value["buy_summary"]:

                sell_orders_size = len(value["buy_summary"])
                amount.append(sell_orders['amount'])
                priceperunit.append(sell_orders['pricePerUnit'])
                orders.append(sell_orders['orders'])
                itemcounter = itemcounter + 1
            if itemcounter != sell_orders_size:
                print("Counter mismatch: {}".format(value["product_id"]))
            itemcounter = 0

            # SET JSON
            jiisön = {
                'measurement': 'item_buy_price',
                'tags': {
                    'product_id': value["product_id"]
                },
                'fields': {
                    'amount': amount,
                    'price_per_unit': priceperunit,
                    'orders': orders
                },
                'time': unix_time
            }
            print(jiisön)
            points.append(jiisön)

            # SET 
            jiisön = {
                'measurement': 'quick_status',
                'tags': {
                    'product_id': value["product_id"]
                },
                'fields': {
                    'sellPrice': value["quick_status"]['sellPrice'],
                    'sellVolume': value["quick_status"]['sellVolume'],
                    'sellMovingWeek': value["quick_status"]['sellMovingWeek'],
                    'sellOrders': value["quick_status"]['sellOrders'],
                    'buyPrice': value["quick_status"]['buyPrice'],
                    'buyVolume': value["quick_status"]['buyVolume'],
                    'buyMovingWeek': value["quick_status"]['buyMovingWeek'],
                    'buyOrders': value["quick_status"]['buyOrders']
                },
                'time': unix_time
            }
            print(jiisön)
        # result = client.write(bucket=bucket,org=org,record=points,write_precision='s')
        # client.flush()

        
        
            
                    

