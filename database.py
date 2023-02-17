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

        buy_orders_size = 0
        sell_orders_size = 0
        for buy_orders in value["sell_summary"]:

            if value["product_id"] == "ABSOLUTE_ENDER_PEARL":
                print(buy_orders)
            buy_orders_size = len(value["sell_summary"])
            amount = buy_orders['amount']
            priceperunit = buy_orders['pricePerUnit']
            orders = buy_orders['orders']

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
            points.append(jiisön)
            itemcounter = itemcounter + 1
        if itemcounter != buy_orders_size:
            print("Counter mismatch: {}".format(value["product_id"]))
        itemcounter = 0

        for sell_orders in value["buy_summary"]:
            sell_orders_size = len(value["buy_summary"])
            amount = sell_orders['amount']
            priceperunit = sell_orders['pricePerUnit']
            orders = sell_orders['orders']

            jiisön = {
                'measurement': 'item_sell_price',
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
            points.append(jiisön)
            itemcounter = itemcounter + 1
        if itemcounter != sell_orders_size:
            print("Counter mismatch: {}".format(value["product_id"]))
        itemcounter = 0
        result = client.write(bucket=bucket,org=org,record=points,write_precision='s')
        client.flush()

        
        
            
                    

