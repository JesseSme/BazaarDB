from imp import reload
from turtle import write_docstringdict
from fastapi_mqtt import FastMQTT, MQTTConfig
from fastapi import FastAPI
from fastapi_utils.tasks import repeat_every
import requests
import uvicorn

from database import *

mqtt_config = MQTTConfig(host = "localhost",
    port= 1883,
    keepalive = 60)

fast_mqtt = FastMQTT(
    config=mqtt_config
)

def success_cb(details, data):
    url, token, org = details
    print(url, token, org)
    data = data.decode('utf-8').split('\n')
    print('Total Rows Inserted:', len(data))  

def error_cb(details, data, exception):
    print(exception)

def retry_cb(details, data, exception):
    print('Retrying because of an exception:', exception)

_token = "UO8_LwtBmc2Ddb8Ev3AWGAEeu_oGLGaR_Cgoc8IdRBoM7VUDmPClC0yeSbL7amWwoKFq11rS1296RhQDsXRLsg=="
org = "BazaarMains"
bucket = "BazaarData"
client = InfluxDBClient(url="http://localhost:8086", token=_token, org=org)
write_api = client.write_api(write_options=SYNCHRONOUS)

app = FastAPI()

fast_mqtt.init_app(app)

bazaar = "https://api.hypixel.net/skyblock/bazaar"

_success_counter = 0
_highest_counter = 0

@app.on_event("startup")
@repeat_every(seconds=30)
def check_db_connection():
    result = client.health()
    print(result)
    if result['status'] != 'pass':
        print("NO CONNECTION")

@app.on_event("startup")
@repeat_every(seconds=2)
def update_bazaar():
    global _success_counter
    global _highest_counter
    r = requests.get(bazaar)
    if r.status_code == 200:
        #print(_success_counter)
        _success_counter = _success_counter + 1
        data = r.json()
        #print(data['success'])
        write_to_db(write_api, org, bucket, data)
    else:
        #print(r.status_code)
        if _success_counter >= _highest_counter:
            _highest_counter = _success_counter
        #print(_highest_counter)

    return data



@fast_mqtt.on_connect()
def connect(client, flags, rc, properties):
    fast_mqtt.client.subscribe("/mqtt") #subscribing mqtt topic 
    print("Connected: ", client, flags, rc, properties)

@fast_mqtt.subscribe("hello/world")
async def home_message(client, topic, payload, qos, properties):
    print("temperature/humidity: ", client, topic, payload.decode(), qos, properties)
    return 0

@fast_mqtt.on_message()
async def message(client, topic, payload, qos, properties):
    print("Received message: ",topic, payload.decode(), qos, properties)
    return 0

@fast_mqtt.on_disconnect()
def disconnect(client, packet, exc=None):
    print("Disconnected")

@fast_mqtt.on_subscribe()
def subscribe(client, mid, qos, properties):
    print("subscribed", client, mid, qos, properties)



if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=6969, log_level="info", reload=True)