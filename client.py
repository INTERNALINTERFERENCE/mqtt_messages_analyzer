import paho.mqtt.client as mqtt
import ssl
import csv
import os.path
from typing import Dict, List

CONNECTION_ID = "analyzer:imagine_guid*"
CONNECTION_ADDRESS = "test.mosquitto.org"
CONNECTION_PORT = 1883

dict: Dict[str, List[int]] = {}

mqtt_client = mqtt.Client(
    client_id=CONNECTION_ID,
    clean_session=True)

def on_connect(client, userdata, flags, rc):
    print("result from connect: {}".format(
        mqtt.connack_string(rc)
    ))
    client.subscribe("#", qos=0)

def on_subscribe(client, userdata, mid, granded_qos):
    print("I've subscribed with QoS: {}".format(
        granded_qos[0]
    ))

def on_message(client, userdata, msg):
    if msg.topic not in dict:
        dict[msg.topic] = [0, 0, float('inf'), int]
    dict[msg.topic][0] += 1
    dict[msg.topic][1] = max(dict[msg.topic][1], len(msg.payload))
    dict[msg.topic][2] = min(dict[msg.topic][2], len(msg.payload))
    dict[msg.topic][3] = (int)((dict[msg.topic][1] + dict[msg.topic][2]) / 2)

def on_publish(client,userdata,result):
    print("data published \n")
    pass

def read_scv():
    if(os.path.exists('analyzed.csv')):
        with open('analyzed.csv', newline='') as f:
            reader = csv.reader(f)
            for row in reader:
                d = {k: [int(i) for i in v.split(';')] for k, v in map(lambda s: s.split(';', maxsplit=1), row)}
                dict.update(d) 

def create_mqtt_client():
    mqtt_client.on_connect = on_connect
    mqtt_client.on_subscribe = on_subscribe
    mqtt_client.on_message = on_message
    mqtt_client.on_publish = on_publish
    mqtt_client.connect(
        host=CONNECTION_ADDRESS,
        port=CONNECTION_PORT)
    mqtt_client.loop_forever()