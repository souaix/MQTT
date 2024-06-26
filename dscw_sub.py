#import paho.mqtt.client as mqtt
import pandas as pd
import json
import time
import sys
import asyncio
import datetime
from sqlalchemy import text
from paho.mqtt import client as mqtt_client

broker = '10.21.98.21'
port = 1883


sys.path.append('/home/cim')
# sys.path.append('C:\\Users\\User\\Desktop\\python')
import connect.connect as cc


eng_cim = cc.connect('CIM_ubuntu', 'iot')
con = eng_cim.connect()	

sql = "SELECT * FROM dscw_iot_list WHERE MQTT = '1'"

eq_list = pd.read_sql_query(sql, eng_cim)

no_list = eq_list["NO"].tolist()


def connect_mqtt() -> mqtt_client:
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client()
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


async def subscribe(client: mqtt_client,topic):
	def on_message(client, userdata, msg):
		try:
			print(msg.topic+" " + msg.payload.decode('utf-8'))
			no = msg.topic.split('/')
			if len(no) ==2:
				msg = msg.payload.decode('utf-8')
				msg = json.loads(msg)
				val = str(msg["object"][0]["value"][0])
				#print(no[1]+":"+str(val))
				insert_sql(no[1],val)

		except Exception as E:
			print(E)

	
	client.subscribe(topic,0)	
	client.on_message = on_message

	await asyncio.sleep(1)
	con.close()
	eng_cim.dispose()

def run(no_list):
	loop = asyncio.get_event_loop()    
	client = connect_mqtt()            
	client.loop_start()

	tasks = [loop.create_task(subscribe(client,"DSCW/"+i)) for i in no_list]

	loop.run_until_complete(asyncio.wait(tasks))
	eng_cim.dispose()
#	loop.run_forever()
#	loop.stop()


def insert_sql(no,val):

    info = eq_list[eq_list["NO"]==no]
    info = info.reset_index(drop=True)
    print(info)
    source = info["SN"][0]
    unit = info["UNIT"][0]	
    machine = info["EQP"][0]

    now = datetime.datetime.now()
    now_time = now.strftime("%Y-%m-%d %H:%M:%S")

    df = pd.DataFrame({ "machine":machine,
                        "source":source,
                        "no":no,
                        "unit":unit,
                        "value":val,
                        "time":now_time},index=[0])

    df.to_sql('dscw',eng_cim,index=False,if_exists='append')


		
		
if __name__ == '__main__':
	run(no_list)
	#eng_cim.dispose()
	
	


    # asyncio.run(run("CO2/No06_10"))
