#!/usr/bin/python3
import paho.mqtt.client as mqtt
import pandas as pd
import json
import time
import sys
import asyncio
import datetime
sys.path.append('/home/cim')
# sys.path.append('C:\\Users\\User\\Desktop\\python')
import connect.connect as cc
eng_cim = cc.connect('CIM_ubuntu', 'iot')


sql = "SELECT * FROM dscw_iot_list WHERE MQTT = '1'"
liq_list = pd.read_sql_query(sql, eng_cim)
no_list = liq_list["NO"].tolist()


# 當地端程式連線伺服器得到回應時，要做的動作
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

    # 將訂閱主題寫在on_connet中
    # 如果我們失去連線或重新連線時
    # 地端程式將會重新訂閱


    arr =[]
    for i in no_list:
        t = ("DSCW/"+i,0)
        arr.append(t)
        print(t)

    client.subscribe(arr)



# 當接收到從伺服器發送的訊息時要進行的動作
def on_message(client, userdata, msg):
    # 轉換編碼utf-8才看得懂中文
    print(msg.topic+" "+ msg.payload.decode('utf-8'))

    no = msg.topic.split('/')
    no = no[1]

    msg = msg.payload.decode('utf-8')
    msg = json.loads(msg)
    vals = msg["object"][0]["value"]

    info = liq_list[liq_list["NO"]==no]
    info = info.reset_index()
    source = info["SN"][0]
    eqp = info["EQP"][0]
    unit = info["UNIT"][0]

    now = datetime.datetime.now()
    now_time = now.strftime("%Y-%m-%d %H:%M:%S")

    df = pd.DataFrame(columns=['machine', 'source','no','unit','value','time'])
    df["value"] = vals
    df["machine"]=eqp
    df["no"]=no
    df["source"]=source
    df["unit"]=unit
    df["time"]=now_time

    df.to_sql('dscw',eng_cim,index=False,if_exists='append')


# 連線設定
# 初始化地端程式
client = mqtt.Client()

# 設定連線的動作
client.on_connect = on_connect

# 設定接收訊息的動作
client.on_message = on_message

# 設定登入帳號密碼
# client.username_pw_set("silva","holy7813")

# 設定連線資訊(IP, Port, 連線時間)
client.connect("10.21.98.21", 1883, 60)

# 開始連線，執行設定的動作和處理重新連線問題
# 也可以手動使用其他loop函式來進行連接
