#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import asyncio
import websockets
import json
import datetime
import time
import sys
import pandas as pd
from threading import Timer
from sqlalchemy import create_engine

# connector
# import connect.connect as cc
# engine, con = cc.connect('CIM','iot')


# engine = create_engine(
#     'mysql+pymysql://thiler:thil1234@10.21.40.126/iot?charset=utf8mb4')
# # con = engine.connect()  # 建立連線

# sql = "SELECT * FROM co2_iot_list"
# co2_list = pd.read_sql_query(sql, engine)

# co2_list['uri'] = "ws://" + co2_list['IP'] + ":3000"


async def listen(websocket_uri):
    while True:
        try:
            async with websockets.connect(websocket_uri) as websocket:

                while True:
                    try:
                        async for message in websocket:
                            # print(f"Received message from {websocket_uri}: {message}")
                            # info = co2_list[co2_list["uri"]==websocket_uri]
                            # info = info.reset_index()
                            # source = info["SN"][0]
                            # eqp = info["EQP"][0]
                            # no = info["NO"][0]
                            # unit = info["UNIT"][0]
                            msg = json.loads(message)
                            # analysis(msg,source,eqp,no,unit)
                            print(msg)

                            await asyncio.sleep(60)

            #             Timer(5, analysis(msg,source,eqp,no,unit)).start()
            #             time.sleep(5)

                    except websockets.exceptions.ConnectionClosedError as e:
                        now_5 = datetime.datetime.now()
                        now_5_time = now_5.strftime("%Y-%m-%d %H:%M:%S")
                        print("連線被異常關閉,5秒後開始重新連接")
                        print(now_5_time)
                        # 寫入log
                        # sql = "INSERT INTO `exe_log` (`ip`,`error`,`time`) values ('"+websocket_uri+"','連線被異常關閉','"+now_5_time+"')"
                        # engine.execute(sql)
                        time.sleep(5)
                        break

                    # except Exception as e:
                    #     print(e)

                    finally:
                        print('disconnect')
                        # engine.dispose()

        except Exception as e:
            now_10 = datetime.datetime.now()
            now_10_time = now_10.strftime("%Y-%m-%d %H:%M:%S")
            print(websocket_uri)
            print(e)
            print("10秒後重新連接")
            # 寫入log
            # sql = "INSERT INTO `exe_log` (`ip`,`error`,`time`) values ('"+websocket_uri+"','連線失敗','"+now_10_time+"')"
            # engine.execute(sql)
            time.sleep(10)

        finally:
            print('disconnect')
            # engine.dispose()


def analysis(msg, source, eqp, no, unit):
    #     source = msg["source"]
    now = datetime.datetime.now()
    now_time = now.strftime("%Y-%m-%d %H:%M:%S")
    source_ = source
    eqp_ = eqp
    no_ = no
    unit_ = unit
    obj = msg["object"][0]['value']
    value = "".join(map(str, obj))

    # sql = "INSERT INTO CO2 (`machine`,`source`,`no`,`unit`,`value`,`time`) values ('"+eqp_+"','"+source_+"','"+no_+"','"+unit_+"','"+value+"','"+now_time+"')"
    # engine.execute(sql)

    #     time.sleep(5)


if __name__ == "__main__":

    # try:
    websocket_uris = ["ws://10.21.84.68:3000"]
    # websocket_uris = co2_list['uri']

    # 创建一个事件循环
    loop = asyncio.get_event_loop()

    # 启动多个WebSocket客户端，每个客户端都在单独的协程中运行
    tasks = [loop.create_task(listen(uri)) for uri in websocket_uris]
    #     tasks = [ Timer(5, loop.create_task(listen(uri))).start() for uri in websocket_uris]

    # 运行事件循环，直到所有协程完成
    #     loop.run_until_complete(asyncio.wait(tasks))
    loop.run_until_complete(asyncio.wait(tasks))

    # 关闭事件循环
    loop.close()

    # except Exception as e:
    #    print(e)
    # error_class = e.__class__.__name__ #取得錯誤類型
    # detail = e.args[0] #取得詳細內容
    #     cl, exc, tb = sys.exc_info() #取得Call Stack
    #     lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
    #     fileName = lastCallStack[0] #取得發生的檔案名稱
    #     lineNum = lastCallStack[1] #取得發生的行號
    #     funcName = lastCallStack[2] #取得發生的函數名稱
    #     errMsg = "File \"{}\", line {}, in {}: [{}] {}".format(fileName, lineNum, funcName, error_class, detail)

    #     print(detail)

    # sql = "INSERT INTO `exe_log` (`error`) values ('"+detail+"')"
    # con.execute(sql)
