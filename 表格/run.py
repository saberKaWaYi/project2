from connect import Connect_Clickhouse,Connect_Mongodb,Connect_Mysql
import threading
from bson import ObjectId
import pandas as pd
from get_zd import get_zd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor,as_completed

class Run:

    def __init__(self,config1,config2,config3):
        self.config1=config1
        self.config2=config2
        self.conn=Connect_Clickhouse(self.config2);self.lock=threading.Lock()
        self.config3=config3
        self.network_list=self.get_network_list()
        self.zd=get_zd()
        self.time=datetime.now()
        self.result=[]

    def run(self):
        with ThreadPoolExecutor(max_workers=1) as executor:
            pool=[]
            for network in self.network_list:
                pool.append(executor.submit(self.fc,network))
            for task in as_completed(pool):
                task.result()
        columns=["hostname","ip","brand","switch","dt","info","region"]
        columns_str=', '.join(columns);values_placeholders=', '.join(['%s']*len(columns))
        sql=f'''
        INSERT INTO cds_report.report_network_lldp ({columns_str}) VALUES ({values_placeholders});
        '''
        conn=Connect_Mysql(self.config3)
        cursor=conn.client.cursor()
        jh1=set(conn.get_table_data("","select lldpLocSysname from topu.between_interface_and_interface")["lldpLocSysname"].values.tolist())
        jh2=["ROS","yunkuan","cloudsdnet","FD2-OT","ATEN","netmonitor","SinTai"]
        for i in range(len(self.result)):
            if self.result[i][3]:
                continue
            if self.result[i][0] in jh1:
                self.result[i][3]=1
                self.result[i][-2]+="但是可以采集得到。"
                continue
            if "-N01" in self.result[i][0] or "-N02" in self.result[i][0]:
                self.result[i][3]=1
                self.result[i][-2]+="人为过滤"
                continue
            for j in jh2:
                if j in self.result[i][0]:
                    self.result[i][3]=1
                    self.result[i][-2]+="人为过滤。"
                    break
        values_list=[tuple(row) for row in self.result]
        cursor.executemany(sql,values_list)
        conn.client.commit()

    def fc(self,info):
        hostname,ip,brand=info[0],info[1],info[2]
        sql1=f'''
        SELECT t1.id
        FROM network_conf.network_conf_sync_record AS t1
        INNER JOIN (
            SELECT hostname, MAX(vid) AS max_vid,MAX(update_time) AS max_update_time
            FROM network_conf.network_conf_sync_record
            GROUP BY hostname
        ) AS t2
        ON t1.hostname = t2.hostname AND t1.vid = t2.max_vid AND t1.update_time = t2.max_update_time
        WHERE t1.hostname like '%{hostname}%' and t1.device_status in (0,1,2) and t1.is_success in (0, 1);
        '''
        with self.lock:
            try:
                id_=self.conn.query(sql1)["id"].values.tolist()[0]
            except:
                self.result.append([hostname,ip,brand,0,self.time,f"网络设备id不存在，查询的sql语句为：{sql1}。",self.zd[hostname]])
                return
        sql2=f"select conf_detail from network_conf.network_conf_detail where sync_id={id_}"
        with self.lock:
            try:
                message=self.conn.client.execute(sql2)[0][0]
            except:
                self.result.append([hostname,ip,brand,0,self.time,f"网络设备没有配置，查询的sql语句为：{sql1}和{sql2}。",self.zd[hostname]])
                return 
        if brand=="junos":
            if "set protocols lldp interface all" not in message:
                self.result.append([hostname,ip,brand,0,self.time,"配置语句没包含\'set protocols lldp interface all\'。",self.zd[hostname]])
            else:
                if message.count("lldp")==1:
                    self.result.append([hostname,ip,brand,0,self.time,"配置语句有且并且仅包含\'set protocols lldp interface all\'，配置不完整需补充。",self.zd[hostname]])
                else:
                    self.result.append([hostname,ip,brand,1,self.time,"配置语句有包含\'set protocols lldp interface all\'，配置完整。",self.zd[hostname]])
        elif brand=="nokia":
            message=message.split("\n")
            jh=set()
            for i in range(len(message)):
                line=message[i].lower().strip()
                if "lldp" in line:
                    jh.add(i)
            lt=sorted(list(jh))
            result=[]
            for i in lt:
                j=i
                while True:
                    j-=1
                    if "port" in message[j]:
                        break
                x=message[j].split()[-1]
                y=message[j+1].split()[-1].strip("\"").split("-")
                a="-".join(y[1:-1]) if len(y)!=1 else "未知设备";b=y[-1] if len(y)!=1 else "未知接口"
                result.append(f"{hostname}|{x}---{a}|{b}")
            if not result:
                self.result.append([hostname,ip,brand,0,self.time,"配置没有lldp的单词。",self.zd[hostname]])
            else:
                self.result.append([hostname,ip,brand,1,self.time,"\n".join(result),self.zd[hostname]])
        elif brand=="cisco" or brand=="fenghuo":
            if "lldp" not in message:
                self.result.append([hostname,ip,brand,0,self.time,"配置没有lldp的单词。",self.zd[hostname]])
            else:
                self.result.append([hostname,ip,brand,1,self.time,"配置包含lldp的单词。",self.zd[hostname]])
        elif brand=="h3c":
            if "lldp global enable" in message:
                self.result.append([hostname,ip,brand,1,self.time,"配置语句有包含\'lldp global enable\'。",self.zd[hostname]])
            else:
                self.result.append([hostname,ip,brand,0,self.time,"配置语句没包含\'lldp global enable\'。",self.zd[hostname]])
        elif brand=="huarong" or "huawei":
            if "lldp enable" in message:
                self.result.append([hostname,ip,brand,1,self.time,"配置语句有包含\'lldp enable\'。",self.zd[hostname]])
            else:
                self.result.append([hostname,ip,brand,0,self.time,"配置语句没包含\'lldp enable\'。",self.zd[hostname]])

    def get_network_list(self):
        conn=Connect_Mongodb(self.config1)
        pipeline=[
            {
                '$match':{
                    'status':1,
                    'asset_status':{
                        '$in':[
                            ObjectId("5f964e31df0dfd65aaa716ec"),
                            ObjectId("5fcef6de94103c791bc2a471")
                        ]
                    }
                }
            },
            {
                '$lookup':{
                    'from':'cds_ci_location_detail',
                    'localField':'_id',
                    'foreignField':'device_id',
                    'as':'location'
                }
            },
            {
                '$match':{
                    'location.status':1
                }
            },
            {
                '$project':{
                    "hostname":1,
                    "device_ip":1,
                    "brand":1
                }
            }
        ]
        temp=pd.DataFrame(list(conn.db.cds_ci_att_value_network.aggregate(pipeline))).astype(str).values.tolist()
        data=[]
        for i in temp:
            hostname=i[2];ip=i[1];brand=i[3]
            if hostname.lower()=="none" or hostname.lower()=="null" or hostname.lower()=="nan" or hostname=="" or hostname=="-" or hostname=="--" or hostname=="---" or hostname==None:
                continue
            if "." not in ip:
                continue
            if brand=="" or brand=="-" or brand=="--" or brand=="---" or brand=="none" or brand=="null" or brand=="nan" or brand==None:
                continue
            hostname="-".join([i.strip() for i in hostname.split("-")]);brand=brand.lower()
            data.append([hostname,ip,brand])
        return data

if __name__=="__main__":
    config1={
        "connection":{
            "TIMES":3,
            "TIME":1
        },
        "mongodb":{
            "HOST":"10.216.141.46",
            "PORT":27017,
            "USERNAME":"manager",
            "PASSWORD":"cds-cloud@2017"
        }
    }
    config2={
        "connection":{
            "TIMES":3,
            "TIME":1
        },
        "clickhouse":{
            "HOST":"10.213.136.35",
            "PORT":9000,
            "USERNAME":"default",
            "PASSWORD":""
        }
    }
    config3={
        "connection":{
            "TIMES":3,
            "TIME":1
        },
        "mysql":{
            "HOST":"10.216.141.30",
            "PORT":19002,
            "USERNAME":"devops_master",
            "PASSWORD":"cds-cloud@2017"
        }
    }
    m=Run(config1,config2,config3)
    m.run()