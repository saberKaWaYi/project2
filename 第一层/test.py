from connect import Connect_Mysql
from concurrent.futures import ThreadPoolExecutor,as_completed
import requests

class Run:

    def __init__(self,config):
        self.config=config
        self.db_mysql=Connect_Mysql(config)
        self.db_mysql_client=self.db_mysql.client.cursor()
        self.data=self.db_mysql.get_table_data("","select hostname,ip,brand from topu.network")[["hostname","ip","brand"]].values.tolist()

    def fc(self,hostname,ip,brand):
        if "." not in ip:
            return
        if hostname.lower() in ["none","null","nan","","-","--","---"]:
            return
        if brand not in ["huawei","huarong","cisco","junos","fenghuo","nokia","h3c"]:
            return
        if brand not in ["junos"]:
            return
        cmd="show ethernet-switching table"
        url_post='http://10.213.136.111:40061/network_app/distribute_config/exec_cmd/'
        config={
            "device_hostname":hostname,
            "operator":"LCL",
            "is_edit":False,
            "cmd":""
        }
        config["cmd"]=cmd
        url_get=f"http://10.216.142.10:40061/network_app/conf/device/data_list/?device_hostname={hostname}"
        response_url_get=requests.get(url_get).json()
        try:
            client_names=response_url_get["data"][0]["net_data"]
        except:
            return
        if client_names:
            temp=[]
            for client_name in client_names.values():
                if client_name["operator"]!="LCL":
                    continue
                temp.append((client_name["client_name"],client_name["created_at"]))
            temp.sort(key=lambda x:x[1])
            if temp:
                config["client_name"]=temp[-1][0]
        try:
            response_url_post=requests.post(url_post,config).json()["data"]["cmd_result"]
        except:
            return
        if not response_url_post:
            if "client_name" in config:
                del config["client_name"]
            try:
                response_url_post=requests.post(url_post,config).json()["data"]["cmd_result"]
            except:
                return
            if not response_url_post:
                return
        print(response_url_post)

    def run(self):
        with ThreadPoolExecutor(max_workers=50) as executor:
            pool=[]
            for i in self.data:
                pool.append(executor.submit(self.fc,i[0],i[1],i[2]))
            for task in as_completed(pool):
                task.result()

if __name__=="__main__":
    config={
        "connection":{
            "TIMES":3,
            "TIME":0.1
        },
        "mysql":{
            "HOST":"10.216.141.30",
            "PORT":19002,
            "USERNAME":"devops_master",
            "PASSWORD":"cds-cloud@2017"
        }
    }
    m=Run(config)
    m.run()