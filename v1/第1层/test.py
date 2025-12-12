from connect import Connect_Mysql,Connect_Clickhouse
import datetime

class Test:

    def __init__(self,config1,config2):
        self.db_mysql=Connect_Mysql(config1)
        self.db_clickhouse=Connect_Clickhouse(config2)

    def test1(self):
        volumes=self.db_mysql.get_table_data("volume","select * from volume")["volume_name"].values.tolist()
        date_time=datetime.datetime.now()-datetime.timedelta(days=1)
        jh=set()
        for volume in volumes:
            volume_temp=volume.split("|")[-1]
            query="select vm from ods_vm.vc_volume_info_all  where toDateTime(monitor_time)  > '{}:00' and name='{}' group by vm;".format(str(date_time)[:16],volume_temp)
            vms=self.db_clickhouse.query(query)["vm"].values.tolist()[0].split(",")
            for vm in vms:
                jh.add(volume+"|"+vm)
        print(jh)
        
if __name__=="__main__":
    config1={
        "connection":{
            "TIMES":1000,
            "TIME":0.1
        },
        "mysql":{
            "HOST":"localhost",
            "PORT":3000,
            "USERNAME":"devops_master",
            "PASSWORD":"cds-cloud@2017"
        }
    }
    config2={
        "connection":{
            "TIMES":1000,
            "TIME":0.1
        },
        "clickhouse":{
            "HOST":"localhost",
            "PORT":5001,
            "USERNAME":"default",
            "PASSWORD":""
        }
    }
    m=Test(config1,config2)
    m.test1() 