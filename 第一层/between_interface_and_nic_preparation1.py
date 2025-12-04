from connect import Connect_Mysql

class Run:

    def __init__(self,config):
        self.config=config
        self.db_mysql=Connect_Mysql(self.config)
        self.db_mysql_client=self.db_mysql.client.cursor()
        self.result=[]

    def create_temp(self):
        sql='''
        create table if not exists topu.temp (
            name VARCHAR(100),
            ipv4_list TEXT
        );
        '''
        self.db_mysql_client.execute(sql)
        self.db_mysql.client.commit()

    def truncate_temp(self):
        sql='''
        truncate table topu.temp;
        '''
        self.db_mysql_client.execute(sql)
        self.db_mysql.client.commit()

    def make_temp(self):
        data=self.db_mysql.get_table_data("","select hostname,ip,brand,description from topu.nic where type='虚拟'")[["hostname","ip","brand","description"]].values.tolist()
        zd={}
        for i in data:
            if "POD" not in i[0]:
                continue
            if "CLU" not in i[0]:
                continue
            x=None;y=None
            for j in i[0].split("-"):
                if "POD" in j:
                    x=j
                if "CLU" in j:
                    y=j
            if not (x and y):
                return
            s=f"{x}-{y}"
            if s not in zd:
                zd[s]=[]
            zd[s].append(i[-1].split("|")[-1])
        for i in zd:
            self.result.append((i,"|".join(zd[i])))

    def insert_temp(self):
        sql='''
        insert into topu.temp (name,ipv4_list) values (%s,%s);
        '''
        self.db_mysql_client.executemany(sql,self.result)
        self.db_mysql.client.commit()

    def run(self):
        self.create_temp()
        self.truncate_temp()
        self.make_temp()
        self.insert_temp()

if __name__=="__main__":
    config={
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
    m=Run(config)
    m.run()