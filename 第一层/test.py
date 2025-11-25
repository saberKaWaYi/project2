from connect import Connect_Mysql

def test1(config):
    conn=Connect_Mysql(config)
    data1=[i for i in conn.get_table_data("","select lldpLocSysName,lldpRemChassisId from topu.between_interface_and_interface")[["lldpLocSysName","lldpRemChassisId"]].values.tolist() if i[1].count(":")==5]
    data2=conn.get_table_data("","select hostname,mac_address from topu.nic where type='物理'").values.tolist()
    dict1={}
    dict2={}
    for i in data1:
        if i[1] not in dict1:
            dict1[i[1]]=[]
        dict1[i[1]].append(i[0])
    for i in data2:
        if i[1] not in dict2:
            dict2[i[1]]=[]
        dict2[i[1]].append(i[0])
    for i in dict1:
        if dict2.get(i,None)==None:
            continue
        print(dict1[i],dict2.get(i,None))

if __name__=="__main__":
    config1={
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
    test1(config1)