from connect import Connect_Mongodb
from bson import ObjectId
import pandas as pd

def get_zd():
    config={
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
    conn=Connect_Mongodb(config)
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
        data.append([hostname,i[0]])
    dict1=dict(zip([i[0] for i in data],[i[1] for i in data]))
    dict2=conn.get_collection("cds_ci_location_detail",{"status":1,"ci_name":"network"},{"position_id":1,"device_id":1})
    dict2=dict(zip(dict2["device_id"].values.tolist(),dict2["position_id"].values.tolist()))
    for i in dict1:
        dict1[i]=dict2[dict1[i]]
    dict3=conn.get_collection("cds_ci_att_value_position",{"status":1},{"_id":1,"region_team":1})
    dict3=dict(zip(dict3["_id"].values.tolist(),dict3["region_team"].values.tolist()))
    for i in dict1:
        x=dict3[dict1[i]]
        x=x[x.index("\'")+1:]
        x=x[:x.index("\'")]
        dict1[i]=x
    dict4=conn.get_collection("cds_dict_detail",{"status":1},{"_id":1,"field_name":1})
    dict4=dict(zip(dict4["_id"].values.tolist(),dict4["field_name"].values.tolist()))
    for i in dict1:
        dict1[i]=dict4[dict1[i]]
    dict5={"华东区域":"南区","日韩区域":"南区","华北区域":"北区","欧洲区域":"北区","华南区域":"南区","港台区域":"南区","华中区域":"南区","西北区域":"北区","西南区域":"南区","东南亚区域":"东南亚-美洲区","北美区域":"东南亚-美洲区","南美区域":"东南亚-美洲区"}
    for i in dict1:
        dict1[i]=dict5[dict1[i]]
    return dict1

if __name__=="__main__":
    print(get_zd())