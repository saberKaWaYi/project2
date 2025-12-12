from connect import Connect_Clickhouse

if __name__=="__main__":
    config={
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
    conn=Connect_Clickhouse(config)
    sql="select conf_detail from network_conf.network_conf_detail where sync_id=4042071"
    result=conn.client.execute(sql)[0][0]
    print(result)