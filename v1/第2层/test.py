from connect import Connect_Nebula

class Test:

    def __init__(self,config):
        self.config=config
        
    def test1(self):
        conn=Connect_Nebula(self.config)
        conn.open_nebula()
        client=conn.client
        client.execute("USE G_2025_08_20")
        result1=client.execute("match (v:server) return id(v)").as_data_frame().values.tolist()
        result2=client.execute("match (v:network) return id(v)").as_data_frame().values.tolist()
        conn.close_nebula()
        result1=set([i[0] for i in result1])
        result2=set([i[0] for i in result2])
        result=set.union(result1,result2)
        print(result)
    
if __name__=="__main__":
    config={
        "connection":{
            "TIMES":1000,
            "TIME":0.1
        },
        "nebula":{
            "HOST":"localhost",
            "PORT":7000,
            "USERNAME":"root",
            "PASSWORD":"cds-cloud@2017",
            "MIN_CONNECTION_POOL_SIZE":1,
            "MAX_CONNECTION_POOL_SIZE":260
        }
    }
    m=Test(config)
    m.test1()