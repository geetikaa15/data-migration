import cassandra
import pandas as pd
import numpy as np
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

auth_provider1 = PlainTextAuthProvider(username='az_orders_stage', password='s9dCLwG8YaDr')
primary_kcp_cluster = Cluster(['10.34.18.7'],port = 9042,auth_provider=auth_provider1)  
session2 = primary_kcp_cluster.connect('orders')

files = 1
#directory ='C:/Users/ibm8980/Desktop/COLT'
try:      
    query = "select order_id, entity,timestamp from orders.kcp_events WHERE timestamp > '2019-11-09 11:59:09.492' and entity ='CreateOrder' limit 10 ALLOW FILTERING"
    query_result1 = session2.execute(query) 
    #print(list(query_result1))
    df_result = pd.DataFrame(query_result1)
    print(df_result)  
        
    # Incrementing the file                            
    print("time finished:" +str(time.time()))

except Exception as e:
   print("No File found for Deletion")
   files += 1 
