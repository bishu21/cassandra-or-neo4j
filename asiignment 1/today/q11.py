from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel
import pandas as pd
import glob
import time
import datetime


import logging

log = logging.getLogger()
log.setLevel('DEBUG')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)


cluster = Cluster(['127.0.0.1'])
session = cluster.connect()

session.execute("drop keyspace test5")

session.execute("""
        CREATE KEYSPACE test5
        WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '2' }
        """)

session.execute("""use test5""")


session.execute("""
        CREATE TABLE mytable (
            
        men_loc text,
        date date,
        count bigint,   
            PRIMARY KEY (date,count,men_loc)
        );
        """)
#query = SimpleStatement("""
 #       INSERT INTO mytable (tid, author, ttext,taid,tl,tlg,tdate)
#        VALUES (%(key)s, %(a)s, %(b)s)
 #       """, consistency_level=ConsistencyLevel.ONE)

l1 = ['2017-12-30']
print(len(l1))
l2 = 0
dict2 ={}
for j in l1:
    data = pd.read_json('/home/bishu/Downloads/workshop_dataset1/'+j+'.json', typ='series')
    l2 = l2 + len(data.index)
    for i in range(len(data.index)):
        df = data.ix[i]

        if(df['location']): 
            if df["mentions"] is not None :
                for k in df['mentions'] :
                    if k :
                        k1=k+df['location']
                        print(k1)
                        if k1 in dict2.keys():
                            dict2[k1] = dict2[k1]+1;
                        else :
                            print(k1)
                            dict2[k1]=1;
                    

print(len(dict2))    
for i in dict2.keys():
    
    session.execute(
     """
         INSERT INTO mytable (date,men_loc,count)
         VALUES (%s,%s,%s)
     """,
    (l1[0],i,dict2[i])
     )

    
 #select * from test5.mytable where date =  '2017-12-30' order by count DESC; 

future = session.execute_async("select men_loc,count from test5.mytable where date =  '2017-12-30' order by count DESC;")
log.info("men_loc\tcount")
log.info("---\t----")

try:
    rows = future.result()
except Exception:
    log.exeception()

for row in rows:
    #print (row)
    log.info('%s\t%s',row[0],row[1])


print(l2)
