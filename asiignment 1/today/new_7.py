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
log.info("drop keyspace...")
session.execute("drop keyspace test5")

log.info("creating keyspace...")
session.execute("""
        CREATE KEYSPACE test5
        WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '2' }
        """)

log.info("setting keyspace...")
session.execute("""use test5""")
log.info("creating table...")

session.execute("""
        CREATE TABLE mytable (
            
            hashtag text,
	    date date,
	    count bigint,	
            PRIMARY KEY (date,count,hashtag)
        );
        """)
#query = SimpleStatement("""
 #       INSERT INTO mytable (tid, author, ttext,taid,tl,tlg,tdate)
#        VALUES (%(key)s, %(a)s, %(b)s)
 #       """, consistency_level=ConsistencyLevel.ONE)

l1 = ['2017-11-26','2017-11-27','2017-11-26']
print(len(l1))
l2 = 0
dict2 ={}
for j in l1:
    data = pd.read_json('/home/bishu/Downloads/workshop_dataset1/'+j+'.json', typ='series')
    l2 = l2 + len(data.index)
    for i in range(len(data.index)):
        df = data.ix[i]

	if df["hashtags"] is not None :
		for k in df["hashtags"] :
			if k :
				if k in dict2.keys():
					dict2[k] = dict2[k]+1;
				else :
					dict2[k]=1;
		

	
for i in dict2.keys():
	
	session.execute(
	 """
		 INSERT INTO mytable (date,hashtag,count)
		 VALUES (%s,%s,%s)
	 """,
	(l1[0],i,dict2[i])
	 )

	
	
	
future = session.execute_async("select count,hashtag from mytable where date='2017-11-26' order by count DESC limit 20")
log.info("count\thashtag")
log.info("---\t----")

try:
	rows = future.result()
except Exception:
	log.exeception()

for row in rows:
	#print (row)
	log.info('%s\t%s',row[0],row[1])

print(l2)
