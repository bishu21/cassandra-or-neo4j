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
            tid text,
	    date date,
            PRIMARY KEY (date,tid)
        );
        """)
#query = SimpleStatement("""
 #       INSERT INTO mytable (tid, author, ttext,taid,tl,tlg,tdate)
#        VALUES (%(key)s, %(a)s, %(b)s)
 #       """, consistency_level=ConsistencyLevel.ONE)

l1 = glob.glob("/home/bishu/Downloads/workshop_dataset1/*.json")
print(len(l1))
l2 = 0
for j in l1:
    data = pd.read_json(j, typ='series')
    l2 = l2 + len(data.index)
    for i in range(len(data.index)):
        df = data.ix[i]
	
	
	print(df["tid"],df['date'])
	
	session.execute(
	 """
		 INSERT INTO mytable (tid,date)
		 VALUES (%s,%s)
	 """,
	(df["tid"],df['date'])
	 )

future = session.execute_async("delete * from mytable where date='2017-11-26'")
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
