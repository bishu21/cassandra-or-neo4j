from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel
import pandas as pd
import glob
import time
import datetime

cluster = Cluster(['127.0.0.1'])
session = cluster.connect()

session.execute("drop keyspace test4")

session.execute("""
        CREATE KEYSPACE test4
        WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '2' }
        """)
session.execute("""use test4""")
session.execute("""
        CREATE TABLE mytable (
            tid text,
            author text,
            ttext text,
            taid text,
            tl text,
            tlg text,
            tdate timestamp,
            PRIMARY KEY ( (author),tdate,tid)
        )WITH CLUSTERING ORDER BY (tdate DESC);
        """)
#query = SimpleStatement("""
 #       INSERT INTO mytable (tid, author, ttext,taid,tl,tlg,tdate)
#        VALUES (%(key)s, %(a)s, %(b)s)
 #       """, consistency_level=ConsistencyLevel.ONE)

l1 = glob.glob("/home/saurav/Downloads/Database_Assaignment1/workshop_dataset1/*.json")
print(len(l1))
l2 = 0
for j in l1:
    data = pd.read_json(j, typ='series')
    l2 = l2 + len(data.index)
    for i in range(len(data.index)):
        df = data.ix[i]
        session.execute(
          """
                 INSERT INTO mytable (tid, author, ttext,taid,tl,tlg,tdate)
                 VALUES (%s, %s, %s, %s, %s, %s, %s)
          """,
          (df["tid"], df["author"], df["tweet_text"], df["author_id"], df["location"], df["lang"],df["datetime"])
        )
        #myDate = "2014-08-01 04:41:52,117"
        #session.execute(query, dict(key="key%d" % i, a='a', b='b'))
        #session.execute(prepared.bind(("%s" % df["tid"], "%s" % df["author"],"%s" % df["tweet_text"],"%s" % df["author_id"],"%s" % df["location"],"%s" % df["lang"],"%s" % mydate)))

print(l2)
