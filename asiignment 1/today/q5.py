from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel
import pandas as pd
import glob
import time
import datetime
import logging



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
            
            hashtags text,

	       date1 date,
           count bigint,
            PRIMARY KEY ((hashtags),count,date1)
        );
        """)
#query = SimpleStatement("""
 #       INSERT INTO mytable (tid, author, ttext,taid,tl,tlg,tdate)
#        VALUES (%(key)s, %(a)s, %(b)s)
 #       """, consistency_level=ConsistencyLevel.ONE)

l1 = glob.glob("/home/bishu/Downloads/workshop_dataset1/*.json")
print(len(l1))
l2 = 0
dict2 ={}
for j in l1:
    data = pd.read_json(j, typ='series')
    l2 = l2 + len(data.index)
    r=0
    for i in range(len(data.index)):
        df = data.ix[i]
	    
        if  ( df['date'] == "2018-01-15" or df['date'] == "2018-01-16" or df['date'] == "2018-01-17" ):
           
            
    	    if df['hashtags'] is not None :
    		  for k in df['hashtags'] :
                    if (k=="AUSOpen"):
                       r=r+1; 
                       
    			
    if(r):			 	
        session.execute(
     	 """
    		 INSERT INTO mytable (hashtags,date1,count)
    		 VALUES (%s, %s,%s)
     	 """,
      	('AUSOpen',df['date'],r)
		       		 )

#select * from test4.mytable where hashtags='AUSOpen' order by count DESC;