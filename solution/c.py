import json

#from bottle import get, run, request, response, static_file
from neo4j.v1 import GraphDatabase

uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "bishu"))

import pandas as pd
import glob
import time
import datetime




l1 = glob.glob("/home/bishu/Downloads/workshop_dataset1/*.json")
print(len(l1))
l2 = 0
with driver.session() as session:
    with session.begin_transaction() as tx:
        tx.run('''MATCH (n) DETACH DELETE n''')
for j in l1:
    data = pd.read_json(j, typ='series')
    l2 = l2 + len(data.index)
    print(j)
    for i in range(len(data.index)):
        df = data.ix[i]
        with driver.session() as session:
            with session.begin_transaction() as tx:
                tx.run(
                '''
                		MERGE (node1:user {author: $au})
        				MERGE (node2:tweet{tid: $ti})
        			    MERGE (node1)-[:Posts]-(node2)
                	''',

                parameters =  {'ti':df["tid"], 'au':df["author_screen_name"]}
                )
        
        if df['mentions'] is not None :
	        for k in df['mentions'] :
	        	if k :
                    with driver.session() as session:
                        with session.begin_transaction() as tx:
        		        	tx.run('''
        		        		MERGE (node1:tweet {tid: $ti})
        						MERGE (node2:mention {Mention: $ment})
        						MERGE (node1)-[:Mentions]-(node2)
        		        		''',
        		        		parameters =  {'ti':df["tid"], 'ment':k})
        	       
#result=graph.run("Match (u:user)-[:POSTS]->(t:tweet) return u.author AS author_name,COLLECT(t.tid) AS TID")
#for author_name,TID in result:
#	print(author_name  ,  TID)
#Match (u:user)-[:POSTS]->(t:tweet)-[:Mentions]->(m:mention) return u.author AS author_name,COLLECT(t.tid) AS TID,m.Mention As ans
print(l2)

