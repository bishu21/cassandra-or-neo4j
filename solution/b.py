import json

#from bottle import get, run, request, response, static_file
from py2neo import Graph

graph = Graph("http://neo4j:bishu@localhost:7474")

import pandas as pd
import glob
import time
import datetime




l1 = glob.glob("/home/bishu/Downloads/workshop_dataset1/*.json")
print(len(l1))
l2 = 0
graph.run('''MATCH (n)
DETACH DELETE n''')
graph.run('''CREATE CONSTRAINT ON (name:user) ASSERT name.Name IS UNIQUE
''')
for j in l1:
    data = pd.read_json(j, typ='series')
    l2 = l2 + len(data.index)
    print(j)
    for i in range(len(data.index)):
        df = data.ix[i]
        
        graph.run(
        '''
        		MERGE (node1:user {author: $au})
				MERGE (node2:tweet{tid: $ti})
				MERGE (node1)-[:Posts]-(node2)
        	''',

        parameters =  {'ti':df["tid"], 'au':df["author_screen_name"]}
        )
        #print(df['mentions'])
        if df['mentions'] is not None :
	        for k in df['mentions'] :
	        	if k :
		        	graph.run('''
		        		MERGE (node1:tweet {tid: $ti})
						MERGE (node2:mention {Mention: $ment})
						MERGE (node1)-[:Mentions]-(node2)
		        		''',
		        		parameters =  {'ti':df["tid"], 'ment':k})
	       
#result=graph.run("Match (u:user)-[:POSTS]->(t:tweet) return u.author AS author_name,COLLECT(t.tid) AS TID")
#for author_name,TID in result:
#	print(author_name  ,  TID)
#Match (u:user)-[:POSTS]->(t:tweet)-[:Mentions]->(m:mention) return u.author AS author_name,COLLECT(t.tid) AS TID,m.Mention As ans
graph.run('''MATCH (n) DETACH DELETE n
DROP CONSTRAINT ON (name:Person) ASSERT name.Name is unique''')
print(l2)

