import json
import glob
from pprint import pprint
from py2neo import Graph
graph = Graph(password="bishu")
#graph.run("UNWIND range(1, 10) AS n RETURN n, n * n as n_sq")
graph.run('''MATCH (n)
DETACH DELETE n''')
list_tweet = []
k=0
for filename in glob.glob("*.json"):
	with open (filename) as json_data:
		j = json.load(json_data)
                print filename
                k+=1
                print k
                for i in j.values():
			list_tweet.append(i)

x = """
CREATE CONSTRAINT ON (s:USER) ASSERT ( s.auth_screen_name) IS UNIQUE
"""
y = """
CREATE CONSTRAINT ON (s:TWEET) ASSERT ( s.tweetid) IS UNIQUE
"""
z = """
CREATE CONSTRAINT ON (s:LOCATION) ASSERT ( s.Location) IS UNIQUE
"""

graph.run(x)
graph.run(y)
graph.run(z)
query0="""
UNWIND {json} as tweet
	merge(u:User {auth_screen_name:tweet.user_men})
	merge(t:Tweet {tweetid:tweet.tid})
        
        create(u)-[r:mentions]->(t)
        

"""

query2="""
UNWIND {json} as tweet
        merge(t:Tweet {tweetid:tweet.tid})
	merge(h:Hashtag {hashtag:tweet.hash})
        create(h)-[r:Hashtags]->(t)
"""

l=0
print l

#graph.run(query0,json = list_tweet)

for i in list_tweet: 

        if i['mentions'] is not None:
           for k in i['mentions']:
                dict2={}
                dict2['tid'] =i['tid']
                     
                dict2['user_men']=k
                graph.run(query0,json = dict2)
                     

for i in list_tweet:	
        dict1={}
       
        l+=1
        print l

        if i['hashtags'] is not None:
		for j in i['hashtags'] :
                        dict1={}
                        dict1['tid']=i['tid']   
                        dict1['hash']=j     
                        graph.run(query2,json =dict1)       
                                      
        			             
                        
                        
	#graph.run(query2,json = list_dict)
		#print(i['mentions'])
#graph.run(query0,json = list_tweet)
#graph.run(query1,json = list_tweet)
#graph.run(query2,json = list_tweet)
#graph.run(query3,json = list_tweet)
#graph.run(query4,json = list_tweet)
#graph.run(query5,json = list_tweet)








