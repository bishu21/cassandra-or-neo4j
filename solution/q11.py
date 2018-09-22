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
                #print filename
               
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
    merge(u:User {auth_screen_name:tweet.author_screen_name})
    merge(t:Tweet {tweetid:tweet.tid})
        
        create(u)-[r:TWEETS]->(t)
        

"""

query2="""
UNWIND {json} as tweet
        merge(u:User {auth_screen_name:tweet.author_screen_name1})
    merge(t:Tweet {tweetid:tweet.reply_id})
    merge(T:Tweet {tweetid:tweet.tid1})    
        create(T)<-[r1:TWEETS]-(u)-[r:reply]->(t)
"""

l=0
print l

graph.run(query0,json = list_tweet)
#graph.run(query2,json = list_tweet)



for i in list_tweet: 

        if i['replyto_source_id'] is not None:
                print(l)
                l=l+1
                dict2={}
                dict2['author_screen_name1'] =i['author_screen_name']
                dict2['tid1']=i['tid']     
                dict2['reply_id']=i['replyto_source_id']
                graph.run(query2,json = dict2)
        
    
#graph.run(query0,json = list_tweet)
#graph.run(query1,json = list_tweet)
#graph.run(query2,json = list_tweet)
#graph.run(query3,json = list_tweet)
#graph.run(query4,json = list_tweet)
#graph.run(query5,json = list_tweet)









