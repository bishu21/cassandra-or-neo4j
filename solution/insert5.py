import json
import glob
from pprint import pprint
from py2neo import Graph
graph = Graph(password="bishu")
#graph.run("UNWIND range(1, 10) AS n RETURN n, n * n as n_sq")
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
	merge(u:User {auth_screen_name:tweet.author_screen_name})
	merge(t:Tweet {tweetid:tweet.tid})
        
        create(u)-[r:TWEETS]->(t)
        

"""
query5="""
UNWIND {json} as tweet
	create(t:Tweet {tweetid:tweet.tid})
        
        

"""
query1="""
UNWIND {json} as tweet1
UNWIND {replyto_source_id} as y
        match(x:Tweet {tweetid:y})
        with collect(x) as x_list,x
UNWIND x_list as qwe
        merge(q:replytweet{replyid:qwe})
        
"""
query2="""
UNWIND {json} as tweet
        merge(t:Tweet {tweetid:tweet.retweet_source_id})
	merge(f:Tweet {tweetid:tweet.tid})
        create(f)-[r:retweet]->(t)
"""

l=0
print l

graph.run(query0,json = list_tweet)
#graph.run(query2,json = list_tweet)



for i in list_tweet:	
        dict1={}
        l+=1
        print l
        if i['retweet_source_id'] is not None:
               
               graph.run(query2,json =i)
		
	
#graph.run(query0,json = list_tweet)
#graph.run(query1,json = list_tweet)
#graph.run(query2,json = list_tweet)
#graph.run(query3,json = list_tweet)
#graph.run(query4,json = list_tweet)
#graph.run(query5,json = list_tweet)









