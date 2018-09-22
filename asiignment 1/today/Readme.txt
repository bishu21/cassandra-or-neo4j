'Question 5 python code and Schema explain below : '

Question 5 Schema, I made Partition key is hashtag beacuse this hashtag will used in 'where' Clause of query . And for clustor key, I used count beacuse we have to sort the tuples in decresing order of count so ,I made count as clustor key.
and for uniqueness the row, used date also.

In python code - first i connect with cassandra server.
then I made a keyspace whose name is "test4"
then I used table with name is "mytable"
then  glob.glob used to get all files from dataset.

then I insert only those Hashtags which is required in table then used dictionary which count the hashtags.


'Question 11 python code and Schema explain below : '

Question 11 Schema, I made Partition key is date beacuse this date will used in 'where' Clause of the query . And for clustor key, I used count beacuse we have to sort the tuples in decresing order of count so ,I made count as clustor key.
and for uniqueness the row, used date also.

In python code - first i connect with cassandra server.
then I made a keyspace whose name is "test4"
then I used table with name is "mytable"
then  glob.glob used to get all files from dataset.

then I made dictionary whose key is string (mention+location) and value is frequency of this .
 