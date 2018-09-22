Match (u:user)-[:Posts]->(t:tweet) return u.author AS author_name,COLLECT(t.tid) AS TID


Match (u:user)-[:Posts]->(t:tweet)-[:Mentions]->(m:mention) where u.author='real_aadil' return u.author AS author_name,t.tid AS TID,COLLECT(m.Mention) AS ANS



