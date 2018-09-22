from neo4j.v1 import GraphDatabase

uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "bishu"))

def print_friends_of(name):
    with driver.session() as session:
        with session.begin_transaction() as tx:
            for record in tx.run("MATCH (a:Database)-[:SAYS]->(f) "
                                 "WHERE a.name = {name} "
                                 "RETURN f.name", name=name):
                print(record["f.name"])

print_friends_of("Neo4j")




''''
Merge (r:user {author:$au} )-[p:POSTS {Date:$date1}]->(s:tweet {tid:$ti})
