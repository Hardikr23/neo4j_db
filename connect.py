from py2neo import Graph,Node
from config import neo_uri, neo_user, neo_pwd

uri= neo_uri
user= neo_user
pwd= neo_pwd

def query_neo4j(query):
    graph = Graph(uri, auth=(user, pwd), port=7474)
    result=graph.run(query).data()
    return result

query="MATCH (n:asset) RETURN n LIMIT 1"
result=query_neo4j(query)
print(result)