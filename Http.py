from py2neo import Graph
from py2neo.data import Node, Relationship
from py2neo.ogm import *
graph = Graph("bolt://localhost:11005", auth=("neo4j", "1234"))

def http_log():
 query="""
 LOAD CSV WITH HEADERS FROM 'file:///http.log.csv' AS row
 WITH row.`id.orig_h` as s_IP, row.`id.resp_h` as d_IP, row.`id.orig_p` as source_port, row.`id.resp_p` as dest_port, row.ts as time, row.uid as U_Id, row.trans_depth as trans_depth, row.method as method, row.host as host, row.uri as uri, row.version as version, row.user_agent as user_agent, row.request_body_len as request_body_len, row.response_body_len as response_body_len, row.status_msg as status_msg, row.resp_fuids as resp_fuids, row.resp_mime_types as resp_mime_types, row.referrer as referrer
 
 MERGE (s_ip: S_IP {s_IP: s_IP})
 MERGE (d_ip: D_IP {d_IP: d_IP})
 MERGE (u_agent: User_Agent {User_Agent: user_agent})
 MERGE (hosts: Host {Host: host})
 MERGE (uris: URI {Uri: uri})
 MERGE (ref: Referrer {Referrer: referrer})
 MERGE (http: HTTP {U_Id: U_Id})
  SET http.time=time, http.trans_depth=trans_depth, http.method=method, http.version=version, http.request_body_len=request_body_len, http.response_body_len=response_body_len, http.status_msg=status_msg, http.resp_fuids=resp_fuids, http.resp_mime_types=resp_mime_types
 
 MERGE (s_ip)-[s_rel:REQUESTS {source_port:source_port}]->(http)
 MERGE (d_ip)-[d_rel:RESPONDS {dest_port:dest_port}]->(http)
 MERGE (s_ip)-[uagent: HAS_USERAGENT]->(u_agent)
 MERGE (http)-[u1agent: HAS_USERAGENT]->(u_agent)
 MERGE (http)-[hashost: HAS_HOST]->(hosts)
 MERGE (http)-[hasuri: HAS_URI]->(uris)
 MERGE (http)-[hasref: HAS_REFERRER]->(ref)
 MERGE (hosts)-[hasuri1: HAS_URI]->(uris)
 MERGE (uris)-[hasreferrer: HAS_REFERRER]->(ref)
 """
 graph.run(query)
 q1="""
 MATCH (i1:S_IP)-[sc:REQUESTS]->(c:HTTP)<-[ct:RESPONDS]-(i2:D_IP) 
 WITH i1 as host, i2 as Destination, count(c) as total_connections
 MERGE (host)-[q:DIRECT_CONNECTION]-(Destination) 
 SET q.count = total_connections
 """
 graph.run(q1)
 q2="""
 MATCH (i1:S_IP)-[r:DIRECT_CONNECTION]-(i2:D_IP) 
 WITH count(i2) as x,i1
 SET i1.degree=x
 """
 graph.run(q2)
 q3="""
 MATCH (i1:S_IP)-[r:DIRECT_CONNECTION]-(i2:D_IP) 
 WITH count(i1) as x,i2
 SET i2.degree=x
 """
 graph.run(q3)
 
def main():
 graph.delete_all() #Deletes all the graphs present in this database
 http_log()

if __name__ == "__main__":
 main()
 
