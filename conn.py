from py2neo import Graph
from py2neo.data import Node, Relationship
from py2neo.ogm import *
graph = Graph("bolt://localhost:11005", auth=("neo4j", "1234"))


def conn_log():
 query="""
 LOAD CSV WITH HEADERS FROM 'file:///conn.log.csv' AS row
 WITH row.`id.orig_h` as Source_IP, row.`id.resp_h` as Destination_IP, row.`id.orig_p` as source_port, row.`id.resp_p` as dest_port, row.ts as time, row.uid as Unique_Id, row.proto as proto, row.service as service, row.duration as duration, row.orig_bytes as orig_bytes, row.resp_bytes as resp_bytes, row.conn_state as conn_state, row.local_orig as local_orig, row.missed_bytes as missed_bytes, row.history as history, row.orig_pkts as orig_pkts, row.orig_ip_bytes as orig_ip_bytes, row.resp_pkts as resp_pkts, row.tunnel_parents as tunnel_parents, row.orig_cc as orig_cc, row.resp_cc as resp_cc
 MERGE (s_ip: S_IP {Source_Ip: Source_IP})
 MERGE (d_ip: D_IP {Destination_IP: Destination_IP})
 MERGE (p: Connection {Unique_Id: Unique_Id})
  SET p.time=time, p.proto=proto, p.service=service, p.duration=duration, p.orig_bytes=orig_bytes, p.resp_bytes=resp_bytes, p.conn_state=conn_state, p.local_orig=local_orig, p.missed_bytes=missed_bytes, p.history=history, p.orig_pkts=orig_pkts, p.orig_ip_bytes=orig_ip_bytes, p.resp_pkts=resp_pkts, p.tunnel_parents=tunnel_parents, p.orig_cc=orig_cc, p.resp_cc=resp_cc
 MERGE (s_ip)-[s_rel:STARTS_CONNECTION {source_port: source_port}]->(p)
 MERGE (p)-[d_rel:CONNECTS_TO {dest_port: dest_port}]->(d_ip)
 """
 graph.run(query)
 q1="""
 MATCH (i1:S_IP)-[sc:STARTS_CONNECTION]->(c:Connection)-[ct:CONNECTS_TO]->(i2:D_IP) 
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
 conn_log()
 
if __name__ == "__main__":
 main()
 
