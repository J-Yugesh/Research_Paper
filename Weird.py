from py2neo import Graph
from py2neo.data import Node, Relationship
from py2neo.ogm import *
graph = Graph("bolt://localhost:11005", auth=("neo4j", "1234"))

def weird_log():
 query="""
 LOAD CSV WITH HEADERS FROM 'file:///weird.log.csv' AS row
 WITH row.`id.orig_h` as s_IP, row.`id.resp_h` as d_IP, row.`id.orig_p` as source_port, row.`id.resp_p` as dest_port, row.ts as time, row.uid as U_Id, row.name as name, row.notice as notice, row.peer as peer
 MERGE (s_ip: S_IP {IP: s_IP})
 MERGE (d_ip: D_IP {IP: d_IP})
 MERGE (weird: Weird {U_Id: U_Id})
  SET weird.time=time, weird.name=name, weird.notice=notice, weird.peer=peer
 
 MERGE (s_ip)-[s_rel:REQUEST {source_port:source_port}]->(weird)
 MERGE (d_ip)-[d_rel:RESPONSE {dest_port:dest_port}]->(weird)
 """
 graph.run(query)
 q1="""
 MATCH (i1:S_IP)-[rq:REQUEST]->(w:Weird)<-[rp:RESPONSE]-(i2:D_IP) 
 WITH i1 as Source, i2 as Destination, count(w) as total_connections 
 MERGE (Source)-[q:DIRECT_CONNECTION]-(Destination) 
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
 weird_log()
 
if __name__ == "__main__":
 main()
 
