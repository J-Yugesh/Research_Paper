from py2neo import Graph
from py2neo.data import Node, Relationship
from py2neo.ogm import *
graph = Graph("bolt://localhost:11005", auth=("neo4j", "1234"))

def dns_log():
 query="""
 LOAD CSV WITH HEADERS FROM 'file:///dns.log.csv' AS row
 WITH row.`id.orig_h` as Source_IP, row.`id.resp_h` as Server, row.`id.orig_p` as source_port, row.`id.resp_p` as dest_port, row.ts as time, row.uid as U_Id, row.proto as proto, row.trans_id as trans_id, row.query as query, row.qclass as qclass, row.qclass_name as qclass_name, row.qtype as qtype, row.qtype_name as qtype_name, row.rcode as rcode, row.rcode_name as rcode_name, row.AA as AA, row.TC as TC, row.RD as RD, row.RA as RA, row.Z as Z, row.answers as answers, row.TTLs as TTLs, row.rejected as rejected
 
 MERGE (s_ip: S_IP {Source_Ip: Source_IP})
 MERGE (server: Server {Server: Server})
 MERGE (ans: IP {Ans: answers})
 MERGE (que: Host {Query: query})
 MERGE (dns: DNS {U_Id: U_Id})
  SET dns.proto=proto, dns.trans_id=trans_id, dns.qclass=qclass, dns.qclass_name=qclass_name, dns.qtype=qtype, dns.qtype_name=qtype_name, dns.rcode=rcode, dns.rcode_name=rcode_name, dns.AA=AA, dns.TC=TC, dns.RD=RD, dns.RA=RA, dns.Z=Z, dns.rejected=rejected
 MERGE (s_ip)-[s_rel:HAS_DNS_REQUEST]->(dns)
 MERGE (server)-[d_rel:HAS_DNS_RESPONSE]->(dns)
 MERGE (dns)-[dns_ip:RESOLVED_TO {TTLs:TTLs}]->(ans)
 MERGE (que)-[host_ip:RESOLVED_TO {Time:time}]->(ans)
 MERGE (dns)-[dns_host:HAS_QUERY]->(que)
 MERGE (s_ip)-[ip_host:HAS_QUERY]->(que)
 """
 graph.run(query)
 q1="""
 MATCH (i1:S_IP)-[sc:HAS_DNS_REQUEST]->(c:DNS)<-[ct:HAS_DNS_RESPONSE]-(i2:Server) 
 WITH i1 as host, i2 as server, count(c) as total_connections
 MERGE (host)-[q:DNS_QUERY]-(server) 
 SET q.count = total_connections
 """
 graph.run(q1)
 q2="""
 MATCH (i1:S_IP)-[r:DNS_QUERY]-(i2:Server) 
 WITH count(i2) as x,i1
 SET i1.degree=x
 """
 graph.run(q2)
 q3="""
 MATCH (i1:S_IP)-[r:DNS_QUERY]-(i2:Server) 
 WITH count(i1) as x,i2
 SET i2.degree=x
 """
 graph.run(q3)
 
def main():
 graph.delete_all() #Deletes all the graphs present in this database
 dns_log()

if __name__ == "__main__":
 main()
 
