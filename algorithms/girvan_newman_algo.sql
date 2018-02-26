CREATE LANGUAGE plpythonu;

CREATE OR REPLACE FUNCTION girvan_newman_algo
(
   conn_string text
)
returns void as $$
import networkx as nx
import psycopg2 as ag
from networkx import edge_betweenness_centrality as betweenness
from networkx.algorithms import community

def check_table_existence():
   res = plpy.execute("SELECT relname FROM pg_stat_all_tables WHERE relname='girvan_newman_result'").nrows()
   if (res == 0):
      plpy.execute("CREATE TABLE girvan_newman_result (cluster_id integer, nodes text[], primary key (cluster_id))")

def most_central_edge(G):
   centrality = betweenness(G,weight='weight')
   return max(centrality, key=centrality.get)

check_table_existence()

G = nx.Graph()
query = "MATCH (n1)-[r1]->(n2) WHERE id(n1) <> id(n2) RETURN id(n1) AS node, id(n2) AS neighbor"
plpy.info(query)
try:
   conn = ag.connect(conn_string)
   cur = conn.cursor()

   cur.execute(query)
   while True:
      records = cur.fetchmany(size=10000)

      if not records:
        break

      G.add_edges_from([tuple(r) for r in records])

   plpy.info("Number of nodes: "+str(G.number_of_nodes())+", Number of edges: "+str(G.number_of_edges()))
   comp = community.girvan_newman(G, most_valuable_edge=most_central_edge)
   plpy.execute("TRUNCATE TABLE girvan_newman_result")
   plan = plpy.prepare("INSERT INTO girvan_newman_result VALUES ($1,$2)",["int","text[]"])
   cluster = 1

   for nodes in tuple(sorted(c) for c in next(comp)):
      '''plpy.info("cluster_id: "+str(cluster))'''
      plpy.execute(plan, [cluster,nodes])
      cluster += 1

except Exception, e:
   plpy.error("Procedure Execution Error")
finally:
   conn.commit()
   cur.close()
   conn.close()

$$ language plpythonu;
