CREATE LANGUAGE plpythonu;

CREATE OR REPLACE FUNCTION girvan_newman_algo
(
   conn_string text
)
returns void as $$
import time
import networkx as nx
import psycopg2 as ag
import community

def check_table_existence():
   res = plpy.execute("SELECT relname FROM pg_stat_all_tables WHERE relname='louvain_method_result'").nrows()
   if (res == 0):
      plpy.execute("CREATE TABLE louvain_method_result (cluster_id integer, nodes text[], primary key (cluster_id))")
   else:
      plpy.execute("TRUNCATE TABLE louvain_method_result")

check_table_existence()

start = time.time()

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

except Exception, e:
   plpy.error("Procedure Execution Error")
finally:
   conn.commit()
   cur.close()
   conn.close()

plpy.info("Number of nodes: "+str(G.number_of_nodes())+", Number of edges: "+str(G.number_of_edges()))

plan = plpy.prepare("INSERT INTO louvain_method_result VALUES ($1,$2)",["int","text[]"])
cluster = 0

plpy.info("GRAPH ANALYSIS IN PROGRESS")
partition = community.best_partition(G)
for com in set(parition.values()):
   cluster = cluster + 1
   list_nodes = [nodes for nodes in partition.keys() if partition[nodes] == com]
   plpy.execute(plan,[cluster,list_nodes])

done = time.time()
elapsed = done - start

plpy.info("Time Elapsed: "+str(elapsed))

$$ language plpythonu;
