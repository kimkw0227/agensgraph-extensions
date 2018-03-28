CREATE LANGUAGE plpythonu;

CREATE OR REPLACE FUNCTION page_rank_algo
(
   conn_string text
)
returns void as $$
import time
import networkx as nx
import psycopg2 as ag
import operator

def check_table_existence():
   res = plpy.execute("SELECT relname FROM pg_stat_all_tables WHERE relname='page_rank_result'").nrows()
   if (res == 0):
      plpy.execute("CREATE TABLE page_rank_result (node_id text, rank_value float, primary key (node_id))")
   else:
      plpy.execute("TRUNCATE TABLE page_rank_result")

check_table_existence()

start = time.time()

G = nx.DiGraph()
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

plan = plpy.prepare("INSERT INTO page_rank_result VALUES ($1,$2)",["text","float"])
cluster = 0

plpy.info("GRAPH ANALYSIS IN PROGRESS")
rank_result = nx.pagerank(G, alpha=0.85)

sorted_rank_result = sorted(rank_result.items(),key=operator.itemgetter(1))
sorted_rank_result.reverse()

final_rank_result = []

for i in range(0,25):
   final_rank_result.append(sorted_rank_result[i])
 
for node_id,rank_value in final_rank_result:
   plpy.execute(plan,[node_id,rank_value])

done = time.time()
elapsed = done - start

plpy.info("Time Elapsed: "+str(elapsed))

$$ language plpythonu;
