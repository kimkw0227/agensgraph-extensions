CREATE OR REPLACE FUNCTION graphml_writer
(
	conn_string text,
	graph_path text,
	cypher text,
	filename text
) 
RETURNS bigint
AS $$
from pygraphml import Graph
from pygraphml import GraphMLParser
import psycopg2 as ag

g = Graph(name=graph_path)

try:
   conn = ag.connect(conn_string)
   cur = conn.cursor()
   cur.execute(cypher)
   for i in cur: 
      n1 = g.add_node(i[0])
      n2 = g.add_node(i[2])
      g.add_edge(n1,n2,directed='true')

except Exception, e:
   plpy.error("Procedure Execution Error: %s" % str(e))
finally:
   conn.commit()
   cur.close()
   conn.close()

parser = GraphMLParser()
parser.write(g, filename)

$$ LANGUAGE plpythonu