 create language plpythonu;

-- Step#1. read csv file and make a temporary table
create or replace function load_csv_file
(
    label_name text,
    csv_path text,
    col_count integer,
    delimiter text
)
returns void as $$
try:
   with plpy.subtransaction():
     plpy.execute("CREATE TABLE public.temp_table()")
     for iter in range(1,col_count+1):
        plpy.execute("ALTER TABLE temp_table ADD COLUMN col_"+str(iter)+" text")
     plpy.execute("COPY temp_table FROM '"+csv_path+"' WITH DELIMITER '"+delimiter+"' QUOTE '\"' CSV ")
     col_first = plpy.execute("SELECT col_1 FROM temp_table LIMIT 1")[0]['col_1']
     col_iter = 1
     rv = plpy.execute("SELECT unnest(string_to_array(trim(temp_table::text, '()'), '','')) AS col_names from temp_table where col_1 ='"+col_first+"'")
     columns = rv[0]['col_names'].split(',')
     plpy.info("Column information: "+str(columns))
     for col in columns:
        plpy.execute("ALTER TABLE temp_table RENAME COLUMN col_"+str(col_iter)+" TO "+col)
        col_iter += 1
     plpy.execute("DELETE FROM temp_table WHERE "+col_first+"='"+col_first+"'")
except plpy.SPIError, e:
   plpy.error("Exception occured: %s" % str(e))
$$ language plpythonu;

create or replace function load_csv_file
(
    label_name text,
    csv_path text,
    col_count integer,
    delimiter text,
    quote text
)
returns void as $$
try:
   with plpy.subtransaction():
     plpy.execute("CREATE TABLE public.temp_table()")
     for iter in range(1,col_count+1):
        plpy.execute("ALTER TABLE temp_table ADD COLUMN col_"+str(iter)+" text")
     plpy.execute("COPY temp_table FROM '"+csv_path+"' WITH DELIMITER '"+delimiter+"' QUOTE '"+quote+"' CSV ")
     col_first = plpy.execute("SELECT col_1 FROM temp_table LIMIT 1")[0]['col_1']
     col_iter = 1
     rv = plpy.execute("SELECT unnest(string_to_array(trim(temp_table::text, '()'), '','')) AS col_names from temp_table where col_1 ='"+col_first+"'")
     columns = rv[0]['col_names'].split(',')
     plpy.info("Column information: "+str(columns))
     for col in columns:
        plpy.execute("ALTER TABLE temp_table RENAME COLUMN col_"+str(col_iter)+" TO "+col)
        col_iter += 1
     plpy.execute("DELETE FROM temp_table WHERE "+col_first+"='"+col_first+"'")
except plpy.SPIError, e:
   plpy.error("Exception occured: %s" % str(e))
$$ language plpythonu;

-- Step#2-1. Load data from the temp table and pull them into a graph 
-- [20171227]Schema parameter added
create or replace function csv_to_edge
(
    conn_string text,
    graph_path text,
    label_name text,
    csv_path text,
    schema text,
    col_count integer,
    delimiter text
)
returns void as $$
import psycopg2 as ag

plpy.info(conn_string)
load_query = "SELECT load_csv_file (\'%s\',\'%s\',%d,\'%s\')" % (label_name,csv_path,col_count,delimiter)
try:
   conn1 = ag.connect(conn_string)
   cur1 = conn1.cursor()
   cur1.execute(load_query)
except Exception, e:
   plpy.error("Procedure Execution Error: %s" % str(e))
finally:
   conn1.commit()
   cur1.close()
   conn1.close()

edge_query = 'MATCH '

try:
   conn2 = ag.connect(conn_string)
   cur2 = conn2.cursor()

   columns = schema.split(',')
   for col in columns:
     col_n_type = col.split(';')
     if col_n_type[1].lower() == '_start':
        cur2.execute("CREATE PROPERTY INDEX ON "+col_n_type[2]+"("+col_n_type[3]+")")
        edge_query = edge_query+"(a:"+col_n_type[2]+" {"+col_n_type[3]+":to_jsonb(t."+col_n_type[0]+")}),"
     elif col_n_type[1].lower() == '_end':
        cur2.execute("CREATE PROPERTY INDEX ON "+col_n_type[2]+"("+col_n_type[3]+")")
        edge_query = edge_query+"(b:"+col_n_type[2]+" {"+col_n_type[3]+":to_jsonb(t."+col_n_type[0]+")})"
     elif col_n_type[1].lower() == 'int':
        cur2.execute("ALTER TABLE temp_table ALTER COLUMN "+col_n_type[0]+" TYPE INTEGER USING (trim("+col_n_type[0]+")::integer)")
     elif col_n_type[1].lower() == 'float':
        cur2.execute("ALTER TABLE temp_table ALTER COLUMN "+col_n_type[0]+" TYPE FLOAT USING (trim("+col_n_type[0]+")::float)")
    
   edge_query = "LOAD FROM temp_table AS t "+edge_query+" CREATE (a)-[:"+label_name+"]->(b)"
   cur2.execute("SET graph_path="+graph_path)
   cur2.execute(edge_query)
except Exception, e:
   plpy.error("Cypher Execution Error: %s" % str(e))
finally:
   conn2.commit()
   cur2.close()
   conn2.close()

plpy.execute("DROP TABLE temp_table CASCADE")
$$ language plpythonu;

create or replace function csv_to_edge
(
    conn_string text,
    graph_path text,
    label_name text,
    csv_path text,
    schema text,
    col_count integer,
    delimiter text,
    quote text
)
returns void as $$
import psycopg2 as ag

plpy.info(conn_string)
load_query = "SELECT load_csv_file (\'%s\',\'%s\',%d,\'%s\',\'%s\')" % (label_name,csv_path,col_count,delimiter,quote)
try:
   conn1 = ag.connect(conn_string)
   cur1 = conn1.cursor()
   cur1.execute(load_query)
except Exception, e:
   plpy.error("Procedure Execution Error: %s" % str(e))
finally:
   conn1.commit()
   cur1.close()
   conn1.close()

edge_query = 'MATCH '

try:
   conn2 = ag.connect(conn_string)
   cur2 = conn2.cursor()

   columns = schema.split(',')
   for col in columns:
     col_n_type = col.split(';')
     if col_n_type[1].lower() == '_start':
        cur2.execute("CREATE PROPERTY INDEX ON "+col_n_type[2]+"("+col_n_type[3]+")")
        edge_query = edge_query+"(a:"+col_n_type[2]+" {"+col_n_type[3]+":to_jsonb(t."+col_n_type[0]+")}),"
     elif col_n_type[1].lower() == '_end':
        cur2.execute("CREATE PROPERTY INDEX ON "+col_n_type[2]+"("+col_n_type[3]+")")
        edge_query = edge_query+"(b:"+col_n_type[2]+" {"+col_n_type[3]+":to_jsonb(t."+col_n_type[0]+")})"
     elif col_n_type[1].lower() == 'int':
        cur2.execute("ALTER TABLE temp_table ALTER COLUMN "+col_n_type[0]+" TYPE INTEGER USING (trim("+col_n_type[0]+")::integer)")
     elif col_n_type[1].lower() == 'float':
        cur2.execute("ALTER TABLE temp_table ALTER COLUMN "+col_n_type[0]+" TYPE FLOAT USING (trim("+col_n_type[0]+")::float)")
    
   edge_query = "LOAD FROM temp_table AS t "+edge_query+" CREATE (a)-[:"+label_name+"]->(b)"
   cur2.execute("SET graph_path="+graph_path)
   cur2.execute(edge_query)
except Exception, e:
   plpy.error("Cypher Execution Error: %s" % str(e))
finally:
   conn2.commit()
   cur2.close()
   conn2.close()

plpy.execute("DROP TABLE temp_table CASCADE")
$$ language plpythonu;