CREATE EXTENSION pg_prewarm;

CREATE OR REPLACE FUNCTION ag_prewarm(graph_path text) RETURNS bigint
AS $$
SELECT pg_prewarm(c.oid) FROM pg_class c
LEFT JOIN pg_namespace n
ON n.oid = c.relnamespace
WHERE nspname = graph_path
AND (relkind = 'i' OR relkind = 'r');
$$ LANGUAGE SQL;