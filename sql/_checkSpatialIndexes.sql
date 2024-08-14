SELECT 
    t.relname AS table_name,
    i.relname AS index_name,
    a.attname AS column_name
FROM 
    pg_class t,
    pg_class i,
    pg_index ix,
    pg_attribute a
WHERE 
    t.oid = ix.indrelid
    AND i.oid = ix.indexrelid
    AND a.attrelid = t.oid
    AND a.attnum = ANY(ix.indkey)
    AND t.relkind = 'r'
    AND t.relname IN ('bag_pand', 'bag_vbo', 'cdwaste')
    AND a.attname = 'geom_28992'
    AND ix.indisprimary = false
ORDER BY 
    t.relname, i.relname;
