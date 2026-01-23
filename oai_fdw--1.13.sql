CREATE FUNCTION oai_fdw_handler() 
  RETURNS fdw_handler AS 'MODULE_PATHNAME' 
  LANGUAGE C STRICT;

CREATE TYPE oai_node AS (
  code text,
  description text
);

CREATE TYPE OAI_IdentityNode AS (
  name text,
  description text
);

CREATE TYPE OAI_Set AS (
  setspec text,
  setname text
);

CREATE TYPE OAI_MetadataFormat AS (
  metadataPrefix text,
  schema text,
  metadataNamespace text
);

CREATE FUNCTION OAI_Version()
  RETURNS text AS 'MODULE_PATHNAME', 'oai_fdw_version'
  LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
  
  COMMENT ON FUNCTION OAI_Version() IS 'Shows current version of oai_fdw and its major libraries';

CREATE FUNCTION OAI_ListMetadataFormats(text)
  RETURNS SETOF OAI_MetadataFormat AS 'MODULE_PATHNAME', 'oai_fdw_listMetadataFormats'
  LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
  
  COMMENT ON FUNCTION OAI_ListMetadataFormats(text) IS 'Lists all metadata formats of a given OAI-PMH server';

CREATE FUNCTION OAI_ListSets(text)
  RETURNS SETOF OAI_Set AS 'MODULE_PATHNAME', 'oai_fdw_listSets'
  LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
  
  COMMENT ON FUNCTION OAI_ListSets(text) IS 'Lists all sets of a given OAI-PMH server';
  
CREATE FUNCTION OAI_Identify(text)
  RETURNS SETOF OAI_IdentityNode AS 'MODULE_PATHNAME', 'oai_fdw_identity'
  LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
  
  COMMENT ON FUNCTION OAI_Identify(text) IS 'Lists settings of a given OAI-PMH server';
  
CREATE FUNCTION oai_fdw_validator(text[], oid)
  RETURNS void AS 'MODULE_PATHNAME'
  LANGUAGE C STRICT;

  COMMENT ON FUNCTION oai_fdw_validator(text[], oid) IS 'OAI foreign data wrapper options validator';
  
CREATE FOREIGN DATA WRAPPER oai_fdw
  HANDLER oai_fdw_handler
  VALIDATOR oai_fdw_validator;

CREATE PROCEDURE OAI_HarvestTable
(oai_table text, 
 target_table text, 
 page_size interval, 
 start_date timestamp, 
 end_date timestamp DEFAULT CURRENT_TIMESTAMP, 
 create_table boolean DEFAULT true,
 exec_verbose boolean DEFAULT false)
LANGUAGE plpgsql AS $$ 
DECLARE 
  rec record;
  fdw text;  
  columns_list text; 
  columns_list_excluded text; 
  datestamp_column text;
  identifier_column text;
  foreign_table_name text;
  total_inserts bigint := 0;
  total_updates bigint := 0;
  target_table_exists boolean := false;  
  final_query text := '';
  insert_query text := '';
  conflict_clause text := '';
  inserted_records  bigint := 0;
  updated_records  bigint := 0; 
BEGIN
  
  IF oai_table !~~ '%.%' OR (oai_table !~~ '"%"."%"' AND oai_table ~~ '"%"') THEN
    oai_table := CURRENT_SCHEMA || '.' || oai_table;
  END IF;
  
  IF target_table !~~ '%.%' OR (target_table !~~ '"%"."%"' AND target_table ~~ '"%"') THEN
    target_table := CURRENT_SCHEMA || '.' || target_table;
  END IF;
  
  IF start_date > end_date THEN
    RAISE EXCEPTION 'invalid time window. The end date [%] lies before the start date [%]',start_date,end_date;
  END IF;
  
  target_table_exists := (SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname||'.'||tablename = target_table));
  
  SELECT   
    srv.foreign_data_wrapper_name AS fdw, 
    tb.foreign_table_name fdw_table_name,
    node_datestamp.attname AS fdw_datestamp,
    node_identifier.attname AS fdw_identifier,
    array_to_string(array_agg(col.attname),', ') AS fdw_table_cols,
    array_to_string(array_agg('EXCLUDED.'||col.attname),', ') AS fdw_table_cols_excluded
  INTO fdw, foreign_table_name, datestamp_column, identifier_column, columns_list, columns_list_excluded
  FROM information_schema._pg_foreign_tables tb
  JOIN information_schema._pg_foreign_table_columns col ON col.relname = tb.foreign_table_name
  JOIN information_schema._pg_foreign_servers srv ON srv.foreign_server_name = tb.foreign_server_name
  LEFT JOIN (
      SELECT nspname, relname, attname 
      FROM information_schema._pg_foreign_table_columns
      WHERE attfdwoptions <@ ARRAY['oai_node=datestamp']) node_datestamp ON 
            node_datestamp.relname = tb.foreign_table_name AND node_datestamp.nspname = tb.foreign_table_schema
  LEFT JOIN (
     SELECT nspname, relname, attname 
     FROM information_schema._pg_foreign_table_columns
     WHERE attfdwoptions <@ ARRAY['oai_node=identifier']) node_identifier ON 
           node_identifier.relname = tb.foreign_table_name AND node_identifier.nspname = tb.foreign_table_schema            
  WHERE tb.foreign_table_schema || '.' || tb.foreign_table_name = oai_table AND
        srv.foreign_data_wrapper_name = 'oai_fdw'
  GROUP BY srv.foreign_data_wrapper_name, tb.foreign_table_name, node_datestamp.attname, node_identifier.attname ;

  IF foreign_table_name IS NULL THEN
    RAISE EXCEPTION 'foreign table "%" does not exist', oai_table;
  END IF;
  
  IF columns_list IS NULL THEN
    RAISE EXCEPTION 'foreign table "%" does not have any oai_node mapping', oai_table;
  END IF; 
    
  IF datestamp_column IS NULL THEN 
    RAISE EXCEPTION 'foreign table "%" has no datestamp column',oai_table;
  END IF;           
                    
  IF create_table = true THEN 
  
    IF NOT target_table_exists THEN
    
      EXECUTE format('CREATE TABLE %s AS SELECT %s FROM %s WITH NO DATA;', target_table, columns_list, oai_table);
      
      IF identifier_column IS NOT NULL THEN 
        EXECUTE format('ALTER TABLE %s ADD PRIMARY KEY (%s);',target_table,identifier_column);      
      ELSE
        RAISE WARNING 'foreign table "%" has no identifier column. It is strongly recommended to map the OAI identifier to a column, as it can ensure that records are not duplicated',oai_table;              
      END IF;    
      RAISE INFO 'target table "%" created',target_table;  
    END IF;
    
  END IF;
  
  FOR rec IN
    SELECT LEAD(page) OVER w AS date_until, page AS date_from
    FROM generate_series(start_date, end_date, page_size) page   
    WINDOW w AS (ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING)
  LOOP
    IF rec.date_until IS NOT NULL THEN             
    
      insert_query := format(E'INSERT INTO %1$s (%2$s)  
                            SELECT %2$s FROM %3$s 
                            WHERE %4$s >= %5$L AND %4$s < %6$L\n', 
                            target_table, columns_list, oai_table, datestamp_column, rec.date_from, rec.date_until);
                      
      IF identifier_column IS NOT NULL THEN     
        conflict_clause := format('ON CONFLICT (%1$s) DO UPDATE SET (%2$s) = (%3$s)',identifier_column, columns_list, columns_list_excluded);
      END IF;    
      
	  final_query := format('
		WITH j AS (
		  %s 
		  %s 
		  RETURNING xmax=0 AS inserted) 
		SELECT 
		  COUNT(*) FILTER (WHERE inserted) AS inserted, 
		  COUNT(*) FILTER (WHERE NOT inserted) AS updated 
		FROM j', insert_query, conflict_clause);
	  
	  RAISE DEBUG '%',final_query;

	  EXECUTE final_query INTO inserted_records, updated_records;   
	  total_inserts := total_inserts + inserted_records;
	  total_updates := total_updates + updated_records;
      COMMIT;

      IF exec_verbose THEN
	    RAISE INFO 'page stored into "%": % records inserted and % updated [% - %]',
		            target_table, 
					inserted_records,
					updated_records,
					to_char(rec.date_from,'yyyy-mm-dd hh24:mi:ss'),to_char(rec.date_until,'yyyy-mm-dd hh24:mi:ss');
      END IF;
    END IF;
  END LOOP;
  RAISE INFO 'OAI harvester complete ("%" -> "%"): % records inserted and % updated [% - %]',
              oai_table,target_table,total_inserts,total_updates,to_char(start_date,'yyyy-mm-dd hh24:mi:ss'),to_char(end_date,'yyyy-mm-dd hh24:mi:ss');
END; $$;

COMMENT ON PROCEDURE OAI_HarvestTable(text,text,interval,timestamp,timestamp,boolean,boolean) IS 'Harvests an OAI foreign table and stores its records in a local table';
