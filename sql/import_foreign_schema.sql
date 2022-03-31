-- Importing schema 'oai_sets'

CREATE SCHEMA ulb_schema;

IMPORT FOREIGN SCHEMA oai_sets 
FROM SERVER oai_server_ulb 
INTO ulb_schema 
  OPTIONS (metadataprefix 'oai_dc');
  
SELECT * FROM information_schema.foreign_tables
WHERE foreign_table_schema = 'ulb_schema';


-- Importing schema 'oai_repository'

IMPORT FOREIGN SCHEMA oai_repository 
FROM SERVER oai_server_ulb 
INTO ulb_schema 
  OPTIONS (metadataprefix 'oai_dc');
  
SELECT * FROM information_schema.foreign_tables
WHERE foreign_table_schema = 'ulb_schema';


-- Import schema 'oai_sets' LIMIT TO 

CREATE SCHEMA ulb_schema2;

IMPORT FOREIGN SCHEMA oai_sets LIMIT TO (ulbmshbw,ulbmshs) 
FROM SERVER oai_server_ulb 
INTO ulb_schema2 
  OPTIONS (metadataprefix 'oai_dc');
  
SELECT * FROM information_schema.foreign_tables
WHERE foreign_table_schema = 'ulb_schema2';


-- Import schema 'oai_sets' EXCEPT 

IMPORT FOREIGN SCHEMA oai_sets EXCEPT (ulbmshbw,ulbmshs) 
FROM SERVER oai_server_ulb 
INTO ulb_schema2 
  OPTIONS (metadataprefix 'oai_dc');

SELECT * FROM information_schema.foreign_tables
WHERE foreign_table_schema = 'ulb_schema2';


-- Importing schema 'oai_sets' (DNB)

CREATE SCHEMA dnb_schema;

IMPORT FOREIGN SCHEMA oai_sets 
FROM SERVER oai_server_dnb 
INTO dnb_schema 
  OPTIONS (metadataprefix 'MARC21-xml');
 
SELECT * FROM dnb_schema."dnb:reiheC" 
WHERE id = 'oai:dnb.de/dnb:reiheC/1253187622';