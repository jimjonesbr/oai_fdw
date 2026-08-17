-- Münster University Library
CREATE SERVER oai_server_ulb FOREIGN DATA WRAPPER oai_fdw 
OPTIONS (url 'https://sammlungen.ulb.uni-muenster.de/oai',
         request_redirect 'true');

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
CREATE SERVER oai_server_dnb FOREIGN DATA WRAPPER oai_fdw
OPTIONS (url 'https://services.dnb.de/oai/repository',
         request_redirect 'true',
         request_max_redirect '1');

-- This user mapping should be ignored, as the OAI repo does not require HTTP authentication
CREATE USER MAPPING FOR postgres 
SERVER oai_server_dnb OPTIONS (user 'admin', password 'secret', proxy_user 'proxyuser', proxy_password 'proxypass');

CREATE SCHEMA dnb_schema;

IMPORT FOREIGN SCHEMA oai_sets 
FROM SERVER oai_server_dnb 
INTO dnb_schema 
  OPTIONS (metadataprefix 'MARC21-xml');
 
SELECT * FROM dnb_schema."dnb:reiheC" 
WHERE id = 'oai:dnb.de/dnb:reiheC/1253187622';