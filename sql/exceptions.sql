\set VERBOSITY terse
-- No metadataprefix SERVER OPTION
CREATE SERVER oai_server_err1 FOREIGN DATA WRAPPER oai_fdw 
OPTIONS (url 'https://services.dnb.de/oai/repository');

-- No url SERVER OPTION
CREATE SERVER oai_server_err2 FOREIGN DATA WRAPPER oai_fdw 
OPTIONS (metadataprefix 'oai_dc');

-- No SERVER OPTION 
CREATE SERVER oai_server_err3 FOREIGN DATA WRAPPER oai_fdw;

-- Unknown SERVER OPTION
CREATE SERVER oai_server_err4 FOREIGN DATA WRAPPER oai_fdw 
OPTIONS (url 'https://services.dnb.de/oai/repository',
         metadataprefix 'oai_dc',
         foo 'bar');  

-- Wrong URL
CREATE SERVER oai_server_err5 FOREIGN DATA WRAPPER oai_fdw 
OPTIONS (url 'a_very_wrong_url',
         metadataprefix 'oai_dc'); 

-- Wrong proxy URL
CREATE SERVER oai_server_err6 FOREIGN DATA WRAPPER oai_fdw 
OPTIONS (url 'https://services.dnb.de/oai/repository',
         metadataprefix 'oai_dc',
         http_proxy 'a_very_wrong_url'); 

-- Empty timeout
CREATE SERVER oai_server_err7 FOREIGN DATA WRAPPER oai_fdw
OPTIONS (url 'https://services.dnb.de/oai/repository',
         metadataprefix 'oai_dc',
         connect_timeout '');  

-- Nevative timeout
CREATE SERVER oai_server_err8 FOREIGN DATA WRAPPER oai_fdw
OPTIONS (url 'https://services.dnb.de/oai/repository',
         metadataprefix 'oai_dc',
         connect_timeout '-1');
         
-- Invalid timeout
CREATE SERVER oai_server_err9 FOREIGN DATA WRAPPER oai_fdw
OPTIONS (url 'https://services.dnb.de/oai/repository',
         metadataprefix 'oai_dc',
         connect_timeout 'foo');         
         
-- Empty metadataprefix
CREATE SERVER oai_server_err10 FOREIGN DATA WRAPPER oai_fdw 
OPTIONS (url 'https://services.dnb.de/oai/repository',
         metadataprefix '');  
         
-- Empty URL
CREATE SERVER oai_server_err11 FOREIGN DATA WRAPPER oai_fdw 
OPTIONS (url '',
         metadataprefix 'oai_dc');  

-- URL without protocol
CREATE SERVER oai_server_err12 FOREIGN DATA WRAPPER oai_fdw 
OPTIONS (url 'services.dnb.de/oai/repository',
         metadataprefix 'oai_dc');  

-- URL with an unsupported protocol
CREATE SERVER oai_server_err13 FOREIGN DATA WRAPPER oai_fdw 
OPTIONS (url 'ftp://services.dnb.de/oai/repository',
         metadataprefix 'oai_dc');  

CREATE FOREIGN TABLE oai_table_err13 (
  status boolean         OPTIONS (oai_node 'status')
) 
SERVER oai_server_err13 OPTIONS (metadataPrefix 'oai_dc');

SELECT * FROM oai_table_err13 LIMIT 1;

-- URL with an invalid protocol
CREATE SERVER oai_server_err14 FOREIGN DATA WRAPPER oai_fdw 
OPTIONS (url 'foo://services.dnb.de/oai/repository',
         metadataprefix 'oai_dc'); 

-- Empty connect_retry
CREATE SERVER oai_server_err15 FOREIGN DATA WRAPPER oai_fdw 
OPTIONS (url 'https://services.dnb.de/oai/repository',
         connect_retry ''); 
         
-- Invalid connect_retry
CREATE SERVER oai_server_err15 FOREIGN DATA WRAPPER oai_fdw 
OPTIONS (url 'https://services.dnb.de/oai/repository',
         connect_retry 'foo');

-- Unreachable proxy server
-- Timeout 1 second
-- Retry 5 times 
-- CREATE SERVER oai_server_err16 FOREIGN DATA WRAPPER oai_fdw 
-- OPTIONS (url 'https://services.dnb.de/oai/repository',
--          http_proxy 'http://proxy.server.im:8080',
--          connect_timeout '1',
--          connect_retry '5');
         
-- CREATE FOREIGN TABLE oai_table_err16 (
--   id text OPTIONS (oai_node 'identifier')
-- ) SERVER oai_server_err16 OPTIONS (metadataPrefix 'oai_dc');

-- SELECT * FROM oai_table_err16 LIMIT 1;


-- Unreachable proxy server
-- Timeout 1 second
-- Invalid retry (-5 times)
CREATE SERVER oai_server_err17 FOREIGN DATA WRAPPER oai_fdw 
OPTIONS (url 'https://services.dnb.de/oai/repository',
         http_proxy 'http://proxy.server.im:8080',
         connect_timeout '1',
         connect_retry '-5');

-- Invalid 'request_redirect' value
CREATE SERVER oai_server_err18 FOREIGN DATA WRAPPER oai_fdw 
OPTIONS (url 'https://services.dnb.de/oai/repository',
         request_redirect 'foo',
         request_max_redirect '1');

SELECT * FROM OAI_Identify('oai_server_err18');

-- Invalid 'request_redirect' value (empty)
CREATE SERVER oai_server_err19 FOREIGN DATA WRAPPER oai_fdw 
OPTIONS (url 'https://services.dnb.de/oai/repository',
         request_redirect '',
         request_max_redirect '1');

SELECT * FROM OAI_Identify('oai_server_err18');

-- Invalid 'request_max_redirect' value
CREATE SERVER oai_server_err20 FOREIGN DATA WRAPPER oai_fdw 
OPTIONS (url 'https://services.dnb.de/oai/repository',
         request_redirect 'true',
         request_max_redirect 'foo');

SELECT * FROM OAI_Identify('oai_server_err19');

-- Invalid 'request_max_redirect' value (empty)
CREATE SERVER oai_server_err21 FOREIGN DATA WRAPPER oai_fdw 
OPTIONS (url 'https://services.dnb.de/oai/repository',
         request_redirect 'true',
         request_max_redirect '');

SELECT * FROM OAI_Identify('oai_server_err21');

-- Unknown COLUMN OPTION value
CREATE FOREIGN TABLE oai_table_err1 (
  id text                OPTIONS (oai_node 'foo'),
  xmldoc xml             OPTIONS (oai_node 'content')
 ) 
 SERVER oai_server_ulb OPTIONS (setspec 'ulbmsuo',
                                metadataPrefix 'oai_dc');
 
-- Empty COLUMN OPTION value
CREATE FOREIGN TABLE oai_table_err2 (
  id text                OPTIONS (oai_node ''),
  xmldoc xml             OPTIONS (oai_node 'content')
 ) 
 SERVER oai_server_ulb OPTIONS (setspec 'ulbmsuo',
                                metadataPrefix 'oai_dc');
 
-- Empty SERVER OPTION value
CREATE FOREIGN TABLE oai_table_err3 (
  id text                OPTIONS (oai_node 'identifier'),
  xmldoc xml             OPTIONS (oai_node 'content')
 ) 
 SERVER oai_server_ulb OPTIONS (setspec '',
                                metadataPrefix 'oai_dc');
 
 -- Empty SERVER OPTION value
CREATE FOREIGN TABLE oai_table_err4 (
  id text                OPTIONS (oai_node 'identifier'),
  xmldoc xml             OPTIONS (foo 'content')
 ) 
 SERVER oai_server_ulb OPTIONS (setspec 'ulbmsuo',
                                metadataPrefix '');
                                       

-- No record found: noRecordsMatch: The value of argument 'until' lies before argument 'from'
SELECT * FROM dnb_zdb_oai_dc
WHERE datestamp BETWEEN '2022-02-01' AND '2021-02-02';

-- cannotDisseminateFormat: foo
SELECT * FROM dnb_zdb_oai_dc
WHERE meta = 'foo';

-- badArgument: Unsupported set 'foo' !
SELECT * FROM dnb_zdb_oai_dc
WHERE setspec <@ ARRAY['foo'];

-- GetRecord: wrong identifier format
SELECT * FROM dnb_zdb_oai_dc
WHERE id = 'foo';

-- GetRecord: identifier does not exist
SELECT * FROM dnb_zdb_oai_dc
WHERE id = 'oai:dnb.de/zdb/0000000000';

-- Wrong data types
CREATE FOREIGN TABLE oai_table_err5 (
  id int                 OPTIONS (oai_node 'identifier')
 ) 
 SERVER oai_server_ulb OPTIONS (metadataPrefix 'oai_dc');

SELECT * FROM oai_table_err5 LIMIT 1;
 
CREATE FOREIGN TABLE oai_table_err6 (
 doc int                 OPTIONS (oai_node 'content')
) 
SERVER oai_server_ulb OPTIONS (metadataPrefix 'oai_dc');

SELECT * FROM oai_table_err6 LIMIT 1;

CREATE FOREIGN TABLE oai_table_err7 (
 sets varchar            OPTIONS (oai_node 'setspec')
) 
SERVER oai_server_ulb OPTIONS (metadataPrefix 'oai_dc');

SELECT * FROM oai_table_err7 LIMIT 1;

CREATE FOREIGN TABLE oai_table_err8 (
 updatedate text   OPTIONS (oai_node 'datestamp')
) 
SERVER oai_server_ulb OPTIONS (metadataPrefix 'oai_dc');

SELECT * FROM oai_table_err8 LIMIT 1;

CREATE FOREIGN TABLE oai_table_err9 (
   format xml            OPTIONS (oai_node 'metadataprefix')
) 
SERVER oai_server_ulb OPTIONS (metadataPrefix 'oai_dc');

SELECT * FROM oai_table_err9 LIMIT 1;

CREATE FOREIGN TABLE oai_table_err10 (
  status text         OPTIONS (oai_node 'status')
) 
SERVER oai_server_ulb OPTIONS (metadataPrefix 'oai_dc');

SELECT * FROM oai_table_err10 LIMIT 1;

-- OAI_ListMetadataFormats: Wrong FOREIGN SERVER
SELECT * FROM OAI_ListMetadataFormats('foo');

-- OAI_ListMetadataFormats: NULL FOREIGN SERVER
SELECT * FROM OAI_ListMetadataFormats(NULL);

-- OAI_ListSets: Wrong FOREIGN SERVER
SELECT * FROM OAI_ListSets('foo');

-- OAI_ListSes: NULL FOREIGN SERVER
SELECT * FROM OAI_ListSets(NULL);

-- OAI_Identify: Wrong FOREIGN SERVER
SELECT * FROM OAI_Identify('foo');

-- OAI_Identify: NULL FOREIGN SERVER
SELECT * FROM OAI_Identify(NULL);

CREATE SCHEMA exception_schema;
-- IMPORT FOREIGN SCHEMA without 'metadataprerifx' OPTION
IMPORT FOREIGN SCHEMA oai_sets FROM SERVER oai_server_ulb INTO exception_schema;

-- IMPORT FOREIGN SCHEMA with invalid 'metadataprerifx' OPTION
IMPORT FOREIGN SCHEMA oai_sets FROM SERVER oai_server_ulb INTO exception_schema OPTIONS (metadataprefix 'foo');

-- IMPORT FOREIGN SCHEMA with unknown OPTION
IMPORT FOREIGN SCHEMA oai_sets FROM SERVER oai_server_ulb INTO exception_schema OPTIONS (metadataprefix 'oai_dc', foo 'bar');

-- IMPORT FOREIGN SCHEMA with unknown foreign schema
IMPORT FOREIGN SCHEMA foo FROM SERVER oai_server_ulb INTO exception_schema OPTIONS (metadataprefix 'oai_dc');

-- UPDATE query
UPDATE ulb_ulbmsuo_oai_dc SET status = true;

-- INSERT query
INSERT INTO ulb_ulbmsuo_oai_dc (status) VALUES (false);

-- DELETE query
DELETE FROM ulb_ulbmsuo_oai_dc;

-- empty user name
CREATE USER MAPPING FOR postgres SERVER oai_server_ulb OPTIONS (user '', password 'foo');

-- empty password
CREATE USER MAPPING FOR postgres SERVER oai_server_ulb OPTIONS (user 'foo', password '');

-- invalid option
CREATE USER MAPPING FOR postgres SERVER oai_server_ulb OPTIONS (user 'jim', foo 'bar');