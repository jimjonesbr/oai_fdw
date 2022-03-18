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

-- Empty metadataprefix
CREATE SERVER oai_server_err6 FOREIGN DATA WRAPPER oai_fdw 
OPTIONS (url 'https://services.dnb.de/oai/repository',
         metadataprefix '');  
         
-- Empty URL
CREATE SERVER oai_server_err7 FOREIGN DATA WRAPPER oai_fdw 
OPTIONS (url '',
         metadataprefix 'oai_dc');  

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