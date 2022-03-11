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
 ) SERVER oai_server_ulb OPTIONS (setspec 'ulbmsuo');
 
-- Empty COLUMN OPTION value
CREATE FOREIGN TABLE oai_table_err2 (
  id text                OPTIONS (oai_node ''),
  xmldoc xml             OPTIONS (oai_node 'content')
 ) SERVER oai_server_ulb OPTIONS (setspec 'ulbmsuo');
 
-- Empty SERVER OPTION value
CREATE FOREIGN TABLE oai_table_err3 (
  id text                OPTIONS (oai_node 'identifier'),
  xmldoc xml             OPTIONS (oai_node 'content')
 ) SERVER oai_server_ulb OPTIONS (setspec '');
 
 -- Empty SERVER OPTION value
CREATE FOREIGN TABLE oai_table_err4 (
  id text                OPTIONS (oai_node 'identifier'),
  xmldoc xml             OPTIONS (foo 'content')
 ) SERVER oai_server_ulb OPTIONS (setspec 'ulbmsuo');

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
