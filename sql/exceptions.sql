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

-- Unknown COLUMN OPTION value
CREATE FOREIGN TABLE oai_table_err1 (
  id text                OPTIONS (oai_node 'foo'),
  xmldoc xml             OPTIONS (oai_node 'content')
 ) SERVER oai_server_ulb OPTIONS (setspec 'ulbmsuo');       
        
