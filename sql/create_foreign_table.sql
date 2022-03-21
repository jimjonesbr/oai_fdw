CREATE FOREIGN TABLE ulb_ulbmsuo_oai_dc (
  id text                OPTIONS (oai_node 'identifier'),
  xmldoc xml             OPTIONS (oai_node 'content'),
  sets text[]            OPTIONS (oai_node 'setspec'),
  updatedate timestamp   OPTIONS (oai_node 'datestamp'),
  format text            OPTIONS (oai_node 'metadataprefix'),
  status boolean         OPTIONS (oai_node 'status')
 ) SERVER oai_server_ulb OPTIONS (setspec 'ulbmsuo', 
                                  metadataPrefix 'oai_dc');

SELECT * FROM ulb_ulbmsuo_oai_dc;

CREATE FOREIGN TABLE foreign_table_without_oai_option (
  foo text
 ) SERVER oai_server_ulb OPTIONS (setspec 'ulbmsuo',
                                  metadataPrefix 'oai_dc');
                                 
SELECT * FROM foreign_table_without_oai_option;

-- FOREIGN TABLE without 'oai_node' OPTION but with the 
-- mandatory SERVER OPTION nodes.
CREATE FOREIGN TABLE foreign_table_without_oai_option2 (
  foo text
 ) SERVER oai_server_ulb OPTIONS (metadataPrefix 'oai_dc');
 
SELECT * FROM foreign_table_without_oai_option2;
 