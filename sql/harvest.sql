CREATE FOREIGN TABLE IF NOT EXISTS ulb_oai_dc (
  id text                OPTIONS (oai_node 'identifier'), 
  xmldoc xml             OPTIONS (oai_node 'content'), 
  sets text[]            OPTIONS (oai_node 'setspec'), 
  updatedate timestamp   OPTIONS (oai_node 'datestamp'),
  format text            OPTIONS (oai_node 'metadataprefix')
 ) SERVER oai_server_ulb OPTIONS (metadataprefix 'oai_dc');

/* Target table without schema */
CALL OAI_HarvestTable('ulb_oai_dc','clone_ulb_oai_dc', interval '5 days', '2022-10-01 00:00:00', '2022-10-17 00:00:00',true,true);
SELECT count(*) FROM clone_ulb_oai_dc;

-- Existing table will be upserted 
CALL OAI_HarvestTable('ulb_oai_dc','clone_ulb_oai_dc', interval '5 days', '2022-10-01 00:00:00', '2022-10-17 00:00:00');
SELECT count(*) FROM clone_ulb_oai_dc;

/* Target table with specific schema */
CALL OAI_HarvestTable('ulb_oai_dc','oai_schema.clone_ulb_oai_dc', interval '5 days', '2022-10-01 00:00:00', '2022-10-17 00:00:00',true,true);
SELECT count(*) FROM oai_schema.clone_ulb_oai_dc;

-- Existing table will be upserted 
CALL OAI_HarvestTable('ulb_oai_dc','oai_schema.clone_ulb_oai_dc', interval '5 days', '2022-10-01 00:00:00', '2022-10-17 00:00:00');
SELECT count(*) FROM oai_schema.clone_ulb_oai_dc;

/* Target table containing special characters */
CALL OAI_HarvestTable('ulb_oai_dc','oai_schema."clone-ulb:oai/dc"', interval '5 days', '2022-10-01 00:00:00', '2022-10-17 00:00:00',true,true);
SELECT count(*) FROM oai_schema."clone-ulb:oai/dc";

/* Target table containing special characters without schema */
CALL OAI_HarvestTable('ulb_oai_dc','"clone-ulb:oai/dc"', interval '5 days', '2022-10-01 00:00:00', '2022-10-17 00:00:00',true,true);
SELECT count(*) FROM "clone-ulb:oai/dc";

/* WARNING: Foreign table without identifier */
CREATE FOREIGN TABLE IF NOT EXISTS table_without_identifier (
  xmldoc xml             OPTIONS (oai_node 'content'), 
  sets text[]            OPTIONS (oai_node 'setspec'), 
  updatedate timestamp   OPTIONS (oai_node 'datestamp'),
  format text            OPTIONS (oai_node 'metadataprefix')
 ) SERVER oai_server_ulb OPTIONS (metadataprefix 'oai_dc');
 
CALL OAI_HarvestTable('table_without_identifier','clone_table_without_identifier', interval '5 days', '2022-10-01 00:00:00', '2022-10-17 00:00:00',true,true); 

/* EXCEPTION: oai_table does not exist */
CALL OAI_HarvestTable('foo','clone_ulb_oai_dc', interval '5 days', '2022-10-01 00:00:00', '2022-10-17 00:00:00',true,true);

/* EXCEPTION: end date smaller than start date */
CALL OAI_HarvestTable('ulb_oai_dc','clone_ulb_oai_dc', interval '5 days', '2022-10-01 00:00:00', '2022-09-01 00:00:00',true,true);

/* EXCEPTION: Foreign table without datestamp */
CREATE FOREIGN TABLE IF NOT EXISTS table_without_datestamp (
  id text                OPTIONS (oai_node 'identifier'), 
  xmldoc xml             OPTIONS (oai_node 'content'), 
  sets text[]            OPTIONS (oai_node 'setspec'), 
  format text            OPTIONS (oai_node 'metadataprefix')
 ) SERVER oai_server_ulb OPTIONS (metadataprefix 'oai_dc');

CALL OAI_HarvestTable('table_without_datestamp', 'clone_table_without_datestamp', interval '5 days', '2022-10-01 00:00:00', '2022-10-17 00:00:00',true,true); 

/* EXCEPTION: Foreign table without datestamp and identifier*/
CREATE FOREIGN TABLE IF NOT EXISTS table_without_datestamp_identifier (
  xmldoc xml             OPTIONS (oai_node 'content'), 
  sets text[]            OPTIONS (oai_node 'setspec'), 
  format text            OPTIONS (oai_node 'metadataprefix')
 ) SERVER oai_server_ulb OPTIONS (metadataprefix 'oai_dc');

CALL OAI_HarvestTable('table_without_datestamp_identifier', 'clone_table_without_datestamp_identifier', interval '5 days', '2022-10-01 00:00:00', '2022-10-17 00:00:00',true,true); 

/* EXCEPTION: Foreign table without any oai_node */
CREATE FOREIGN TABLE IF NOT EXISTS table_without_oai_node (
  xmldoc xml, 
  sets text[], 
  format text
 ) SERVER oai_server_ulb OPTIONS (metadataprefix 'oai_dc');

CALL OAI_HarvestTable('table_without_oai_node', 'clone_table_without_oai_node', interval '5 days', '2022-10-01 00:00:00', '2022-10-17 00:00:00',true,true); 
