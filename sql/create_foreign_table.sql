CREATE FOREIGN TABLE ulb_ulbmsuo_oai_dc (
  id text                OPTIONS (oai_attribute 'identifier'),
  xmldoc xml             OPTIONS (oai_attribute 'content'),
  sets text[]            OPTIONS (oai_attribute 'setspec'),
  updatedate timestamp   OPTIONS (oai_attribute 'datestamp'),
  format text            OPTIONS (oai_attribute 'metadataprefix')
 ) SERVER oai_server_ulb OPTIONS (setspec 'ulbmsuo');

CREATE TABLE tb_ulb_ulbmsuo_oai_dc AS SELECT * FROM ulb_ulbmsuo_oai_dc;

SELECT
  count(*) = count(distinct id) records_are_distinct,
  count(*) > 0 AS not_empty
FROM tb_ulb_ulbmsuo_oai_dc;;
