SET client_min_messages = 'debug2';

DROP EXTENSION IF EXISTS oai_fdw;
CREATE EXTENSION oai_fdw;

SELECT oai_version();

-- SELECT * FROM oai_fdw_listMetadataFormats('oai_server_ulb');

         
/**

CREATE SERVER oai_server FOREIGN DATA WRAPPER oai_fdw 
OPTIONS (url 'https://sammlungen.ulb.uni-muenster.de/oai',
         metadataPrefix 'oai_dc');
         
CREATE SERVER oai_server_dnb FOREIGN DATA WRAPPER oai_fdw 
OPTIONS (url 'https://services.dnb.de/oai/repository',
         metadataPrefix 'oai_dc');

CREATE SERVER oai_server_ulb FOREIGN DATA WRAPPER oai_fdw 
OPTIONS (url 'https://sammlungen.ulb.uni-muenster.de/oai',
         metadataPrefix 'oai_dc'); 
         
         

CREATE FOREIGN TABLE ulb_ulbmsuo_oai_dc (id text) 
SERVER oai_server OPTIONS (setspec 'ulbmsuo');

SELECT id FROM ulb_ulbmsuo_oai_dc;


SELECT * FROM oai_fdw_listMetadataFormats('oai_server');

 
         
SELECT * FROM oai_fdw_listMetadataFormats('oai_server_dnb');
CREATE FOREIGN TABLE ulb_ulbmsuo_oai_dc (
  id text                OPTIONS (oai_attribute 'identifier'),
  xmldoc xml             OPTIONS (oai_attribute 'content'),
  sets text[]            OPTIONS (oai_attribute 'setspec'),
  updatedate timestamp   OPTIONS (oai_attribute 'datestamp'),
  format text            OPTIONS (oai_attribute 'metadataprefix')
 ) SERVER oai_server OPTIONS (setspec 'ulbmsuo');

DROP TABLE IF EXISTS tb_ulb_ulbmsuo_oai_dc;
CREATE TABLE tb_ulb_ulbmsuo_oai_dc
AS SELECT * FROM ulb_ulbmsuo_oai_dc;

SELECT count(*),count(distinct id) FROM tb_ulb_ulbmsuo_oai_dc;


--

CREATE FOREIGN TABLE dnb_zdb_oai_dc (
  id text OPTIONS (oai_attribute 'identifier'),
  content text OPTIONS (oai_attribute 'content'),
  setspec text[] OPTIONS (oai_attribute 'setspec'),
  datestamp timestamp OPTIONS (oai_attribute 'datestamp'),
  meta text OPTIONS (oai_attribute 'metadataprefix')
 )
SERVER oai_server;


DROP TABLE IF EXISTS tb_dnb_zdb_oai_dc;
CREATE TABLE tb_dnb_zdb_oai_dc AS
SELECT * FROM dnb_zdb_oai_dc
WHERE
  meta = 'MARC21-xml' AND
  datestamp BETWEEN '2022-02-01' AND '2022-02-02' AND
  setspec <@ ARRAY['dnb:reiheC'];


---

CREATE FOREIGN TABLE dnb_maps_marc21 (
  id text OPTIONS (oai_attribute 'identifier'),
  content text OPTIONS (oai_attribute 'content'),
  setspec text[] OPTIONS (oai_attribute 'setspec'),
  datestamp timestamp OPTIONS (oai_attribute 'datestamp'),
  meta text OPTIONS (oai_attribute 'metadataprefix')
 )
SERVER oai_server OPTIONS (setspec 'dnb:reiheC');


DROP TABLE IF EXISTS tb_dnb_maps_marc21;
CREATE TABLE tb_dnb_maps_marc21 AS
SELECT * FROM dnb_maps_marc21
WHERE datestamp BETWEEN '2021-01-01' AND '2021-01-31';

SELECT count(*),count(distinct id) FROM tb_dnb_maps_marc21;

*/
