SET client_min_messages = 'debug3';

DROP EXTENSION IF EXISTS oai_fdw CASCADE;
CREATE EXTENSION oai_fdw;

SELECT oai_version();


/*
CREATE SERVER oai_server_ulb FOREIGN DATA WRAPPER oai_fdw 
OPTIONS (url 'https://sammlungen.ulb.uni-muenster.de/oai');

CREATE FOREIGN TABLE ulb_mods (
  id text                OPTIONS (oai_node 'identifier'),  
  sets text[]            OPTIONS (oai_node 'setspec'),
  updatedate timestamp   OPTIONS (oai_node 'datestamp'),
  format text            OPTIONS (oai_node 'metadataprefix'),
  status boolean         OPTIONS (oai_node 'status'),
  xmldoc xml             OPTIONS (oai_node 'content')
 ) SERVER oai_server_ulb OPTIONS (metadataPrefix 'oai_dc',setspec 'ulbmsuo');

--SELECT id,updatedate FROM ulb_mods LIMIT 1;
--EXPLAIN ANALYSE
-- SELECT count(*) FROM ulb_mods WHERE updatedate BETWEEN '2022-02-01' AND '2022-02-02' LIMIT 10;

--UPDATE ulb_mods set status = true;

SELECT * FROM ulb_mods limit 11;

*/
--LIMIT 56;

--DELETE FROM ulb_mods WHERE status = true;
--INSERT INTO ulb_mods (status) VALUES (false);

--group by 1 ;

-- SELECT * FROM oai_fdw_listMetadataFormats('oai_server_ulb');

         
/**









DROP SERVER oai_server_ulb CASCADE;

CREATE SERVER oai_server_ulb FOREIGN DATA WRAPPER oai_fdw 
OPTIONS (url 'https://sammlungen.ulb.uni-muenster.de/oai');  

CREATE FOREIGN TABLE ulb_mods (
  id text                OPTIONS (oai_node 'identifier'),
  xmldoc xml             OPTIONS (oai_node 'content'),
  sets text[]            OPTIONS (oai_node 'setspec'),
  updatedate timestamp   OPTIONS (oai_node 'datestamp'),
  format text            OPTIONS (oai_node 'metadataprefix'),
  status boolean         OPTIONS (oai_node 'status')
 ) SERVER oai_server_ulb OPTIONS (metadataPrefix 'mods');
								  
CREATE MATERIALIZED VIEW mvw_ulb_mods AS
SELECT * FROM ulb_mods;

SELECT count(*) FROM mvw_ulb_mods

--Successfully run. Total query runtime: 1 hr 28 min.
--1 rows affected.





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
