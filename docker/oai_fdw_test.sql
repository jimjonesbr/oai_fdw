DROP EXTENSION IF EXISTS oai_fdw;
CREATE EXTENSION oai_fdw;

--SET client_min_messages to 'debug1';

CREATE SERVER oai_server FOREIGN DATA WRAPPER oai_fdw;


CREATE FOREIGN TABLE ulb_ulbmsuo_oai_dc (
  id text                OPTIONS (oai_attribute 'identifier'),
  xmldoc xml             OPTIONS (oai_attribute 'content'),
  sets text[]            OPTIONS (oai_attribute 'setspec'),
  updatedate timestamp   OPTIONS (oai_attribute 'datestamp'),
  format text            OPTIONS (oai_attribute 'metadataPrefix')
 ) SERVER oai_server OPTIONS (url 'https://sammlungen.ulb.uni-muenster.de/oai',
                                  metadataPrefix 'oai_dc',
                                  set 'ulbmsuo');

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
  meta text OPTIONS (oai_attribute 'metadataPrefix')
 )
SERVER oai_server OPTIONS (url 'https://services.dnb.de/oai/repository',
                           metadataPrefix 'oai_dc');


DROP TABLE IF EXISTS tb_dnb_zdb_oai_dc;
CREATE TABLE tb_dnb_zdb_oai_dc AS
SELECT * FROM dnb_zdb_oai_dc
WHERE
  meta = 'MARC21-xml' AND
  datestamp BETWEEN '2022-02-01' AND '2022-02-02' AND
  setspec <@ ARRAY['dnb:reiheC'];


---


SET client_min_messages to 'debug1';

CREATE FOREIGN TABLE dnb_maps_marc21 (
  id text OPTIONS (oai_attribute 'identifier'),
  content text OPTIONS (oai_attribute 'content'),
  setspec text[] OPTIONS (oai_attribute 'setspec'),
  datestamp timestamp OPTIONS (oai_attribute 'datestamp'),
  meta text OPTIONS (oai_attribute 'metadataPrefix')
 )
SERVER oai_server OPTIONS (url 'https://services.dnb.de/oai/repository',
                           metadataPrefix 'MARC21-xml',
                           set 'dnb:reiheC');


DROP TABLE IF EXISTS tb_dnb_maps_marc21;
CREATE TABLE tb_dnb_maps_marc21 AS
SELECT * FROM dnb_maps_marc21
WHERE datestamp BETWEEN '2021-01-01' AND '2021-12-31';

SELECT count(*),count(distinct id) FROM tb_dnb_maps_marc21;
