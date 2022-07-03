CREATE SERVER oai_server_ulb FOREIGN DATA WRAPPER oai_fdw 
OPTIONS (url 'https://sammlungen.ulb.uni-muenster.de/oai');      

CREATE FOREIGN TABLE ulb_ulbmsuo_oai_dc (
  id text                OPTIONS (oai_node 'identifier'),
  xmldoc xml             OPTIONS (oai_node 'content'),
  sets text[]            OPTIONS (oai_node 'setspec'),
  updatedate timestamp   OPTIONS (oai_node 'datestamp'),
  format text            OPTIONS (oai_node 'metadataprefix'),
  status boolean         OPTIONS (oai_node 'status')
 ) SERVER oai_server_ulb OPTIONS (setspec 'ulbmsuo', 
                                  metadataPrefix 'oai_dc');


SELECT * FROM ulb_ulbmsuo_oai_dc LIMIT 10;

SET client_min_messages = 'debug1';
-----------------------------------------------------------
-- Limitation!
-- Data processed in the client
SELECT count(*) FILTER (WHERE updatedate > '2012-10-01') 
FROM ulb_ulbmsuo_oai_dc;

SELECT count(*)
FROM ulb_ulbmsuo_oai_dc
WHERE updatedate > '2012-10-01';
-------------------------------------------------------------

SELECT id, sets, format, updatedate,count(*) OVER w 
FROM ulb_ulbmsuo_oai_dc
WINDOW w AS (PARTITION BY sets ORDER BY updatedate
	         RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);



-------------------------------------------------------------
CREATE FOREIGN TABLE ulb_ulbmsuo_oai_dc2 (
  id text                OPTIONS (oai_node 'identifier'),
  xmldoc xml             OPTIONS (oai_node 'content'),
  sets text[]            OPTIONS (oai_node 'setspec'),
  updatedate timestamp   OPTIONS (oai_node 'datestamp'),
  format text            OPTIONS (oai_node 'metadataprefix'),
  status boolean         OPTIONS (oai_node 'status')
 ) SERVER oai_server_ulb OPTIONS (setspec 'ulbmsuo', 
                                  metadataPrefix 'oai_dc');

-- Limitation: JOIN performed locally.
SELECT * 
FROM ulb_ulbmsuo_oai_dc t1
JOIN ulb_ulbmsuo_oai_dc2 t2 ON t1.id = t2.id;



CREATE FOREIGN TABLE ulb_total (
  id text                OPTIONS (oai_node 'identifier'),
  xmldoc xml             OPTIONS (oai_node 'content'),
  sets text[]            OPTIONS (oai_node 'setspec'),
  updatedate timestamp   OPTIONS (oai_node 'datestamp'),
  format text            OPTIONS (oai_node 'metadataprefix'),
  status boolean         OPTIONS (oai_node 'status')
 ) SERVER oai_server_ulb OPTIONS (metadataPrefix 'oai_dc');
 
 CREATE TABLE tb_ulb_total2 AS
 SELECT * FROM ulb_total;