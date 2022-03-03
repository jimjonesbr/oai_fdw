CREATE FOREIGN TABLE dnb_zdb_oai_dc (
  id text OPTIONS (oai_attribute 'identifier'),
  content text OPTIONS (oai_attribute 'content'),
  setspec text[] OPTIONS (oai_attribute 'setspec'),
  datestamp timestamp OPTIONS (oai_attribute 'datestamp'),
  meta text OPTIONS (oai_attribute 'metadataPrefix')
 )
SERVER oai_server_dnb OPTIONS (set 'zdb');

--override 'set' option
--modify 'metadataPrefix'
CREATE TABLE tb_dnb_zdb_oai_dc AS
SELECT * FROM dnb_zdb_oai_dc
WHERE
  meta = 'MARC21-xml' AND
  datestamp BETWEEN '2022-02-01' AND '2022-02-02' AND
  setspec <@ ARRAY['dnb:reiheC'];

SELECT
  count(*) = count(distinct id) records_are_distinct,
  count(*) > 0 AS not_empty
FROM tb_dnb_zdb_oai_dc;


--override 'set' option
--maps January,2021
CREATE TABLE tb_dnb_maps_marc21 AS
SELECT * FROM dnb_zdb_oai_dc
WHERE
  datestamp BETWEEN '2021-01-01' AND '2021-01-31' AND
  setspec <@ ARRAY['dnb:reiheC'];


SELECT
  count(*) = count(distinct id) records_are_distinct,
  count(*) > 0 AS not_empty
FROM tb_dnb_maps_marc21;
