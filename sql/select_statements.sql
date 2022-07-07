CREATE FOREIGN TABLE dnb_zdb_oai_dc (
  id text OPTIONS (oai_node 'identifier'),
  content text OPTIONS (oai_node 'content'),
  setspec text[] OPTIONS (oai_node 'setspec'),
  datestamp timestamp OPTIONS (oai_node 'datestamp'),
  meta text OPTIONS (oai_node 'metadataprefix')
 )
SERVER oai_server_dnb OPTIONS (setspec 'zdb', 
                               metadataPrefix 'oai_dc');

--override 'setspec' option
--modify 'metadataPrefix'
SELECT ROW_NUMBER() OVER (), * FROM dnb_zdb_oai_dc
WHERE
  meta = 'MARC21-xml' AND
  datestamp BETWEEN '2022-02-01' AND '2022-02-02' AND
  setspec <@ ARRAY['dnb:reiheC'];


--override 'set' option
--maps January,2021
SELECT ROW_NUMBER() OVER (), * 
FROM dnb_zdb_oai_dc
WHERE
  datestamp BETWEEN '2021-01-01' AND '2021-01-31' AND
  setspec <@ ARRAY['dnb:reiheC'];

-- counting (computed locally)
SELECT count(*) 
FROM dnb_zdb_oai_dc
WHERE
  datestamp BETWEEN '2021-01-01' AND '2021-01-31' AND
  setspec <@ ARRAY['dnb:reiheC'];
  
-- GetRecord request
SELECT * FROM dnb_zdb_oai_dc
WHERE id = 'oai:dnb.de/zdb/1250800153';


-- ListIdentifiers request
SELECT id,datestamp,meta,setspec 
FROM dnb_zdb_oai_dc
WHERE datestamp BETWEEN '2021-01-01' AND '2021-01-03';

-- SELECT using timestamps hh24:mm:ss hh24:mi:ss
SELECT * FROM davidrumsey_oai_dc 
WHERE updatedate BETWEEN '2022-07-04 03:23:24' AND '2022-07-04 03:23:25';
