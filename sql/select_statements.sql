CREATE FOREIGN TABLE dnb_zdb_oai_dc (
  id text OPTIONS (oai_node 'identifier'),
  content text OPTIONS (oai_node 'content'),
  setspec text[] OPTIONS (oai_node 'setspec'),
  datestamp timestamp OPTIONS (oai_node 'datestamp'),
  meta text OPTIONS (oai_node 'metadataprefix')
 )
SERVER oai_server_dnb OPTIONS (setspec 'zdb', 
                               metadataPrefix 'oai_dc');

-- records between '2021-01-03' and '2021-01-04'
SELECT ROW_NUMBER() OVER (), id, content::xml IS DOCUMENT AS content, setspec, datestamp, meta
FROM dnb_zdb_oai_dc
WHERE
  datestamp BETWEEN '2021-01-03' AND '2021-01-04';

-- Records between '2022-02-01' and '2022-02-02'.
-- Override 'setspec' option -> dnb:reiheC.
-- Override 'metadataPrefix' -> MARC21/XML.
SELECT ROW_NUMBER() OVER (), id, content::xml IS DOCUMENT AS content, setspec, datestamp, meta
FROM dnb_zdb_oai_dc
WHERE
  datestamp BETWEEN '2022-02-01' AND '2022-02-02' AND
  meta = 'MARC21-xml' AND
  setspec <@ ARRAY['dnb:reiheC'];

-- Counting (computed locally!) between '2021-01-01' and '2021-01-31.
-- Override 'setspec' option -> dnb:reiheC.
SELECT count(*) FILTER (WHERE content::xml IS DOCUMENT)
FROM dnb_zdb_oai_dc
WHERE
  datestamp BETWEEN '2021-01-01' AND '2021-01-05' AND
  setspec <@ ARRAY['dnb:reiheC'];
  
-- GetRecord request
SELECT * FROM dnb_zdb_oai_dc
WHERE id = 'oai:dnb.de/zdb/1250800153';

-- ListIdentifiers request.
-- An OAI foreign table without a content column will be queried
-- with ListIndentifier requests.
CREATE FOREIGN TABLE dnb_zdb_oai_dc_nocontent (
  id text OPTIONS (oai_node 'identifier'),
  setspec text[] OPTIONS (oai_node 'setspec'),
  datestamp timestamp OPTIONS (oai_node 'datestamp'),
  meta text OPTIONS (oai_node 'metadataprefix')
 )                                             
SERVER oai_server_dnb OPTIONS (setspec 'zdb', 
                               metadataPrefix 'oai_dc');

-- Identifiers between '2021-01-01' and '2021-01-03'.
SELECT * 
FROM dnb_zdb_oai_dc_nocontent
WHERE datestamp BETWEEN '2021-01-03' AND '2021-01-04';

