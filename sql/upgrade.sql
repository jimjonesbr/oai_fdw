CREATE SERVER oai_server_dnb FOREIGN DATA WRAPPER oai_fdw
OPTIONS (url 'https://services.dnb.de/oai/repository',
         request_redirect 'true',
         request_max_redirect '1');

CREATE FOREIGN TABLE dnb_zdb_oai_dc (
  id text OPTIONS (oai_node 'identifier'),
  content text OPTIONS (oai_node 'content'),
  setspec text[] OPTIONS (oai_node 'setspec'),
  datestamp timestamp OPTIONS (oai_node 'datestamp'),
  meta text OPTIONS (oai_node 'metadataprefix')
 )
SERVER oai_server_dnb OPTIONS (setspec 'zdb', 
                               metadataPrefix 'oai_dc');

ALTER EXTENSION oai_fdw UPDATE TO '1.12';
SELECT extversion FROM pg_extension WHERE extname = 'oai_fdw';

DROP SERVER oai_server_dnb CASCADE;