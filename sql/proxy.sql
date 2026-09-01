SET client_min_messages TO DEBUG1;
 
/* tests for proxy servers without password */
CREATE SERVER oai_server_dnb FOREIGN DATA WRAPPER oai_fdw
OPTIONS (url 'https://services.dnb.de/oai/repository',
         http_proxy 'http://172.19.42.100:3128');

CREATE USER MAPPING FOR postgres 
SERVER oai_server_dnb OPTIONS (user 'admin', password 'secret', proxy_user 'proxyuser', proxy_password 'will-be-ignored');

CREATE FOREIGN TABLE dnb_zdb_oai_dc (
  id text OPTIONS (oai_node 'identifier'),
  content text OPTIONS (oai_node 'content'),
  setspec text[] OPTIONS (oai_node 'setspec'),
  datestamp timestamp OPTIONS (oai_node 'datestamp'),
  meta text OPTIONS (oai_node 'metadataprefix')
 )
SERVER oai_server_dnb OPTIONS (setspec 'zdb', 
                               metadataPrefix 'oai_dc');

SELECT ROW_NUMBER() OVER (), id, content::xml IS DOCUMENT AS content, setspec, datestamp, meta
FROM dnb_zdb_oai_dc
WHERE
  datestamp BETWEEN '2021-01-03' AND '2021-01-04';

ALTER USER MAPPING FOR postgres
SERVER oai_server_dnb OPTIONS (DROP proxy_password);

SELECT ROW_NUMBER() OVER (), id, content::xml IS DOCUMENT AS content, setspec, datestamp, meta
FROM dnb_zdb_oai_dc
WHERE
  datestamp BETWEEN '2021-01-03' AND '2021-01-04';

DROP SERVER oai_server_dnb CASCADE;

/* tests for proxy servers with password */
CREATE SERVER oai_server_dnb FOREIGN DATA WRAPPER oai_fdw
OPTIONS (url 'https://services.dnb.de/oai/repository',
         http_proxy 'http://172.19.42.101:3128');

CREATE FOREIGN TABLE dnb_zdb_oai_dc (
  id text OPTIONS (oai_node 'identifier'),
  content text OPTIONS (oai_node 'content'),
  setspec text[] OPTIONS (oai_node 'setspec'),
  datestamp timestamp OPTIONS (oai_node 'datestamp'),
  meta text OPTIONS (oai_node 'metadataprefix')
 )
SERVER oai_server_dnb OPTIONS (setspec 'zdb', 
                               metadataPrefix 'oai_dc');

CREATE USER MAPPING FOR postgres 
SERVER oai_server_dnb OPTIONS (user 'admin', password 'secret', proxy_user 'proxyuser', proxy_password 'proxypass');

SELECT ROW_NUMBER() OVER (), id, content::xml IS DOCUMENT AS content, setspec, datestamp, meta
FROM dnb_zdb_oai_dc
WHERE
  datestamp BETWEEN '2021-01-03' AND '2021-01-04';

ALTER USER MAPPING FOR postgres
SERVER oai_server_dnb OPTIONS (SET proxy_password 'WRONGPASS!!!');

SELECT ROW_NUMBER() OVER (), id, content::xml IS DOCUMENT AS content, setspec, datestamp, meta
FROM dnb_zdb_oai_dc
WHERE
  datestamp BETWEEN '2021-01-03' AND '2021-01-04';

DROP SERVER oai_server_dnb CASCADE;