-- German National Library
CREATE SERVER oai_server_dnb FOREIGN DATA WRAPPER oai_fdw
OPTIONS (url 'https://services.dnb.de/oai/repository',
         request_redirect 'true',
         request_max_redirect '1');

-- This user mapping should be ignored, as the OAI repo does not require HTTP authentication
CREATE USER MAPPING FOR postgres 
SERVER oai_server_dnb OPTIONS (user 'admin', password 'secret', proxy_user 'proxyuser', proxy_password 'proxypass');

DROP SERVER oai_server_dnb CASCADE;