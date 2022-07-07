CREATE EXTENSION oai_fdw;

-- German National Library
CREATE SERVER oai_server_dnb FOREIGN DATA WRAPPER oai_fdw 
OPTIONS (url 'https://services.dnb.de/oai/repository');

-- Münster University Library
CREATE SERVER oai_server_ulb FOREIGN DATA WRAPPER oai_fdw 
OPTIONS (url 'https://sammlungen.ulb.uni-muenster.de/oai');         

-- British and Irish Orthoptic Journal	
CREATE SERVER oai_server_bioj FOREIGN DATA WRAPPER oai_fdw 
OPTIONS (url 'https://www.bioj-online.com/jms/index.php/up/oai/');   

-- Brazilian Journal of Implantology and Health Sciences
CREATE SERVER oai_server_bjihs FOREIGN DATA WRAPPER oai_fdw 
OPTIONS (url 'https://bjihs.emnuvens.com.br/bjihs/oai');   

-- National Library of the Netherlands
CREATE SERVER oai_server_kbnl FOREIGN DATA WRAPPER oai_fdw 
OPTIONS (url 'http://services.kb.nl/mdo/oai');  

-- David Rumsey Historical Maps
CREATE SERVER oai_server_davidrumsey FOREIGN DATA WRAPPER oai_fdw 
OPTIONS (url 'http://www.davidrumsey.com/luna/servlet/oai');  