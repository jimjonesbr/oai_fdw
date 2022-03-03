-- No metadataPrefix
CREATE SERVER oai_server_err1 FOREIGN DATA WRAPPER oai_fdw 
OPTIONS (url 'https://services.dnb.de/oai/repository');

-- No url
CREATE SERVER oai_server_err2 FOREIGN DATA WRAPPER oai_fdw 
OPTIONS (metadataPrefix 'oai_dc');

-- No options 
CREATE SERVER oai_server_err3 FOREIGN DATA WRAPPER oai_fdw;

-- Warning: unknown option
CREATE SERVER oai_server_warn1 FOREIGN DATA WRAPPER oai_fdw 
OPTIONS (url 'https://services.dnb.de/oai/repository',
         metadataPrefix 'oai_dc',
         foo 'bar');         
        