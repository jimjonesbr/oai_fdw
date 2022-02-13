--DROP EXTENSION IF EXISTS oai_fdw;
--CREATE EXTENSION oai_fdw;

SET client_min_messages to 'debug';

CREATE SERVER oai_server FOREIGN DATA WRAPPER oai_fdw;

--CREATE FOREIGN TABLE sequential_ints ( val int ) SERVER oai_server OPTIONS (start '10', end '15', url 'http://...', metadataPrefix 'marc21/xml');
--SELECT * FROM sequential_ints;

CREATE FOREIGN TABLE sequential_ints2 (val int, identifier text, doc text, datestamp text) 
SERVER oai_server OPTIONS (url 'https://sammlungen.ulb.uni-muenster.de/oai', 
                           set 'ulbmshs', 
                           metadataPrefix 'oai_dc', 
                           from '1980-04-01', 
                           until '2022-05-10');


SELECT * FROM sequential_ints2;

