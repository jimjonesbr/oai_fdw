--DROP EXTENSION IF EXISTS oai_fdw;
--CREATE EXTENSION oai_fdw;

SET client_min_messages to 'debug';

CREATE SERVER oai_server FOREIGN DATA WRAPPER oai_fdw;

--CREATE FOREIGN TABLE sequential_ints ( val int ) SERVER oai_server OPTIONS (start '10', end '15', url 'http://...', metadataPrefix 'marc21/xml');
--SELECT * FROM sequential_ints;

CREATE FOREIGN TABLE oai_ulb_ulbmshs (identifier text, content xml, setpec text[], datestamp timestamp) 
SERVER oai_server OPTIONS (url 'https://sammlungen.ulb.uni-muenster.de/oai', 
                           set 'ulbmshs', 
                           metadataPrefix 'oai_dc', 
                           from '2015-07-15', 
                           until '2015-07-15');


SELECT * FROM oai_ulb_ulbmshs LIMIT 26;

--2003-01-01 05:34:51.381304