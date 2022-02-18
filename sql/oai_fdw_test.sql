--DROP EXTENSION IF EXISTS oai_fdw;
--CREATE EXTENSION oai_fdw;

--SET client_min_messages to 'debug';

CREATE SERVER oai_server FOREIGN DATA WRAPPER oai_fdw;

--CREATE FOREIGN TABLE sequential_ints ( val int ) SERVER oai_server OPTIONS (start '10', end '15', url 'http://...', metadataPrefix 'marc21/xml');
--SELECT * FROM sequential_ints;

CREATE FOREIGN TABLE oai_ulb_ulbmshs (identifier text, content xml, setpec text[], datestamp timestamp) 
SERVER oai_server OPTIONS (url 'https://sammlungen.ulb.uni-muenster.de/oai', 
                           set 'ulbmshs', 
                           metadataPrefix 'oai_dc', 
                           from '2015-07-15', 
                           until '2015-07-15');


SELECT identifier, content, setpec, datestamp FROM oai_ulb_ulbmshs LIMIT 2;

/*

CREATE FOREIGN TABLE oai_ulb_ulbmshs_no_interval (identifier text, content xml, setpec text[], datestamp timestamp) 
SERVER oai_server OPTIONS (url 'https://sammlungen.ulb.uni-muenster.de/oai', 
                           set 'ulbmshs', 
                           metadataPrefix 'oai_dc');


SELECT identifier, content, setpec, datestamp FROM oai_ulb_ulbmshs_no_interval LIMIT 2;


CREATE FOREIGN TABLE oai_ulb_ulbmshs_no_interval_no_set (identifier text, content xml, setpec text[], datestamp timestamp) 
SERVER oai_server OPTIONS (url 'https://sammlungen.ulb.uni-muenster.de/oai', 
                           metadataPrefix 'oai_dc');


SELECT identifier, content, setpec, datestamp FROM oai_ulb_ulbmshs_no_interval_no_set LIMIT 2;


CREATE FOREIGN TABLE oai_ulb_ulbmshs_no_interval_no_set_no_prefix (identifier text, content xml, setpec text[], datestamp timestamp) 
SERVER oai_server OPTIONS (url 'https://sammlungen.ulb.uni-muenster.de/oai');

SELECT * FROM oai_ulb_ulbmshs_no_interval_no_set_no_prefix;


CREATE FOREIGN TABLE oai_ulb_ulbmshs_no_options (identifier text, content xml, setpec text[], datestamp timestamp) 
SERVER oai_server;

SELECT * FROM oai_ulb_ulbmshs_no_options;

CREATE FOREIGN TABLE oai_missung_columns (identifier text, content xml, setpec text[]) 
SERVER oai_server;

SELECT * FROM oai_missung_columns;


CREATE FOREIGN TABLE oai_wrong_types (identifier text, content xml, setpec text, datestamp timestamp) 
SERVER oai_server;

SELECT * FROM oai_wrong_types;


CREATE FOREIGN TABLE oai_unknown_prefix (identifier text, content xml, setpec text[], datestamp timestamp) 
SERVER oai_server OPTIONS (url 'https://sammlungen.ulb.uni-muenster.de/oai', 
                           metadataPrefix 'foo');

SELECT * FROM oai_unknown_prefix;

CREATE FOREIGN TABLE oai_unknown_set (identifier text, content xml, setpec text[], datestamp timestamp) 
SERVER oai_server OPTIONS (url 'https://sammlungen.ulb.uni-muenster.de/oai', 
                           metadataPrefix 'oai_dc',
						   set 'foo');
						   
SELECT * FROM oai_unknown_set;


CREATE FOREIGN TABLE oai_no_records (identifier text, content xml, setpec text[], datestamp timestamp) 
SERVER oai_server OPTIONS (url 'https://sammlungen.ulb.uni-muenster.de/oai', 
                           set 'ulbmshs', 
                           metadataPrefix 'oai_dc', 
                           from '2025-07-15', 
                           until '2025-07-15');

SELECT * FROM oai_no_records;


CREATE FOREIGN TABLE oai_wrong_url (identifier text, content xml, setpec text[], datestamp timestamp) 
SERVER oai_server OPTIONS (url 'https://sammlungen.ulb.uni-muenster.de', 
                           set 'ulbmshs', 
                           metadataPrefix 'oai_dc', 
                           from '2025-07-15', 
                           until '2025-07-15');

SELECT * FROM oai_wrong_url;


CREATE FOREIGN TABLE oai_invalid_url (identifier text, content xml, setpec text[], datestamp timestamp) 
SERVER oai_server OPTIONS (url 'najhakjakjhakhakhakhakjgakjajh', 
						   metadataPrefix 'oai_dc', 
                           set 'ulbmshs');

SELECT * FROM oai_invalid_url;

*/