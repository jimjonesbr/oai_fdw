CREATE EXTENSION oai_fdw;

SELECT extversion FROM pg_extension WHERE extname = 'oai_fdw';

DROP EXTENSION oai_fdw;
