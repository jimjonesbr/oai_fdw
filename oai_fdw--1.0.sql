CREATE FUNCTION oai_fdw_handler() 
  RETURNS fdw_handler AS 'MODULE_PATHNAME' 
  LANGUAGE C STRICT;

/*
CREATE FUNCTION oai_fdw_validator(text[], oid)
  RETURNS void
  AS 'MODULE_PATHNAME'
  LANGUAGE 'c' STRICT;

CREATE FOREIGN DATA WRAPPER oai_fdw 
  HANDLER oai_fdw_handler
  VALIDATOR oai_fdw_validator;
*/
CREATE TYPE oai_node AS (
	code text,
	description text
);

CREATE OR REPLACE FUNCTION oai_fdw_version()
  RETURNS text AS 'MODULE_PATHNAME', 'oai_fdw_version'
  LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION oai_fdw_ListMetadataFormats(text)
  RETURNS SETOF oai_node AS 'MODULE_PATHNAME', 'oai_fdw_listMetadataFormats'
  LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
  
CREATE FUNCTION oai_fdw_validator(text[], oid)
  RETURNS void AS 'MODULE_PATHNAME'
  LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER oai_fdw
	HANDLER oai_fdw_handler
	VALIDATOR oai_fdw_validator;


COMMENT ON FUNCTION oai_fdw_validator(text[], oid)
IS 'OAI foreign data wrapper options validator';
