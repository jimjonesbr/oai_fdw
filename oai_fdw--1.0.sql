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

CREATE TYPE OAI_Set AS (
	setspec text,
	setname text
);

CREATE TYPE OAI_MetadataFormat AS (
	metadataPrefix text,
	schema text,
	metadataNamespace text
);

CREATE OR REPLACE FUNCTION OAI_Version()
  RETURNS text AS 'MODULE_PATHNAME', 'oai_fdw_version'
  LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION OAI_ListMetadataFormats(text)
  RETURNS SETOF OAI_MetadataFormat AS 'MODULE_PATHNAME', 'oai_fdw_listMetadataFormats'
  LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION OAI_ListSets(text)
  RETURNS SETOF OAI_Set AS 'MODULE_PATHNAME', 'oai_fdw_listSets'
  LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
  
CREATE FUNCTION oai_fdw_validator(text[], oid)
  RETURNS void AS 'MODULE_PATHNAME'
  LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER oai_fdw
	HANDLER oai_fdw_handler
	VALIDATOR oai_fdw_validator;


COMMENT ON FUNCTION oai_fdw_validator(text[], oid)
IS 'OAI foreign data wrapper options validator';
