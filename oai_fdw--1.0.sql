CREATE FUNCTION oai_fdw_handler() 
  RETURNS fdw_handler AS 'MODULE_PATHNAME' 
  LANGUAGE C STRICT;

CREATE TYPE oai_node AS (
  code text,
  description text
);

CREATE TYPE OAI_IdentityNode AS (
  name text,
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
  
  COMMENT ON FUNCTION OAI_Version() IS 'Shows current version of oai_fdw and its major libraries';

CREATE OR REPLACE FUNCTION OAI_ListMetadataFormats(text)
  RETURNS SETOF OAI_MetadataFormat AS 'MODULE_PATHNAME', 'oai_fdw_listMetadataFormats'
  LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
  
  COMMENT ON FUNCTION OAI_ListMetadataFormats(text) IS 'Lists all metadata formats of a given OAI-PMH server';

CREATE OR REPLACE FUNCTION OAI_ListSets(text)
  RETURNS SETOF OAI_Set AS 'MODULE_PATHNAME', 'oai_fdw_listSets'
  LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
  
  COMMENT ON FUNCTION OAI_ListSets(text) IS 'Lists all sets of a given OAI-PMH server';
  
CREATE OR REPLACE FUNCTION OAI_Identify(text)
  RETURNS SETOF OAI_IdentityNode AS 'MODULE_PATHNAME', 'oai_fdw_identity'
  LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
  
  COMMENT ON FUNCTION OAI_Identify(text) IS 'Lists settings of a given OAI-PMH server';
  
CREATE FUNCTION oai_fdw_validator(text[], oid)
  RETURNS void AS 'MODULE_PATHNAME'
  LANGUAGE C STRICT;

  COMMENT ON FUNCTION oai_fdw_validator(text[], oid) IS 'OAI foreign data wrapper options validator';
  
CREATE FOREIGN DATA WRAPPER oai_fdw
  HANDLER oai_fdw_handler
  VALIDATOR oai_fdw_validator;
  