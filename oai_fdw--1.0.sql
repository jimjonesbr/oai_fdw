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

CREATE FUNCTION oai_fdw_validator(text[], oid)
	RETURNS void
	AS 'MODULE_PATHNAME'
	LANGUAGE 'c' STRICT;

CREATE FOREIGN DATA WRAPPER oai_fdw
	HANDLER oai_fdw_handler
	VALIDATOR oai_fdw_validator;


COMMENT ON FUNCTION oai_fdw_validator(text[], oid)
IS 'OAI foreign data wrapper options validator';
