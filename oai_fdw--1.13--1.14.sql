CREATE OR REPLACE FUNCTION OAI_Version()
  RETURNS text AS 'MODULE_PATHNAME', 'oai_fdw_version'
  LANGUAGE C VOLATILE STRICT;
  
  COMMENT ON FUNCTION OAI_Version() IS 'Shows current version of oai_fdw and its major libraries';

CREATE OR REPLACE FUNCTION OAI_ListMetadataFormats(text)
  RETURNS SETOF OAI_MetadataFormat AS 'MODULE_PATHNAME', 'oai_fdw_listMetadataFormats'
  LANGUAGE C VOLATILE STRICT;
  
  COMMENT ON FUNCTION OAI_ListMetadataFormats(text) IS 'Lists all metadata formats of a given OAI-PMH server';

CREATE OR REPLACE FUNCTION OAI_ListSets(text)
  RETURNS SETOF OAI_Set AS 'MODULE_PATHNAME', 'oai_fdw_listSets'
  LANGUAGE C VOLATILE STRICT;
  
  COMMENT ON FUNCTION OAI_ListSets(text) IS 'Lists all sets of a given OAI-PMH server';
  
CREATE OR REPLACE FUNCTION OAI_Identify(text)
  RETURNS SETOF OAI_IdentityNode AS 'MODULE_PATHNAME', 'oai_fdw_identity'
  LANGUAGE C VOLATILE STRICT;

/* unused PG type */
DROP TYPE oai_node;

/* new function */
CREATE FUNCTION oai_fdw_settings()
RETURNS text AS 'MODULE_PATHNAME', 'oai_fdw_settings'
LANGUAGE C STABLE STRICT;

COMMENT ON FUNCTION oai_fdw_settings() IS 'Returns detailed dependency information including optional components';

CREATE VIEW oai_fdw_settings AS
    WITH version_string AS (
        SELECT oai_fdw_settings() AS v
    )
    SELECT component, version
    FROM version_string,
    LATERAL (VALUES
        ('oai_fdw',    substring(v from 'oai_fdw\s+([^\s,]+)')),
        ('PostgreSQL', substring(v from 'PostgreSQL\s+([^,]+)')),
        ('libxml',     substring(v from 'libxml\s+([^,]+)')),
        ('libcurl',    substring(v from 'libcurl\s+([^,]+)')),
        ('ssl',        substring(v from ',ssl\s+([^,]+)')),
        ('zlib',       substring(v from ',zlib\s+([^,]+)')),
        ('libSSH',     substring(v from ',libSSH\s+([^,]+)')),
        ('nghttp2',    substring(v from ',nghttp2\s+([^,]+)')),
        ('compiler',   substring(v from 'compiled by\s+([^,]+)')),
        ('built',      substring(v from 'built on\s+([^,]+)'))
    ) AS components(component, version)
    WHERE version IS NOT NULL;

COMMENT ON VIEW oai_fdw_settings IS 'Parse detailed dependency information into component versions';

/* new signature: oai_version -> oai_fdw_version */
CREATE FUNCTION oai_fdw_version()
RETURNS text AS 'MODULE_PATHNAME', 'oai_fdw_version'
LANGUAGE C VOLATILE STRICT;
  
COMMENT ON FUNCTION oai_fdw_version() IS 'Shows current version of oai_fdw and its major libraries';
