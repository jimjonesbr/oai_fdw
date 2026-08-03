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