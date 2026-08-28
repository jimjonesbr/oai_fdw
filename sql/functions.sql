\set VERBOSITY terse
SET client_min_messages TO DEBUG1;

SELECT OAI_Version() IS NULL;
SELECT * FROM OAI_ListMetadataFormats('oai_server_ulb') 
ORDER BY metadataprefix COLLATE "C";

SELECT * FROM OAI_ListMetadataFormats('oai_server_dnb') 
ORDER BY metadataprefix COLLATE "C";

SELECT * FROM OAI_ListSets('oai_server_ulb') 
WHERE setname IS NOT NULL AND setspec IS NOT NULL 
ORDER BY setname  COLLATE "C"
FETCH FIRST ROW ONLY;

SELECT * FROM OAI_ListSets('oai_server_dnb') 
WHERE setname IS NOT NULL AND setspec IS NOT NULL 
ORDER BY setname  COLLATE "C"
FETCH FIRST ROW ONLY;

SELECT count(*) > 0 
FROM OAI_ListSets('oai_server_dnb') 
WHERE setname IS NOT NULL AND setspec IS NOT NULL;

SELECT * FROM OAI_Identify('oai_server_ulb')
ORDER BY name COLLATE "C";

SELECT * FROM OAI_Identify('oai_server_dnb')
ORDER BY name COLLATE "C";
