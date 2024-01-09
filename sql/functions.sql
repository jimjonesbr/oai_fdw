SELECT OAI_Version() IS NULL;
SELECT * FROM OAI_ListMetadataFormats('oai_server_ulb') 
ORDER BY schema;

SELECT * FROM OAI_ListMetadataFormats('oai_server_dnb') 
ORDER BY schema;

SELECT * FROM OAI_ListMetadataFormats('oai_server_bioj') 
ORDER BY schema;

SELECT * FROM OAI_ListMetadataFormats('oai_server_bjihs') 
ORDER BY schema;

SELECT * FROM OAI_ListMetadataFormats('oai_server_davidrumsey') 
ORDER BY schema;

SELECT * FROM OAI_ListSets('oai_server_ulb') 
WHERE setname IS NOT NULL AND setspec IS NOT NULL 
ORDER BY setname 
FETCH FIRST ROW ONLY;

SELECT * FROM OAI_ListSets('oai_server_dnb') 
WHERE setname IS NOT NULL AND setspec IS NOT NULL 
ORDER BY setname 
FETCH FIRST ROW ONLY;

SELECT * FROM OAI_ListSets('oai_server_bjihs') 
WHERE setname IS NOT NULL AND setspec IS NOT NULL 
ORDER BY setname 
FETCH FIRST ROW ONLY;

SELECT * FROM OAI_ListSets('oai_server_davidrumsey') 
WHERE setname IS NOT NULL AND setspec IS NOT NULL 
ORDER BY setname 
FETCH FIRST ROW ONLY;

SELECT count(*) > 0 
FROM OAI_ListSets('oai_server_dnb') 
WHERE setname IS NOT NULL AND setspec IS NOT NULL;

SELECT * FROM OAI_ListSets('oai_server_bioj')
ORDER BY setspec;

SELECT * FROM OAI_Identify('oai_server_ulb')
ORDER BY name;

SELECT * FROM OAI_Identify('oai_server_dnb')
ORDER BY name;

SELECT * FROM OAI_Identify('oai_server_bioj')
ORDER BY name;

SELECT * FROM OAI_Identify('oai_server_bjihs')
ORDER BY name;

SELECT * FROM OAI_Identify('oai_server_davidrumsey')
ORDER BY name;

