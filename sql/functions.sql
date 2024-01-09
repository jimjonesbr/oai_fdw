SELECT OAI_Version() IS NULL;
SELECT * FROM OAI_ListMetadataFormats('oai_server_ulb') 
ORDER BY metadataprefix;

SELECT * FROM OAI_ListMetadataFormats('oai_server_dnb') 
ORDER BY metadataprefix;

SELECT * FROM OAI_ListMetadataFormats('oai_server_bioj') 
ORDER BY metadataprefix;

SELECT * FROM OAI_ListMetadataFormats('oai_server_bjihs') 
ORDER BY metadataprefix;

SELECT * FROM OAI_ListMetadataFormats('oai_server_davidrumsey') 
ORDER BY metadataprefix;

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
ORDER BY 1;

SELECT * FROM OAI_Identify('oai_server_ulb')
ORDER BY 1;

SELECT * FROM OAI_Identify('oai_server_dnb')
ORDER BY 1;

SELECT * FROM OAI_Identify('oai_server_bioj')
ORDER BY 1;

SELECT * FROM OAI_Identify('oai_server_bjihs')
ORDER BY 1;

SELECT * FROM OAI_Identify('oai_server_davidrumsey')
ORDER BY 1;

