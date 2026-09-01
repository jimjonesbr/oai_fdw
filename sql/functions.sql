\set VERBOSITY terse
SET client_min_messages TO DEBUG1;

CREATE SERVER oai_server_dnb FOREIGN DATA WRAPPER oai_fdw
OPTIONS (url 'https://services.dnb.de/oai/repository',
         request_redirect 'true',
         request_max_redirect '1');

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

-- Test oai_fdw_version() returns a non-empty string
SELECT length(oai_fdw_version()) > 0 AS version_exists;

-- Test oai_fdw_version() contains expected components
SELECT 
    oai_fdw_version() ~ 'oai_fdw\s+[0-9]+\.[0-9]+' AS has_oai_fdw_version,
    oai_fdw_version() ~ 'PostgreSQL\s+[0-9]+' AS has_postgresql_version,
    oai_fdw_version() ~ 'libxml\s+[0-9]+\.[0-9]+' AS has_libxml_version,
    oai_fdw_version() ~ 'libcurl\s+[0-9]+\.[0-9]+' AS has_libcurl_version;

-- Test nominatim_fdw_settings() C function returns a non-empty string
SELECT length(nominatim_fdw_settings()) > 0 AS settings_exists;

-- Test nominatim_fdw_settings() C function contains expected core components
SELECT 
    oai_fdw_settings() ~ 'oai_fdw\s+[0-9]+\.[0-9]+' AS has_oai_fdw,
    oai_fdw_settings() ~ 'PostgreSQL\s+[0-9]+' AS has_postgresql,
    oai_fdw_settings() ~ 'libxml\s+[0-9]+\.[0-9]+' AS has_libxml,
    oai_fdw_settings() ~ 'libcurl\s+[0-9]+\.[0-9]+' AS has_libcurl;

-- Test oai_fdw_settings view returns expected components
SELECT component, version IS NOT NULL AS has_version
FROM oai_fdw_settings
ORDER BY component COLLATE "C" DESC;

-- Test that oai_fdw_settings view returns core components
SELECT 
    COUNT(*) >= 5 AS has_minimum_components,
    COUNT(*) FILTER (WHERE component = 'oai_fdw') = 1 AS has_oai_fdw,
    COUNT(*) FILTER (WHERE component = 'PostgreSQL') = 1 AS has_postgresql,
    COUNT(*) FILTER (WHERE component = 'libxml') = 1 AS has_libxml,
    COUNT(*) FILTER (WHERE component = 'libcurl') = 1 AS has_libcurl
FROM oai_fdw_settings;

DROP SERVER oai_server_dnb CASCADE;