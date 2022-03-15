SELECT oai_fdw_version() IS NULL;
SELECT * FROM oai_fdw_listMetadataFormats('oai_server_ulb');
SELECT * FROM oai_fdw_listMetadataFormats('oai_server_dnb');